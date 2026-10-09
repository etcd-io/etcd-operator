// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	testRecoverAnnotation = "operator.etcd.io/recover-quorum-from"
	testForceFlag         = "--force-new-cluster"
)

func quorumRecoveryScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))
	return scheme
}

// quorumRecoveryCluster returns a 3-member cluster with persistent storage,
// the only kind a lost-quorum recovery can preserve data for.
func quorumRecoveryCluster() *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "1"},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:        3,
			Version:     "3.5.17",
			StorageSpec: &ecv1alpha1.StorageSpec{AccessModes: corev1.ReadWriteOnce},
		},
	}
}

// quorumRecoveryMembers returns Ready members 0..n-1 with the cleanup
// finalizer, as the controller creates them.
func quorumRecoveryMembers(ec *ecv1alpha1.EtcdCluster, n int) []*ecv1alpha1.EtcdMember {
	members := make([]*ecv1alpha1.EtcdMember, 0, n)
	for i := range n {
		m := createMemberWithPhase(ec, i, ecv1alpha1.EtcdMemberReady)
		m.Finalizers = []string{memberCleanupFinalizer}
		members = append(members, m)
	}
	return members
}

func memberPod(ec *ecv1alpha1.EtcdCluster, ordinal int, args ...string) *corev1.Pod {
	p := makeConfigDriftPod(ec, ordinal, EtcdClusterHash(ec))
	p.Spec.Containers = []corev1.Container{{Name: "etcd", Args: args}}
	return p
}

// recoveryFixture builds the fake client and reconcile state for one
// dispatch pass. Pods listed in pods are both stored and handed to dispatch.
type recoveryFixture struct {
	ec      *ecv1alpha1.EtcdCluster
	members []*ecv1alpha1.EtcdMember
	pods    []*corev1.Pod
	extra   []client.Object
}

func (f recoveryFixture) build(t *testing.T) (*EtcdClusterReconciler, client.Client) {
	t.Helper()
	scheme := quorumRecoveryScheme(t)
	objs := make([]client.Object, 0, 1+len(f.members)+len(f.pods)+len(f.extra))
	objs = append(objs, f.ec)
	for _, m := range f.members {
		objs = append(objs, m)
	}
	for _, p := range f.pods {
		objs = append(objs, p)
	}
	objs = append(objs, f.extra...)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ecv1alpha1.EtcdCluster{}, &ecv1alpha1.EtcdMember{}).
		WithObjects(objs...).
		Build()
	return &EtcdClusterReconciler{Client: c, Scheme: scheme}, c
}

// snapshot reads the cluster, its members and Pods back from the client the
// way fetchAndValidateState would at the start of the next reconcile.
func snapshot(t *testing.T, c client.Client, ec *ecv1alpha1.EtcdCluster) *reconcileState {
	t.Helper()
	ctx := t.Context()
	cluster := &ecv1alpha1.EtcdCluster{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(ec), cluster))
	memberList := &ecv1alpha1.EtcdMemberList{}
	require.NoError(t, c.List(ctx, memberList, client.InNamespace(ec.Namespace)))
	slices.SortFunc(memberList.Items, func(a, b ecv1alpha1.EtcdMember) int { return a.Spec.Ordinal - b.Spec.Ordinal })
	podList := &corev1.PodList{}
	require.NoError(t, c.List(ctx, podList, client.InNamespace(ec.Namespace)))
	pods := make([]*corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pods = append(pods, &podList.Items[i])
	}
	slices.SortFunc(pods, func(a, b *corev1.Pod) int { return podOrdinal(a.Name, ec.Name) - podOrdinal(b.Name, ec.Name) })
	return &reconcileState{cluster: cluster, members: memberList.Items, pods: pods}
}

func statusFor(index uint64, learner bool) etcdutils.EpHealth {
	return etcdutils.EpHealth{
		Health: true,
		Status: &clientv3.StatusResponse{Header: &etcdserverpb.ResponseHeader{}, RaftIndex: index, IsLearner: learner},
	}
}

// aloneMemberList is what the survivor answers once --force-new-cluster has
// taken effect: itself and nothing else.
func aloneMemberList(name string) *clientv3.MemberListResponse {
	return &clientv3.MemberListResponse{Members: []*etcdserverpb.Member{{
		ID:       1,
		Name:     name,
		PeerURLs: []string{"http://" + name + ".etcd.default.svc.cluster.local:2380"},
	}}}
}

func TestQuorumRecoveryRequest(t *testing.T) {
	unhealthy := &etcdutils.ClusterHealth{Healthy: false}

	// A human operator starts the recovery by naming the survivor; the
	// decision must be recorded before anything is touched (§4.8 step 1).
	t.Run("Request on an unhealthy cluster records the survivor and removes the annotation", func(t *testing.T) {
		ec := quorumRecoveryCluster()
		ec.Annotations = map[string]string{testRecoverAnnotation: "1"}
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3), pods: []*corev1.Pod{memberPod(ec, 1)}}
		r, c := f.build(t)
		s := snapshot(t, c, ec)
		s.health = unhealthy

		res, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		got := snapshot(t, c, ec)
		require.NotNil(t, got.cluster.Status.QuorumRecovery, "the recovery must be recorded")
		assert.Equal(t, 1, got.cluster.Status.QuorumRecovery.Survivor)
		assert.NotContains(t, got.cluster.Annotations, testRecoverAnnotation)
		assert.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(f.pods[0]), &corev1.Pod{}), "nothing is touched before the decision is recorded")
	})

	t.Run("auto picks the reachable voting member with the highest raft index", func(t *testing.T) {
		ec := quorumRecoveryCluster()
		ec.Annotations = map[string]string{testRecoverAnnotation: "auto"}
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3), pods: []*corev1.Pod{memberPod(ec, 0), memberPod(ec, 1), memberPod(ec, 2)}}
		r, c := f.build(t)
		s := snapshot(t, c, ec)
		s.health = &etcdutils.ClusterHealth{Healthy: false, Members: map[string]etcdutils.EpHealth{
			"etcd-0": statusFor(10, false),
			"etcd-1": statusFor(12, false),
			"etcd-2": statusFor(15, true), // a learner is never a survivor
		}}

		_, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)

		got := snapshot(t, c, ec)
		require.NotNil(t, got.cluster.Status.QuorumRecovery)
		assert.Equal(t, 1, got.cluster.Status.QuorumRecovery.Survivor)
	})

	t.Run("auto breaks a tie with the lowest ordinal", func(t *testing.T) {
		ec := quorumRecoveryCluster()
		ec.Annotations = map[string]string{testRecoverAnnotation: "auto"}
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3), pods: []*corev1.Pod{memberPod(ec, 1), memberPod(ec, 2)}}
		r, c := f.build(t)
		s := snapshot(t, c, ec)
		s.health = &etcdutils.ClusterHealth{Healthy: false, Members: map[string]etcdutils.EpHealth{
			"etcd-1": statusFor(20, false),
			"etcd-2": statusFor(20, false),
		}}

		_, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)

		got := snapshot(t, c, ec)
		require.NotNil(t, got.cluster.Status.QuorumRecovery)
		assert.Equal(t, 1, got.cluster.Status.QuorumRecovery.Survivor)
	})

	// auto must land on a member the recovery can use, not stop at the first
	// one with the highest index.
	autoSkips := []struct {
		name   string
		tweak  func(members []*ecv1alpha1.EtcdMember)
		health map[string]etcdutils.EpHealth
	}{
		{
			name:  "auto skips a member that is not a voter, whatever its raft index",
			tweak: func(m []*ecv1alpha1.EtcdMember) { m[2].Status.Phase = ecv1alpha1.EtcdMemberReplacing },
			health: map[string]etcdutils.EpHealth{
				"etcd-1": statusFor(12, false),
				"etcd-2": statusFor(20, false),
			},
		},
		{
			name: "auto skips an unhealthy member, whatever its raft index",
			health: map[string]etcdutils.EpHealth{
				"etcd-1": statusFor(12, false),
				"etcd-2": {Health: false, Status: statusFor(20, false).Status},
			},
		},
		{
			name: "auto skips a healthy member whose status is unavailable",
			health: map[string]etcdutils.EpHealth{
				"etcd-1": statusFor(12, false),
				"etcd-2": {Health: true},
			},
		},
	}
	for _, tt := range autoSkips {
		t.Run(tt.name, func(t *testing.T) {
			ec := quorumRecoveryCluster()
			ec.Annotations = map[string]string{testRecoverAnnotation: "auto"}
			members := quorumRecoveryMembers(ec, 3)
			if tt.tweak != nil {
				tt.tweak(members)
			}
			f := recoveryFixture{ec: ec, members: members, pods: []*corev1.Pod{memberPod(ec, 1), memberPod(ec, 2)}}
			r, c := f.build(t)
			s := snapshot(t, c, ec)
			s.health = &etcdutils.ClusterHealth{Healthy: false, Members: tt.health}

			_, err := r.dispatch(t.Context(), s)
			require.NoError(t, err)

			got := snapshot(t, c, ec)
			require.NotNil(t, got.cluster.Status.QuorumRecovery, "auto must still find a survivor")
			assert.Equal(t, 1, got.cluster.Status.QuorumRecovery.Survivor)
		})
	}

	// A request that can't be honored now is dropped, so it can't start a
	// recovery months later when the cluster loses quorum for another reason.
	tests := []struct {
		name    string
		value   string
		health  *etcdutils.ClusterHealth
		tweak   func(ec *ecv1alpha1.EtcdCluster, members []*ecv1alpha1.EtcdMember)
		healthy bool
	}{
		{name: "healthy cluster", value: "1", health: &etcdutils.ClusterHealth{Healthy: true}},
		{name: "not an ordinal", value: "etcd-1", health: unhealthy},
		{name: "negative ordinal", value: "-1", health: unhealthy},
		{name: "no member with that ordinal", value: "7", health: unhealthy},
		{
			name: "member is not a voter", value: "1", health: unhealthy,
			tweak: func(_ *ecv1alpha1.EtcdCluster, m []*ecv1alpha1.EtcdMember) {
				m[1].Status.Phase = ecv1alpha1.EtcdMemberReplacing
			},
		},
		{
			name: "member is a learner", value: "1",
			health: &etcdutils.ClusterHealth{Healthy: false, Members: map[string]etcdutils.EpHealth{"etcd-1": statusFor(9, true)}},
		},
		{
			name: "no persistent storage", value: "1", health: unhealthy,
			tweak: func(ec *ecv1alpha1.EtcdCluster, _ []*ecv1alpha1.EtcdMember) { ec.Spec.StorageSpec = nil },
		},
		{name: "auto with no reachable member", value: "auto", health: unhealthy},
	}
	for _, tt := range tests {
		t.Run("Request is dropped without a recovery: "+tt.name, func(t *testing.T) {
			ec := quorumRecoveryCluster()
			ec.Annotations = map[string]string{testRecoverAnnotation: tt.value}
			members := quorumRecoveryMembers(ec, 3)
			if tt.tweak != nil {
				tt.tweak(ec, members)
			}
			f := recoveryFixture{ec: ec, members: members, pods: []*corev1.Pod{memberPod(ec, 0), memberPod(ec, 1), memberPod(ec, 2)}}
			r, c := f.build(t)
			s := snapshot(t, c, ec)
			s.health = tt.health

			_, err := r.dispatch(t.Context(), s)
			require.NoError(t, err)

			got := snapshot(t, c, ec)
			assert.Nil(t, got.cluster.Status.QuorumRecovery, "no recovery may start")
			assert.NotContains(t, got.cluster.Annotations, testRecoverAnnotation, "the request must be dropped")
			assert.Len(t, got.pods, 3, "no Pod may be touched")
		})
	}
}

func TestContinueQuorumRecovery(t *testing.T) {
	recovering := func() *ecv1alpha1.EtcdCluster {
		ec := quorumRecoveryCluster()
		ec.Status.QuorumRecovery = &ecv1alpha1.QuorumRecoveryStatus{Survivor: 1}
		return ec
	}

	t.Run("Survivor Pod running without the flag is deleted", func(t *testing.T) {
		ec := recovering()
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3), pods: []*corev1.Pod{memberPod(ec, 1)}}
		r, c := f.build(t)
		s := snapshot(t, c, ec)
		s.health = &etcdutils.ClusterHealth{Healthy: false}

		_, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)

		err = c.Get(t.Context(), client.ObjectKeyFromObject(f.pods[0]), &corev1.Pod{})
		assert.True(t, apierrors.IsNotFound(err), "the survivor must be restarted with --force-new-cluster")
	})

	t.Run("Missing survivor Pod is created with the flag and an empty spec hash", func(t *testing.T) {
		ec := recovering()
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3)}
		r, c := f.build(t)
		s := snapshot(t, c, ec)

		_, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)

		pod := &corev1.Pod{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: "etcd-1", Namespace: ec.Namespace}, pod))
		assert.Contains(t, pod.Spec.Containers[0].Args, testForceFlag)
		// An empty hash makes updateConfig recreate the Pod without the flag
		// once the cluster is healthy, before scale-out adds members back.
		assert.Empty(t, pod.Annotations[HashMetadataKey])
	})

	t.Run("Recovery is cancelled when quorum is back before the survivor restarts", func(t *testing.T) {
		ec := recovering()
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3), pods: []*corev1.Pod{memberPod(ec, 0), memberPod(ec, 1), memberPod(ec, 2)}}
		r, c := f.build(t)
		s := snapshot(t, c, ec)
		s.health = &etcdutils.ClusterHealth{Healthy: true}

		_, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)

		got := snapshot(t, c, ec)
		assert.Nil(t, got.cluster.Status.QuorumRecovery)
		assert.Len(t, got.pods, 3)
		assert.Len(t, got.members, 3)
		assert.Zero(t, got.cluster.Status.RecoveryCount, "a cancelled recovery is not a recovery")
	})

	t.Run("Cancelling brings back a survivor Pod that was already deleted", func(t *testing.T) {
		ec := recovering()
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3), pods: []*corev1.Pod{memberPod(ec, 0), memberPod(ec, 2)}}
		r, c := f.build(t)
		s := snapshot(t, c, ec)
		s.health = &etcdutils.ClusterHealth{Healthy: true}

		_, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)

		pod := &corev1.Pod{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: "etcd-1", Namespace: ec.Namespace}, pod))
		assert.NotContains(t, pod.Spec.Containers[0].Args, testForceFlag)
		assert.Nil(t, snapshot(t, c, ec).cluster.Status.QuorumRecovery)
	})

	t.Run("Restarted survivor that is not alone yet terminates nobody", func(t *testing.T) {
		ec := recovering()
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3), pods: []*corev1.Pod{memberPod(ec, 1, testForceFlag)}}
		r, c := f.build(t)
		s := snapshot(t, c, ec)
		s.health = &etcdutils.ClusterHealth{Healthy: false}

		_, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)

		for _, m := range snapshot(t, c, ec).members {
			assert.Nil(t, m.DeletionTimestamp, "%s must not be terminated yet", m.Name)
		}
	})

	t.Run("Leftover request annotation is removed while a recovery runs", func(t *testing.T) {
		ec := recovering()
		ec.Annotations = map[string]string{testRecoverAnnotation: "2"}
		f := recoveryFixture{ec: ec, members: quorumRecoveryMembers(ec, 3), pods: []*corev1.Pod{memberPod(ec, 1, testForceFlag)}}
		r, c := f.build(t)
		s := snapshot(t, c, ec)

		_, err := r.dispatch(t.Context(), s)
		require.NoError(t, err)

		got := snapshot(t, c, ec)
		assert.NotContains(t, got.cluster.Annotations, testRecoverAnnotation)
		assert.Equal(t, 1, got.cluster.Status.QuorumRecovery.Survivor)
	})

	// The whole §4.8 mechanism: terminate the others once the survivor runs
	// alone, then record the recovery so scale-out can rebuild the cluster.
	t.Run("Recovery terminates the other members and finishes", func(t *testing.T) {
		ec := recovering()
		pvcs := make([]client.Object, 0, 3)
		for _, name := range []string{"etcd-0", "etcd-1", "etcd-2"} {
			pvcs = append(pvcs, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: pvcNameForMember(name), Namespace: ec.Namespace}})
		}
		f := recoveryFixture{
			ec:      ec,
			members: quorumRecoveryMembers(ec, 3),
			pods:    []*corev1.Pod{memberPod(ec, 0), memberPod(ec, 1, testForceFlag), memberPod(ec, 2)},
			extra:   pvcs,
		}
		r, c := f.build(t)

		for pass := 0; pass < 10; pass++ {
			s := snapshot(t, c, ec)
			if s.cluster.Status.QuorumRecovery == nil {
				break
			}
			s.health = &etcdutils.ClusterHealth{Healthy: true}
			s.memberListResp = aloneMemberList("etcd-1")
			res, err := r.dispatch(t.Context(), s)
			require.NoError(t, err)
			assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		}

		got := snapshot(t, c, ec)
		require.Nil(t, got.cluster.Status.QuorumRecovery, "the recovery must finish")
		assert.Equal(t, int32(1), got.cluster.Status.RecoveryCount)
		assert.Equal(t, "1", got.cluster.Status.LastRecoveredFrom)
		assert.NotNil(t, got.cluster.Status.LastRecoveryTime)
		require.Len(t, got.members, 1)
		assert.Equal(t, "etcd-1", got.members[0].Name)
		require.Len(t, got.pods, 1)
		assert.Equal(t, "etcd-1", got.pods[0].Name)
		for _, name := range []string{"etcd-0", "etcd-2"} {
			err := c.Get(t.Context(), client.ObjectKey{Name: pvcNameForMember(name), Namespace: ec.Namespace}, &corev1.PersistentVolumeClaim{})
			assert.True(t, apierrors.IsNotFound(err), "the PVC of %s must be deleted", name)
		}
		err := c.Get(t.Context(), client.ObjectKey{Name: pvcNameForMember("etcd-1"), Namespace: ec.Namespace}, &corev1.PersistentVolumeClaim{})
		assert.NoError(t, err, "the survivor keeps its data")
	})
}

// etcd applies --force-new-cluster on every start, so a survivor that kept
// the flag would drop the members scale-out adds back the next time its
// container restarts. The flag must go before scale-out runs.
func TestRecoveredSurvivorIsRecreatedBeforeScaleOut(t *testing.T) {
	ec := quorumRecoveryCluster()
	ec.Status.RecoveryCount = 1
	members := quorumRecoveryMembers(ec, 2)[1:] // only etcd-1 is left
	pod := memberPod(ec, 1, testForceFlag)
	pod.Annotations[HashMetadataKey] = ""
	f := recoveryFixture{ec: ec, members: members, pods: []*corev1.Pod{pod}}
	r, c := f.build(t)
	s := snapshot(t, c, ec)
	s.health = &etcdutils.ClusterHealth{Healthy: true}
	s.memberListResp = aloneMemberList("etcd-1")

	_, err := r.dispatch(t.Context(), s)
	require.NoError(t, err)

	got := snapshot(t, c, ec)
	require.Len(t, got.members, 1, "scale-out must wait until the survivor runs without the flag")
	assert.Equal(t, ecv1alpha1.EtcdMemberRecreating, got.members[0].Status.Phase)
}

func TestHealthCheckPods(t *testing.T) {
	ec := quorumRecoveryCluster()
	plain := []*corev1.Pod{memberPod(ec, 0), memberPod(ec, 1), memberPod(ec, 2)}
	forced := []*corev1.Pod{memberPod(ec, 0), memberPod(ec, 1, testForceFlag), memberPod(ec, 2)}

	tests := []struct {
		name     string
		recovery *ecv1alpha1.QuorumRecoveryStatus
		pods     []*corev1.Pod
		want     []string
	}{
		{name: "no recovery asks every Pod", pods: forced, want: []string{"etcd-0", "etcd-1", "etcd-2"}},
		{
			name: "recovery before the survivor restarts asks every Pod", recovery: &ecv1alpha1.QuorumRecoveryStatus{Survivor: 1},
			pods: plain, want: []string{"etcd-0", "etcd-1", "etcd-2"},
		},
		{
			name: "recovery after the survivor restarts asks only the survivor", recovery: &ecv1alpha1.QuorumRecoveryStatus{Survivor: 1},
			pods: forced, want: []string{"etcd-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := ec.DeepCopy()
			cluster.Status.QuorumRecovery = tt.recovery
			got := healthCheckPods(&reconcileState{cluster: cluster, pods: tt.pods})
			names := make([]string, 0, len(got))
			for _, p := range got {
				names = append(names, p.Name)
			}
			assert.Equal(t, tt.want, names)
		})
	}
}
