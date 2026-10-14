package controller

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
)

// recoveryFixture is a three-member cluster with persistent storage, whose
// objects are stored in a fake client by reconciler.
type recoveryFixture struct {
	cluster *ecv1alpha1.EtcdCluster
	members []*ecv1alpha1.EtcdMember
	pods    []*corev1.Pod
	pvcs    []*corev1.PersistentVolumeClaim
	funcs   interceptor.Funcs
}

// newRecoveryFixture returns Ready members 0 to 2 with the cleanup finalizer,
// as the controller creates them, and no Pods.
func newRecoveryFixture() *recoveryFixture {
	f := &recoveryFixture{cluster: leaveTestCluster()}
	for ordinal := range 3 {
		m := createMemberWithPhase(f.cluster, ordinal, ecv1alpha1.EtcdMemberReady)
		m.Finalizers = []string{memberCleanupFinalizer}
		f.members = append(f.members, m)
	}
	return f
}

func (f *recoveryFixture) requestRecoveryFrom(value string) {
	f.cluster.Annotations = map[string]string{recoverClusterFromAnnotation: value}
}

// addPod adds the Pod of member ordinal, starting etcd with args. reconciler
// gives it the cluster's current spec hash.
func (f *recoveryFixture) addPod(ordinal int, args ...string) *corev1.Pod {
	p := makeConfigDriftPod(f.cluster, ordinal, "")
	p.Spec.Containers = []corev1.Container{{Name: "etcd", Args: args}}
	f.pods = append(f.pods, p)
	return p
}

// addPVC adds the PVC of member ordinal.
func (f *recoveryFixture) addPVC(ordinal int) {
	f.pvcs = append(f.pvcs, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      pvcNameForMember(memberPodName(f.cluster.Name, ordinal)),
		Namespace: f.cluster.Namespace,
	}})
}

func (f *recoveryFixture) reconciler(t *testing.T) *EtcdClusterReconciler {
	t.Helper()
	scheme := leaveTestScheme(t)
	// The controller hashes the spec it reads back from the API server, and
	// a quantity in the spec only hashes the same once it went through JSON.
	data, err := json.Marshal(f.cluster)
	require.NoError(t, err)
	stored := &ecv1alpha1.EtcdCluster{}
	require.NoError(t, json.Unmarshal(data, stored))
	hash := EtcdClusterHash(stored)

	objs := make([]client.Object, 0, 1+len(f.members)+len(f.pods)+len(f.pvcs))
	objs = append(objs, f.cluster)
	for _, m := range f.members {
		objs = append(objs, m)
	}
	for _, p := range f.pods {
		if p.Annotations == nil {
			p.Annotations = map[string]string{HashMetadataKey: hash}
		}
		objs = append(objs, p)
	}
	for _, pvc := range f.pvcs {
		objs = append(objs, pvc)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ecv1alpha1.EtcdCluster{}, &ecv1alpha1.EtcdMember{}).
		WithObjects(objs...).
		WithInterceptorFuncs(f.funcs).
		Build()
	return &EtcdClusterReconciler{Client: c, Scheme: scheme, Recorder: events.NewFakeRecorder(100)}
}

// nextEvent returns the next event r recorded, or "" if there is none.
func nextEvent(r *EtcdClusterReconciler) string {
	select {
	case e := <-r.Recorder.(*events.FakeRecorder).Events:
		return e
	default:
		return ""
	}
}

// readState reads the cluster, its members and their Pods back from the
// client, sorted by ordinal the way fetchAndValidateState lists them.
func readState(t *testing.T, r *EtcdClusterReconciler, ec *ecv1alpha1.EtcdCluster, health *etcdutils.ClusterHealth) *reconcileState {
	t.Helper()
	ctx := t.Context()
	cluster := &ecv1alpha1.EtcdCluster{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(ec), cluster))
	members := &ecv1alpha1.EtcdMemberList{}
	require.NoError(t, r.List(ctx, members, client.InNamespace(ec.Namespace)))
	slices.SortFunc(members.Items, func(a, b ecv1alpha1.EtcdMember) int { return a.Spec.Ordinal - b.Spec.Ordinal })
	podList := &corev1.PodList{}
	require.NoError(t, r.List(ctx, podList, client.InNamespace(ec.Namespace)))
	pods := make([]*corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pods = append(pods, &podList.Items[i])
	}
	slices.SortFunc(pods, func(a, b *corev1.Pod) int { return podOrdinal(a.Name, ec.Name) - podOrdinal(b.Name, ec.Name) })
	return &reconcileState{cluster: cluster, members: members.Items, pods: pods, health: health}
}

// survivorPod returns the Pod of member 1, the survivor every recovery test
// recovers from, or nil if it doesn't exist.
func survivorPod(t *testing.T, r *EtcdClusterReconciler, ec *ecv1alpha1.EtcdCluster) *corev1.Pod {
	t.Helper()
	return memberPodOf(t, r, ec, 1)
}

// memberPodOf returns the Pod of member ordinal, or nil if it doesn't exist.
func memberPodOf(t *testing.T, r *EtcdClusterReconciler, ec *ecv1alpha1.EtcdCluster, ordinal int) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	err := r.Get(t.Context(), client.ObjectKey{Name: memberPodName(ec.Name, ordinal), Namespace: ec.Namespace}, pod)
	if apierrors.IsNotFound(err) {
		return nil
	}
	require.NoError(t, err)
	return pod
}

// forceNewClusterArg is the etcd flag a lost-quorum recovery restarts the
// survivor with.
const forceNewClusterArg = "--force-new-cluster"

// memberNames returns the names of the EtcdMembers left in s.
func memberNames(s *reconcileState) []string {
	names := make([]string, 0, len(s.members))
	for _, m := range s.members {
		names = append(names, m.Name)
	}
	return names
}

// pvcExists reports whether the PVC of member ordinal still exists.
func pvcExists(t *testing.T, r *EtcdClusterReconciler, ec *ecv1alpha1.EtcdCluster, ordinal int) bool {
	t.Helper()
	err := r.Get(t.Context(), client.ObjectKey{Name: pvcNameForMember(memberPodName(ec.Name, ordinal)), Namespace: ec.Namespace}, &corev1.PersistentVolumeClaim{})
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

var (
	now       = metav1.Now()
	unhealthy = &etcdutils.ClusterHealth{Healthy: false}
	healthy   = &etcdutils.ClusterHealth{Healthy: true}
)

func TestMaybeStartQuorumRecovery(t *testing.T) {
	// Starting only records the survivor: nothing is touched before the
	// decision is persisted (§4.8).
	t.Run("Records the requested survivor and touches nothing else", func(t *testing.T) {
		f := newRecoveryFixture()
		f.requestRecoveryFrom("1")
		f.addPod(1)
		r := f.reconciler(t)

		res, err := r.dispatch(t.Context(), readState(t, r, f.cluster, unhealthy))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		s := readState(t, r, f.cluster, nil)
		require.NotNil(t, s.cluster.Status.QuorumRecovery)
		assert.Equal(t, 1, s.cluster.Status.QuorumRecovery.Survivor)
		assert.Equal(t, "1", s.cluster.Annotations[recoverClusterFromAnnotation], "the annotation stays until the recovery completes")
		assert.NotNil(t, survivorPod(t, r, f.cluster))
		for _, m := range s.members {
			assert.Nil(t, m.DeletionTimestamp, "%s must not be terminated before the recovery is recorded", m.Name)
			assert.Equal(t, ecv1alpha1.EtcdMemberReady, m.Status.Phase, "%s must not be touched before the recovery is recorded", m.Name)
		}
	})

	// A request that names a member the recovery can't start from is ignored:
	// recreating that member's Pod would lose data.
	tests := []struct {
		name   string
		value  string
		health *etcdutils.ClusterHealth
		tweak  func(f *recoveryFixture)
	}{
		{name: "the cluster is healthy", value: "1", health: healthy},
		{name: "the value is not an ordinal", value: "etcd-1", health: unhealthy},
		{name: "the ordinal is negative", value: "-1", health: unhealthy},
		{name: "no member has the ordinal", value: "7", health: unhealthy},
		{
			name: "the member is being deleted", value: "1", health: unhealthy,
			tweak: func(f *recoveryFixture) { f.members[1].DeletionTimestamp = &now },
		},
		{
			name: "the member is not a voting member", value: "1", health: unhealthy,
			tweak: func(f *recoveryFixture) { f.members[1].Status.Phase = ecv1alpha1.EtcdMemberReplacing },
		},
		{
			name: "the member is being recreated", value: "1", health: unhealthy,
			tweak: func(f *recoveryFixture) { f.members[1].Status.Phase = ecv1alpha1.EtcdMemberRecreating },
		},
	}
	for _, tt := range tests {
		t.Run("Ignores a request when "+tt.name, func(t *testing.T) {
			f := newRecoveryFixture()
			f.requestRecoveryFrom(tt.value)
			f.addPod(1)
			if tt.tweak != nil {
				tt.tweak(f)
			}
			r := f.reconciler(t)

			res, err := r.maybeStartQuorumRecovery(t.Context(), readState(t, r, f.cluster, tt.health))
			require.NoError(t, err)
			assert.Equal(t, ctrl.Result{}, res)

			s := readState(t, r, f.cluster, nil)
			assert.Nil(t, s.cluster.Status.QuorumRecovery)
			assert.Equal(t, tt.value, s.cluster.Annotations[recoverClusterFromAnnotation])
			assert.NotNil(t, survivorPod(t, r, f.cluster))
		})
	}
}

func TestContinueQuorumRecovery(t *testing.T) {
	recovering := func() *recoveryFixture {
		f := newRecoveryFixture()
		f.requestRecoveryFrom("1")
		f.cluster.Status.QuorumRecovery = &ecv1alpha1.QuorumRecoveryStatus{Survivor: 1}
		return f
	}
	t.Run("Unhealthy cluster: deletes the survivor's Pod and terminates the other members", func(t *testing.T) {
		f := recovering()
		for ordinal := range 3 {
			f.addPod(ordinal)
			f.addPVC(ordinal)
		}
		r := f.reconciler(t)

		res, err := r.dispatch(t.Context(), readState(t, r, f.cluster, unhealthy))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		assert.Nil(t, survivorPod(t, r, f.cluster), "the survivor's Pod must go so it can come back with --force-new-cluster")
		assert.Equal(t, "Normal QuorumRecoveryStarted Recovering the cluster from EtcdMember etcd-1", nextEvent(r))
		for _, ordinal := range []int{0, 2} {
			assert.Nil(t, memberPodOf(t, r, f.cluster, ordinal), "etcd-%d must stop before the survivor comes back as a new cluster", ordinal)
		}
		s := readState(t, r, f.cluster, nil)
		assert.Equal(t, []string{"etcd-1"}, memberNames(s), "only the survivor's EtcdMember may remain")
		assert.False(t, pvcExists(t, r, f.cluster, 0))
		assert.True(t, pvcExists(t, r, f.cluster, 1), "the survivor's data must be kept")
		assert.False(t, pvcExists(t, r, f.cluster, 2))
		assert.Equal(t, &ecv1alpha1.QuorumRecoveryStatus{Survivor: 1}, s.cluster.Status.QuorumRecovery)
	})

	t.Run("Unhealthy cluster: creates the survivor's Pod with --force-new-cluster once the old one is gone", func(t *testing.T) {
		f := recovering()
		r := f.reconciler(t)

		_, err := r.dispatch(t.Context(), readState(t, r, f.cluster, unhealthy))
		require.NoError(t, err)

		pod := survivorPod(t, r, f.cluster)
		require.NotNil(t, pod)
		assert.Contains(t, pod.Spec.Containers[0].Args, forceNewClusterArg)
		env := envVarsToMap(pod)
		assert.Equal(t, string(etcdClusterStateExisting), env["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Equal(t, initialClusterForPod(readState(t, r, f.cluster, nil), f.members[1], true), env["ETCD_INITIAL_CLUSTER"])
		require.Len(t, pod.OwnerReferences, 1)
		assert.Equal(t, f.members[1].Name, pod.OwnerReferences[0].Name)
		require.Len(t, pod.Spec.Volumes, 1, "etcd must start from the survivor's data")
		assert.Equal(t, pvcNameForMember(f.members[1].Name), pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)
		assert.Equal(t, "Normal QuorumRecoveryStarted Recovering the cluster from EtcdMember etcd-1", nextEvent(r))
	})

	t.Run("Unhealthy cluster: leaves a survivor Pod that already has --force-new-cluster alone", func(t *testing.T) {
		f := recovering()
		f.addPod(1, forceNewClusterArg)
		r := f.reconciler(t)
		before := survivorPod(t, r, f.cluster)

		_, err := r.dispatch(t.Context(), readState(t, r, f.cluster, unhealthy))
		require.NoError(t, err)

		after := survivorPod(t, r, f.cluster)
		require.NotNil(t, after)
		assert.Equal(t, before.ResourceVersion, after.ResourceVersion)
		assert.Empty(t, nextEvent(r), "the event is only published when the Pod is about to be recreated")
	})

	t.Run("Unhealthy cluster: waits while the survivor's old Pod is being deleted", func(t *testing.T) {
		f := recovering()
		old := f.addPod(1)
		old.DeletionTimestamp = &now
		old.Finalizers = []string{"test.etcd.io/hold"}
		r := f.reconciler(t)

		_, err := r.dispatch(t.Context(), readState(t, r, f.cluster, unhealthy))
		require.NoError(t, err)

		pod := survivorPod(t, r, f.cluster)
		require.NotNil(t, pod)
		assert.NotContains(t, pod.Spec.Containers[0].Args, forceNewClusterArg, "the old Pod must not be replaced before it is gone")
		assert.Empty(t, nextEvent(r))
	})

	// A pass that failed between deleting an old member's Pod and its
	// EtcdMember leaves the cluster healthy, so the healthy branch finishes
	// terminating the old members.
	t.Run("Healthy cluster: removes the annotation, terminates the other members and clears QuorumRecovery", func(t *testing.T) {
		f := recovering()
		f.addPod(0)
		f.addPod(1, forceNewClusterArg)
		r := f.reconciler(t)

		res, err := r.dispatch(t.Context(), readState(t, r, f.cluster, healthy))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		s := readState(t, r, f.cluster, nil)
		assert.Nil(t, s.cluster.Status.QuorumRecovery)
		assert.NotContains(t, s.cluster.Annotations, recoverClusterFromAnnotation)
		assert.Equal(t, int32(1), s.cluster.Status.RecoveryCount)
		assert.Equal(t, "1", s.cluster.Status.LastRecoveredFrom)
		assert.NotNil(t, s.cluster.Status.LastRecoveryTime)
		assert.Equal(t, []string{"etcd-1"}, memberNames(s))
		assert.Nil(t, memberPodOf(t, r, f.cluster, 0))
		assert.NotNil(t, survivorPod(t, r, f.cluster))
	})

	t.Run("Healthy cluster: clears QuorumRecovery when no other member is left", func(t *testing.T) {
		f := recovering()
		f.members = f.members[1:2]
		f.addPod(1, forceNewClusterArg)
		r := f.reconciler(t)

		_, err := r.dispatch(t.Context(), readState(t, r, f.cluster, healthy))
		require.NoError(t, err)

		s := readState(t, r, f.cluster, nil)
		assert.Nil(t, s.cluster.Status.QuorumRecovery)
		assert.Equal(t, int32(1), s.cluster.Status.RecoveryCount)
		require.Len(t, s.members, 1)
		assert.Nil(t, s.members[0].DeletionTimestamp)
	})

	// Fail fast (§4.8): QuorumRecovery is only cleared once every step
	// before it succeeded, so the next reconcile resumes the recovery.
	t.Run("Keeps QuorumRecovery when removing the annotation fails", func(t *testing.T) {
		f := recovering()
		f.addPod(1, forceNewClusterArg)
		patchErr := errors.New("patch failed")
		f.funcs = interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*ecv1alpha1.EtcdCluster); ok {
					return patchErr
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}
		r := f.reconciler(t)

		_, err := r.dispatch(t.Context(), readState(t, r, f.cluster, healthy))
		require.ErrorIs(t, err, patchErr)

		s := readState(t, r, f.cluster, nil)
		assert.Equal(t, &ecv1alpha1.QuorumRecoveryStatus{Survivor: 1}, s.cluster.Status.QuorumRecovery)
		assert.Equal(t, "1", s.cluster.Annotations[recoverClusterFromAnnotation])
		assert.Len(t, s.members, 3, "nothing after the failed step may run")
	})

	t.Run("Returns an error while the survivor's EtcdMember is missing", func(t *testing.T) {
		f := recovering()
		f.members = []*ecv1alpha1.EtcdMember{f.members[0], f.members[2]}
		r := f.reconciler(t)

		_, err := r.dispatch(t.Context(), readState(t, r, f.cluster, unhealthy))
		require.Error(t, err)

		s := readState(t, r, f.cluster, nil)
		assert.Equal(t, &ecv1alpha1.QuorumRecoveryStatus{Survivor: 1}, s.cluster.Status.QuorumRecovery)
		assert.Equal(t, []string{"etcd-0", "etcd-2"}, memberNames(s))
	})
}
