/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestFetchAndValidateState verifies the fetchAndValidateState helper across
// a range of conditions (missing cluster, no pods, pods owned by this cluster,
// and pods owned by a different cluster).
func TestFetchAndValidateState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	// helper to build a minimal owned pod with a specific etcd image tag.
	ownedPod := func(clusterName, namespace, uid, imageTag string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName + "-0",
				Namespace: namespace,
				Labels: map[string]string{
					"app":        clusterName,
					"controller": clusterName,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       clusterName,
					UID:        types.UID(uid),
					Controller: pointerToBool(true),
				}},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "etcd", Image: "gcr.io/etcd-development/etcd:" + imageTag},
				},
			},
		}
	}

	cases := []struct {
		name   string
		req    ctrl.Request
		ec     *ecv1alpha1.EtcdCluster
		pods   []*corev1.Pod
		assert func(t *testing.T, state *reconcileState, res ctrl.Result, err error)
	}{
		{
			name: "EtcdCluster Not Found",
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				assert.Nil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "No Pods Found",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "1"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				require.NotNil(t, state)
				assert.Equal(t, "etcd", state.cluster.Name)
				assert.Empty(t, state.pods)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Pod Exists and Owned",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "2"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			pods: []*corev1.Pod{ownedPod("etcd", "default", "2", "3.5.17")},
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				require.NotNil(t, state)
				assert.Equal(t, "etcd", state.cluster.Name)
				assert.Len(t, state.pods, 1)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Pod Not Owned By This Cluster Is Ignored",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "3"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			// Pod has different UID owner → filtered out by listOwnedPods.
			pods: []*corev1.Pod{ownedPod("etcd", "default", "other-uid", "3.5.17")},
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				require.NotNil(t, state)
				assert.Empty(t, state.pods)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			objs := []client.Object{}
			if tc.ec != nil {
				objs = append(objs, tc.ec)
			}
			for _, pod := range tc.pods {
				objs = append(objs, pod)
			}

			builder := fake.NewClientBuilder().WithScheme(scheme)
			if len(objs) > 0 {
				builder.WithObjects(objs...)
			}
			fakeClient := builder.Build()
			r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

			state, res, err := r.fetchAndValidateState(ctx, tc.req)
			tc.assert(t, state, res, err)
		})
	}
}

// TestValidateSpec verifies the validateSpec helper's upgrade-path validation,
// which checks the desired version in EtcdCluster.Spec against the version
// currently running in the first pod's image tag.
func TestValidateSpec(t *testing.T) {
	// helper to build a minimal pod with a specific etcd image tag.
	podWithImage := func(imageTag string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "etcd-0", Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "etcd", Image: "gcr.io/etcd-development/etcd:" + imageTag},
				},
			},
		}
	}

	cases := []struct {
		name    string
		version string
		pods    []*corev1.Pod
		assert  func(t *testing.T, err error)
	}{
		{
			name:    "No pods yet is a no-op",
			version: "3.5.17",
			assert: func(t *testing.T, err error) {
				assert.NoError(t, err)
			},
		},
		{
			name:    "Valid upgrade path",
			version: "3.6.17",
			pods:    []*corev1.Pod{podWithImage("3.5.17")},
			assert: func(t *testing.T, err error) {
				assert.NoError(t, err)
			},
		},
		{
			name:    "Cannot parse pod image tag",
			version: "3.6.17",
			pods:    []*corev1.Pod{podWithImage("")},
			assert: func(t *testing.T, err error) {
				// Image has no ":" so version can't be extracted; treated as a no-op.
				assert.NoError(t, err)
			},
		},
		{
			name:    "Invalid upgrade path",
			version: "3.7.1",
			pods:    []*corev1.Pod{podWithImage("3.5.17")},
			assert: func(t *testing.T, err error) {
				assert.Error(t, err)
			},
		},
		{
			name:    "Downgrades are unsupported",
			version: "3.5.1",
			pods:    []*corev1.Pod{podWithImage("3.6.10")},
			assert: func(t *testing.T, err error) {
				assert.Error(t, err)
			},
		},
		{
			name:    "Upgrade with non-semver versions",
			version: "foo",
			pods:    []*corev1.Pod{podWithImage("bar")},
			assert: func(t *testing.T, err error) {
				assert.NoError(t, err)
			},
		},
		{
			name:    "Equal tags are a no-op even if they are not semver",
			version: "bar",
			pods:    []*corev1.Pod{podWithImage("bar")},
			assert: func(t *testing.T, err error) {
				assert.NoError(t, err)
			},
		},
	}

	r := &EtcdClusterReconciler{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			state := &reconcileState{
				cluster: &ecv1alpha1.EtcdCluster{
					ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default"},
					Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: tc.version},
				},
				pods: tc.pods,
			}
			err := r.validateSpec(ctx, state)
			tc.assert(t, err)
		})
	}
}

// TestBootstrapCluster verifies the bootstrapCluster helper creates the first
// member pod and the headless Service when none exist, and is a no-op when the
// cluster is already bootstrapped.
func TestBootstrapCluster(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
			UID:       "1",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    3,
			Version: "3.5.17",
		},
	}

	t.Run("Initial Creation — no pods exist", func(t *testing.T) {
		ctx := t.Context()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		res, err := r.bootstrapCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		// Headless Service should be created.
		svc := &corev1.Service{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc))
		assert.Equal(t, "None", svc.Spec.ClusterIP)

		// Pod-0 should be created.
		pod := &corev1.Pod{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "etcd-0", Namespace: ec.Namespace}, pod))

		// Verify per-pod env vars contain the expected etcd bootstrap config.
		envMap := make(map[string]string)
		for _, e := range pod.Spec.Containers[0].Env {
			envMap[e.Name] = e.Value
		}
		assert.Equal(t, string(etcdClusterStateNew), envMap["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, envMap["ETCD_INITIAL_CLUSTER"], "etcd-0=")
		assert.Equal(t, etcdDataDir, envMap["ETCD_DATA_DIR"])

		// Pod must be owned by the EtcdCluster.
		require.Len(t, pod.OwnerReferences, 1)
		assert.Equal(t, ec.Name, pod.OwnerReferences[0].Name)
	})

	t.Run("Already Bootstrapped — pods exist", func(t *testing.T) {
		ctx := t.Context()

		pod0 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd-0",
				Namespace: ec.Namespace,
				Labels:    etcdClusterLabels(ec),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
		}
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: ec.Name, Namespace: ec.Namespace},
			Spec:       corev1.ServiceSpec{ClusterIP: "None"},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, pod0, svc).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, pods: []*corev1.Pod{pod0}}

		res, err := r.bootstrapCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res) // no requeue

		// No additional pods should have been created.
		podList := &corev1.PodList{}
		require.NoError(t, fakeClient.List(ctx, podList, client.InNamespace(ec.Namespace)))
		assert.Len(t, podList.Items, 1)
	})

	t.Run("Service created if missing even when pods exist", func(t *testing.T) {
		ctx := t.Context()

		pod0 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd-0",
				Namespace: ec.Namespace,
				Labels:    etcdClusterLabels(ec),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, pod0).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, pods: []*corev1.Pod{pod0}}

		res, err := r.bootstrapCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)

		svc := &corev1.Service{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc))
		assert.Equal(t, "None", svc.Spec.ClusterIP)
	})
}

type moveLeaderActivity struct {
	calls     int
	endpoints []string
	memberID  uint64
}

func NewFakeMoveLeader(t *testing.T, err error) *moveLeaderActivity {
	t.Helper()

	ml := &moveLeaderActivity{}
	original := moveLeader
	t.Cleanup(func() { moveLeader = original })

	moveLeader = func(cfg etcdutils.ClientConfig, memberID uint64) error {
		ml.calls++
		ml.endpoints = cfg.Endpoints
		ml.memberID = memberID
		return err
	}
	return ml
}

const (
	updateConfigCluster   = "etcd"
	updateConfigNamespace = "default"

	staleConfigHash = "000000000000"
)

func getMemberID(ordinal int) uint64 { return uint64(100 * (ordinal + 1)) }

func clientEndpoint(ordinal int) string {
	return clientEndpointForOrdinal(updateConfigCluster, updateConfigNamespace, ordinal, false)
}

func newCluster(t *testing.T, size int) *ecv1alpha1.EtcdCluster {
	t.Helper()
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: updateConfigCluster, Namespace: updateConfigNamespace},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:          size,
			Version:       "3.5.17",
			ImageRegistry: DefaultImageRegistry,
		},
	}
	require.NoError(t, k8sClient.Create(t.Context(), ec))
	t.Cleanup(func() {
		_ = k8sClient.Delete(context.Background(), ec)
	})
	return ec
}

func newMemberPods(t *testing.T, ec *ecv1alpha1.EtcdCluster, size int, staleOrdinals ...int) {
	t.Helper()
	desiredHash := EtcdClusterHash(ec)

	for ordinal := range size {
		pod := buildMemberPod(ec, memberPodName(ec.Name, ordinal), etcdClusterStateExisting, "ignored")
		pod.Annotations[HashMetadataKey] = desiredHash
		if slices.Contains(staleOrdinals, ordinal) {
			pod.Annotations[HashMetadataKey] = staleConfigHash
		}
		require.NoError(t, controllerutil.SetControllerReference(ec, pod, scheme.Scheme))
		require.NoError(t, k8sClient.Create(t.Context(), pod))
		t.Cleanup(func() {
			_ = k8sClient.Delete(context.Background(), pod, client.GracePeriodSeconds(0))
		})
	}
}

func setupReconcileState(t *testing.T, ec *ecv1alpha1.EtcdCluster, leaderOrdinal int) *reconcileState {
	t.Helper()
	pods, err := listOwnedPods(t.Context(), k8sClient, ec)
	require.NoError(t, err)

	state := &reconcileState{
		cluster:        ec,
		pods:           pods,
		memberListResp: &clientv3.MemberListResponse{},
	}
	for _, pod := range pods {
		ordinal := podOrdinal(pod.Name, ec.Name)
		state.memberListResp.Members = append(state.memberListResp.Members,
			&etcdserverpb.Member{ID: getMemberID(ordinal), Name: pod.Name})
		state.memberHealth = append(state.memberHealth, etcdutils.EpHealth{
			Ep:     clientEndpoint(ordinal),
			Health: true,
			Status: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{MemberId: getMemberID(ordinal)},
				Leader: getMemberID(leaderOrdinal),
			},
		})
	}
	return state
}

func movedLeadership(from, to int) moveLeaderActivity {
	return moveLeaderActivity{
		calls:     1,
		memberID:  getMemberID(to),
		endpoints: []string{clientEndpoint(from)},
	}
}

func remainingOrdinals(t *testing.T, ec *ecv1alpha1.EtcdCluster) []int {
	t.Helper()
	pods, err := listOwnedPods(t.Context(), k8sClient, ec)
	require.NoError(t, err)

	ordinals := make([]int, 0, len(pods))
	for _, pod := range pods {
		ordinals = append(ordinals, podOrdinal(pod.Name, ec.Name))
	}
	slices.Sort(ordinals)
	return ordinals
}

func TestUpdateConfig(t *testing.T) {
	tests := []struct {
		name           string
		size           int
		leaderOrdinal  int
		staleOrdinals  []int
		moveLeaderErr  error
		wantRemaining  []int
		wantMoveLeader moveLeaderActivity
	}{
		{
			name:          "no drift falls through to the next phase",
			size:          3,
			leaderOrdinal: 1,
			wantRemaining: []int{0, 1, 2},
		},
		{
			name:          "every pod drifted recreates the highest ordinal only",
			size:          3,
			leaderOrdinal: 0,
			staleOrdinals: []int{0, 1, 2},
			wantRemaining: []int{0, 1},
		},
		{
			name:          "drifted pod that is not the leader keeps leadership in place",
			size:          3,
			leaderOrdinal: 0,
			staleOrdinals: []int{2},
			wantRemaining: []int{0, 1},
		},
		{
			name:           "drifted leader hands leadership to the lowest ordinal",
			size:           3,
			leaderOrdinal:  2,
			staleOrdinals:  []int{2},
			wantRemaining:  []int{0, 1},
			wantMoveLeader: movedLeadership(2, 0),
		},
		{
			name:           "drifted leader on the lowest ordinal hands leadership to the next one",
			size:           3,
			leaderOrdinal:  0,
			staleOrdinals:  []int{0},
			wantRemaining:  []int{1, 2},
			wantMoveLeader: movedLeadership(0, 1),
		},
		{
			name:          "drifted lowest ordinal that is not the leader keeps leadership in place",
			size:          3,
			leaderOrdinal: 1,
			staleOrdinals: []int{0},
			wantRemaining: []int{1, 2},
		},
		{
			name:           "a failed transfer still recreates the pod",
			size:           3,
			leaderOrdinal:  2,
			staleOrdinals:  []int{2},
			moveLeaderErr:  errors.New("etcdserver: request timed out"),
			wantRemaining:  []int{0, 1},
			wantMoveLeader: movedLeadership(2, 0),
		},
		{
			name:          "single member cluster has nowhere to move leadership to",
			size:          1,
			leaderOrdinal: 0,
			staleOrdinals: []int{0},
			wantRemaining: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := newCluster(t, tt.size)
			newMemberPods(t, ec, tt.size, tt.staleOrdinals...)
			state := setupReconcileState(t, ec, tt.leaderOrdinal)
			require.Len(t, state.pods, tt.size)

			r := &EtcdClusterReconciler{Client: k8sClient, Scheme: scheme.Scheme}
			moveLeader := NewFakeMoveLeader(t, tt.moveLeaderErr)

			res, err := r.updateConfig(t.Context(), state)
			require.NoError(t, err)

			wantResult := ctrl.Result{}
			if len(tt.wantRemaining) != tt.size {
				wantResult = ctrl.Result{RequeueAfter: requeueDuration}
			}
			assert.Equal(t, wantResult, res)
			assert.Equal(t, tt.wantRemaining, remainingOrdinals(t, ec))
			assert.Equal(t, tt.wantMoveLeader, *moveLeader)
		})
	}
}

func TestUpdateConfigOnAlreadyGonePods(t *testing.T) {
	ec := newCluster(t, 3)
	newMemberPods(t, ec, 3, 2)
	state := setupReconcileState(t, ec, 0)

	r := &EtcdClusterReconciler{Client: k8sClient, Scheme: scheme.Scheme}
	NewFakeMoveLeader(t, nil)

	require.NoError(t, k8sClient.Delete(t.Context(), state.pods[2], client.GracePeriodSeconds(0)))

	res, err := r.updateConfig(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	assert.Equal(t, []int{0, 1}, remainingOrdinals(t, ec))
}
