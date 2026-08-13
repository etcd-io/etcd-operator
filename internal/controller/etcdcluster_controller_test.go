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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
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

// TestEnsureClusterPrereqs verifies the cert/TLS-config/headless-Service
// setup that runs unconditionally every reconcile, independent of members.
func TestEnsureClusterPrereqs(t *testing.T) {
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

	t.Run("Creates headless Service when missing", func(t *testing.T) {
		ctx := t.Context()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		err := r.ensureClusterPrereqs(ctx, state)
		assert.NoError(t, err)

		svc := &corev1.Service{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc))
		assert.Equal(t, "None", svc.Spec.ClusterIP)
	})

	t.Run("No-op when Service already exists", func(t *testing.T) {
		ctx := t.Context()
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: ec.Name, Namespace: ec.Namespace},
			Spec:       corev1.ServiceSpec{ClusterIP: "None"},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, svc).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		err := r.ensureClusterPrereqs(ctx, state)
		assert.NoError(t, err)
	})
}

// TestScaleClusterBootstrap verifies the scale-out dispatcher step's
// bootstrap special case (§4.6 step 2): with zero existing members, it
// creates EtcdMember ordinal 0 and its Pod directly. For any other ordinal,
// only the EtcdMember shell is created — join mechanics are M3's TODO.
func TestScaleClusterBootstrap(t *testing.T) {
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

	t.Run("No members — creates EtcdMember ordinal 0 and its Pod", func(t *testing.T) {
		ctx := t.Context()
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&ecv1alpha1.EtcdMember{}).
			WithObjects(ec).
			Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		res, err := r.scaleCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		member := &ecv1alpha1.EtcdMember{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "etcd-0", Namespace: ec.Namespace}, member))
		assert.Equal(t, 0, member.Spec.Ordinal)
		assert.Equal(t, ecv1alpha1.EtcdMemberPending, member.Status.Phase)
		assert.Contains(t, member.Finalizers, memberCleanupFinalizer)
		require.Len(t, member.OwnerReferences, 1)
		assert.Equal(t, ec.Name, member.OwnerReferences[0].Name)

		pod := &corev1.Pod{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "etcd-0", Namespace: ec.Namespace}, pod))
		require.Len(t, pod.OwnerReferences, 1)
		assert.Equal(t, ec.Name, pod.OwnerReferences[0].Name)
	})

	t.Run("One ready member — creates EtcdMember shell only, no Pod", func(t *testing.T) {
		ctx := t.Context()
		member0 := &ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "etcd-0",
				Namespace:  ec.Namespace,
				Finalizers: []string{memberCleanupFinalizer},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: 0, Version: ec.Spec.Version},
			Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&ecv1alpha1.EtcdMember{}).
			WithObjects(ec, member0).
			Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, members: []ecv1alpha1.EtcdMember{*member0}}

		res, err := r.scaleCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		member := &ecv1alpha1.EtcdMember{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "etcd-1", Namespace: ec.Namespace}, member))
		assert.Equal(t, 1, member.Spec.Ordinal)
		assert.Equal(t, ecv1alpha1.EtcdMemberPending, member.Status.Phase)

		// No Pod should have been created for the non-bootstrap ordinal.
		pod := &corev1.Pod{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: "etcd-1", Namespace: ec.Namespace}, pod)
		assert.True(t, apierrors.IsNotFound(err))
	})

	t.Run("At desired size — no-op", func(t *testing.T) {
		ctx := t.Context()
		ec3 := ec.DeepCopy()
		members := make([]ecv1alpha1.EtcdMember, 0, 3)
		for i := range 3 {
			members = append(members, ecv1alpha1.EtcdMember{
				Spec:   ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: i, Version: ec.Spec.Version},
				Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady},
			})
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec3).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec3, members: members}

		res, err := r.scaleCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)
	})

	t.Run("Over desired size — scale-in deletes the highest ordinal", func(t *testing.T) {
		ctx := t.Context()
		ec1 := ec.DeepCopy()
		ec1.Spec.Size = 1
		members := make([]ecv1alpha1.EtcdMember, 0, 2)
		objs := make([]client.Object, 0, 3)
		objs = append(objs, ec1)
		for i := range 2 {
			m := &ecv1alpha1.EtcdMember{
				ObjectMeta: metav1.ObjectMeta{Name: etcdMemberName(ec1.Name, i), Namespace: ec1.Namespace},
				Spec:       ecv1alpha1.EtcdMemberSpec{ClusterName: ec1.Name, Ordinal: i, Version: ec1.Spec.Version},
				Status:     ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady},
			}
			members = append(members, *m)
			objs = append(objs, m)
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec1, members: members}

		res, err := r.scaleCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		remaining := &ecv1alpha1.EtcdMemberList{}
		require.NoError(t, fakeClient.List(ctx, remaining, client.InNamespace(ec1.Namespace)))
		require.Len(t, remaining.Items, 1)
		assert.Equal(t, 0, remaining.Items[0].Spec.Ordinal)
	})
}

// TestDispatch verifies §4.9's priority order: each item either claims the
// loop or falls through to the next, and a higher-priority item wins even
// though most of the actions behind it are still M2 TODO no-ops.
func TestDispatch(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	baseCluster := func() *ecv1alpha1.EtcdCluster {
		return &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "1"},
			Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
		}
	}

	t.Run("Paused wins regardless of other state", func(t *testing.T) {
		ctx := t.Context()
		ec := baseCluster()
		ec.Spec.Paused = true
		ec.Status.QuorumRecovery = &ecv1alpha1.QuorumRecoveryStatus{Survivor: 0}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		res, err := r.dispatch(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	})

	t.Run("Terminating member stops the loop before scale-out is attempted", func(t *testing.T) {
		ctx := t.Context()
		ec := baseCluster()
		now := metav1.Now()
		terminating := ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "etcd-0",
				Namespace:         ec.Namespace,
				DeletionTimestamp: &now,
				Finalizers:        []string{memberCleanupFinalizer},
			},
			Spec:   ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: 0, Version: ec.Spec.Version},
			Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberTerminating},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, &terminating).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, members: []ecv1alpha1.EtcdMember{terminating}}

		res, err := r.dispatch(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		// The interim finalizer-removal workaround (step 7) lets the
		// Terminating member actually disappear, and scale-out must not
		// have run in the same call to replace it.
		list := &ecv1alpha1.EtcdMemberList{}
		require.NoError(t, fakeClient.List(ctx, list, client.InNamespace(ec.Namespace)))
		assert.Empty(t, list.Items)
	})

	t.Run("Not-ready member stops the loop before scale-out is attempted", func(t *testing.T) {
		ctx := t.Context()
		ec := baseCluster()
		pending := ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: "etcd-0", Namespace: ec.Namespace},
			Spec:       ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: 0, Version: ec.Spec.Version},
			Status:     ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberPending},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, &pending).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, members: []ecv1alpha1.EtcdMember{pending}}

		res, err := r.dispatch(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		list := &ecv1alpha1.EtcdMemberList{}
		require.NoError(t, fakeClient.List(ctx, list, client.InNamespace(ec.Namespace)))
		assert.Len(t, list.Items, 1)
	})

	t.Run("Falls through to scale-out when everything is Ready", func(t *testing.T) {
		ctx := t.Context()
		ec := baseCluster()
		ready := ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd-0",
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: 0, Version: ec.Spec.Version},
			Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&ecv1alpha1.EtcdMember{}).
			WithObjects(ec, &ready).
			Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, members: []ecv1alpha1.EtcdMember{ready}}

		res, err := r.dispatch(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		// Scale-out should have created ordinal 1's EtcdMember shell.
		member := &ecv1alpha1.EtcdMember{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "etcd-1", Namespace: ec.Namespace}, member))
		assert.Equal(t, 1, member.Spec.Ordinal)
	})
}

// TestEnsureClusterFinalizer verifies clusterCleanupFinalizer gets added
// exactly once.
func TestEnsureClusterFinalizer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = ecv1alpha1.AddToScheme(scheme)

	t.Run("Adds the finalizer when missing", func(t *testing.T) {
		ctx := t.Context()
		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default"},
			Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		require.NoError(t, r.ensureClusterFinalizer(ctx, state))

		got := &ecv1alpha1.EtcdCluster{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "etcd", Namespace: "default"}, got))
		assert.Equal(t, []string{clusterCleanupFinalizer}, got.Finalizers)
	})

	t.Run("No-op when already present", func(t *testing.T) {
		ctx := t.Context()
		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", Finalizers: []string{clusterCleanupFinalizer}},
			Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		require.NoError(t, r.ensureClusterFinalizer(ctx, state))
		assert.Equal(t, []string{clusterCleanupFinalizer}, ec.Finalizers)
	})
}

// TestFinalizeCluster verifies the EtcdCluster deletion path: owned
// EtcdMembers are removed one dispatch-step-7-style pass at a time, and only
// once none remain does the cluster's own finalizer get released.
func TestFinalizeCluster(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = ecv1alpha1.AddToScheme(scheme)

	terminatingCluster := func() *ecv1alpha1.EtcdCluster {
		now := metav1.Now()
		return &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "etcd",
				Namespace:         "default",
				DeletionTimestamp: &now,
				Finalizers:        []string{clusterCleanupFinalizer},
			},
			Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
		}
	}

	t.Run("Deletes a live member and requeues", func(t *testing.T) {
		ctx := t.Context()
		ec := terminatingCluster()
		member := ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: "etcd-0", Namespace: ec.Namespace, Finalizers: []string{memberCleanupFinalizer}},
			Spec:       ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: 0, Version: ec.Spec.Version},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, &member).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, members: []ecv1alpha1.EtcdMember{member}}

		res, err := r.finalizeCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		got := &ecv1alpha1.EtcdMember{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "etcd-0", Namespace: ec.Namespace}, got))
		assert.NotNil(t, got.DeletionTimestamp, "Delete should have been called on the still-live member")

		// The cluster itself must still carry its finalizer: it's not done
		// until the member is actually gone too.
		gotCluster := &ecv1alpha1.EtcdCluster{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "etcd", Namespace: "default"}, gotCluster))
		assert.Equal(t, []string{clusterCleanupFinalizer}, gotCluster.Finalizers)
	})

	t.Run("Clears an already-Terminating member's finalizer and requeues", func(t *testing.T) {
		ctx := t.Context()
		ec := terminatingCluster()
		now := metav1.Now()
		member := ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "etcd-0",
				Namespace:         ec.Namespace,
				DeletionTimestamp: &now,
				Finalizers:        []string{memberCleanupFinalizer},
			},
			Spec: ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: 0, Version: ec.Spec.Version},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, &member).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, members: []ecv1alpha1.EtcdMember{member}}

		res, err := r.finalizeCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		list := &ecv1alpha1.EtcdMemberList{}
		require.NoError(t, fakeClient.List(ctx, list, client.InNamespace(ec.Namespace)))
		assert.Empty(t, list.Items, "clearing the last finalizer should let the fake client remove it")
	})

	t.Run("Releases the cluster's own finalizer once no members remain", func(t *testing.T) {
		ctx := t.Context()
		ec := terminatingCluster()

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		res, err := r.finalizeCluster(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)

		got := &ecv1alpha1.EtcdCluster{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: "etcd", Namespace: "default"}, got)
		if err == nil {
			assert.Empty(t, got.Finalizers)
		} else {
			assert.True(t, apierrors.IsNotFound(err), "clearing the last finalizer should let the fake client remove it")
		}
	})
}
