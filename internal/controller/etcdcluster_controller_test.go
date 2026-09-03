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
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestFetchAndValidateState verifies the fetchAndValidateState helper across
// a range of conditions (missing cluster, no pods, pods owned via their
// EtcdMember by this cluster, and pods owned via a member of a different
// cluster).
func TestFetchAndValidateState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	// helper to build a minimal EtcdMember together with its Pod (controlled
	// by that member), running a specific etcd image tag. The member's
	// controller reference points at an EtcdCluster with the given UID.
	memberAndPod := func(clusterName, namespace, clusterUID, imageTag string) (*ecv1alpha1.EtcdMember, *corev1.Pod) {
		member := &ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName + "-0",
				Namespace: namespace,
				Labels:    clusterNameLabels(clusterName),
				UID:       types.UID("member-" + clusterName + "-0"),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: new(true),
				}},
			},
			Spec: ecv1alpha1.EtcdMemberSpec{ClusterName: clusterName, Ordinal: 0},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName + "-0",
				Namespace: namespace,
				Labels: map[string]string{
					"app":        clusterName,
					"controller": clusterName,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdMember",
					Name:       member.Name,
					UID:        member.UID,
					Controller: new(true),
				}},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "etcd", Image: "gcr.io/etcd-development/etcd:" + imageTag},
				},
			},
		}
		return member, pod
	}
	ownedMember, ownedPod := memberAndPod("etcd", "default", "2", "3.5.17")
	foreignMember, foreignPod := memberAndPod("etcd", "default", "other-uid", "3.5.17")

	cases := []struct {
		name    string
		req     ctrl.Request
		ec      *ecv1alpha1.EtcdCluster
		members []*ecv1alpha1.EtcdMember
		pods    []*corev1.Pod
		assert  func(t *testing.T, state *reconcileState, res ctrl.Result, err error)
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
			members: []*ecv1alpha1.EtcdMember{ownedMember},
			pods:    []*corev1.Pod{ownedPod},
			req:     ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
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
			// The Pod's controlling member belongs to a different cluster
			// (UID mismatch) → filtered out by listOwnedPods.
			members: []*ecv1alpha1.EtcdMember{foreignMember},
			pods:    []*corev1.Pod{foreignPod},
			req:     ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
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
			for _, member := range tc.members {
				objs = append(objs, member)
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

// TestScaleCluster verifies that scaling only changes the desired set of
// EtcdMember objects. Provisioning is owned by reconcileEtcdMember and must
// never run from this policy-level helper.
func TestScaleCluster(t *testing.T) {
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

	t.Run("`scaleCluster` creates exactly one Pending member without a Pod", func(t *testing.T) {
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
		assert.Equal(t, ec.Name, member.Labels[clusterNameLabel])

		members := &ecv1alpha1.EtcdMemberList{}
		require.NoError(t, fakeClient.List(ctx, members, client.InNamespace(ec.Namespace)))
		assert.Len(t, members.Items, 1)

		pod := &corev1.Pod{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: "etcd-0", Namespace: ec.Namespace}, pod)
		assert.True(t, apierrors.IsNotFound(err), "scaleCluster must not provision the bootstrap Pod")
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
					Controller: new(true),
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

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&ecv1alpha1.EtcdMember{}).
			WithObjects(ec, &pending).
			Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, members: []ecv1alpha1.EtcdMember{pending}}

		res, err := r.dispatch(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		list := &ecv1alpha1.EtcdMemberList{}
		require.NoError(t, fakeClient.List(ctx, list, client.InNamespace(ec.Namespace)))
		require.Len(t, list.Items, 1)
		assert.Equal(t, ecv1alpha1.EtcdMemberProvisioning, list.Items[0].Status.Phase)
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
					Controller: new(true),
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

func TestGapAwareScaleOutCreatesLowestMissingOrdinal(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))

	clusterUID := types.UID("cluster-uid")
	cluster := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: clusterUID},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
	}
	readyMember := func(ordinal int) ecv1alpha1.EtcdMember {
		return ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      etcdMemberName(cluster.Name, ordinal),
				Namespace: cluster.Namespace,
			},
			Spec: ecv1alpha1.EtcdMemberSpec{
				ClusterName: cluster.Name,
				Ordinal:     ordinal,
				Version:     cluster.Spec.Version,
			},
			Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady},
		}
	}
	member0 := readyMember(0)
	member2 := readyMember(2)
	pod0 := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: member0.Name, Namespace: cluster.Namespace}}
	pod2 := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: member2.Name, Namespace: cluster.Namespace}}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ecv1alpha1.EtcdMember{}).
		WithObjects(cluster, &member0, &member2, pod0, pod2).
		Build()
	reconciler := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
	state := &reconcileState{
		cluster: cluster,
		members: []ecv1alpha1.EtcdMember{member0, member2},
		pods:    []*corev1.Pod{pod0, pod2},
	}

	result, err := reconciler.scaleCluster(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, result)
	target := &ecv1alpha1.EtcdMember{}
	require.NoError(t, fakeClient.Get(t.Context(), client.ObjectKey{
		Name: "etcd-1", Namespace: cluster.Namespace,
	}, target))
	require.Len(t, target.OwnerReferences, 1)
	require.Equal(t, clusterUID, target.OwnerReferences[0].UID)
	assert.Equal(t, 1, target.Spec.Ordinal)

	// First lifecycle pass durably enters Provisioning.
	state.members = []ecv1alpha1.EtcdMember{member0, *target, member2}
	result, err = reconciler.reconcileEtcdMember(t.Context(), state, target)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, result)
	assert.Equal(t, ecv1alpha1.EtcdMemberProvisioning, target.Status.Phase)
}

// TestReconcilePaused verifies requirement 15's pause gate: Spec.Paused
// skips dispatch (so no scale-out/EtcdMember mutation happens) but the
// cluster-prereqs and always-on-refresh phases ahead of it in Reconcile
// still run, since dispatch is no longer where Paused is checked (moved out
// per reconcile_loop_v0.3.0.png's dedicated "Pause Reconciliation" box).
func TestReconcilePaused(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "1"},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17", Paused: true},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ecv1alpha1.EtcdCluster{}).
		WithObjects(ec).
		Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	res, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: ec.Name, Namespace: ec.Namespace}})
	assert.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

	// Dispatch (scale-out included) must not have run: no EtcdMember got
	// created towards Spec.Size, even though there are zero today.
	members := &ecv1alpha1.EtcdMemberList{}
	require.NoError(t, fakeClient.List(t.Context(), members, client.InNamespace(ec.Namespace)))
	assert.Empty(t, members.Items)

	// Cluster prerequisites (ahead of the Pause check) must still have run.
	svc := &corev1.Service{}
	assert.NoError(t, fakeClient.Get(t.Context(), client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc))
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

// TestUpgradeCluster verifies that upgradeCluster picks the correct member,
// updates its Spec.Version, and sets Phase=Recreating on it — and that it is
// a no-op when all members are already at the desired version.
func TestUpgradeCluster(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	const targetVersion = "v3.5.33"
	const oldVersion = "v3.5.32"

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "1"},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:          3,
			Version:       targetVersion,
			ImageRegistry: "gcr.io/etcd-development/etcd",
		},
	}

	// makeMember builds an EtcdMember owned by ec with the given ordinal and version.
	makeMember := func(ordinal int, version string) *ecv1alpha1.EtcdMember {
		return &ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      etcdMemberName(ec.Name, ordinal),
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: new(true),
				}},
			},
			Spec:   ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: ordinal, Version: version},
			Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady},
		}
	}

	// makeHealth builds a ClusterHealth snapshot. entries maps pod name → [memberID, leaderID].
	makeHealth := func(entries map[string][2]uint64) *etcdutils.ClusterHealth {
		members := make(map[string]etcdutils.EpHealth, len(entries))
		for name, ids := range entries {
			members[name] = etcdutils.EpHealth{
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{MemberId: ids[0]},
					Leader: ids[1],
				},
			}
		}
		return &etcdutils.ClusterHealth{Healthy: true, Members: members}
	}

	healthInfoWithLeader := func(leaderPod string) *etcdutils.ClusterHealth {
		ids := map[string]uint64{"etcd-0": 10, "etcd-1": 20, "etcd-2": 30}
		leaderID := ids[leaderPod]
		entries := make(map[string][2]uint64, len(ids))
		for name, memberID := range ids {
			entries[name] = [2]uint64{memberID, leaderID}
		}
		return makeHealth(entries)
	}

	tests := []struct {
		name           string
		memberVersions [3]string // versions for ordinals 0, 1, 2
		health         *etcdutils.ClusterHealth
		wantResult     ctrl.Result
		wantRecreating string   // pod name expected to be Phase=Recreating; "" means no-op
		wantUntouched  []string // pod names that must remain Ready at their original version
	}{
		{
			name:           "all members already at target version — no-op",
			memberVersions: [3]string{targetVersion, targetVersion, targetVersion},
			health:         nil,
			wantResult:     ctrl.Result{},
			wantRecreating: "",
		},
		{
			name:           "highest-ordinal non-leader picked when no health info",
			memberVersions: [3]string{oldVersion, oldVersion, oldVersion},
			health:         nil,
			wantResult:     ctrl.Result{RequeueAfter: requeueDuration},
			wantRecreating: "etcd-2",
			wantUntouched:  []string{"etcd-0", "etcd-1"},
		},
		{
			name:           "leader deferred — highest non-leader picked instead",
			memberVersions: [3]string{oldVersion, oldVersion, oldVersion},
			health:         healthInfoWithLeader("etcd-2"),
			wantResult:     ctrl.Result{RequeueAfter: requeueDuration},
			wantRecreating: "etcd-1",
			wantUntouched:  []string{"etcd-2"},
		},
		{
			name:           "only the leader needs upgrading — leader picked as last resort",
			memberVersions: [3]string{targetVersion, targetVersion, oldVersion},
			health:         healthInfoWithLeader("etcd-2"),
			wantResult:     ctrl.Result{RequeueAfter: requeueDuration},
			wantRecreating: "etcd-2",
			wantUntouched:  []string{"etcd-0", "etcd-1"},
		},
		{
			name:           "mid-ordinal leader skipped, already-upgraded member skipped — lower ordinal member picked",
			memberVersions: [3]string{oldVersion, oldVersion, targetVersion},
			health:         healthInfoWithLeader("etcd-1"),
			wantResult:     ctrl.Result{RequeueAfter: requeueDuration},
			wantRecreating: "etcd-0",
			wantUntouched:  []string{"etcd-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			members := [3]*ecv1alpha1.EtcdMember{
				makeMember(0, tt.memberVersions[0]),
				makeMember(1, tt.memberVersions[1]),
				makeMember(2, tt.memberVersions[2]),
			}

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&ecv1alpha1.EtcdMember{}).
				WithObjects(ec, members[0], members[1], members[2]).Build()
			r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
			state := &reconcileState{
				cluster: ec,
				members: []ecv1alpha1.EtcdMember{*members[0], *members[1], *members[2]},
				health:  tt.health,
			}

			res, err := r.upgradeCluster(ctx, state)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantResult, res)

			if tt.wantRecreating == "" {
				// No-op: all members must remain Ready and unmodified.
				for _, m := range members {
					got := &ecv1alpha1.EtcdMember{}
					require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: m.Name, Namespace: m.Namespace}, got))
					assert.Equal(t, ecv1alpha1.EtcdMemberReady, got.Status.Phase)
				}
				return
			}

			// The target member must have Spec.Version updated and Phase=Recreating.
			got := &ecv1alpha1.EtcdMember{}
			require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: tt.wantRecreating, Namespace: ec.Namespace}, got))
			assert.Equal(t, targetVersion, got.Spec.Version, "Spec.Version must be updated to target")
			assert.Equal(t, ecv1alpha1.EtcdMemberRecreating, got.Status.Phase, "Phase must be Recreating")

			// Untouched members must remain Ready at their original version.
			for _, name := range tt.wantUntouched {
				var originalVersion string
				for _, m := range members {
					if m.Name == name {
						originalVersion = m.Spec.Version
						break
					}
				}
				got := &ecv1alpha1.EtcdMember{}
				require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ec.Namespace}, got))
				assert.Equal(t, originalVersion, got.Spec.Version)
				assert.Equal(t, ecv1alpha1.EtcdMemberReady, got.Status.Phase)
			}
		})
	}
}
