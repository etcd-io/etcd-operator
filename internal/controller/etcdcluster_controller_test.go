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
// pods owned by a different cluster, and version-upgrade validation).
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
		{
			name: "Valid upgrade path",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "2"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.6.17"},
			},
			pods: []*corev1.Pod{ownedPod("etcd", "default", "2", "3.5.17")},
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Cannot parse pod image tag",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "2"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.6.17"},
			},
			pods: []*corev1.Pod{ownedPod("etcd", "default", "2", "")},
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				// Image has no ":" so version can't be extracted; state is returned but no error.
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Invalid upgrade path",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "2"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.7.1"},
			},
			pods: []*corev1.Pod{ownedPod("etcd", "default", "2", "3.5.17")},
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				assert.Nil(t, state)
				assert.Error(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Downgrades are unsupported",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "2"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.1"},
			},
			pods: []*corev1.Pod{ownedPod("etcd", "default", "2", "3.6.10")},
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				assert.Nil(t, state)
				assert.Error(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Upgrade with non-semver versions",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "2"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "foo"},
			},
			pods: []*corev1.Pod{ownedPod("etcd", "default", "2", "bar")},
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Equal tags are a no-op even if they are not semver",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "2"},
				Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "bar"},
			},
			pods: []*corev1.Pod{ownedPod("etcd", "default", "2", "bar")},
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error) {
				require.NotNil(t, state)
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
