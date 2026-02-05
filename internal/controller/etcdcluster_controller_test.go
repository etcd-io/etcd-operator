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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestFetchAndValidateState describes the scenarios for the fetchAndValidateState
// helper. Each sub-test will set up a fake client with different existing
// resources and assert on the returned state, result and error.
func TestFetchAndValidateState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	cases := []struct {
		name   string
		req    ctrl.Request
		ec     *ecv1alpha1.EtcdCluster
		sts    *appsv1.StatefulSet
		assert func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet)
	}{
		{
			name: "EtcdCluster Not Found",
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, _ *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				assert.Nil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "StatefulSet Not Found",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "1",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.Equal(t, ec.Name, state.cluster.Name)
				assert.Nil(t, state.sts)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Resources Exist and Owned",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.Equal(t, ec.Name, state.cluster.Name)
				require.NotNil(t, state.sts)
				assert.Equal(t, sts.Name, state.sts.Name)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "StatefulSet Not Owned",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "3",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, _ *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				assert.Nil(t, state)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "not controlled")
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Valid upgrade path",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.6.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:3.5.17"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Cannot parse StatefulSet image tag",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.6.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd#notag"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Invalid upgrade path",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.7.1"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:3.5.17"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.Nil(t, state)
				assert.Error(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Downgrades are unsupported",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.1"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:3.6.10"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.Nil(t, state)
				assert.Error(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Upgrade with non-semver versions",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "foo"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:bar"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				require.NotNil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Equal tags are a no-op even if they are not semver",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "bar"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Image: "gcr.io/etcd-development/etcd:bar"},
							},
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
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
			if tc.sts != nil {
				objs = append(objs, tc.sts)
			}

			builder := fake.NewClientBuilder().WithScheme(scheme)
			if len(objs) > 0 {
				builder.WithObjects(objs...)
			}
			fakeClient := builder.Build()

			r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

			state, res, err := r.fetchAndValidateState(ctx, tc.req)
			tc.assert(t, state, res, err, tc.ec, tc.sts)
		})
	}
}

// TestBootstrapStatefulSet outlines tests for ensuring StatefulSet and Service
// creation and bootstrap logic.
func TestBootstrapStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
			UID:       "1",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    1,
			Version: "3.5.17",
		},
	}

	t.Run("Initial Creation", func(t *testing.T) {
		ctx := t.Context()

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		res, err := r.bootstrapStatefulSet(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		require.NotNil(t, state.sts)
		assert.NotNil(t, state.sts.Spec.Replicas)
		assert.Equal(t, int32(0), *state.sts.Spec.Replicas)

		sts := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, sts)
		assert.NoError(t, err)
		assert.NotNil(t, sts.Spec.Replicas)
		assert.Equal(t, int32(0), *sts.Spec.Replicas)

		svc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc)
		assert.NoError(t, err)
		assert.Equal(t, "None", svc.Spec.ClusterIP)

		cm := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, cm)
		assert.NoError(t, err)
	})

	t.Run("Bootstrap from Zero", func(t *testing.T) {
		ctx := t.Context()

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: pointerToInt32(0),
			},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		}

		cm := newEtcdClusterState(ec, 0)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts, cm).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		oldRV := sts.ResourceVersion
		res, err := r.bootstrapStatefulSet(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		updatedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, updatedSTS)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), *updatedSTS.Spec.Replicas)
		assert.Equal(t, int32(1), updatedSTS.Status.ReadyReplicas)
		assert.NotEqual(t, oldRV, updatedSTS.ResourceVersion)

		require.NotNil(t, state.sts)
		assert.Equal(t, int32(1), *state.sts.Spec.Replicas)

		svc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc)
		assert.NoError(t, err)
		assert.Equal(t, "None", svc.Spec.ClusterIP)

		cmUpdated := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, cmUpdated)
		assert.NoError(t, err)
		assert.Equal(t, "new", cmUpdated.Data["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, cmUpdated.Data["ETCD_INITIAL_CLUSTER"], "etcd-0=")
	})

	t.Run("Resources Already Exist", func(t *testing.T) {
		ctx := t.Context()

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(1)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		}

		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
			},
			Spec: corev1.ServiceSpec{ClusterIP: "None"},
		}

		cm := newEtcdClusterState(ec, 1)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts.DeepCopy(), svc.DeepCopy(), cm.DeepCopy()).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		// Capture current objects to verify no updates occur.
		storedSTS := sts.DeepCopy()
		storedSvc := svc.DeepCopy()
		storedCM := cm.DeepCopy()
		res, err := r.bootstrapStatefulSet(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)

		fetchedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, fetchedSTS)
		assert.NoError(t, err)
		assert.Equal(t, storedSTS.Spec, fetchedSTS.Spec)

		fetchedSvc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, fetchedSvc)
		assert.NoError(t, err)
		assert.Equal(t, storedSvc.Spec, fetchedSvc.Spec)

		fetchedCM := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, fetchedCM)
		assert.NoError(t, err)
		assert.Equal(t, storedCM.Data, fetchedCM.Data)
	})
}
