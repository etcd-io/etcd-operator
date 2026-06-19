/*
Copyright 2025.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// podMonitorTestScheme returns a scheme that knows the EtcdCluster types and the
// unstructured PodMonitor GVK so the fake client can store and retrieve the
// PodMonitor object reconcilePodMonitor creates.
func podMonitorTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = ecv1alpha1.AddToScheme(s)
	s.AddKnownTypeWithName(podMonitorGVK, &unstructured.Unstructured{})
	listGVK := podMonitorGVK
	listGVK.Kind += "List"
	s.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return s
}

// getPodMonitor fetches the cluster's PodMonitor via the unstructured GVK.
func getPodMonitor(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster) (*unstructured.Unstructured, error) {
	u := newPodMonitorObject(ec)
	err := c.Get(ctx, types.NamespacedName{Namespace: ec.Namespace, Name: ec.Name}, u)
	return u, err
}

func newMetricsCluster(podMonitorEnabled bool, port, interval string) *ecv1alpha1.EtcdCluster {
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd-x", Namespace: "ns1"},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3},
	}
	enabled := true
	ec.Spec.Metrics = &ecv1alpha1.MetricsSpec{
		Enabled: &enabled,
		PodMonitor: &ecv1alpha1.PodMonitorSpec{
			Enabled:  podMonitorEnabled,
			Port:     port,
			Interval: interval,
		},
	}
	return ec
}

// TestReconcilePodMonitor_CreatesWhenEnabled asserts the PodMonitor is created
// with the expected pod selector, port, interval, and controller owner ref.
func TestReconcilePodMonitor_CreatesWhenEnabled(t *testing.T) {
	scheme := podMonitorTestScheme()
	ec := newMetricsCluster(true, "client", "45s")
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	r.reconcilePodMonitor(context.Background(), ec)

	pm, err := getPodMonitor(context.Background(), fakeClient, ec)
	require.NoError(t, err, "expected PodMonitor to be created")

	sel, found, err := unstructured.NestedStringMap(pm.Object, "spec", "selector", "matchLabels")
	require.NoError(t, err)
	require.True(t, found, "selector.matchLabels missing")
	assert.Equal(t, ec.Name, sel["app"])
	assert.Equal(t, ec.Name, sel["controller"])

	endpoints, found, err := unstructured.NestedSlice(pm.Object, "spec", "podMetricsEndpoints")
	require.NoError(t, err)
	require.True(t, found, "podMetricsEndpoints missing")
	require.Len(t, endpoints, 1)
	endpoint := endpoints[0].(map[string]any)
	assert.Equal(t, "client", endpoint["port"])
	assert.Equal(t, "/metrics", endpoint["path"])
	assert.Equal(t, "45s", endpoint["interval"])

	owners := pm.GetOwnerReferences()
	require.Len(t, owners, 1)
	assert.Equal(t, ec.Name, owners[0].Name)
	require.NotNil(t, owners[0].Controller)
	assert.True(t, *owners[0].Controller)
}

// TestReconcilePodMonitor_DefaultsPortWhenUnset asserts an unset port falls back
// to the default scrape port.
func TestReconcilePodMonitor_DefaultsPortWhenUnset(t *testing.T) {
	scheme := podMonitorTestScheme()
	ec := newMetricsCluster(true, "", "")
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	r.reconcilePodMonitor(context.Background(), ec)

	pm, err := getPodMonitor(context.Background(), fakeClient, ec)
	require.NoError(t, err)
	endpoints, _, err := unstructured.NestedSlice(pm.Object, "spec", "podMetricsEndpoints")
	require.NoError(t, err)
	require.Len(t, endpoints, 1)
	endpoint := endpoints[0].(map[string]any)
	assert.Equal(t, defaultPodMonitorPort, endpoint["port"])
	// interval omitted when unset.
	_, hasInterval := endpoint["interval"]
	assert.False(t, hasInterval, "interval should be omitted when unset")
}

// TestReconcilePodMonitor_CRDAbsentIsSoftSkip asserts that a missing PodMonitor
// CRD (NoMatchError on the underlying Get) is treated as a soft skip: the
// reconcile does not panic and creates nothing.
func TestReconcilePodMonitor_CRDAbsentIsSoftSkip(t *testing.T) {
	scheme := podMonitorTestScheme()
	ec := newMetricsCluster(true, "client", "30s")

	noMatch := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: podMonitorGVK.Group, Kind: podMonitorGVK.Kind}}
	getCalled := false
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if obj.GetObjectKind().GroupVersionKind() == podMonitorGVK {
					getCalled = true
					return noMatch
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	// Must not panic and must not surface the error (reconcilePodMonitor returns void).
	assert.NotPanics(t, func() {
		r.reconcilePodMonitor(context.Background(), ec)
	})
	assert.True(t, getCalled, "expected a PodMonitor Get attempt")
}

// TestReconcilePodMonitor_DeletesWhenDisabled asserts a previously created
// PodMonitor is removed once the feature is disabled.
func TestReconcilePodMonitor_DeletesWhenDisabled(t *testing.T) {
	scheme := podMonitorTestScheme()
	ec := newMetricsCluster(true, "client", "30s")
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	// Create it first.
	r.reconcilePodMonitor(context.Background(), ec)
	if _, err := getPodMonitor(context.Background(), fakeClient, ec); err != nil {
		t.Fatalf("precondition: PodMonitor not created: %v", err)
	}

	// Now disable and reconcile again.
	ec.Spec.Metrics.PodMonitor.Enabled = false
	r.reconcilePodMonitor(context.Background(), ec)

	_, err := getPodMonitor(context.Background(), fakeClient, ec)
	assert.True(t, apierrors.IsNotFound(err), "expected PodMonitor to be deleted, got err=%v", err)
}

// TestReconcilePodMonitor_DeleteNotFoundSwallowed asserts that disabling the
// feature when no PodMonitor exists is a no-op (the NotFound on delete is
// swallowed and nothing panics).
func TestReconcilePodMonitor_DeleteNotFoundSwallowed(t *testing.T) {
	scheme := podMonitorTestScheme()
	ec := newMetricsCluster(false, "client", "30s")
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	assert.NotPanics(t, func() {
		r.reconcilePodMonitor(context.Background(), ec)
	})
	_, err := getPodMonitor(context.Background(), fakeClient, ec)
	assert.True(t, apierrors.IsNotFound(err), "expected no PodMonitor to exist")
}
