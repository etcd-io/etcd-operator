package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestResourceRequirementsAdmission(t *testing.T) {
	cluster := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "operator.etcd.io/v1alpha1",
		"kind":       "EtcdCluster",
		"metadata":   map[string]interface{}{"name": "resource-validation", "namespace": "default"},
		"spec": map[string]interface{}{
			"size": int64(1), "version": testEtcdVersion,
			"podTemplate": map[string]interface{}{"spec": map[string]interface{}{
				"resources": map[string]interface{}{"requests": map[string]interface{}{"cpu": "100m", "memory": "256Mi"}},
			}},
		},
	}}
	require.NoError(t, k8sClient.Create(t.Context(), cluster))
	t.Cleanup(func() { require.NoError(t, k8sClient.Delete(context.Background(), cluster)) })

	for _, tc := range []struct {
		name      string
		resources map[string]interface{}
	}{
		{"missing requests", map[string]interface{}{}},
		{"empty requests", map[string]interface{}{"requests": map[string]interface{}{}}},
		{"missing memory", map[string]interface{}{"requests": map[string]interface{}{"cpu": "1"}}},
		{"missing cpu", map[string]interface{}{"requests": map[string]interface{}{"memory": "1Gi"}}},
		{"zero cpu", map[string]interface{}{"requests": map[string]interface{}{"cpu": "0", "memory": "1Gi"}}},
		{"negative memory", map[string]interface{}{"requests": map[string]interface{}{"cpu": "1", "memory": "-1Gi"}}},
		{"invalid quantity", map[string]interface{}{"requests": map[string]interface{}{"cpu": "bad", "memory": "1Gi"}}},
		{"unsupported resource", map[string]interface{}{"requests": map[string]interface{}{"cpu": "1", "memory": "1Gi", "example.com/device": "1"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			updated := cluster.DeepCopy()
			require.NoError(t, unstructured.SetNestedMap(updated.Object, tc.resources, "spec", "podTemplate", "spec", "resources"))
			err := k8sClient.Update(t.Context(), updated)
			require.True(t, apierrors.IsInvalid(err), "expected admission rejection, got %v", err)
			stored := cluster.DeepCopy()
			require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), stored))
			require.Equal(t, cluster.GetGeneration(), stored.GetGeneration(), "invalid update must not trigger reconciliation")
		})
	}

	for _, field := range []string{"limits", "claims"} {
		t.Run("reject unsupported "+field, func(t *testing.T) {
			updated := cluster.DeepCopy()
			var value interface{} = map[string]interface{}{"cpu": "1"}
			if field == "claims" {
				value = []interface{}{map[string]interface{}{"name": "device"}}
			}
			require.NoError(t, unstructured.SetNestedField(updated.Object, value, "spec", "podTemplate", "spec", "resources", field))
			err := k8sClient.Update(t.Context(), updated, &client.UpdateOptions{FieldValidation: metav1.FieldValidationStrict})
			require.Error(t, err)
			require.Contains(t, err.Error(), "unknown field")
		})
	}
}
