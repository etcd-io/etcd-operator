package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestDBSizeCELValidation drives the apply-time CEL rules on quotaBackendBytes
// and the autoCompaction fields against the real envtest apiserver. A negative
// quota disables the quota in etcd, a byte-scale typo alarms the cluster
// read-only, and a duration retention in revision mode is misread by etcd as a
// nanosecond revision count — all must be rejected at admission.
func TestDBSizeCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	qty := func(s string) *resource.Quantity {
		q := resource.MustParse(s)
		return &q
	}

	tests := []struct {
		name      string
		mutate    func(*ecv1alpha1.EtcdClusterSpec)
		wantApply bool
	}{
		{
			name:      "quota 8Gi accepted",
			mutate:    func(s *ecv1alpha1.EtcdClusterSpec) { s.QuotaBackendBytes = qty("8Gi") },
			wantApply: true,
		},
		{
			name:      "quota at the 100Mi floor accepted",
			mutate:    func(s *ecv1alpha1.EtcdClusterSpec) { s.QuotaBackendBytes = qty("100Mi") },
			wantApply: true,
		},
		{
			name:      "quota as plain byte integer accepted when large enough",
			mutate:    func(s *ecv1alpha1.EtcdClusterSpec) { s.QuotaBackendBytes = qty("2147483648") },
			wantApply: true,
		},
		{
			name:      "negative quota rejected (etcd would disable the quota)",
			mutate:    func(s *ecv1alpha1.EtcdClusterSpec) { s.QuotaBackendBytes = qty("-1") },
			wantApply: false,
		},
		{
			name:      "byte-scale typo rejected (\"8\" for \"8Gi\")",
			mutate:    func(s *ecv1alpha1.EtcdClusterSpec) { s.QuotaBackendBytes = qty("8") },
			wantApply: false,
		},
		{
			name:      "milli quantity rejected",
			mutate:    func(s *ecv1alpha1.EtcdClusterSpec) { s.QuotaBackendBytes = qty("500m") },
			wantApply: false,
		},
		{
			name: "periodic mode with duration retention accepted",
			mutate: func(s *ecv1alpha1.EtcdClusterSpec) {
				s.AutoCompactionMode, s.AutoCompactionRetention = "periodic", "5m"
			},
			wantApply: true,
		},
		{
			name: "revision mode with integer retention accepted",
			mutate: func(s *ecv1alpha1.EtcdClusterSpec) {
				s.AutoCompactionMode, s.AutoCompactionRetention = "revision", "1000"
			},
			wantApply: true,
		},
		{
			name: "revision mode with duration retention rejected (etcd reads it as nanoseconds of revisions)",
			mutate: func(s *ecv1alpha1.EtcdClusterSpec) {
				s.AutoCompactionMode, s.AutoCompactionRetention = "revision", "5m"
			},
			wantApply: false,
		},
		{
			name: "revision mode without retention rejected",
			mutate: func(s *ecv1alpha1.EtcdClusterSpec) {
				s.AutoCompactionMode = "revision"
			},
			wantApply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "dbsize-cel-test-",
					Namespace:    "default",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "v3.6.12"},
			}
			tt.mutate(&ec.Spec)
			err := k8sClient.Create(t.Context(), ec)
			if tt.wantApply {
				require.NoError(t, err, "apiserver should accept a valid spec")
				_ = k8sClient.Delete(t.Context(), ec, &client.DeleteOptions{})
			} else {
				assert.Error(t, err, "apiserver should reject the spec via CEL")
			}
		})
	}
}
