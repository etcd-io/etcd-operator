package controller

import (
	"testing"
	"time"

	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestTLSCELValidation drives the apply-time CEL XValidation rules on TLSSurface
// against the real envtest apiserver (k8sClient applies the generated CRD). These
// are the rules that "cannot be applied wrong" -- the apiserver rejects them before
// the controller ever sees the object. The reconcile-time backstop is covered
// separately in TestValidateTLS.
func TestTLSCELValidation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	cm := func(issuerName string) *ecv1alpha1.TLSCertManagerProvider {
		return &ecv1alpha1.TLSCertManagerProvider{
			IssuerRef: cmmeta.IssuerReference{
				Kind: "ClusterIssuer",
				Name: issuerName,
			},
		}
	}
	dur := func(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

	tests := []struct {
		name      string
		surface   ecv1alpha1.TLSSurface
		wantApply bool // true => apiserver should accept
	}{
		{
			name:      "valid cert-manager mTLS surface accepted",
			surface:   ecv1alpha1.TLSSurface{Provider: ecv1alpha1.TLSProviderCertManager, CertManager: cm("etcd-ca-issuer")},
			wantApply: true,
		},
		{
			name:      "auto provider surface accepted",
			surface:   ecv1alpha1.TLSSurface{Provider: ecv1alpha1.TLSProviderAuto},
			wantApply: true,
		},
		{
			name:      "cert-manager provider without the certManager block rejected",
			surface:   ecv1alpha1.TLSSurface{Provider: ecv1alpha1.TLSProviderCertManager},
			wantApply: false,
		},
		{
			name:      "certManager block under auto provider rejected",
			surface:   ecv1alpha1.TLSSurface{Provider: ecv1alpha1.TLSProviderAuto, CertManager: cm("etcd-ca-issuer")},
			wantApply: false,
		},
		{
			name: "auto block under cert-manager provider rejected",
			surface: ecv1alpha1.TLSSurface{
				Provider:    ecv1alpha1.TLSProviderCertManager,
				CertManager: cm("etcd-ca-issuer"),
				Auto:        &ecv1alpha1.TLSAutoProvider{},
			},
			wantApply: false,
		},
		{
			name: "clientCertAuth true with cert-manager but empty issuerRef.name rejected",
			surface: ecv1alpha1.TLSSurface{
				Provider:       ecv1alpha1.TLSProviderCertManager,
				ClientCertAuth: boolPtr(true),
				CertManager:    cm(""),
			},
			wantApply: false,
		},
		{
			name: "server-only TLS (clientCertAuth false) accepted",
			surface: ecv1alpha1.TLSSurface{
				Provider:       ecv1alpha1.TLSProviderCertManager,
				ClientCertAuth: boolPtr(false),
				CertManager:    cm("etcd-ca-issuer"),
			},
			wantApply: true,
		},
		{
			name: "issuerRef.kind omitted accepted (defaults to Issuer)",
			surface: ecv1alpha1.TLSSurface{
				Provider: ecv1alpha1.TLSProviderCertManager,
				CertManager: &ecv1alpha1.TLSCertManagerProvider{
					IssuerRef: cmmeta.IssuerReference{Name: "etcd-ca-issuer"},
				},
			},
			wantApply: true,
		},
		{
			name: "bad issuerRef.kind rejected by CEL",
			surface: ecv1alpha1.TLSSurface{
				Provider: ecv1alpha1.TLSProviderCertManager,
				CertManager: &ecv1alpha1.TLSCertManagerProvider{
					IssuerRef: cmmeta.IssuerReference{Kind: "Bogus", Name: "etcd-ca-issuer"},
				},
			},
			wantApply: false,
		},
		{
			name: "bad provider rejected by enum",
			surface: ecv1alpha1.TLSSurface{
				Provider: "vault",
			},
			wantApply: false,
		},
		{
			name: "cert-manager duration below 1h rejected",
			surface: func() ecv1alpha1.TLSSurface {
				s := ecv1alpha1.TLSSurface{Provider: ecv1alpha1.TLSProviderCertManager, CertManager: cm("etcd-ca-issuer")}
				s.CertManager.Duration = dur(30 * time.Minute)
				return s
			}(),
			wantApply: false,
		},
		{
			name: "cert-manager duration of 90 days accepted",
			surface: func() ecv1alpha1.TLSSurface {
				s := ecv1alpha1.TLSSurface{Provider: ecv1alpha1.TLSProviderCertManager, CertManager: cm("etcd-ca-issuer")}
				s.CertManager.Duration = dur(2160 * time.Hour)
				return s
			}(),
			wantApply: true,
		},
		{
			name: "auto duration below 365 days rejected",
			surface: ecv1alpha1.TLSSurface{
				Provider: ecv1alpha1.TLSProviderAuto,
				Auto:     &ecv1alpha1.TLSAutoProvider{Duration: dur(720 * time.Hour)},
			},
			wantApply: false,
		},
		{
			name: "auto duration of 365 days accepted",
			surface: ecv1alpha1.TLSSurface{
				Provider: ecv1alpha1.TLSProviderAuto,
				Auto:     &ecv1alpha1.TLSAutoProvider{Duration: dur(8760 * time.Hour)},
			},
			wantApply: true,
		},
		{
			name: "zero renewBefore rejected",
			surface: ecv1alpha1.TLSSurface{
				Provider: ecv1alpha1.TLSProviderAuto,
				Auto:     &ecv1alpha1.TLSAutoProvider{RenewBefore: dur(0)},
			},
			wantApply: false,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			surface := tt.surface
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "cel-test-",
					Namespace:    "default",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{
					Size:    3,
					Version: "v3.6.12",
					// Alternate which surface carries the config so both surfaces'
					// XValidation rules are exercised across the table.
					TLS: tlsForSurface(i%2 == 0, &surface),
				},
			}
			err := k8sClient.Create(t.Context(), ec)
			if tt.wantApply {
				require.NoError(t, err, "apiserver should accept a valid surface")
				_ = k8sClient.Delete(t.Context(), ec, &client.DeleteOptions{})
			} else {
				assert.Error(t, err, "apiserver should reject an invalid surface via CEL")
			}
		})
	}
}

// TestTLSDaySuffixDurationRejected asserts the CEL duration() guards reject
// day-suffix strings at ADMISSION. The CRD renders *metav1.Duration as a bare
// string schema, so without these guards "365d" would be admitted and stored,
// then wedge the controller's typed decode (time.ParseDuration has no 'd'
// unit). Raw JSON is used because the Go client cannot even marshal an
// unparseable metav1.Duration.
func TestTLSDaySuffixDurationRejected(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	for _, tc := range []struct {
		name  string
		block string
	}{
		{"auto 365d", `"provider":"auto","auto":{"duration":"365d"}`},
		{"certManager 90d", `"provider":"cert-manager.io","certManager":{"issuerRef":{"name":"x"},"duration":"90d"}`},
		{"renewBefore 30d", `"provider":"auto","auto":{"renewBefore":"30d"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := &unstructured.Unstructured{}
			raw := `{
				"apiVersion": "operator.etcd.io/v1alpha1",
				"kind": "EtcdCluster",
				"metadata": {"generateName": "cel-day-", "namespace": "default"},
				"spec": {"size": 3, "version": "v3.6.12",
					"tls": {"client": {` + tc.block + `}}}
			}`
			require.NoError(t, u.UnmarshalJSON([]byte(raw)))
			err := k8sClient.Create(t.Context(), u)
			assert.Error(t, err, "day-suffix duration must be rejected at admission")
		})
	}
}

func tlsForSurface(onClient bool, s *ecv1alpha1.TLSSurface) *ecv1alpha1.EtcdClusterTLS {
	if onClient {
		return &ecv1alpha1.EtcdClusterTLS{Client: s}
	}
	return &ecv1alpha1.EtcdClusterTLS{Peer: s}
}
