package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	cm := func(issuerName string) ecv1alpha1.ProviderConfig {
		return ecv1alpha1.ProviderConfig{
			CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
				IssuerKind: "ClusterIssuer",
				IssuerName: issuerName,
			},
		}
	}

	tests := []struct {
		name      string
		surface   ecv1alpha1.TLSSurface
		wantApply bool // true => apiserver should accept
	}{
		{
			name:      "valid cert-manager mTLS surface accepted",
			surface:   ecv1alpha1.TLSSurface{Provider: "cert-manager", ProviderCfg: cm("etcd-ca-issuer")},
			wantApply: true,
		},
		{
			name:      "auto provider surface accepted",
			surface:   ecv1alpha1.TLSSurface{Provider: "auto"},
			wantApply: true,
		},
		{
			name:      "cert-manager provider without certManagerCfg rejected",
			surface:   ecv1alpha1.TLSSurface{Provider: "cert-manager"},
			wantApply: false,
		},
		{
			name:      "certManagerCfg under auto provider rejected",
			surface:   ecv1alpha1.TLSSurface{Provider: "auto", ProviderCfg: cm("etcd-ca-issuer")},
			wantApply: false,
		},
		{
			name: "clientCertAuth true with cert-manager but empty issuerName rejected",
			surface: ecv1alpha1.TLSSurface{
				Provider:       "cert-manager",
				ClientCertAuth: boolPtr(true),
				ProviderCfg:    cm(""),
			},
			wantApply: false,
		},
		{
			name: "server-only TLS (clientCertAuth false) without issuerName accepted",
			surface: ecv1alpha1.TLSSurface{
				Provider:       "cert-manager",
				ClientCertAuth: boolPtr(false),
				ProviderCfg:    cm("etcd-ca-issuer"),
			},
			wantApply: true,
		},
		{
			name: "bad issuerKind rejected by enum",
			surface: ecv1alpha1.TLSSurface{
				Provider: "cert-manager",
				ProviderCfg: ecv1alpha1.ProviderConfig{
					CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
						IssuerKind: "Bogus",
						IssuerName: "etcd-ca-issuer",
					},
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

func tlsForSurface(onClient bool, s *ecv1alpha1.TLSSurface) *ecv1alpha1.EtcdClusterTLS {
	if onClient {
		return &ecv1alpha1.EtcdClusterTLS{Client: s}
	}
	return &ecv1alpha1.EtcdClusterTLS{Peer: s}
}
