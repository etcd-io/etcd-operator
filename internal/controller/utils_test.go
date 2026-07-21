package controller

import (
	"net"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/certificate"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

func pointerToBool(value bool) *bool {
	return &value
}

// ---------------------------------------------------------------------------
// createHeadlessServiceIfNotExist
// ---------------------------------------------------------------------------

func TestCreateHeadlessServiceIfNotExist(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-etcd", Namespace: "default"},
	}

	t.Run("creates headless service if it does not exist", func(t *testing.T) {
		err := createHeadlessServiceIfNotExist(ctx, logger, fakeClient, ec, scheme)
		assert.NoError(t, err)

		service := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: "test-etcd", Namespace: "default"}, service)
		assert.NoError(t, err)
		assert.Equal(t, "None", service.Spec.ClusterIP)
		assert.Equal(t, map[string]string{
			"app":        "test-etcd",
			"controller": "test-etcd",
		}, service.Spec.Selector)
		// PublishNotReadyAddresses must be true so that during a cluster scale-out
		// (e.g. 1->3 nodes), CoreDNS returns the NotReady new member's Pod IP in
		// the headless Service's A record set. etcd v3.6's peer-port checkCertSAN
		// does a forward-DNS lookup of the cert's DNSName against the connecting
		// pod IP; without this flag, the new member's IP is missing from the
		// lookup result, peer TLS handshake fails ("tls: \"<ip>\" does not match any
		// of DNSNames"), etcd bootstrap dies, and the new pod never becomes Ready.
		// See analysis in internal/controller/utils.go createHeadlessServiceIfNotExist.
		assert.True(t, service.Spec.PublishNotReadyAddresses,
			"headless Service for an etcd cluster MUST publish not-ready addresses; otherwise peer-bootstrap TLS hangs forever")
		require.Len(t, service.OwnerReferences, 1)
		assert.Equal(t, ec.Name, service.OwnerReferences[0].Name)
	})

	t.Run("does not create service if it already exists", func(t *testing.T) {
		err := createHeadlessServiceIfNotExist(ctx, logger, fakeClient, ec, scheme)
		assert.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// validateEtcdUpgradePath
// ---------------------------------------------------------------------------

func TestValidateEtcdUpgradePath(t *testing.T) {
	etcdVersions := []semver.Version{
		{Major: 3, Minor: 0},
		{Major: 3, Minor: 1},
		{Major: 3, Minor: 2},
		{Major: 3, Minor: 3},
		{Major: 3, Minor: 4},
		{Major: 3, Minor: 5},
		{Major: 3, Minor: 6},
		{Major: 3, Minor: 7},
		{Major: 4, Minor: 0},
	}

	tests := []struct {
		name      string
		current   string
		target    string
		canParse  bool
		expectErr bool
	}{
		{name: "equal versions", current: "3.2.0", target: "3.2.0", canParse: true, expectErr: false},
		{name: "valid minor level upgrade", current: "3.4.0", target: "3.5.0", canParse: true, expectErr: false},
		{name: "valid patch level upgrade", current: "3.4.0", target: "3.4.1", canParse: true, expectErr: false},
		{name: "invalid current version", current: "invalid", target: "3.1.0", canParse: false, expectErr: true},
		{name: "invalid target version", current: "3.1.0", target: "invalid", canParse: false, expectErr: true},
		{name: "minor downgrade not allowed", current: "3.2.0", target: "3.1.0", canParse: true, expectErr: true},
		{name: "patch downgrade not allowed", current: "3.5.1", target: "3.5.0", canParse: true, expectErr: true},
		{name: "unknown current version", current: "3.9.0", target: "4.0.0", canParse: true, expectErr: true},
		{name: "unknown target version", current: "4.0.0", target: "4.1.0", canParse: true, expectErr: true},
		{name: "invalid upgrade skipping minor", current: "3.4.0", target: "3.6.0", canParse: true, expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canParse, err := validateEtcdUpgradePath(etcdVersions, tt.current, tt.target)
			if canParse != tt.canParse {
				t.Fatalf("expected canParse=%v, got %v", tt.canParse, canParse)
			}
			if tt.expectErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("did not expect error, got %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Certificate config helpers
// ---------------------------------------------------------------------------

func TestCreateAutoCertificateConfig(t *testing.T) {
	tests := []struct {
		name     string
		ec       *ecv1alpha1.EtcdCluster
		expected *certInterface.Config
		wantErr  bool
	}{
		{
			name: "auto config with all fields set",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "test-namespace"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					TLS: &ecv1alpha1.TLSCertificate{
						Provider: string(certificate.Auto),
						ProviderCfg: ecv1alpha1.ProviderConfig{
							AutoCfg: &ecv1alpha1.ProviderAutoConfig{
								CommonConfig: ecv1alpha1.CommonConfig{
									CommonName:       "custom.example.com",
									Organization:     []string{"Test Org"},
									ValidityDuration: "720h",
									AltNames: ecv1alpha1.AltNames{
										DNSNames: []string{"custom1.example.com", "custom2.example.com"},
									},
								},
							},
						},
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "custom.example.com",
				Organization:     []string{"Test Org"},
				ValidityDuration: 720 * time.Hour,
				AltNames: certInterface.AltNames{
					DNSNames: []string{"custom1.example.com", "custom2.example.com"},
					IPs:      make([]net.IP, 2),
				},
			},
			wantErr: false,
		},
		{
			name: "auto config with nil AutoCfg — uses defaults",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "test-namespace"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					TLS: &ecv1alpha1.TLSCertificate{
						Provider:    string(certificate.Auto),
						ProviderCfg: ecv1alpha1.ProviderConfig{AutoCfg: nil},
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "test-cluster.test-namespace.svc.cluster.local",
				Organization:     nil,
				ValidityDuration: certInterface.DefaultAutoValidity,
				AltNames: certInterface.AltNames{
					DNSNames: []string{
						"*.test-cluster.test-namespace.svc.cluster.local",
						"test-cluster.test-namespace.svc.cluster.local",
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := createAutoCertificateConfig(tt.ec)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.expected.CommonName, result.CommonName)
				assert.Equal(t, tt.expected.Organization, result.Organization)
				assert.Equal(t, tt.expected.ValidityDuration, result.ValidityDuration)
				assert.Equal(t, tt.expected.AltNames.DNSNames, result.AltNames.DNSNames)
				assert.Equal(t, tt.expected.AltNames.IPs, result.AltNames.IPs)
			}
		})
	}
}

func TestCreateCMCertificateConfig(t *testing.T) {
	tests := []struct {
		name     string
		ec       *ecv1alpha1.EtcdCluster
		expected *certInterface.Config
		wantErr  bool
	}{
		{
			name: "cert-manager config with all fields set",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "test-namespace"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					TLS: &ecv1alpha1.TLSCertificate{
						Provider: string(certificate.CertManager),
						ProviderCfg: ecv1alpha1.ProviderConfig{
							CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
								CommonConfig: ecv1alpha1.CommonConfig{
									CommonName:       "cm.example.com",
									Organization:     []string{"CM Org"},
									ValidityDuration: "1440h",
									AltNames: ecv1alpha1.AltNames{
										DNSNames: []string{"cm1.example.com", "cm2.example.com"},
									},
								},
								IssuerName: "test-issuer",
								IssuerKind: "ClusterIssuer",
							},
						},
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "cm.example.com",
				Organization:     []string{"CM Org"},
				ValidityDuration: 1440 * time.Hour,
				AltNames: certInterface.AltNames{
					DNSNames: []string{"cm1.example.com", "cm2.example.com"},
					IPs:      make([]net.IP, 2),
				},
				ExtraConfig: map[string]any{
					"issuerName": "test-issuer",
					"issuerKind": "ClusterIssuer",
				},
			},
			wantErr: false,
		},
		{
			name: "cert-manager config with nil CertManagerCfg",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "test-namespace"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					TLS: &ecv1alpha1.TLSCertificate{
						Provider:    string(certificate.CertManager),
						ProviderCfg: ecv1alpha1.ProviderConfig{CertManagerCfg: nil},
					},
				},
			},
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := createCMCertificateConfig(tt.ec)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.expected.CommonName, result.CommonName)
				assert.Equal(t, tt.expected.Organization, result.Organization)
				assert.Equal(t, tt.expected.ValidityDuration, result.ValidityDuration)
				assert.Equal(t, tt.expected.AltNames.DNSNames, result.AltNames.DNSNames)
				assert.Equal(t, tt.expected.AltNames.IPs, result.AltNames.IPs)
				assert.Equal(t, tt.expected.ExtraConfig, result.ExtraConfig)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// peerEndpointForOrdinalIndex — scheme reflects TLS configuration
// ---------------------------------------------------------------------------

func TestPeerEndpointForOrdinal(t *testing.T) {
	mkCluster := func(name, namespace string, tls *ecv1alpha1.TLSCertificate) *ecv1alpha1.EtcdCluster {
		return &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: "1"},
			Spec:       ecv1alpha1.EtcdClusterSpec{TLS: tls},
		}
	}

	httpCluster := mkCluster("test-cluster", "default", nil)
	httpsCluster := mkCluster("test-cluster", "default", &ecv1alpha1.TLSCertificate{Provider: "auto"})

	// Replace the trailing scheme expectations; the member name is scheme-agnostic.
	const wantName = "test-cluster-0"
	httpName, httpURL := peerEndpointForOrdinalIndex(httpCluster, 0)
	httpsName, httpsURL := peerEndpointForOrdinalIndex(httpsCluster, 0)

	assert.Equal(t, wantName, httpName)
	assert.Equal(t, wantName, httpsName)

	assert.Equal(t, "http://test-cluster-0.test-cluster.default.svc.cluster.local:2380", httpURL)
	assert.Equal(t, "https://test-cluster-0.test-cluster.default.svc.cluster.local:2380", httpsURL)
}

// ---------------------------------------------------------------------------
// verifySecretHasCA — error path when a cert Secret lacks ca.crt
// ---------------------------------------------------------------------------

func TestVerifySecretHasCA(t *testing.T) {
	mkSecret := func(data map[string][]byte) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "x-tls", Namespace: "default"},
			Data:       data,
		}
	}

	t.Run("with ca.crt passes", func(t *testing.T) {
		err := verifySecretHasCA(mkSecret(map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
			"ca.crt":  []byte("ca"),
		}), string(certificate.Auto))
		assert.NoError(t, err)
	})

	t.Run("without ca.crt errors with provider hint", func(t *testing.T) {
		err := verifySecretHasCA(mkSecret(map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
		}), string(certificate.CertManager))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ca.crt")
		assert.Contains(t, err.Error(), "cert-manager")
	})
}
