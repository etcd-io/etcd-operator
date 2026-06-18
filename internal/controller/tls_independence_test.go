package controller

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))
	return scheme
}

func discardLogger() logr.Logger { return logr.Discard() }

func mustQuantity(s string) resource.Quantity { return resource.MustParse(s) }

// genClientKeypair returns a (cert, key, ca) PEM triple suitable for
// buildClientTLSConfig: a self-signed leaf is its own CA here, which is fine for
// exercising the operator-side config building (CA-capability across members is an
// e2e concern, out of scope for this unit).
func genClientKeypair(t *testing.T) (certPEM, keyPEM, caPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "etcd-operator-client"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM, certPEM // ca == the self-signed cert
}

// boolPtr is a small helper for the *bool ClientCertAuth field.
func boolPtr(b bool) *bool { return &b }

// cmSurface builds a cert-manager TLSSurface with mTLS on by default.
func cmSurface(clientCertAuth *bool) *ecv1alpha1.TLSSurface {
	return &ecv1alpha1.TLSSurface{
		Provider: "cert-manager",
		ProviderCfg: ecv1alpha1.ProviderConfig{
			CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
				IssuerKind: "ClusterIssuer",
				IssuerName: "etcd-ca-issuer",
			},
		},
		ClientCertAuth: clientCertAuth,
	}
}

func clusterWithTLS(name string, tls *ecv1alpha1.EtcdClusterTLS) *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "v3.6.12", TLS: tls},
	}
}

// TestTLSIndependenceArgsMatrix asserts that the peer and client surfaces drive the
// rendered etcd args fully independently: peer flags/scheme follow the peer surface,
// server flags/scheme follow the client surface, and each --*client-cert-auth flag
// follows its own surface's toggle.
func TestTLSIndependenceArgsMatrix(t *testing.T) {
	const name = "ec"

	cleartextArgs := []string{
		"--name=$(POD_NAME)",
		"--listen-peer-urls=http://0.0.0.0:2380",
		"--listen-client-urls=http://0.0.0.0:2379",
		"--initial-advertise-peer-urls=http://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2380",
		"--advertise-client-urls=http://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2379",
	}

	serverFlags := []string{
		"--cert-file=/etc/etcd/server-tls/tls.crt",
		"--key-file=/etc/etcd/server-tls/tls.key",
		"--trusted-ca-file=/etc/etcd/server-tls/ca.crt",
	}
	peerFlags := []string{
		"--peer-cert-file=/etc/etcd/peer-tls/tls.crt",
		"--peer-key-file=/etc/etcd/peer-tls/tls.key",
		"--peer-trusted-ca-file=/etc/etcd/peer-tls/ca.crt",
	}

	tests := []struct {
		name     string
		tls      *ecv1alpha1.EtcdClusterTLS
		expected []string
	}{
		{
			name:     "neither (cleartext) is byte-identical to today",
			tls:      nil,
			expected: cleartextArgs,
		},
		{
			name: "peer-only: peer https + peer flags, client stays cleartext",
			tls:  &ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil)},
			expected: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=https://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=https://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2379",
				peerFlags[0], peerFlags[1], peerFlags[2], "--peer-client-cert-auth",
			},
		},
		{
			name: "client-only: client https + server flags, peer stays cleartext",
			tls:  &ecv1alpha1.EtcdClusterTLS{Client: cmSurface(nil)},
			expected: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=https://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=https://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2379",
				serverFlags[0], serverFlags[1], serverFlags[2], "--client-cert-auth",
			},
		},
		{
			name: "both: all eight flags, both schemes https",
			tls:  &ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil), Client: cmSurface(nil)},
			expected: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=https://0.0.0.0:2380",
				"--listen-client-urls=https://0.0.0.0:2379",
				"--initial-advertise-peer-urls=https://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=https://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2379",
				serverFlags[0], serverFlags[1], serverFlags[2], "--client-cert-auth",
				peerFlags[0], peerFlags[1], peerFlags[2], "--peer-client-cert-auth",
			},
		},
		{
			name: "peer mTLS opt-out: --peer-client-cert-auth absent, server mTLS still on",
			tls: &ecv1alpha1.EtcdClusterTLS{
				Peer:   cmSurface(boolPtr(false)),
				Client: cmSurface(boolPtr(true)),
			},
			expected: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=https://0.0.0.0:2380",
				"--listen-client-urls=https://0.0.0.0:2379",
				"--initial-advertise-peer-urls=https://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=https://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2379",
				serverFlags[0], serverFlags[1], serverFlags[2], "--client-cert-auth",
				peerFlags[0], peerFlags[1], peerFlags[2],
			},
		},
		{
			name: "client mTLS opt-out: --client-cert-auth absent, peer mTLS still on",
			tls: &ecv1alpha1.EtcdClusterTLS{
				Peer:   cmSurface(boolPtr(true)),
				Client: cmSurface(boolPtr(false)),
			},
			expected: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=https://0.0.0.0:2380",
				"--listen-client-urls=https://0.0.0.0:2379",
				"--initial-advertise-peer-urls=https://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=https://$(POD_NAME).ec.$(POD_NAMESPACE).svc.cluster.local:2379",
				serverFlags[0], serverFlags[1], serverFlags[2],
				peerFlags[0], peerFlags[1], peerFlags[2], "--peer-client-cert-auth",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := clusterWithTLS(name, tt.tls)
			got := createArgs(name, nil, tlsArgsFor(ec))
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestTLSIndependenceSchemes asserts peerScheme/clientScheme are independent.
func TestTLSIndependenceSchemes(t *testing.T) {
	tests := []struct {
		name             string
		tls              *ecv1alpha1.EtcdClusterTLS
		wantPeerScheme   string
		wantClientScheme string
	}{
		{"neither", nil, "http", "http"},
		{"peer-only", &ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil)}, "https", "http"},
		{"client-only", &ecv1alpha1.EtcdClusterTLS{Client: cmSurface(nil)}, "http", "https"},
		{"both", &ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil), Client: cmSurface(nil)}, "https", "https"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := clusterWithTLS("ec", tt.tls)
			assert.Equal(t, tt.wantPeerScheme, peerScheme(ec), "peerScheme")
			assert.Equal(t, tt.wantClientScheme, clientScheme(ec), "clientScheme")
			// Endpoint generators must agree with the per-surface scheme.
			_, peerURL := peerEndpointForOrdinalIndex(ec, 0)
			assert.Contains(t, peerURL, tt.wantPeerScheme+"://", "peer endpoint scheme")
		})
	}
}

// stsContainer renders the StatefulSet for ec and returns the etcd container plus
// pod volumes, so cert mounts/volumes can be asserted per surface.
func stsContainer(t *testing.T, ec *ecv1alpha1.EtcdCluster) (corev1.Container, []corev1.Volume) {
	t.Helper()
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	require.NoError(t, createOrPatchStatefulSet(t.Context(), discardLogger(), ec, c, 1, scheme))
	sts, err := getStatefulSet(t.Context(), c, ec.Name, ec.Namespace)
	require.NoError(t, err)
	require.Len(t, sts.Spec.Template.Spec.Containers, 1)
	return sts.Spec.Template.Spec.Containers[0], sts.Spec.Template.Spec.Volumes
}

func volumeNames(vols []corev1.Volume) []string {
	out := make([]string, 0, len(vols))
	for _, v := range vols {
		out = append(out, v.Name)
	}
	return out
}

func mountNames(mounts []corev1.VolumeMount) []string {
	out := make([]string, 0, len(mounts))
	for _, m := range mounts {
		out = append(out, m.Name)
	}
	return out
}

// TestTLSIndependenceMounts asserts the server-cert mount is gated on the client
// surface and the peer-cert mount on the peer surface, independently.
func TestTLSIndependenceMounts(t *testing.T) {
	tests := []struct {
		name       string
		tls        *ecv1alpha1.EtcdClusterTLS
		wantServer bool
		wantPeer   bool
	}{
		{"neither: no cert volumes/mounts", nil, false, false},
		{"peer-only: peer mount only", &ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil)}, false, true},
		{"client-only: server mount only", &ecv1alpha1.EtcdClusterTLS{Client: cmSurface(nil)}, true, false},
		{"both: server + peer mounts", &ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil), Client: cmSurface(nil)}, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := clusterWithTLS("ec", tt.tls)
			container, vols := stsContainer(t, ec)
			vNames := volumeNames(vols)
			mNames := mountNames(container.VolumeMounts)

			assert.Equal(t, tt.wantServer, contains(vNames, serverCertVolumeName), "server volume")
			assert.Equal(t, tt.wantServer, contains(mNames, serverCertVolumeName), "server mount")
			assert.Equal(t, tt.wantPeer, contains(vNames, peerCertVolumeName), "peer volume")
			assert.Equal(t, tt.wantPeer, contains(mNames, peerCertVolumeName), "peer mount")
		})
	}
}

// TestTLSIndependenceMountsCoexistWithStorage asserts cert mounts and the storage
// data-dir mount coexist (append-not-assign).
func TestTLSIndependenceMountsCoexistWithStorage(t *testing.T) {
	ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil), Client: cmSurface(nil)})
	ec.Spec.StorageSpec = &ecv1alpha1.StorageSpec{
		VolumeSizeRequest: mustQuantity("1Gi"),
	}
	container, _ := stsContainer(t, ec)
	mNames := mountNames(container.VolumeMounts)
	assert.Contains(t, mNames, volumeName, "storage data-dir mount")
	assert.Contains(t, mNames, serverCertVolumeName, "server cert mount")
	assert.Contains(t, mNames, peerCertVolumeName, "peer cert mount")
}

// TestBuildClientTLSConfigGatedOnClientSurface asserts the operator's *tls.Config is
// built iff the client surface is configured, and nil otherwise (cleartext).
func TestBuildClientTLSConfigGatedOnClientSurface(t *testing.T) {
	scheme := newScheme(t)

	// peer-only and neither: operator config must be nil (no secret read at all).
	for _, tls := range []*ecv1alpha1.EtcdClusterTLS{
		nil,
		{Peer: cmSurface(nil)},
	} {
		ec := clusterWithTLS("ec", tls)
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		cfg, err := buildClientTLSConfig(t.Context(), ec, c)
		require.NoError(t, err)
		assert.Nil(t, cfg, "operator client tls.Config must be nil without a client surface")
	}

	// client surface set but secret missing => explicit error (not silent nil).
	ec := clusterWithTLS("ec", &ecv1alpha1.EtcdClusterTLS{Client: cmSurface(nil)})
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	_, err := buildClientTLSConfig(t.Context(), ec, c)
	require.Error(t, err, "missing client secret must be an error")

	// client surface set with a valid secret => non-nil config with the CA pinned.
	cert, key, ca := genClientKeypair(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: getClientCertName("ec"), Namespace: "ns"},
		Data: map[string][]byte{
			tlsCertFile: cert,
			tlsKeyFile:  key,
			tlsCAFile:   ca,
		},
	}
	c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	cfg, err := buildClientTLSConfig(t.Context(), ec, c)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.RootCAs, "RootCAs must be pinned from the secret CA")
	assert.Len(t, cfg.Certificates, 1)

	// client surface set, secret present but missing ca.crt => explicit error
	// (not a silent fall-through to system roots).
	secretNoCA := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: getClientCertName("ec"), Namespace: "ns"},
		Data: map[string][]byte{
			tlsCertFile: cert,
			tlsKeyFile:  key,
		},
	}
	c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(secretNoCA).Build()
	_, err = buildClientTLSConfig(t.Context(), ec, c)
	require.Error(t, err, "missing ca.crt must be an explicit error")
}

// TestValidateTLS covers the reconcile-time anti-misconfiguration backstop.
func TestValidateTLS(t *testing.T) {
	tests := []struct {
		name    string
		tls     *ecv1alpha1.EtcdClusterTLS
		wantErr bool
	}{
		{"nil tls is valid (fully cleartext)", nil, false},
		{
			name:    "valid cert-manager mTLS on both surfaces",
			tls:     &ecv1alpha1.EtcdClusterTLS{Peer: cmSurface(nil), Client: cmSurface(nil)},
			wantErr: false,
		},
		{
			name: "cert-manager provider without certManagerCfg is rejected",
			tls: &ecv1alpha1.EtcdClusterTLS{
				Client: &ecv1alpha1.TLSSurface{Provider: "cert-manager"},
			},
			wantErr: true,
		},
		{
			name: "certManagerCfg under auto provider is rejected",
			tls: &ecv1alpha1.EtcdClusterTLS{
				Client: &ecv1alpha1.TLSSurface{
					Provider: "auto",
					ProviderCfg: ecv1alpha1.ProviderConfig{
						CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
							IssuerKind: "ClusterIssuer", IssuerName: "x",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "clientCertAuth (mTLS) with cert-manager but no issuerName is rejected",
			tls: &ecv1alpha1.EtcdClusterTLS{
				Client: &ecv1alpha1.TLSSurface{
					Provider:       "cert-manager",
					ClientCertAuth: boolPtr(true),
					ProviderCfg: ecv1alpha1.ProviderConfig{
						CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{IssuerKind: "ClusterIssuer"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "server-only TLS (clientCertAuth false) without issuerName is allowed",
			tls: &ecv1alpha1.EtcdClusterTLS{
				Client: &ecv1alpha1.TLSSurface{
					Provider:       "cert-manager",
					ClientCertAuth: boolPtr(false),
					ProviderCfg: ecv1alpha1.ProviderConfig{
						CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{IssuerKind: "ClusterIssuer", IssuerName: "x"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "auto provider on both surfaces is valid",
			tls: &ecv1alpha1.EtcdClusterTLS{
				Peer:   &ecv1alpha1.TLSSurface{Provider: "auto"},
				Client: &ecv1alpha1.TLSSurface{Provider: "auto"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validateTLS(clusterWithTLS("ec", tt.tls))
			if tt.wantErr {
				assert.NotEmpty(t, errs, "expected a validation error")
			} else {
				assert.Empty(t, errs, "expected no validation error: %v", errs)
			}
		})
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
