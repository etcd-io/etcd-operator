package auto

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

// The operator uses the server certificate as a client to check etcd member
// status, so the auto-generated cert must carry the ClientAuth key usage.
func TestEnsureCertificateSecretCertHasClientAuth(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := clientfake.NewClientBuilder().WithScheme(scheme).Build()
	provider := New(fakeClient)

	secretKey := client.ObjectKey{Name: "test-server-tls", Namespace: "default"}
	cfg := &interfaces.Config{
		CommonName:       "etcd.test",
		ValidityDuration: interfaces.DefaultAutoValidity,
		AltNames: interfaces.AltNames{
			DNSNames: []string{"test.default.svc.cluster.local"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, provider.EnsureCertificateSecret(ctx, secretKey, cfg))

	// Fetch the generated Secret and verify the cert carries ClientAuth.
	secret := &corev1.Secret{}
	require.NoError(t, fakeClient.Get(ctx, secretKey, secret))

	certPEM, ok := secret.Data["tls.crt"]
	require.True(t, ok, "secret must contain tls.crt")
	require.NotEmpty(t, certPEM)

	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block, "tls.crt is valid PEM")

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err, "tls.crt parses as a certificate")

	assert.Contains(t, cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth,
		"auto cert must carry ClientAuth so the operator can present it as a client (design D4-a)")
	assert.Contains(t, cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth,
		"auto cert must keep ServerAuth for its primary server role")

	// Sanity: the secret is structurally complete for an etcd TLS mount.
	assert.NotEmpty(t, secret.Data["tls.key"])
	assert.NotEmpty(t, secret.Data["ca.crt"])
	assert.Equal(t, corev1.SecretTypeTLS, secret.Type)
}

// ValidateCertificateSecret has to inspect the secret it fetched. A secret
// without a certificate in it, or one holding a certificate that has expired,
// is not a valid certificate secret.
func TestValidateCertificateSecretRejectsInvalidSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	expiredCertPEM, expiredKeyPEM := expiredSelfSignedPair(t)

	for name, tc := range map[string]struct {
		data    map[string][]byte
		wantErr error
	}{
		"secret without a certificate": {
			data:    map[string][]byte{},
			wantErr: interfaces.ErrTLSCert,
		},
		"certificate without a private key": {
			data:    map[string][]byte{"tls.crt": expiredCertPEM},
			wantErr: interfaces.ErrTLSKey,
		},
		"expired certificate": {
			data: map[string][]byte{
				"tls.crt": expiredCertPEM,
				"tls.key": expiredKeyPEM,
			},
			wantErr: interfaces.ErrCertExpired,
		},
	} {
		t.Run(name, func(t *testing.T) {
			secretKey := client.ObjectKey{Name: "test-server-tls", Namespace: "default"}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretKey.Name,
					Namespace: secretKey.Namespace,
				},
				Type: corev1.SecretTypeTLS,
				Data: tc.data,
			}

			fakeClient := clientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(secret).
				Build()
			provider := New(fakeClient)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			require.ErrorIs(t, provider.ValidateCertificateSecret(ctx, secretKey, nil), tc.wantErr)
		})
	}
}

// expiredSelfSignedPair returns a PEM certificate that expired an hour ago and
// the EC private key belonging to it.
func expiredSelfSignedPair(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "etcd.test"},
		NotBefore:    now.Add(-2 * time.Hour),
		NotAfter:     now.Add(-time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
