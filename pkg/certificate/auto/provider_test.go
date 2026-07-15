package auto

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
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
