package auto

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

func TestCreateNewSecretIncludesIPSANs(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ac := &Provider{Client: c}

	secretKey := client.ObjectKey{Name: "test-cert", Namespace: "test-ns"}
	cfg := &interfaces.Config{
		CommonName:       "test.example.com",
		ValidityDuration: 365 * 24 * time.Hour,
		AltNames: interfaces.AltNames{
			DNSNames: []string{"test.example.com"},
			IPs:      []net.IP{net.ParseIP("10.96.99.99")},
		},
	}

	require.NoError(t, ac.createNewSecret(context.Background(), secretKey, cfg))

	var secret corev1.Secret
	require.NoError(t, c.Get(context.Background(), secretKey, &secret))

	block, _ := pem.Decode(secret.Data[corev1.TLSCertKey])
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	var ipStrings []string
	for _, ip := range cert.IPAddresses {
		ipStrings = append(ipStrings, ip.String())
	}
	assert.Contains(t, ipStrings, "10.96.99.99")
	assert.Contains(t, cert.DNSNames, "test.example.com")
}
