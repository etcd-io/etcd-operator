package cert_manager

import (
	"context"
	"net"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

func TestCreateCertificatePropagatesIPAddresses(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, certmanagerv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	cm := &CertManagerProvider{c}

	secretKey := client.ObjectKey{Name: "test-cert", Namespace: "test-ns"}
	cfg := &interfaces.Config{
		CommonName:       "test.example.com",
		ValidityDuration: 90 * 24 * time.Hour,
		AltNames: interfaces.AltNames{
			DNSNames: []string{"test.example.com"},
			IPs:      []net.IP{net.ParseIP("10.96.99.99"), net.ParseIP("192.168.1.10")},
		},
		ExtraConfig: map[string]any{
			IssuerNameKey: "test-issuer",
			IssuerKindKey: "ClusterIssuer",
		},
	}

	require.NoError(t, cm.createCertificate(context.Background(), secretKey, cfg))

	cert := &certmanagerv1.Certificate{}
	require.NoError(t, c.Get(context.Background(), secretKey, cert))
	assert.Equal(t, []string{"10.96.99.99", "192.168.1.10"}, cert.Spec.IPAddresses)
	assert.Equal(t, []string{"test.example.com"}, cert.Spec.DNSNames)
}
