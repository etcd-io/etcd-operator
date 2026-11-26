package cert_manager

import (
	"context"
	"net"
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// GetCertificateConfig has to return the Certificate's IP addresses, not as
// many nil IPs.
func TestGetCertificateConfigIPAddresses(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, certmanagerv1.AddToScheme(scheme))

	certificate := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{Name: "test-server-tls", Namespace: "default"},
		Spec: certmanagerv1.CertificateSpec{
			CommonName:  "etcd.test",
			Subject:     &certmanagerv1.X509Subject{},
			DNSNames:    []string{"etcd.test"},
			IPAddresses: []string{"10.0.0.1", "fd00::1"},
			Duration:    &metav1.Duration{Duration: time.Hour},
			IssuerRef:   cmmeta.IssuerReference{Name: "test-issuer", Kind: "ClusterIssuer"},
		},
	}
	fakeClient := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(certificate).Build()

	cfg, err := New(fakeClient).GetCertificateConfig(context.Background(), client.ObjectKeyFromObject(certificate))
	require.NoError(t, err)
	assert.Equal(t, []net.IP{net.ParseIP("10.0.0.1"), net.ParseIP("fd00::1")}, cfg.AltNames.IPs)
}
