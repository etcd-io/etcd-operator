package cert_manager

import (
	"testing"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

func newFakeProvider(t *testing.T, objs ...client.Object) (*CertManagerProvider, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := certmanagerv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add cert-manager scheme: %v", err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &CertManagerProvider{cl}, cl
}

// TestCreateCertificateSpec verifies the typed Config fields reach the
// generated cert-manager Certificate spec.
func TestCreateCertificateSpec(t *testing.T) {
	provider, cl := newFakeProvider(t)

	renewBefore := &metav1.Duration{Duration: 360 * time.Hour}
	secretKey := client.ObjectKey{Name: "typed-cert", Namespace: "default"}
	cfg := &interfaces.Config{
		CommonName:    "etcd.default.svc.cluster.local",
		Organizations: []string{"etcd-operator"},
		DNSNames:      []string{"*.etcd.default.svc.cluster.local"},
		IPAddresses:   []string{"10.0.0.5"},
		Duration:      2160 * time.Hour,
		RenewBefore:   renewBefore,
		IssuerRef: &cmmeta.IssuerReference{
			Name: "etcd-ca",
			Kind: "ClusterIssuer",
		},
	}

	if err := provider.createCertificate(t.Context(), secretKey, cfg); err != nil {
		t.Fatalf("createCertificate failed: %v", err)
	}

	var cert certmanagerv1.Certificate
	if err := cl.Get(t.Context(), secretKey, &cert); err != nil {
		t.Fatalf("Certificate not created: %v", err)
	}
	if cert.Spec.IssuerRef != *cfg.IssuerRef {
		t.Errorf("IssuerRef = %+v, want %+v", cert.Spec.IssuerRef, *cfg.IssuerRef)
	}
	if cert.Spec.RenewBefore == nil || cert.Spec.RenewBefore.Duration != renewBefore.Duration {
		t.Errorf("RenewBefore = %v, want %v", cert.Spec.RenewBefore, renewBefore)
	}
	if len(cert.Spec.IPAddresses) != 1 || cert.Spec.IPAddresses[0] != "10.0.0.5" {
		t.Errorf("IPAddresses = %v, want [10.0.0.5]", cert.Spec.IPAddresses)
	}
	if cert.Spec.Duration == nil || cert.Spec.Duration.Duration != cfg.Duration {
		t.Errorf("Duration = %v, want %v", cert.Spec.Duration, cfg.Duration)
	}
}

// TestCreateCertificateRequiresIssuerRef verifies a nil IssuerRef is rejected
// before any Certificate object is created.
func TestCreateCertificateRequiresIssuerRef(t *testing.T) {
	provider, _ := newFakeProvider(t)

	secretKey := client.ObjectKey{Name: "no-issuer", Namespace: "default"}
	err := provider.createCertificate(t.Context(), secretKey, &interfaces.Config{CommonName: "x"})
	if err == nil {
		t.Fatal("createCertificate accepted a nil IssuerRef")
	}
}

// TestValidateCertificateConfigKindDefault verifies an empty issuerRef.kind is
// resolved to cert-manager's documented default, "Issuer".
func TestValidateCertificateConfigKindDefault(t *testing.T) {
	issuer := &certmanagerv1.Issuer{
		ObjectMeta: metav1.ObjectMeta{Name: "ns-issuer", Namespace: "default"},
	}
	provider, _ := newFakeProvider(t, issuer)

	cfg := &interfaces.Config{
		IssuerRef: &cmmeta.IssuerReference{Name: "ns-issuer"}, // Kind omitted
	}
	if err := provider.validateCertificateConfig(t.Context(), "default", cfg); err != nil {
		t.Fatalf("empty issuerRef.kind should default to Issuer, got error: %v", err)
	}

	missing := &interfaces.Config{
		IssuerRef: &cmmeta.IssuerReference{Name: "absent"},
	}
	if err := provider.validateCertificateConfig(t.Context(), "default", missing); err == nil {
		t.Fatal("validateCertificateConfig accepted a missing namespaced Issuer")
	}
}
