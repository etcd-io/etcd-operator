package auto

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

// TestEnsureCertificateSecretIPSANs verifies that user-supplied IP SANs land in
// the generated self-signed certificate alongside DNS SANs.
func TestEnsureCertificateSecretIPSANs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	provider := New(cl)

	secretKey := client.ObjectKey{Name: "ip-san-cert", Namespace: "default"}
	cfg := &interfaces.Config{
		CommonName:  "etcd.default.svc.cluster.local",
		DNSNames:    []string{"member.etcd.default.svc.cluster.local"},
		IPAddresses: []string{"10.0.0.5", "192.168.1.9"},
		Duration:    interfaces.DefaultAutoValidity,
	}

	if err := provider.EnsureCertificateSecret(t.Context(), secretKey, cfg); err != nil {
		t.Fatalf("EnsureCertificateSecret failed: %v", err)
	}

	var secret corev1.Secret
	if err := cl.Get(t.Context(), secretKey, &secret); err != nil {
		t.Fatalf("generated secret not found: %v", err)
	}
	cert, err := parseCertificateFromSecret(&secret)
	if err != nil {
		t.Fatalf("failed to parse generated certificate: %v", err)
	}

	gotIPs := map[string]bool{}
	for _, ip := range cert.IPAddresses {
		gotIPs[ip.String()] = true
	}
	for _, want := range cfg.IPAddresses {
		if !gotIPs[want] {
			t.Errorf("IP SAN %s missing from generated certificate, got %v", want, cert.IPAddresses)
		}
	}

	gotDNS := map[string]bool{}
	for _, name := range cert.DNSNames {
		gotDNS[name] = true
	}
	if !gotDNS["member.etcd.default.svc.cluster.local"] {
		t.Errorf("DNS SAN missing from generated certificate, got %v", cert.DNSNames)
	}
}

// TestGetCertificateConfigRoundTrip verifies that a config used for creation is
// echoed back by GetCertificateConfig, including IP SANs as literal strings.
func TestGetCertificateConfigRoundTrip(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	provider := New(cl)

	secretKey := client.ObjectKey{Name: "roundtrip-cert", Namespace: "default"}
	cfg := &interfaces.Config{
		CommonName:  "etcd.default.svc.cluster.local",
		IPAddresses: []string{"10.0.0.5"},
		Duration:    interfaces.DefaultAutoValidity,
	}
	if err := provider.EnsureCertificateSecret(t.Context(), secretKey, cfg); err != nil {
		t.Fatalf("EnsureCertificateSecret failed: %v", err)
	}

	got, err := provider.GetCertificateConfig(t.Context(), secretKey)
	if err != nil {
		t.Fatalf("GetCertificateConfig failed: %v", err)
	}
	if got.CommonName != cfg.CommonName {
		t.Errorf("CommonName = %q, want %q", got.CommonName, cfg.CommonName)
	}
	if len(got.IPAddresses) != 1 || got.IPAddresses[0] != "10.0.0.5" {
		t.Errorf("IPAddresses = %v, want [10.0.0.5]", got.IPAddresses)
	}
}
