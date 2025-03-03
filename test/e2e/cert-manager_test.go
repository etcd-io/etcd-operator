package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	certmanager "go.etcd.io/etcd-operator/pkg/certificate/cert-manager"
	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

func TestCertManagerCM(t *testing.T) {
	feature := features.New("Cert-Manager Certificate").WithLabel("app", "cert-manager")

	feature.Setup(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cmProvider := certmanager.New()
			_, getCertConfigErr := cmProvider.GetCertificateConfig(ctx, "test-cert-secret", cfg.Namespace())
			if errors.IsNotFound(getCertConfigErr) {
				t.Log(getCertConfigErr)
			} else {
				t.Fatal(getCertConfigErr)
			}
			cmConfig := &interfaces.Config{
				AltNames: interfaces.AltNames{
					DNSNames: []string{"test.com"},
				},
				ExtraConfig: map[string]interface{}{
					"issuerName": "test-issuer",
					"issuerKind": "Issuer",
				},
			}
			err := cmProvider.EnsureCertificateSecret(ctx, "test-cert-secret", cfg.Namespace(), cmConfig)
			if err != nil {
				t.Fatal(err)
			}
			return ctx
		})

	feature.Assess("Validate certificate secret",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cmProvider := certmanager.New()
			valid, err := cmProvider.ValidateCertificateSecret(ctx, "test-cert-secret", cfg.Namespace(), &interfaces.Config{})
			if err != nil {
				t.Fatal(err)
			}
			assert.True(t, valid)
			return ctx
		})

	feature.Teardown(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cmProvider := certmanager.New()
			deleteSecretErr := cmProvider.DeleteCertificateSecret(ctx, "test-cert-secret", cfg.Namespace())
			if errors.IsNotFound(deleteSecretErr) {
				t.Log(deleteSecretErr)
			} else {
				t.Fatal(deleteSecretErr)
			}
			deleteCertErr := cmProvider.RevokeCertificate(ctx, "test-cert-secret", cfg.Namespace())
			if errors.IsNotFound(deleteCertErr) {
				t.Log(deleteCertErr)
			} else {
				t.Fatal(deleteCertErr)
			}
			return ctx
		})

	_ = testEnv.Test(t, feature.Feature())
}
