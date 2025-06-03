package e2e

import (
	"context"
	"testing"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	apiextensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"go.etcd.io/etcd-operator/pkg/certificate/cert_manager"
)

const cmIssuerName = "selfsigned"
const cmCertificateName = "sample-cert"

func TestCertManagerProvider(t *testing.T) {
	feature := features.New("Cert-Manager Certificate").WithLabel("app", "cert-manager")

	cmIssuer := &certv1.ClusterIssuer{
		TypeMeta: metav1.TypeMeta{
			Kind: "ClusterIssuer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: cmIssuerName,
		},
		Spec: certv1.IssuerSpec{
			IssuerConfig: certv1.IssuerConfig{
				SelfSigned: &certv1.SelfSignedIssuer{},
			}},
	}

	feature.Setup(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			_ = appsv1.AddToScheme(client.Resources().GetScheme())
			_ = corev1.AddToScheme(client.Resources().GetScheme())
			_ = certv1.AddToScheme(client.Resources().GetScheme())
			_ = apiextensionsV1.AddToScheme(client.Resources().GetScheme())

			var issuer certv1.ClusterIssuer
			if err := client.Resources().Get(ctx, cmIssuerName, "", &issuer); err != nil {
				if k8serrors.IsNotFound(err) {
					t.Logf("No cert-manager selfsigned issuer found, creating clusterIssuer: %s", cmIssuerName)
					// Create cert-manager issuer
					if err := client.Resources().Create(ctx, cmIssuer); err != nil {
						t.Fatalf("unable to create cert-manager selfsigned issuer: %s", err)
					}
				} else {
					t.Fatalf("Failed to get cert-manager selfsigned issuer: %s", cmIssuerName)
				}
			}
			return ctx
		})

	feature.Assess("Get certificate config",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
			_, err := cmProvider.GetCertificateConfig(ctx, cmCertificateName, "")
			if err != nil {
				t.Logf("Cert-Manager Certificate not found")
			}
			return ctx
		})

	feature.Teardown(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
			err := cmProvider.RevokeCertificate(ctx, cmCertificateName, "")
			if err != nil {
				t.Logf("Cert-Manager Certificate not found")
			}
			return ctx
		})

	_ = testEnv.Test(t, feature.Feature())
}
