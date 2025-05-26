package e2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	apiextensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"go.etcd.io/etcd-operator/pkg/certificate/cert_manager"
	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

const (
	cmIssuerName           = "selfsigned"
	cmIssuerType           = "ClusterIssuer"
	cmCertificateName      = "sample-cert"
	cmCertificateNamespace = "default"
	cmCertificateValidity  = 24 * time.Hour
)

func TestCertManagerProvider(t *testing.T) {
	feature := features.New("Cert-Manager Certificate").WithLabel("app", "cert-manager")

	cmIssuer := &certv1.ClusterIssuer{
		TypeMeta: metav1.TypeMeta{
			Kind: cmIssuerType,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: cmIssuerName,
		},
		Spec: certv1.IssuerSpec{
			IssuerConfig: certv1.IssuerConfig{
				SelfSigned: &certv1.SelfSignedIssuer{},
			},
		},
	}

	cmConfig := &interfaces.Config{
		CommonName:       cmCertificateName,
		ValidityDuration: cmCertificateValidity,
		ExtraConfig: map[string]any{
			"issuerName": cmIssuerName,
			"issuerKind": cmIssuerType,
		},
	}

	feature.Setup(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			_ = appsv1.AddToScheme(client.Resources().GetScheme())
			_ = corev1.AddToScheme(client.Resources().GetScheme())
			_ = certv1.AddToScheme(client.Resources().GetScheme())
			_ = apiextensionsV1.AddToScheme(client.Resources().GetScheme())

			var issuer certv1.ClusterIssuer
			if err := client.Resources().Get(ctx, cmIssuerName, cmCertificateNamespace, &issuer); err != nil {
				if k8serrors.IsNotFound(err) {
					t.Logf("No cert-manager selfsigned issuer found, creating clusterIssuer: %s", cmIssuerName)
					// Create cert-manager issuer
					if err := client.Resources().Create(ctx, cmIssuer); err != nil {
						t.Fatalf("unable to create cert-manager selfsigned issuer: %s", err)
					}
				} else {
					t.Fatalf("Failed to get cert-manager selfsigned issuer: %s", cmIssuerName)
				}
				t.Logf("Created clusterIssuer: %s", cmIssuerName)
			}

			return ctx
		})

	feature.Assess("Ensure certificate",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
			err := cmProvider.EnsureCertificateSecret(ctx, cmCertificateName, cmCertificateNamespace, cmConfig)
			if err != nil {
				t.Fatalf("Cert-Manager Certificate could not be created: %v", err)
			}
			return ctx
		})

	feature.Assess("Validate certificate secret",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
			err := cmProvider.ValidateCertificateSecret(ctx, cmCertificateName, cmCertificateNamespace, cmConfig)
			if err != nil {
				t.Fatalf("Failed to validate Cert-Manager Certificate secret: %v", err)
			}
			return ctx
		})

	feature.Assess("Get certificate config",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
			config, err := cmProvider.GetCertificateConfig(ctx, cmCertificateName, cmCertificateNamespace)
			if err != nil {
				t.Fatalf("Cert-Manager Certificate not found: %v", err)
			}
			if !reflect.DeepEqual(config, cmConfig) {
				t.Fatalf("Cert-Manager Certificate config does not match with the given config")
			}
			return ctx
		})

	feature.Assess("Delete certificate secret",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
			err := cmProvider.DeleteCertificateSecret(ctx, cmCertificateName, cmCertificateNamespace)
			if err != nil {
				t.Fatalf("Failed to delete Certificate secret: %v", err)
			}
			return ctx
		})

	feature.Assess("Verify Delete certificate",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
			_, err := cmProvider.GetCertificateConfig(ctx, cmCertificateName, cmCertificateNamespace)
			if err == nil {
				t.Fatalf("Cert-Manager Certificate found, deletion failed: %v", err)
			}
			return ctx
		})

	_ = testEnv.Test(t, feature.Feature())
}
