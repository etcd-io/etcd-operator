package e2e

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
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

var cmIssuer = &certv1.ClusterIssuer{
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

func TestCertManagerProvider(t *testing.T) {
	feature := features.New("Cert-Manager Certificate").WithLabel("app", "cert-manager")

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
			cl := cfg.Client()
			cmProvider := cert_manager.New(cl.Resources().GetControllerRuntimeClient())
			secretKey := client.ObjectKey{Name: cmCertificateName, Namespace: cmCertificateNamespace}
			err := cmProvider.EnsureCertificateSecret(ctx, secretKey, cmConfig)
			if err != nil {
				t.Fatalf("Cert-Manager Certificate could not be created: %v", err)
			}
			return ctx
		})

	feature.Assess("Validate certificate secret",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cl := cfg.Client()
			cmProvider := cert_manager.New(cl.Resources().GetControllerRuntimeClient())
			secretKey := client.ObjectKey{Name: cmCertificateName, Namespace: cmCertificateNamespace}
			err := cmProvider.ValidateCertificateSecret(ctx, secretKey, cmConfig)
			if err != nil {
				t.Fatalf("Failed to validate Cert-Manager Certificate secret: %v", err)
			}
			return ctx
		})

	feature.Assess("Get certificate config",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cl := cfg.Client()
			cmProvider := cert_manager.New(cl.Resources().GetControllerRuntimeClient())
			secretKey := client.ObjectKey{Name: cmCertificateName, Namespace: cmCertificateNamespace}
			config, err := cmProvider.GetCertificateConfig(ctx, secretKey)
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
			cl := cfg.Client()
			cmProvider := cert_manager.New(cl.Resources().GetControllerRuntimeClient())
			secretKey := client.ObjectKey{Name: cmCertificateName, Namespace: cmCertificateNamespace}
			err := cmProvider.DeleteCertificateSecret(ctx, secretKey)
			if err != nil {
				t.Fatalf("Failed to delete Certificate secret: %v", err)
			}
			return ctx
		})

	feature.Assess("Verify Delete certificate",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cl := cfg.Client()
			cmProvider := cert_manager.New(cl.Resources().GetControllerRuntimeClient())
			secretKey := client.ObjectKey{Name: cmCertificateName, Namespace: cmCertificateNamespace}
			_, err := cmProvider.GetCertificateConfig(ctx, secretKey)
			if err == nil {
				t.Fatalf("Cert-Manager Certificate found, deletion failed: %v", err)
			}
			return ctx
		})

	_ = testEnv.Test(t, feature.Feature())
}

func TestClusterCertCreation(t *testing.T) {
	feature := features.New("cluster-cert-creation")

	const etcdClusterName = "etcd-cluster-cert"
	const size = 3

	etcdCluster := &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdClusterName,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: etcdVersion,
			TLS: &ecv1alpha1.TLSCertificate{
				Provider: "cert-manager",
				ProviderCfg: ecv1alpha1.ProviderConfig{
					CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
						CommonConfig: ecv1alpha1.CommonConfig{
							CommonName:       "etcd-operator-system",
							ValidityDuration: "90h",
						},
						IssuerKind: cmIssuerType,
						IssuerName: cmIssuerName,
					},
				},
			},
		},
	}

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
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

		// create etcd cluster
		if err := client.Resources().Create(ctx, etcdCluster); err != nil {
			t.Fatalf("unable to create etcd cluster: %s", err)
		}

		// get etcd cluster object
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, etcdClusterName, namespace, &ec); err != nil {
			t.Fatalf("unable to fetch etcd cluster: %s", err)
		}

		return ctx
	})

	feature.Assess("Check certificates exist",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()
			// checks if client, server, peer certificates are created in the respective namespace
			if err := wait.For(
				func(context.Context) (bool, error) {
					return validateResourceExists(ctx, client, etcdClusterName, namespace, "certificate")
				},
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(10*time.Second),
			); err != nil {
				t.Fatalf("timed out waiting for certificate: %s", err)
			}

			return ctx
		},
	)

	feature.Assess("Check certificate secrets exist",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			client := c.Client()
			// checks if corresponding client, server, peer secrets are created in the respective namespace
			if err := wait.For(
				func(context.Context) (bool, error) {
					return validateResourceExists(ctx, client, etcdClusterName, namespace, "secret")
				},
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(10*time.Second),
			); err != nil {
				t.Fatalf("timed out waiting for certificate: %s", err)
			}
			return ctx
		},
	)

	feature.Assess("Verify Data Operations",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// verify etcdCluster is accessible via client certificate with put and get
			verifyDataOperations(t, c, etcdClusterName, "test-key", "test-value")
			return ctx
		},
	)

	_ = testEnv.Test(t, feature.Feature())
}

func validateResourceExists(ctx context.Context, client klient.Client,
	etcdClusterName, etcdClusterNamespace, resourceType string) (bool, error) {
	clientCertName := fmt.Sprintf("%s-client-tls", etcdClusterName)
	serverCertName := fmt.Sprintf("%s-server-tls", etcdClusterName)
	peerCertName := fmt.Sprintf("%s-peer-tls", etcdClusterName)

	var obj any

	switch resourceType {
	case "certificate":
		var certObj certv1.Certificate
		obj = &certObj
	case "secret":
		var secretObj corev1.Secret
		obj = &secretObj
	default:
		return false, fmt.Errorf("invalid resource type: %v", resourceType)
	}

	runtimeObj, err := obj.(k8s.Object)
	if !err {
		return false, fmt.Errorf("object does not implement runtime.Object: %T", obj)
	}

	if err := client.Resources().Get(ctx, clientCertName, etcdClusterNamespace, runtimeObj); err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get Client %s: %v", resourceType, err)
	}

	if err := client.Resources().Get(ctx, serverCertName, etcdClusterNamespace, runtimeObj); err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get Server %s: %v", resourceType, err)
	}

	if err := client.Resources().Get(ctx, peerCertName, etcdClusterNamespace, runtimeObj); err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get Peer %s: %v", resourceType, err)
	}
	return true, nil
}
