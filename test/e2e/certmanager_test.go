package e2e

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	apiextensionsV1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/certificate/cert_manager"
	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

const cmIssuerName = "selfsigned"
const etcdClusterName = "etcd-cluster-test"
const size = 3

func TestCertManagerProvider(t *testing.T) {
	feature := features.New("Cert-Manager Certificate").WithLabel("app", "cert-manager")

	// TODO: pick these from testdata yamls
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
			Version: "v3.5.18",
			TLS: &ecv1alpha1.TLSCertificate{
				Provider: "cert-manager",
				ProviderCfg: &ecv1alpha1.ProviderConfig{
					CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
						CommonName:   "etcd.etcd-operator-system",
						Organization: []string{"etcd-operator"},
						DNSNames:     []string{"etcd.etcd-operator-system"},
						IPs:          []net.IP{net.ParseIP("192.168.0.5")},
						Duration:     2160,
						IssuerName:   "selfsigned",
						IssuerKind:   "ClusterIssuer",
					},
				},
			},
		},
	}

	cmIssuer := &certv1.ClusterIssuer{
		TypeMeta: metav1.TypeMeta{
			Kind: "ClusterIssuer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "selfsigned",
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
			_ = ecv1alpha1.AddToScheme(client.Resources().GetScheme())

			var issuer certv1.ClusterIssuer
			if err := client.Resources().Get(ctx, cmIssuerName, "", &issuer); err != nil {
				if k8serrors.IsNotFound(err) {
					t.Logf("No cert-manager selfsigned issuer found, creating clusterIssuer: %s", cmIssuerName)
					// Create cert-manager issuer
					if err := client.Resources().Create(ctx, cmIssuer); err != nil {
						t.Fatalf("unable to craete cert-manager selfsigned issuer: %s", err)
					}
				} else {
					t.Fatalf("Failed to get cert-manager selfsigned issuer: %s", cmIssuerName)
				}
			}

			name := "etcdclusters.operator.etcd.io"
			var crd apiextensionsV1.CustomResourceDefinition
			if err := client.Resources().Get(ctx, name, "", &crd); err != nil {
				t.Fatalf("Failed due to error: %s", err)
			}

			if crd.Spec.Group != "operator.etcd.io" {
				t.Fatalf("Expected crd group to be operator.etcd.io, got %s", crd.Spec.Group)
			}

			// Create the etcd cluster resource
			if err := client.Resources().Create(ctx, etcdCluster); err != nil {
				t.Fatalf("unable to create an etcd cluster resource of size 1: %s", err)
			}

			if err := wait.For(
				conditions.New(client.Resources()).ResourceMatch(etcdCluster, func(object k8s.Object) bool {
					return true
				}),
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(30*time.Second),
			); err != nil {
				t.Fatal(err)
			}
			return ctx
		})

	feature.Assess("Check for client certificates",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			validateCertificates(ctx, client, t, "client")
			return ctx
		})

	feature.Assess("Check for server certificates",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			validateCertificates(ctx, client, t, "server")
			return ctx
		})

	feature.Assess("Check for peer certificates",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			validateCertificates(ctx, client, t, "peer")
			return ctx
		})

	feature.Teardown(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()
			deleteCertificates(ctx, client, t, "client")
			deleteCertificates(ctx, client, t, "server")
			deleteCertificates(ctx, client, t, "peer")
			return ctx
		})

	_ = testEnv.Test(t, feature.Feature())
}

func validateCertificates(ctx context.Context, client klient.Client, t *testing.T, certType string) {
	cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
	podList := corev1.PodList{}
	cmCert := certv1.Certificate{}
	// Check if the certificate type is Client
	if certType == "client" {
		cmCertName := fmt.Sprintf("%s-%s-tls", etcdClusterName, certType)
		certErr := client.Resources().Get(ctx, cmCertName, namespace, &cmCert)
		if certErr != nil {
			if k8serrors.IsNotFound(certErr) {
				t.Fatalf("No %s certificate found: %s", cmCertName, certErr)
			} else {
				t.Logf("Failed to get %s certificate: %s", certErr, cmCertName)
				t.Fatal(certErr)
			}
		}
		// Check if the secret created is a valid secret
		valid, secretErr := cmProvider.ValidateCertificateSecret(ctx, cmCertName, namespace, &interfaces.Config{})
		if secretErr != nil {
			t.Logf("Invalid secret: %s", cmCertName)
			t.Fatal(secretErr)
		} else {
			if !valid {
				t.Fatalf("No secret %s found", cmCertName)
			}
		}
	} else {
		// If the certificate type is Server or Peer, check for each member
		labelSelector := fmt.Sprintf("app=%s", etcdClusterName)
		err := client.Resources().List(ctx, &podList, resources.WithLabelSelector(labelSelector))
		if err != nil {
			t.Log("Failed to get StatefulSet pods")
			t.Fatal(err)
		}
		for _, pod := range podList.Items {
			cmCertName := fmt.Sprintf("%s-%s-tls", pod.Name, certType)
			certErr := client.Resources().Get(ctx, cmCertName, namespace, &cmCert)
			if certErr != nil {
				if k8serrors.IsNotFound(certErr) {
					t.Fatalf("No %s certificate found: %s", cmCertName, certErr)
				} else {
					t.Logf("Failed to get %s certificate: %s", certType, cmCertName)
					t.Fatal(certErr)
				}
			}
			valid, secretErr := cmProvider.ValidateCertificateSecret(ctx, cmCertName, namespace, &interfaces.Config{})
			if secretErr != nil {
				t.Logf("Invalid secret: %s", cmCertName)
				t.Fatal(secretErr)
			} else {
				if !valid {
					t.Fatalf("No secret %s found", cmCertName)
				}
			}
			assert.True(t, valid)
		}
	}
}

func deleteCertificates(ctx context.Context, client klient.Client, t *testing.T, certType string) {
	cmProvider := cert_manager.New(client.Resources().GetControllerRuntimeClient())
	podList := corev1.PodList{}
	cmCert := certv1.Certificate{}
	cmSecret := corev1.Secret{}
	// Check if the certificate type is Client
	if certType == "client" {
		cmCertName := fmt.Sprintf("%s-%s-tls", etcdClusterName, certType)
		deleteErr := cmProvider.RevokeCertificate(ctx, cmCertName, namespace)
		if deleteErr != nil {
			t.Fatalf("Failed to remvoke certificate: %s", deleteErr)
		}
		// Check if certificate has been deleted successfully
		certErr := client.Resources().Get(ctx, cmCertName, namespace, &cmCert)
		if certErr != nil {
			if k8serrors.IsNotFound(certErr) {
				t.Logf("No %s certificate to delete: %s", cmCertName, certErr)
			} else {
				t.Logf("Failed to get %s certificate for deletion: %s", certErr, cmCertName)
			}
		}
		// Check if secret has been deleted successfully
		secretErr := client.Resources().Get(ctx, cmCertName, namespace, &cmSecret)
		if secretErr != nil {
			if k8serrors.IsNotFound(secretErr) {
				t.Logf("No %s secret to delete: %s", cmCertName, secretErr)
			} else {
				t.Logf("Failed to get %s secret for deletion: %s", secretErr, cmCertName)
			}
		}
	} else {
		// If the certificate type is Server or Peer, check for each member
		labelSelector := fmt.Sprintf("app=%s", etcdClusterName)
		err := client.Resources().List(ctx, &podList, resources.WithLabelSelector(labelSelector))
		if err != nil {
			t.Log("Failed to get StatefulSet pods")
		}
		for _, pod := range podList.Items {
			cmCertName := fmt.Sprintf("%s-%s-tls", pod.Name, certType)
			deleteErr := cmProvider.RevokeCertificate(ctx, cmCertName, namespace)
			if deleteErr != nil {
				t.Fatalf("Failed to remvoke certificate: %s", deleteErr)
			}
			// Check if certificate has been deleted successfully
			certErr := client.Resources().Get(ctx, cmCertName, namespace, &cmCert)
			if certErr != nil {
				if k8serrors.IsNotFound(certErr) {
					t.Logf("No %s certificate to delete: %s", cmCertName, certErr)
				}
				t.Logf("Failed to get %s certificate for deletion: %s", certType, cmCertName)
			}
			// Check if secret has been deleted successfully
			secretErr := client.Resources().Get(ctx, cmCertName, namespace, &cmSecret)
			if secretErr != nil {
				if k8serrors.IsNotFound(secretErr) {
					t.Logf("No %s secret to delete: %s", cmCertName, secretErr)
				} else {
					t.Logf("Failed to get %s secret for deletion: %s", secretErr, cmCertName)
				}
			}
		}
	}
}
