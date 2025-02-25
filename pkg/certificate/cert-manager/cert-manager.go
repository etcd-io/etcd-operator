package cert_manager

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

type CertManagerProvider struct {
	client.Client
	Scheme *runtime.Scheme
}

func (cm *CertManagerProvider) EnsureCertificateSecret(
	ctx context.Context,
	secretName string,
	namespace string,
	cfg *interfaces.Config) error {
	foundCert := &certmanagerv1.Certificate{}
	foundErr := cm.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, foundCert)
	if foundErr != nil {
		return foundErr
	}

	issuerName, ok := cfg.ExtraConfig["issuerName"].(string)
	if !ok {
		return fmt.Errorf("issuerName is missing in ExtraConfig")
	}
	issuerKind, ok := cfg.ExtraConfig["issuerKind"].(string)
	if !ok {
		return fmt.Errorf("issuerKind is missing in ExtraConfig")
	}

	certificateResource := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Spec: certmanagerv1.CertificateSpec{
			SecretName: secretName,
			DNSNames:   cfg.AltNames.DNSNames,
			IssuerRef: cmmeta.ObjectReference{
				Name: issuerName,
				Kind: issuerKind,
			},
		},
	}

	createErr := cm.Create(ctx, certificateResource)
	if createErr != nil {
		return createErr
	}

	return nil

}

func (cm *CertManagerProvider) ValidateCertificateSecret(
	ctx context.Context,
	secretName string,
	namespace string,
	cfg *interfaces.Config) (bool, error) {
	secret := &corev1.Secret{}
	err := cm.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, secret)
	if err != nil {
		return false, fmt.Errorf("failed to get secret: %v", err)
	}

	certificateData, exists := secret.Data["tls.crt"]
	if !exists {
		return false, fmt.Errorf("interfaces not found in secret")
	}

	parseCert, err := x509.ParseCertificate(certificateData)
	if err != nil {
		return false, fmt.Errorf("failed to parse interfaces: %v", err)
	}

	if parseCert.NotAfter.Before(time.Now()) {
		return false, fmt.Errorf("interfaces has expired")
	}

	return true, nil
}

func (cm *CertManagerProvider) DeleteCertificateSecret(ctx context.Context, secretName string, namespace string) error {
	secret := &corev1.Secret{}
	err := cm.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, secret)
	if err != nil {
		return fmt.Errorf("failed to get secret: %v", err)
	}

	// Delete the Secret
	err = cm.Delete(ctx, secret)
	if err != nil {
		return fmt.Errorf("failed to delete secret: %v", err)
	}

	return nil
}

func (cm *CertManagerProvider) RevokeCertificate(ctx context.Context, secretName string, namespace string) error {
	// TODO
	panic("TODO")
}

func (cm *CertManagerProvider) GetCertificateConfig(
	ctx context.Context,
	secretName string,
	namespace string) (*interfaces.Config, error) {
	cmCertificate := &certmanagerv1.Certificate{}
	err := cm.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, cmCertificate)
	if err != nil {
		return nil, fmt.Errorf("failed to get interfaces: %v", err)
	}

	cfg := &interfaces.Config{
		CommonName:   cmCertificate.Spec.CommonName,
		Organization: cmCertificate.Spec.Subject.Organizations,
		AltNames: interfaces.AltNames{
			DNSNames: cmCertificate.Spec.DNSNames,
			IPs:      make([]net.IP, len(cmCertificate.Spec.IPAddresses)),
		},
		ValidityDuration: cmCertificate.Spec.Duration.Duration,
		ExtraConfig: map[string]any{
			"issuerName": cmCertificate.Spec.IssuerRef.Name,
			"issuerKind": cmCertificate.Spec.IssuerRef.Kind,
		},
	}

	return cfg, nil
}
