package cert_manager

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

type Provider struct {
	client.Client
}

func New(client client.Client) *Provider {
	return &Provider{
		client,
	}
}

func (cm *Provider) createCertificate(
	ctx context.Context,
	secretName string,
	namespace string,
	cfg *interfaces.Config) error {
	issuerName := cfg.ExtraConfig["issuerName"].(string)
	issuerKind := cfg.ExtraConfig["issuerKind"].(string)

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

	return cm.Create(ctx, certificateResource)
}

// parsePrivateKey parses the private key from the PEM-encoded data.
func parsePrivateKey(privateKeyData []byte) (crypto.PrivateKey, error) {
	// Try to parse the private key in the format it might be provided in (e.g., PKCS#8, PEM)
	block, _ := pem.Decode(privateKeyData)
	if block == nil {
		return nil, errors.New("failed to decode private key: invalid PEM")
	}

	// Parse the private key from the PEM block
	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// Try to parse the private key in another format (e.g., RSA)
		privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}
	}

	return privateKey, nil
}

// checkKeyPair checks if the private key matches the certificate.
func checkKeyPair(cert *x509.Certificate, privateKey crypto.PrivateKey) error {
	switch key := privateKey.(type) {
	case *rsa.PrivateKey:
		// Check if the private key matches the certificate by validating the public key
		pub := cert.PublicKey.(*rsa.PublicKey)
		if !key.PublicKey.Equal(pub) {
			return errors.New("private key does not match the public key in the certificate")
		}
	case *ecdsa.PrivateKey:
		// Check if the private key matches the certificate by validating the public key
		pub := cert.PublicKey.(*ecdsa.PublicKey)
		if !key.PublicKey.Equal(pub) {
			return errors.New("private key does not match the public key in the certificate")
		}
	default:
		return fmt.Errorf("unsupported private key type: %T", key)
	}

	return nil
}

func (cm *Provider) EnsureCertificateSecret(
	ctx context.Context,
	secretName string,
	namespace string,
	cfg *interfaces.Config) error {
	issuerName, found := cfg.ExtraConfig["issuerName"].(string)
	if !found || len(strings.TrimSpace(issuerName)) == 0 {
		return errors.New("issuerName is missing in ExtraConfig")
	}
	issuerKind, found := cfg.ExtraConfig["issuerKind"].(string)
	if !found || len(strings.TrimSpace(issuerKind)) == 0 {
		return errors.New("issuerKind is missing in ExtraConfig")
	}

	checkCertSecret, valErr := cm.ValidateCertificateSecret(ctx, secretName, namespace, cfg)
	if valErr != nil {
		return fmt.Errorf("invalid certificate secret: %s present in namespace: %s, please delete and try again.\nError: %s",
			secretName, namespace, valErr)
	}
	if checkCertSecret {
		return fmt.Errorf("valid certificate secret: %s already present in namespace: %s , skipping Certificate creation",
			secretName, namespace)
	}
	log.Printf("certificate secret: %s not present in namespace: %s , creating new Certificate",
		secretName, namespace)

	return cm.createCertificate(ctx, secretName, namespace, cfg)
}

func (cm *Provider) ValidateCertificateSecret(
	ctx context.Context,
	secretName string,
	namespace string,
	_ *interfaces.Config) (bool, error) {
	secret := &corev1.Secret{}
	err := cm.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, secret)
	if err != nil {
		return false, nil
	}

	certificateData, exists := secret.Data["tls.crt"]
	if !exists {
		return false, errors.New("certificate not found in secret")
	}

	decodeCertificatePem, _ := pem.Decode(certificateData)
	if decodeCertificatePem == nil {
		return false, errors.New("failed to decode PEM block")
	}

	privateKeyData, keyExists := secret.Data["tls.key"]
	if !keyExists {
		return false, errors.New("private key not found in secret")
	}

	parseCert, err := x509.ParseCertificate(decodeCertificatePem.Bytes)
	if err != nil {
		return false, fmt.Errorf("failed to parse certificate: %w", err)
	}

	if parseCert.NotAfter.Before(time.Now()) {
		return false, errors.New("certificate has expired")
	}

	privateKey, err := parsePrivateKey(privateKeyData)
	if err != nil {
		return false, fmt.Errorf("failed to parse private key: %w", err)
	}

	if checkKeyPairErr := checkKeyPair(parseCert, privateKey); checkKeyPairErr != nil {
		return false, fmt.Errorf("private key does not match certificate: %w", checkKeyPairErr)
	}

	return true, nil
}

func (cm *Provider) DeleteCertificateSecret(ctx context.Context, secretName string, namespace string) error {
	secret := &corev1.Secret{}
	err := cm.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, secret)
	if err != nil {
		return fmt.Errorf("failed to get secret: %w", err)
	}

	// Delete the Secret
	err = cm.Delete(ctx, secret)
	if err != nil {
		return fmt.Errorf("failed to delete secret: %w", err)
	}

	return nil
}

func (cm *Provider) RevokeCertificate(ctx context.Context, secretName string, namespace string) error {
	cmCertificate := &certmanagerv1.Certificate{}
	getCertificateErr := cm.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, cmCertificate)
	if getCertificateErr != nil {
		return getCertificateErr
	}

	deleteCertificateErr := cm.Delete(ctx, cmCertificate)
	if deleteCertificateErr != nil {
		return deleteCertificateErr
	}

	// By default, cert-manager Certificate deletion does not delete the associated secret.
	// Existing secret will allow to services relying on that Certificate, so additionally delete it
	// More info: https://cert-manager.io/docs/usage/certificate/#cleaning-up-secrets-when-certificates-are-deleted
	deleteCertificateSecretErr := cm.DeleteCertificateSecret(ctx, secretName, namespace)
	if deleteCertificateSecretErr != nil {
		if k8serrors.IsNotFound(deleteCertificateSecretErr) {
			fmt.Println("Certificate secret not found, maybe already deleted")
		} else {
			return deleteCertificateSecretErr
		}

	}

	return nil
}

func (cm *Provider) GetCertificateConfig(
	ctx context.Context,
	secretName string,
	namespace string) (*interfaces.Config, error) {
	cmCertificate := &certmanagerv1.Certificate{}
	err := cm.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, cmCertificate)
	if err != nil {
		return nil, fmt.Errorf("failed to get certificate: %w", err)
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
