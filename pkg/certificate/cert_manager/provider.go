package cert_manager

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
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

const (
	IssuerNameKey = "issuerName"
	IssuerKindKey = "issuerKind"
)

type CertManagerProvider struct {
	client.Client
}

var _ interfaces.Provider = (*CertManagerProvider)(nil)

func New(c client.Client) interfaces.Provider {
	return &CertManagerProvider{
		c,
	}
}

func (cm *CertManagerProvider) EnsureCertificateSecret(ctx context.Context, secretKey client.ObjectKey,
	cfg *interfaces.Config) error {
	cmCertificate := &certmanagerv1.Certificate{}
	err := cm.Get(ctx, secretKey, cmCertificate)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			valErr := cm.validateCertificateConfig(ctx, secretKey.Namespace, cfg)
			if valErr != nil {
				return valErr
			}
			err := cm.createCertificate(ctx, secretKey, cfg)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	log.Printf("Valid certificate: %s present in namespace: %s, checking certificate status...",
		secretKey.Name, secretKey.Namespace)

	err = cm.checkCertificateStatus(secretKey.Name, secretKey.Namespace, ctx)
	if err != nil && !errors.Is(err, interfaces.ErrUnknown) {
		for try := range interfaces.MaxRetries {
			// Wait for the certificate to be in "Ready" state
			// Reference: https://cert-manager.io/docs/usage/certificate/#inner-workings-diagram-for-developers
			log.Printf("Certificate Status: retry attempt %v, after %v, error: %v", try, interfaces.RetryInterval, err)
			time.Sleep(interfaces.RetryInterval)
			err = cm.checkCertificateStatus(secretKey.Name, secretKey.Namespace, ctx)
			if err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
	} else {
		return err
	}

	log.Printf("Certificate Status: %s ready in namespace: %s, validating associated secret...",
		secretKey.Name, secretKey.Namespace)

	err = cm.ValidateCertificateSecret(ctx, secretKey, cfg)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return err
		} else {
			return fmt.Errorf("invalid certificate secret: %s present in namespace: %s, please delete and try again.\nError: %s",
				secretKey.Name, secretKey.Namespace, err)
		}
	}

	log.Printf("Valid certificate secret: %s already present in namespace: %s", secretKey.Name, secretKey.Namespace)
	return nil
}

func (cm *CertManagerProvider) ValidateCertificateSecret(ctx context.Context, secretKey client.ObjectKey,
	_ *interfaces.Config) error {
	var err error
	secret := &corev1.Secret{}
	err = cm.Get(ctx, secretKey, secret)
	if err != nil && k8serrors.IsNotFound(err) {
		for try := range interfaces.MaxRetries {
			// Wait for cert-manager reconciler to create the associated certificate secret
			// Reference: https://cert-manager.io/docs/usage/certificate/#inner-workings-diagram-for-developers
			log.Printf("Valid certificate secret: retry attempt %v, after %v, error: %v", try+1, interfaces.RetryInterval, err)
			time.Sleep(interfaces.RetryInterval)
			err = cm.Get(ctx, secretKey, secret)
			if err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
	} else {
		return err
	}

	certificateData, exists := secret.Data["tls.crt"]
	if !exists {
		return interfaces.ErrTLSCert
	}

	decodeCertificatePem, _ := pem.Decode(certificateData)
	if decodeCertificatePem == nil {
		return interfaces.ErrDecodeCert
	}

	privateKeyData, keyExists := secret.Data["tls.key"]
	if !keyExists {
		return interfaces.ErrTLSKey
	}

	parseCert, err := x509.ParseCertificate(decodeCertificatePem.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	now := time.Now()
	if parseCert.NotBefore.After(now) {
		return interfaces.ErrCertNotYetValid
	}
	if parseCert.NotAfter.Before(now) {
		return interfaces.ErrCertExpired
	}

	privateKey, err := parsePrivateKey(privateKeyData)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	if checkKeyPairErr := checkKeyPair(parseCert, privateKey); checkKeyPairErr != nil {
		return fmt.Errorf("private key does not match certificate: %w", checkKeyPairErr)
	}

	return nil
}

func (cm *CertManagerProvider) DeleteCertificateSecret(ctx context.Context, secretKey client.ObjectKey) error {
	cmCertificate := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretKey.Name,
			Namespace: secretKey.Namespace,
		},
	}
	certStatusErr := cm.checkCertificateStatus(secretKey.Name, secretKey.Namespace, ctx)
	if certStatusErr != nil {
		log.Printf("Certificate associated not ready yet, try again later.")
		return certStatusErr
	}

	dErr := cm.Delete(ctx, cmCertificate)
	if dErr != nil {
		return dErr
	}

	// By default, cert-manager Certificate deletion does not delete the associated secret.
	// Existing secret will allow to services relying on that Certificate, so additionally delete it
	// More info: https://cert-manager.io/docs/usage/certificate/#cleaning-up-secrets-when-certificates-are-deleted
	cmSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretKey.Name,
			Namespace: secretKey.Namespace,
		},
	}
	dSecretErr := cm.Delete(ctx, cmSecret)
	if dSecretErr != nil {
		if k8serrors.IsNotFound(dSecretErr) {
			fmt.Println("Certificate secret not found, maybe already deleted")
		} else {
			return dSecretErr
		}

	}

	return nil
}

// RevokeCertificate is not supported, certificates can only be deleted which is handled by DeleteCertificateSecret
// as per official documentation: https://cert-manager.io/docs/usage/certificate/#inner-workings-diagram-for-developers
func (cm *CertManagerProvider) RevokeCertificate(ctx context.Context, secretKey client.ObjectKey) error {
	return nil
}

func (cm *CertManagerProvider) GetCertificateConfig(ctx context.Context,
	secretKey client.ObjectKey) (*interfaces.Config, error) {
	cmCertificate := &certmanagerv1.Certificate{}
	err := cm.Get(ctx, secretKey, cmCertificate)
	if err != nil {
		return nil, fmt.Errorf("failed to get certificate: %w", err)
	}

	var ipAddresses []net.IP
	if len(cmCertificate.Spec.IPAddresses) != 0 {
		ipAddresses = make([]net.IP, len(cmCertificate.Spec.IPAddresses))
	} else {
		ipAddresses = nil
	}

	cfg := &interfaces.Config{
		CommonName:   cmCertificate.Spec.CommonName,
		Organization: cmCertificate.Spec.Subject.Organizations,
		AltNames: interfaces.AltNames{
			DNSNames: cmCertificate.Spec.DNSNames,
			IPs:      ipAddresses,
		},
		ValidityDuration: cmCertificate.Spec.Duration.Duration,
		ExtraConfig: map[string]any{
			IssuerNameKey: cmCertificate.Spec.IssuerRef.Name,
			IssuerKindKey: cmCertificate.Spec.IssuerRef.Kind,
		},
	}

	return cfg, nil
}

// checkCertificateStatus returns the current status of the certificate creation
func (cm *CertManagerProvider) checkCertificateStatus(certificateName, namespace string, ctx context.Context) error {
	cmCertificate := &certmanagerv1.Certificate{}
	err := cm.Get(ctx, client.ObjectKey{Name: certificateName, Namespace: namespace}, cmCertificate)
	if err != nil {
		return err
	}
	cmStatus := cmCertificate.Status.Conditions
	for _, condition := range cmStatus {
		switch condition.Type {
		case certmanagerv1.CertificateConditionReady:
			log.Printf("Certificate Ready: %v (Reason: %s, Message: %s)\n",
				condition.Status, condition.Reason, condition.Message)
			return nil
		case certmanagerv1.CertificateConditionIssuing:
			return fmt.Errorf("certificate Issuing: %v (Reason: %s, Message: %s), error: %w",
				condition.Status, condition.Reason, condition.Message, interfaces.ErrPending)
		default:
			return fmt.Errorf("certificate status unknown: %v (Reason: %s, Message: %s), error: %w",
				condition.Status, condition.Reason, condition.Message, interfaces.ErrUnknown)
		}
	}
	return nil
}

// checkIssuerExists checks for if the provided issuer is present in the namespace/cluster
func (cm *CertManagerProvider) checkIssuerExists(issuerName, issuerKind, namespace string, ctx context.Context) error {
	switch issuerKind {
	case "Issuer":
		issuer := &certmanagerv1.Issuer{}
		err := cm.Get(ctx, client.ObjectKey{Name: issuerName, Namespace: namespace}, issuer)
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf("issuer %s not found in namespace %s", issuerName, namespace)
		}
	case "ClusterIssuer":
		clusterIssuer := &certmanagerv1.ClusterIssuer{}
		err := cm.Get(ctx, client.ObjectKey{Name: issuerName}, clusterIssuer)
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf("clusterIssuer %s not found", issuerName)
		}
	default:
		return fmt.Errorf("unsupported issuer kind: %s", issuerKind)
	}
	return nil
}

// validateCertificateConfig checks if the config passed is valid
func (cm *CertManagerProvider) validateCertificateConfig(ctx context.Context, namespace string,
	cfg *interfaces.Config) error {
	issuerName, isValid := cfg.ExtraConfig[IssuerNameKey].(string)
	if !isValid {
		return fmt.Errorf("value for %s not correctly provided, try again", IssuerNameKey)
	}
	issuerKind, isValid := cfg.ExtraConfig[IssuerKindKey].(string)
	if !isValid {
		return fmt.Errorf("value for %s not correctly provided, try again", IssuerKindKey)
	}
	checkIssuerExist := cm.checkIssuerExists(issuerName, issuerKind, namespace, ctx)
	if checkIssuerExist != nil {
		return checkIssuerExist
	}
	return nil
}

// createCertificate creates a cert-manager Certificate resource in the specified namespace.
// DNSNames and IPAddresses if not user-defined, will be set to default value in runtime:
// fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", ec.Name, index, ec.Name, ec.Namespace)
// returns an error if the Certificate resource cannot be created.
func (cm *CertManagerProvider) createCertificate(ctx context.Context, secretKey client.ObjectKey,
	cfg *interfaces.Config) error {
	issuerName, isValid := cfg.ExtraConfig[IssuerNameKey].(string)
	if !isValid {
		return fmt.Errorf("value for %s not correctly provided, try again", IssuerNameKey)
	}
	issuerKind, isValid := cfg.ExtraConfig[IssuerKindKey].(string)
	if !isValid {
		return fmt.Errorf("value for %s not correctly provided, try again", IssuerKindKey)
	}

	certificateResource := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretKey.Name,
			Namespace: secretKey.Namespace,
		},
		Spec: certmanagerv1.CertificateSpec{
			CommonName: cfg.CommonName,
			Subject: &certmanagerv1.X509Subject{
				Organizations: cfg.Organization,
			},
			SecretName:  secretKey.Name,
			DNSNames:    cfg.AltNames.DNSNames,
			IPAddresses: strings.Fields(strings.Trim(fmt.Sprint(cfg.AltNames.IPs), "[]")),
			IssuerRef: cmmeta.IssuerReference{
				Name: issuerName,
				Kind: issuerKind,
			},
			Duration: &metav1.Duration{Duration: cfg.ValidityDuration},
		},
	}

	return cm.Create(ctx, certificateResource)
}

// parsePrivateKey parses the private key from the PEM-encoded data.
func parsePrivateKey(privateKeyData []byte) (crypto.PrivateKey, error) {
	block, _ := pem.Decode(privateKeyData)
	if block == nil {
		return nil, errors.New("failed to decode private key: invalid PEM")
	}

	// Parse the private key from the PEM block
	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// Parse the private key in another format (e.g., RSA)
		privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}
	}

	return privateKey, nil
}

// checkKeyPair checks if the private key matches the certificate by validating the public key
func checkKeyPair(cert *x509.Certificate, privateKey crypto.PrivateKey) error {
	switch key := privateKey.(type) {
	case *rsa.PrivateKey:
		pub, ok := cert.PublicKey.(*rsa.PublicKey)
		if !ok || !key.PublicKey.Equal(pub) {
			return interfaces.ErrRSAKeyPair
		}
	case *ecdsa.PrivateKey:
		pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
		if !ok || !key.PublicKey.Equal(pub) {
			return interfaces.ErrECDSAKeyPair
		}
	case *ed25519.PrivateKey:
		pub, ok := cert.PublicKey.(ed25519.PublicKey)
		if !ok || !bytes.Equal(key.Public().(ed25519.PublicKey), pub) {
			return interfaces.ErrED25519KeyPair
		}
	default:
		return fmt.Errorf("unsupported private key type: %T", key)
	}

	return nil
}
