package auto

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
	"go.etcd.io/etcd/client/pkg/v3/transport"
)

const (
	DefaultValidity = 365 * 24 * time.Hour
)

type Provider struct {
	client.Client
}

var _ interfaces.Provider = (*Provider)(nil)

func New(c client.Client) interfaces.Provider {
	return &Provider{
		c,
	}
}

func (ac *Provider) EnsureCertificateSecret(ctx context.Context, secretName, namespace string,
	cfg *interfaces.Config) error {
	var secret corev1.Secret
	err := ac.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, &secret)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			err := ac.createNewSecret(ctx, secretName, namespace, cfg)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	err = ac.ValidateCertificateSecret(ctx, secretName, namespace, cfg)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return err
		} else {
			return fmt.Errorf("invalid certificate secret: %s present in namespace: %s, please delete and try again.\nError: %w",
				secretName, namespace, err)
		}
	}

	log.Printf("Valid certificate secret: %s already present in namespace: %s", secretName, namespace)
	return nil
}

func (ac *Provider) ValidateCertificateSecret(ctx context.Context, secretName, namespace string,
	_ *interfaces.Config) error {
	var err error
	secret := &corev1.Secret{}
	err = ac.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, secret)
	if err != nil && k8serrors.IsNotFound(err) {
		for try := range interfaces.MaxRetries {
			// Wait for the certificate secret to get created
			log.Printf("Valid certificate secret: retry attempt %v, after %v, error: %v", try+1, interfaces.RetryInterval, err)
			time.Sleep(interfaces.RetryInterval)
			err = ac.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, secret)
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

	if parseCert.NotAfter.Before(time.Now()) {
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

func (ac *Provider) DeleteCertificateSecret(ctx context.Context, secretName, namespace string) error {
	var secret corev1.Secret
	err := ac.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, &secret)
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return ac.Delete(ctx, &secret)
}

func (ac *Provider) RevokeCertificate(ctx context.Context, secretName string, namespace string) error {
	return ac.DeleteCertificateSecret(ctx, secretName, namespace)
}

func (ac *Provider) GetCertificateConfig(ctx context.Context,
	secretName, namespace string) (*interfaces.Config, error) {
	var autoCertSecret corev1.Secret
	err := ac.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, &autoCertSecret)
	if err != nil {
		return nil, fmt.Errorf("failed to get certificate: %w", err)
	}

	return &interfaces.Config{}, nil
}

// parsePrivateKey parses the private key from the PEM-encoded data.
func parsePrivateKey(privateKeyData []byte) (crypto.PrivateKey, error) {
	block, _ := pem.Decode(privateKeyData)
	if block == nil {
		return nil, errors.New("failed to decode private key: invalid PEM")
	}

	// Parse the private key from the PEM block
	privateKey, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
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

// createNewSecret generates or updates a Kubernetes TLS Secret
// with a self-signed certificate in the specified namespace.
// DNSNames and IPAddresses if not user-defined, will be set to default value in runtime:
// fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", ec.Name, index, ec.Name, ec.Namespace)
// minimum validityDuration is 365 days
// https://github.com/etcd-io/etcd/blob/b87bc1c3a275d7d4904f4d201b963a2de2264f0d/client/pkg/transport/listener.go#L275
func (ac *Provider) createNewSecret(ctx context.Context, secretName, namespace string,
	cfg *interfaces.Config) error {
	validity := DefaultValidity
	if cfg.ValidityDuration != 0 {
		validity = cfg.ValidityDuration * time.Hour
	}

	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("etcd-auto-cert-%s-*", secretName))
	if err != nil {
		return fmt.Errorf("failed to create temp dir for certificate generation: %w", err)
	}

	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	var hosts []string
	if cfg != nil {
		if cfg.CommonName != "" {
			hosts = append(hosts, cfg.CommonName)
		}
		if len(cfg.AltNames.DNSNames) > 0 {
			hosts = append(hosts, cfg.AltNames.DNSNames...)
		}
	}

	tlsInfo, selfCertErr := transport.SelfCert(zap.NewNop(), tmpDir, hosts, uint(validity/DefaultValidity))
	if selfCertErr != nil {
		return fmt.Errorf("certificate creation via transport.SelfCert failed: %w", selfCertErr)
	}

	certPath := tlsInfo.CertFile
	keyPath := tlsInfo.KeyFile
	caPath := tlsInfo.TrustedCAFile

	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return fmt.Errorf("failed to read generated cert file %s: %w", certPath, err)
	}

	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("failed to read generated key file %s: %w", keyPath, err)
	}

	caPEM, err := os.ReadFile(caPath)
	if err != nil || len(caPEM) == 0 {
		// use certPEM when CA file is not found or empty
		caPEM = certPEM
	}

	// Validate cert and key pair
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		return fmt.Errorf("generated keypair invalid: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"tls.crt": certPEM,
			"tls.key": keyPEM,
			"ca.crt":  caPEM,
		},
	}

	// Create or Update certificate Secret
	var existing corev1.Secret
	getErr := ac.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, &existing)

	if k8serrors.IsNotFound(getErr) {
		createErr := ac.Create(ctx, secret)
		if createErr != nil {
			return fmt.Errorf("failed to create secret: %w", err)
		}
	} else if k8serrors.IsAlreadyExists(getErr) {
		log.Printf("Certificate secret: %s already present, updating...", secretName)
		existing.Data = secret.Data
		if updateErr := ac.Update(ctx, &existing); updateErr != nil {
			return fmt.Errorf("failed to update existing secret: %w", updateErr)
		}
	} else {
		return fmt.Errorf("failed to create secret: %w", err)
	}

	return nil
}
