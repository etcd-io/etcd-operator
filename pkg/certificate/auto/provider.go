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
	"net"
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
	// oneYearInHours is used to convert validity duration to years for transport.SelfCert.
	// transport.SelfCert expects the validity parameter to be in years (uint).
	oneYearInHours = 365 * 24 * time.Hour
)

type Provider struct {
	client.Client
	config *interfaces.Config
}

var _ interfaces.Provider = (*Provider)(nil)

func New(c client.Client) interfaces.Provider {
	return &Provider{
		Client: c,
		config: nil,
	}
}

func (ac *Provider) EnsureCertificateSecret(ctx context.Context, secretKey client.ObjectKey,
	cfg *interfaces.Config) error {
	// Save the user defined AutoConfig so that it can be returned from GetCertificateConfig
	ac.config = cfg

	var secret corev1.Secret
	err := ac.Get(ctx, secretKey, &secret)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			err := ac.createNewSecret(ctx, secretKey, cfg)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	err = ac.ValidateCertificateSecret(ctx, secretKey, cfg)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return err
		} else {
			return fmt.Errorf("invalid certificate secret: %s present in namespace: %s, please delete and try again.\nError: %w",
				secretKey.Name, secretKey.Namespace, err)
		}
	}

	log.Printf("Valid certificate secret: %s already present in namespace: %s", secretKey.Name, secretKey.Namespace)
	return nil
}

func (ac *Provider) ValidateCertificateSecret(ctx context.Context, secretKey client.ObjectKey,
	_ *interfaces.Config) error {
	var err error
	secret := &corev1.Secret{}
	err = ac.Get(ctx, secretKey, secret)
	if err != nil && k8serrors.IsNotFound(err) {
		for try := range interfaces.MaxRetries {
			// Wait for the certificate secret to get created
			log.Printf("Valid certificate secret: retry attempt %v, after %v, error: %v", try+1, interfaces.RetryInterval, err)
			time.Sleep(interfaces.RetryInterval)
			err = ac.Get(ctx, secretKey, secret)
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

func (ac *Provider) DeleteCertificateSecret(ctx context.Context, secretKey client.ObjectKey) error {
	var secret corev1.Secret
	if err := ac.Get(ctx, secretKey, &secret); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return ac.Delete(ctx, &secret)
}

func (ac *Provider) RevokeCertificate(ctx context.Context, secretKey client.ObjectKey) error {
	return ac.DeleteCertificateSecret(ctx, secretKey)
}

func (ac *Provider) GetCertificateConfig(ctx context.Context,
	secretKey client.ObjectKey) (*interfaces.Config, error) {
	var autoCertSecret corev1.Secret
	err := ac.Get(ctx, secretKey, &autoCertSecret)
	if err != nil {
		return nil, fmt.Errorf("failed to get certificate: %w", err)
	}

	if ac.config != nil {
		return ac.config, nil
	}

	return nil, fmt.Errorf("failed to get user-defined autoConfig")
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
		return nil, fmt.Errorf("failed to parse private key: %w", err)
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
func (ac *Provider) createNewSecret(ctx context.Context, secretKey client.ObjectKey,
	cfg *interfaces.Config) error {
	validity := interfaces.DefaultAutoValidity
	if cfg.ValidityDuration != 0 {
		validity = cfg.ValidityDuration
	}

	// Validate validity duration: minimum is 1 year as required by etcd
	if validity < oneYearInHours {
		return fmt.Errorf("validity duration must be at least 1 year (365 days), got: %v", validity)
	}

	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("etcd-auto-cert-%s-*", secretKey.Name))
	if err != nil {
		return fmt.Errorf("failed to create temp dir for certificate generation: %w", err)
	}

	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	var hosts []string
	if cfg != nil {
		// hostPort is formed by joining the hostname(CommonName/DNSNames) with a
		// dummy port "5500" to avoid unhandled parsing error in transport.SelfCert()
		// using net.SplitHostPort() which expects complete hostname with port
		// https://github.com/etcd-io/etcd/blob/19779eea01337dbcc3768b6383068cd0d78b06c7/client/pkg/transport/listener.go#L289
		if cfg.CommonName != "" {
			hostPort := net.JoinHostPort(cfg.CommonName, "5500")
			hosts = append(hosts, hostPort)
		}
		if len(cfg.AltNames.DNSNames) > 0 {
			for _, dnsName := range cfg.AltNames.DNSNames {
				hostPort := net.JoinHostPort(dnsName, "5500")
				hosts = append(hosts, hostPort)
			}
		}
	}

	log.Printf("calling SelfCert with hosts: %v", hosts)

	// Convert validity duration to years for transport.SelfCert
	// transport.SelfCert expects the validity parameter to be in years (uint)
	validityYears := uint(validity / oneYearInHours)
	if validityYears == 0 {
		// This should not happen due to the earlier check, but adding as a safeguard
		return fmt.Errorf("validity duration converts to 0 years, must be at least 1 year")
	}

	tlsInfo, selfCertErr := transport.SelfCert(zap.NewNop(), tmpDir, hosts, validityYears)
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
		// Use certPEM when CA file is not found or empty
		caPEM = certPEM
	}

	// Validate cert and key pair
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		return fmt.Errorf("generated keypair is invalid: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretKey.Name,
			Namespace: secretKey.Namespace,
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
	getErr := ac.Get(ctx, secretKey, &existing)

	if k8serrors.IsNotFound(getErr) {
		createErr := ac.Create(ctx, secret)
		if createErr != nil {
			return fmt.Errorf("failed to create secret: %w", err)
		}
	} else if k8serrors.IsAlreadyExists(getErr) {
		log.Printf("Certificate secret: %s already present, updating...", secretKey.Name)
		existing.Data = secret.Data
		if updateErr := ac.Update(ctx, &existing); updateErr != nil {
			return fmt.Errorf("failed to update existing secret: %w", updateErr)
		}
	} else {
		return fmt.Errorf("failed to create secret: %w", err)
	}

	return nil
}
