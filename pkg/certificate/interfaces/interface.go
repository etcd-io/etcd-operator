package certificate

import (
	"context"
	"errors"
	"net"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	// ErrPending is returned when the Certificate is not in "Ready" state
	ErrPending = errors.New("certificate creation pending")

	// ErrUnknown is returned when the Certificate status does not match the provider defined states
	ErrUnknown = errors.New("certificate status unknown")

	// ErrTLSKey is returned when private key not found in Certificate secret
	ErrTLSKey = errors.New("private key not found in secret")

	// ErrTLSCert is returned when private key certificate not found in Certificate secret
	ErrTLSCert = errors.New("certificate not found in secret")

	// ErrDecodeCert is returned when failed to decode PEM block of tls.crt of Certificate secret
	ErrDecodeCert = errors.New("failed to decode PEM block")

	// ErrCertExpired is returned when certificate has expired
	ErrCertExpired = errors.New("certificate has expired")

	// ErrRSAKeyPair is returned when private key(RSA) does not match the public key in the Certificate secret
	ErrRSAKeyPair = errors.New("private key(RSA) does not match the public key in the certificate")

	// ErrECDSAKeyPair is returned when private key(ECDSA) does not match the public key in the Certificate secret
	ErrECDSAKeyPair = errors.New("private key(ECDSA) does not match the public key in the certificate")

	// ErrED25519KeyPair is returned when private key(ED25519) does not match the public key in the Certificate secret
	ErrED25519KeyPair = errors.New("private key(ED25519) does not match the public key in the certificate")
)

const (
	// MaxRetries is the maximum number of retry attempts for EnsureCertificateSecret, ValidateCertificateSecret
	// with a delay of RetryInterval between consecutive retries
	MaxRetries    = 36
	RetryInterval = 5 * time.Second
)

// AltNames contains the domain names and IP addresses that will be added
// to the x509 certificate SubAltNames fields. The values will be passed
// directly to the x509.Certificate object.
type AltNames struct {
	DNSNames []string
	IPs      []net.IP
}

// Config contains the basic fields required for creating a certificate
type Config struct {
	CommonName       string
	Organization     []string
	AltNames         AltNames
	ValidityDuration time.Duration
	CABundleSecret   string

	// ExtraConfig contains provider specific configurations.
	ExtraConfig map[string]any
}

type Provider interface {
	// EnsureCertificateSecret ensures the specified certificate is
	// available as a Secret in Kubernetes. If the Secret does not
	// exist, it will be created.
	//
	// Parameters:
	// - ctx: Context for cancellation and deadlines.
	// - secretKey: ObjectKey containing the name and namespace of the Secret to ensure.
	// - cfg: Configuration for the certificate.
	//
	// Returns:
	// - nil if the operation succeeds, or an error otherwise.
	EnsureCertificateSecret(ctx context.Context, secretKey client.ObjectKey, cfg *Config) error

	// ValidateCertificateSecret validates the certificate stored
	// in the specified Secret. This checks if the certificate is
	// valid (e.g., not expired, matches configuration).
	//
	// Parameters:
	// - ctx: Context for cancellation and deadlines.
	// - secretKey: ObjectKey containing the name and namespace of the Secret to validate.
	// - cfg: Configuration to validate against.
	//
	// Returns:
	// - nil if the Secret is valid, otherwise returns
	//   an error if validation fails.
	ValidateCertificateSecret(ctx context.Context, secretKey client.ObjectKey, cfg *Config) error

	// DeleteCertificateSecret explicitly deletes the Secret containing
	// the certificate. This should only be used if the certificate
	// is no longer needed.
	//
	// Parameters:
	// - ctx: Context for cancellation and deadlines.
	// - secretKey: ObjectKey containing the name and namespace of the Secret to delete.
	//
	// Returns:
	// - nil if the operation succeeds, or an error otherwise.
	DeleteCertificateSecret(ctx context.Context, secretKey client.ObjectKey) error

	// RevokeCertificate revokes a certificate if supported by the provider.
	//
	// Parameters:
	// - ctx: Context for cancellation and deadlines.
	// - secretKey: ObjectKey containing the name and namespace of the Secret containing the certificate to revoke.
	//
	// Returns:
	// - nil if the revocation succeeds, or an error otherwise.
	RevokeCertificate(ctx context.Context, secretKey client.ObjectKey) error

	// GetCertificateConfig returns the certificate configuration from the provider.
	//
	// Parameters:
	// - ctx: Context for cancellation and deadlines.
	// - secretKey: ObjectKey containing the name and namespace of the Secret containing the certificate.
	//
	// Returns:
	// - Config if the Secret exists and is valid, or an error otherwise.
	GetCertificateConfig(ctx context.Context, secretKey client.ObjectKey) (*Config, error)
}
