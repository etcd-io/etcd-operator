package certificate

import (
	"context"
	"errors"
	"time"

	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	// ErrCertNotYetValid is returned when certificate is not yet valid
	ErrCertNotYetValid = errors.New("certificate is not yet valid")

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

	// DefaultAutoValidity is the default validity duration for auto-generated certificates (365 days)
	DefaultAutoValidity = 365 * 24 * time.Hour

	// DefaultCertManagerValidity is the default validity duration for cert-manager certificates (90 days)
	DefaultCertManagerValidity = 90 * 24 * time.Hour

	// DefaultDomainName is the default domain name for creating certificates
	DefaultDomainName = "svc.cluster.local"
)

// Config contains the basic fields required for creating a certificate
type Config struct {
	CommonName    string
	Organizations []string
	DNSNames      []string
	// IPAddresses are IP subject alternative names as literal IP strings.
	IPAddresses []string
	// Duration is the requested certificate lifetime, already resolved by the
	// caller (providers do not apply their own default).
	Duration time.Duration
	// RenewBefore is passed through to providers that support renewal;
	// nil means the provider default.
	RenewBefore *metav1.Duration
	// IssuerRef selects the cert-manager issuer signing the certificate.
	// It is nil for providers that mint their own certificates.
	IssuerRef *cmmeta.IssuerReference
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
