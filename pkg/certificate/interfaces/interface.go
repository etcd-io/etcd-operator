/*
 * Copyright 2025.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package certificate

import (
	"context"
	"net"
	"time"
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
	CABundle         []byte

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
	// - secretName: Name of the Secret to ensure.
	// - namespace: Namespace where the Secret should reside.
	// - cfg: Configuration for the certificate.
	//
	// Returns:
	// - nil if the operation succeeds, or an error otherwise.
	EnsureCertificateSecret(ctx context.Context, secretName string, namespace string, cfg *Config) error

	// ValidateCertificateSecret validates the certificate stored
	// in the specified Secret. This checks if the certificate is
	// valid (e.g., not expired, matches configuration).
	//
	// Parameters:
	// - ctx: Context for cancellation and deadlines.
	// - secretName: Name of the Secret to validate.
	// - namespace: Namespace where the Secret resides.
	// - cfg: Configuration to validate against.
	//
	// Returns:
	// - true if the Secret is valid, false otherwise, along with
	//   an error if validation fails.
	ValidateCertificateSecret(ctx context.Context, secretName string, namespace string, cfg *Config) (bool, error)

	// DeleteCertificateSecret explicitly deletes the Secret containing
	// the certificate. This should only be used if the certificate
	// is no longer needed.
	//
	// Parameters:
	// - ctx: Context for cancellation and deadlines.
	// - secretName: Name of the Secret to delete.
	// - namespace: Namespace where the Secret resides.
	//
	// Returns:
	// - nil if the operation succeeds, or an error otherwise.
	DeleteCertificateSecret(ctx context.Context, secretName string, namespace string) error

	// RevokeCertificate revokes a certificate if supported by the provider.
	//
	// Parameters:
	// - ctx: Context for cancellation and deadlines.
	// - secretName: Name of the Secret containing the certificate to revoke.
	// - namespace: Namespace where the Secret resides.
	//
	// Returns:
	// - nil if the revocation succeeds, or an error otherwise.
	RevokeCertificate(ctx context.Context, secretName string, namespace string) error

	// GetCertificateConfig returns the certificate configuration from the provider.
	//
	// Parameters:
	// - secretName: Name of the Secret containing the certificate.
	// - namespace: Namespace where the Secret resides.
	//
	// Returns:
	// - Config if the Secret exists and is valid, or an error otherwise.
	GetCertificateConfig(ctx context.Context, secretName string, namespace string) (*Config, error)
}
