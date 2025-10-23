package auto

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	interfaces "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

type AutoCertProvider struct {
	client.Client
}

var _ interfaces.Provider = (*AutoCertProvider)(nil)

func New(c client.Client) interfaces.Provider {
	return &AutoCertProvider{
		c,
	}
}

func (ac *AutoCertProvider) EnsureCertificateSecret(ctx context.Context, secretName, namespace string,
	cfg *interfaces.Config) error {
	return nil
}

func (ac *AutoCertProvider) ValidateCertificateSecret(ctx context.Context, secretName, namespace string,
	_ *interfaces.Config) error {
	return nil
}

func (ac *AutoCertProvider) DeleteCertificateSecret(ctx context.Context, secretName, namespace string) error {
	return nil
}

func (ac *AutoCertProvider) RevokeCertificate(ctx context.Context, secretName string, namespace string) error {
	return nil
}

func (ac *AutoCertProvider) GetCertificateConfig(ctx context.Context,
	secretName, namespace string) (*interfaces.Config, error) {
	return &interfaces.Config{}, nil
}
