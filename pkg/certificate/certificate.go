package certificate

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"go.etcd.io/etcd-operator/pkg/certificate/cert_manager"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

type ProviderType string

const (
	Auto        ProviderType = "auto"
	CertManager ProviderType = "cert-manager"
	// add more ...
)

func NewProvider(pt ProviderType, c client.Client) (certInterface.Provider, error) {
	switch pt {
	case Auto:
		return nil, nil // change me later
	case CertManager:
		return cert_manager.New(c), nil // change me later
	}

	return nil, fmt.Errorf("unknown provider type: %s", pt)
}
