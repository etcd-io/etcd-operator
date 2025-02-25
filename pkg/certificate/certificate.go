package certificate

import (
	"fmt"

	certManager "go.etcd.io/etcd-operator/pkg/certificate/cert-manager"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

type ProviderType string

const (
	Auto        ProviderType = "auto"
	CertManager ProviderType = "cert-manager"
	// add more ...
)

func NewProvider(pt ProviderType) (certInterface.Provider, error) {
	switch pt {
	case Auto:
		return nil, nil // change me later
	case CertManager:
		return &certManager.CertManagerProvider{}, nil // change me later
	}

	return nil, fmt.Errorf("unknown provider type: %s", pt)
}
