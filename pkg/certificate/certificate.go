package certificate

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"go.etcd.io/etcd-operator/pkg/certificate/auto"
	certManager "go.etcd.io/etcd-operator/pkg/certificate/cert_manager"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

type ProviderType string

const (
	Auto        ProviderType = "auto"
	CertManager ProviderType = "cert-manager"
	// add more ...
)

func NewProvider(pt ProviderType, c client.Client, scheme *runtime.Scheme) (certInterface.Provider, error) {
	switch pt {
	case Auto:
		return auto.New(c, scheme), nil
	case CertManager:
		return certManager.New(c, scheme), nil
	}

	return nil, fmt.Errorf("unknown provider type: %s", pt)
}
