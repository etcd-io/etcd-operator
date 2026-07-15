package controller

import (
	"crypto/sha256"
	"encoding/hex"

	"k8s.io/utils/dump"

	"go.etcd.io/etcd-operator/api/v1alpha1"
)

const (
	hashLength = 12
)

func EtcdClusterHash(ec *v1alpha1.EtcdCluster) string {
	clusterSpec := ec.DeepCopy().Spec
	// size is not used for calculating the hash
	clusterSpec.Size = 0
	// version is handled separately
	clusterSpec.Version = ""
	clusterSpec.EtcdOptions = createArgs(ec.Name, ec.Spec.EtcdOptions, clusterTLSEnabled(ec))
	// we don't want to roll etcd pods when metadata is changed.
	// instead, we'd do in-place update for them.
	if clusterSpec.PodTemplate != nil {
		clusterSpec.PodTemplate.Metadata = nil
	}
	deterministicString := dump.ForHash(clusterSpec)
	hasher := sha256.New()
	hasher.Write([]byte(deterministicString))
	return hex.EncodeToString(hasher.Sum(nil))[:hashLength]
}
