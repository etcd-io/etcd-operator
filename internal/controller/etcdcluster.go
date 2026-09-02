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

// clusterHashInput is the explicit set of EtcdClusterSpec fields that
// determine the etcd Pod's configuration. Only these fields participate in
// EtcdClusterHash; a new spec field must be added here deliberately to
// become part of the hash. Fields intentionally left out:
//   - Size: scaling is handled separately from configuration.
//   - Version: upgrades roll one member at a time elsewhere.
//   - PodTemplate.Metadata: applied in place without a Pod roll.
//   - Paused, AllowVersionUpgradeOnRepair: operator knobs, not Pod config.
//
// Because the input is this private struct rather than EtcdClusterSpec
// itself, cosmetic API changes (renaming unrelated fields, adding new
// operator knobs) cannot change the hash; only changes here or to a
// whitelisted nested type do.
type clusterHashInput struct {
	ImageRegistry string
	StorageSpec   *v1alpha1.StorageSpec
	TLS           *v1alpha1.TLSCertificate
	EtcdOptions   []string
	PodSpec       *v1alpha1.PodSpec
}

// EtcdClusterHash returns a stable hash over the fields of the cluster spec
// that determine the etcd Pod's configuration (see clusterHashInput).
// Changing any other spec field must not change the hash, so toggling
// operator knobs or scaling the cluster never marks Pods as drifted; in
// particular a PodTemplate without a Spec — metadata only — hashes like no
// PodTemplate at all.
//
// NOTE: changing this function or any whitelisted nested type (StorageSpec,
// TLSCertificate, PodSpec) changes every hash output. After an operator
// upgrade, every existing Pod's stored hash annotation would then look
// drifted, rolling the whole cluster once. TestEtcdClusterHashPinned fails
// on such a change; when it does, revisit this function before updating its
// pinned expected value.
func EtcdClusterHash(ec *v1alpha1.EtcdCluster) string {
	input := clusterHashInput{
		ImageRegistry: ec.Spec.ImageRegistry,
		StorageSpec:   ec.Spec.StorageSpec,
		TLS:           ec.Spec.TLS,
		// Hash the effective argument list: operator default arguments
		// (including the TLS-dependent ones) merged with the user's options,
		// so the hash reflects what the etcd process actually runs.
		EtcdOptions: createArgs(ec.Name, ec.Spec.EtcdOptions, clusterTLSEnabled(ec)),
	}
	if ec.Spec.PodTemplate != nil {
		// Only Spec rolls Pods; Metadata is applied in place. A nil Spec
		// (or a metadata-only PodTemplate) hashes like no PodTemplate.
		input.PodSpec = ec.Spec.PodTemplate.Spec
	}
	deterministicString := dump.ForHash(input)
	hasher := sha256.New()
	hasher.Write([]byte(deterministicString))
	return hex.EncodeToString(hasher.Sum(nil))[:hashLength]
}
