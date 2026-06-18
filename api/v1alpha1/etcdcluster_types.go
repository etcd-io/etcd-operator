/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"net"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// EtcdClusterSpec defines the desired state of EtcdCluster.
type EtcdClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Size is the expected size of the etcd cluster.
	// +kubebuilder:validation:Minimum=1
	Size int `json:"size"`
	// ImageRegistry specifies the container registry that hosts the etcd images.
	// If unset, it defaults to the value provided via the controller's
	// --image-registry flag, which itself defaults to "gcr.io/etcd-development/etcd".
	ImageRegistry string `json:"imageRegistry,omitempty"`
	// Version is the expected version of the etcd container image.
	Version string `json:"version"`
	// StorageSpec is the name of the StorageSpec to use for the etcd cluster. If not provided, then each POD just uses the temporary storage inside the container.
	StorageSpec *StorageSpec `json:"storageSpec,omitempty"`
	// TLS configures etcd's two independent TLS surfaces (peer and client/server).
	// Each surface is optional and configured fully independently; a nil surface
	// means that surface is served/dialed in cleartext. When TLS itself is nil, the
	// entire cluster (peer + client + operator client) is cleartext, byte-identical
	// to a TLS-free deployment.
	TLS *EtcdClusterTLS `json:"tls,omitempty"`
	// etcd configuration options are passed as command line arguments to the etcd container, refer to etcd documentation for configuration options applicable for the version of etcd being used.
	EtcdOptions []string `json:"etcdOptions,omitempty"`
	// PodTemplate is the pod template to use for the etcd cluster.
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`
}

type PodTemplate struct {
	// Metadata is the metadata to add to the pod.
	Metadata *PodMetadata `json:"metadata,omitempty"`
	Spec     *PodSpec     `json:"spec,omitempty"`
}

type PodSpec struct {
	Affinity     *corev1.Affinity    `json:"affinity,omitempty"`
	NodeSelector map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
}

type PodMetadata struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// EtcdClusterTLS configures etcd's two independent TLS surfaces. Each surface is
// optional; a nil surface means that surface is served/dialed in cleartext (http).
// The two surfaces are configured fully independently -- different providers,
// issuers, and client-cert-auth policy are allowed and expected. Both surfaces nil
// is legal and means fully-cleartext (today's default); it is intentional, not an
// error, so there is no "at least one surface" validation.
type EtcdClusterTLS struct {
	// Peer configures etcd<->etcd (peer) TLS. When nil, peer traffic is cleartext.
	// A configured peer surface REQUIRES a CA-capable issuer shared by all members
	// so members can mutually verify; a self-signed *leaf* issuer cannot form a
	// multi-member cluster. (CA-capability lives on the cert-manager Issuer object,
	// not on this spec, so that check is enforced at reconcile time, not via CEL.)
	// +optional
	Peer *TLSSurface `json:"peer,omitempty"`

	// Client configures client->etcd (server) TLS AND, transitively, the operator's
	// own etcd client identity (the operator authenticates to etcd as a client).
	// When nil, client traffic is cleartext and the operator dials cleartext.
	// +optional
	Client *TLSSurface `json:"client,omitempty"`
}

// TLSSurface is the full, independent TLS configuration for ONE surface (peer or
// client). It carries its own provider, provider config (issuer), and mutual
// client-cert-auth policy.
//
// The two XValidation rules below are the apply-time anti-misconfiguration
// guardrails (plan Decision 2.1/2.2): they reject incoherent provider/config
// combinations and mTLS-without-a-resolvable-CA at the API server, so a user
// "cannot misconfigure" these from the spec alone. Rules that require reading
// cluster objects (issuer existence, peer CA-capability, client/server CA match)
// cannot be expressed in CEL and are enforced at reconcile time instead (plan
// Decision 2.5-2.7, Decision 3) -- see validateTLSSurface and the cert-manager
// provider's validateCertificateConfig.
//
// +kubebuilder:validation:XValidation:rule="self.provider != 'cert-manager' || has(self.providerCfg.certManagerCfg)",message="provider 'cert-manager' requires providerCfg.certManagerCfg"
// +kubebuilder:validation:XValidation:rule="self.provider == 'cert-manager' || !has(self.providerCfg.certManagerCfg)",message="providerCfg.certManagerCfg may only be set when provider is 'cert-manager'"
// +kubebuilder:validation:XValidation:rule="!self.clientCertAuth || self.provider != 'cert-manager' || (has(self.providerCfg.certManagerCfg) && size(self.providerCfg.certManagerCfg.issuerName) > 0)",message="clientCertAuth requires a trusted CA: set providerCfg.certManagerCfg.issuerName"
type TLSSurface struct {
	// Provider selects the certificate provider for THIS surface.
	// Defaults to "auto" when empty.
	// +kubebuilder:validation:Enum=auto;cert-manager
	// +optional
	Provider string `json:"provider,omitempty"`

	// ProviderCfg is the provider-specific config for THIS surface.
	// +optional
	ProviderCfg ProviderConfig `json:"providerCfg,omitempty"`

	// ClientCertAuth toggles mutual cert auth for THIS surface (etcd's
	// --client-cert-auth for the client surface, --peer-client-cert-auth for the
	// peer surface). Defaults to true (mTLS). Set false to serve server-only TLS
	// where clients authenticate by other means (password/token). When true with
	// the cert-manager provider a trusted CA (issuerName) is REQUIRED, enforced by
	// the XValidation rule above.
	// +kubebuilder:default=true
	// +optional
	ClientCertAuth *bool `json:"clientCertAuth,omitempty"`
}

type ProviderConfig struct {
	AutoCfg        *ProviderAutoConfig        `json:"autoCfg,omitempty"`
	CertManagerCfg *ProviderCertManagerConfig `json:"certManagerCfg,omitempty"`
}

type AltNames struct {
	// DNSNames is the expected array of DNS subject alternative names.
	// if empty defaults to $(POD_NAME).$(ETCD_CLUSTER_NAME).$(POD_NAMESPACE).svc.cluster.local
	// +optional
	DNSNames []string `json:"dnsNames,omitempty"`

	// IPs is the expected array of IP address subject alternative names.
	// +optional
	IPs []net.IP `json:"ipAddresses,omitempty"`
}

type CommonConfig struct {
	// CommonName is the expected common name X509 certificate subject attribute.
	// Should have a length of 64 characters or fewer to avoid generating invalid CSRs.
	// +optional
	CommonName string `json:"commonName,omitempty"`

	// Organization is the expected array of Organization names to be used on the Certificate.
	// +optional
	Organization []string `json:"organizations,omitempty"`

	// AltNames contains the domain names and IP addresses that will be added
	// to the x509 certificate SubAltNames fields. The values will be passed
	// directly to the x509.Certificate object.
	AltNames AltNames `json:"altNames,omitempty"`

	// ValidityDuration is the expected duration until which the certificate will be valid,
	// expects in human-readable duration: 100d12h, if empty defaults to 90d for cert-manager
	// and 365d for auto as per: https://github.com/etcd-io/etcd/blob/b87bc1c3a275d7d4904f4d201b963a2de2264f0d/client/pkg/transport/listener.go#L275
	// +optional
	ValidityDuration string `json:"validityDuration,omitempty"`
}

type ProviderAutoConfig struct {
	// CommonConfig is the struct of common fields required to create a certificate
	CommonConfig `json:",inline"`
}

type ProviderCertManagerConfig struct {
	// CommonConfig is the struct of common fields required to create a certificate
	CommonConfig `json:",inline"`

	// IssuerKind is the expected kind of Issuer, either "ClusterIssuer" or "Issuer".
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	IssuerKind string `json:"issuerKind"`

	// IssuerName is the expected name of Issuer required to issue a certificate
	IssuerName string `json:"issuerName"`

	// IssuerGroup is the API group of the issuer referenced by IssuerKind/IssuerName.
	// Empty defaults to "cert-manager.io". Set this to target issuers served by an
	// external/intermediate issuer group (e.g. an out-of-tree CA controller).
	// +optional
	IssuerGroup string `json:"issuerGroup,omitempty"`
}

// EtcdClusterStatus defines the observed state of EtcdCluster.
type EtcdClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// ObservedGeneration is the most recent generation observed for this EtcdCluster by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// CurrentReplicas is the number of etcd pods managed by the StatefulSet for this cluster.
	// This reflects the .spec.replicas of the underlying StatefulSet.
	// +optional
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`

	// ReadyReplicas is the number of etcd pods managed by the StatefulSet that are currently ready.
	// This reflects the .status.readyReplicas of the underlying StatefulSet.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// MemberCount is the number of members currently registered in the etcd cluster,
	// as reported by the etcd 'member list' API. This may differ from CurrentReplicas
	// during scaling operations or if members are added/removed outside the operator's direct control.
	// +optional
	MemberCount int32 `json:"memberCount,omitempty"`

	// CurrentVersion is the observed etcd version of the cluster.
	// This is typically derived from the version of the healthy leader or a consensus among healthy members.
	// +optional
	CurrentVersion string `json:"currentVersion,omitempty"`

	// LeaderID is the hex-encoded ID of the current etcd cluster leader, if one exists and is known.
	// +optional
	LeaderID string `json:"leaderID,omitempty"`

	// TODO: expose LastDefragTime once the controller owns automated defragmentation.

	// Members provides the status of each individual etcd member.
	// +optional
	// +listType=map
	// +listMapKey=id
	// Alternative listMapKey could be 'name' if 'id' is not always immediately available or stable during init.
	// However, 'id' is more canonical once a member is part of the cluster.
	Members []MemberStatus `json:"members,omitempty"`

	// Conditions represent the latest available observations of the EtcdCluster's state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// MemberStatus defines the observed state of a single etcd member.
type MemberStatus struct {
	// Name of the etcd member, typically the pod name (e.g., "etcd-cluster-example-0").
	// This can also be the name reported by etcd itself if set.
	// +optional
	Name string `json:"name,omitempty"`

	// ID is the hex-encoded member ID as reported by etcd.
	// This is the canonical identifier for an etcd member.
	ID string `json:"id"` // Made non-optional as it's key for identification

	// Version of etcd running on this member.
	// +optional
	Version string `json:"version,omitempty"`

	// IsHealthy indicates if the member is considered healthy.
	// A member is healthy if its etcd /health endpoint is reachable and reports OK,
	// and its Status endpoint does not report any 'Errors'.
	IsHealthy bool `json:"isHealthy"` // No omitempty, always show health

	// IsLearner indicates if the member is currently a learner in the etcd cluster.
	// +optional
	IsLearner bool `json:"isLearner,omitempty"`

	// IsLeader indicates if this member is currently the cluster leader.
	// +optional
	IsLeader bool `json:"isLeader,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// EtcdCluster is the Schema for the etcdclusters API.
type EtcdCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdClusterSpec   `json:"spec,omitempty"`
	Status EtcdClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdClusterList contains a list of EtcdCluster.
type EtcdClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdCluster `json:"items"`
}

type StorageSpec struct {
	AccessModes       corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`      // `ReadWriteOnce` (default) or `ReadWriteMany`. Note that `ReadOnlyMany` isn't allowed.
	StorageClassName  string                            `json:"storageClassName,omitempty"` // optional, the default one will be used if not specified
	PVCName           string                            `json:"pvcName,omitempty"`          // optional, only used when access mode is ReadWriteMany
	VolumeSizeRequest resource.Quantity                 `json:"volumeSizeRequest"`          // required.
	VolumeSizeLimit   resource.Quantity                 `json:"volumeSizeLimit,omitempty"`  // optional
}
