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
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
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
	//
	// TLS is effectively create-time: it flows into the pod template and cert mounts.
	// Toggling it on (or off) on a running cluster rolls the StatefulSet into a mixed
	// http/https membership whose peers cannot connect, dropping quorum. The supported
	// path is a NEW TLS cluster plus data migration, not an in-place flip.
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

// TLSProvider names the certificate provider for one TLS surface. It is the
// discriminator of the flattened union on TLSSurface. Values are domain-style
// identifiers (cf. StorageClass.provisioner): the operator's built-in
// self-signed provider is "auto"; cert-manager is addressed by its API group.
// +kubebuilder:validation:Enum=auto;cert-manager.io
type TLSProvider string

const (
	// TLSProviderAuto selects the operator's built-in self-signed provider.
	TLSProviderAuto TLSProvider = "auto"
	// TLSProviderCertManager selects cert-manager issuance.
	TLSProviderCertManager TLSProvider = "cert-manager.io"
)

// TLSSurface is the full, independent TLS configuration for ONE surface (peer or
// client): a provider-discriminated union (provider selects which member block
// below is honored), the surface's mutual client-cert-auth policy, and an
// optional additional trust bundle.
//
// The XValidation rules below are the apply-time anti-misconfiguration
// guardrails: they reject incoherent provider/member-block combinations and
// mTLS-without-a-resolvable-CA at the API server, so a user "cannot
// misconfigure" these from the spec alone. Rules that require reading cluster
// objects (issuer existence, peer CA-capability, client/server CA match) cannot
// be expressed in CEL and are enforced at reconcile time instead -- see
// validateTLSSurface and the cert-manager provider's validateCertificateConfig.
//
// +kubebuilder:validation:XValidation:rule="self.provider != 'cert-manager.io' || has(self.certManager)",message="provider 'cert-manager.io' requires the certManager block"
// +kubebuilder:validation:XValidation:rule="self.provider == 'cert-manager.io' || !has(self.certManager)",message="certManager may only be set when provider is 'cert-manager.io'"
// +kubebuilder:validation:XValidation:rule="self.provider == 'auto' || !has(self.auto)",message="auto may only be set when provider is 'auto'"
// +kubebuilder:validation:XValidation:rule="!self.clientCertAuth || self.provider != 'cert-manager.io' || (has(self.certManager) && size(self.certManager.issuerRef.name) > 0)",message="clientCertAuth requires a trusted CA: set certManager.issuerRef.name"
type TLSSurface struct {
	// Provider selects the certificate provider for THIS surface and names
	// which member block below is honored. Defaults to "auto".
	// +kubebuilder:default=auto
	// +optional
	Provider TLSProvider `json:"provider,omitempty"`

	// Auto configures the operator's built-in self-signed provider for THIS
	// surface. Only valid when provider is "auto".
	// +optional
	Auto *TLSAutoProvider `json:"auto,omitempty"`

	// CertManager configures cert-manager issuance for THIS surface.
	// Required when provider is "cert-manager.io"; forbidden otherwise.
	// +optional
	CertManager *TLSCertManagerProvider `json:"certManager,omitempty"`

	// ClientCertAuth toggles mutual cert auth for THIS surface (etcd's
	// --client-cert-auth for the client surface, --peer-client-cert-auth for the
	// peer surface). Defaults to true (mTLS). Set false to serve server-only TLS
	// where clients authenticate by other means (password/token). When true with
	// the cert-manager provider a trusted CA (issuerRef.name) is REQUIRED,
	// enforced by the XValidation rule above.
	// +kubebuilder:default=true
	// +optional
	ClientCertAuth *bool `json:"clientCertAuth,omitempty"`

	// TrustBundleConfigMapRef references a ConfigMap in the EtcdCluster's
	// namespace carrying one or more additional PEM CA certificates under the
	// fixed key "ca.crt". The bundle is APPENDED to (never replaces) this
	// surface's MEMBER-SIDE trusted CA set: the operator concatenates it with
	// the issued CA into the file behind --trusted-ca-file /
	// --peer-trusted-ca-file. It broadens which client/peer certificates etcd
	// members accept, NOT which servers the operator trusts when dialing etcd
	// (the operator pins the issuing CA only). etcd reads trusted-CA files at
	// process start only, so trust changes take effect per member on its next
	// restart. Intended for CA-rotation overlap windows.
	// +optional
	TrustBundleConfigMapRef *TrustBundleConfigMapRef `json:"trustBundleConfigMapRef,omitempty"`
}

// TrustBundleConfigMapRef is a LocalObjectReference-style pointer to a
// ConfigMap holding extra trusted CAs under the key "ca.crt".
type TrustBundleConfigMapRef struct {
	// Name of the ConfigMap.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// EffectiveProvider resolves the surface's provider, defaulting to "auto" when
// empty (objects written by an apiserver are already defaulted; this covers
// fake-client / envtest-less code paths).
func (s *TLSSurface) EffectiveProvider() TLSProvider {
	if s == nil || s.Provider == "" {
		return TLSProviderAuto
	}
	return s.Provider
}

// TLSAutoProvider tunes the built-in self-signed provider for one surface.
// All fields are optional; empty values derive defaults from the cluster.
type TLSAutoProvider struct {
	// CommonName is the X509 subject CN. Keep to 64 characters or fewer to
	// avoid generating invalid CSRs.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	CommonName string `json:"commonName,omitempty"`

	// Organizations are the X509 subject O values.
	// +optional
	Organizations []string `json:"organizations,omitempty"`

	// DNSNames are the DNS subject alternative names. Empty defaults to
	// *.<cluster>.<ns>.svc.cluster.local and <cluster>.<ns>.svc.cluster.local
	// (required for the operator's hostname verification of members).
	// +optional
	DNSNames []string `json:"dnsNames,omitempty"`

	// IPAddresses are IP subject alternative names, as literal IP strings.
	// +optional
	IPAddresses []string `json:"ipAddresses,omitempty"`

	// Duration is the requested certificate lifetime. The auto provider
	// requires at least 8760h (365 days); empty defaults to 8760h. Go duration
	// units only (h/m/s); day suffixes like "365d" are not accepted.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('8760h')",message="auto provider certificates must be valid for at least 8760h (365 days); use Go duration units h/m/s (day suffixes like '365d' are not accepted)"
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`

	// RenewBefore is reserved: the auto provider does not yet renew
	// certificates. Accepted for parity with the certManager block.
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="renewBefore must be a positive Go duration using h/m/s units (day suffixes are not accepted)"
	// +optional
	RenewBefore *metav1.Duration `json:"renewBefore,omitempty"`
}

// TLSCertManagerProvider configures cert-manager issuance for one surface.
type TLSCertManagerProvider struct {
	// IssuerRef references the cert-manager Issuer or ClusterIssuer that signs
	// this surface's certificates. kind defaults to "Issuer"; group defaults to
	// "cert-manager.io". Point it at a CA Issuer to issue from a custom CA.
	// +kubebuilder:validation:XValidation:rule="!has(self.kind) || self.kind in ['Issuer', 'ClusterIssuer']",message="issuerRef.kind must be 'Issuer' or 'ClusterIssuer'"
	IssuerRef cmmeta.IssuerReference `json:"issuerRef"`

	// CommonName is the X509 subject CN. Keep to 64 characters or fewer to
	// avoid generating invalid CSRs.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	CommonName string `json:"commonName,omitempty"`

	// Organizations are the X509 subject O values.
	// +optional
	Organizations []string `json:"organizations,omitempty"`

	// DNSNames are the DNS subject alternative names. Empty defaults to
	// *.<cluster>.<ns>.svc.cluster.local and <cluster>.<ns>.svc.cluster.local
	// (required for the operator's hostname verification of members).
	// +optional
	DNSNames []string `json:"dnsNames,omitempty"`

	// IPAddresses are IP subject alternative names, as literal IP strings.
	// +optional
	IPAddresses []string `json:"ipAddresses,omitempty"`

	// Duration is the requested certificate lifetime, passed through to
	// Certificate.spec.duration. Empty defaults to 2160h (90 days). Go duration
	// units only (h/m/s); day suffixes like "90d" are not accepted, and
	// cert-manager's minimum is 1h.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1h')",message="duration must be at least 1h (cert-manager minimum) using Go duration units h/m/s (day suffixes like '90d' are not accepted)"
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`

	// RenewBefore is passed through to Certificate.spec.renewBefore; empty
	// leaves cert-manager's default renewal policy in place.
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="renewBefore must be a positive Go duration using h/m/s units (day suffixes are not accepted)"
	// +optional
	RenewBefore *metav1.Duration `json:"renewBefore,omitempty"`
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
