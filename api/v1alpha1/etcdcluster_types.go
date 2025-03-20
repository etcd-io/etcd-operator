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
	Size int `json:"size"`
	// Version is the expected version of the etcd container image.
	Version string `json:"version"`
	// StorageSpec is the name of the StorageSpec to use for the etcd cluster. If not provided, then each POD just uses the temporary storage inside the container.
	StorageSpec *StorageSpec `json:"storageSpec,omitempty"`
	// TLS is the TLS certificate configuration to use for the etcd cluster and etcd operator.
	TLS *TLSCertificate `json:"tls,omitempty"`
	// etcd configuration options are passed as command line arguments to the etcd container, refer to etcd documentation for configuration options applicable for the version of etcd being used.
	EtcdOptions []string `json:"etcdOptions,omitempty"`
}

type TLSCertificate struct {
	Provider    string         `json:"provider,omitempty"` // Defaults to Auto provider if not present
	ProviderCfg ProviderConfig `json:"providerCfg,omitempty"`
}

type ProviderConfig struct {
	AutoCfg        *ProviderAutoConfig        `json:"autoCfg,omitempty"`
	CertManagerCfg *ProviderCertManagerConfig `json:"certManagerCfg,omitempty"`
}

type ProviderAutoConfig struct {
}

type ProviderCertManagerConfig struct {
	// accepts a secret name with CABundle present
	// this secret will be used to create an Issuer
	// +optional
	CABundle string `json:"caBundle,omitempty"`

	// accepts a string for a human-readable name of the certificate
	CommonName string `json:"commonName"`

	// accepts a string array, if nil programmatically populates the
	// CommonName as DNSNames
	// +optional
	DNSNames []string `json:"dnsNames,omitempty"`

	// accepts a string which can be parsed with time.ParseDuration()
	// if nil, defaults to 90 days
	// +optional
	Duration string `json:"duration,omitempty"`

	// accepts a net.IP array of IP Addresses as alternative names
	// +optional
	IPs []net.IP `json:"ipAddresses,omitempty"`

	// accepts a string as the kind of Issuer
	// either "ClusterIssuer" or "Issuer"
	// if nil, programmatically creates a selfsigned issuer
	// +optional
	IssuerKind string `json:"issuerKind,omitempty"`

	// accepts a string as the name of Issuer
	// if nil, programmatically creates an issuer with name:selfsigned
	// +optional
	IssuerName string `json:"issuerName,omitempty"`

	// organizations to be used on the Certificate.
	// +optional
	Organization []string `json:"organizations,omitempty"`
}

// EtcdClusterStatus defines the observed state of EtcdCluster.
type EtcdClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
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

func init() {
	SchemeBuilder.Register(&EtcdCluster{}, &EtcdClusterList{})
}
