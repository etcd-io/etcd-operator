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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupProvider enumerates the supported object-storage backends a snapshot
// can be uploaded to. The set is intentionally open: the controller dispatches
// on this value to a pluggable provider implementation in pkg/objectstore, so
// adding a new backend is a matter of registering a new provider and a new
// value here.
// +kubebuilder:validation:Enum=s3;gcs
type BackupProvider string

const (
	// BackupProviderS3 uploads the snapshot to an AWS S3 (or S3-compatible)
	// bucket via the aws-sdk-go-v2 provider.
	BackupProviderS3 BackupProvider = "s3"
	// BackupProviderGCS uploads the snapshot to a Google Cloud Storage bucket
	// via the cloud.google.com/go/storage provider.
	BackupProviderGCS BackupProvider = "gcs"
)

// BackupPhase is a high-level summary of where an EtcdBackup is in its
// lifecycle. It is surfaced on .status.phase for at-a-glance reporting.
type BackupPhase string

const (
	// BackupPhasePending means the backup has been accepted but work has not
	// started yet.
	BackupPhasePending BackupPhase = "Pending"
	// BackupPhaseSnapshotting means a snapshot is being taken from a member.
	BackupPhaseSnapshotting BackupPhase = "Snapshotting"
	// BackupPhaseUploading means the snapshot is being uploaded to object storage.
	BackupPhaseUploading BackupPhase = "Uploading"
	// BackupPhaseCompleted means the snapshot was uploaded successfully.
	BackupPhaseCompleted BackupPhase = "Completed"
	// BackupPhaseFailed means the backup failed; see conditions for details.
	BackupPhaseFailed BackupPhase = "Failed"
)

// Condition types reported on EtcdBackup status.
const (
	// BackupConditionSucceeded is True when the snapshot has been taken and
	// uploaded to the destination object store.
	BackupConditionSucceeded = "Succeeded"
)

// S3DestinationSpec describes an AWS S3 (or S3-compatible) upload target.
type S3DestinationSpec struct {
	// Bucket is the destination S3 bucket name.
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Region is the AWS region the bucket resides in (e.g. "us-east-1").
	// +optional
	Region string `json:"region,omitempty"`

	// Endpoint overrides the S3 endpoint, enabling S3-compatible stores such
	// as MinIO or Ceph RGW. If empty, the default AWS endpoint for the region
	// is used.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// ForcePathStyle forces path-style addressing (bucket in the path rather
	// than the host). Required by most S3-compatible stores.
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`
}

// GCSDestinationSpec describes a Google Cloud Storage upload target.
type GCSDestinationSpec struct {
	// Bucket is the destination GCS bucket name.
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Endpoint overrides the GCS endpoint, enabling GCS-compatible emulators
	// such as fake-gcs-server or the gcloud storage testbench for hermetic,
	// credential-free testing. It must point at the JSON API root the emulator
	// serves (e.g. "http://fake-gcs:9000/storage/v1/"). When set, the client is
	// pointed at this endpoint and runs unauthenticated, mirroring the S3
	// endpoint override that targets MinIO. If empty, the real Google endpoint
	// and the normal credential chain are used.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
}

// BackupDestination describes where a snapshot is uploaded. Exactly one
// provider-specific block must be populated and it must match Provider.
type BackupDestination struct {
	// Provider selects the object-storage backend.
	Provider BackupProvider `json:"provider"`

	// Prefix is an optional key prefix (a.k.a. "folder") within the bucket
	// under which the snapshot object is written. A trailing slash is optional.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// SecretRef references a Secret in the EtcdBackup's namespace holding the
	// credentials for the provider. The expected keys depend on the provider:
	//   - s3:  accessKeyID / secretAccessKey (and optionally sessionToken)
	//   - gcs: serviceAccountJSON (a GCP service-account key)
	// If unset, the controller falls back to ambient credentials available to
	// the operator pod (e.g. IRSA / Workload Identity), which is the
	// recommended production posture.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// S3 holds S3-specific destination configuration. Required when
	// Provider is "s3".
	// +optional
	S3 *S3DestinationSpec `json:"s3,omitempty"`

	// GCS holds GCS-specific destination configuration. Required when
	// Provider is "gcs".
	// +optional
	GCS *GCSDestinationSpec `json:"gcs,omitempty"`
}

// EtcdBackupSpec defines the desired state of an EtcdBackup.
type EtcdBackupSpec struct {
	// ClusterRef references the EtcdCluster to snapshot. The cluster must live
	// in the same namespace as this EtcdBackup.
	// +kubebuilder:validation:MinLength=1
	ClusterRef string `json:"clusterRef"`

	// Destination describes the object-storage target for the snapshot.
	Destination BackupDestination `json:"destination"`

	// SnapshotTimeout bounds how long the snapshot-save step may run before it
	// is considered failed. Defaults to 10m if unset.
	// +optional
	SnapshotTimeout *metav1.Duration `json:"snapshotTimeout,omitempty"`

	// Retention, when set, asks the controller to delete older snapshots under
	// the same bucket/prefix once more than RetainCount snapshots exist. A
	// value of 0 (the default) disables retention pruning.
	// +optional
	Retention *RetentionPolicy `json:"retention,omitempty"`
}

// RetentionPolicy controls automatic pruning of old snapshots.
type RetentionPolicy struct {
	// RetainCount is the number of most-recent snapshots to keep under the
	// destination bucket/prefix. Older snapshots are deleted after a
	// successful upload. Zero disables pruning.
	// +kubebuilder:validation:Minimum=0
	RetainCount int32 `json:"retainCount,omitempty"`
}

// EtcdBackupStatus defines the observed state of an EtcdBackup.
type EtcdBackupStatus struct {
	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase is a high-level summary of the backup lifecycle.
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// SnapshotLocation is the fully-qualified URI of the uploaded snapshot
	// (e.g. "s3://my-bucket/etcd/backup-...db" or "gs://my-bucket/...").
	// +optional
	SnapshotLocation string `json:"snapshotLocation,omitempty"`

	// SnapshotSizeBytes is the size of the uploaded snapshot in bytes.
	// +optional
	SnapshotSizeBytes int64 `json:"snapshotSizeBytes,omitempty"`

	// CompletionTime is when the snapshot finished uploading successfully.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions represent the latest available observations of the backup's state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.destination.provider`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Location",type=string,JSONPath=`.status.snapshotLocation`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdBackup is the Schema for the etcdbackups API. It represents a single
// point-in-time snapshot of an EtcdCluster uploaded to object storage.
type EtcdBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdBackupSpec   `json:"spec,omitempty"`
	Status EtcdBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdBackupList contains a list of EtcdBackup.
type EtcdBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdBackup `json:"items"`
}
