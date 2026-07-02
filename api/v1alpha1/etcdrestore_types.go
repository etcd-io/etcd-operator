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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RestorePhase is a high-level summary of where an EtcdRestore is in its
// lifecycle. It is surfaced on .status.phase for at-a-glance reporting.
type RestorePhase string

const (
	// RestorePhasePending means the restore has been accepted but work has not
	// started yet.
	RestorePhasePending RestorePhase = "Pending"
	// RestorePhaseDownloading means the snapshot is being fetched from object
	// storage.
	RestorePhaseDownloading RestorePhase = "Downloading"
	// RestorePhaseRestoring means the snapshot is being written into a fresh
	// data directory on the target member and the new cluster is bootstrapping.
	RestorePhaseRestoring RestorePhase = "Restoring"
	// RestorePhaseCompleted means the snapshot was restored into the target
	// cluster successfully.
	RestorePhaseCompleted RestorePhase = "Completed"
	// RestorePhaseFailed means the restore failed; see conditions for details.
	RestorePhaseFailed RestorePhase = "Failed"
)

// Condition types reported on EtcdRestore status.
const (
	// RestoreConditionSucceeded is True when the snapshot has been downloaded
	// and restored into the target cluster.
	RestoreConditionSucceeded = "Succeeded"
)

// SnapshotSource describes where the snapshot to restore comes from. Exactly
// one of BackupRef or Location must be set; the controller rejects a source
// that sets both or neither.
type SnapshotSource struct {
	// BackupRef names a completed EtcdBackup in the same namespace whose
	// uploaded snapshot should be restored. The controller reads that backup's
	// destination (bucket/prefix/provider/secretRef) and recorded
	// snapshotLocation, so this is the convenient path when restoring a backup
	// the operator itself produced.
	// +optional
	BackupRef *BackupReference `json:"backupRef,omitempty"`

	// Location fully describes a snapshot object independently of any
	// EtcdBackup resource. Use this to restore a snapshot taken out-of-band or
	// after the originating EtcdBackup has been deleted.
	// +optional
	Location *SnapshotLocation `json:"location,omitempty"`
}

// BackupReference points at an EtcdBackup in the same namespace.
type BackupReference struct {
	// Name is the metadata.name of the EtcdBackup to restore from.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// SnapshotLocation describes an explicit snapshot object in object storage. It
// reuses BackupDestination (provider, bucket, prefix, secretRef) and adds the
// object key relative to the destination prefix.
type SnapshotLocation struct {
	// Destination is the object-storage location (provider, bucket, prefix and
	// optional secretRef) the snapshot lives in. The same secretRef semantics
	// as EtcdBackup apply: omit it to use ambient credentials.
	Destination BackupDestination `json:"destination"`

	// Key is the object key of the snapshot, relative to the destination
	// Prefix (e.g. "my-cluster/backup-1-20260617T010203Z.db"). It is joined
	// with the destination prefix exactly as the backup path joins them, so a
	// value copied verbatim from an EtcdBackup's status round-trips.
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// RestoreTarget describes the new EtcdCluster the snapshot is restored into. A
// restore always targets a fresh, empty cluster: the restored data directory
// must be the cluster's genesis, never overlaid onto an existing member, so the
// controller refuses to proceed if a non-empty EtcdCluster of this name already
// exists.
type RestoreTarget struct {
	// Name is the metadata.name of the EtcdCluster to create (in the
	// EtcdRestore's namespace) and restore into. It must not already exist
	// unless it exists and reports zero ready members (an empty shell).
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Size is the desired size of the restored cluster. Defaults to 1 if unset;
	// a single-member restore is the safest default because etcd's snapshot
	// restore bootstraps a single-node cluster which is then grown.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Size int `json:"size,omitempty"`

	// Version is the etcd version for the restored cluster. If empty, the
	// controller inherits the version recorded on the source EtcdBackup (when
	// restoring via backupRef) or requires it to be set explicitly.
	// +optional
	Version string `json:"version,omitempty"`

	// StorageSpec optionally requests persistent storage for the restored
	// cluster, mirroring EtcdClusterSpec.StorageSpec. When omitted the restored
	// cluster uses ephemeral container storage.
	// +optional
	StorageSpec *StorageSpec `json:"storageSpec,omitempty"`
}

// EtcdRestoreSpec defines the desired state of an EtcdRestore: take a snapshot
// from object storage and bootstrap a new EtcdCluster from it.
type EtcdRestoreSpec struct {
	// Source selects the snapshot to restore.
	Source SnapshotSource `json:"source"`

	// Target describes the EtcdCluster to create and restore into.
	Target RestoreTarget `json:"target"`

	// RestoreTimeout bounds how long the download+restore step may run before it
	// is considered failed. Defaults to 10m if unset.
	// +optional
	RestoreTimeout *metav1.Duration `json:"restoreTimeout,omitempty"`
}

// EtcdRestoreStatus defines the observed state of an EtcdRestore.
type EtcdRestoreStatus struct {
	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase is a high-level summary of the restore lifecycle.
	// +optional
	Phase RestorePhase `json:"phase,omitempty"`

	// SnapshotLocation is the fully-qualified URI of the snapshot that was
	// restored (e.g. "s3://my-bucket/etcd/backups/...db").
	// +optional
	SnapshotLocation string `json:"snapshotLocation,omitempty"`

	// RestoredCluster is the name of the EtcdCluster the snapshot was restored
	// into, once created.
	// +optional
	RestoredCluster string `json:"restoredCluster,omitempty"`

	// CompletionTime is when the restore finished successfully.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions represent the latest available observations of the restore's state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.status.snapshotLocation`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdRestore is the Schema for the etcdrestores API. It represents restoring a
// snapshot from object storage into a new EtcdCluster.
type EtcdRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdRestoreSpec   `json:"spec,omitempty"`
	Status EtcdRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdRestoreList contains a list of EtcdRestore.
type EtcdRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdRestore `json:"items"`
}
