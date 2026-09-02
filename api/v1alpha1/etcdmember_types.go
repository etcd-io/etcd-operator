// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EtcdMemberSpec defines the desired state of one etcd member/ordinal.
type EtcdMemberSpec struct {
	// ClusterName is the owning EtcdCluster's name (same namespace).
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="clusterName is immutable"
	ClusterName string `json:"clusterName"`

	// Ordinal is this member's fixed position, e.g. 0, 1, 2. The member Pod
	// and PVC are named "{clusterName}-{ordinal}" / "etcd-data-{clusterName}-{ordinal}".
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ordinal is immutable"
	Ordinal int `json:"ordinal"`

	// Version is the etcd version this member's Pod should run. Normally
	// equal to EtcdCluster.Spec.Version, but during a rolling upgrade
	// EtcdClusterReconciler bumps this one member at a time, so members can
	// transiently run different versions than the cluster's target.
	Version string `json:"version"`
}

// EtcdMemberPhase is the coarse lifecycle phase of a member.
type EtcdMemberPhase string

const (
	// EtcdMemberPending means the controller hasn't started creating this member's resources yet.
	EtcdMemberPending EtcdMemberPhase = "Pending"
	// EtcdMemberProvisioning means resource creation is under way: Pod not yet healthy, OR healthy but still an unpromoted learner.
	EtcdMemberProvisioning EtcdMemberPhase = "Provisioning"
	// EtcdMemberUpgrading means the member is upgrading from one version of etcd to another 
	EtcdMemberUpgrading EtcdMemberPhase = "Upgrading"
	// EtcdMemberReady means the member is healthy AND (bootstrap member OR promoted to voting member).
	EtcdMemberReady EtcdMemberPhase = "Ready"
	// EtcdMemberRecreating means the member is unhealthy and a Pod delete+recreate is in progress.
	EtcdMemberRecreating EtcdMemberPhase = "Recreating"
	// EtcdMemberReplacing means recreate retries are exhausted (or a CORRUPT alarm fired); the member is leaving, about to rejoin fresh.
	EtcdMemberReplacing EtcdMemberPhase = "Replacing"
	// EtcdMemberTerminating means DeletionTimestamp is set; the member is leaving the cluster for good.
	EtcdMemberTerminating EtcdMemberPhase = "Terminating"
)

// EtcdMemberStatus defines the observed state of a single etcd member.
type EtcdMemberStatus struct {
	// +optional
	Phase EtcdMemberPhase `json:"phase,omitempty"`

	// MemberName is this member's name, always "{clusterName}-{ordinal}" —
	// the same value is used as both the etcd member name and the Pod name.
	// Recorded for convenience/observability only, never used to make a
	// decision (the name is fully deterministic from Spec.Ordinal, and the
	// Pod-recovery ladder only ever needs to know whether a Pod currently
	// exists and how long it's existed, both a live check against that
	// name — no identity-tracking field is needed to tell a stale Pod apart
	// from a fresh one).
	// +optional
	MemberName string `json:"memberName,omitempty"`

	// MemberID is the hex etcd member ID, recorded for
	// observability/reporting only. EtcdClusterReconciler never uses this
	// persisted value to decide anything — wherever a member's live etcd
	// identity is needed (e.g. MemberRemove's target in the leave sequence),
	// it's found by a live MemberList lookup keyed on this ordinal's
	// deterministic peer URL, never by trusting this field is still
	// current.
	// +optional
	MemberID string `json:"memberID,omitempty"`

	// CurrentVersion is the observed etcd version of this member.
	// +optional
	CurrentVersion string `json:"currentVersion,omitempty"`

	// IsHealthy indicates whether the member's etcd process is healthy, as
	// observed by a live check every reconcile.
	IsHealthy bool `json:"isHealthy"`

	// IsLearner indicates whether the member is currently a learner in the etcd cluster.
	// +optional
	IsLearner bool `json:"isLearner,omitempty"`

	// IsLeader indicates whether this member is currently the cluster leader.
	// +optional
	IsLeader bool `json:"isLeader,omitempty"`

	// RecreateCount is the number of consecutive Pod recreations performed
	// while trying to get this member healthy — whether it's a Provisioning
	// member whose Pod never came up, or a Ready member that regressed and
	// is now Recreating. Bumped once each recreate actually happens, after
	// the mutation rather than before. Reset to 0 both when the member
	// reaches Ready and when it escalates to Phase=Replacing. At
	// RecreateCount == 3 the reconciler gives up on "just recreate the
	// Pod" and moves to Phase=Replacing.
	// +optional
	RecreateCount int32 `json:"recreateCount,omitempty"`

	// LastDefragTime records when this member last finished a defrag
	// attempt — successful or timed out. Used to tell whether this member
	// still needs defragment in the current NOSPACE cycle, without redoing
	// one already attempted.
	// +optional
	LastDefragTime *metav1.Time `json:"lastDefragTime,omitempty"`

	// Conditions represent the latest available observations of the EtcdMember's state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Ordinal",type=integer,JSONPath=`.spec.ordinal`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="RecreateCount",type=integer,JSONPath=`.status.recreateCount`

// EtcdMember is the Schema for the etcdmembers API.
type EtcdMember struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdMemberSpec   `json:"spec,omitempty"`
	Status EtcdMemberStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdMemberList contains a list of EtcdMember.
type EtcdMemberList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdMember `json:"items"`
}
