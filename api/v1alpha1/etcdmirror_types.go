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

// EtcdMirrorSpec defines the desired state of an EtcdMirror.
//
// +kubebuilder:validation:XValidation:rule="!(has(self.sync) && has(self.sync.destPrefix) && size(self.sync.destPrefix) > 0 && has(self.sync.noDestPrefix) && self.sync.noDestPrefix)",message="sync.destPrefix and sync.noDestPrefix are mutually exclusive"
type EtcdMirrorSpec struct {
	// Source is the etcd cluster keys are read from. EtcdMirror never writes
	// back to Source; the agent's source-side client is only ever used for
	// Get/Watch, never Put/Delete/Txn.
	Source EtcdMirrorEndpoint `json:"source"`

	// Target is the etcd cluster keys are written into.
	//
	// SECURITY PREREQUISITE: Target.Auth's credential (and/or the target
	// client certificate's associated etcd RBAC role) MUST be an etcd RBAC
	// user restricted via a range-scoped grant-permission to Target.Prefix (or
	// Sync.DestPrefix's resulting range) -- never a cluster-admin-equivalent
	// credential. The agent's client-side rewriteKey/prefix logic is defense
	// against bugs in the agent's own code; it is NOT a security boundary. A
	// compromised agent process, a buggy rewriteKey, or an empty/typo'd prefix
	// with an over-privileged credential can write or delete anywhere in the
	// target cluster's entire keyspace. This is a deployment prerequisite the
	// operator (human) must configure in etcd itself (via `etcdctl role
	// grant-permission --prefix=true <role> readwrite <target-prefix>`) before
	// pointing an EtcdMirror at it; the controller cannot create or enforce
	// etcd-native RBAC roles itself, but it does check for and surface an
	// unrestricted-looking target credential where etcd's auth API makes that
	// detectable (see Safety Guards).
	Target EtcdMirrorEndpoint `json:"target"`

	// ExpectEmptyPrefix, when true, makes the controller verify the target's
	// effective destination prefix has zero keys before the agent's FIRST-EVER
	// SyncBase begins (i.e. no valid checkpoint exists yet for this source
	// cluster identity), refusing to proceed (Phase -> Failed, condition
	// EmptyTargetViolation) if it is already non-empty. Analogous to
	// EtcdRestore's assertClusterEmpty guard.
	//
	// SCOPE, STATED PLAINLY: this is single-shot, genesis-only protection. It
	// is checked exactly once, at the moment a fresh (or cluster-identity-
	// invalidated, see Checkpoint Lifecycle) InitialSync begins. It does NOT
	// re-arm on a compaction-triggered forced resync later in the CR's life
	// (that resync reuses the already-established target prefix ownership),
	// and it does NOT protect against a foreign writer landing keys under the
	// same target prefix at any point AFTER the first successful sync -- there
	// is no key-tagging/provenance marker in v1 that would let the agent
	// distinguish "my own prior output" from "someone else's write" on a
	// later re-check (see Non-Goals). Operators who need ongoing enforcement
	// that nothing else writes to the target prefix must arrange that via
	// etcd RBAC (grant only this mirror's target credential write access to
	// the prefix) rather than relying on ExpectEmptyPrefix, which is a
	// bring-up guard, not a standing invariant.
	// Defaults to false; operators standing up a NEW mirror onto a fresh
	// prefix should set this true.
	// +optional
	ExpectEmptyPrefix bool `json:"expectEmptyPrefix,omitempty"`

	// Sync tunes runtime sync behavior (batching, rate limiting, prefix
	// rewrite, backoff).
	// +optional
	Sync EtcdMirrorSyncSpec `json:"sync,omitempty"`

	// Checkpoint configures the agent's local durable progress checkpoint.
	// +optional
	Checkpoint *EtcdMirrorCheckpointSpec `json:"checkpoint,omitempty"`

	// Reconciliation optionally enables a periodic full diff-and-repair pass
	// layered on top of the continuous watch-based mirror.
	// +optional
	Reconciliation *EtcdMirrorReconciliationSpec `json:"reconciliation,omitempty"`

	// PodTemplate carries scheduling/affinity/labels/annotations for the agent
	// pod, reusing EtcdClusterSpec's PodTemplate shape verbatim.
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`

	// Paused, when true, tells the controller to scale the agent StatefulSet to
	// zero replicas without deleting the CR or its checkpoint PVC. For planned
	// maintenance windows on either cluster without losing sync position. The
	// agent's own retry/backoff loop handles transient interruptions on its
	// own; Paused is for deliberate, operator-initiated stops.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// EtcdMirrorEndpoint describes how to reach, authenticate to, and scope one
// side (source or target) of a mirror. Both sides need an identical shape
// (address resolution + prefix + TLS + auth), so one type serves both roles,
// the same way BackupDestination is reused verbatim between EtcdBackup and
// EtcdRestore rather than forked into near-duplicate per-role types.
//
// Exactly one of EndpointList or ServiceRef must be set.
//
// +kubebuilder:validation:XValidation:rule="(has(self.endpointList) && size(self.endpointList) > 0) != has(self.serviceRef)",message="exactly one of endpointList or serviceRef must be set"
type EtcdMirrorEndpoint struct {
	// EndpointList is a raw set of etcd client-URL host:port (or
	// scheme://host:port) strings, e.g. "https://etcd-rke1.example.com:2379".
	// This is the ONLY supported mechanism for a cluster external to this
	// Kubernetes cluster (e.g. an RKE1/AWS source reached over a public NLB) --
	// there is deliberately no "tunnel" or "port-forward" mode. A
	// `kubectl port-forward` is a client-attached, ephemeral process tied to a
	// human's terminal; it does not survive this mirror pod restarting,
	// rescheduling, or the operator itself restarting, which defeats the
	// entire point of a supervised, restart-tolerant workload. If you need a
	// persistent tunnel, terminate it yourself (VPN, Interconnect, NLB)
	// upstream of this CR and hand EtcdMirror the resulting stable
	// endpoint(s); network reachability itself is out of scope for this CRD.
	//
	// IP-LITERAL ENDPOINTS: if any entry here is a bare IP literal (plausible
	// for an NLB endpoint with no DNS name), Go's TLS stack requires an IP SAN
	// (not a DNS SAN) on the peer certificate for verification to succeed.
	// Either set the corresponding EtcdMirrorTLS.ServerName to a hostname that
	// IS present as a DNS SAN on the certificate, or ensure the certificate
	// carries an IP SAN matching the literal. Operators who hit a verification
	// failure here should fix the SAN/ServerName mismatch, not reach for
	// InsecureSkipVerify to work around it.
	// +optional
	EndpointList []string `json:"endpointList,omitempty"`

	// ServiceRef points at a Kubernetes Service in this cluster whose DNS name
	// resolves the etcd client endpoint(s). The rarer same-cluster or
	// co-located case (e.g. mirroring between two EtcdClusters both running in
	// this GKE cluster). Namespace defaults to the EtcdMirror's own namespace
	// when empty.
	// +optional
	ServiceRef *EtcdMirrorServiceRef `json:"serviceRef,omitempty"`

	// Prefix is the etcd key prefix on THIS side. On Source, only keys under
	// this prefix are synced; empty means the whole keyspace. On Target, this
	// is the prefix under which the agent writes mirrored keys after any
	// Sync.DestPrefix remap is applied.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// TLS configures the agent's client TLS identity/trust for THIS side. Nil
	// means the agent dials this side in cleartext.
	// +optional
	TLS *EtcdMirrorTLS `json:"tls,omitempty"`

	// Auth configures etcd username/password (RBAC) auth for THIS side,
	// ambient-or-secretRef per the objectstore_creds.go pattern.
	// +optional
	Auth *EtcdMirrorAuth `json:"auth,omitempty"`
}

// EtcdMirrorServiceRef points at a Service and the client port on it to dial.
type EtcdMirrorServiceRef struct {
	// Name is the Service name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace defaults to the EtcdMirror's own namespace when empty.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Port is the Service port name or number exposing etcd's client API.
	// Defaults to "client" when empty, matching PodMonitorSpec.Port's
	// convention elsewhere in this API group.
	// +optional
	Port string `json:"port,omitempty"`
}

// EtcdMirrorTLS configures the mirror agent's client TLS identity for one side
// of a mirror. Plain secretRef, not a reuse of EtcdClusterTLS/TLSSurface --
// the mirror agent is always a client to clusters it does not own, so the
// issuer-selection machinery in TLSSurface/ProviderCertManagerConfig doesn't apply.
//
// +kubebuilder:validation:XValidation:rule="!self.insecureSkipVerify || self.insecureSkipVerifyAcknowledgeRisk",message="insecureSkipVerify requires insecureSkipVerifyAcknowledgeRisk to also be true"
// +kubebuilder:validation:XValidation:rule="has(self.secretRef.name) && size(self.secretRef.name) > 0",message="secretRef.name is required"
type EtcdMirrorTLS struct {
	// SecretRef names a Secret (in the EtcdMirror's namespace) holding this
	// side's TLS material in the standard kubernetes.io/tls-compatible shape:
	//   - ca.crt:  PEM CA bundle used to verify the peer's server certificate.
	//     Required for TLS unless InsecureSkipVerify is true.
	//   - tls.crt / tls.key: PEM client certificate + key, for mTLS. Optional --
	//     omit both for server-auth-only TLS (verify the peer, authenticate to
	//     it via Auth instead, or not at all).
	// This is the same key layout a cert-manager Certificate's spec.secretName
	// Secret uses, so a cert-manager Certificate (or `kubectl create secret
	// tls`, or any other issuance mechanism) can populate this Secret with zero
	// coupling from this CRD to cert-manager.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`

	// InsecureSkipVerify disables server certificate verification. Strongly
	// discouraged, especially for a source reached over the public internet.
	// Requires InsecureSkipVerifyAcknowledgeRisk to also be set true
	// (CEL-enforced on this type). The controller additionally emits a
	// standing Warning event whenever this is true.
	// +optional
	// +kubebuilder:default=false
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// InsecureSkipVerifyAcknowledgeRisk must independently be set true
	// whenever InsecureSkipVerify is true (CEL-enforced companion field). Its
	// only purpose is to require a deliberate, separate, reviewable line in
	// the manifest diff before disabling TLS verification.
	// +optional
	// +kubebuilder:default=false
	InsecureSkipVerifyAcknowledgeRisk bool `json:"insecureSkipVerifyAcknowledgeRisk,omitempty"`

	// ServerName overrides the TLS ServerName (SNI) used for verification, for
	// cases where the dialed endpoint's address doesn't match a SAN on the
	// certificate (e.g. dialing an NLB IP directly -- see EndpointList's
	// IP-literal guidance).
	// +optional
	ServerName string `json:"serverName,omitempty"`
}

// EtcdMirrorAuth configures etcd RBAC username/password auth for one side.
//
// +kubebuilder:validation:XValidation:rule="has(self.secretRef.name) && size(self.secretRef.name) > 0",message="secretRef.name is required"
type EtcdMirrorAuth struct {
	// SecretRef names a Secret holding "username" and "password" keys. If this
	// whole Auth block is nil, the agent does not call etcd's Authenticate()
	// at all. There is no ambient fallback for etcd username/password auth
	// (unlike objectstore's IRSA/Workload Identity).
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// EtcdMirrorSyncSpec tunes the mirror's runtime sync behavior. Defaults are
// chosen to match etcdctl make-mirror's own defaults where one exists (e.g.
// MaxTxnOps=128).
type EtcdMirrorSyncSpec struct {
	// DestPrefix rewrites Source.Prefix to a different prefix on the target,
	// via an anchored strip-and-reprefix (strings.TrimPrefix(key,
	// source.Prefix), then destPrefix + rest) -- never make-mirror's naive
	// first-occurrence strings.Replace. Mutually exclusive with NoDestPrefix.
	// Empty (and NoDestPrefix false) means the source prefix is reused
	// verbatim on the target, landing keys under Target.Prefix + the stripped
	// key remainder.
	// +optional
	DestPrefix string `json:"destPrefix,omitempty"`

	// NoDestPrefix strips Source.Prefix entirely rather than remapping it,
	// mirroring make-mirror's --no-dest-prefix. Mutually exclusive with a
	// non-empty DestPrefix (rejected by EtcdMirrorSpec's XValidation rule).
	// +optional
	NoDestPrefix bool `json:"noDestPrefix,omitempty"`

	// MaxTxnOps bounds how many put/delete operations the agent batches into a
	// single destination Txn, applied uniformly to BOTH the initial SyncBase
	// phase and the watch-driven SyncUpdates phase. Defaults to 128 when unset.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxTxnOps int32 `json:"maxTxnOps,omitempty"`

	// MaxOpsPerSecond rate-limits the agent's destination write rate (a simple
	// token bucket over puts+deletes/sec), applied to BOTH InitialSync's Txn
	// stream and SyncUpdates. Zero (default) means unlimited.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxOpsPerSecond int32 `json:"maxOpsPerSecond,omitempty"`

	// ReconnectBackoff bounds the retry/backoff loop wrapping every Syncer call
	// and every destination Txn call. Defaults to exponential backoff from 1s
	// to 30s, uncapped in attempt count, when unset.
	// +optional
	ReconnectBackoff *EtcdMirrorBackoffSpec `json:"reconnectBackoff,omitempty"`

	// DialTimeout bounds how long the agent waits to establish the initial
	// client connection to each side. Defaults to 10s when unset.
	// +optional
	DialTimeout *metav1.Duration `json:"dialTimeout,omitempty"`
}

type EtcdMirrorBackoffSpec struct {
	// +optional
	InitialDelay *metav1.Duration `json:"initialDelay,omitempty"`
	// +optional
	MaxDelay *metav1.Duration `json:"maxDelay,omitempty"`
}

// EtcdMirrorCheckpointSpec configures the agent's local PVC-backed checkpoint.
type EtcdMirrorCheckpointSpec struct {
	// StorageSpec requests persistent storage for the checkpoint file, reusing
	// EtcdClusterSpec's StorageSpec shape. When nil the controller mounts an
	// emptyDir instead. StorageSpec should be set for any production mirror; a
	// small size (e.g. 64Mi) is sufficient.
	// +optional
	StorageSpec *StorageSpec `json:"storageSpec,omitempty"`

	// SyncInterval controls how often the agent flushes its in-memory
	// last-applied revision to the checkpoint file (atomic write: temp file +
	// fsync + rename). Defaults to 5s when unset.
	// +optional
	SyncInterval *metav1.Duration `json:"syncInterval,omitempty"`
}

// EtcdMirrorReconciliationSpec configures an OPTIONAL periodic full
// reconciliation pass layered on top of the continuous watch-based mirror.
type EtcdMirrorReconciliationSpec struct {
	// Enabled toggles the periodic full reconciliation pass. Defaults to
	// false: it is a diff of the full prefix contents on both sides
	// (O(keyspace size)), so it is opt-in.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Interval between reconciliation passes. Defaults to 1h when Enabled and
	// unset.
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`

	// DeleteOrphans, when true, allows reconciliation to DELETE target keys
	// under the destination prefix that have no corresponding source key.
	// Defaults to false.
	// +optional
	DeleteOrphans bool `json:"deleteOrphans,omitempty"`
}

// EtcdMirrorPhase is a high-level summary of an EtcdMirror's lifecycle.
// Unlike BackupPhase/RestorePhase, most phases here are NOT terminal -- a
// healthy mirror spends its life cycling between Syncing and (briefly, on
// transient errors) Degraded; there is no "Completed" state.
type EtcdMirrorPhase string

const (
	// EtcdMirrorPhasePending means the EtcdMirror has been accepted but the
	// agent workload has not been created yet.
	EtcdMirrorPhasePending EtcdMirrorPhase = "Pending"
	// EtcdMirrorPhaseConnecting means the agent pod is running and establishing
	// client connections to both Source and Target.
	EtcdMirrorPhaseConnecting EtcdMirrorPhase = "Connecting"
	// EtcdMirrorPhaseInitialSync means the agent is running SyncBase: the
	// paginated, revision-pinned full range scan establishing the base
	// revision. Entered on a genesis start (no valid checkpoint), a
	// cluster-identity-invalidated checkpoint, or an agent-initiated forced
	// resync after compaction (see EtcdMirrorConditionCompacted).
	EtcdMirrorPhaseInitialSync EtcdMirrorPhase = "InitialSync"
	// EtcdMirrorPhaseSyncing is the steady-state: SyncBase has completed (or
	// was skipped via checkpoint resume) and SyncUpdates is watching and
	// applying live changes. A healthy mirror spends effectively all its time
	// here.
	EtcdMirrorPhaseSyncing EtcdMirrorPhase = "Syncing"
	// EtcdMirrorPhaseDegraded means the agent hit a recoverable condition
	// (reconnect backoff to either side, target throttling backoff, or an
	// in-progress compaction-forced resync) and is retrying/self-healing.
	// Non-terminal: expected to return to Syncing/InitialSync automatically
	// without operator action.
	EtcdMirrorPhaseDegraded EtcdMirrorPhase = "Degraded"
	// EtcdMirrorPhasePaused means spec.paused is true; the agent StatefulSet is
	// scaled to zero. The checkpoint is retained so resuming picks up from the
	// last-applied revision (subject to the same cluster-identity check on
	// resume as any other restart).
	EtcdMirrorPhasePaused EtcdMirrorPhase = "Paused"
	// EtcdMirrorPhaseFailed means the mirror hit a terminal, non-recoverable
	// error (ExpectEmptyPrefix violated at genesis, malformed cert material,
	// unresolvable spec misconfiguration) requiring operator intervention.
	EtcdMirrorPhaseFailed EtcdMirrorPhase = "Failed"
)

// Condition types reported on EtcdMirror status.
const (
	// EtcdMirrorConditionAvailable is True only when the agent pod is running,
	// in the Syncing phase, AND the loop-liveness check shows forward progress
	// within the configured staleness threshold. A wedged main loop that keeps
	// answering /statusz with a stale-but-structurally-valid response is
	// explicitly NOT allowed to read as Available=True; see
	// ReplicationLagExceeded below for the companion signal.
	EtcdMirrorConditionAvailable = "Available"
	// EtcdMirrorConditionSourceReachable is True when the agent's last attempt
	// to reach Source succeeded. Split from TargetReachable because in the
	// primary use case (RKE1 source over the public internet) source
	// reachability is the most likely persistent failure mode.
	EtcdMirrorConditionSourceReachable = "SourceReachable"
	// EtcdMirrorConditionTargetReachable is the target-side analogue.
	EtcdMirrorConditionTargetReachable = "TargetReachable"
	// EtcdMirrorConditionTargetThrottled is True while the agent is backing
	// off from throttling-class errors on Target (etcd ErrTooManyRequests /
	// gRPC ResourceExhausted / quota-exhausted), handled distinctly from a
	// plain connection drop (a more conservative backoff curve). Kept separate
	// from TargetReachable=False because "target is up but rejecting my write
	// rate" is a different, actionable signal than "target is unreachable."
	EtcdMirrorConditionTargetThrottled = "TargetThrottled"
	// EtcdMirrorConditionInitialSyncComplete is True once SyncBase has
	// completed at least once against the currently-checkpointed source
	// cluster identity. Durable -- does not flip back to False on a
	// compaction-forced resync (Compacted covers that). Reset to False if the
	// checkpoint is invalidated by a cluster-identity mismatch.
	EtcdMirrorConditionInitialSyncComplete = "InitialSyncComplete"
	// EtcdMirrorConditionCompacted is True (Reason "ForcedResync") while the
	// agent is auto-healing a forced fresh SyncBase after detecting
	// source-side compaction raced the watch (a WatchResponse with
	// Canceled=true and CompactRevision != 0), rather than crash-looping the
	// way etcdctl make-mirror does. Reverts to False once the forced resync
	// completes and steady-state watching resumes.
	EtcdMirrorConditionCompacted = "Compacted"
	// EtcdMirrorConditionReplicationLagExceeded is True when SourceRevision -
	// LastAppliedRevision (or, more precisely, the agent's own "time since
	// last successful destination apply" loop-liveness measure) has exceeded a
	// threshold for a sustained duration D. A deadlocked target-write retry
	// loop can keep the process alive and /statusz technically responding,
	// but it cannot keep making progress, and this condition is derived from
	// progress, not liveness. Threshold and duration D are agent-internal
	// constants in v1 (not a spec knob).
	EtcdMirrorConditionReplicationLagExceeded = "ReplicationLagExceeded"
	// EtcdMirrorConditionDriftDetected is True when the last reconciliation
	// pass (if spec.reconciliation.enabled) found a nonzero number of
	// orphaned/missing keys. Carries counts in Message. Present only when
	// reconciliation is enabled; sticky until the next pass reports clean.
	EtcdMirrorConditionDriftDetected = "DriftDetected"
	// EtcdMirrorConditionEmptyTargetViolation is True (and terminal, Phase ->
	// Failed) when ExpectEmptyPrefix was set and the destination prefix was
	// found non-empty before the first-ever InitialSync began.
	EtcdMirrorConditionEmptyTargetViolation = "EmptyTargetViolation"
)

// EtcdMirrorStatus defines the observed state of an EtcdMirror. Progress
// fields are synced periodically (not per-op), so they are a coarse,
// point-in-time mirror of the agent's authoritative local checkpoint, useful
// for kubectl/dashboards, not an audit log.
type EtcdMirrorStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	Phase EtcdMirrorPhase `json:"phase,omitempty"`

	// LastAppliedRevision is the source etcd revision through which the target
	// is known to be caught up, as of the last periodic status sync. The
	// agent's local checkpoint file is the authoritative, hot-path copy; this
	// is a periodically-synced mirror of it for observability.
	// +optional
	LastAppliedRevision int64 `json:"lastAppliedRevision,omitempty"`

	// SourceRevision is the source cluster's revision as of the last status
	// sync, for computing an approximate replication lag (SourceRevision -
	// LastAppliedRevision) at a glance.
	// +optional
	SourceRevision int64 `json:"sourceRevision,omitempty"`

	// SourceClusterID is the source etcd cluster's cluster ID, as observed on
	// the response header of the agent's most recent successful call against
	// Source. Surfaced for operator debugging of checkpoint-invalidation
	// events -- a changed SourceClusterID across reconciles is the visible
	// symptom of "source endpoint now points at a different cluster than the
	// checkpoint was taken against."
	// +optional
	SourceClusterID string `json:"sourceClusterID,omitempty"`

	// InitialSyncKeyCount, InitialSyncStartTime, InitialSyncCompletionTime
	// track the base-sync phase for observability.
	// +optional
	InitialSyncKeyCount int64 `json:"initialSyncKeyCount,omitempty"`
	// +optional
	InitialSyncStartTime *metav1.Time `json:"initialSyncStartTime,omitempty"`
	// +optional
	InitialSyncCompletionTime *metav1.Time `json:"initialSyncCompletionTime,omitempty"`

	// ForcedResyncCount counts how many times the agent has auto-healed from a
	// source-compaction-raced-the-watch error, OR a cluster-identity-mismatch
	// checkpoint invalidation, by re-running SyncBase. Monotonically
	// increasing, never reset.
	// +optional
	ForcedResyncCount int32 `json:"forcedResyncCount,omitempty"`

	// LastReconciliationTime and LastReconciliationDrift record the most
	// recent periodic reconciliation pass, when spec.reconciliation.enabled.
	// +optional
	LastReconciliationTime *metav1.Time `json:"lastReconciliationTime,omitempty"`
	// +optional
	LastReconciliationDrift *EtcdMirrorDriftInfo `json:"lastReconciliationDrift,omitempty"`

	// LastStatusSyncTime is when status was last refreshed from the agent, so
	// staleness (e.g. a wedged agent that stopped responding but hasn't
	// crashed) is directly observable.
	// +optional
	LastStatusSyncTime *metav1.Time `json:"lastStatusSyncTime,omitempty"`

	// LastProgressTime is when the agent last recorded ANY successful
	// destination apply (a completed Txn during InitialSync, SyncUpdates, or
	// reconciliation). Distinct from LastStatusSyncTime: /statusz can keep
	// responding on-time from a wedged loop that has stopped applying writes,
	// so this field is what ReplicationLagExceeded is actually derived from.
	// +optional
	LastProgressTime *metav1.Time `json:"lastProgressTime,omitempty"`

	// AgentPod is the name of the current agent pod (the sole pod of the
	// size-1 StatefulSet), for convenient kubectl logs/exec.
	// +optional
	AgentPod string `json:"agentPod,omitempty"`

	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

type EtcdMirrorDriftInfo struct {
	MissingKeys int64 `json:"missingKeys,omitempty"`
	OrphanKeys  int64 `json:"orphanKeys,omitempty"`
	Repaired    bool  `json:"repaired,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.source.prefix`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target.prefix`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Revision",type=integer,JSONPath=`.status.lastAppliedRevision`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdMirror is the Schema for the etcdmirrors API. It describes a continuous,
// one-way key-range sync from a source etcd cluster to a target etcd cluster,
// run as a single supervised pod (a size-1 StatefulSet) in this cluster.
//
// EtcdMirror is deliberately one-way only: clientv3/mirror.Syncer has no
// bidirectional primitive, and etcd itself has no concept of "this write
// originated from a mirror, don't re-mirror it back." Two opposite-direction
// EtcdMirrors pointed at each other create an unbounded write-ping-pong with no
// conflict resolution; see Non-Goals.
type EtcdMirror struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdMirrorSpec   `json:"spec,omitempty"`
	Status EtcdMirrorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdMirrorList contains a list of EtcdMirror.
type EtcdMirrorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdMirror `json:"items"`
}
