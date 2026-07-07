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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EtcdMirrorMode selects the mirror's operating mode.
type EtcdMirrorMode string

const (
	// EtcdMirrorModeSync is normal continuous replication.
	EtcdMirrorModeSync EtcdMirrorMode = "Sync"
	// EtcdMirrorModeDrain prepares for cutover: the agent records the source
	// revision observed when Drain is requested (status.cutover.drainTargetRevision),
	// keeps replicating until the checkpoint watermark reaches it, runs a
	// verification pass (per-side key counts, lease-backed key count), then
	// sets the CutoverReady condition and flips the fence key's role to
	// Primary so any straggler apply fails its mod-revision compare loudly.
	// Runbook: quiesce source writers -> set mode=Drain ->
	// `kubectl wait --for=condition=CutoverReady etcdmirror/<name>` ->
	// purge/re-lease lease-backed keys -> repoint clients -> delete the CR.
	EtcdMirrorModeDrain EtcdMirrorMode = "Drain"
)

// EtcdMirrorInitialSyncMode governs how the agent treats pre-existing keys
// under the effective destination prefix at genesis (first-ever sync, or a
// checkpoint invalidated by a cluster-identity mismatch).
type EtcdMirrorInitialSyncMode string

const (
	// EtcdMirrorInitialSyncRequireEmpty refuses to start if the destination
	// prefix already holds any key (Phase -> Failed, condition
	// EmptyTargetViolation). The reserved checkpoint key is excluded by exact
	// match.
	//
	// RE-ARM CONTRACT: a source OR target cluster-ID mismatch invalidates
	// the checkpoint, forces genesis, and RE-ARMS this check. An ordinary
	// forced resync (Compacted) does NOT re-check RequireEmpty: the decoded,
	// ownership-validated fence proves the destination data is this link's
	// own.
	EtcdMirrorInitialSyncRequireEmpty EtcdMirrorInitialSyncMode = "RequireEmpty"
	// EtcdMirrorInitialSyncOverwrite scans and writes over whatever is there.
	// Keys present on the target but absent on the source are left alone.
	EtcdMirrorInitialSyncOverwrite EtcdMirrorInitialSyncMode = "Overwrite"
	// EtcdMirrorInitialSyncOverwriteAndPrune is Overwrite plus one mandatory
	// orphan-prune pass after the scan: target keys under the destination
	// prefix with no source counterpart are deleted. This makes reversal onto
	// a previously-populated prefix (failback) a first-class correct
	// operation instead of silently resurrecting deleted keys.
	EtcdMirrorInitialSyncOverwriteAndPrune EtcdMirrorInitialSyncMode = "OverwriteAndPrune"
)

// EtcdMirrorSpec defines the desired state of an EtcdMirror.
//
// Range-defining and rewrite fields are immutable (CEL transition rules
// below): source.prefix, target.prefix, sync.destPrefix, sync.excludePrefixes
// and checkpoint.key. Changing what range is mirrored, or where it lands,
// mid-life silently diverges: a restarted agent resumes from its checkpoint
// without a scan, so removing an exclusion never backfills pre-existing keys
// and adding one strands already-mirrored keys as permanent orphans — all
// with every condition green. Endpoints stay mutable — rotating an NLB DNS
// name or adding a member to the same cluster is routine; pointing at a
// different cluster is caught at runtime by the checkpoint's dual-cluster-ID
// binding, not by spec validation.
//
// The transition rules compare VALUES, presence-normalized: for these
// fields the empty value and an absent field are semantically identical
// (prefix "" = whole keyspace, destPrefix "" = strip the source prefix), and
// Go typed clients drop explicit "" through omitempty — presence-based rules
// would reject every typed-client update of a CR created with an explicit
// empty string.
//
// +kubebuilder:validation:XValidation:rule="(has(self.source.prefix) ? self.source.prefix : \"\") == (has(oldSelf.source.prefix) ? oldSelf.source.prefix : \"\")",message="source.prefix is immutable"
// +kubebuilder:validation:XValidation:rule="(has(self.target.prefix) ? self.target.prefix : \"\") == (has(oldSelf.target.prefix) ? oldSelf.target.prefix : \"\")",message="target.prefix is immutable"
// +kubebuilder:validation:XValidation:rule="(has(self.sync) && has(self.sync.destPrefix) ? self.sync.destPrefix : \"\") == (has(oldSelf.sync) && has(oldSelf.sync.destPrefix) ? oldSelf.sync.destPrefix : \"\")",message="sync.destPrefix is immutable"
// +kubebuilder:validation:XValidation:rule="(has(self.sync) && has(self.sync.excludePrefixes) ? self.sync.excludePrefixes : []) == (has(oldSelf.sync) && has(oldSelf.sync.excludePrefixes) ? oldSelf.sync.excludePrefixes : [])",message="sync.excludePrefixes is immutable"
// +kubebuilder:validation:XValidation:rule="(has(self.checkpoint) && has(self.checkpoint.key) ? self.checkpoint.key : \"\") == (has(oldSelf.checkpoint) && has(oldSelf.checkpoint.key) ? oldSelf.checkpoint.key : \"\")",message="checkpoint.key is immutable"
// +kubebuilder:validation:XValidation:rule="!has(self.checkpoint) || !has(self.checkpoint.key) || self.checkpoint.key == \"\" || self.checkpoint.key.startsWith((has(self.target.prefix) ? self.target.prefix : \"\") + (has(self.sync) && has(self.sync.destPrefix) ? self.sync.destPrefix : \"\"))",message="checkpoint.key must live under the effective destination prefix (target.prefix + sync.destPrefix)"
type EtcdMirrorSpec struct {
	// Mode selects continuous replication (Sync, the default) or a cutover
	// drain (Drain). See EtcdMirrorModeDrain for the cutover contract.
	// +optional
	// +kubebuilder:validation:Enum=Sync;Drain
	// +kubebuilder:default=Sync
	Mode EtcdMirrorMode `json:"mode,omitempty"`

	// Source is the etcd cluster keys are read from. EtcdMirror never writes
	// back to Source; the agent's source-side client is only ever used for
	// Get/Watch, never Put/Delete/Txn.
	//
	// VERSION FLOOR: source etcd must be >= 3.4 (probed via maintenance
	// Status() at connect; below the floor the mirror goes Failed with reason
	// UnsupportedVersion). >= 3.4.25 / 3.5.8 is the recommended floor: below
	// it, watch progress notifications are unreliable and the agent cannot
	// trust the watermark machinery that drives lag, the checkpoint, and the
	// Drain gate.
	Source EtcdMirrorEndpoint `json:"source"`

	// Target is the etcd cluster keys are written into.
	//
	// SECURITY PREREQUISITE: Target's credential (etcd RBAC user and/or the
	// client certificate's role) MUST be range-scoped to the effective
	// destination prefix — never a cluster-admin-equivalent credential. The
	// agent's client-side rewrite logic is defense against bugs in its own
	// code, NOT a security boundary. The grant must also cover the reserved
	// checkpoint key (see Checkpoint). Configure via `etcdctl role
	// grant-permission --prefix=true <role> readwrite <dest-prefix>` before
	// pointing an EtcdMirror at the cluster.
	//
	// The target must run with auto-compaction enabled: forced-resync churn
	// and prune passes march an uncompacted target toward its storage quota
	// (2GiB by default), which surfaces as TargetQuotaExhausted.
	Target EtcdMirrorEndpoint `json:"target"`

	// InitialSync governs genesis behavior: how pre-existing destination keys
	// are treated (Mode) and optionally where replication starts
	// (StartRevision).
	// +optional
	InitialSync *EtcdMirrorInitialSyncSpec `json:"initialSync,omitempty"`

	// Sync tunes runtime sync behavior (batching, paging, rate limiting,
	// prefix rewrite, timeouts, backoff).
	// +optional
	Sync EtcdMirrorSyncSpec `json:"sync,omitempty"`

	// Checkpoint configures the reserved checkpoint/fence key on the target.
	// +optional
	Checkpoint *EtcdMirrorCheckpointSpec `json:"checkpoint,omitempty"`

	// Reconciliation optionally enables a periodic full diff-and-repair pass
	// layered on top of the continuous watch-based mirror. Independent of
	// this setting, one reconciliation-with-delete pass always runs after any
	// forced resync (mark-and-sweep), and as the OverwriteAndPrune genesis
	// pass and the Drain verification pass.
	// +optional
	Reconciliation *EtcdMirrorReconciliationSpec `json:"reconciliation,omitempty"`

	// PodTemplate carries scheduling/affinity/labels/annotations for the
	// agent pod, reusing EtcdClusterSpec's PodTemplate shape verbatim.
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`

	// Resources are the agent container's compute resources. The agent's
	// memory model is bounded by Sync.PageBytes (single in-flight scan page,
	// no unbounded read-ahead); size limits accordingly.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Paused, when true, scales the agent Deployment to zero without deleting
	// the CR or its checkpoint. The checkpoint lives in the target etcd, so
	// resume picks up from the last fenced watermark. NOTE: pausing longer
	// than the source's compaction retention guarantees a full forced resync
	// on resume — there is no free lunch past the retention window.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// EtcdMirrorInitialSyncSpec governs the genesis scan.
//
// +kubebuilder:validation:XValidation:rule="!(has(self.startRevision) && self.startRevision > 0 && (!has(self.mode) || self.mode == 'RequireEmpty'))",message="initialSync.startRevision requires initialSync.mode Overwrite or OverwriteAndPrune (a seeded target is not empty)"
type EtcdMirrorInitialSyncSpec struct {
	// Mode governs pre-existing destination keys at genesis. Defaults to
	// RequireEmpty (refuse a non-empty destination prefix).
	// +optional
	// +kubebuilder:validation:Enum=RequireEmpty;Overwrite;OverwriteAndPrune
	// +kubebuilder:default=RequireEmpty
	Mode EtcdMirrorInitialSyncMode `json:"mode,omitempty"`

	// StartRevision, when > 0, skips the genesis scan entirely and starts
	// watching from StartRevision+1. For fidelity-preserving seeds: restore
	// the target from a source snapshot (`etcdutl snapshot restore
	// --bump-revision --mark-compacted`), then mirror only the delta.
	// Requires Mode Overwrite or OverwriteAndPrune (CEL-enforced).
	// +optional
	// +kubebuilder:validation:Minimum=0
	StartRevision int64 `json:"startRevision,omitempty"`
}

// EtcdMirrorEndpoint describes how to reach, authenticate to, and scope one
// side (source or target) of a mirror. Both sides need an identical shape
// (address resolution + prefix + TLS + auth), so one type serves both roles,
// the same way BackupDestination is reused verbatim between EtcdBackup and
// EtcdRestore rather than forked into near-duplicate per-role types.
//
// Exactly one of EndpointList or ServiceRef must be set. An empty
// endpointList ([]) is treated as unset, per Kubernetes list conventions —
// so `endpointList: []` alongside a serviceRef is accepted.
//
// Endpoint scheme and the TLS block must agree (CEL-enforced both ways):
// http:// endpoints with a tls block would silently drop TLS at dial time;
// https:// endpoints without one would dial with undeclared system-roots
// TLS. The agent derives the dial scheme from the presence of the tls block,
// so the declared contract is true by construction.
//
// +kubebuilder:validation:XValidation:rule="(has(self.endpointList) && size(self.endpointList) > 0) != has(self.serviceRef)",message="exactly one of endpointList or serviceRef must be set"
// +kubebuilder:validation:XValidation:rule="!(has(self.tls) && has(self.endpointList) && self.endpointList.exists(e, e.startsWith('http://')))",message="http:// endpoints conflict with a tls block: use https:// endpoints or remove tls"
// +kubebuilder:validation:XValidation:rule="!(!has(self.tls) && has(self.endpointList) && self.endpointList.exists(e, e.startsWith('https://')))",message="https:// endpoints require a tls block (an empty tls block selects server-auth TLS against system trust roots)"
type EtcdMirrorEndpoint struct {
	// EndpointList is a raw set of etcd client-URL host:port (or
	// scheme://host:port) strings, e.g. "https://etcd-rke1.example.com:2379".
	// This is the ONLY supported mechanism for a cluster external to this
	// Kubernetes cluster (e.g. an RKE1/AWS source reached over a public NLB);
	// there is deliberately no tunnel/port-forward mode — terminate any
	// tunnel upstream and hand EtcdMirror the resulting stable endpoint(s).
	//
	// Prefer listing per-member endpoints over a single load-balancer VIP: a
	// TCP-health-checked VIP cannot see etcd quorum, and the client's own
	// balancer handles per-member failover.
	//
	// IP-LITERAL ENDPOINTS: Go's TLS stack requires an IP SAN (not a DNS SAN)
	// to verify a bare-IP endpoint. Either set EtcdMirrorTLS.ServerName to a
	// hostname present as a DNS SAN on the certificate, or ensure the
	// certificate carries a matching IP SAN. Fix the SAN/ServerName mismatch;
	// do not reach for InsecureSkipVerify.
	// +optional
	EndpointList []string `json:"endpointList,omitempty"`

	// ServiceRef points at a Kubernetes Service in this cluster whose DNS
	// name resolves the etcd client endpoint(s), for the same-cluster case.
	// Namespace defaults to the EtcdMirror's own namespace when empty.
	// +optional
	ServiceRef *EtcdMirrorServiceRef `json:"serviceRef,omitempty"`

	// Prefix is the etcd key prefix on THIS side. On Source, only keys under
	// this prefix are synced; empty means the whole keyspace. On Target, this
	// is the prefix under which mirrored keys land (see EtcdMirrorSyncSpec's
	// rewrite formula). Immutable after creation.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// TLS configures the agent's client TLS for THIS side. Nil means the
	// agent dials this side in cleartext (and https:// endpoints are
	// CEL-rejected). An empty block means server-auth TLS verified against
	// the system trust roots.
	// +optional
	TLS *EtcdMirrorTLS `json:"tls,omitempty"`

	// Auth configures etcd username/password (RBAC) auth for THIS side.
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

// EtcdMirrorTLS configures the mirror agent's client TLS for one side of a
// mirror. Plain secretRef, not a reuse of EtcdClusterTLS/TLSSurface — the
// agent is always a client to clusters it does not own, so issuer-selection
// machinery doesn't apply.
//
// ROTATION CONTRACT: the agent re-reads TLS material from the mounted Secret
// on every handshake (transport.TLSInfo file paths, not a one-shot
// tls.Config); certificate rotation requires no pod restart.
//
// +kubebuilder:validation:XValidation:rule="!self.insecureSkipVerify || self.insecureSkipVerifyAcknowledgeRisk",message="insecureSkipVerify requires insecureSkipVerifyAcknowledgeRisk to also be true"
// +kubebuilder:validation:XValidation:rule="!has(self.secretRef) || (has(self.secretRef.name) && size(self.secretRef.name) > 0)",message="secretRef.name must be non-empty when secretRef is set"
type EtcdMirrorTLS struct {
	// SecretRef names a Secret (in the EtcdMirror's namespace) holding this
	// side's TLS material in the standard kubernetes.io/tls-compatible shape:
	//   - ca.crt:  PEM CA bundle used to verify the peer's server certificate
	//     (unless CABundleRef overrides it, or InsecureSkipVerify is true).
	//   - tls.crt / tls.key: PEM client certificate + key, for mTLS.
	//     Optional — omit both for server-auth-only TLS.
	// Nil means no client identity and verification against the system trust
	// roots (the etcdctl default). Note a source running with
	// --client-cert-auth (the RKE1 default) rejects certless clients at the
	// handshake regardless of etcd RBAC auth; server-auth-only + Auth is not
	// viable against such a source.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// CABundleRef optionally sources the trust anchors from a separate
	// Secret or ConfigMap key, decoupling trust from the identity Secret
	// (Gateway API caCertificateRefs precedent). Takes precedence over
	// SecretRef's ca.crt.
	// +optional
	CABundleRef *EtcdMirrorCABundleRef `json:"caBundleRef,omitempty"`

	// InsecureSkipVerify disables server certificate verification. Strongly
	// discouraged, especially for a source reached over the public internet.
	// Requires InsecureSkipVerifyAcknowledgeRisk to also be set true
	// (CEL-enforced). The controller additionally emits a standing Warning
	// event whenever this is true.
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

	// ServerName overrides the TLS ServerName (SNI) used for verification,
	// for cases where the dialed address doesn't match a SAN on the
	// certificate (e.g. dialing an NLB IP directly). Applies to EVERY
	// endpoint in the list, so mixing endpoints with different certificates
	// behind one ServerName will fail verification on the mismatched ones.
	// +optional
	ServerName string `json:"serverName,omitempty"`
}

// EtcdMirrorCABundleRef points at one key of a Secret or ConfigMap holding a
// PEM CA bundle.
type EtcdMirrorCABundleRef struct {
	// Kind is Secret or ConfigMap. Defaults to ConfigMap.
	// +optional
	// +kubebuilder:validation:Enum=Secret;ConfigMap
	// +kubebuilder:default=ConfigMap
	Kind string `json:"kind,omitempty"`

	// Name of the Secret or ConfigMap, in the EtcdMirror's namespace.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key within the object. Defaults to "ca.crt".
	// +optional
	Key string `json:"key,omitempty"`
}

// EtcdMirrorAuth configures etcd RBAC username/password auth for one side.
//
// +kubebuilder:validation:XValidation:rule="has(self.secretRef.name) && size(self.secretRef.name) > 0",message="secretRef.name is required"
type EtcdMirrorAuth struct {
	// SecretRef names a Secret holding "username" and "password" keys. If
	// this whole Auth block is nil, the agent does not call etcd's
	// Authenticate() at all; the pinned v3 client transparently re-auths on
	// token expiry. PRECEDENCE: when both a client certificate and Auth are
	// supplied, etcd uses the token identity, not the certificate CN — the
	// Auth user must hold the range-scoped role.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// EtcdMirrorSyncSpec tunes the mirror's runtime sync behavior.
//
// KEY REWRITE — one formula, no other composition:
//
//	key' = target.prefix + destPrefix + TrimPrefix(key, source.prefix)
//
// (anchored strip-and-reprefix; never a substring replace).
//
// BATCHING INVARIANT: target Txns flush ONLY at source-revision boundaries —
// a source revision's events are never split across Txns, whole revisions
// are coalesced up to the MaxTxnOps/TxnFlushBytes watermarks, and one op
// slot in MaxTxnOps is always reserved for the checkpoint write that rides
// in the same Txn. A single source revision larger than MaxTxnOps is applied
// as one oversized Txn (provision the target's --max-txn-ops accordingly)
// with the checkpoint held until it lands.
//
// RETENTION PREREQUISITE: the source's compaction retention window must
// exceed the worst-case initial scan + throttled drain time, approximately
// sourceKeyCount / min(effective scan rate, MaxOpsPerSecond). If it does
// not, genesis (and every forced resync) loses the race with compaction and
// the mirror livelocks (surfaced via the resync-loop detector).
type EtcdMirrorSyncSpec struct {
	// DestPrefix is the middle term of the rewrite formula above. Default ""
	// means the source prefix is stripped and key remainders land directly
	// under target.prefix. Immutable after creation.
	// +optional
	DestPrefix string `json:"destPrefix,omitempty"`

	// ExcludePrefixes lists source key prefixes (full source-side keys, e.g.
	// "/registry/events/") skipped entirely: not scanned, not watched, not
	// counted, not pruned. Use to drop high-churn low-value ranges and cut
	// WAN cost, or to skip lease-backed ranges that don't survive mirroring.
	// Nested/duplicate entries are normalized by the agent (an entry covered
	// by another is dropped). Immutable after creation (it defines the
	// mirrored range): removing an exclusion would require a backfill scan
	// the checkpoint-resume path never runs, and adding one would strand
	// already-mirrored keys as permanent orphans — change it via
	// delete-and-recreate with an appropriate initialSync.mode instead.
	// +optional
	// +kubebuilder:validation:MaxItems=64
	ExcludePrefixes []string `json:"excludePrefixes,omitempty"`

	// MaxTxnOps bounds how many operations the agent batches into a single
	// target Txn, including the reserved checkpoint-write slot. Must not
	// exceed the target's --max-txn-ops (etcd default 128). Defaults to 128.
	// +optional
	// +kubebuilder:validation:Minimum=2
	MaxTxnOps int32 `json:"maxTxnOps,omitempty"`

	// TxnFlushBytes is the byte watermark at which a batch is flushed (at the
	// next source-revision boundary). Keep well under etcd's request size
	// limits: a Txn over ~1.5MiB is rejected by the server and one over 2MiB
	// by the client send cap — both classified permanent errors, not
	// throttling. Defaults to 1Mi.
	// +optional
	TxnFlushBytes *resource.Quantity `json:"txnFlushBytes,omitempty"`

	// PageKeyLimit bounds keys per source scan page during InitialSync and
	// reconciliation. The scan is pull-based, one page in flight — no
	// read-ahead — so this and PageBytes bound agent memory. Defaults to 512.
	// +optional
	// +kubebuilder:validation:Minimum=1
	PageKeyLimit int32 `json:"pageKeyLimit,omitempty"`

	// PageBytes bounds bytes per source scan page. Defaults to 1Mi.
	// +optional
	PageBytes *resource.Quantity `json:"pageBytes,omitempty"`

	// WatchBufferBytes bounds the memory used to buffer watch events
	// observed from R0 while the genesis scan runs (the reflector replay
	// buffer). On overflow the agent cancels the source watch and restarts
	// the scan from a fresh R0 (see the InitialSyncCompactionRaced event) —
	// a bounded retry instead of unbounded growth when source churn outruns
	// scan+apply throughput. Defaults to 16Mi (must stay in lockstep with
	// pkg/mirroragent's DefaultWatchBufferBytes).
	// +optional
	WatchBufferBytes *resource.Quantity `json:"watchBufferBytes,omitempty"`

	// MaxOpsPerSecond rate-limits the agent's target write rate (a token
	// bucket over puts+deletes/sec), applied to both the genesis scan and
	// watch-driven applies. Zero (default) means unlimited. Mind the
	// retention prerequisite above when throttling.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxOpsPerSecond int32 `json:"maxOpsPerSecond,omitempty"`

	// RequestTimeout is the per-RPC context deadline applied to every unary
	// call on both sides (watches excluded — they are long-lived by design
	// and covered by progress-notification liveness instead). Without it a
	// blackholed call through an NLB never errors and backoff never engages.
	// Defaults to 30s.
	// +optional
	RequestTimeout *metav1.Duration `json:"requestTimeout,omitempty"`

	// DialTimeout bounds establishing the initial client connection to each
	// side. Defaults to 10s.
	// +optional
	DialTimeout *metav1.Duration `json:"dialTimeout,omitempty"`

	// ReconnectBackoff bounds the retry/backoff loop wrapping connection-class
	// errors. Throttling-class errors (target rate rejection) use a more
	// conservative curve derived from the same bounds; quota exhaustion
	// (TargetQuotaExhausted) and permanent errors are never retried through
	// this loop. Defaults to exponential backoff from 1s to 30s.
	// +optional
	ReconnectBackoff *EtcdMirrorBackoffSpec `json:"reconnectBackoff,omitempty"`
}

type EtcdMirrorBackoffSpec struct {
	// +optional
	InitialDelay *metav1.Duration `json:"initialDelay,omitempty"`
	// +optional
	MaxDelay *metav1.Duration `json:"maxDelay,omitempty"`
}

// EtcdMirrorCheckpointSpec configures the reserved checkpoint/fence key the
// agent maintains IN THE TARGET etcd. The checkpoint (the source-revision
// watermark plus {linkUID, epoch, role}) is written in the SAME Txn as every
// applied batch and fenced with a mod-revision compare on EVERY write path
// (applies, reconciliation repairs, prune deletes), so two agents can never
// interleave writes and a straggler apply after cutover fails loudly. The
// key is excluded by exact match from scans, counts, prune passes, and the
// RequireEmpty check; the target RBAC grant must cover it; CR deletion
// removes it via a delete-one-key finalizer.
type EtcdMirrorCheckpointSpec struct {
	// Key overrides the reserved checkpoint key. Defaults to the effective
	// destination prefix + "\x00etcdmirror-checkpoint" — the \x00 byte after
	// the prefix cannot collide with any real key under it. MUST live under
	// the effective destination prefix (target.prefix + sync.destPrefix,
	// CEL-enforced): the range-scoped target credential covers it, and the
	// exact-match exclusion from scans/counts/prune only works inside the
	// mirrored range. Immutable after creation.
	// +optional
	Key string `json:"key,omitempty"`
}

// EtcdMirrorReconciliationSpec configures the periodic full reconciliation
// pass. The same engine also runs unconditionally (regardless of Enabled or
// DeleteOrphans) as the post-forced-resync mark-and-sweep, the
// OverwriteAndPrune genesis pass, and the Drain verification pass.
type EtcdMirrorReconciliationSpec struct {
	// Enabled toggles the PERIODIC pass. Defaults to false: it is a full
	// diff of the prefix contents on both sides (O(keyspace)), so it is
	// opt-in.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Interval between periodic passes. Defaults to 1h when Enabled.
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`

	// DeleteOrphans, when true, allows the PERIODIC pass to delete target
	// keys under the destination prefix that have no corresponding source
	// key. Defaults to false. (Forced-resync sweeps and OverwriteAndPrune
	// always delete orphans; this knob only governs the periodic pass.)
	// +optional
	DeleteOrphans bool `json:"deleteOrphans,omitempty"`
}

// EtcdMirrorPhase is a high-level summary of an EtcdMirror's lifecycle.
// Unlike BackupPhase/RestorePhase, most phases here are NOT terminal — a
// healthy mirror spends its life in Syncing; there is no "Completed" state.
type EtcdMirrorPhase string

const (
	// EtcdMirrorPhasePending means the EtcdMirror has been accepted but the
	// agent workload has not been created yet.
	EtcdMirrorPhasePending EtcdMirrorPhase = "Pending"
	// EtcdMirrorPhaseConnecting means the agent pod is running, establishing
	// client connections to both sides and probing versions/cluster IDs.
	EtcdMirrorPhaseConnecting EtcdMirrorPhase = "Connecting"
	// EtcdMirrorPhaseInitialSync means the agent is running the genesis scan:
	// an UNPINNED chunked scan with the watch already open from the revision
	// observed before the scan started, buffered events replayed over the
	// scanned base (reflector pattern). Because pages read at the current
	// revision, mid-scan compaction cannot fail the scan. Also entered during
	// a forced resync (then with condition Compacted=True/Reason=ForcedResync).
	EtcdMirrorPhaseInitialSync EtcdMirrorPhase = "InitialSync"
	// EtcdMirrorPhaseSyncing is the steady state: watching and applying live
	// changes, watermark advancing via progress notifications.
	EtcdMirrorPhaseSyncing EtcdMirrorPhase = "Syncing"
	// EtcdMirrorPhaseDegraded means the agent is in a retry/backoff loop
	// (connection or throttling class) and is expected to self-heal. Forced
	// resyncs are NOT Degraded; they report as InitialSync + Compacted=True.
	EtcdMirrorPhaseDegraded EtcdMirrorPhase = "Degraded"
	// EtcdMirrorPhasePaused means spec.paused is true; the agent Deployment
	// is scaled to zero. The checkpoint is retained in the target.
	EtcdMirrorPhasePaused EtcdMirrorPhase = "Paused"
	// EtcdMirrorPhaseFailed means a terminal, non-recoverable error requiring
	// operator intervention: EmptyTargetViolation at genesis, source below
	// the 3.4 version floor (UnsupportedVersion), a permanent-class write
	// error (oversized revision vs target limits), malformed cert material,
	// or unresolvable spec misconfiguration.
	EtcdMirrorPhaseFailed EtcdMirrorPhase = "Failed"
)

// Condition types reported on EtcdMirror status.
//
// PAGING ALGEBRA (for alert authors): page on Available=False sustained for
// your tolerance window UNLESS Compacted=True AND progress fields are
// advancing (a forced resync healing itself); TargetQuotaExhausted and
// ResyncLoopDetected page immediately — neither self-heals.
const (
	// EtcdMirrorConditionAvailable is True only when the agent pod is
	// running, in the Syncing phase, AND the checkpoint watermark is
	// advancing (via applies or watch progress notifications) within the
	// staleness threshold. Watermark-derived, not apply-derived: an idle
	// prefix on a live watch stays Available; a wedged loop that stops
	// confirming progress does not.
	EtcdMirrorConditionAvailable = "Available"
	// EtcdMirrorConditionSourceReachable is True when the agent's last
	// attempt to reach Source succeeded. Split from TargetReachable because
	// in the primary use case (source over the public internet) source
	// reachability is the most likely persistent failure mode.
	EtcdMirrorConditionSourceReachable = "SourceReachable"
	// EtcdMirrorConditionTargetReachable is the target-side analogue.
	EtcdMirrorConditionTargetReachable = "TargetReachable"
	// EtcdMirrorConditionTargetThrottled is True while the agent is backing
	// off from throttling-class errors on Target (rate rejection /
	// ErrTooManyRequests). Distinct from TargetReachable=False ("up but
	// rejecting my write rate" is actionable differently) and from
	// TargetQuotaExhausted (backoff cannot heal a full quota).
	EtcdMirrorConditionTargetThrottled = "TargetThrottled"
	// EtcdMirrorConditionTargetQuotaExhausted is True when a target write
	// failed with etcd's NOSPACE (rpctypes.ErrNoSpace). Permanent until an
	// operator compacts/defrags/disarms the target; the agent stops writing
	// rather than burning backoff against a full quota. Detected from the
	// typed write-path error, never AlarmList (which needs root).
	EtcdMirrorConditionTargetQuotaExhausted = "TargetQuotaExhausted"
	// EtcdMirrorConditionInitialSyncComplete is True once the genesis scan
	// has completed against the currently-checkpointed cluster identities.
	// Durable across forced resyncs (Compacted covers those); reset to False
	// when the checkpoint is invalidated by a mismatch of EITHER bound
	// cluster ID (source or target), which also re-arms the RequireEmpty
	// check.
	EtcdMirrorConditionInitialSyncComplete = "InitialSyncComplete"
	// EtcdMirrorConditionCompacted is True (Reason ForcedResync) while the
	// agent heals from source compaction outrunning the watch (restart or
	// pause longer than retention; a WatchResponse with CompactRevision !=
	// 0). Mid-scan compaction is NOT in this class — the unpinned scan is
	// immune by construction. Every forced resync ends with a mandatory
	// mark-and-sweep prune. Reverts to False when steady-state resumes.
	EtcdMirrorConditionCompacted = "Compacted"
	// EtcdMirrorConditionResyncLoopDetected is True when N consecutive forced
	// resyncs completed without reaching steady state — the livelock
	// signature of source retention < scan+drain time. Does not self-heal:
	// raise retention, raise MaxOpsPerSecond, or shrink the prefix.
	EtcdMirrorConditionResyncLoopDetected = "ResyncLoopDetected"
	// EtcdMirrorConditionReplicationLagExceeded is True when the checkpoint
	// watermark has stayed more than a threshold behind the source's
	// current revision for a sustained duration. Both terms come from the
	// same watch/progress machinery (never from comparing the two live
	// status fields, which snapshot at different instants). Threshold and
	// duration are controller-internal constants in v1.
	EtcdMirrorConditionReplicationLagExceeded = "ReplicationLagExceeded"
	// EtcdMirrorConditionDriftDetected is True when the last reconciliation
	// pass found orphaned/missing keys. Carries counts in Message. Sticky
	// until the next pass reports clean.
	EtcdMirrorConditionDriftDetected = "DriftDetected"
	// EtcdMirrorConditionEmptyTargetViolation is True (terminal, Phase ->
	// Failed) when initialSync.mode is RequireEmpty and the destination
	// prefix was non-empty at genesis. The condition Message embeds the
	// exact `etcdctl del` command for the offending range. The reserved
	// checkpoint key is excluded by exact match.
	EtcdMirrorConditionEmptyTargetViolation = "EmptyTargetViolation"
	// EtcdMirrorConditionCutoverReady is True when spec.mode is Drain, the
	// watermark has reached status.cutover.drainTargetRevision, and the
	// verification pass succeeded. From then the fence key's role is
	// Primary and any straggler mirror apply fails its compare. Gate
	// promotion on `kubectl wait --for=condition=CutoverReady`.
	EtcdMirrorConditionCutoverReady = "CutoverReady"
	// EtcdMirrorConditionInvariantsHeld is the composed verification verdict:
	// True when ReplicationLagExceeded is False, the per-side key counts
	// (status.sourceKeyCount/targetKeyCount, reserved key excluded) are
	// equal, DriftDetected is False, and the pass that produced the counts
	// is fresh. Freshness: with the periodic reconciliation pass enabled,
	// within 2x spec.reconciliation.interval; with it disabled the counts
	// only refresh on mandatory passes (forced-resync sweeps,
	// OverwriteAndPrune genesis, drain verification), so the condition is
	// Unknown/stale-reasoned once the last such pass ages out — enable
	// reconciliation to make this a continuous signal. It means
	// "verification invariants hold", never "safe to cut over" — cutover is
	// gated on CutoverReady, which additionally requires spec.mode Drain and
	// a reached drainTargetRevision.
	EtcdMirrorConditionInvariantsHeld = "InvariantsHeld"
	// EtcdMirrorConditionLearnerEndpoint is True when the maintenance
	// Status() probe reports IsLearner=true for a configured endpoint.
	// Non-blocking (learners self-heal and the balancer routes around them),
	// but a learner pick can serve stale reads mid-catch-up.
	EtcdMirrorConditionLearnerEndpoint = "LearnerEndpoint"
	// EtcdMirrorConditionPrefixConflict is controller-set: another
	// EtcdMirror targets an overlapping effective destination range on the
	// same target cluster. Declared now so the name is API contract before
	// any setter exists. Independent of the controller check, the agent
	// itself refuses (permanently, Phase=Failed) to prune a reserved fence
	// key owned by a different link, so an undetected overlap stops loudly
	// instead of destroying the sibling mirror's fence and data.
	EtcdMirrorConditionPrefixConflict = "PrefixConflict"
	// EtcdMirrorConditionDirectionConflict is controller-set: this
	// mirror's bound source/target cluster IDs are the inverse of another
	// EtcdMirror's — two CRs forming a two-way loop, caught by cluster-ID
	// binding even when a respelled endpoint string would fool a spec
	// comparison.
	EtcdMirrorConditionDirectionConflict = "DirectionConflict"
)

// Condition reasons and Event reasons that are part of the API contract.
const (
	// EtcdMirrorReasonForcedResync is the Compacted condition's reason while
	// a forced resync is in flight.
	EtcdMirrorReasonForcedResync = "ForcedResync"
	// EtcdMirrorReasonUnsupportedVersion is the Failed-phase reason when the
	// source is below the 3.4 floor.
	EtcdMirrorReasonUnsupportedVersion = "UnsupportedVersion"

	// EtcdMirrorReasonCompacted / EtcdMirrorReasonClusterIDMismatch name WHY
	// a forced resync was required (mirroring pkg/mirroragent's ResyncReason
	// values). Surfaces: the Compacted condition's MESSAGE and the
	// forced-resync events' messages (and, later, the forced-resync metric's
	// reason label). The Compacted condition's Reason is always ForcedResync
	// while a resync is in flight — alert on that, not on these — and
	// forcedResyncCount is a plain counter with no per-reason breakdown. The
	// ClusterIDMismatch message additionally names the mismatched side.
	EtcdMirrorReasonCompacted         = "Compacted"
	EtcdMirrorReasonClusterIDMismatch = "ClusterIDMismatch"
	// EtcdMirrorReasonCheckpointInvalid is a Failed-phase reason: the stored
	// checkpoint was corrupt or of an unknown wire version. PERMANENT — the
	// agent fails closed and never auto-resyncs; the operator must inspect
	// and delete the reserved key to recover.
	EtcdMirrorReasonCheckpointInvalid = "CheckpointInvalid"

	// EtcdMirrorEventForcedResyncStarted / Completed bracket every forced
	// resync (Warning/Normal respectively).
	EtcdMirrorEventForcedResyncStarted   = "ForcedResyncStarted"
	EtcdMirrorEventForcedResyncCompleted = "ForcedResyncCompleted"
	// EtcdMirrorEventCheckpointInvalidated is emitted when the checkpoint is
	// discarded because a bound cluster ID (source or target) no longer
	// matches, forcing genesis and re-arming the RequireEmpty check.
	EtcdMirrorEventCheckpointInvalidated = "CheckpointInvalidated"
	// EtcdMirrorEventInitialSyncCompactionRaced marks an InitialSync attempt
	// aborted and restarted from a fresh R0. Two causes, named in the event
	// message: WatchBufferOverflow (the replay buffer exceeded
	// sync.watchBufferBytes before the base scan completed — a memory-bound
	// retry, NOT a compaction race) and WatchCompactedMidScan (a watch
	// reconnect landed below the source compact revision — the rare genuine
	// race). One event name so operators can alert on scan restarts; the
	// cause string prevents conflating "buffer too small for churn" with
	// "compaction won a race the design eliminates". Repeated occurrences
	// count toward ResyncLoopDetected.
	EtcdMirrorEventInitialSyncCompactionRaced = "InitialSyncCompactionRaced"
	// EtcdMirrorEventCertificateExpiringSoon warns that a referenced TLS
	// certificate (client leaf or CA) expires within the controller's
	// lead window; the agent's expiry gauge is the metric counterpart.
	EtcdMirrorEventCertificateExpiringSoon = "CertificateExpiringSoon"
	// EtcdMirrorEventInsecureSkipVerifyEnabled is the standing Warning
	// promised by EtcdMirrorTLS.InsecureSkipVerify's contract.
	EtcdMirrorEventInsecureSkipVerifyEnabled = "InsecureSkipVerifyEnabled"
)

// EtcdMirrorStatus defines the observed state of an EtcdMirror. Progress
// fields are synced periodically (not per-op), so they are a coarse,
// point-in-time mirror of the agent's authoritative checkpoint in the target
// etcd, useful for kubectl/dashboards, not an audit log.
//
// CUTOVER GATE CONTRACT: never compute "caught up" by comparing
// SourceRevision to LastAppliedRevision — they snapshot at different
// instants, and SourceRevision advances on out-of-prefix writes (revisions
// are cluster-global). The manual gate is: quiesce source writers, read the
// source's current revision R yourself (`etcdctl endpoint status`), then
// poll until LastAppliedRevision >= R. The in-CR gate is spec.mode=Drain +
// the CutoverReady condition. Relax target RBAC only after the mirror is
// paused or deleted.
type EtcdMirrorStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	Phase EtcdMirrorPhase `json:"phase,omitempty"`

	// LastAppliedRevision is the checkpoint watermark: the source revision
	// through which the target is caught up, advanced by applies AND by
	// watch progress notifications on idle prefixes. The fenced checkpoint
	// key in the target etcd is the authoritative copy; this mirrors it.
	// +optional
	LastAppliedRevision int64 `json:"lastAppliedRevision,omitempty"`

	// SourceRevision is the source cluster's revision as of the last status
	// sync. Cluster-global: it advances on writes outside the mirrored
	// prefix, so SourceRevision - LastAppliedRevision OVERSTATES lag for
	// prefix-scoped mirrors. See the cutover gate contract above.
	// +optional
	SourceRevision int64 `json:"sourceRevision,omitempty"`

	// SourceClusterID and TargetClusterID are the cluster IDs both bound
	// into the checkpoint. Either changing across reconciles is the visible
	// symptom of an endpoint now pointing at a different cluster than the
	// checkpoint was taken against (CheckpointInvalidated event, forced
	// genesis, RequireEmpty re-armed).
	// +optional
	SourceClusterID string `json:"sourceClusterID,omitempty"`
	// +optional
	TargetClusterID string `json:"targetClusterID,omitempty"`

	// SourceVersion and TargetVersion are the etcd server versions from the
	// maintenance Status() probe at connect.
	// +optional
	SourceVersion string `json:"sourceVersion,omitempty"`
	// +optional
	TargetVersion string `json:"targetVersion,omitempty"`

	// InitialSyncKeyCount is the number of keys applied so far by the
	// current/last genesis scan. Live-updating during InitialSync (each
	// status sync), so InitialSyncKeyCount/InitialSyncTotalKeyCount is a
	// progress fraction.
	// +optional
	InitialSyncKeyCount int64 `json:"initialSyncKeyCount,omitempty"`
	// InitialSyncTotalKeyCount is the denominator: the source-side key count
	// under the prefix observed at scan start (first page's RangeResponse
	// count).
	// +optional
	InitialSyncTotalKeyCount int64 `json:"initialSyncTotalKeyCount,omitempty"`
	// +optional
	InitialSyncStartTime *metav1.Time `json:"initialSyncStartTime,omitempty"`
	// +optional
	InitialSyncCompletionTime *metav1.Time `json:"initialSyncCompletionTime,omitempty"`

	// LeaseBackedKeyCount is the number of mirrored keys whose source copy is
	// lease-backed (kv.Lease != 0). Mirrored copies are NOT lease-backed —
	// leases are stripped (see Fidelity Caveats in docs/etcdmirror.md) — so a
	// nonzero count means the cutover runbook's purge/re-lease step applies.
	// +optional
	LeaseBackedKeyCount int64 `json:"leaseBackedKeyCount,omitempty"`

	// ForcedResyncCount counts forced resyncs (compaction outran the watch,
	// checkpoint invalidated by a cluster-ID mismatch, or checkpoint
	// corrupt/unknown-version). Monotonic, never reset.
	// +optional
	ForcedResyncCount int32 `json:"forcedResyncCount,omitempty"`

	// ScanRestartCount counts genesis-scan attempts aborted and restarted
	// from a fresh R0 (watch-buffer overflow or a mid-scan watch compaction;
	// see the InitialSyncCompactionRaced event). Monotonic, never reset.
	// +optional
	ScanRestartCount int64 `json:"scanRestartCount,omitempty"`

	// LastReconciliationTime and LastReconciliationDrift record the most
	// recent reconciliation pass (periodic or mandatory).
	// +optional
	LastReconciliationTime *metav1.Time `json:"lastReconciliationTime,omitempty"`
	// +optional
	LastReconciliationDrift *EtcdMirrorDriftInfo `json:"lastReconciliationDrift,omitempty"`

	// SourceKeyCount and TargetKeyCount are the per-side key counts from the
	// most recent diff/verification pass (reserved checkpoint key and
	// excluded prefixes not counted; drain-verification source reads pinned
	// at the drained revision with a compacted-fallback re-read). Populated
	// by every pass that runs regardless of spec.reconciliation.enabled —
	// the mandatory mark-and-sweep after any forced resync, the
	// OverwriteAndPrune genesis pass, and the drain verification — plus the
	// periodic pass when it is enabled. NOT refreshed on every status sync:
	// a healthy RequireEmpty mirror that never forces a resync only gets
	// counts from an enabled periodic pass. This is the equality signal
	// InvariantsHeld reads; status.cutover's copies remain the frozen
	// drain-time snapshot.
	// +optional
	SourceKeyCount int64 `json:"sourceKeyCount,omitempty"`
	// +optional
	TargetKeyCount int64 `json:"targetKeyCount,omitempty"`

	// LastStatusSyncTime is when status was last refreshed from the agent, so
	// staleness of everything above is directly observable.
	// +optional
	LastStatusSyncTime *metav1.Time `json:"lastStatusSyncTime,omitempty"`

	// LastProgressTime is when the watermark last advanced (apply or watch
	// progress notification). Distinct from LastStatusSyncTime: status can
	// keep syncing from a wedged loop; this field is what Available and
	// ReplicationLagExceeded are derived from.
	// +optional
	LastProgressTime *metav1.Time `json:"lastProgressTime,omitempty"`

	// Cutover is populated while spec.mode is Drain.
	// +optional
	Cutover *EtcdMirrorCutoverStatus `json:"cutover,omitempty"`

	// AgentPod is the name of the current agent pod (the sole pod of the
	// size-1 Deployment), for convenient kubectl logs/exec.
	// +optional
	AgentPod string `json:"agentPod,omitempty"`

	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// EtcdMirrorCutoverStatus tracks a Drain-mode cutover.
type EtcdMirrorCutoverStatus struct {
	// DrainTargetRevision is the source revision observed when Drain was
	// requested — the revision the watermark must reach.
	// +optional
	DrainTargetRevision int64 `json:"drainTargetRevision,omitempty"`
	// DrainedRevision is the watermark at which the drain completed.
	// +optional
	DrainedRevision int64 `json:"drainedRevision,omitempty"`
	// VerifiedTime is when the post-drain verification pass succeeded.
	// +optional
	VerifiedTime *metav1.Time `json:"verifiedTime,omitempty"`
	// SourceKeyCount and TargetKeyCount are the per-side key counts from the
	// verification pass (source read pinned at the drained revision;
	// reserved checkpoint key excluded).
	// +optional
	SourceKeyCount int64 `json:"sourceKeyCount,omitempty"`
	// +optional
	TargetKeyCount int64 `json:"targetKeyCount,omitempty"`
	// LeasedKeyCount is LeaseBackedKeyCount frozen at drain completion, for
	// the runbook's purge/re-lease step.
	// +optional
	LeasedKeyCount int64 `json:"leasedKeyCount,omitempty"`
}

type EtcdMirrorDriftInfo struct {
	// MissingKeys were present on the source but absent on the target.
	MissingKeys int64 `json:"missingKeys,omitempty"`
	// DivergentKeys were present on both sides with different values —
	// distinct from MissingKeys so "a resync dropped keys" is never
	// conflated with "a blind window went stale".
	DivergentKeys int64 `json:"divergentKeys,omitempty"`
	// OrphanKeys were present on the target with no source counterpart.
	OrphanKeys int64 `json:"orphanKeys,omitempty"`
	// Repaired is true when the pass wrote fixes rather than only reporting.
	Repaired bool `json:"repaired,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Available",type=string,JSONPath=`.status.conditions[?(@.type=="Available")].status`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Revision",type=integer,JSONPath=`.status.lastAppliedRevision`
// +kubebuilder:printcolumn:name="Source-Rev",type=integer,JSONPath=`.status.sourceRevision`
// +kubebuilder:printcolumn:name="Last-Progress",type=date,JSONPath=`.status.lastProgressTime`
// +kubebuilder:printcolumn:name="Source",type=string,priority=1,JSONPath=`.spec.source.prefix`
// +kubebuilder:printcolumn:name="Target",type=string,priority=1,JSONPath=`.spec.target.prefix`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdMirror is the Schema for the etcdmirrors API. It describes a
// continuous, one-way key-range sync from a source etcd cluster to a target
// etcd cluster, run as a single supervised stateless pod (a size-1
// Deployment; progress lives in a fenced checkpoint key in the target etcd,
// not on a volume).
//
// It is a byte-copy of keys and values, not a replica: revisions, versions,
// and create/mod ordering are target-assigned, and leases are stripped. See
// Fidelity Caveats in docs/etcdmirror.md before depending on anything but
// key/value content.
//
// Two-way sync: never — etcd revisions are cluster-local and there is no
// per-key provenance channel, so bidirectional sync is structurally
// inexpressible. Reversal for cutover/failback: yes — delete the CR and
// create a new one with swapped endpoints, initialSync.mode
// OverwriteAndPrune, only after the forward mirror reported CutoverReady.
// See docs/etcdmirror.md.
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
