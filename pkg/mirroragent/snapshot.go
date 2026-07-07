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

package mirroragent

import "time"

// Phase mirrors EtcdMirrorPhase in api/v1alpha1 (the library adds Drained,
// which the controller maps to the CutoverReady condition).
type Phase string

const (
	PhaseConnecting  Phase = "Connecting"
	PhaseInitialSync Phase = "InitialSync"
	PhaseSyncing     Phase = "Syncing"
	PhaseDegraded    Phase = "Degraded"
	PhaseFailed      Phase = "Failed"
	// PhaseDrained means the drain completed, verification passed, and the
	// fence role is Primary. Run has returned; the agent will never write
	// again.
	PhaseDrained Phase = "Drained"
)

// ScanRestartCause says why a genesis scan attempt was aborted and restarted
// from a fresh R0. Both causes surface as one operator-facing event
// (InitialSyncCompactionRaced) with the cause named in the message, so
// "buffer too small for churn" is never conflated with "compaction won a
// race the design eliminates". Repeated restarts count toward the
// resync-loop detector.
type ScanRestartCause string

const (
	// ScanRestartWatchBufferOverflow: the replay buffer exceeded
	// Config.WatchBufferBytes before the base scan completed — a
	// memory-bound retry, NOT a compaction race.
	ScanRestartWatchBufferOverflow ScanRestartCause = "WatchBufferOverflow"
	// ScanRestartWatchCompactedMidScan: a watch reconnect landed below the
	// source compact revision while the scan was still running — the rare
	// genuine race.
	ScanRestartWatchCompactedMidScan ScanRestartCause = "WatchCompactedMidScan"
)

// Drift is the outcome of one reconciliation pass.
type Drift struct {
	// MissingKeys were present on the source but absent on the target
	// (repaired when the pass repairs).
	MissingKeys int64 `json:"missingKeys"`
	// DivergentKeys were present on both sides with different values
	// (repaired to source truth when the pass repairs). Distinct from
	// MissingKeys so operators can tell "a resync dropped keys" from "a
	// blind window went stale".
	DivergentKeys int64 `json:"divergentKeys"`
	// OrphanKeys were present on the target with no source counterpart
	// (deleted when the pass deletes orphans).
	OrphanKeys int64 `json:"orphanKeys"`
	// Repaired is true when the pass wrote fixes rather than only reporting.
	Repaired bool `json:"repaired"`
}

// CutoverStatus tracks a Drain-mode cutover; see EtcdMirrorCutoverStatus.
type CutoverStatus struct {
	// DrainTargetRevision is the source revision observed when the drain
	// started — the revision the watermark must reach.
	DrainTargetRevision int64 `json:"drainTargetRevision"`
	// DrainedRevision is the watermark at which the drain completed.
	DrainedRevision int64 `json:"drainedRevision"`
	// VerifiedTime is when the post-drain verification pass succeeded.
	VerifiedTime time.Time `json:"verifiedTime,omitzero"`
	// SourceKeyCount / TargetKeyCount are the per-side key counts from the
	// verification pass (source read pinned at DrainedRevision with an
	// unpinned fallback if compacted; excluded prefixes and the reserved
	// checkpoint key are not counted).
	SourceKeyCount int64 `json:"sourceKeyCount"`
	TargetKeyCount int64 `json:"targetKeyCount"`
	// LeasedKeyCount is the lease-backed key count frozen at drain
	// completion, for the runbook's purge/re-lease step.
	LeasedKeyCount int64 `json:"leasedKeyCount"`
}

// Snapshot is a point-in-time copy of the agent's state, safe to retain.
// Later rungs (the agent binary's /statusz, the controller's status sync)
// poll this instead of scraping internals.
//
// The JSON tags ARE the /statusz wire contract: the agent binary marshals a
// Snapshot verbatim and the controller decodes into this same type, so the
// two rungs cannot drift. Zero times are omitted (omitzero = "not yet").
type Snapshot struct {
	Phase Phase `json:"phase"`

	SourceVersion string `json:"sourceVersion"`
	TargetVersion string `json:"targetVersion"`
	// SourceClusterID / TargetClusterID as probed at connect (0 = not yet
	// probed). Both are bound into the checkpoint.
	SourceClusterID uint64 `json:"sourceClusterID"`
	TargetClusterID uint64 `json:"targetClusterID"`

	// Watermark is the checkpoint watermark: the source revision through
	// which the target is caught up, advanced by applies AND by watch
	// progress notifications on idle prefixes. The fenced checkpoint key in
	// the target etcd is the authoritative copy; this mirrors it.
	Watermark int64 `json:"watermark"`
	// SourceRevision is the source cluster's revision as of the last watch
	// header. Cluster-global: it advances on out-of-prefix writes, so
	// SourceRevision-Watermark overstates lag for prefix-scoped mirrors.
	SourceRevision int64 `json:"sourceRevision"`
	// LastProgressTime is when the watermark last advanced. This — not
	// apply activity — is the liveness signal: an idle prefix on a live
	// watch keeps progressing via notifications.
	LastProgressTime time.Time `json:"lastProgressTime,omitzero"`

	InitialSyncKeyCount       int64     `json:"initialSyncKeyCount"`
	InitialSyncTotalKeyCount  int64     `json:"initialSyncTotalKeyCount"`
	InitialSyncStartTime      time.Time `json:"initialSyncStartTime,omitzero"`
	InitialSyncCompletionTime time.Time `json:"initialSyncCompletionTime,omitzero"`

	// KeysAppliedTotal is the monotonic count of data operations (puts plus
	// deletes; the checkpoint Put rides free) committed to the target in
	// fenced Txns, across scans, tails, repairs, and prunes. The denominator
	// for apply-rate metrics.
	KeysAppliedTotal int64 `json:"keysAppliedTotal"`

	// LeaseBackedKeyCount is the number of mirrored keys whose source copy
	// is lease-backed (kv.Lease != 0). Mirrored copies are NOT lease-backed
	// — leases are stripped — so a nonzero count means the cutover
	// runbook's purge/re-lease step applies.
	LeaseBackedKeyCount int64 `json:"leaseBackedKeyCount"`

	// ForcedResyncCount is monotonic, never reset. LastResyncReason is the
	// most recent trigger. ForcedResyncCountByReason splits the same count
	// by trigger (the labeled-counter surface; values sum to
	// ForcedResyncCount). ResyncLoopDetected latches when
	// ResyncLoopThreshold consecutive resyncs completed without reaching
	// steady state (the livelock signature of retention < scan+drain time);
	// it clears only when steady state is reached.
	ForcedResyncCount         int64                  `json:"forcedResyncCount"`
	ForcedResyncCountByReason map[ResyncReason]int64 `json:"forcedResyncCountByReason,omitempty"`
	LastResyncReason          ResyncReason           `json:"lastResyncReason"`
	ResyncLoopDetected        bool                   `json:"resyncLoopDetected"`

	// ScanRestartCount is monotonic: genesis scan attempts aborted and
	// restarted from a fresh R0 (see ScanRestartCause). Distinct from
	// ForcedResyncCount — a restart is a bounded retry within InitialSync,
	// not a checkpoint invalidation — but restarts count toward the same
	// resync-loop detector.
	ScanRestartCount     int64            `json:"scanRestartCount"`
	LastScanRestartCause ScanRestartCause `json:"lastScanRestartCause"`

	// SourceKeyCount / TargetKeyCount are the per-side in-scope key counts
	// observed by the most recent reconciliation, prune, or drain
	// verification pass (excluded prefixes and the reserved checkpoint key
	// not counted). Populated by every pass that runs regardless of config —
	// forced-resync sweeps, the OverwriteAndPrune genesis pass, and drain
	// verification — plus the periodic pass when Config.ReconcileInterval
	// enables it, but NOT refreshed outside those passes: a healthy
	// mirror that never resyncs only gets counts from an enabled periodic
	// pass. This is the equality signal the controller's InvariantsHeld
	// condition reads.
	SourceKeyCount int64 `json:"sourceKeyCount"`
	TargetKeyCount int64 `json:"targetKeyCount"`

	// Condition-shaped flags.
	Throttled      bool `json:"throttled"`
	QuotaExhausted bool `json:"quotaExhausted"`
	Compacted      bool `json:"compacted"`

	// LastReconcileTime is when the most recent reconciliation/verification
	// pass completed — periodic or mandatory, including the count-only drain
	// verification — the freshness input to the controller's InvariantsHeld
	// condition. LastReconcileDrift is the most recent FULL diff's outcome;
	// count-only verifications never overwrite it (a count check cannot
	// attest DivergentKeys).
	LastReconcileTime  time.Time `json:"lastReconcileTime,omitzero"`
	LastReconcileDrift *Drift    `json:"lastReconcileDrift,omitempty"`

	// LastError / LastErrorClass describe the most recent classified
	// failure ("" when the last attempt succeeded).
	LastError      string `json:"lastError"`
	LastErrorClass Class  `json:"lastErrorClass"`

	// Cutover is populated once a drain starts; CutoverReady flips when the
	// fence role is Primary.
	CutoverReady bool           `json:"cutoverReady"`
	Cutover      *CutoverStatus `json:"cutover,omitempty"`
}
