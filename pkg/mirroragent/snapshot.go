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
	MissingKeys int64
	// DivergentKeys were present on both sides with different values
	// (repaired to source truth when the pass repairs). Distinct from
	// MissingKeys so operators can tell "a resync dropped keys" from "a
	// blind window went stale".
	DivergentKeys int64
	// OrphanKeys were present on the target with no source counterpart
	// (deleted when the pass deletes orphans).
	OrphanKeys int64
	// Repaired is true when the pass wrote fixes rather than only reporting.
	Repaired bool
}

// CutoverStatus tracks a Drain-mode cutover; see EtcdMirrorCutoverStatus.
type CutoverStatus struct {
	// DrainTargetRevision is the source revision observed when the drain
	// started — the revision the watermark must reach.
	DrainTargetRevision int64
	// DrainedRevision is the watermark at which the drain completed.
	DrainedRevision int64
	// VerifiedTime is when the post-drain verification pass succeeded.
	VerifiedTime time.Time
	// SourceKeyCount / TargetKeyCount are the per-side key counts from the
	// verification pass (source read pinned at DrainedRevision with an
	// unpinned fallback if compacted; excluded prefixes and the reserved
	// checkpoint key are not counted).
	SourceKeyCount int64
	TargetKeyCount int64
	// LeasedKeyCount is the lease-backed key count frozen at drain
	// completion, for the runbook's purge/re-lease step.
	LeasedKeyCount int64
}

// Snapshot is a point-in-time copy of the agent's state, safe to retain.
// Later rungs (the agent binary's /statusz, the controller's status sync)
// poll this instead of scraping internals.
type Snapshot struct {
	Phase Phase

	SourceVersion string
	TargetVersion string
	// SourceClusterID / TargetClusterID as probed at connect (0 = not yet
	// probed). Both are bound into the checkpoint.
	SourceClusterID uint64
	TargetClusterID uint64

	// Watermark is the checkpoint watermark: the source revision through
	// which the target is caught up, advanced by applies AND by watch
	// progress notifications on idle prefixes. The fenced checkpoint key in
	// the target etcd is the authoritative copy; this mirrors it.
	Watermark int64
	// SourceRevision is the source cluster's revision as of the last watch
	// header. Cluster-global: it advances on out-of-prefix writes, so
	// SourceRevision-Watermark overstates lag for prefix-scoped mirrors.
	SourceRevision int64
	// LastProgressTime is when the watermark last advanced. This — not
	// apply activity — is the liveness signal: an idle prefix on a live
	// watch keeps progressing via notifications.
	LastProgressTime time.Time

	InitialSyncKeyCount       int64
	InitialSyncTotalKeyCount  int64
	InitialSyncStartTime      time.Time
	InitialSyncCompletionTime time.Time

	// LeaseBackedKeyCount is the number of mirrored keys whose source copy
	// is lease-backed (kv.Lease != 0). Mirrored copies are NOT lease-backed
	// — leases are stripped — so a nonzero count means the cutover
	// runbook's purge/re-lease step applies.
	LeaseBackedKeyCount int64

	// ForcedResyncCount is monotonic, never reset. LastResyncReason is the
	// most recent trigger. ResyncLoopDetected latches when
	// ResyncLoopThreshold consecutive resyncs completed without reaching
	// steady state (the livelock signature of retention < scan+drain time);
	// it clears only when steady state is reached.
	ForcedResyncCount  int64
	LastResyncReason   ResyncReason
	ResyncLoopDetected bool

	// ScanRestartCount is monotonic: genesis scan attempts aborted and
	// restarted from a fresh R0 (see ScanRestartCause). Distinct from
	// ForcedResyncCount — a restart is a bounded retry within InitialSync,
	// not a checkpoint invalidation — but restarts count toward the same
	// resync-loop detector.
	ScanRestartCount     int64
	LastScanRestartCause ScanRestartCause

	// SourceKeyCount / TargetKeyCount are the per-side in-scope key counts
	// observed by the most recent reconciliation, prune, or drain
	// verification pass (excluded prefixes and the reserved checkpoint key
	// not counted). Populated by every pass that runs regardless of config —
	// forced-resync sweeps, the OverwriteAndPrune genesis pass, drain
	// verification, and the periodic pass when Config.ReconcileInterval
	// enables it — but NOT refreshed outside those passes: a healthy
	// mirror that never resyncs only gets counts from an enabled periodic
	// pass. This is the equality signal the controller's InvariantsHeld
	// condition reads.
	SourceKeyCount int64
	TargetKeyCount int64

	// Condition-shaped flags.
	Throttled      bool
	QuotaExhausted bool
	Compacted      bool

	// LastReconcileTime is when the most recent reconciliation/verification
	// pass completed — periodic or mandatory, including the count-only drain
	// verification — the freshness input to the controller's InvariantsHeld
	// condition. LastReconcileDrift is the most recent FULL diff's outcome;
	// count-only verifications never overwrite it (a count check cannot
	// attest DivergentKeys).
	LastReconcileTime  time.Time
	LastReconcileDrift *Drift

	// LastError / LastErrorClass describe the most recent classified
	// failure ("" when the last attempt succeeded).
	LastError      string
	LastErrorClass Class

	// Cutover is populated once a drain starts; CutoverReady flips when the
	// fence role is Primary.
	CutoverReady bool
	Cutover      *CutoverStatus
}
