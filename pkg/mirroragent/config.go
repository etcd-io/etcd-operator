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

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Mode selects the agent's operating mode. Mirrors EtcdMirrorMode in
// api/v1alpha1.
type Mode string

const (
	// ModeSync is normal continuous replication.
	ModeSync Mode = "Sync"
	// ModeDrain prepares for cutover: the agent records the source revision
	// observed when the drain starts, keeps replicating until the checkpoint
	// watermark reaches it, runs a verification pass, then flips the fence
	// key's role to Primary so any straggler apply fails its mod-revision
	// compare loudly, and returns from Run.
	ModeDrain Mode = "Drain"
)

// InitialSyncMode governs pre-existing keys under the effective destination
// prefix at genesis. Mirrors EtcdMirrorInitialSyncMode in api/v1alpha1.
type InitialSyncMode string

const (
	// InitialSyncRequireEmpty refuses to start if the destination prefix
	// already holds any key (the reserved checkpoint key is excluded by
	// exact match). Re-arms whenever the checkpoint is invalidated by a
	// cluster-identity mismatch.
	InitialSyncRequireEmpty InitialSyncMode = "RequireEmpty"
	// InitialSyncOverwrite scans and writes over whatever is there. Keys
	// present on the target but absent on the source are left alone.
	InitialSyncOverwrite InitialSyncMode = "Overwrite"
	// InitialSyncOverwriteAndPrune is Overwrite plus one mandatory
	// orphan-prune pass after the scan, making reversal onto a
	// previously-populated prefix (failback) a first-class correct operation.
	InitialSyncOverwriteAndPrune InitialSyncMode = "OverwriteAndPrune"
)

// DefaultCheckpointKeySuffix is appended to the effective destination prefix
// to form the default reserved checkpoint key. The \x00 byte after the
// prefix cannot collide with any real key under it.
const DefaultCheckpointKeySuffix = "\x00etcdmirror-checkpoint"

// Defaults, matching the CRD field defaults in api/v1alpha1.
const (
	DefaultMaxTxnOps      = 128
	DefaultTxnFlushBytes  = 1 << 20 // 1Mi
	DefaultPageKeyLimit   = 512
	DefaultPageBytes      = 1 << 20 // 1Mi
	DefaultRequestTimeout = 30 * time.Second
	DefaultBackoffInitial = 1 * time.Second
	DefaultBackoffMax     = 30 * time.Second
	// DefaultReconcilePeriod is the spec→Config translation default that
	// cmd/mirror-agent applies for spec.reconciliation.enabled with a nil
	// interval; the engine itself treats 0 as disabled.
	DefaultReconcilePeriod = time.Hour

	// DefaultWatchBufferBytes bounds the in-memory replay buffer for watch
	// events observed from R0 while the genesis scan runs. Must stay in
	// lockstep with the EtcdMirror CRD's spec.sync.watchBufferBytes default.
	DefaultWatchBufferBytes = 16 << 20 // 16Mi
	// DefaultProgressInterval is how often the agent issues a client-driven
	// RequestProgress on the source watch (server-side notify intervals are
	// uncontrollable on foreign clusters).
	DefaultProgressInterval = 45 * time.Second
	// DefaultResyncLoopThreshold is how many consecutive forced resyncs
	// (without reaching steady state in between) trip the livelock detector
	// — the signature of source retention < scan+drain time.
	DefaultResyncLoopThreshold = 3
	// DefaultQuotaProbeInterval is how often a quota-exhausted (NOSPACE)
	// agent re-probes the target. Deliberately a slow flat poll, not
	// backoff: quota exhaustion only heals when an operator acts.
	DefaultQuotaProbeInterval = time.Minute
)

// Config is the engine's plain-Go configuration. Fields mirror the
// EtcdMirror CRD spec (api/v1alpha1/etcdmirror_types.go); doc comments here
// and there are the PR1<->PR2 alignment contract.
type Config struct {
	// LinkUID uniquely identifies this mirror link (source, target, prefix
	// tuple); typically the EtcdMirror object's UID. Stamped into the fence
	// key: a checkpoint carrying a different LinkUID is another link's fence
	// and the agent refuses to touch it. Required.
	LinkUID string

	// Epoch is this agent generation within the link, monotonically
	// increased by the supervisor on each re-deploy. An agent that finds a
	// higher epoch in the fence stops permanently (a newer generation owns
	// the link); a lower stored epoch is taken over via the fenced write
	// path. Must be >= 1.
	Epoch int64

	// Mode selects continuous replication (Sync, the default) or a cutover
	// drain (Drain). See ModeDrain; RequestDrain flips a running agent.
	Mode Mode

	// SourcePrefix scopes which source keys are mirrored; empty means the
	// whole keyspace.
	SourcePrefix string
	// TargetPrefix is the prefix under which mirrored keys land.
	TargetPrefix string
	// DestPrefix is the middle term of the rewrite formula
	//
	//	key' = TargetPrefix + DestPrefix + TrimPrefix(key, SourcePrefix)
	//
	// Default "" means the source prefix is stripped and key remainders land
	// directly under TargetPrefix.
	DestPrefix string
	// ExcludePrefixes lists source key prefixes (full source-side keys)
	// skipped entirely: not scanned, not watched, not counted, not pruned.
	// Nested or duplicate entries are normalized away at defaulting time (a
	// prefix covered by another is dropped) so range subtraction and count
	// corrections each see a disjoint set.
	ExcludePrefixes []string

	// InitialSyncMode governs pre-existing destination keys at genesis.
	// Defaults to RequireEmpty.
	InitialSyncMode InitialSyncMode
	// StartRevision, when > 0, skips the genesis scan entirely and starts
	// watching from StartRevision+1 (for fidelity-preserving snapshot
	// seeds). Requires InitialSyncMode Overwrite or OverwriteAndPrune.
	StartRevision int64

	// CheckpointKey overrides the reserved checkpoint/fence key on the
	// target. Defaults to the effective destination prefix +
	// DefaultCheckpointKeySuffix. The key is excluded by exact match from
	// scans, counts, prune passes, and the RequireEmpty check; the target
	// RBAC grant must cover it.
	CheckpointKey string

	// MaxTxnOps bounds how many operations the agent batches into a single
	// target Txn, including the reserved checkpoint-write slot. Must not
	// exceed the target's --max-txn-ops. Defaults to 128; minimum 2.
	MaxTxnOps int
	// TxnFlushBytes is the byte watermark at which a batch is flushed (at
	// the next source-revision boundary). Defaults to 1Mi.
	TxnFlushBytes int64
	// PageKeyLimit bounds keys per source scan page. The scan is pull-based,
	// one page in flight — no read-ahead. Defaults to 512.
	PageKeyLimit int
	// PageBytes bounds bytes per source scan page. etcd Range has no byte
	// limit, so this is enforced adaptively: the next page's key limit is
	// derived from the observed bytes/key of the previous page. Defaults
	// to 1Mi.
	PageBytes int64
	// MaxOpsPerSecond rate-limits target writes (puts+deletes/sec, token
	// bucket), applied to both the genesis scan and watch-driven applies.
	// Zero means unlimited.
	MaxOpsPerSecond int
	// RequestTimeout is the per-RPC context deadline applied to every unary
	// call on both sides (watches excluded — they are long-lived by design
	// and covered by progress-notification liveness instead). Defaults
	// to 30s.
	RequestTimeout time.Duration

	// BackoffInitialDelay/BackoffMaxDelay bound the retry loop for
	// connection-class errors. Throttling-class errors use a more
	// conservative curve derived from the same bounds; quota exhaustion and
	// permanent errors are never retried through this loop. Defaults:
	// 1s to 30s.
	BackoffInitialDelay time.Duration
	BackoffMaxDelay     time.Duration

	// ReconcileInterval > 0 enables the periodic full diff-and-repair pass,
	// executed inline on the steady-state tail loop — never concurrently
	// with the genesis scan, a forced-resync sweep, or a drain (a requested
	// drain's own verification supersedes it) — and re-scheduled a full
	// interval after each periodic pass and after the genesis/forced-resync
	// sweep (a drain's verification is terminal and never re-arms; a
	// requested drain gates the periodic pass anyway), keeping the key
	// counts within the CRD's 2x-interval freshness contract whenever
	// passes complete faster than the interval. Independent of this, one
	// reconciliation-with-delete pass always runs after any forced resync
	// (mark-and-sweep), as the OverwriteAndPrune genesis pass, and before
	// the Drain verification. 0 disables the periodic pass (the CRD's
	// spec.reconciliation.enabled maps to this; DefaultReconcilePeriod is
	// the translation default for Enabled with a nil interval).
	ReconcileInterval time.Duration
	// ReconcileDeleteOrphans allows the PERIODIC pass to delete target keys
	// with no source counterpart; when false the pass still repairs missing
	// and divergent keys and reports orphans in the drift. Forced-resync
	// sweeps and OverwriteAndPrune always delete orphans.
	ReconcileDeleteOrphans bool

	// WatchBufferBytes bounds the memory used to buffer watch events
	// observed from R0 while the genesis scan runs (the reflector replay
	// buffer). On overflow the agent cancels the source watch and restarts
	// the scan from a fresh R0 — a bounded retry instead of unbounded growth
	// when source churn outruns scan+apply throughput. The restart is
	// surfaced as Snapshot.LastScanRestartCause WatchBufferOverflow (the
	// controller maps it to the InitialSyncCompactionRaced event) and
	// repeated overflows count toward the resync-loop detector. Defaults to
	// DefaultWatchBufferBytes; must stay in lockstep with the CRD's
	// spec.sync.watchBufferBytes. Must be >= 0 (0 = default).
	WatchBufferBytes int64

	// Agent-internal knobs (not part of the CRD in v1).
	ProgressInterval    time.Duration
	ResyncLoopThreshold int
	QuotaProbeInterval  time.Duration
}

// withDefaults returns a copy with zero fields replaced by defaults.
func (c Config) withDefaults() Config {
	if c.Mode == "" {
		c.Mode = ModeSync
	}
	if c.InitialSyncMode == "" {
		c.InitialSyncMode = InitialSyncRequireEmpty
	}
	if c.CheckpointKey == "" {
		c.CheckpointKey = c.EffectiveDestPrefix() + DefaultCheckpointKeySuffix
	}
	if c.MaxTxnOps == 0 {
		c.MaxTxnOps = DefaultMaxTxnOps
	}
	if c.TxnFlushBytes == 0 {
		c.TxnFlushBytes = DefaultTxnFlushBytes
	}
	if c.PageKeyLimit == 0 {
		c.PageKeyLimit = DefaultPageKeyLimit
	}
	if c.PageBytes == 0 {
		c.PageBytes = DefaultPageBytes
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = DefaultRequestTimeout
	}
	if c.BackoffInitialDelay == 0 {
		c.BackoffInitialDelay = DefaultBackoffInitial
	}
	if c.BackoffMaxDelay == 0 {
		c.BackoffMaxDelay = DefaultBackoffMax
	}
	if c.WatchBufferBytes == 0 {
		c.WatchBufferBytes = DefaultWatchBufferBytes
	}
	if c.ProgressInterval == 0 {
		c.ProgressInterval = DefaultProgressInterval
	}
	if c.ResyncLoopThreshold == 0 {
		c.ResyncLoopThreshold = DefaultResyncLoopThreshold
	}
	if c.QuotaProbeInterval == 0 {
		c.QuotaProbeInterval = DefaultQuotaProbeInterval
	}
	c.ExcludePrefixes = normalizePrefixes(c.ExcludePrefixes)
	return c
}

// normalizePrefixes sorts prefixes and drops any entry covered by another
// (nested or duplicate). Both the scan-range subtraction and the per-prefix
// count corrections assume a disjoint set: a key covered by two overlapping
// entries must never be subtracted from a count twice.
func normalizePrefixes(in []string) []string {
	if len(in) < 2 {
		return in
	}
	sorted := make([]string, len(in))
	copy(sorted, in)
	sort.Strings(sorted)
	out := sorted[:0]
	for _, p := range sorted {
		if len(out) > 0 && strings.HasPrefix(p, out[len(out)-1]) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// Validate checks the configuration after defaulting.
func (c Config) Validate() error {
	if c.LinkUID == "" {
		return fmt.Errorf("linkUID is required")
	}
	if c.Epoch < 1 {
		return fmt.Errorf("epoch must be >= 1, got %d", c.Epoch)
	}
	if c.Mode != ModeSync && c.Mode != ModeDrain {
		return fmt.Errorf("invalid mode %q", c.Mode)
	}
	switch c.InitialSyncMode {
	case InitialSyncRequireEmpty, InitialSyncOverwrite, InitialSyncOverwriteAndPrune:
	default:
		return fmt.Errorf("invalid initialSyncMode %q", c.InitialSyncMode)
	}
	if c.StartRevision < 0 {
		return fmt.Errorf("startRevision must be >= 0, got %d", c.StartRevision)
	}
	if c.StartRevision > 0 && c.InitialSyncMode == InitialSyncRequireEmpty {
		return fmt.Errorf("startRevision requires initialSyncMode Overwrite or OverwriteAndPrune")
	}
	if c.MaxTxnOps < 2 {
		return fmt.Errorf("maxTxnOps must be >= 2 (one op slot is reserved for the checkpoint), got %d", c.MaxTxnOps)
	}
	if c.TxnFlushBytes < 1 || c.PageBytes < 1 || c.PageKeyLimit < 1 {
		return fmt.Errorf("txnFlushBytes, pageBytes and pageKeyLimit must be positive")
	}
	if c.MaxOpsPerSecond < 0 {
		return fmt.Errorf("maxOpsPerSecond must be >= 0, got %d", c.MaxOpsPerSecond)
	}
	if c.ReconcileInterval < 0 {
		return fmt.Errorf("reconcileInterval must be >= 0, got %v", c.ReconcileInterval)
	}
	if c.WatchBufferBytes < 0 {
		return fmt.Errorf("watchBufferBytes must be >= 0, got %d", c.WatchBufferBytes)
	}
	if !strings.HasPrefix(c.CheckpointKey, c.EffectiveDestPrefix()) {
		return fmt.Errorf("checkpointKey must live under the effective destination prefix")
	}
	for _, p := range c.ExcludePrefixes {
		if p == "" {
			return fmt.Errorf("excludePrefixes entries must be non-empty")
		}
	}
	return nil
}

// EffectiveDestPrefix is TargetPrefix + DestPrefix: the target-side prefix
// every mirrored key lands under, and the range RequireEmpty and prune
// passes operate on.
func (c Config) EffectiveDestPrefix() string {
	return c.TargetPrefix + c.DestPrefix
}
