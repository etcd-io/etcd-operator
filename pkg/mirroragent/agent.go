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
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// errDrained signals a completed Drain cutover up the Run stack; Run maps it
// to a nil return.
var errDrained = errors.New("drain completed")

// errFenceLost means a fence claim raced an older agent generation's write
// between our read and our takeover Txn. fenceViolation adopts the raced
// write's mod revision before returning it, so the caller's retry re-runs
// the takeover against the current fence: loadFence's loop re-reads and
// retries, and applyFenced retries any claim made before this generation's
// first successful commit (the genesis / startFromRevision claims on a
// fresh target). After that first commit (Agent.claimed), older generations
// fail their own compares and can never move the fence again, so a
// post-claim loss escalates to a permanent FenceError.
var errFenceLost = errors.New("fence claim raced an older generation write")

// Agent is the replication engine. Create with New, drive with Run (once),
// observe with Snapshot from any goroutine.
type Agent struct {
	cfg Config
	src Client
	dst Client
	rw  *rewriter
	bo  *backoff

	srcClusterID uint64
	dstClusterID uint64
	// trustProgress gates watermark advancement from watch progress
	// notifications (source >= 3.4.25 / 3.5.8).
	trustProgress bool

	// fence is the engine's cached copy of the reserved key's value;
	// fenceModRev is the mod revision EVERY write path compares against.
	// Both are owned by the Run goroutine.
	fence       FenceValue
	fenceModRev int64
	// claimed is true once this generation committed its first fenced Txn:
	// from then on a lost fence compare is a permanent violation, never a
	// retryable claim race. Owned by the Run goroutine.
	claimed bool
	// prunePending mirrors FenceValue.PrunePending: set when a forced resync
	// starts, stamped into every checkpoint, cleared only after the mandatory
	// mark-and-sweep prune completed. Owned by the Run goroutine.
	prunePending bool

	// watchCancel cancels the live source watch; applyFenced invokes it via
	// cancelSourceWatch on sustained target backoff, and a long paced diff
	// pass via maybeCancelWatchForPacedRepair, so clientv3's unbounded
	// per-watcher response buffer cannot grow for the duration of a target
	// stall or a paced repair. Owned by the Run goroutine (set by
	// genesis/tail, consumed by the apply path, which runs on the same
	// goroutine).
	watchCancel func()

	drainReq atomic.Bool

	// consecutiveResyncs counts forced resyncs without reaching steady state
	// in between (owned by the Run goroutine). restartBo paces Run's
	// genesis-restart loop; it is deliberately separate from the shared bo,
	// whose curves reset on every successful apply inside a scan attempt.
	consecutiveResyncs int
	restartBo          *backoff

	// nextReconcile is the periodic pass's next deadline (zero until the
	// first tail arms it); owned by the Run goroutine. Mandatory sweeps
	// re-arm it — they produce the same signal.
	nextReconcile time.Time

	mu   sync.Mutex
	snap Snapshot
}

// startState says how a replication cycle begins: fresh genesis, resumed
// scan, resumed tail, or a forced resync.
type startState struct {
	haveCheckpoint bool
	scanning       bool
	scanCursor     string
	subRevision    int64
	watermark      int64
	// forced marks a forced resync: full re-scan plus a mandatory
	// mark-and-sweep prune pass; RequireEmpty is NOT re-checked (the decoded,
	// ownership-validated fence proves the destination data is this link's
	// own). Persisted across restarts as FenceValue.PrunePending so a crash
	// mid-forced-resync cannot silently drop the owed sweep.
	forced bool
	// rearmEmpty re-arms the RequireEmpty check (cluster-identity mismatch).
	rearmEmpty bool
}

// New validates cfg (after defaulting) and builds an Agent over the two
// clients. The caller owns client lifecycle and TLS/auth material.
func New(cfg Config, source, target Client) (*Agent, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Agent{
		cfg:       cfg,
		src:       source,
		dst:       target,
		rw:        newRewriter(cfg),
		bo:        newBackoff(cfg.BackoffInitialDelay, cfg.BackoffMaxDelay),
		restartBo: newBackoff(cfg.BackoffInitialDelay, cfg.BackoffMaxDelay),
		snap:      Snapshot{Phase: PhaseConnecting},
	}, nil
}

// Snapshot returns a point-in-time copy of the agent's state, safe to retain.
func (a *Agent) Snapshot() Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.snap
	if a.snap.Cutover != nil {
		c := *a.snap.Cutover
		s.Cutover = &c
	}
	if a.snap.LastReconcileDrift != nil {
		d := *a.snap.LastReconcileDrift
		s.LastReconcileDrift = &d
	}
	s.ForcedResyncCountByReason = maps.Clone(a.snap.ForcedResyncCountByReason)
	return s
}

// RequestDrain flips a running Sync agent into a drain, as if Mode were
// ModeDrain.
func (a *Agent) RequestDrain() { a.drainReq.Store(true) }

// Run executes the replication loop until ctx is cancelled (returns
// ctx.Err()), a Drain completes (returns nil), or a permanent failure occurs
// (returns the classified error).
func (a *Agent) Run(ctx context.Context) error {
	if err := a.connect(ctx); err != nil {
		return a.fail(ctx, err)
	}
	st, err := a.loadFence(ctx)
	if err != nil {
		return a.fail(ctx, err)
	}
	for {
		err := a.cycle(ctx, st)
		var restart *scanRestartError
		switch {
		case err == nil:
			return nil // drain completed
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.As(err, &restart):
			// Bounded genesis retry: restart the scan from a fresh R0. The
			// dropped replay buffer may have held deletes, so the restarted
			// attempt owes a mark-and-sweep (forced). The dedicated restart
			// backoff keeps repeated restarts (churn outrunning the buffer)
			// off a hot loop AND escalating: the shared curve resets on every
			// successful apply inside a doomed attempt, so it would stay
			// pinned at the initial delay forever.
			a.noteScanRestart(restart)
			if serr := sleepCtx(ctx, a.restartBo.next(ClassTransient)); serr != nil {
				return serr
			}
			st = startState{forced: true}
		case Classify(err) == ClassResync:
			a.noteResync(err)
			st = startState{forced: true}
		default:
			return a.fail(ctx, err)
		}
	}
}

// cycle runs one replication attempt from the given start state.
func (a *Agent) cycle(ctx context.Context, st startState) error {
	var err error
	switch {
	case st.haveCheckpoint && !st.scanning:
		err = a.tail(ctx, nil, nil, st.watermark)
	case !st.haveCheckpoint && !st.forced && a.cfg.StartRevision > 0:
		err = a.startFromRevision(ctx)
	default:
		err = a.genesis(ctx, st)
	}
	if errors.Is(err, errDrained) {
		return nil
	}
	return err
}

// startFromRevision skips the genesis scan (fidelity-preserving snapshot
// seed) and tails from StartRevision+1.
func (a *Agent) startFromRevision(ctx context.Context) error {
	if err := a.applyFenced(ctx, nil, a.newFence(a.cfg.StartRevision, false, "", 0), "", 0); err != nil {
		return err
	}
	a.advanceWatermark(a.cfg.StartRevision)
	return a.tail(ctx, nil, nil, a.cfg.StartRevision)
}

// connect probes both sides' version and cluster identity (maintenance
// Status at connect) and enforces the >=3.4 hard floor.
func (a *Agent) connect(ctx context.Context) error {
	a.setPhase(PhaseConnecting)
	srcInfo, srcID, err := a.probe(ctx, "source", a.src)
	if err != nil {
		return err
	}
	dstInfo, dstID, err := a.probe(ctx, "target", a.dst)
	if err != nil {
		return err
	}
	a.srcClusterID, a.dstClusterID = srcID, dstID
	a.trustProgress = srcInfo.TrustProgressNotify
	a.update(func(s *Snapshot) {
		s.SourceVersion = srcInfo.Version
		s.TargetVersion = dstInfo.Version
		s.SourceClusterID = srcID
		s.TargetClusterID = dstID
	})
	return nil
}

func (a *Agent) probe(ctx context.Context, side string, cl Client) (versionInfo, uint64, error) {
	eps := cl.Endpoints()
	if len(eps) == 0 {
		return versionInfo{}, 0, &ConfigError{Detail: side + " client has no endpoints"}
	}
	// Status dials the named endpoint directly, bypassing the balancer, so
	// the probe must rotate endpoints itself: one blackholed member must not
	// wedge the agent in Connecting while healthy quorum members exist.
	attempt := 0
	for {
		tctx, cancel := context.WithTimeout(ctx, a.cfg.RequestTimeout)
		resp, err := cl.Status(tctx, eps[attempt%len(eps)])
		cancel()
		attempt++
		if err == nil {
			if attempt > 1 {
				a.bo.noteSuccess()
			}
			vi, verr := classifyVersion(side, resp.Version)
			if verr != nil {
				return versionInfo{}, 0, verr
			}
			return vi, resp.Header.ClusterId, nil
		}
		if ctx.Err() != nil {
			return versionInfo{}, 0, ctx.Err()
		}
		class := Classify(err)
		if class == ClassPermanent || class == ClassResync {
			return versionInfo{}, 0, fmt.Errorf("probing %s: %w", side, err)
		}
		a.recordErr(err, class)
		if serr := sleepCtx(ctx, a.bo.next(class)); serr != nil {
			return versionInfo{}, 0, serr
		}
	}
}

// loadFence reads the reserved key, validates ownership, and — when a valid
// checkpoint of this link exists — takes the fence over for this agent
// generation so any straggler generation fails its next compare.
func (a *Agent) loadFence(ctx context.Context) (startState, error) {
	for {
		resp, err := a.getRetry(ctx, a.dst, a.cfg.CheckpointKey)
		if err != nil {
			return startState{}, err
		}
		if len(resp.Kvs) == 0 {
			// Fresh target: the fence is claimed at genesis start, after the
			// RequireEmpty gate, so a violation writes nothing.
			a.fenceModRev = 0
			return startState{}, nil
		}
		kv := resp.Kvs[0]
		a.fenceModRev = kv.ModRevision
		f, derr := DecodeFenceValue(kv.Value)
		if derr != nil {
			// Corrupt or unknown-version checkpoint: fail CLOSED. Nothing
			// about the stored value is knowable — not the owning link, not
			// the epoch, not whether a cutover already flipped the role to
			// Primary — so no write (least of all a resync's prune) is
			// provably safe. Permanent: the operator must inspect the
			// reserved key and delete it to recover.
			return startState{}, fmt.Errorf("reserved key %q: %w", a.cfg.CheckpointKey, derr)
		}
		takeover, st, verr := a.validateFence(f)
		if verr != nil {
			return startState{}, verr
		}
		if !takeover {
			return st, nil
		}
		// Take the fence over for this generation before anything else runs.
		f.Epoch = a.cfg.Epoch
		err = a.commitFenced(ctx, nil, f)
		if err == nil {
			a.advanceWatermark(f.Watermark)
			return startState{
				haveCheckpoint: true,
				scanning:       f.Scanning,
				scanCursor:     f.ScanCursor,
				subRevision:    f.SubRevision,
				watermark:      f.Watermark,
				// A crash mid-forced-resync leaves the owed mark-and-sweep
				// recorded in the fence; the resumed scan must still prune.
				forced: f.PrunePending,
			}, nil
		}
		if errors.Is(err, errFenceLost) {
			continue // an older generation wrote in the read/claim window
		}
		class := Classify(err)
		switch class {
		case ClassPermanent, ClassResync:
			return startState{}, err
		case ClassQuota:
			a.recordErr(err, class)
			a.update(func(s *Snapshot) { s.QuotaExhausted = true; s.Phase = PhaseDegraded })
			if serr := sleepCtx(ctx, a.cfg.QuotaProbeInterval); serr != nil {
				return startState{}, serr
			}
		default:
			a.recordErr(err, class)
			if serr := sleepCtx(ctx, a.bo.next(class)); serr != nil {
				return startState{}, serr
			}
		}
	}
}

// validateFence checks a decoded checkpoint against this agent's identity.
// takeover is true when the fence is ours to take over; otherwise st is the
// forced-resync start state or err is terminal.
func (a *Agent) validateFence(f FenceValue) (takeover bool, st startState, err error) {
	if f.LinkUID != a.cfg.LinkUID {
		return false, startState{}, &FenceError{Detail: fmt.Sprintf(
			"reserved key %q is owned by link %q, not %q",
			a.cfg.CheckpointKey, f.LinkUID, a.cfg.LinkUID)}
	}
	if f.Role == RolePrimary {
		return false, startState{}, &FenceError{
			Detail: "fence role is Primary: cutover completed, mirror writes are forbidden",
		}
	}
	if f.Epoch > a.cfg.Epoch {
		return false, startState{}, &FenceError{Detail: fmt.Sprintf(
			"newer agent epoch %d owns the link (this agent is epoch %d)",
			f.Epoch, a.cfg.Epoch)}
	}
	if f.SourceClusterID != a.srcClusterID || f.TargetClusterID != a.dstClusterID {
		a.noteResync(&ResyncError{Reason: ResyncReasonClusterIDMismatch, Cause: fmt.Errorf(
			"checkpoint bound to source=%d target=%d, probed source=%d target=%d",
			f.SourceClusterID, f.TargetClusterID, a.srcClusterID, a.dstClusterID)})
		return false, startState{forced: true, rearmEmpty: true}, nil
	}
	return true, startState{}, nil
}

// newFence builds the checkpoint document for this agent generation.
func (a *Agent) newFence(watermark int64, scanning bool, cursor string, subrev int64) FenceValue {
	return FenceValue{
		LinkUID:         a.cfg.LinkUID,
		Epoch:           a.cfg.Epoch,
		Role:            RoleMirror,
		Watermark:       watermark,
		Scanning:        scanning,
		ScanCursor:      cursor,
		SubRevision:     subrev,
		PrunePending:    a.prunePending,
		SourceClusterID: a.srcClusterID,
		TargetClusterID: a.dstClusterID,
	}
}

// noteResync records a forced resync and drives the livelock detector.
func (a *Agent) noteResync(err error) {
	reason := ResyncReasonCompacted
	var re *ResyncError
	if errors.As(err, &re) {
		reason = re.Reason
	}
	a.consecutiveResyncs++
	loop := a.consecutiveResyncs >= a.cfg.ResyncLoopThreshold
	a.update(func(s *Snapshot) {
		s.ForcedResyncCount++
		if s.ForcedResyncCountByReason == nil {
			s.ForcedResyncCountByReason = make(map[ResyncReason]int64, 2)
		}
		s.ForcedResyncCountByReason[reason]++
		s.LastResyncReason = reason
		s.Compacted = reason == ResyncReasonCompacted
		if loop {
			s.ResyncLoopDetected = true
		}
	})
}

// noteScanRestart records an aborted genesis attempt (buffer overflow or a
// watch reconnect below the compact revision mid-scan). Restarts do not
// invalidate the checkpoint — ForcedResyncCount is untouched — but they
// count toward the same livelock detector: repeated restarts are the
// signature of churn or retention outrunning scan throughput.
func (a *Agent) noteScanRestart(e *scanRestartError) {
	a.consecutiveResyncs++
	loop := a.consecutiveResyncs >= a.cfg.ResyncLoopThreshold
	a.update(func(s *Snapshot) {
		s.ScanRestartCount++
		s.LastScanRestartCause = e.Cause
		if loop {
			s.ResyncLoopDetected = true
		}
	})
}

// steadyState is reached on the first successfully applied LIVE watch
// response of a tail: it resets the resync-loop detector and the restart
// backoff. Genesis replay-buffer applies must never reach here — they run
// INSIDE the resync the detector is counting, and a churning source (the
// canonical livelock trigger) guarantees a non-empty replay buffer, so a
// replay-driven reset would keep the detector at zero forever.
func (a *Agent) steadyState() {
	a.restartBo.reset()
	if a.consecutiveResyncs == 0 {
		return
	}
	a.consecutiveResyncs = 0
	a.update(func(s *Snapshot) {
		s.ResyncLoopDetected = false
		s.Compacted = false
	})
}

// cancelSourceWatch tears down the live source watch (if any) so clientv3
// stops buffering undelivered responses while the target is parked or in
// sustained backoff; the tail re-watches from the checkpoint watermark once
// applies succeed again. Idempotent.
func (a *Agent) cancelSourceWatch() {
	if a.watchCancel != nil {
		a.watchCancel()
		a.watchCancel = nil
	}
}

func (a *Agent) fail(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	class := Classify(err)
	a.update(func(s *Snapshot) {
		s.LastError = err.Error()
		s.LastErrorClass = class
		if s.Phase != PhaseDrained {
			s.Phase = PhaseFailed
		}
	})
	return err
}

func (a *Agent) update(fn func(*Snapshot)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fn(&a.snap)
}

func (a *Agent) setPhase(p Phase) {
	a.update(func(s *Snapshot) { s.Phase = p })
}

func (a *Agent) phase() Phase {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snap.Phase
}

func (a *Agent) watermark() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snap.Watermark
}

func (a *Agent) advanceWatermark(rev int64) {
	a.update(func(s *Snapshot) {
		if rev > s.Watermark {
			s.Watermark = rev
		}
		s.LastProgressTime = time.Now()
	})
}

func (a *Agent) recordErr(err error, class Class) {
	a.update(func(s *Snapshot) {
		s.LastError = err.Error()
		s.LastErrorClass = class
	})
}

// pace enforces MaxOpsPerSecond with simple pre-write sleeping.
func (a *Agent) pace(ctx context.Context, n int) {
	if a.cfg.MaxOpsPerSecond <= 0 || n == 0 {
		return
	}
	d := time.Duration(n) * time.Second / time.Duration(a.cfg.MaxOpsPerSecond)
	_ = sleepCtx(ctx, d)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// getRetry performs a unary Get under the per-RPC deadline with the standard
// read retry policy: transient/throttle back off, resync and permanent
// propagate. A success after retries resets the backoff curves, so one old
// saturated burst does not pin every later isolated retry at the max delay.
func (a *Agent) getRetry(
	ctx context.Context, cl Client, key string, opts ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
	retried := false
	for {
		tctx, cancel := context.WithTimeout(ctx, a.cfg.RequestTimeout)
		resp, err := cl.Get(tctx, key, opts...)
		cancel()
		if err == nil {
			if retried {
				a.bo.noteSuccess()
			}
			return resp, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		class := Classify(err)
		if class == ClassPermanent || class == ClassResync {
			return nil, err
		}
		a.recordErr(err, class)
		retried = true
		if serr := sleepCtx(ctx, a.bo.next(class)); serr != nil {
			return nil, serr
		}
	}
}
