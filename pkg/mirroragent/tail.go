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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// tail is the live watch-replay loop. wch may carry a watch opened before a
// genesis scan (whose buffered events replay over the scanned base), with
// wcancel its cancel func; when wch is nil, a watch opens at fromRev+1.
// Transient watch failures re-watch from the checkpoint watermark;
// compaction of the resume revision propagates as a forced resync. The
// current watch's cancel is published via Agent.watchCancel so a stalled
// apply can stop the source stream (bounded memory during target stalls).
func (a *Agent) tail(ctx context.Context, wch clientv3.WatchChan, wcancel context.CancelFunc, fromRev int64) error {
	a.setPhase(PhaseSyncing)
	// Arm the periodic reconciliation deadline on first reaching steady
	// state: one full interval out, never during or before a genesis scan.
	if a.cfg.ReconcileInterval > 0 && a.nextReconcile.IsZero() {
		a.scheduleNextReconcile()
	}
	srcStart, srcEnd := a.rw.sourceRange()
	for {
		if wch == nil {
			rev := a.watermark()
			if rev < fromRev {
				rev = fromRev
			}
			var wctx context.Context
			wctx, wcancel = context.WithCancel(clientv3.WithRequireLeader(ctx))
			wch = a.src.Watch(wctx, srcStart, clientv3.WithRange(srcEnd),
				clientv3.WithRev(rev+1), clientv3.WithProgressNotify())
		}
		a.watchCancel = wcancel
		err := a.consume(ctx, wch)
		a.watchCancel = nil
		if wcancel != nil {
			wcancel()
		}
		wch, wcancel = nil, nil
		if errors.Is(err, errDrained) || ctx.Err() != nil {
			return err
		}
		switch class := Classify(err); class {
		case ClassTransient, ClassThrottle:
			a.recordErr(err, class)
			a.setPhase(PhaseDegraded)
			if serr := sleepCtx(ctx, a.bo.next(class)); serr != nil {
				return serr
			}
			a.setPhase(PhaseSyncing)
		default:
			return err
		}
	}
}

// consume applies watch responses until the channel closes or fails. It
// drives client-side progress requests (server-side notify intervals are
// uncontrollable on foreign clusters) and checks the drain gate between
// responses.
func (a *Agent) consume(ctx context.Context, wch clientv3.WatchChan) error {
	ticker := time.NewTicker(a.cfg.ProgressInterval)
	defer ticker.Stop()
	// The periodic reconciliation deadline rides the same select (nil channel
	// when disabled, so the case never fires). The timer is rebuilt from the
	// persistent deadline on every consume entry — a mandatory sweep between
	// cycles re-armed it and is picked up automatically.
	var reconcileC <-chan time.Time
	var reconcileTimer *time.Timer
	if a.cfg.ReconcileInterval > 0 {
		reconcileTimer = time.NewTimer(time.Until(a.nextReconcile))
		defer reconcileTimer.Stop()
		reconcileC = reconcileTimer.C
	}
	for {
		if err := a.maybeDrain(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reconcileC:
			if err := a.maybeReconcile(ctx); err != nil {
				return err
			}
			reconcileTimer.Reset(time.Until(a.nextReconcile))
		case <-ticker.C:
			// The progress request MUST carry the same outgoing metadata as
			// the Watch call: clientv3 keys watcher gRPC streams by ctx
			// metadata, and every engine watch is opened WithRequireLeader.
			// A bare ctx here would address an empty stream and no
			// notification would ever arrive on an idle prefix.
			rctx, cancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), a.cfg.RequestTimeout)
			_ = a.src.RequestProgress(rctx)
			cancel()
		case wr, ok := <-wch:
			if !ok {
				return fmt.Errorf("source watch channel closed")
			}
			if wr.CompactRevision != 0 {
				return &ResyncError{Reason: ResyncReasonCompacted, Cause: wr.Err()}
			}
			if err := wr.Err(); err != nil {
				return err
			}
			if err := a.handleResponse(ctx, &wr, true); err != nil {
				return err
			}
		}
	}
}

// handleResponse applies one watch response: whole revisions coalesced into
// fenced Txns, flushed only at source-revision boundaries; trusted progress
// notifications advance the watermark checkpoint on idle prefixes. live is
// true only for responses consumed from a live tail channel — genesis
// replay-buffer applies pass false so they never reset the resync-loop
// detector from inside the very resync it is counting.
func (a *Agent) handleResponse(ctx context.Context, wr *clientv3.WatchResponse, live bool) error {
	hdrRev := wr.Header.Revision
	a.update(func(s *Snapshot) {
		if hdrRev > s.SourceRevision {
			s.SourceRevision = hdrRev
		}
	})
	if wr.IsProgressNotify() {
		if !a.trustProgress || hdrRev <= a.watermark() {
			return nil
		}
		if err := a.applyFenced(ctx, nil, a.newFence(hdrRev, false, "", 0), "", 0); err != nil {
			return err
		}
		a.advanceWatermark(hdrRev)
		return nil
	}

	ops := make([]kvOp, 0, len(wr.Events))
	revs := make([]int64, 0, len(wr.Events))
	var leased int64
	for _, ev := range wr.Events {
		srcKey := string(ev.Kv.Key)
		dstKey, ok := a.rw.rewrite(srcKey)
		if !ok {
			continue
		}
		op := kvOp{key: dstKey, srcKey: srcKey}
		if ev.Type == clientv3.EventTypeDelete {
			op.isDelete = true
		} else {
			op.value = string(ev.Kv.Value)
			if ev.Kv.Lease != 0 {
				leased++
			}
		}
		ops = append(ops, op)
		revs = append(revs, ev.Kv.ModRevision)
	}
	if leased > 0 {
		a.update(func(s *Snapshot) { s.LeaseBackedKeyCount += leased })
	}
	if len(ops) == 0 {
		return nil
	}
	// A source revision's events never span watch responses, so flushing at
	// the end of the response only ever cuts at a revision boundary.
	b := newBatcher(a.cfg.MaxTxnOps, a.cfg.TxnFlushBytes)
	for _, g := range groupByRevision(ops, revs) {
		for _, fs := range b.add(g) {
			if err := a.applyLiveFlush(ctx, &fs); err != nil {
				return err
			}
		}
	}
	if fs := b.flush(); fs != nil {
		if err := a.applyLiveFlush(ctx, fs); err != nil {
			return err
		}
	}
	if live {
		a.steadyState()
	}
	return nil
}

// applyLiveFlush applies one revision-complete flush set; the checkpoint
// watermark advances to the set's last complete revision in the same Txn.
func (a *Agent) applyLiveFlush(ctx context.Context, fs *flushSet) error {
	if err := a.applyOps(ctx, fs, a.newFence(fs.watermark, false, "", 0)); err != nil {
		return err
	}
	a.advanceWatermark(fs.watermark)
	return nil
}

// maybeDrain drives Drain mode: record the drain target revision once, then
// complete the cutover when the checkpoint watermark reaches it.
func (a *Agent) maybeDrain(ctx context.Context) error {
	if a.cfg.Mode != ModeDrain && !a.drainReq.Load() {
		return nil
	}
	snap := a.Snapshot()
	if snap.Cutover == nil {
		srcStart, srcEnd := a.rw.sourceRange()
		var target int64
		if a.trustProgress {
			resp, err := a.getRetry(ctx, a.src, srcStart,
				clientv3.WithRange(srcEnd), clientv3.WithCountOnly())
			if err != nil {
				return err
			}
			target = resp.Header.Revision
		} else {
			// Below the progress-trust floor (source < 3.4.25 / 3.5.8) the
			// watermark advances ONLY on applied in-prefix events, so a
			// cluster-revision drain target would never terminate on a shared
			// source: out-of-prefix writes push it past the last in-prefix
			// event while the drain itself quiesces in-prefix writers. Fall
			// back to the highest in-scope mod revision; a tombstone above it
			// (an in-flight delete) is caught and repaired by the drain
			// verification pass before the role flips.
			resp, err := a.getRetry(ctx, a.src, srcStart, clientv3.WithRange(srcEnd),
				clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
				clientv3.WithLimit(1), clientv3.WithKeysOnly())
			if err != nil {
				return err
			}
			if len(resp.Kvs) > 0 {
				target = resp.Kvs[0].ModRevision
			} else {
				target = a.watermark()
			}
		}
		a.update(func(s *Snapshot) {
			s.Cutover = &CutoverStatus{DrainTargetRevision: target}
			if target > s.SourceRevision {
				s.SourceRevision = target
			}
		})
		snap = a.Snapshot()
	}
	if snap.Watermark < snap.Cutover.DrainTargetRevision {
		return nil
	}
	return a.completeDrain(ctx)
}

// completeDrain verifies convergence, records the cutover block, and flips
// the fence role to Primary so any straggler mirror apply fails its compare
// loudly. Returns errDrained on success.
func (a *Agent) completeDrain(ctx context.Context) error {
	srcN, dstN, err := a.verifyCounts(ctx)
	if err != nil {
		return err
	}
	a.recordKeyCounts(srcN, dstN)
	if srcN != dstN {
		// One repair+prune pass and a recount; a persisting mismatch is real
		// divergence and must fail the drain rather than cut over.
		drift, rerr := a.reconcilePass(ctx, true, true)
		if rerr != nil {
			return rerr
		}
		a.recordDrift(drift)
		if srcN, dstN, err = a.verifyCounts(ctx); err != nil {
			return err
		}
		a.recordKeyCounts(srcN, dstN)
		if srcN != dstN {
			return &DrainVerificationError{SourceKeys: srcN, TargetKeys: dstN}
		}
	}
	wm := a.watermark()
	f := a.newFence(wm, false, "", 0)
	f.Role = RolePrimary
	if err := a.applyFenced(ctx, nil, f, "", 0); err != nil {
		return err
	}
	now := time.Now()
	a.update(func(s *Snapshot) {
		c := *s.Cutover
		c.DrainedRevision = wm
		c.VerifiedTime = now
		c.SourceKeyCount = srcN
		c.TargetKeyCount = dstN
		c.LeasedKeyCount = s.LeaseBackedKeyCount
		s.Cutover = &c
		s.CutoverReady = true
		s.Phase = PhaseDrained
	})
	return errDrained
}
