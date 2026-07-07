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
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// reconcilePass is the shared diff-and-repair pass: the OverwriteAndPrune
// genesis pass, the mandatory mark-and-sweep after any forced resync, the
// periodic pass when Config.ReconcileInterval enables it, and the Drain
// verification repair. It merges bounded pages of BOTH sides in
// key order — the source via the excluded-range-elided scan windows (so
// excluded data is never transferred, matching the genesis scan), the target
// one page at a time — so agent memory stays bounded by two pages no matter
// how divergent the target is (an orphan-heavy target is exactly the state
// this pass exists to repair). Repair puts source-truth values over missing
// or value-divergent target keys, deleteOrphans removes target keys with no
// source counterpart. The reserved checkpoint key and images of excluded
// prefixes are never touched, a sibling link's fence key aborts the pass
// (PrefixConflictError) instead of being pruned, and every repair/delete
// rides the same fenced Txn path as applies. cancelWatchWhenPaced is set by
// callers running while the live source watch is open and unread (see
// maybeCancelWatchForPacedRepair). On success the pass publishes its counts,
// completion time, and drift in one Snapshot update.
func (a *Agent) reconcilePass(ctx context.Context, repair, deleteOrphans, cancelWatchWhenPaced bool) error {
	drift := Drift{Repaired: repair || deleteOrphans}
	dstStart, dstEnd := a.rw.destRange()
	b := newBatcher(a.cfg.MaxTxnOps, a.cfg.TxnFlushBytes)
	dstCursor := dstStart
	var srcSeen, dstSeen int64
	var pacedOps int
	pager := &sourcePager{a: a, ranges: a.rw.scanRanges()}
	for {
		spage, more, err := pager.next(ctx)
		if err != nil {
			return err
		}
		// The window of target keys this source page is authoritative for;
		// the last page's window swallows the tail of the destination range.
		winEnd := dstEnd
		if more && len(spage) > 0 {
			winEnd = nextKey(a.rw.image(string(spage[len(spage)-1].Key)))
		}
		expected := make(map[string]string, len(spage))
		for _, kv := range spage {
			if dstKey, ok := a.rw.rewrite(string(kv.Key)); ok {
				expected[dstKey] = string(kv.Value)
				srcSeen++
			}
		}
		// Stream the target side of the window one page at a time — never
		// materialized whole.
		tcursor := dstCursor
		for {
			tresp, terr := a.getRetry(ctx, a.dst, tcursor, clientv3.WithRange(winEnd),
				clientv3.WithLimit(int64(a.cfg.PageKeyLimit)),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
			if terr != nil {
				return terr
			}
			var ops []kvOp
			for _, kv := range tresp.Kvs {
				k := string(kv.Key)
				if k == a.cfg.CheckpointKey || a.rw.excludedImage(k) {
					continue
				}
				dstSeen++
				want, ok := expected[k]
				if !ok {
					if cerr := a.checkForeignFence(k, kv.Value); cerr != nil {
						return cerr
					}
					drift.OrphanKeys++
					if deleteOrphans {
						ops = append(ops, kvOp{key: k, isDelete: true})
					}
					continue
				}
				delete(expected, k)
				if want != string(kv.Value) {
					drift.DivergentKeys++
					if repair {
						ops = append(ops, kvOp{key: k, value: want})
					}
				}
			}
			if err := a.enqueueRepairs(ctx, b, ops); err != nil {
				return err
			}
			pacedOps += len(ops)
			a.maybeCancelWatchForPacedRepair(cancelWatchWhenPaced, pacedOps)
			if len(tresp.Kvs) == 0 || !tresp.More {
				break
			}
			tcursor = nextKey(string(tresp.Kvs[len(tresp.Kvs)-1].Key))
		}
		// Source keys of this window with no target counterpart.
		var missing []kvOp
		for k, v := range expected {
			drift.MissingKeys++
			if repair {
				missing = append(missing, kvOp{key: k, value: v})
			}
		}
		if err := a.enqueueRepairs(ctx, b, missing); err != nil {
			return err
		}
		pacedOps += len(missing)
		a.maybeCancelWatchForPacedRepair(cancelWatchWhenPaced, pacedOps)
		dstCursor = winEnd
		if !more {
			break
		}
	}
	if fs := b.flush(); fs != nil {
		if err := a.applyRepairFlush(ctx, fs); err != nil {
			return err
		}
	}
	// Publish the pass outcome in ONE update: the per-side key counts as
	// observed by this pass (pre-repair — the equality signal behind the
	// InvariantsHeld condition), the completion timestamp, and the drift.
	// The API contract binds the reconciliation time and drift as one
	// record, so a concurrent Snapshot must never pair this pass's
	// counts/timestamp with a previous pass's drift.
	a.update(func(s *Snapshot) {
		s.SourceKeyCount = srcSeen
		s.TargetKeyCount = dstSeen
		s.LastReconcileTime = time.Now()
		s.LastReconcileDrift = &drift
	})
	return nil
}

// pacedRepairSecondsBeforeWatchCancel is how many seconds' worth of
// MaxOpsPerSecond pacing a diff pass may queue as repairs before the live
// source watch is cancelled (maybeCancelWatchForPacedRepair).
const pacedRepairSecondsBeforeWatchCancel = 3

// maybeCancelWatchForPacedRepair bounds clientv3's unbounded per-watcher
// buffer during a diff pass that runs while the live source watch is open
// and UNREAD — the periodic pass and the drain repair, both inline in
// consume; the genesis sweep's watch drains into the byte-bounded replay
// buffer instead and passes enabled=false. applyOps paces every repair
// flush by MaxOpsPerSecond, so a healthy pass repairing D keys leaves the
// watch unread for ~D/MaxOpsPerSecond seconds while the source keeps
// writing — a stall the backoff-driven cancels in applyFenced never see.
// Once the queued repairs exceed a few seconds of pacing, cancel the watch:
// the tail re-watches from the checkpoint watermark (the pass never moves
// it), and a compacted resume revision escalates to a forced resync whose
// mandatory sweep supersedes the pass anyway.
func (a *Agent) maybeCancelWatchForPacedRepair(enabled bool, pacedOps int) {
	if !enabled || a.cfg.MaxOpsPerSecond <= 0 ||
		pacedOps <= pacedRepairSecondsBeforeWatchCancel*a.cfg.MaxOpsPerSecond {
		return
	}
	a.cancelSourceWatch()
}

// enqueueRepairs pushes repair/prune ops through the shared batcher,
// applying any flush sets that become due.
func (a *Agent) enqueueRepairs(ctx context.Context, b *batcher, ops []kvOp) error {
	for _, op := range ops {
		g := revGroup{rev: a.fence.Watermark, ops: []kvOp{op}}
		for _, fs := range b.add(g) {
			if err := a.applyRepairFlush(ctx, &fs); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkForeignFence refuses to treat another EtcdMirror link's reserved
// checkpoint/fence key (recognized by the \x00-after-prefix reserved-key
// convention plus a decodable fence document) as a prunable orphan:
// overlapping destination prefixes must stop loudly instead of silently
// destroying the sibling link's fence and data.
func (a *Agent) checkForeignFence(key string, value []byte) error {
	if !strings.Contains(key, "\x00") {
		return nil
	}
	fv, err := DecodeFenceValue(value)
	if err != nil || fv.LinkUID == a.cfg.LinkUID {
		return nil
	}
	return &PrefixConflictError{
		Key:          RedactKey(a.cfg.EffectiveDestPrefix(), []byte(key)),
		OwnerLinkUID: fv.LinkUID,
	}
}

// sourcePager yields the source side of the reconcile merge one bounded page
// at a time, walking the excluded-range-elided scan windows in key order so
// excluded data is never transferred (server-side elision, matching the
// genesis scan). The reported more flag is false only on the final page of
// the final non-empty window; a one-page lookahead across window boundaries
// decides it.
type sourcePager struct {
	a      *Agent
	ranges []scanRange
	ri     int
	cursor string // resume point within ranges[ri]; "" means the range start
	buf    *srcPage
}

type srcPage struct {
	kvs  []*mvccpb.KeyValue
	more bool // resp.More within the page's own window
}

func (p *sourcePager) next(ctx context.Context) ([]*mvccpb.KeyValue, bool, error) {
	cur := p.buf
	p.buf = nil
	if cur == nil {
		var err error
		if cur, err = p.fetch(ctx); err != nil {
			return nil, false, err
		}
	}
	if cur == nil {
		return nil, false, nil
	}
	if cur.more {
		return cur.kvs, true, nil
	}
	// Window boundary: look one page ahead to decide whether any source key
	// remains in a later window.
	nxt, err := p.fetch(ctx)
	if err != nil {
		return nil, false, err
	}
	p.buf = nxt
	return cur.kvs, nxt != nil, nil
}

// fetch returns the next non-empty page across the remaining windows, or nil
// when every window is exhausted.
func (p *sourcePager) fetch(ctx context.Context) (*srcPage, error) {
	for p.ri < len(p.ranges) {
		kr := p.ranges[p.ri]
		start := kr.start
		if p.cursor != "" {
			start = p.cursor
		}
		resp, err := p.a.getRetry(ctx, p.a.src, start, clientv3.WithRange(kr.end),
			clientv3.WithLimit(int64(p.a.cfg.PageKeyLimit)),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			return nil, err
		}
		if resp.More {
			p.cursor = nextKey(string(resp.Kvs[len(resp.Kvs)-1].Key))
		} else {
			p.ri++
			p.cursor = ""
		}
		if len(resp.Kvs) > 0 {
			return &srcPage{kvs: resp.Kvs, more: resp.More}, nil
		}
	}
	return nil, nil
}

// recordKeyCounts publishes the per-side in-scope key counts observed by a
// count-only drain verification, timestamped as a pass completion — the
// freshness input to the controller's InvariantsHeld condition. It never
// touches LastReconcileDrift (count equality cannot attest DivergentKeys);
// full diff passes publish counts, timestamp, and drift atomically inside
// reconcilePass instead. completeDrain stamps counts BEFORE its equality
// check, so a failed drain leaves a fresh timestamp beside unequal counts:
// freshness alone never attests a healthy pass.
func (a *Agent) recordKeyCounts(srcN, dstN int64) {
	a.update(func(s *Snapshot) {
		s.SourceKeyCount = srcN
		s.TargetKeyCount = dstN
		s.LastReconcileTime = time.Now()
	})
}

// reconcileDue reports whether the periodic pass should run at now: never
// when disabled (ReconcileInterval <= 0), before the tail armed the deadline,
// before the deadline itself, or once a drain is requested — the drain's own
// verification pass (repair + prune + recount) supersedes the periodic pass.
func (a *Agent) reconcileDue(now time.Time) bool {
	return a.cfg.ReconcileInterval > 0 &&
		!a.nextReconcile.IsZero() &&
		!now.Before(a.nextReconcile) &&
		a.cfg.Mode != ModeDrain &&
		!a.drainReq.Load()
}

// scheduleNextReconcile pushes the periodic deadline one full interval out
// from now; a no-op when the periodic pass is disabled. Mandatory sweeps call
// this too — they just produced the same signal, so re-running the periodic
// pass sooner adds cost without information.
func (a *Agent) scheduleNextReconcile() {
	if a.cfg.ReconcileInterval <= 0 {
		return
	}
	a.nextReconcile = time.Now().Add(a.cfg.ReconcileInterval)
}

// maybeReconcile runs one periodic diff-and-repair pass when due, inline on
// the Run goroutine — never concurrently with a genesis scan (consume is not
// running), a forced-resync sweep (same), a drain (gated), or itself. A
// drain-gated fire still re-arms the deadline so the tail's timer never spins
// on a stale one. Error flow: transient, throttle, and quota errors are
// absorbed INSIDE the pass by getRetry/applyFenced — the pass blocks until
// they heal or ctx cancels (a wedged pass cannot be preempted by a drain
// check) — and no pass read is revision-pinned, so what actually escapes
// through consume → tail → Run is ClassPermanent (fails the agent) or ctx
// cancellation. tail's transient/Resync arms remain as taxonomy-consistent
// safety nets; the transient arm also recovers the watch-closed error after
// a pass cancelled the source watch (maybeCancelWatchForPacedRepair) by
// re-watching from the checkpoint watermark.
func (a *Agent) maybeReconcile(ctx context.Context) error {
	if !a.reconcileDue(time.Now()) {
		a.scheduleNextReconcile()
		return nil
	}
	if err := a.reconcilePass(ctx, true, a.cfg.ReconcileDeleteOrphans, true); err != nil {
		return err
	}
	a.scheduleNextReconcile()
	return nil
}

// applyRepairFlush writes repair/prune ops under the fence without moving
// any checkpoint field: the pass is positionless, only the compare matters.
func (a *Agent) applyRepairFlush(ctx context.Context, fs *flushSet) error {
	f := a.fence
	f.Epoch = a.cfg.Epoch
	return a.applyOps(ctx, fs, f)
}

// verifyCounts counts in-scope keys on both sides: excluded prefixes are
// elided via the scan windows and the reserved checkpoint key is not counted
// on the target. Source counts are pinned at the checkpoint watermark (the
// drained revision during a drain) so a non-quiesced source still yields a
// coherent snapshot; if that revision has been compacted the count falls
// back to an unpinned re-read.
func (a *Agent) verifyCounts(ctx context.Context) (srcN, dstN int64, err error) {
	if srcN, err = a.countSourceInScope(ctx, a.watermark()); err != nil {
		if Classify(err) != ClassResync {
			return 0, 0, err
		}
		if srcN, err = a.countSourceInScope(ctx, 0); err != nil {
			return 0, 0, err
		}
	}
	dstStart, dstEnd := a.rw.destRange()
	if dstN, err = a.countRange(ctx, a.dst, dstStart, dstEnd, 0); err != nil {
		return 0, 0, err
	}
	ck, err := a.getRetry(ctx, a.dst, a.cfg.CheckpointKey, clientv3.WithCountOnly())
	if err != nil {
		return 0, 0, err
	}
	dstN -= ck.Count
	for _, p := range a.cfg.ExcludePrefixes {
		if !strings.HasPrefix(p, a.cfg.SourcePrefix) {
			continue
		}
		s, e := keyRange(a.rw.image(p))
		n, cerr := a.countRange(ctx, a.dst, s, e, 0)
		if cerr != nil {
			return 0, 0, cerr
		}
		dstN -= n
	}
	return srcN, dstN, nil
}

// countSourceInScope sums the in-scope source key count over the scan
// windows, pinned at rev when rev > 0.
func (a *Agent) countSourceInScope(ctx context.Context, rev int64) (int64, error) {
	var total int64
	for _, kr := range a.rw.scanRanges() {
		n, err := a.countRange(ctx, a.src, kr.start, kr.end, rev)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (a *Agent) countRange(ctx context.Context, cl Client, start, end string, rev int64) (int64, error) {
	opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithCountOnly()}
	if rev > 0 {
		opts = append(opts, clientv3.WithRev(rev))
	}
	resp, err := a.getRetry(ctx, cl, start, opts...)
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}
