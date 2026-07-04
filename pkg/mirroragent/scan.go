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
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// scanRestartError aborts a genesis attempt so Run restarts the scan from a
// fresh R0 — a bounded retry, distinct from a forced resync (the checkpoint
// is not invalidated). Buffered watch events were dropped, so the restarted
// scan owes a mark-and-sweep prune (deletes from the dropped window must not
// resurrect).
type scanRestartError struct {
	Cause ScanRestartCause
	Err   error
}

func (e *scanRestartError) Error() string {
	return fmt.Sprintf("genesis scan restart required (%s): %v", e.Cause, e.Err)
}
func (e *scanRestartError) Unwrap() error { return e.Err }

// replayBuffer drains the watch opened before a genesis scan into a
// byte-bounded buffer, so the reflector replay base is bounded by
// Config.WatchBufferBytes instead of clientv3's unbounded internal queue.
// On overflow — or a watch reconnect landing below the source compact
// revision — it cancels the watch and records a scanRestartError.
type replayBuffer struct {
	limit       int64
	cancelWatch context.CancelFunc

	mu    sync.Mutex
	resps []clientv3.WatchResponse
	bytes int64
	fail  *scanRestartError

	stopc chan struct{}
	donec chan struct{}
}

func newReplayBuffer(limit int64, cancelWatch context.CancelFunc) *replayBuffer {
	return &replayBuffer{
		limit:       limit,
		cancelWatch: cancelWatch,
		stopc:       make(chan struct{}),
		donec:       make(chan struct{}),
	}
}

// fill consumes wch until stop is called, the channel dies, or a restart
// condition hits. Run it in a goroutine; it never blocks the scan.
func (rb *replayBuffer) fill(wch clientv3.WatchChan) {
	defer close(rb.donec)
	for {
		select {
		case <-rb.stopc:
			return
		case wr, ok := <-wch:
			if !ok {
				// Channel died (transient watch failure): keep what was
				// buffered; the tail re-watches from the watermark and the
				// missed span comes from etcd's watch history.
				return
			}
			if wr.CompactRevision != 0 {
				rb.setFail(&scanRestartError{Cause: ScanRestartWatchCompactedMidScan, Err: wr.Err()})
				return
			}
			if wr.Err() != nil || wr.IsProgressNotify() {
				// Errors surface via channel close; progress notifications
				// carry nothing to replay (the watermark must stay the scan
				// base R0 while scanning).
				continue
			}
			var n int64
			for _, ev := range wr.Events {
				n += int64(len(ev.Kv.Key) + len(ev.Kv.Value))
			}
			rb.mu.Lock()
			rb.bytes += n
			over := rb.bytes > rb.limit
			if !over {
				rb.resps = append(rb.resps, wr)
			}
			rb.mu.Unlock()
			if over {
				rb.setFail(&scanRestartError{
					Cause: ScanRestartWatchBufferOverflow,
					Err: fmt.Errorf("replay buffer exceeded %d bytes before the base scan completed",
						rb.limit),
				})
				return
			}
		}
	}
}

// setFail records the restart condition and cancels the watch so the client
// stops accumulating events for a doomed attempt.
func (rb *replayBuffer) setFail(e *scanRestartError) {
	rb.mu.Lock()
	rb.fail = e
	rb.mu.Unlock()
	rb.cancelWatch()
}

// err returns the pending scanRestartError, if any.
func (rb *replayBuffer) err() error {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.fail != nil {
		return rb.fail
	}
	return nil
}

// stop halts filling and hands back the buffered responses for replay.
func (rb *replayBuffer) stop() []clientv3.WatchResponse {
	close(rb.stopc)
	<-rb.donec
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.resps
}

// genesis is the cold-start / forced-resync path: RequireEmpty gate, fence
// claim, watch-before-scan, unpinned chunked scan, optional prune pass,
// replay of the buffered watch events, then the live tail over the same
// watch channel.
func (a *Agent) genesis(ctx context.Context, st startState) error {
	a.setPhase(PhaseInitialSync)
	srcStart, srcEnd := a.rw.sourceRange()

	// A forced resync owes a mandatory mark-and-sweep, as does an
	// OverwriteAndPrune genesis. The obligation is stamped into every
	// checkpoint (FenceValue.PrunePending) until the prune completes, so a
	// crash mid-genesis cannot silently drop the owed sweep.
	if st.forced || a.cfg.InitialSyncMode == InitialSyncOverwriteAndPrune {
		a.prunePending = true
	}

	// Forced resyncs never re-check RequireEmpty: the fence proves the
	// destination data is this link's own. A cluster-identity mismatch
	// re-arms it.
	checkEmpty := (!st.haveCheckpoint && !st.forced) || st.rearmEmpty
	if a.cfg.InitialSyncMode == InitialSyncRequireEmpty && checkEmpty {
		if err := a.requireEmpty(ctx); err != nil {
			return err
		}
	}

	var r0 int64
	cursor := srcStart
	subrev := int64(0)
	if st.haveCheckpoint && st.scanning && st.scanCursor != "" {
		// Resume an interrupted scan from its cursor at the recorded base.
		r0 = st.watermark
		cursor = nextKey(st.scanCursor)
		subrev = st.subRevision
	} else {
		// Observe the scan base BEFORE scanning: the watch starts at r0+1
		// and buffered events replay over the scanned base. The windows'
		// counts are the InitialSync denominator, for free (excluded
		// prefixes are elided from the windows, so they are never counted).
		rev, total, err := a.countScanRanges(ctx)
		if err != nil {
			return err
		}
		r0 = rev
		a.update(func(s *Snapshot) {
			s.InitialSyncTotalKeyCount = total
			s.InitialSyncKeyCount = 0
			s.InitialSyncStartTime = time.Now()
			s.InitialSyncCompletionTime = time.Time{}
			s.LeaseBackedKeyCount = 0
			s.SourceRevision = r0
		})
	}

	// Claim (or refresh) the fence before any data write.
	if err := a.applyFenced(ctx, nil, a.newFence(r0, true, st.scanCursor, subrev), "", 0); err != nil {
		return err
	}

	// Open the watch BEFORE the scan (reflector pattern): events during the
	// scan buffer (byte-bounded) and replay over the scanned base
	// afterwards, so mid-scan compaction cannot invalidate anything the
	// scan needs — the scan itself reads unpinned, at the current revision.
	wctx, wcancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer wcancel()
	wch := a.src.Watch(wctx, srcStart, clientv3.WithRange(srcEnd),
		clientv3.WithRev(r0+1), clientv3.WithProgressNotify())
	rb := newReplayBuffer(a.cfg.WatchBufferBytes, wcancel)
	go rb.fill(wch)
	// Publish the cancel so a sustained target stall during the scan or the
	// replay can stop the source stream (the tail then re-watches from the
	// watermark). rb.fill tolerates the resulting channel death by design.
	a.watchCancel = wcancel

	if err := a.scan(ctx, cursor, r0, subrev, rb); err != nil {
		return err
	}

	// The mandatory mark-and-sweep: the OverwriteAndPrune genesis pass and
	// every forced resync, cleared only once the pass completed.
	if a.prunePending {
		drift, err := a.reconcilePass(ctx, true, true)
		if err != nil {
			return err
		}
		a.prunePending = false
		a.update(func(s *Snapshot) {
			s.LastReconcileTime = time.Now()
			d := drift
			s.LastReconcileDrift = &d
		})
	}

	// Scan complete: first revision-complete checkpoint at the scan base.
	if err := a.applyFenced(ctx, nil, a.newFence(r0, false, "", 0), "", 0); err != nil {
		return err
	}
	a.update(func(s *Snapshot) {
		s.InitialSyncCompletionTime = time.Now()
		if r0 > s.Watermark {
			s.Watermark = r0
		}
		s.LastProgressTime = time.Now()
	})

	// Hand the watch over from the buffer to the live tail: stop filling,
	// replay what was buffered over the scanned base, then consume the
	// still-open channel directly. A restart condition recorded at any
	// point during scan/prune aborts the attempt instead.
	buffered := rb.stop()
	if err := rb.err(); err != nil {
		return err
	}
	for i := range buffered {
		// live=false: replay applies run INSIDE the (possibly forced-resync)
		// genesis and must not reset the resync-loop detector.
		if err := a.handleResponse(ctx, &buffered[i], false); err != nil {
			return err
		}
	}
	return a.tail(ctx, wch, wcancel, r0)
}

// countScanRanges counts the in-scope source keys window by window and
// returns the revision R0 observed by the first (linearizable) read plus
// the total count. No Get ever spans an excluded range.
func (a *Agent) countScanRanges(ctx context.Context) (r0, total int64, err error) {
	ranges := a.rw.scanRanges()
	if len(ranges) == 0 {
		// Everything is excluded: one point read still pins R0.
		start, _ := a.rw.sourceRange()
		resp, gerr := a.getRetry(ctx, a.src, start, clientv3.WithCountOnly())
		if gerr != nil {
			return 0, 0, gerr
		}
		return resp.Header.Revision, 0, nil
	}
	for i, kr := range ranges {
		resp, gerr := a.getRetry(ctx, a.src, kr.start,
			clientv3.WithRange(kr.end), clientv3.WithCountOnly())
		if gerr != nil {
			return 0, 0, gerr
		}
		if i == 0 {
			r0 = resp.Header.Revision
		}
		total += resp.Count
	}
	return r0, total, nil
}

// scan pulls byte-bounded pages at the current revision (never pinned) over
// the decomposed in-scope windows and applies them as fenced Txns carrying
// the in-scan checkpoint. rb is polled between pages so an overflowed or
// compacted replay buffer aborts the attempt promptly.
func (a *Agent) scan(ctx context.Context, cursor string, r0, subrev int64, rb *replayBuffer) error {
	b := newBatcher(a.cfg.MaxTxnOps, a.cfg.TxnFlushBytes)
	limit := a.cfg.PageKeyLimit
	for _, kr := range a.rw.scanRanges() {
		if !endAfter(kr.end, cursor) {
			continue // window fully below the resume cursor
		}
		next := kr.start
		if cursor > next {
			next = cursor
		}
		var err error
		if limit, subrev, err = a.scanWindow(ctx, next, kr.end, r0, limit, subrev, b, rb); err != nil {
			return err
		}
	}
	if fs := b.flush(); fs != nil {
		subrev++
		return a.applyScanFlush(ctx, fs, subrev)
	}
	return rb.err()
}

// scanWindow pages one [cursor, end) window through the shared batcher.
func (a *Agent) scanWindow(
	ctx context.Context, cursor, end string, r0 int64,
	limit int, subrev int64, b *batcher, rb *replayBuffer,
) (int, int64, error) {
	for {
		if err := rb.err(); err != nil {
			return limit, subrev, err
		}
		resp, err := a.getRetry(ctx, a.src, cursor, clientv3.WithRange(end),
			clientv3.WithLimit(int64(limit)),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			return limit, subrev, err
		}
		if len(resp.Kvs) == 0 {
			return limit, subrev, nil
		}
		var pageBytes, leased int64
		for _, kv := range resp.Kvs {
			pageBytes += int64(len(kv.Key) + len(kv.Value))
			dstKey, ok := a.rw.rewrite(string(kv.Key))
			if !ok {
				continue
			}
			if kv.Lease != 0 {
				leased++
			}
			// Synthetic single-key groups: a snapshot has no per-key
			// revision boundaries to preserve.
			g := revGroup{rev: r0, ops: []kvOp{{
				key: dstKey, value: string(kv.Value), srcKey: string(kv.Key),
			}}}
			for _, fs := range b.add(g) {
				subrev++
				if err := a.applyScanFlush(ctx, &fs, subrev); err != nil {
					return limit, subrev, err
				}
			}
		}
		if leased > 0 {
			a.update(func(s *Snapshot) { s.LeaseBackedKeyCount += leased })
		}
		limit = adaptLimit(limit, a.cfg.PageKeyLimit, pageBytes, a.cfg.PageBytes)
		if !resp.More {
			return limit, subrev, nil
		}
		cursor = nextKey(string(resp.Kvs[len(resp.Kvs)-1].Key))
	}
}

// applyScanFlush applies one scan flush set with the in-scan checkpoint
// (Scanning=true, cursor advanced, page ordinal in SubRevision) riding the
// same Txn, so a restarted agent resumes the scan instead of starting over.
func (a *Agent) applyScanFlush(ctx context.Context, fs *flushSet, subrev int64) error {
	f := a.newFence(fs.watermark, true, fs.lastSrcKey, subrev)
	if err := a.applyOps(ctx, fs, f); err != nil {
		return err
	}
	a.update(func(s *Snapshot) {
		s.InitialSyncKeyCount += int64(len(fs.ops))
		s.LastProgressTime = time.Now()
	})
	return nil
}

// requireEmpty enforces InitialSyncRequireEmpty over the effective
// destination prefix; the reserved checkpoint key is excluded by exact
// match.
func (a *Agent) requireEmpty(ctx context.Context) error {
	start, end := a.rw.destRange()
	resp, err := a.getRetry(ctx, a.dst, start, clientv3.WithRange(end), clientv3.WithCountOnly())
	if err != nil {
		return err
	}
	n := resp.Count
	if n > 0 {
		ck, cerr := a.getRetry(ctx, a.dst, a.cfg.CheckpointKey, clientv3.WithCountOnly())
		if cerr != nil {
			return cerr
		}
		n -= ck.Count
	}
	if n > 0 {
		return &EmptyTargetViolationError{RangeStart: start, RangeEnd: end, KeyCount: n}
	}
	return nil
}

// adaptLimit derives the next page's key limit from the observed bytes/key,
// enforcing the PageBytes bound etcd Range lacks natively.
func adaptLimit(cur, maxLimit int, gotBytes, maxBytes int64) int {
	if gotBytes <= 0 {
		return cur
	}
	switch {
	case gotBytes > maxBytes && cur > 1:
		cur /= 2
		if cur < 1 {
			cur = 1
		}
	case gotBytes*2 < maxBytes && cur < maxLimit:
		cur *= 2
		if cur > maxLimit {
			cur = maxLimit
		}
	}
	return cur
}
