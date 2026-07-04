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

// Integration tests for the Design-3 gap-closure delta: replay-buffer
// overflow restarts, the durable PrunePending flag, cluster-ID re-arm,
// the resync-loop latch, exclude-range elision, oversize permanence,
// fail-closed checkpoints, the version floor, progress-notify liveness,
// and duplicate-replay idempotence.
package mirroragent_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// bigValueClient returns a client whose send cap admits values larger than
// the 2MiB clientv3 default, for seeding oversize test data.
func bigValueClient(t *testing.T, target *clientv3.Client) *clientv3.Client {
	t.Helper()
	cfg := mirroragent.NewClientConfig(target.Endpoints(), nil, 5*time.Second)
	cfg.MaxCallSendMsgSize = 16 << 20
	cli, err := clientv3.New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// TestWatchBufferOverflowRestart covers the bounded replay buffer: with a
// tiny WatchBufferBytes and sustained mid-scan churn, genesis attempts are
// aborted and restarted from a fresh R0 (never unbounded growth), the cause
// is surfaced in the snapshot, repeated restarts trip the resync-loop
// detector, and once churn stops the mirror still converges byte-exact.
func TestWatchBufferOverflowRestart(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.WatchBufferBytes = 512 // a handful of events
	cfg.MaxOpsPerSecond = 150  // keep each scan attempt slow enough to race
	cfg.PageKeyLimit = 25
	cfg.ResyncLoopThreshold = 2

	putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 150)

	r := startAgent(t, cfg, src, dst)

	// Churn during the scan: each write lands in the replay buffer. Values
	// are sized so a SINGLE churn event overflows the 512-byte buffer —
	// overflow must not depend on sustaining a wall-clock churn rate on a
	// loaded runner.
	churnValue := strings.Repeat("c", 1024)
	churnCtx, stopChurn := context.WithCancel(t.Context())
	defer stopChurn()
	churnDone := make(chan struct{})
	go func() {
		defer close(churnDone)
		for i := 0; churnCtx.Err() == nil; i++ {
			k := fmt.Sprintf("churn-%04d", i)
			if _, err := src.Put(churnCtx, cfg.SourcePrefix+k, churnValue); err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	snap := waitSnap(t, r.agent, 30*time.Second, "buffer overflow restarts",
		func(s mirroragent.Snapshot) bool { return s.ScanRestartCount >= 2 })
	assert.Equal(t, mirroragent.ScanRestartWatchBufferOverflow, snap.LastScanRestartCause)
	assert.EqualValues(t, 0, snap.ForcedResyncCount,
		"a buffer-bound restart is not a forced resync — the checkpoint was never invalidated")
	waitSnap(t, r.agent, 30*time.Second, "resync-loop detector tripped by repeated overflows",
		func(s mirroragent.Snapshot) bool { return s.ResyncLoopDetected })

	stopChurn()
	<-churnDone

	// Byte-exact convergence against source truth, re-read each poll: a
	// churn Put cancelled mid-flight can still land server-side after the
	// stop signal, so the authority is whatever the source holds NOW.
	var want map[string]string
	deadline := time.Now().Add(90 * time.Second)
	for {
		sresp, err := src.Get(t.Context(), cfg.SourcePrefix, clientv3.WithPrefix())
		require.NoError(t, err)
		want = make(map[string]string, len(sresp.Kvs))
		for _, kv := range sresp.Kvs {
			want[cfg.TargetPrefix+strings.TrimPrefix(string(kv.Key), cfg.SourcePrefix)] = string(kv.Value)
		}
		if got := targetData(t, dst, cfg); mapsEqual(got, want) {
			break
		}
		require.True(t, time.Now().Before(deadline),
			"target never converged to source truth after churn stopped")
		time.Sleep(100 * time.Millisecond)
	}

	// Post-churn writes reach steady state and clear the detector. A write
	// can still be swallowed into a final scan/replay (which must NOT clear
	// the latch), so keep writing until one lands as a live tail apply.
	deadline = time.Now().Add(30 * time.Second)
	for i := 0; r.agent.Snapshot().ResyncLoopDetected; i++ {
		require.True(t, time.Now().Before(deadline),
			"detector never cleared at steady state despite live writes")
		_, err := src.Put(t.Context(), fmt.Sprintf("/src/settled-%d", i), "yes")
		require.NoError(t, err)
		time.Sleep(150 * time.Millisecond)
	}
}

// TestPrunePendingCrashResume is THE new-mechanism test: an agent that is
// killed mid-forced-resync — after the fence records PrunePending=true but
// before the mark-and-sweep ran — must still run the prune after restart,
// so a delete from the blind window cannot resurrect on the target.
func TestPrunePendingCrashResume(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 40)
	r1 := startAgent(t, cfg, src, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	waitSnap(t, r1.agent, 10*time.Second, "run 1 Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	require.ErrorIs(t, r1.stop(t), context.Canceled)

	// Blind window while the agent is down: delete one mirrored key, add
	// two, then compact past the watermark so the re-watch is doomed.
	ctx := t.Context()
	_, err := src.Delete(ctx, "/src/key-0005")
	require.NoError(t, err)
	delete(want, "/dst/key-0005")
	for i := range 2 {
		k := fmt.Sprintf("late-%d", i)
		_, perr := src.Put(ctx, cfg.SourcePrefix+k, "late")
		require.NoError(t, perr)
		want[cfg.TargetPrefix+k] = "late"
	}
	head, err := src.Get(ctx, "/src/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	_, err = src.Compact(ctx, head.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)

	// Run 2 hits ErrCompacted and starts the forced resync (slowed down and
	// with small Txns so the fence records a MID-SCAN cursor). It is killed
	// only once the fence shows PrunePending AND a non-empty scan cursor, so
	// run 3 must exercise the cursor-resume branch, not the fresh-R0 path.
	cfg2 := cfg
	cfg2.Epoch = 2
	cfg2.MaxOpsPerSecond = 25
	cfg2.MaxTxnOps = 6 // 5 data slots: the cursor advances every 5 keys
	r2 := startAgent(t, cfg2, src, dst)
	deadline := time.Now().Add(20 * time.Second)
	for {
		require.True(t, time.Now().Before(deadline),
			"fence never recorded PrunePending with a mid-scan cursor")
		resp, gerr := dst.Get(ctx, checkpointKey(cfg))
		require.NoError(t, gerr)
		if len(resp.Kvs) == 1 {
			f, derr := mirroragent.DecodeFenceValue(resp.Kvs[0].Value)
			require.NoError(t, derr)
			if f.PrunePending && f.Scanning && f.ScanCursor != "" {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	r2.cancel()
	require.ErrorIs(t, r2.waitErr(t, 15*time.Second), context.Canceled)

	// The kill must have landed mid-scan for the resume branch to be under
	// test at all; the paced scan (40 keys at 25 ops/s) makes this window
	// seconds wide.
	killed, _ := readFence(t, dst, cfg)
	require.True(t, killed.Scanning && killed.ScanCursor != "",
		"test premise: run 2 must die MID-SCAN (fence: %+v) — widen the pacing if this trips", killed)

	// The blind-window delete has resurrection potential right now.
	resp, err := dst.Get(ctx, "/dst/key-0005")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1,
		"test setup: the deleted key must still be on the target before the prune")

	// Run 3 resumes purely from the durable fence state: the scan continues
	// from the recorded cursor (no re-count, no re-applied pre-cursor keys)
	// and the owed sweep still runs without re-detecting the compaction.
	cfg3 := cfg
	cfg3.Epoch = 3
	rsrc := &rangeRecordingClient{Client: src}
	r3 := startAgent(t, cfg3, rsrc, dst)
	waitTargetData(t, dst, cfg, 30*time.Second, want)
	snap := waitSnap(t, r3.agent, 20*time.Second, "run 3 Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	assert.EqualValues(t, 0, snap.ForcedResyncCount,
		"run 3 resumed an owed prune; it did not have to re-detect the compaction")
	f, _ := readFence(t, dst, cfg)
	assert.False(t, f.PrunePending, "the flag clears once the sweep completed")
	assert.False(t, f.Scanning)

	// Resume-branch pin: run 3's FIRST source read must be the scan page
	// starting just after the recorded cursor. The fresh-R0 path would open
	// with a CountOnly read at the range start instead — and a broken resume
	// (panic/skip) could only be reached through that first read.
	windows := rsrc.recorded()
	require.NotEmpty(t, windows)
	assert.Equal(t, killed.ScanCursor+"\x00", windows[0][0],
		"run 3's first source read must resume at nextKey(fence.ScanCursor), not rescan from the start")
}

// TestClusterIDMismatchRearm covers the dual-identity binding: a checkpoint
// bound to a different source OR target cluster forces genesis and RE-ARMS
// RequireEmpty (unlike an ordinary forced resync, which skips it because the
// fence proves ownership).
func TestClusterIDMismatchRearm(t *testing.T) {
	t.Run("FreshTargetRearms", func(t *testing.T) {
		src := startEtcd(t, nil)
		dstA := startEtcd(t, nil)
		cfg := baseCfg("/src/", "/dst/")

		want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 5)
		r1 := startAgent(t, cfg, src, dstA)
		waitTargetData(t, dstA, cfg, 20*time.Second, want)
		require.ErrorIs(t, r1.stop(t), context.Canceled)

		// A rebuilt target carrying the old fence (e.g. restored from a
		// snapshot of another cluster) plus pre-existing data.
		dstB := startEtcd(t, nil)
		ctx := t.Context()
		fresp, err := dstA.Get(ctx, checkpointKey(cfg))
		require.NoError(t, err)
		require.Len(t, fresp.Kvs, 1)
		staleFence := string(fresp.Kvs[0].Value)
		_, err = dstB.Put(ctx, checkpointKey(cfg), staleFence)
		require.NoError(t, err)
		_, err = dstB.Put(ctx, "/dst/preexisting", "dirty")
		require.NoError(t, err)

		cfg2 := cfg
		cfg2.Epoch = 2
		r2 := startAgent(t, cfg2, src, dstB)
		runErr := r2.waitErr(t, 20*time.Second)
		var ev *mirroragent.EmptyTargetViolationError
		require.ErrorAs(t, runErr, &ev,
			"the re-armed RequireEmpty must trip on the non-empty fresh target, got: %v", runErr)
		snap := r2.agent.Snapshot()
		assert.Equal(t, mirroragent.ResyncReasonClusterIDMismatch, snap.LastResyncReason)
		assert.EqualValues(t, 1, snap.ForcedResyncCount)

		// Failing the gate must write nothing — the stale fence is intact.
		after, err := dstB.Get(ctx, checkpointKey(cfg))
		require.NoError(t, err)
		require.Len(t, after.Kvs, 1)
		assert.Equal(t, staleFence, string(after.Kvs[0].Value))
	})

	t.Run("FreshSourceRearms", func(t *testing.T) {
		srcA := startEtcd(t, nil)
		dst := startEtcd(t, nil)
		cfg := baseCfg("/src/", "/dst/")

		want := putN(t, srcA, cfg.SourcePrefix, cfg.TargetPrefix, 5)
		r1 := startAgent(t, cfg, srcA, dst)
		waitTargetData(t, dst, cfg, 20*time.Second, want)
		require.ErrorIs(t, r1.stop(t), context.Canceled)

		// Repointing at a different source cluster: the mirrored data on the
		// target is no longer provably "this link's own" relative to the new
		// source, so RequireEmpty re-arms and trips on it.
		srcB := startEtcd(t, nil)
		_, err := srcB.Put(t.Context(), "/src/other", "different-world")
		require.NoError(t, err)

		cfg2 := cfg
		cfg2.Epoch = 2
		r2 := startAgent(t, cfg2, srcB, dst)
		runErr := r2.waitErr(t, 20*time.Second)
		var ev *mirroragent.EmptyTargetViolationError
		require.ErrorAs(t, runErr, &ev, "got: %v", runErr)
		assert.Equal(t, mirroragent.ResyncReasonClusterIDMismatch, r2.agent.Snapshot().LastResyncReason)
	})

	t.Run("FreshSourceOverwriteAndPruneConverges", func(t *testing.T) {
		srcA := startEtcd(t, nil)
		dst := startEtcd(t, nil)
		cfg := baseCfg("/src/", "/dst/")
		cfg.InitialSyncMode = mirroragent.InitialSyncOverwrite

		want := putN(t, srcA, cfg.SourcePrefix, cfg.TargetPrefix, 5)
		r1 := startAgent(t, cfg, srcA, dst)
		waitTargetData(t, dst, cfg, 20*time.Second, want)
		require.ErrorIs(t, r1.stop(t), context.Canceled)

		srcB := startEtcd(t, nil)
		_, err := srcB.Put(t.Context(), "/src/fresh", "new-world")
		require.NoError(t, err)

		cfg2 := cfg
		cfg2.Epoch = 2
		r2 := startAgent(t, cfg2, srcB, dst)
		// The mismatch forces genesis with a mandatory mark-and-sweep: srcA's
		// five keys are orphans against srcB and must be pruned even though
		// the mode is plain Overwrite.
		waitTargetData(t, dst, cfg, 30*time.Second, map[string]string{"/dst/fresh": "new-world"})
		snap := r2.agent.Snapshot()
		assert.Equal(t, mirroragent.ResyncReasonClusterIDMismatch, snap.LastResyncReason)
		f, _ := readFence(t, dst, cfg)
		assert.False(t, f.PrunePending)
		assert.Equal(t, snap.SourceClusterID, f.SourceClusterID,
			"the checkpoint must re-bind to the new source cluster identity")
	})
}

// breakableWatchClient wraps a source client so the test can (a) kill the
// live watch stream and (b) serve injected already-compacted watch responses
// — the deterministic stand-in for "retention outran the watch".
type breakableWatchClient struct {
	mirroragent.Client
	inject atomic.Bool

	mu      sync.Mutex
	cancels []context.CancelFunc
}

func (c *breakableWatchClient) Watch(
	ctx context.Context, key string, opts ...clientv3.OpOption,
) clientv3.WatchChan {
	if c.inject.Load() {
		ch := make(chan clientv3.WatchResponse, 1)
		ch <- clientv3.WatchResponse{Canceled: true, CompactRevision: 3}
		close(ch)
		return ch
	}
	wctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancels = append(c.cancels, cancel)
	c.mu.Unlock()
	return c.Client.Watch(wctx, key, opts...)
}

func (c *breakableWatchClient) breakWatches() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cancel := range c.cancels {
		cancel()
	}
	c.cancels = nil
}

// TestResyncLoopLatch covers the livelock detector: consecutive compaction
// failures without an intervening steady state latch ResyncLoopDetected, the
// latch does not self-clear while the loop continues, and it clears only
// when steady state is finally reached.
func TestResyncLoopLatch(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ResyncLoopThreshold = 2

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 8)
	bsrc := &breakableWatchClient{Client: src}
	r := startAgent(t, cfg, bsrc, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	waitSnap(t, r.agent, 10*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	// Kill the live watch; every re-watch now lands below the "compact
	// revision" — the retention < scan-time livelock signature.
	bsrc.inject.Store(true)
	bsrc.breakWatches()

	snap := waitSnap(t, r.agent, 20*time.Second, "first forced resync",
		func(s mirroragent.Snapshot) bool { return s.ForcedResyncCount >= 1 })
	assert.Equal(t, mirroragent.ResyncReasonCompacted, snap.LastResyncReason)
	waitSnap(t, r.agent, 20*time.Second, "livelock latch",
		func(s mirroragent.Snapshot) bool { return s.ResyncLoopDetected })

	// The latch must hold while the loop continues.
	time.Sleep(400 * time.Millisecond)
	assert.True(t, r.agent.Snapshot().ResyncLoopDetected, "the latch must not self-clear mid-loop")

	// Heal the source; the next attempt converges.
	bsrc.inject.Store(false)
	_, err := src.Put(t.Context(), "/src/healed", "ok")
	require.NoError(t, err)
	want["/dst/healed"] = "ok"
	waitTargetData(t, dst, cfg, 30*time.Second, want)

	// Steady state = a successfully applied TAIL response; only that clears
	// the latch (scan convergence alone does not).
	_, err = src.Put(t.Context(), "/src/steady", "ok")
	require.NoError(t, err)
	want["/dst/steady"] = "ok"
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	waitSnap(t, r.agent, 20*time.Second, "latch clears at steady state",
		func(s mirroragent.Snapshot) bool { return !s.ResyncLoopDetected && !s.Compacted })
}

// rangeRecordingClient records every Get's [start, end) window, to prove
// exclusion by range decomposition rather than client-side filtering.
type rangeRecordingClient struct {
	mirroragent.Client
	mu      sync.Mutex
	windows [][2]string
}

func (c *rangeRecordingClient) Get(
	ctx context.Context, key string, opts ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
	op := clientv3.OpGet(key, opts...)
	c.mu.Lock()
	c.windows = append(c.windows, [2]string{string(op.KeyBytes()), string(op.RangeBytes())})
	c.mu.Unlock()
	return c.Client.Get(ctx, key, opts...)
}

func (c *rangeRecordingClient) recorded() [][2]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][2]string(nil), c.windows...)
}

// TestExcludeRangeElision covers exclude handling at the RPC level: no
// source Get window may even overlap an excluded range — excluded data is
// never transferred, not fetched-then-dropped. The mode is deliberately
// OverwriteAndPrune so the assertion also covers the reconcile/prune pass
// (the code path every forced resync and drain repair runs): operators
// exclude e.g. /secrets/ precisely so those values never leave the source
// network, resyncs included.
func TestExcludeRangeElision(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.InitialSyncMode = mirroragent.InitialSyncOverwriteAndPrune
	cfg.ExcludePrefixes = []string{"/src/skip/"}
	cfg.PageKeyLimit = 3 // several pages, several windows

	ctx := t.Context()
	want := map[string]string{}
	for i := range 10 {
		a := fmt.Sprintf("/src/aa-%02d", i)
		z := fmt.Sprintf("/src/zz-%02d", i)
		s := fmt.Sprintf("/src/skip/%02d", i)
		for _, kv := range [][2]string{{a, "a"}, {z, "z"}, {s, "never"}} {
			_, err := src.Put(ctx, kv[0], kv[1])
			require.NoError(t, err)
		}
		want["/dst/"+strings.TrimPrefix(a, "/src/")] = "a"
		want["/dst/"+strings.TrimPrefix(z, "/src/")] = "z"
	}

	rsrc := &rangeRecordingClient{Client: src}
	r := startAgent(t, cfg, rsrc, dst)
	snap := waitSnap(t, r.agent, 20*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	waitTargetData(t, dst, cfg, 10*time.Second, want)
	assert.EqualValues(t, 20, snap.InitialSyncTotalKeyCount,
		"excluded keys must not appear in the InitialSync denominator")

	// A live write under the excluded prefix must not arrive either (the
	// watch filters client-side; only range reads decompose).
	_, err := src.Put(ctx, "/src/skip/live", "never")
	require.NoError(t, err)
	_, err = src.Put(ctx, "/src/aa-live", "a")
	require.NoError(t, err)
	want["/dst/aa-live"] = "a"
	waitTargetData(t, dst, cfg, 10*time.Second, want)

	exclStart, exclEnd := "/src/skip/", "/src/skip0"
	for _, w := range rsrc.recorded() {
		start, end := w[0], w[1]
		if end == "" {
			end = start + "\x00" // point Get
		}
		overlaps := start < exclEnd && (end == "\x00" || end > exclStart)
		assert.False(t, overlaps,
			"source Get window [%q, %q) overlaps the excluded range — exclusion must be server-side elision",
			w[0], w[1])
	}
}

// TestOversizedPermanentBands covers both oversize failure bands: the
// server's request-size reject and the gRPC client send cap. Both classify
// Permanent with distinct causes and a redacted key — never throttling,
// never a retry loop.
func TestOversizedPermanentBands(t *testing.T) {
	//nolint:dupl // the two bands intentionally mirror each other with different servers/causes
	t.Run("ServerRejectBand", func(t *testing.T) {
		src := startEtcd(t, func(c *embed.Config) { c.MaxRequestBytes = 8 << 20 })
		dst := startEtcd(t, nil) // default ~1.5MiB request ceiling
		cfg := baseCfg("/src/", "/dst/")
		cfg.InitialSyncMode = mirroragent.InitialSyncOverwrite

		seed := bigValueClient(t, src)
		_, err := seed.Put(t.Context(), "/src/poison-key-server", strings.Repeat("v", 1600*1024))
		require.NoError(t, err)

		countingDst := &countingClient{Client: dst}
		r := startAgent(t, cfg, src, countingDst)
		runErr := r.waitErr(t, 30*time.Second)
		var tle *mirroragent.TooLargeError
		require.ErrorAs(t, runErr, &tle, "got: %v", runErr)
		assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(runErr))
		assert.Contains(t, runErr.Error(), "request is too large",
			"the server reject band must surface its distinct cause")
		assert.Contains(t, tle.Key, "/dst/…", "the offending key must be surfaced redacted")
		assert.NotContains(t, tle.Key, "poison", "raw key bytes must never surface")

		// Permanent means no retry loop: fence claim + the poison attempt
		// (single-revision set — no shrink) + slack, counted over the WHOLE
		// run (Run has returned, so this bounds every commit ever made).
		assert.LessOrEqual(t, countingDst.txns.Load(), int64(4),
			"a permanent oversize must never be retried")
		assert.Equal(t, mirroragent.PhaseFailed, r.agent.Snapshot().Phase)
	})

	//nolint:dupl // the two bands intentionally mirror each other with different servers/causes
	t.Run("ClientSendCapBand", func(t *testing.T) {
		src := startEtcd(t, func(c *embed.Config) { c.MaxRequestBytes = 8 << 20 })
		// The target server would accept it; the CLIENT's 2MiB send cap is
		// the limit under test.
		dst := startEtcd(t, func(c *embed.Config) { c.MaxRequestBytes = 8 << 20 })
		cfg := baseCfg("/src/", "/dst/")
		cfg.InitialSyncMode = mirroragent.InitialSyncOverwrite

		seed := bigValueClient(t, src)
		_, err := seed.Put(t.Context(), "/src/poison-key-client", strings.Repeat("v", 3<<20))
		require.NoError(t, err)

		countingDst := &countingClient{Client: dst}
		r := startAgent(t, cfg, src, countingDst)
		runErr := r.waitErr(t, 30*time.Second)
		var tle *mirroragent.TooLargeError
		require.ErrorAs(t, runErr, &tle, "got: %v", runErr)
		assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(runErr))
		assert.Contains(t, runErr.Error(), "larger than max",
			"the client send cap band must surface its distinct cause")
		assert.NotContains(t, tle.Key, "poison")
		assert.LessOrEqual(t, countingDst.txns.Load(), int64(4),
			"a permanent client-cap oversize must never be retried")
		snap := r.agent.Snapshot()
		assert.Equal(t, mirroragent.ClassPermanent, snap.LastErrorClass,
			"the send cap shares ResourceExhausted with throttling and must NOT classify Throttle")
		assert.False(t, snap.Throttled)
	})
}

// TestCorruptCheckpointFailsClosed covers the fail-closed contract: garbage
// or an unknown-version document at the reserved key stops the agent
// permanently — no genesis, no resync, zero writes.
func TestCorruptCheckpointFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "Garbage", raw: "\x00\x01 not a checkpoint"},
		{name: "FutureVersion", raw: `{"v":99,"linkUID":"test-link","epoch":1,"role":"Mirror",` +
			`"watermark":10,"sourceClusterID":"1","targetClusterID":"2"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := startEtcd(t, nil)
			dst := startEtcd(t, nil)
			cfg := baseCfg("/src/", "/dst/")
			putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 3)

			ctx := t.Context()
			_, err := dst.Put(ctx, checkpointKey(cfg), tc.raw)
			require.NoError(t, err)
			preRev, err := dst.Get(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithCountOnly())
			require.NoError(t, err)

			r := startAgent(t, cfg, src, dst)
			runErr := r.waitErr(t, 20*time.Second)
			var ci *mirroragent.CheckpointInvalidError
			require.ErrorAs(t, runErr, &ci, "got: %v", runErr)
			assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(runErr))
			snap := r.agent.Snapshot()
			assert.Equal(t, mirroragent.PhaseFailed, snap.Phase)
			assert.EqualValues(t, 0, snap.ForcedResyncCount, "fail closed means NO resync")

			// Zero writes of any kind: the target revision did not move and
			// the reserved key still holds the exact original bytes.
			post, err := dst.Get(ctx, checkpointKey(cfg))
			require.NoError(t, err)
			require.Len(t, post.Kvs, 1)
			assert.Equal(t, tc.raw, string(post.Kvs[0].Value))
			assert.Equal(t, preRev.Header.Revision, post.Header.Revision,
				"the agent must not have written anything to the target")
		})
	}
}

// versionStubClient overrides the maintenance Status version — the seam for
// the version-floor probe.
type versionStubClient struct {
	mirroragent.Client
	version string
}

func (c *versionStubClient) Status(
	ctx context.Context, endpoint string,
) (*clientv3.StatusResponse, error) {
	resp, err := c.Client.Status(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	resp.Version = c.version
	return resp, nil
}

// TestVersionFloor covers the declared >=3.4 hard floor: a source below it
// fails permanently at connect, before any scan read or target write.
func TestVersionFloor(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 3)

	countingSrc := &countingClient{Client: src}
	countingDst := &countingClient{Client: dst}
	stubSrc := &versionStubClient{Client: countingSrc, version: "3.3.0"}

	r := startAgent(t, cfg, stubSrc, countingDst)
	runErr := r.waitErr(t, 20*time.Second)
	var uv *mirroragent.UnsupportedVersionError
	require.ErrorAs(t, runErr, &uv, "got: %v", runErr)
	assert.Equal(t, "source", uv.Side)
	assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(runErr))
	assert.Equal(t, mirroragent.PhaseFailed, r.agent.Snapshot().Phase)

	assert.EqualValues(t, 0, countingSrc.gets.Load(), "no scan read may precede the version gate")
	assert.EqualValues(t, 0, countingDst.txns.Load(), "no target write may precede the version gate")
}

// TestProgressNotifyAdvancesIdleWatermark is the RequestProgress metadata
// regression test: on an idle prefix the watermark must still advance via
// client-driven progress requests. It fails if the RequestProgress context
// metadata ever diverges from the Watch context (watcher gRPC streams are
// keyed by outgoing metadata, and the engine watches WithRequireLeader).
func TestProgressNotifyAdvancesIdleWatermark(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ProgressInterval = 150 * time.Millisecond

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 3)
	r := startAgent(t, cfg, src, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)

	// The mirrored prefix goes idle; only out-of-prefix writes move the
	// cluster revision.
	ctx := t.Context()
	var outRev int64
	for i := range 5 {
		resp, err := src.Put(ctx, fmt.Sprintf("/elsewhere/%d", i), "x")
		require.NoError(t, err)
		outRev = resp.Header.Revision
	}

	snap := waitSnap(t, r.agent, 20*time.Second, "watermark advance on an idle prefix",
		func(s mirroragent.Snapshot) bool { return s.Watermark >= outRev })
	assert.GreaterOrEqual(t, snap.SourceRevision, outRev)
	f, _ := readFence(t, dst, cfg)
	assert.GreaterOrEqual(t, f.Watermark, outRev,
		"the fenced checkpoint — not just the snapshot — must carry the progress watermark")
}

// duplicatingWatchClient delivers every data-bearing watch response twice.
type duplicatingWatchClient struct {
	mirroragent.Client
}

func (c *duplicatingWatchClient) Watch(
	ctx context.Context, key string, opts ...clientv3.OpOption,
) clientv3.WatchChan {
	in := c.Client.Watch(ctx, key, opts...)
	out := make(chan clientv3.WatchResponse)
	go func() {
		defer close(out)
		for wr := range in {
			out <- wr
			if len(wr.Events) > 0 && wr.Err() == nil {
				out <- wr
			}
		}
	}()
	return out
}

// TestDuplicateReplayIdempotent covers replay idempotence: duplicate event
// delivery (the reflector's overlap case, forced here for every response)
// re-puts value-identical keys — no drift, no divergence, and the drain
// verification still proves per-side equality.
func TestDuplicateReplayIdempotent(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	dupSrc := &duplicatingWatchClient{Client: src}
	r := startAgent(t, cfg, dupSrc, dst)
	waitSnap(t, r.agent, 20*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	ctx := t.Context()
	want := map[string]string{}
	for i := range 10 {
		k := fmt.Sprintf("dup-%02d", i)
		_, err := src.Put(ctx, cfg.SourcePrefix+k, "v")
		require.NoError(t, err)
		want[cfg.TargetPrefix+k] = "v"
	}
	_, err := src.Delete(ctx, "/src/dup-03")
	require.NoError(t, err)
	delete(want, "/dst/dup-03")

	waitTargetData(t, dst, cfg, 20*time.Second, want)
	snap := r.agent.Snapshot()
	assert.EqualValues(t, 0, snap.ForcedResyncCount)
	assert.Empty(t, snap.LastError, "duplicate delivery must not surface any error")

	// The strongest idempotence proof: drain verification counts both sides
	// and only cuts over on exact equality.
	r.agent.RequestDrain()
	require.NoError(t, r.waitErr(t, 30*time.Second), "a completed drain returns nil")
	final := r.agent.Snapshot()
	require.NotNil(t, final.Cutover)
	assert.EqualValues(t, 9, final.Cutover.SourceKeyCount)
	assert.EqualValues(t, 9, final.Cutover.TargetKeyCount)
	assert.EqualValues(t, 9, final.SourceKeyCount,
		"the always-on per-side key counts must be populated by the verification pass")
	assert.EqualValues(t, 9, final.TargetKeyCount)
	assert.True(t, final.CutoverReady)
}

// ambiguousTxnClient makes the next successful Commit report an ambiguous
// timeout: the Txn commits server-side but the caller sees DeadlineExceeded
// — the classic blackholed-NLB lost-response scenario.
type ambiguousTxnClient struct {
	mirroragent.Client
	arm atomic.Bool
}

func (c *ambiguousTxnClient) Txn(ctx context.Context) clientv3.Txn {
	return &ambiguousTxn{inner: c.Client.Txn(ctx), c: c}
}

type ambiguousTxn struct {
	inner clientv3.Txn
	c     *ambiguousTxnClient
}

func (t *ambiguousTxn) If(cs ...clientv3.Cmp) clientv3.Txn { t.inner = t.inner.If(cs...); return t }
func (t *ambiguousTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.inner = t.inner.Then(ops...)
	return t
}
func (t *ambiguousTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.inner = t.inner.Else(ops...)
	return t
}
func (t *ambiguousTxn) Commit() (*clientv3.TxnResponse, error) {
	resp, err := t.inner.Commit()
	if err == nil && resp.Succeeded && t.c.arm.CompareAndSwap(true, false) {
		return nil, context.DeadlineExceeded
	}
	return resp, err
}

// TestAmbiguousCommitAdopted covers the spec's re-read-recompute-retry rule
// for fenced Txns: when a committed Txn's response is lost, the retry's
// compare fails against the agent's OWN write — the engine must recognize
// the stored fence as this attempt's value and adopt it, never misreport a
// permanent fence violation.
func TestAmbiguousCommitAdopted(t *testing.T) {
	t.Run("SteadyApply", func(t *testing.T) {
		src := startEtcd(t, nil)
		dst := startEtcd(t, nil)
		cfg := baseCfg("/src/", "/dst/")

		amb := &ambiguousTxnClient{Client: dst}
		r := startAgent(t, cfg, src, amb)
		waitSnap(t, r.agent, 20*time.Second, "Syncing",
			func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

		amb.arm.Store(true)
		putResp, err := src.Put(t.Context(), "/src/ambiguous", "survives")
		require.NoError(t, err)
		waitTargetData(t, dst, cfg, 20*time.Second, map[string]string{"/dst/ambiguous": "survives"})

		snap := waitSnap(t, r.agent, 20*time.Second, "recovered to Syncing",
			func(s mirroragent.Snapshot) bool {
				return s.Phase == mirroragent.PhaseSyncing && s.Watermark >= putResp.Header.Revision
			})
		assert.EqualValues(t, 0, snap.ForcedResyncCount)
		f, _ := readFence(t, dst, cfg)
		assert.Equal(t, putResp.Header.Revision, f.Watermark,
			"the adopted commit's checkpoint is the authoritative watermark")
		select {
		case runErr := <-r.done:
			t.Fatalf("Run returned after an ambiguous commit: %v", runErr)
		default:
		}
	})

	t.Run("DrainRoleFlip", func(t *testing.T) {
		src := startEtcd(t, nil)
		dst := startEtcd(t, nil)
		cfg := baseCfg("/src/", "/dst/")
		// Progress notifications on the quiesced source carry hdrRev ==
		// watermark and are skipped without a Txn, so the armed injection
		// deterministically hits the role-flip Txn. (The interval must stay
		// short — the ticker is also what wakes consume to check the drain
		// gate.)

		want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 5)
		amb := &ambiguousTxnClient{Client: dst}
		r := startAgent(t, cfg, src, amb)
		waitTargetData(t, dst, cfg, 20*time.Second, want)
		waitSnap(t, r.agent, 20*time.Second, "Syncing",
			func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

		// The role-flip Txn commits but its response is lost: the retry must
		// recognize the stored Primary fence as its own write and complete
		// the drain — the pre-fix behavior wedged the cutover as a permanent
		// fence violation with the target already flipped.
		amb.arm.Store(true)
		r.agent.RequestDrain()
		require.NoError(t, r.waitErr(t, 30*time.Second), "a completed drain returns nil")

		snap := r.agent.Snapshot()
		assert.Equal(t, mirroragent.PhaseDrained, snap.Phase)
		assert.True(t, snap.CutoverReady)
		require.NotNil(t, snap.Cutover)
		assert.False(t, snap.Cutover.VerifiedTime.IsZero())
		f, _ := readFence(t, dst, cfg)
		assert.Equal(t, mirroragent.RolePrimary, f.Role)
	})
}

// claimRaceClient injects an OLD-epoch fence write between loadFence's read
// (which saw no key) and this generation's genesis claim Txn — the rolling-
// redeploy race where the outgoing generation lands its last checkpoint in
// the read/claim window.
type claimRaceClient struct {
	mirroragent.Client
	raw   *clientv3.Client
	key   string
	stale string
	armed atomic.Bool
}

func (c *claimRaceClient) Txn(ctx context.Context) clientv3.Txn {
	if c.armed.CompareAndSwap(true, false) {
		if _, err := c.raw.Put(ctx, c.key, c.stale); err != nil {
			panic("claimRaceClient: injecting stale fence: " + err.Error())
		}
	}
	return c.Client.Txn(ctx)
}

// TestGenesisClaimRaceTakesOver: a genesis fence claim that loses the race
// against an older generation's write must adopt the raced mod revision and
// retry the takeover — not fail the agent with a permanent FenceError.
func TestGenesisClaimRaceTakesOver(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.Epoch = 2

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 5)

	stale, err := mirroragent.FenceValue{
		LinkUID:   cfg.LinkUID,
		Epoch:     1,
		Role:      mirroragent.RoleMirror,
		Watermark: 1,
	}.Encode()
	require.NoError(t, err)

	race := &claimRaceClient{Client: dst, raw: dst, key: checkpointKey(cfg), stale: stale}
	race.armed.Store(true)
	r := startAgent(t, cfg, src, race)

	waitTargetData(t, dst, cfg, 20*time.Second, want)
	snap := waitSnap(t, r.agent, 20*time.Second, "Syncing after claim-race takeover",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	assert.EqualValues(t, 0, snap.ForcedResyncCount)
	f, _ := readFence(t, dst, cfg)
	assert.Equal(t, int64(2), f.Epoch, "the new generation must have taken the fence over")
	select {
	case runErr := <-r.done:
		t.Fatalf("Run returned after a claim race: %v", runErr)
	default:
	}
}

// replayLoopWatchClient drives the exact shape of the resync livelock the
// detector exists for: every genesis watch delivers ONE fabricated in-scope
// event (so the replay buffer is never empty) and then dies; every tail
// re-watch reports already-compacted. Heal() restores real watches.
type replayLoopWatchClient struct {
	mirroragent.Client
	raw       *clientv3.Client
	srcPrefix string
	healed    atomic.Bool

	mu    sync.Mutex
	calls int
}

func (c *replayLoopWatchClient) Watch(
	ctx context.Context, key string, opts ...clientv3.OpOption,
) clientv3.WatchChan {
	if c.healed.Load() {
		return c.Client.Watch(ctx, key, opts...)
	}
	c.mu.Lock()
	c.calls++
	n := c.calls
	c.mu.Unlock()
	ch := make(chan clientv3.WatchResponse, 1)
	if n%2 == 0 {
		// Tail re-watch: retention outran the watch.
		ch <- clientv3.WatchResponse{Canceled: true, CompactRevision: 3}
		close(ch)
		return ch
	}
	// Genesis watch: one fabricated in-scope event at an existing revision,
	// then channel death. The replay buffer keeps the event and genesis
	// applies it — the exact sequence that must NOT reset the livelock
	// detector mid-resync.
	head, err := c.raw.Get(context.Background(), c.srcPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		close(ch)
		return ch
	}
	ch <- clientv3.WatchResponse{
		Header: *head.Header,
		Events: []*clientv3.Event{{
			Type: clientv3.EventTypePut,
			Kv: &mvccpb.KeyValue{
				Key:         []byte(c.srcPrefix + "replay-marker"),
				Value:       []byte("replayed"),
				ModRevision: head.Header.Revision,
			},
		}},
	}
	close(ch)
	return ch
}

// TestResyncLoopLatchWithReplay: the livelock detector must latch even when
// every forced resync's replay buffer applies events — a churning source is
// the CANONICAL livelock trigger (retention < scan+apply time), and replay
// applies run inside the resync being counted, so they must never count as
// steady state.
func TestResyncLoopLatchWithReplay(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ResyncLoopThreshold = 2

	putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 8)
	loop := &replayLoopWatchClient{Client: src, raw: src, srcPrefix: cfg.SourcePrefix}
	r := startAgent(t, cfg, loop, dst)

	snap := waitSnap(t, r.agent, 30*time.Second, "livelock latch despite replay applies",
		func(s mirroragent.Snapshot) bool { return s.ResyncLoopDetected })
	assert.GreaterOrEqual(t, snap.ForcedResyncCount, int64(2))

	// Heal: real watches again. A write applied during a LIVE tail clears
	// the latch; writes swallowed by the healing resync's own scan do not,
	// so keep writing until the agent has settled into a live tail. (This
	// test pins the latch behavior; byte-exactness is covered elsewhere.)
	loop.healed.Store(true)
	deadline := time.Now().Add(30 * time.Second)
	for i := 0; r.agent.Snapshot().ResyncLoopDetected; i++ {
		require.True(t, time.Now().Before(deadline),
			"latch never cleared after healing despite live writes")
		_, err := src.Put(t.Context(), cfg.SourcePrefix+fmt.Sprintf("steady-%d", i), "ok")
		require.NoError(t, err)
		time.Sleep(150 * time.Millisecond)
	}
}

// TestDrainCompletesWithoutProgressTrust: on a source below the
// progress-notify trust floor (3.4.x < 3.4.25 / 3.5.x < 3.5.8) the drain
// target must be derived from the highest in-scope mod revision, not the
// cluster revision — out-of-prefix writes would otherwise park the drain in
// PhaseSyncing forever with no error (the watermark cannot advance past the
// last in-prefix event without trusted progress notifications).
func TestDrainCompletesWithoutProgressTrust(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 6)
	stubSrc := &versionStubClient{Client: src, version: "3.5.7"}
	r := startAgent(t, cfg, stubSrc, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	waitSnap(t, r.agent, 20*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	// Out-of-prefix writes push the cluster revision past every in-prefix
	// event — the quiesced-prefix drain must still terminate.
	ctx := t.Context()
	var clusterRev int64
	for i := range 5 {
		resp, err := src.Put(ctx, fmt.Sprintf("/elsewhere/%d", i), "x")
		require.NoError(t, err)
		clusterRev = resp.Header.Revision
	}

	r.agent.RequestDrain()
	require.NoError(t, r.waitErr(t, 30*time.Second), "the drain must terminate below the trust floor")
	snap := r.agent.Snapshot()
	assert.Equal(t, mirroragent.PhaseDrained, snap.Phase)
	assert.True(t, snap.CutoverReady)
	require.NotNil(t, snap.Cutover)
	assert.Less(t, snap.Cutover.DrainTargetRevision, clusterRev,
		"the drain target must be the in-scope high-water mark, not the cluster revision")
	f, _ := readFence(t, dst, cfg)
	assert.Equal(t, mirroragent.RolePrimary, f.Role)
}

// TestProgressNotifyNotTrustedBelowFloor: on a 3.5.7 source the watermark
// must NOT advance on an idle prefix (progress notifications are unreliable
// below 3.4.25/3.5.8 and may report revisions ahead of delivered events —
// trusting them checkpoints past undelivered data), while applies still
// advance it.
func TestProgressNotifyNotTrustedBelowFloor(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ProgressInterval = 100 * time.Millisecond

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 3)
	stubSrc := &versionStubClient{Client: src, version: "3.5.7"}
	r := startAgent(t, cfg, stubSrc, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	base := waitSnap(t, r.agent, 10*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	ctx := t.Context()
	var outRev int64
	for i := range 5 {
		resp, err := src.Put(ctx, fmt.Sprintf("/elsewhere/%d", i), "x")
		require.NoError(t, err)
		outRev = resp.Header.Revision
	}
	// Several progress intervals: an untrusted notification must not move
	// the watermark past the last applied in-prefix revision.
	time.Sleep(6 * cfg.ProgressInterval)
	mid := r.agent.Snapshot()
	assert.Less(t, mid.Watermark, outRev,
		"the watermark must not advance from untrusted progress notifications")
	assert.Equal(t, base.Watermark, mid.Watermark)

	// An applied in-prefix event still advances it.
	putResp, err := src.Put(ctx, "/src/applied", "yes")
	require.NoError(t, err)
	want["/dst/applied"] = "yes"
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	waitSnap(t, r.agent, 10*time.Second, "watermark advances on applies",
		func(s mirroragent.Snapshot) bool { return s.Watermark >= putResp.Header.Revision })
}

// flakyEndpointClient reports two endpoints, the first of which is
// blackholed for the maintenance Status probe.
type flakyEndpointClient struct {
	mirroragent.Client
	bad string
}

func (c *flakyEndpointClient) Endpoints() []string {
	return append([]string{c.bad}, c.Client.Endpoints()...)
}

func (c *flakyEndpointClient) Status(
	ctx context.Context, endpoint string,
) (*clientv3.StatusResponse, error) {
	if endpoint == c.bad {
		return nil, status.Error(codes.Unavailable, "blackholed endpoint")
	}
	return c.Client.Status(ctx, endpoint)
}

// TestProbeRotatesEndpoints: Status dials the named endpoint directly
// (bypassing the balancer), so the connect probe must rotate through the
// endpoint list — one dead member/NAT mapping must not wedge the agent in
// Connecting while healthy endpoints exist.
func TestProbeRotatesEndpoints(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 3)
	flaky := &flakyEndpointClient{Client: src, bad: "http://127.0.0.1:1"}
	r := startAgent(t, cfg, flaky, dst)
	waitSnap(t, r.agent, 20*time.Second, "Syncing despite a blackholed first endpoint",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	waitTargetData(t, dst, cfg, 10*time.Second, want)
}

// TestPruneRefusesForeignFence: a prune pass that encounters ANOTHER link's
// reserved fence key inside this link's destination prefix (overlapping
// destination prefixes — e.g. a second EtcdMirror at a nested prefix) must
// stop loudly with a PrefixConflictError instead of deleting the sibling's
// fence and data as orphans.
func TestPruneRefusesForeignFence(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.InitialSyncMode = mirroragent.InitialSyncOverwriteAndPrune

	putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 3)

	// A sibling mirror lives at the nested prefix /dst/sub/ with its own
	// fence and data.
	foreignFence, err := mirroragent.FenceValue{
		LinkUID:   "other-link",
		Epoch:     1,
		Role:      mirroragent.RoleMirror,
		Watermark: 7,
	}.Encode()
	require.NoError(t, err)
	ctx := t.Context()
	foreignKey := "/dst/sub/" + mirroragent.DefaultCheckpointKeySuffix
	_, err = dst.Put(ctx, foreignKey, foreignFence)
	require.NoError(t, err)
	_, err = dst.Put(ctx, "/dst/sub/data", "sibling-owned")
	require.NoError(t, err)

	r := startAgent(t, cfg, src, dst)
	runErr := r.waitErr(t, 30*time.Second)
	var pc *mirroragent.PrefixConflictError
	require.ErrorAs(t, runErr, &pc, "got: %v", runErr)
	assert.Equal(t, "other-link", pc.OwnerLinkUID)
	assert.NotContains(t, pc.Key, "sub", "the foreign key must be surfaced redacted")
	assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(runErr))
	assert.Equal(t, mirroragent.PhaseFailed, r.agent.Snapshot().Phase)

	// Nothing of the sibling's was destroyed.
	got, err := dst.Get(ctx, foreignKey)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1, "the sibling's fence key must survive")
	assert.Equal(t, foreignFence, string(got.Kvs[0].Value))
	data, err := dst.Get(ctx, "/dst/sub/data")
	require.NoError(t, err)
	require.Len(t, data.Kvs, 1, "the sibling's data must survive")
}

// TestShrinkOnTargetTxnLimit: the target is a foreign cluster whose
// --max-txn-ops the operator cannot inspect. A multi-revision flush set the
// target rejects must be re-committed at revision granularity (one shrink
// attempt) instead of failing the agent permanently — only an irreducible
// single revision is a true permanent oversize.
func TestShrinkOnTargetTxnLimit(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, func(c *embed.Config) { c.MaxTxnOps = 8 })
	cfg := baseCfg("/src/", "/dst/")
	cfg.InitialSyncMode = mirroragent.InitialSyncOverwrite
	cfg.MaxTxnOps = 64 // engine batches far past the target's limit of 8

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 20)

	r := startAgent(t, cfg, src, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	snap := waitSnap(t, r.agent, 20*time.Second, "Syncing after shrink",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	assert.EqualValues(t, 0, snap.ForcedResyncCount)
	assert.Empty(t, snap.LastError, "a successful shrink must clear the recorded error")

	// Live tail keeps working under the same limit.
	_, err := src.Put(t.Context(), "/src/after", "ok")
	require.NoError(t, err)
	want["/dst/after"] = "ok"
	waitTargetData(t, dst, cfg, 10*time.Second, want)
}

// txnFailingClient fails every Txn with a transient error while armed.
type txnFailingClient struct {
	mirroragent.Client
	failing atomic.Bool
}

func (c *txnFailingClient) Txn(ctx context.Context) clientv3.Txn {
	return &failableTxn{inner: c.Client.Txn(ctx), c: c}
}

type failableTxn struct {
	inner clientv3.Txn
	c     *txnFailingClient
}

func (t *failableTxn) If(cs ...clientv3.Cmp) clientv3.Txn   { t.inner = t.inner.If(cs...); return t }
func (t *failableTxn) Then(ops ...clientv3.Op) clientv3.Txn { t.inner = t.inner.Then(ops...); return t }
func (t *failableTxn) Else(ops ...clientv3.Op) clientv3.Txn { t.inner = t.inner.Else(ops...); return t }
func (t *failableTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.c.failing.Load() {
		return nil, status.Error(codes.Unavailable, "injected target stall")
	}
	return t.inner.Commit()
}

// watchCountingClient counts Watch calls.
type watchCountingClient struct {
	mirroragent.Client
	watches atomic.Int64
}

func (c *watchCountingClient) Watch(
	ctx context.Context, key string, opts ...clientv3.OpOption,
) clientv3.WatchChan {
	c.watches.Add(1)
	return c.Client.Watch(ctx, key, opts...)
}

// TestSustainedTargetBackoffCancelsWatch: while the target stalls, clientv3
// buffers undelivered source watch responses without bound — after a few
// backoff rounds the engine must cancel the source watch (bounding memory)
// and, once the target heals, resume from the checkpoint watermark on a
// fresh watch with nothing lost.
func TestSustainedTargetBackoffCancelsWatch(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 3)
	countingSrc := &watchCountingClient{Client: src}
	failingDst := &txnFailingClient{Client: dst}
	r := startAgent(t, cfg, countingSrc, failingDst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	waitSnap(t, r.agent, 10*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	before := countingSrc.watches.Load()

	// Stall the target and keep the source churning into the stalled apply.
	failingDst.failing.Store(true)
	ctx := t.Context()
	_, err := src.Put(ctx, "/src/stall-trigger", "x")
	require.NoError(t, err)
	want["/dst/stall-trigger"] = "x"
	waitSnap(t, r.agent, 20*time.Second, "Degraded during the stall",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseDegraded })
	// Ride out several backoff rounds (50ms initial, 500ms cap) so the
	// watch-cancel threshold (3 rounds) is comfortably crossed.
	time.Sleep(1 * time.Second)

	failingDst.failing.Store(false)
	for i := range 3 {
		k := fmt.Sprintf("post-stall-%d", i)
		_, perr := src.Put(ctx, cfg.SourcePrefix+k, "y")
		require.NoError(t, perr)
		want[cfg.TargetPrefix+k] = "y"
	}
	waitTargetData(t, dst, cfg, 30*time.Second, want)
	waitSnap(t, r.agent, 20*time.Second, "Syncing after the stall",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	assert.Greater(t, countingSrc.watches.Load(), before,
		"the stall must have cancelled the source watch and re-watched from the checkpoint")
	assert.EqualValues(t, 0, r.agent.Snapshot().ForcedResyncCount)
}
