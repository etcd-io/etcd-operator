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

// Integration tests for the mirror engine against two in-process embedded
// etcd servers (source + target). Each test exercises one contract from the
// Design-3 spec; intervals are shrunk via engine config knobs to keep the
// suite fast.
package mirroragent_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// TestColdStart covers scenario 1: a populated source prefix is fully
// scanned onto the target, the checkpoint/fence key carries the scan-base
// watermark, and the reserved key is excluded from the data invariants.
func TestColdStart(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 50)
	r0 := sourceRevision(t, src, cfg.SourcePrefix)

	r := startAgent(t, cfg, src, dst)
	snap := waitSnap(t, r.agent, 20*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	assert.EqualValues(t, 50, snap.InitialSyncKeyCount)
	assert.EqualValues(t, 50, snap.InitialSyncTotalKeyCount)
	assert.EqualValues(t, 0, snap.ForcedResyncCount)
	assert.Equal(t, r0, snap.Watermark)

	// Data invariant: exactly the 50 mirrored keys, reserved key excluded.
	waitTargetData(t, dst, cfg, 10*time.Second, want)
	raw, err := dst.Get(t.Context(), cfg.TargetPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	assert.EqualValues(t, 51, raw.Count,
		"raw range must hold the 50 data keys plus the reserved checkpoint key")

	f, _ := readFence(t, dst, cfg)
	assert.Equal(t, cfg.LinkUID, f.LinkUID)
	assert.Equal(t, cfg.Epoch, f.Epoch)
	assert.Equal(t, mirroragent.RoleMirror, f.Role)
	assert.Equal(t, r0, f.Watermark, "checkpoint watermark must be the scan base revision")
	assert.False(t, f.Scanning)
}

// TestLiveTail covers scenario 2: puts, deletes, and a multi-key source Txn
// arriving during the tail land exactly, and one source revision lands as
// ONE target Txn — every key of the source Txn plus the checkpoint write
// shares a single target mod revision (revision-aligned batching; a partial
// revision is unobservable at any point because the Txn is atomic).
func TestLiveTail(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	r := startAgent(t, cfg, src, dst)
	waitSnap(t, r.agent, 20*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	ctx := t.Context()
	_, err := src.Put(ctx, "/src/k1", "v1")
	require.NoError(t, err)
	waitTargetData(t, dst, cfg, 10*time.Second, map[string]string{"/dst/k1": "v1"})

	_, err = src.Delete(ctx, "/src/k1")
	require.NoError(t, err)
	waitTargetData(t, dst, cfg, 10*time.Second, map[string]string{})

	// One multi-key source Txn = one source revision.
	txnResp, err := src.Txn(ctx).Then(
		clientv3.OpPut("/src/t1", "a"),
		clientv3.OpPut("/src/t2", "b"),
		clientv3.OpPut("/src/t3", "c"),
	).Commit()
	require.NoError(t, err)
	srcTxnRev := txnResp.Header.Revision

	waitTargetData(t, dst, cfg, 10*time.Second,
		map[string]string{"/dst/t1": "a", "/dst/t2": "b", "/dst/t3": "c"})

	resp, err := dst.Get(ctx, "/dst/t1", clientv3.WithRange("/dst/t4"))
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 3)
	applyRev := resp.Kvs[0].ModRevision
	for _, kv := range resp.Kvs {
		assert.Equal(t, applyRev, kv.ModRevision,
			"all keys of one source revision must land in one target Txn")
	}
	f, fenceModRev := readFence(t, dst, cfg)
	assert.Equal(t, applyRev, fenceModRev,
		"the checkpoint must be written in the SAME Txn as the applied batch")
	assert.Equal(t, srcTxnRev, f.Watermark,
		"checkpoint watermark must be the applied source revision")
}

// TestMidScanCompaction covers scenario 3, the Design-3 headline: the source
// is compacted aggressively while a rate-limited scan is in flight, and the
// scan still converges with NO forced resync — the unpinned scan plus
// watch-replay-from-R0 eliminates mid-scan compaction as a failure class.
func TestMidScanCompaction(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.MaxOpsPerSecond = 150 // ~2s scan for 300 keys
	cfg.PageKeyLimit = 50
	cfg.MaxTxnOps = 20

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 300)
	r0 := sourceRevision(t, src, cfg.SourcePrefix)

	r := startAgent(t, cfg, src, dst)
	waitSnap(t, r.agent, 20*time.Second, "scan started",
		func(s mirroragent.Snapshot) bool { return s.InitialSyncKeyCount > 0 })

	// Hammer the source: live writes advance the revision and compaction
	// chases the head while the scan is still running.
	ctx := t.Context()
	for i := range 30 {
		k := fmt.Sprintf("live-%03d", i)
		_, err := src.Put(ctx, cfg.SourcePrefix+k, "live")
		require.NoError(t, err)
		want[cfg.TargetPrefix+k] = "live"
		cur, err := src.Get(ctx, cfg.SourcePrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		require.NoError(t, err)
		_, _ = src.Compact(ctx, cur.Header.Revision) // errors ("already compacted") are fine
		time.Sleep(50 * time.Millisecond)
	}

	// Prove the compactions bit: a revision-pinned read at the scan base —
	// what a mirror.Syncer-style pinned scan would issue — is now impossible.
	_, err := src.Get(ctx, cfg.SourcePrefix, clientv3.WithPrefix(), clientv3.WithRev(r0))
	require.ErrorIs(t, rpctypes.Error(err), rpctypes.ErrCompacted,
		"the scan base must actually be compacted for this test to mean anything")

	waitTargetData(t, dst, cfg, 30*time.Second, want)
	snap := waitSnap(t, r.agent, 20*time.Second, "Syncing after compacted scan",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	assert.EqualValues(t, 0, snap.ForcedResyncCount,
		"mid-scan compaction must not force a resync (class elimination)")
	assert.False(t, snap.Compacted)
	assert.False(t, snap.ResyncLoopDetected)
}

// TestRestartResume covers scenario 4: a stopped engine restarted with the
// same linkUID/epoch resumes from the checkpoint in the target — zero source
// range reads (no rescan), only the watch replays the missed writes.
func TestRestartResume(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 30)

	r1 := startAgent(t, cfg, src, dst)
	waitSnap(t, r1.agent, 20*time.Second, "first run Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
	waitTargetData(t, dst, cfg, 10*time.Second, want)
	err := r1.stop(t)
	require.ErrorIs(t, err, context.Canceled)

	// Writes while the engine is down.
	ctx := t.Context()
	for i := range 5 {
		k := fmt.Sprintf("extra-%d", i)
		_, perr := src.Put(ctx, cfg.SourcePrefix+k, "late")
		require.NoError(t, perr)
		want[cfg.TargetPrefix+k] = "late"
	}
	lastRev := sourceRevision(t, src, cfg.SourcePrefix)
	stopped, _ := readFence(t, dst, cfg)

	countingSrc := &countingClient{Client: src}
	recordingSrc := &watchRevRecordingClient{Client: countingSrc}
	r2 := startAgent(t, cfg, recordingSrc, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	snap := waitSnap(t, r2.agent, 10*time.Second, "resumed watermark",
		func(s mirroragent.Snapshot) bool { return s.Watermark >= lastRev })

	assert.EqualValues(t, 0, countingSrc.gets.Load(),
		"resume must not issue ANY source range read — no rescan")
	// Pin the resume revision itself: zero Gets only rules out RANGE
	// rescans; a watch opened at rev 1 would replay the whole history
	// (idempotently, so no data assertion can catch it) and re-transfer the
	// full prefix on every restart.
	revs := recordingSrc.recorded()
	require.NotEmpty(t, revs)
	assert.Equal(t, stopped.Watermark+1, revs[0],
		"the resumed watch must open at exactly checkpointWatermark+1")
	assert.EqualValues(t, 0, snap.InitialSyncKeyCount, "resume must not re-run the initial scan")
	assert.EqualValues(t, 0, snap.ForcedResyncCount)
	f, _ := readFence(t, dst, cfg)
	assert.GreaterOrEqual(t, f.Watermark, lastRev)
}

// TestFenceOverlap covers scenario 5: two engines share the reserved key; a
// newer epoch takes the fence over, the stale writer's next Txn fails its
// mod-revision compare and stops with the fencing error, and the target
// state is the new writer's alone.
func TestFenceOverlap(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfgA := baseCfg("/src/", "/dst/")

	putN(t, src, cfgA.SourcePrefix, cfgA.TargetPrefix, 5)

	rA := startAgent(t, cfgA, src, dst)
	waitSnap(t, rA.agent, 20*time.Second, "A Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	cfgB := cfgA
	cfgB.Epoch = 2
	rB := startAgent(t, cfgB, src, dst)
	waitSnap(t, rB.agent, 20*time.Second, "B Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	// Both engines watch this write; only epoch 2 may land it.
	_, err := src.Put(t.Context(), "/src/fence-probe", "who-wins")
	require.NoError(t, err)

	errA := rA.waitErr(t, 20*time.Second)
	var fe *mirroragent.FenceError
	require.ErrorAs(t, errA, &fe, "stale engine must stop with the fencing error, got: %v", errA)
	assert.Contains(t, fe.Detail, "epoch 2")
	assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(errA))
	assert.Equal(t, mirroragent.PhaseFailed, rA.agent.Snapshot().Phase)

	resp, err := dst.Get(t.Context(), "/dst/fence-probe")
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1, "the new writer must have applied the probe write")
	assert.Equal(t, "who-wins", string(resp.Kvs[0].Value))
	f, _ := readFence(t, dst, cfgB)
	assert.Equal(t, int64(2), f.Epoch, "target fence must be the new writer's alone")
	assert.Equal(t, mirroragent.PhaseSyncing, rB.agent.Snapshot().Phase)
}

// TestRoleFlipCutoverFence covers scenario 6: flipping the fence role to
// Primary (simulated cutover) makes the running engine's next apply fail
// loudly with the cutover-fence error class, and nothing lands after it.
func TestRoleFlipCutoverFence(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")

	putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 3)
	r := startAgent(t, cfg, src, dst)
	waitSnap(t, r.agent, 20*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	// Simulated cutover: rewrite the fence with role=Primary.
	f, _ := readFence(t, dst, cfg)
	f.Role = mirroragent.RolePrimary
	val, err := f.Encode()
	require.NoError(t, err)
	_, err = dst.Put(t.Context(), checkpointKey(cfg), val)
	require.NoError(t, err)

	_, err = src.Put(t.Context(), "/src/after-cutover", "straggler")
	require.NoError(t, err)

	runErr := r.waitErr(t, 20*time.Second)
	var fe *mirroragent.FenceError
	require.ErrorAs(t, runErr, &fe, "engine must stop with the fencing error, got: %v", runErr)
	assert.Contains(t, fe.Detail, "Primary")
	assert.Contains(t, fe.Detail, "cutover")
	assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(runErr))

	resp, err := dst.Get(t.Context(), "/dst/after-cutover")
	require.NoError(t, err)
	assert.Empty(t, resp.Kvs, "no straggler apply may land after the role flip")
}

// TestOversizedRevision covers scenario 7: a single source revision whose
// total bytes and op count both exceed the flush watermarks is applied as
// ONE oversized Txn with the checkpoint riding in it (held until it lands).
// The target's --max-txn-ops (embed MaxTxnOps) is bumped to make room.
func TestOversizedRevision(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, func(c *embed.Config) { c.MaxTxnOps = 64 })
	cfg := baseCfg("/src/", "/dst/")
	cfg.MaxTxnOps = 6        // 5 data op slots + checkpoint slot
	cfg.TxnFlushBytes = 2048 // the txn below is ~4.8KiB
	cfg.MaxOpsPerSecond = 0

	r := startAgent(t, cfg, src, dst)
	waitSnap(t, r.agent, 20*time.Second, "Syncing",
		func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })

	big := strings.Repeat("v", 600)
	ops := make([]clientv3.Op, 0, 8)
	want := make(map[string]string, 8)
	for i := range 8 {
		k := fmt.Sprintf("big-%d", i)
		ops = append(ops, clientv3.OpPut("/src/"+k, big))
		want["/dst/"+k] = big
	}
	txnResp, err := src.Txn(t.Context()).Then(ops...).Commit()
	require.NoError(t, err)

	waitTargetData(t, dst, cfg, 20*time.Second, want)
	resp, err := dst.Get(t.Context(), "/dst/big-", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 8)
	applyRev := resp.Kvs[0].ModRevision
	for _, kv := range resp.Kvs {
		assert.Equal(t, applyRev, kv.ModRevision,
			"an oversized source revision must be applied as ONE Txn, never split")
	}
	f, fenceModRev := readFence(t, dst, cfg)
	assert.Equal(t, applyRev, fenceModRev,
		"the checkpoint must be held and land in the same oversized Txn")
	assert.Equal(t, txnResp.Header.Revision, f.Watermark)
}

// TestInitialSyncModes covers scenario 8: the three initialSync modes plus
// excludePrefixes. Sub-scenarios share one server pair on disjoint prefixes.
func TestInitialSyncModes(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	ctx := t.Context()

	t.Run("RequireEmptyViolation", func(t *testing.T) {
		cfg := baseCfg("/m1/", "/d1/")
		_, err := dst.Put(ctx, "/d1/existing", "dirty")
		require.NoError(t, err)

		r := startAgent(t, cfg, src, dst)
		runErr := r.waitErr(t, 20*time.Second)
		var ev *mirroragent.EmptyTargetViolationError
		require.ErrorAs(t, runErr, &ev, "got: %v", runErr)
		assert.EqualValues(t, 1, ev.KeyCount)
		assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(runErr))
		assert.Equal(t, mirroragent.PhaseFailed, r.agent.Snapshot().Phase)
		// A RequireEmpty violation must write nothing, not even the fence.
		resp, err := dst.Get(ctx, checkpointKey(cfg))
		require.NoError(t, err)
		assert.Empty(t, resp.Kvs)
	})

	t.Run("OverwriteKeepsOrphans", func(t *testing.T) {
		cfg := baseCfg("/m2/", "/d2/")
		cfg.InitialSyncMode = mirroragent.InitialSyncOverwrite
		_, err := dst.Put(ctx, "/d2/stale", "old")
		require.NoError(t, err)
		_, err = dst.Put(ctx, "/d2/orphan", "keep-me")
		require.NoError(t, err)
		_, err = src.Put(ctx, "/m2/stale", "new")
		require.NoError(t, err)
		_, err = src.Put(ctx, "/m2/fresh", "1")
		require.NoError(t, err)

		r := startAgent(t, cfg, src, dst)
		waitSnap(t, r.agent, 20*time.Second, "Syncing",
			func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
		waitTargetData(t, dst, cfg, 10*time.Second, map[string]string{
			"/d2/stale":  "new",     // overwritten with source truth
			"/d2/fresh":  "1",       // mirrored
			"/d2/orphan": "keep-me", // Overwrite leaves orphans alone
		})
	})

	t.Run("OverwriteAndPruneRemovesOrphans", func(t *testing.T) {
		cfg := baseCfg("/m3/", "/d3/")
		cfg.InitialSyncMode = mirroragent.InitialSyncOverwriteAndPrune
		// Simulates failback onto a stale prefix: "zombie" stands for a key
		// deleted on the (new-primary) source after cutover — the prune pass
		// must remove it from the old copy.
		_, err := dst.Put(ctx, "/d3/stale", "old")
		require.NoError(t, err)
		_, err = dst.Put(ctx, "/d3/zombie", "deleted-post-cutover")
		require.NoError(t, err)
		_, err = src.Put(ctx, "/m3/stale", "new")
		require.NoError(t, err)
		_, err = src.Put(ctx, "/m3/fresh", "1")
		require.NoError(t, err)

		r := startAgent(t, cfg, src, dst)
		snap := waitSnap(t, r.agent, 20*time.Second, "Syncing",
			func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
		waitTargetData(t, dst, cfg, 10*time.Second, map[string]string{
			"/d3/stale": "new",
			"/d3/fresh": "1",
		})
		require.NotNil(t, snap.LastReconcileDrift)
		assert.EqualValues(t, 1, snap.LastReconcileDrift.OrphanKeys,
			"the post-cutover-deleted key must be counted and pruned as an orphan")
	})

	t.Run("ExcludePrefixes", func(t *testing.T) {
		cfg := baseCfg("/m4/", "/d4/")
		cfg.InitialSyncMode = mirroragent.InitialSyncOverwriteAndPrune
		cfg.ExcludePrefixes = []string{"/m4/skip/"}
		_, err := src.Put(ctx, "/m4/keep/a", "1")
		require.NoError(t, err)
		_, err = src.Put(ctx, "/m4/skip/b", "2")
		require.NoError(t, err)
		// Pre-existing target key under the excluded image: never pruned.
		_, err = dst.Put(ctx, "/d4/skip/old", "not-an-orphan")
		require.NoError(t, err)

		r := startAgent(t, cfg, src, dst)
		waitSnap(t, r.agent, 20*time.Second, "Syncing",
			func(s mirroragent.Snapshot) bool { return s.Phase == mirroragent.PhaseSyncing })
		waitTargetData(t, dst, cfg, 10*time.Second, map[string]string{
			"/d4/keep/a":   "1",
			"/d4/skip/old": "not-an-orphan",
		})
		// A live write under the excluded prefix must not arrive either.
		_, err = src.Put(ctx, "/m4/skip/c", "3")
		require.NoError(t, err)
		_, err = src.Put(ctx, "/m4/keep/d", "4")
		require.NoError(t, err)
		waitTargetData(t, dst, cfg, 10*time.Second, map[string]string{
			"/d4/keep/a":   "1",
			"/d4/keep/d":   "4",
			"/d4/skip/old": "not-an-orphan",
		})
	})
}

// TestDrain covers scenario 9: with a quiesced source and mode Drain, the
// engine drains, verifies, reports a stable drained revision, and flips the
// fence role to Primary.
func TestDrain(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.Mode = mirroragent.ModeDrain

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 12)
	r0 := sourceRevision(t, src, cfg.SourcePrefix)

	r := startAgent(t, cfg, src, dst)
	runErr := r.waitErr(t, 30*time.Second)
	require.NoError(t, runErr, "a completed drain returns nil")

	snap := r.agent.Snapshot()
	assert.Equal(t, mirroragent.PhaseDrained, snap.Phase)
	assert.True(t, snap.CutoverReady)
	require.NotNil(t, snap.Cutover)
	assert.Equal(t, r0, snap.Cutover.DrainTargetRevision)
	assert.GreaterOrEqual(t, snap.Cutover.DrainedRevision, snap.Cutover.DrainTargetRevision)
	assert.EqualValues(t, 12, snap.Cutover.SourceKeyCount)
	assert.EqualValues(t, 12, snap.Cutover.TargetKeyCount)
	assert.False(t, snap.Cutover.VerifiedTime.IsZero())

	waitTargetData(t, dst, cfg, 5*time.Second, want)
	f, _ := readFence(t, dst, cfg)
	assert.Equal(t, mirroragent.RolePrimary, f.Role, "drain completion must flip the fence to Primary")
	assert.Equal(t, snap.Cutover.DrainedRevision, f.Watermark)

	// Stability: the reported drained revision does not move.
	time.Sleep(300 * time.Millisecond)
	again := r.agent.Snapshot()
	assert.Equal(t, snap.Cutover.DrainedRevision, again.Cutover.DrainedRevision)
}

// TestTargetQuotaExhausted covers scenario 10: a target with an exhausted
// backend quota yields the typed TargetQuotaExhausted classification and the
// engine parks on the flat probe interval instead of hot-retrying.
func TestTargetQuotaExhausted(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, func(c *embed.Config) { c.QuotaBackendBytes = 4 * 1024 * 1024 })
	cfg := baseCfg("/src/", "/dst/")
	cfg.QuotaProbeInterval = 250 * time.Millisecond

	// Fill the target past its quota until NOSPACE trips.
	big := strings.Repeat("x", 1<<20)
	sawNoSpace := false
	for i := range 40 {
		_, err := dst.Put(t.Context(), fmt.Sprintf("/fill/%02d", i), big)
		if err != nil && errors.Is(rpctypes.Error(err), rpctypes.ErrNoSpace) {
			sawNoSpace = true
			break
		}
		require.NoError(t, err)
	}
	require.True(t, sawNoSpace, "target never hit NOSPACE while filling")

	putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 2)
	countingDst := &countingClient{Client: dst}
	r := startAgent(t, cfg, src, countingDst)

	snap := waitSnap(t, r.agent, 20*time.Second, "quota classification",
		func(s mirroragent.Snapshot) bool { return s.QuotaExhausted })
	assert.Equal(t, mirroragent.ClassQuota, snap.LastErrorClass,
		"NOSPACE must classify as the quota class, never throttling or transient")
	assert.Equal(t, mirroragent.PhaseDegraded, snap.Phase)

	// No hot retry loop: attempts are paced by QuotaProbeInterval.
	before := countingDst.txns.Load()
	time.Sleep(1200 * time.Millisecond)
	delta := countingDst.txns.Load() - before
	assert.LessOrEqual(t, delta, int64(8),
		"quota parking must probe on the flat interval, not spin (saw %d Txns in 1.2s)", delta)

	// Run must still be parked, not returned.
	select {
	case err := <-r.done:
		t.Fatalf("Run returned during quota park: %v", err)
	default:
	}
}
