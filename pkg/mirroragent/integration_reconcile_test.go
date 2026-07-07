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

package mirroragent_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestPeriodicReconcileRepairsDrift: target damage invisible to the source
// watch — a divergent value and a deleted mirrored key — is found and
// repaired byte-exactly by the periodic pass, and a later pass records
// matching per-side key counts.
func TestPeriodicReconcileRepairsDrift(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ReconcileInterval = 500 * time.Millisecond

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 20)

	r := startAgent(t, cfg, src, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	// Land in the quiet window right after a pass completes, so the next
	// pass observes both damages together.
	waitSnap(t, r.agent, 20*time.Second, "first periodic pass",
		func(s mirroragent.Snapshot) bool { return !s.LastReconcileTime.IsZero() })

	ctx := t.Context()
	// One commit revision for both damages: drift is replaced wholesale each
	// pass, so damages split across a pass boundary would never appear in
	// the same drift and the conjunction below would time out.
	_, err := dst.Txn(ctx).Then(
		clientv3.OpPut("/dst/key-0000", "tampered"),
		clientv3.OpDelete("/dst/key-0001"),
	).Commit()
	require.NoError(t, err)

	snap := waitSnap(t, r.agent, 20*time.Second, "drift observed by the periodic pass",
		func(s mirroragent.Snapshot) bool {
			return s.LastReconcileDrift != nil &&
				s.LastReconcileDrift.DivergentKeys >= 1 &&
				s.LastReconcileDrift.MissingKeys >= 1
		})
	assert.True(t, snap.LastReconcileDrift.Repaired, "the periodic pass must repair, not just report")

	waitTargetData(t, dst, cfg, 10*time.Second, want)
	waitSnap(t, r.agent, 20*time.Second, "matching counts after repair",
		func(s mirroragent.Snapshot) bool {
			return s.SourceKeyCount == 20 && s.TargetKeyCount == 20
		})
}

// startConvergedWithOrphan converges a small mirror with the periodic pass
// enabled at the given orphan policy, then plants an orphan key directly
// under the destination prefix (no source counterpart).
func startConvergedWithOrphan(t *testing.T, deleteOrphans bool) (*agentRun, *clientv3.Client) {
	t.Helper()
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ReconcileInterval = 300 * time.Millisecond
	cfg.ReconcileDeleteOrphans = deleteOrphans

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 5)
	r := startAgent(t, cfg, src, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)

	_, err := dst.Put(t.Context(), orphanKey, "planted")
	require.NoError(t, err)
	return r, dst
}

const orphanKey = "/dst/zz-orphan"

// TestPeriodicReconcileOrphanPolicy: ReconcileDeleteOrphans gates ONLY the
// deletion of target keys with no source counterpart — a report-only pass
// still repairs and keeps reporting the orphan pass after pass.
func TestPeriodicReconcileOrphanPolicy(t *testing.T) {
	t.Run("report only", func(t *testing.T) {
		r, dst := startConvergedWithOrphan(t, false)
		first := waitSnap(t, r.agent, 20*time.Second, "orphan reported",
			func(s mirroragent.Snapshot) bool {
				return s.LastReconcileDrift != nil && s.LastReconcileDrift.OrphanKeys >= 1
			})
		// A second full pass: still reported, still not deleted.
		waitSnap(t, r.agent, 20*time.Second, "a later pass reporting the orphan",
			func(s mirroragent.Snapshot) bool {
				return s.LastReconcileTime.After(first.LastReconcileTime) &&
					s.LastReconcileDrift != nil && s.LastReconcileDrift.OrphanKeys >= 1
			})
		got, err := dst.Get(t.Context(), orphanKey)
		require.NoError(t, err)
		require.Len(t, got.Kvs, 1, "deleteOrphans=false must never delete the orphan")
	})

	t.Run("delete", func(t *testing.T) {
		r, dst := startConvergedWithOrphan(t, true)
		snap := waitSnap(t, r.agent, 20*time.Second, "orphan reported and deleted",
			func(s mirroragent.Snapshot) bool {
				return s.LastReconcileDrift != nil && s.LastReconcileDrift.OrphanKeys >= 1
			})
		assert.True(t, snap.LastReconcileDrift.Repaired)
		got, err := dst.Get(t.Context(), orphanKey)
		require.NoError(t, err)
		assert.Empty(t, got.Kvs, "deleteOrphans=true must delete the orphan")
	})
}

// TestPeriodicReconcileAfterGenesisOnly: with a RequireEmpty cold start
// (no mandatory sweep) the ONLY producer of LastReconcileTime is the
// periodic pass, and even an interval much shorter than the throttled
// genesis scan never fires during it — the first pass completes after
// InitialSyncCompletionTime.
func TestPeriodicReconcileAfterGenesisOnly(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ReconcileInterval = 50 * time.Millisecond
	cfg.MaxOpsPerSecond = 100 // stretch the 300-key genesis scan to ~3s

	putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 300)

	r := startAgent(t, cfg, src, dst)
	snap := waitSnap(t, r.agent, 60*time.Second, "first periodic pass",
		func(s mirroragent.Snapshot) bool { return !s.LastReconcileTime.IsZero() })
	require.False(t, snap.InitialSyncCompletionTime.IsZero())
	assert.False(t, snap.LastReconcileTime.Before(snap.InitialSyncCompletionTime),
		"no periodic pass may run before the genesis scan completed")
	assert.EqualValues(t, 300, snap.SourceKeyCount)
	assert.EqualValues(t, 300, snap.TargetKeyCount)
}

// TestPeriodicReconcilePacedRepairRecovers: a periodic pass repairing more
// than a few seconds' worth of MaxOpsPerSecond pacing cancels the live
// source watch mid-pass (the bound on clientv3's unbounded per-watcher
// buffer while the pass leaves the watch unread); the tail re-watches from
// the checkpoint watermark, so the repair still converges and live
// replication resumes afterwards.
func TestPeriodicReconcilePacedRepairRecovers(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ReconcileInterval = 300 * time.Millisecond
	cfg.MaxOpsPerSecond = 30 // 100 repairs > 3s of pacing: mid-pass cancel

	const n = 100
	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, n)
	r := startAgent(t, cfg, src, dst)
	waitTargetData(t, dst, cfg, 30*time.Second, want)

	// One commit revision so a single pass observes all n divergent keys and
	// crosses the paced-repair cancel threshold within that pass.
	ctx := t.Context()
	ops := make([]clientv3.Op, 0, n)
	for i := range n {
		ops = append(ops, clientv3.OpPut(fmt.Sprintf("/dst/key-%04d", i), "tampered"))
	}
	_, err := dst.Txn(ctx).Then(ops...).Commit()
	require.NoError(t, err)

	waitTargetData(t, dst, cfg, 30*time.Second, want)
	// The watch the pass cancelled was re-established from the watermark: a
	// fresh source write still replicates.
	_, err = src.Put(ctx, cfg.SourcePrefix+"after-repair", "live")
	require.NoError(t, err)
	want[cfg.TargetPrefix+"after-repair"] = "live"
	waitTargetData(t, dst, cfg, 20*time.Second, want)
	assert.NotEqual(t, mirroragent.PhaseFailed, r.agent.Snapshot().Phase)
}

// TestPeriodicReconcileFenceAbort: a fence taken over by a newer agent
// generation aborts the periodic pass's first repair write permanently —
// the pass rides the same fenced Txn path as every other write, so the
// stale generation cannot "repair" anything after losing the fence.
func TestPeriodicReconcileFenceAbort(t *testing.T) {
	src := startEtcd(t, nil)
	dst := startEtcd(t, nil)
	cfg := baseCfg("/src/", "/dst/")
	cfg.ReconcileInterval = 300 * time.Millisecond
	// Keep the progress driver quiet so the periodic repair is the only
	// active write path on the idle source.
	cfg.ProgressInterval = 30 * time.Second

	want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 5)
	r := startAgent(t, cfg, src, dst)
	waitTargetData(t, dst, cfg, 20*time.Second, want)

	// A newer generation of this link takes the fence over externally...
	cur, _ := readFence(t, dst, cfg)
	takeover := cur
	takeover.Epoch = cur.Epoch + 1
	val, err := takeover.Encode()
	require.NoError(t, err)
	ctx := t.Context()
	_, err = dst.Put(ctx, checkpointKey(cfg), val)
	require.NoError(t, err)
	// ...then a divergent target key forces the next pass to attempt a
	// fenced repair write.
	_, err = dst.Put(ctx, "/dst/key-0000", "tampered")
	require.NoError(t, err)

	runErr := r.waitErr(t, 30*time.Second)
	var fe *mirroragent.FenceError
	require.ErrorAs(t, runErr, &fe, "got: %v", runErr)
	assert.Equal(t, mirroragent.ClassPermanent, mirroragent.Classify(runErr))
	assert.Equal(t, mirroragent.PhaseFailed, r.agent.Snapshot().Phase)

	// The aborted pass wrote nothing after the fence moved.
	got, err := dst.Get(ctx, "/dst/key-0000")
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	assert.Equal(t, "tampered", string(got.Kvs[0].Value))
}

// TestMandatoryPassesPopulateCountsWithoutPeriodic: the always-on contract —
// with the periodic pass disabled (ReconcileInterval 0, the engine image of
// spec.reconciliation absent), the mandatory passes still populate the
// per-side key counts and stamp the pass-completion time.
func TestMandatoryPassesPopulateCountsWithoutPeriodic(t *testing.T) {
	t.Run("overwrite and prune genesis", func(t *testing.T) {
		src := startEtcd(t, nil)
		dst := startEtcd(t, nil)
		cfg := baseCfg("/src/", "/dst/")
		cfg.InitialSyncMode = mirroragent.InitialSyncOverwriteAndPrune

		// Pre-populated target: a stale value at a mirrored key.
		_, err := dst.Put(t.Context(), "/dst/key-0000", "stale")
		require.NoError(t, err)
		want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 10)

		r := startAgent(t, cfg, src, dst)
		waitTargetData(t, dst, cfg, 20*time.Second, want)
		snap := waitSnap(t, r.agent, 20*time.Second, "mandatory sweep counts",
			func(s mirroragent.Snapshot) bool { return !s.LastReconcileTime.IsZero() })
		assert.EqualValues(t, 10, snap.SourceKeyCount)
		assert.EqualValues(t, 10, snap.TargetKeyCount)
		assert.NotNil(t, snap.LastReconcileDrift, "the mandatory sweep is a full diff")
	})

	t.Run("drain verification", func(t *testing.T) {
		src := startEtcd(t, nil)
		dst := startEtcd(t, nil)
		cfg := baseCfg("/src/", "/dst/")

		want := putN(t, src, cfg.SourcePrefix, cfg.TargetPrefix, 8)
		r := startAgent(t, cfg, src, dst)
		waitTargetData(t, dst, cfg, 20*time.Second, want)

		r.agent.RequestDrain()
		require.NoError(t, r.waitErr(t, 30*time.Second), "a completed drain returns nil")
		snap := r.agent.Snapshot()
		assert.Equal(t, mirroragent.PhaseDrained, snap.Phase)
		require.NotNil(t, snap.Cutover)
		assert.EqualValues(t, 8, snap.Cutover.SourceKeyCount)
		assert.EqualValues(t, 8, snap.Cutover.TargetKeyCount)
		assert.EqualValues(t, 8, snap.SourceKeyCount)
		assert.EqualValues(t, 8, snap.TargetKeyCount)
		assert.False(t, snap.LastReconcileTime.IsZero(),
			"the count-only drain verification must stamp the pass time")
		assert.Nil(t, snap.LastReconcileDrift,
			"a count-only verification must not fabricate a drift")
	})
}
