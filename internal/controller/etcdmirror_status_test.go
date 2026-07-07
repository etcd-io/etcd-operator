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

package controller

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

func statusFixtureMirror() *ecv1alpha1.EtcdMirror {
	em := minimalMirror()
	em.Spec.Target.Prefix = "/mirrored/"
	return em
}

func TestInvariantsHeldComposition(t *testing.T) {
	now := time.Now()

	apply := func(em *ecv1alpha1.EtcdMirror, snap *mirroragent.Snapshot) {
		applySnapshotToStatus(em, snap, now, time.Time{})
	}

	t.Run("fresh equal no-drift is True", func(t *testing.T) {
		em := statusFixtureMirror()
		apply(em, healthySnapshot())
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionTrue, reasonInvariantsHold)
	})

	t.Run("count mismatch is False KeyCountMismatch", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.TargetKeyCount = snap.SourceKeyCount - 1
		apply(em, snap)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionFalse, reasonKeyCountMismatch)
	})

	t.Run("sustained lag is False LagExceeded", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.SourceRevision = snap.Watermark + lagRevisionThreshold + 1
		// lagSince predates the sustain window
		applySnapshotToStatus(em, snap, now, now.Add(-lagSustainDuration-time.Minute))
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionReplicationLagExceeded, metav1.ConditionTrue, reasonLagSustained)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionFalse, reasonLagExceeded)
	})

	t.Run("drift is False DriftDetected", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.LastReconcileDrift = &mirroragent.Drift{OrphanKeys: 2}
		apply(em, snap)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionFalse, reasonDriftDetected)
	})

	t.Run("stale pass is Unknown StaleVerificationPass", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.LastReconcileTime = now.Add(-2*mirroragent.DefaultReconcilePeriod - time.Minute)
		apply(em, snap)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionUnknown, reasonStaleVerificationPass)
	})

	t.Run("never ran is Unknown NoVerificationPass", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.LastReconcileTime = time.Time{}
		snap.LastReconcileDrift = nil
		apply(em, snap)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionUnknown, reasonNoVerificationPass)
	})

	t.Run("enabled interval shrinks the freshness window", func(t *testing.T) {
		em := statusFixtureMirror()
		em.Spec.Reconciliation = &ecv1alpha1.EtcdMirrorReconciliationSpec{
			Enabled:  true,
			Interval: &metav1.Duration{Duration: 10 * time.Minute},
		}
		snap := healthySnapshot()
		snap.LastReconcileTime = now.Add(-25 * time.Minute) // > 2x10m, << 2h
		apply(em, snap)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionUnknown, reasonStaleVerificationPass)
	})

	t.Run("disabled reconciliation uses the 1h default window", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.LastReconcileTime = now.Add(-90 * time.Minute) // < 2h
		apply(em, snap)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionTrue, reasonInvariantsHold)
	})
}

func TestConditionsFromSnapshot(t *testing.T) {
	now := time.Now()

	t.Run("healthy", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		lag := applySnapshotToStatus(em, snap, now, time.Time{})
		assert.True(t, lag.IsZero())
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionTrue, reasonWatermarkAdvancing)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionTargetThrottled, metav1.ConditionFalse, "")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionTargetQuotaExhausted, metav1.ConditionFalse, "")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionCompacted, metav1.ConditionFalse, "")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionResyncLoopDetected, metav1.ConditionFalse, "")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionDriftDetected, metav1.ConditionFalse, "")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionInitialSyncComplete, metav1.ConditionTrue, "")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation, metav1.ConditionFalse, "")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionLearnerEndpoint, metav1.ConditionFalse, "")
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseSyncing, em.Status.Phase)
		assert.Equal(t, fmt.Sprintf("%x", snap.SourceClusterID), em.Status.SourceClusterID)
		assert.Equal(t, fmt.Sprintf("%x", snap.TargetClusterID), em.Status.TargetClusterID)
		assert.Equal(t, int64(1200), em.Status.LastAppliedRevision)
		require.NotNil(t, em.Status.LastStatusSyncTime)
	})

	t.Run("throttled while degraded", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseDegraded
		snap.Throttled = true
		snap.LastError = "etcdserver: too many requests"
		applySnapshotToStatus(em, snap, now, time.Time{})
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionTargetThrottled, metav1.ConditionTrue, "")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonBackoff)
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseDegraded, em.Status.Phase)
	})

	t.Run("quota exhausted", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.QuotaExhausted = true
		applySnapshotToStatus(em, snap, now, time.Time{})
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionTargetQuotaExhausted, metav1.ConditionTrue, "")
	})

	t.Run("compacted forced resync", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseInitialSync
		snap.Compacted = true
		snap.LastResyncReason = mirroragent.ResyncReasonCompacted
		applySnapshotToStatus(em, snap, now, time.Time{})
		cond := requireCond(t, em, ecv1alpha1.EtcdMirrorConditionCompacted, metav1.ConditionTrue,
			ecv1alpha1.EtcdMirrorReasonForcedResync)
		assert.Contains(t, cond.Message, "Compacted")
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonInitialSync)
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseInitialSync, em.Status.Phase)
	})

	t.Run("resync loop", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.ResyncLoopDetected = true
		applySnapshotToStatus(em, snap, now, time.Time{})
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionResyncLoopDetected, metav1.ConditionTrue, "")
	})

	t.Run("drift with counts in message", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.LastReconcileDrift = &mirroragent.Drift{MissingKeys: 1, DivergentKeys: 2, OrphanKeys: 3}
		applySnapshotToStatus(em, snap, now, time.Time{})
		cond := requireCond(t, em, ecv1alpha1.EtcdMirrorConditionDriftDetected, metav1.ConditionTrue, "")
		assert.Contains(t, cond.Message, "1 missing")
		assert.Contains(t, cond.Message, "2 divergent")
		assert.Contains(t, cond.Message, "3 orphan")
	})

	t.Run("drift condition is sticky when the snapshot carries no diff outcome", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.LastReconcileDrift = &mirroragent.Drift{OrphanKeys: 1}
		applySnapshotToStatus(em, snap, now, time.Time{})
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionDriftDetected, metav1.ConditionTrue, "")

		snap2 := healthySnapshot()
		snap2.LastReconcileDrift = nil
		applySnapshotToStatus(em, snap2, now, time.Time{})
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionDriftDetected, metav1.ConditionTrue, "")
	})

	t.Run("learner endpoint names the side", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.SourceLearner = true
		applySnapshotToStatus(em, snap, now, time.Time{})
		cond := requireCond(t, em, ecv1alpha1.EtcdMirrorConditionLearnerEndpoint, metav1.ConditionTrue, "")
		assert.Contains(t, cond.Message, "source")
		assert.NotContains(t, cond.Message, "target")
	})

	t.Run("failed with typed reason", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseFailed
		snap.LastError = "source etcd 3.3.0 below the 3.4 floor"
		snap.LastErrorReason = "UnsupportedVersion"
		applySnapshotToStatus(em, snap, now, time.Time{})
		cond := requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse,
			ecv1alpha1.EtcdMirrorReasonUnsupportedVersion)
		assert.Equal(t, snap.LastError, cond.Message)
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseFailed, em.Status.Phase)
	})

	t.Run("failed untyped falls back to PermanentError", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseFailed
		snap.LastError = "some terminal error"
		applySnapshotToStatus(em, snap, now, time.Time{})
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonPermanentError)
	})

	t.Run("empty target violation embeds the etcdctl del command", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseFailed
		snap.LastError = "destination prefix not empty"
		snap.LastErrorReason = "EmptyTargetViolation"
		applySnapshotToStatus(em, snap, now, time.Time{})
		cond := requireCond(t, em, ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation, metav1.ConditionTrue, "")
		assert.Contains(t, cond.Message, `etcdctl del "/mirrored/" "/mirrored0"`)
		assert.Contains(t, cond.Message, "checkpoint key is excluded")
	})

	t.Run("prefix conflict runtime backstop", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseFailed
		snap.LastError = "fence key owned by another link"
		snap.LastErrorReason = "PrefixConflict"
		applySnapshotToStatus(em, snap, now, time.Time{})
		cond := requireCond(t, em, ecv1alpha1.EtcdMirrorConditionPrefixConflict, metav1.ConditionTrue, reasonConflict)
		assert.Equal(t, snap.LastError, cond.Message)
	})

	t.Run("drained maps to Syncing plus CutoverReady and DrainComplete", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseDrained
		snap.CutoverReady = true
		snap.LastProgressTime = now.Add(-time.Hour) // a drained agent stops progressing; still Available
		snap.Cutover = &mirroragent.CutoverStatus{
			DrainTargetRevision: 1234,
			DrainedRevision:     1234,
			VerifiedTime:        now,
			SourceKeyCount:      500,
			TargetKeyCount:      500,
			LeasedKeyCount:      3,
		}
		applySnapshotToStatus(em, snap, now, time.Time{})
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseSyncing, em.Status.Phase)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionTrue, reasonDrainComplete)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionCutoverReady, metav1.ConditionTrue, "")
		require.NotNil(t, em.Status.Cutover)
		assert.Equal(t, int64(1234), em.Status.Cutover.DrainTargetRevision)
		assert.Equal(t, int64(3), em.Status.Cutover.LeasedKeyCount)
	})

	t.Run("stalled progress is not Available", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.LastProgressTime = now.Add(-availableStaleThreshold - time.Second)
		applySnapshotToStatus(em, snap, now, time.Time{})
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonProgressStalled)
	})

	t.Run("lag below sustain window is False WithinSustainWindow and starts the ledger", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.SourceRevision = snap.Watermark + lagRevisionThreshold + 1
		lag := applySnapshotToStatus(em, snap, now, time.Time{})
		assert.Equal(t, now, lag)
		requireCond(t, em, ecv1alpha1.EtcdMirrorConditionReplicationLagExceeded,
			metav1.ConditionFalse, reasonWithinSustainWindow)
	})

	t.Run("key counts only copied when a pass ran", func(t *testing.T) {
		em := statusFixtureMirror()
		snap := healthySnapshot()
		snap.LastReconcileTime = time.Time{}
		snap.LastReconcileDrift = nil
		applySnapshotToStatus(em, snap, now, time.Time{})
		assert.Zero(t, em.Status.SourceKeyCount)
		assert.Zero(t, em.Status.TargetKeyCount)
	})
}
