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
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

const (
	// statusPollInterval drives the /statusz poll for active mirrors (the
	// Owns(Deployment) watch supplies the workload edge; status freshness is
	// requeue-driven).
	statusPollInterval = 15 * time.Second
	// slowRequeueInterval is for Paused/Failed/guard-blocked mirrors. The
	// agent lingers serving a terminal /statusz, so Failed mirrors keep being
	// polled, just slowly.
	slowRequeueInterval = time.Minute

	// availableStaleThreshold: the watermark must have advanced within this
	// window (3x the agent's progress-notification interval) for Available.
	availableStaleThreshold = 3 * mirroragent.DefaultProgressInterval

	// ReplicationLagExceeded's controller-internal constants (v1): the
	// watermark more than lagRevisionThreshold revisions behind the source,
	// continuously for lagSustainDuration.
	lagRevisionThreshold = int64(5000)
	lagSustainDuration   = 5 * time.Minute
)

// Condition/validation reasons owned by the controller (the API-contract
// reasons live in api/v1alpha1).
const (
	reasonAgentImageNotConfigured = "AgentImageNotConfigured"
	reasonServiceNotFound         = "ServiceNotFound"
	reasonSecretNotFound          = "SecretNotFound"
	reasonInvalidTLSSecret        = "InvalidTLSSecret"
	reasonInvalidAuthSecret       = "InvalidAuthSecret"
	reasonInvalidConfig           = "InvalidConfig"
	reasonAgentPodNotReady        = "AgentPodNotReady"
	reasonAgentStatusUnreachable  = "AgentStatusUnreachable"
	reasonSnapshotDecodeFailed    = "SnapshotDecodeFailed"
	reasonPaused                  = "Paused"
	reasonPermanentError          = "PermanentError"
	reasonDrainComplete           = "DrainComplete"
	reasonProgressStalled         = "ProgressStalled"
	reasonWatermarkAdvancing      = "WatermarkAdvancing"
	reasonConnecting              = "Connecting"
	reasonInitialSync             = "InitialSync"
	reasonBackoff                 = "Backoff"

	reasonNoConflict            = "NoConflict"
	reasonNotThrottled          = "NotThrottled"
	reasonQuotaOK               = "QuotaOK"
	reasonSteadyState           = "SteadyState"
	reasonNoResyncLoop          = "NoResyncLoop"
	reasonNoDrift               = "NoDrift"
	reasonInProgress            = "InProgress"
	reasonComplete              = "Complete"
	reasonNoViolation           = "NoViolation"
	reasonNotDrained            = "NotDrained"
	reasonNoLearner             = "NoLearner"
	reasonWithinThreshold       = "WithinThreshold"
	reasonWithinSustainWindow   = "WithinSustainWindow"
	reasonLagSustained          = "LagSustained"
	reasonNoVerificationPass    = "NoVerificationPass"
	reasonStaleVerificationPass = "StaleVerificationPass"
	reasonLagExceeded           = "LagExceeded"
	reasonKeyCountMismatch      = "KeyCountMismatch"
	reasonDriftDetected         = "DriftDetected"
	reasonInvariantsHold        = "InvariantsHold"
	reasonLearnerServing        = "LearnerServing"
	reasonViolation             = "Violation"
	reasonConflict              = "Conflict"
)

func setMirrorCondition(em *ecv1alpha1.EtcdMirror, condType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&em.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: em.Generation,
	})
}

func setBoolCondition(em *ecv1alpha1.EtcdMirror, condType string, val bool, trueReason, trueMsg, falseReason, falseMsg string) {
	if val {
		setMirrorCondition(em, condType, metav1.ConditionTrue, trueReason, trueMsg)
	} else {
		setMirrorCondition(em, condType, metav1.ConditionFalse, falseReason, falseMsg)
	}
}

func metaTimeOrNil(t time.Time) *metav1.Time {
	if t.IsZero() {
		return nil
	}
	mt := metav1.NewTime(t)
	return &mt
}

func hexClusterID(id uint64) string {
	if id == 0 {
		return ""
	}
	return fmt.Sprintf("%x", id)
}

// invariantsFreshnessWindow is the InvariantsHeld staleness window: 2x the
// configured periodic reconciliation interval, or 2x the agent's default
// period when the interval is unset or the periodic pass is disabled (counts
// then only refresh on mandatory passes, and the condition goes Unknown once
// the last one ages out).
func invariantsFreshnessWindow(em *ecv1alpha1.EtcdMirror) time.Duration {
	rec := em.Spec.Reconciliation
	if rec != nil && rec.Enabled && rec.Interval != nil && rec.Interval.Duration > 0 {
		return 2 * rec.Interval.Duration
	}
	return 2 * mirroragent.DefaultReconcilePeriod
}

// phaseFromSnapshot maps agent phases onto API phases. The library-only
// Drained phase maps to Syncing — the API has no Drained phase, a completed
// drain is not page-worthy, and CutoverReady carries the drain outcome.
func phaseFromSnapshot(p mirroragent.Phase) ecv1alpha1.EtcdMirrorPhase {
	if p == mirroragent.PhaseDrained {
		return ecv1alpha1.EtcdMirrorPhaseSyncing
	}
	return ecv1alpha1.EtcdMirrorPhase(p)
}

// applySnapshotToStatus maps one successfully polled snapshot onto em's
// status fields and conditions. lagSince is the in-memory lag ledger value
// for this CR (zero = not currently over the threshold); the updated value is
// returned for the caller to persist in the ledger. Pure except for em
// mutation — the envtest seam's whole point is that this never dials
// anything.
func applySnapshotToStatus(
	em *ecv1alpha1.EtcdMirror, snap *mirroragent.Snapshot, now time.Time, lagSince time.Time,
) time.Time {
	st := &em.Status

	st.ObservedGeneration = em.Generation
	st.Phase = phaseFromSnapshot(snap.Phase)
	st.LastAppliedRevision = snap.Watermark
	st.SourceRevision = snap.SourceRevision
	st.SourceClusterID = hexClusterID(snap.SourceClusterID)
	st.TargetClusterID = hexClusterID(snap.TargetClusterID)
	st.SourceVersion = snap.SourceVersion
	st.TargetVersion = snap.TargetVersion
	st.InitialSyncKeyCount = snap.InitialSyncKeyCount
	st.InitialSyncTotalKeyCount = snap.InitialSyncTotalKeyCount
	st.InitialSyncStartTime = metaTimeOrNil(snap.InitialSyncStartTime)
	st.InitialSyncCompletionTime = metaTimeOrNil(snap.InitialSyncCompletionTime)
	st.LeaseBackedKeyCount = snap.LeaseBackedKeyCount
	st.ForcedResyncCount = int32(snap.ForcedResyncCount)
	st.ScanRestartCount = snap.ScanRestartCount
	st.LastReconciliationTime = metaTimeOrNil(snap.LastReconcileTime)
	if snap.LastReconcileDrift != nil {
		st.LastReconciliationDrift = &ecv1alpha1.EtcdMirrorDriftInfo{
			MissingKeys:   snap.LastReconcileDrift.MissingKeys,
			DivergentKeys: snap.LastReconcileDrift.DivergentKeys,
			OrphanKeys:    snap.LastReconcileDrift.OrphanKeys,
			Repaired:      snap.LastReconcileDrift.Repaired,
		}
	}
	if !snap.LastReconcileTime.IsZero() {
		st.SourceKeyCount = snap.SourceKeyCount
		st.TargetKeyCount = snap.TargetKeyCount
	}
	st.LastProgressTime = metaTimeOrNil(snap.LastProgressTime)
	if snap.Cutover != nil {
		st.Cutover = &ecv1alpha1.EtcdMirrorCutoverStatus{
			DrainTargetRevision: snap.Cutover.DrainTargetRevision,
			DrainedRevision:     snap.Cutover.DrainedRevision,
			VerifiedTime:        metaTimeOrNil(snap.Cutover.VerifiedTime),
			SourceKeyCount:      snap.Cutover.SourceKeyCount,
			TargetKeyCount:      snap.Cutover.TargetKeyCount,
			LeasedKeyCount:      snap.Cutover.LeasedKeyCount,
		}
	}
	st.LastStatusSyncTime = metaTimeOrNil(now)

	lagSince = setLagCondition(em, snap, now, lagSince)
	setAvailableCondition(em, snap, now)
	setSnapshotFlagConditions(em, snap)
	setInvariantsHeldCondition(em, snap, now)
	return lagSince
}

// setLagCondition maintains ReplicationLagExceeded from the raw snapshot
// terms plus the caller's lag ledger: over the revision threshold
// continuously for the sustain window. Both terms come from the same
// watch/progress machinery in the agent.
func setLagCondition(
	em *ecv1alpha1.EtcdMirror, snap *mirroragent.Snapshot, now time.Time, lagSince time.Time,
) time.Time {
	gap := snap.SourceRevision - snap.Watermark
	if gap <= lagRevisionThreshold {
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionReplicationLagExceeded,
			metav1.ConditionFalse, reasonWithinThreshold,
			fmt.Sprintf("watermark is %d revisions behind the source (threshold %d)", gap, lagRevisionThreshold))
		return time.Time{}
	}
	if lagSince.IsZero() {
		lagSince = now
	}
	if now.Sub(lagSince) >= lagSustainDuration {
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionReplicationLagExceeded,
			metav1.ConditionTrue, reasonLagSustained,
			fmt.Sprintf("watermark has been more than %d revisions behind the source for %s (current gap %d); "+
				"note SourceRevision advances on out-of-prefix writes, so the gap overstates lag for prefix-scoped mirrors",
				lagRevisionThreshold, now.Sub(lagSince).Round(time.Second), gap))
	} else {
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionReplicationLagExceeded,
			metav1.ConditionFalse, reasonWithinSustainWindow,
			fmt.Sprintf("watermark gap %d exceeds threshold %d but not yet for the %s sustain window",
				gap, lagRevisionThreshold, lagSustainDuration))
	}
	return lagSince
}

func setAvailableCondition(em *ecv1alpha1.EtcdMirror, snap *mirroragent.Snapshot, now time.Time) {
	const cond = ecv1alpha1.EtcdMirrorConditionAvailable
	switch snap.Phase {
	case mirroragent.PhaseDrained:
		setMirrorCondition(em, cond, metav1.ConditionTrue, reasonDrainComplete,
			"drain completed and verified; the fence role is Primary")
	case mirroragent.PhaseSyncing:
		if snap.LastProgressTime.IsZero() || now.Sub(snap.LastProgressTime) > availableStaleThreshold {
			setMirrorCondition(em, cond, metav1.ConditionFalse, reasonProgressStalled,
				fmt.Sprintf("watermark has not advanced within %s", availableStaleThreshold))
		} else {
			setMirrorCondition(em, cond, metav1.ConditionTrue, reasonWatermarkAdvancing,
				"agent is syncing and the checkpoint watermark is advancing")
		}
	case mirroragent.PhaseConnecting:
		setMirrorCondition(em, cond, metav1.ConditionFalse, reasonConnecting,
			"agent is establishing client connections to both sides")
	case mirroragent.PhaseInitialSync:
		setMirrorCondition(em, cond, metav1.ConditionFalse, reasonInitialSync,
			fmt.Sprintf("genesis scan in progress: %d/%d keys",
				snap.InitialSyncKeyCount, snap.InitialSyncTotalKeyCount))
	case mirroragent.PhaseDegraded:
		setMirrorCondition(em, cond, metav1.ConditionFalse, reasonBackoff,
			"agent is in a retry/backoff loop: "+snap.LastError)
	case mirroragent.PhaseFailed:
		reason := snap.LastErrorReason
		if reason == "" {
			reason = reasonPermanentError
		}
		setMirrorCondition(em, cond, metav1.ConditionFalse, reason, snap.LastError)
	default:
		setMirrorCondition(em, cond, metav1.ConditionUnknown, "UnknownPhase",
			fmt.Sprintf("agent reported unknown phase %q", snap.Phase))
	}
}

// setSnapshotFlagConditions maps the snapshot's condition-shaped flags.
func setSnapshotFlagConditions(em *ecv1alpha1.EtcdMirror, snap *mirroragent.Snapshot) {
	setBoolCondition(em, ecv1alpha1.EtcdMirrorConditionTargetThrottled, snap.Throttled,
		"Throttled", "target is rejecting the agent's write rate; the agent is backing off "+
			"(raise the target's rate limits or lower sync.maxOpsPerSecond)",
		reasonNotThrottled, "target is accepting writes")
	setBoolCondition(em, ecv1alpha1.EtcdMirrorConditionTargetQuotaExhausted, snap.QuotaExhausted,
		"QuotaExhausted", "target write failed with NOSPACE; compact/defrag/disarm the target "+
			"(backoff cannot heal a full quota — the agent stopped writing)",
		reasonQuotaOK, "target storage quota has headroom")

	if snap.Compacted {
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionCompacted, metav1.ConditionTrue,
			ecv1alpha1.EtcdMirrorReasonForcedResync,
			fmt.Sprintf("forced resync in flight (trigger: %s)", snap.LastResyncReason))
	} else {
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionCompacted, metav1.ConditionFalse,
			reasonSteadyState, "no forced resync in flight")
	}

	setBoolCondition(em, ecv1alpha1.EtcdMirrorConditionResyncLoopDetected, snap.ResyncLoopDetected,
		"ResyncLoop", "consecutive forced resyncs never reached steady state — the livelock signature of "+
			"source compaction retention < scan+drain time; raise retention, raise maxOpsPerSecond, or shrink the prefix",
		reasonNoResyncLoop, "no resync livelock detected")

	// DriftDetected is sticky: only recomputed when the agent reports a full
	// diff outcome (count-only verifications never overwrite it).
	if snap.LastReconcileDrift != nil {
		d := snap.LastReconcileDrift
		total := d.MissingKeys + d.DivergentKeys + d.OrphanKeys
		setBoolCondition(em, ecv1alpha1.EtcdMirrorConditionDriftDetected, total > 0,
			reasonDriftDetected, fmt.Sprintf("last reconciliation pass found %d missing, %d divergent, %d orphan keys (repaired: %t)",
				d.MissingKeys, d.DivergentKeys, d.OrphanKeys, d.Repaired),
			reasonNoDrift, "last reconciliation pass found no drift")
	}

	setBoolCondition(em, ecv1alpha1.EtcdMirrorConditionInitialSyncComplete,
		!snap.InitialSyncCompletionTime.IsZero(),
		reasonComplete, "genesis scan completed against the checkpointed cluster identities",
		reasonInProgress, "genesis scan has not completed")

	violation := snap.Phase == mirroragent.PhaseFailed && snap.LastErrorReason == "EmptyTargetViolation"
	setBoolCondition(em, ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation, violation,
		reasonViolation, fmt.Sprintf(
			"destination prefix was non-empty at genesis with initialSync.mode RequireEmpty. "+
				"To clear the range, run: %s (the reserved checkpoint key is excluded by exact match and recreated by the agent). %s",
			etcdctlDelCommand(effectiveDestPrefix(em)), snap.LastError),
		reasonNoViolation, "destination prefix passed the RequireEmpty check")

	setBoolCondition(em, ecv1alpha1.EtcdMirrorConditionCutoverReady, snap.CutoverReady,
		"CutoverReady", "watermark reached the drain target revision and verification passed; "+
			"the fence role is Primary — safe to repoint clients",
		reasonNotDrained, "cutover gate not reached (requires spec.mode Drain, a reached drain target revision, and a verification pass)")

	if snap.SourceLearner || snap.TargetLearner {
		var sides []string
		if snap.SourceLearner {
			sides = append(sides, sideSourceName)
		}
		if snap.TargetLearner {
			sides = append(sides, sideTargetName)
		}
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionLearnerEndpoint, metav1.ConditionTrue,
			reasonLearnerServing,
			fmt.Sprintf("the maintenance Status() probe reported a learner on: %s (non-blocking, but a learner pick can serve stale reads)",
				strings.Join(sides, ", ")))
	} else {
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionLearnerEndpoint, metav1.ConditionFalse,
			reasonNoLearner, "no probed endpoint reported IsLearner")
	}

	// Runtime PrefixConflict backstop: the agent refuses (permanently) to
	// touch a fence key owned by a different link even when the controller's
	// spec-derived guard missed the overlap.
	if snap.Phase == mirroragent.PhaseFailed && snap.LastErrorReason == "PrefixConflict" {
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionPrefixConflict, metav1.ConditionTrue,
			reasonConflict, snap.LastError)
	}
}

// setInvariantsHeldCondition composes the verification verdict: lag ok +
// per-side key counts equal + no drift, from a fresh pass.
func setInvariantsHeldCondition(em *ecv1alpha1.EtcdMirror, snap *mirroragent.Snapshot, now time.Time) {
	const cond = ecv1alpha1.EtcdMirrorConditionInvariantsHeld
	if snap.LastReconcileTime.IsZero() {
		setMirrorCondition(em, cond, metav1.ConditionUnknown, reasonNoVerificationPass,
			"no reconciliation/verification pass has run yet (enable spec.reconciliation for a continuous signal)")
		return
	}
	window := invariantsFreshnessWindow(em)
	if now.Sub(snap.LastReconcileTime) > window {
		setMirrorCondition(em, cond, metav1.ConditionUnknown, reasonStaleVerificationPass,
			fmt.Sprintf("last verification pass is older than %s (enable spec.reconciliation for a continuous signal)", window))
		return
	}
	switch {
	case meta.IsStatusConditionTrue(em.Status.Conditions, ecv1alpha1.EtcdMirrorConditionReplicationLagExceeded):
		setMirrorCondition(em, cond, metav1.ConditionFalse, reasonLagExceeded,
			"ReplicationLagExceeded is True")
	case snap.SourceKeyCount != snap.TargetKeyCount:
		setMirrorCondition(em, cond, metav1.ConditionFalse, reasonKeyCountMismatch,
			fmt.Sprintf("per-side key counts differ: source %d, target %d", snap.SourceKeyCount, snap.TargetKeyCount))
	case meta.IsStatusConditionTrue(em.Status.Conditions, ecv1alpha1.EtcdMirrorConditionDriftDetected):
		setMirrorCondition(em, cond, metav1.ConditionFalse, reasonDriftDetected,
			"DriftDetected is True")
	default:
		setMirrorCondition(em, cond, metav1.ConditionTrue, reasonInvariantsHold,
			fmt.Sprintf("lag within threshold, per-side key counts equal (%d), no drift; pass fresh within %s",
				snap.SourceKeyCount, invariantsFreshnessWindow(em)))
	}
}
