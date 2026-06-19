/*
Copyright 2025.

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

// Quorum-loss disaster recovery.
//
// An etcd cluster makes progress only while a majority (quorum) of its voting
// members are reachable and can elect a leader. If a majority is permanently
// lost — e.g. two of three members' pods AND their data volumes are destroyed —
// the cluster can never elect a leader again on its own. etcd's documented
// disaster-recovery procedure for this is:
//
//  1. Take the data directory of a SURVIVING member.
//  2. Restart that one member with `--force-new-cluster`, which rewrites its
//     raft membership to contain only itself, producing a healthy
//     single-member cluster that retains all committed key/value data.
//  3. Re-add the other members one at a time.
//
// This file implements that procedure as an idempotent controller state machine
// that maps cleanly onto the operator's StatefulSet model:
//
//   - The survivor is always pod ordinal 0, whose PVC the operator preserves.
//   - "Restart pod-0 with --force-new-cluster" is expressed by patching the
//     StatefulSet down to a single replica and injecting the
//     `--force-new-cluster` flag (plus an existing-state config) onto the etcd
//     container, then rolling pod-0.
//   - Once pod-0 is a healthy single-member cluster, the flag is removed and the
//     EXISTING reconcile loop's scale-out path (learner add + promote) rebuilds
//     the cluster back up to Spec.Size. We deliberately reuse that path rather
//     than duplicating membership logic here.
//
// Detection is guarded so it ONLY fires on true quorum loss:
//
//   - A MAJORITY of the expected members must be unreachable / no leader must be
//     electable. A single failed member out of three never triggers recovery —
//     the cluster still has quorum and self-heals via normal reconciliation.
//   - The condition must persist for quorumLossGracePeriod. A transient blip
//     (rolling restart, brief network partition, node reboot) that clears within
//     the window is ignored. The first-observed time is persisted on status so
//     the window survives controller restarts.

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// quorumLossGracePeriod is how long a candidate quorum-loss condition must
	// persist before the controller commits to a disruptive rebuild. It must be
	// comfortably longer than a normal rolling restart / leader election so we
	// never recover a cluster that was about to heal on its own.
	quorumLossGracePeriod = 60 * time.Second

	// forceNewClusterArg is the etcd flag that rewrites a member's raft
	// membership to contain only itself, bootstrapping a new single-member
	// cluster from existing data.
	forceNewClusterArg = "--force-new-cluster"

	// recoveryForceNewClusterAnnotation is set on the StatefulSet while a rebuild
	// is in progress. It is the single source of truth for "is --force-new-cluster
	// currently injected", making the rebuild step idempotent and observable with
	// `kubectl get sts -o yaml` even mid-recovery.
	recoveryForceNewClusterAnnotation = "operator.etcd.io/force-new-cluster"

	// ConditionRecovering is the status condition type the operator raises while
	// a quorum-loss recovery is in progress.
	ConditionRecovering = "Recovering"

	// ConditionDataLossPossible is the status condition type the operator raises
	// when it rebuilds a cluster from a single survivor. The rebuild retains only
	// the data committed to that survivor, so any write the destroyed majority had
	// committed but not yet replicated to the survivor is lost. This condition is
	// set True and LEFT True after a rebuild so the loss remains visible long after
	// the (transient) Recovering condition clears — it is an audit marker, not a
	// liveness signal, and a human is expected to acknowledge it.
	ConditionDataLossPossible = "DataLossPossible"
)

// Event reasons surfaced on the EtcdCluster object during recovery.
const (
	EventReasonQuorumLossDetected = "QuorumLossDetected"
	EventReasonRecoveryStarted    = "RecoveryStarted"
	EventReasonRecoveryRebuilding = "RecoveryRebuilding"
	EventReasonRecoveryScalingOut = "RecoveryScalingOut"
	EventReasonRecoveryCompleted  = "RecoveryCompleted"
	// EventReasonPossibleDataLoss is a Warning surfaced LOUDLY when a rebuild
	// from a survivor completes. It is the user-visible counterpart of the
	// DataLossPossible condition and the DataLoss status accounting.
	EventReasonPossibleDataLoss = "PossibleDataLoss"
)

// quorumAssessment is the outcome of inspecting member health for quorum loss.
type quorumAssessment struct {
	// lost is true when a majority of expected members are unreachable AND no
	// leader is electable — i.e. the cluster cannot make progress on its own.
	lost bool
	// expected is the desired voting-member count (cluster Spec.Size).
	expected int
	// reachable is the number of members that answered a status/health probe.
	reachable int
	// hasLeader is true if any reachable member reports a current leader.
	hasLeader bool
	// reason is a short human-readable explanation, used in logs/events.
	reason string
}

// assessQuorum is a pure function (no I/O) that decides whether the observed
// member health represents true quorum loss for a cluster of the given desired
// size. It is the single, table-testable decision point for detection.
//
// memberListErr is the error (if any) returned by the etcd member-list call;
// when the whole cluster is down this call fails and health is empty. We treat a
// failed member list combined with zero reachable members as a strong
// quorum-loss signal, but still require the majority-unreachable arithmetic to
// hold so a 1-node cluster's single restart is never misclassified.
func assessQuorum(desiredSize int, health []etcdutils.EpHealth, memberListErr error) quorumAssessment {
	a := quorumAssessment{expected: desiredSize}

	for i := range health {
		// Only count voting members toward quorum. A learner answers health
		// probes but cannot vote in an election, so a healthy learner does not
		// contribute to the quorum arithmetic. This mirrors countHealthyVoting.
		if health[i].Health && (health[i].Status == nil || !health[i].Status.IsLearner) {
			a.reachable++
		}
		if health[i].Status != nil && health[i].Status.Leader != 0 {
			a.hasLeader = true
		}
	}

	// Quorum for an N-member cluster is floor(N/2)+1. The cluster has lost quorum
	// when fewer than that many members are reachable.
	quorum := desiredSize/2 + 1

	// A single-member cluster has a quorum of 1 and no fault tolerance; a "down"
	// single member is an ordinary pod restart, not a disaster we can recover
	// from another survivor. Never auto-rebuild a size-1 cluster.
	if desiredSize <= 1 {
		a.reason = "single-member cluster has no quorum-loss recovery path"
		return a
	}

	// If a leader is visible, quorum exists by definition — do not recover.
	if a.hasLeader {
		a.reason = fmt.Sprintf("leader present, %d/%d members reachable", a.reachable, desiredSize)
		return a
	}

	if a.reachable >= quorum {
		// Enough members are up to (re)elect a leader; this is a transient
		// leaderless window, not quorum loss. Let normal reconciliation wait it out.
		a.reason = fmt.Sprintf("no leader yet but %d/%d members reachable (quorum %d) — electable", a.reachable, desiredSize, quorum)
		return a
	}

	// Majority unreachable and no leader: true quorum loss.
	a.lost = true
	if memberListErr != nil {
		a.reason = fmt.Sprintf("quorum lost: %d/%d members reachable (quorum %d), member list failed: %v",
			a.reachable, desiredSize, quorum, memberListErr)
	} else {
		a.reason = fmt.Sprintf("quorum lost: %d/%d members reachable (quorum %d), no leader electable",
			a.reachable, desiredSize, quorum)
	}
	return a
}

// recoveryActive reports whether a COMMITTED recovery is currently in flight,
// i.e. the controller has actually started rebuilding (Rebuilding or ScalingOut).
// The Detecting phase is deliberately excluded: during the grace window the
// cluster has only a *suspected* quorum loss that may still self-heal, so normal
// reconciliation must keep running and detection must remain cancellable.
// Callers use this to short-circuit normal reconciliation so scale logic doesn't
// fight the recovery state machine once a rebuild is underway.
func recoveryActive(ec *ecv1alpha1.EtcdCluster) bool {
	r := ec.Status.Recovery
	if r == nil {
		return false
	}
	return r.Phase == ecv1alpha1.RecoveryPhaseRebuilding || r.Phase == ecv1alpha1.RecoveryPhaseScalingOut
}

// maybeRecoverQuorum is the single entry point the controller calls from its
// health/reconcile path. It returns handled=true when it has taken ownership of
// this reconcile (the caller should requeue and not run normal scaling), along
// with the requeue delay. It is fully idempotent: calling it repeatedly with the
// same observed state advances the state machine by at most one safe step.
//
// health/memberListErr describe what the health check observed this loop. When a
// recovery is already in flight, those observations are re-derived against the
// current single-member cluster.
func (r *EtcdClusterReconciler) maybeRecoverQuorum(
	ctx context.Context,
	s *reconcileState,
	health []etcdutils.EpHealth,
	memberListErr error,
) (handled bool, requeueAfter time.Duration, err error) {
	logger := log.FromContext(ctx).WithName("quorum-recovery")

	// If a recovery is already running, drive it forward regardless of the
	// detection heuristic (the cluster is intentionally degraded mid-rebuild).
	if recoveryActive(s.cluster) {
		return r.advanceRecovery(ctx, logger, s, memberListErr)
	}

	a := assessQuorum(s.cluster.Spec.Size, health, memberListErr)
	if !a.lost {
		// Healthy or transiently-degraded: clear any stale detection timestamp so
		// a future blip starts its grace window fresh.
		if s.cluster.Status.Recovery != nil && s.cluster.Status.Recovery.Phase == ecv1alpha1.RecoveryPhaseDetecting {
			logger.Info("Candidate quorum loss cleared before grace period elapsed; cancelling detection", "detail", a.reason)
			s.cluster.Status.Recovery = nil
			meta.RemoveStatusCondition(&s.cluster.Status.Conditions, ConditionRecovering)
		}
		return false, 0, nil
	}

	// Quorum loss observed. Start (or continue) the grace-period clock.
	now := metav1.Now()
	if s.cluster.Status.Recovery == nil || s.cluster.Status.Recovery.Phase != ecv1alpha1.RecoveryPhaseDetecting {
		logger.Info("Candidate quorum loss observed; starting grace period before recovery", "grace", quorumLossGracePeriod, "detail", a.reason)
		s.cluster.Status.Recovery = &ecv1alpha1.RecoveryStatus{
			Phase:              ecv1alpha1.RecoveryPhaseDetecting,
			DetectedTime:       &now,
			LastTransitionTime: &now,
			Message:            a.reason,
		}
		r.setRecoveringCondition(s.cluster, metav1.ConditionFalse, "QuorumLossSuspected", a.reason)
		r.eventf(s.cluster, corev1.EventTypeWarning, EventReasonQuorumLossDetected,
			"Suspected quorum loss: %s. Waiting %s to confirm before recovery.", a.reason, quorumLossGracePeriod)
		return true, requeueDuration, nil
	}

	// Already in Detecting: has the condition persisted long enough?
	detected := s.cluster.Status.Recovery.DetectedTime
	if detected != nil && now.Sub(detected.Time) < quorumLossGracePeriod {
		remaining := quorumLossGracePeriod - now.Sub(detected.Time)
		logger.Info("Quorum loss still within grace period; not recovering yet", "remaining", remaining.Round(time.Second), "detail", a.reason)
		return true, requeueDuration, nil
	}

	// Grace period elapsed and quorum is still lost: commit to recovery. This is
	// the one place we cross from "suspected" to "destructive action", so it is
	// where the durable attempt counter is bumped.
	s.cluster.Status.Recovery.Attempts++
	logger.Info("Sustained quorum loss confirmed; initiating disaster recovery",
		"detail", a.reason, "attempt", s.cluster.Status.Recovery.Attempts)
	r.eventf(s.cluster, corev1.EventTypeWarning, EventReasonRecoveryStarted,
		"Quorum loss sustained for %s (recovery attempt #%d); rebuilding cluster from survivor pod %s-0. WARNING: rebuilding from a single survivor may lose writes not yet replicated to it.",
		quorumLossGracePeriod, s.cluster.Status.Recovery.Attempts, s.cluster.Name)
	r.transitionRecovery(s.cluster, ecv1alpha1.RecoveryPhaseRebuilding,
		fmt.Sprintf("rebuilding single-member cluster from survivor ordinal 0 (%s)", a.reason))
	return r.advanceRecovery(ctx, logger, s, memberListErr)
}

// advanceRecovery executes one step of the recovery state machine based on the
// persisted phase. Each branch is idempotent.
func (r *EtcdClusterReconciler) advanceRecovery(
	ctx context.Context,
	logger logr.Logger,
	s *reconcileState,
	memberListErr error,
) (bool, time.Duration, error) {
	switch s.cluster.Status.Recovery.Phase {
	case ecv1alpha1.RecoveryPhaseRebuilding:
		return r.recoveryRebuild(ctx, logger, s)
	case ecv1alpha1.RecoveryPhaseScalingOut:
		return r.recoveryScaleOut(logger, s, memberListErr)
	default:
		// Detecting handled by caller; Completed/empty means nothing to do.
		return false, 0, nil
	}
}

// recoveryRebuild forces pod-0 to bootstrap a fresh single-member cluster from
// its surviving data directory. It is idempotent across these sub-steps:
//
//  1. Patch the StatefulSet to 1 replica and inject --force-new-cluster (marked
//     by recoveryForceNewClusterAnnotation), rolling pod-0.
//  2. Wait until the single member is healthy and reports itself as leader.
//  3. Remove --force-new-cluster (so a future pod-0 restart doesn't re-fork the
//     cluster) and advance to ScalingOut.
func (r *EtcdClusterReconciler) recoveryRebuild(
	ctx context.Context,
	logger logr.Logger,
	s *reconcileState,
) (bool, time.Duration, error) {
	sts := s.sts

	if !stsHasForceNewCluster(sts) {
		// Safety gate before the destructive flag goes on: confirm the survivor pod
		// actually exists. --force-new-cluster is irreversible (it rewrites raft
		// membership), so we must not arm it for a pod the StatefulSet has not yet
		// created — doing so against a not-yet-scheduled / freshly-replaced ordinal-0
		// risks forking from an empty data dir. If the pod is absent this loop, hold
		// and requeue; the StatefulSet will (re)create it.
		survivorPodName := fmt.Sprintf("%s-%d", sts.Name, s.cluster.Status.Recovery.SurvivorOrdinal)
		var survivor corev1.Pod
		if err := r.Get(ctx, client.ObjectKey{Namespace: sts.Namespace, Name: survivorPodName}, &survivor); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("Rebuild step 1: survivor pod not present yet; holding before arming --force-new-cluster",
					"survivorPod", survivorPodName)
				return true, requeueDuration, nil
			}
			return true, 0, fmt.Errorf("failed to read survivor pod %s/%s before rebuild: %w", sts.Namespace, survivorPodName, err)
		}

		logger.Info("Rebuild step 1: scaling StatefulSet to single survivor and injecting --force-new-cluster",
			"survivorOrdinal", s.cluster.Status.Recovery.SurvivorOrdinal, "survivorPod", survivorPodName)
		r.eventf(s.cluster, corev1.EventTypeWarning, EventReasonRecoveryRebuilding,
			"Rebuilding: restarting %s with --force-new-cluster from surviving data.", survivorPodName)
		// Realign the state ConfigMap with the single-member bootstrap. The
		// ConfigMap is consumed by pod-0 via EnvFrom and otherwise still advertises
		// a multi-member "existing" cluster, which contradicts --force-new-cluster.
		// --force-new-cluster rewrites membership from pod-0's surviving data dir
		// and ignores ETCD_INITIAL_CLUSTER*, so a surviving data dir masks the
		// inconsistency — but on an empty/replaced PVC etcd would otherwise try to
		// bootstrap a fresh 3-member "existing" cluster and hang. Rewriting to a
		// single-member "new" state removes that footgun; the scale-out path
		// restores the multi-member state per member as it re-adds them.
		if err := applyEtcdClusterState(ctx, s.cluster, 1, r.Client, r.Scheme, logger); err != nil {
			return true, 0, fmt.Errorf("failed to reset cluster state ConfigMap for rebuild: %w", err)
		}
		if err := r.patchStatefulSetForForceNewCluster(ctx, sts, true); err != nil {
			return true, 0, fmt.Errorf("failed to inject --force-new-cluster: %w", err)
		}
		// Give the pod time to roll and bootstrap.
		return true, requeueDuration, nil
	}

	// Force-new-cluster is injected; check whether the single survivor is healthy.
	singleEndpoint := []string{clientEndpointForOrdinalIndex(sts, 0)}
	health, healthErr := r.clusterHealth(singleEndpoint)
	if healthErr != nil || len(health) == 0 || !health[0].Health || health[0].Status == nil {
		logger.Info("Rebuild step 2: waiting for survivor to bootstrap single-member cluster",
			"healthErr", healthErr)
		return true, requeueDuration, nil
	}

	st := health[0].Status
	if st.Leader == 0 || st.Leader != st.Header.MemberId {
		logger.Info("Rebuild step 2: survivor up but not yet self-leader; waiting",
			"leader", st.Leader, "self", st.Header.MemberId)
		return true, requeueDuration, nil
	}

	// Single-member cluster is healthy and is its own leader. Before we move on,
	// account for the data loss this rebuild may have incurred: the cluster now
	// contains exactly what THIS survivor had on disk. Anything the destroyed
	// majority committed beyond the survivor's revision is gone. Surface that
	// loudly and durably — Event + condition + structured log + status — so it is
	// never silent. Record it once (the first time we confirm the survivor) so a
	// requeue can't overwrite the captured revision with a later, post-write one.
	if s.cluster.Status.Recovery.DataLoss == nil {
		r.recordPossibleDataLoss(logger, s.cluster, st)
	}

	// Drop the flag so the pod won't re-fork on its next restart, then move to
	// scale-out.
	logger.Info("Rebuild complete: survivor is a healthy single-member cluster; removing --force-new-cluster")
	if err := r.patchStatefulSetForForceNewCluster(ctx, sts, false); err != nil {
		return true, 0, fmt.Errorf("failed to remove --force-new-cluster after rebuild: %w", err)
	}
	r.transitionRecovery(s.cluster, ecv1alpha1.RecoveryPhaseScalingOut,
		"single-member cluster restored; re-adding members one at a time")
	r.eventf(s.cluster, corev1.EventTypeNormal, EventReasonRecoveryScalingOut,
		"Survivor healthy; re-adding members to reach desired size %d.", s.cluster.Spec.Size)
	return true, requeueDuration, nil
}

// recordPossibleDataLoss captures and LOUDLY surfaces the data-loss accounting
// for a completed rebuild. The survivor's status (st) is the source of truth for
// what was retained: its member ID and the highest revision committed to its
// local store. Anything the destroyed majority committed above that revision did
// not survive --force-new-cluster and is unrecoverable.
//
// This is deliberately multi-channel so the loss cannot be missed:
//   - status.recovery.dataLoss: machine-readable accounting that persists.
//   - DataLossPossible condition (True): visible in `kubectl get`/`describe`,
//     left True until a human acknowledges it.
//   - a Warning Event: shows up in `kubectl describe` and event streams/alerts.
//   - a structured log at the operator.
func (r *EtcdClusterReconciler) recordPossibleDataLoss(
	logger logr.Logger,
	ec *ecv1alpha1.EtcdCluster,
	st *clientv3.StatusResponse,
) {
	now := metav1.Now()

	var (
		memberID string
		revision int64
		raftIdx  uint64
	)
	if st.Header != nil {
		memberID = fmt.Sprintf("%x", st.Header.MemberId)
		revision = st.Header.Revision
	}
	raftIdx = st.RaftIndex

	msg := fmt.Sprintf(
		"recovered with possible data loss; rebuilt from survivor member %s (ordinal %d) at revision %d. "+
			"Writes committed by the lost majority above revision %d were NOT retained.",
		memberID, ec.Status.Recovery.SurvivorOrdinal, revision, revision)

	ec.Status.Recovery.DataLoss = &ecv1alpha1.DataLossInfo{
		SurvivorMemberID: memberID,
		SurvivorRevision: revision,
		RaftIndex:        raftIdx,
		RecoveredTime:    &now,
		Message:          msg,
	}

	// Audit marker: set True and leave it True. This is not a liveness signal;
	// it records that a lossy recovery happened and is awaiting human review.
	meta.SetStatusCondition(&ec.Status.Conditions, metav1.Condition{
		Type:               ConditionDataLossPossible,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: ec.Generation,
		Reason:             "RebuiltFromSurvivor",
		Message:            msg,
	})

	logger.Info("POSSIBLE DATA LOSS during quorum-loss recovery",
		"survivorMemberID", memberID,
		"survivorRevision", revision,
		"survivorRaftIndex", raftIdx,
		"survivorOrdinal", ec.Status.Recovery.SurvivorOrdinal,
		"detail", msg)

	r.eventf(ec, corev1.EventTypeWarning, EventReasonPossibleDataLoss, "%s", msg)
}

// recoveryScaleOut checks whether the cluster has been rebuilt to its desired
// size and quorum. It does NOT itself add members — it hands control back to the
// normal reconcile path (which owns the learner add/promote logic) and only
// declares completion once Spec.Size healthy voting members exist. This keeps
// all membership-change code in one place.
func (r *EtcdClusterReconciler) recoveryScaleOut(
	logger logr.Logger,
	s *reconcileState,
	memberListErr error,
) (bool, time.Duration, error) {
	// Guard the delegation to reconcileClusterState. That path derives the
	// member count from s.memberListResp and, when it is nil, treats the cluster
	// as having zero members — which makes targetReplica != memberCnt scale the
	// StatefulSet DOWN, removing a pod mid-rebuild and reversing recovery. A
	// transient member-list failure (rolling learner pod, DNS hiccup) must NOT be
	// interpreted as "a member was removed", so we keep ownership of the loop and
	// requeue until the observation is usable again.
	if memberListErr != nil || s.memberListResp == nil {
		logger.Info("Recovery scale-out: member list unavailable this loop; holding instead of delegating",
			"memberListErr", memberListErr)
		return true, requeueDuration, nil
	}

	healthy, leaderPresent := countHealthyVoting(s.memberHealth)

	if leaderPresent && healthy >= s.cluster.Spec.Size {
		logger.Info("Recovery complete: cluster restored to desired size with quorum",
			"healthyVoting", healthy, "desiredSize", s.cluster.Spec.Size)
		r.transitionRecovery(s.cluster, ecv1alpha1.RecoveryPhaseCompleted,
			fmt.Sprintf("recovered to %d healthy members with quorum", healthy))
		r.setRecoveringCondition(s.cluster, metav1.ConditionFalse, "RecoveryCompleted",
			fmt.Sprintf("Cluster recovered to %d healthy members.", healthy))
		r.eventf(s.cluster, corev1.EventTypeNormal, EventReasonRecoveryCompleted,
			"Quorum-loss recovery complete: %d healthy members with quorum.", healthy)
		// Hand back to normal reconciliation for this and subsequent loops.
		return false, 0, nil
	}

	// Not yet at desired size: let the normal scale-out path run this loop (it
	// adds at most one learner per loop). We stay in ScalingOut but return
	// handled=false so reconcileClusterState executes.
	logger.Info("Recovery scale-out in progress; delegating member re-add to normal reconcile path",
		"healthyVoting", healthy, "desiredSize", s.cluster.Spec.Size, "leaderPresent", leaderPresent)
	return false, 0, nil
}

// countHealthyVoting returns the number of healthy, non-learner members and
// whether a leader is present among them.
func countHealthyVoting(health []etcdutils.EpHealth) (healthy int, leaderPresent bool) {
	for i := range health {
		h := health[i]
		if !h.Health || h.Status == nil {
			continue
		}
		if h.Status.IsLearner {
			continue
		}
		healthy++
		if h.Status.Leader != 0 {
			leaderPresent = true
		}
	}
	return healthy, leaderPresent
}

// stsHasForceNewCluster reports whether the StatefulSet currently carries the
// recovery marker annotation (and therefore the --force-new-cluster flag).
func stsHasForceNewCluster(sts *appsv1.StatefulSet) bool {
	if sts == nil || sts.Annotations == nil {
		return false
	}
	return sts.Annotations[recoveryForceNewClusterAnnotation] == "true"
}

// patchStatefulSetForForceNewCluster toggles single-member rebuild mode on the
// StatefulSet: it sets replicas to 1, adds/removes the --force-new-cluster flag
// on the etcd container, and stamps/clears the marker annotation. It patches the
// live object in place so the change is minimal and survives operator restarts.
func (r *EtcdClusterReconciler) patchStatefulSetForForceNewCluster(
	ctx context.Context,
	sts *appsv1.StatefulSet,
	enable bool,
) error {
	base := sts.DeepCopy()

	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}

	one := int32(1)
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		return fmt.Errorf("statefulset %s/%s has no containers", sts.Namespace, sts.Name)
	}
	container := &sts.Spec.Template.Spec.Containers[0]

	if enable {
		sts.Spec.Replicas = &one
		sts.Annotations[recoveryForceNewClusterAnnotation] = "true"
		if !containsArg(container.Args, forceNewClusterArg) {
			container.Args = append(container.Args, forceNewClusterArg)
		}
	} else {
		delete(sts.Annotations, recoveryForceNewClusterAnnotation)
		container.Args = removeArg(container.Args, forceNewClusterArg)
	}

	return r.Patch(ctx, sts, client.MergeFrom(base))
}

func containsArg(args []string, target string) bool {
	for _, a := range args {
		if a == target {
			return true
		}
	}
	return false
}

func removeArg(args []string, target string) []string {
	out := args[:0]
	for _, a := range args {
		if a != target {
			out = append(out, a)
		}
	}
	return out
}

// transitionRecovery moves the recovery state machine to a new phase, stamping
// the transition time and message and keeping the Recovering condition in sync.
func (r *EtcdClusterReconciler) transitionRecovery(ec *ecv1alpha1.EtcdCluster, phase ecv1alpha1.RecoveryPhase, msg string) {
	now := metav1.Now()
	if ec.Status.Recovery == nil {
		ec.Status.Recovery = &ecv1alpha1.RecoveryStatus{DetectedTime: &now}
	}
	ec.Status.Recovery.Phase = phase
	ec.Status.Recovery.LastTransitionTime = &now
	ec.Status.Recovery.Message = msg

	if phase != ecv1alpha1.RecoveryPhaseCompleted {
		r.setRecoveringCondition(ec, metav1.ConditionTrue, string(phase), msg)
	}
}

// setRecoveringCondition sets the standard Recovering condition on the cluster.
func (r *EtcdClusterReconciler) setRecoveringCondition(ec *ecv1alpha1.EtcdCluster, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&ec.Status.Conditions, metav1.Condition{
		Type:               ConditionRecovering,
		Status:             status,
		ObservedGeneration: ec.Generation,
		Reason:             reason,
		Message:            msg,
	})
}

// eventf records an Event on the cluster if a recorder is configured. It is nil-safe
// so unit tests can exercise the state machine without wiring an event recorder.
func (r *EtcdClusterReconciler) eventf(ec *ecv1alpha1.EtcdCluster, eventType, reason, msgFmt string, args ...interface{}) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(ec, nil, eventType, reason, reason, msgFmt, args...)
}
