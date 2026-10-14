package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// recoverClusterFromAnnotation is set by a human operator on an EtcdCluster to
// start a lost-quorum recovery (design doc §4.8). The value is the ordinal of
// the member to recover from (the survivor). Automatic recovery is not
// supported, so this annotation is the only way to start one. It is removed
// once the recovery has completed.
const recoverClusterFromAnnotation = "operator.etcd.io/recover-cluster-from"

// errQuorumRecoveryNotImplemented is returned by the scaffolded steps below
// that still need an implementation.
var errQuorumRecoveryNotImplemented = errors.New("lost-quorum recovery step not implemented yet")

// maybeStartQuorumRecovery decides whether to *start* a lost-quorum recovery
// (§4.8 "Starting", §4.9 item 7, the Start lost-quorum recovery row of §4.12).
//
// It is only reached after CORRUPT/NOSPACE/per-member repair found nothing to
// do. It starts recovery only when all of these hold:
//   - the etcd cluster is still unhealthy;
//   - the recoverClusterFromAnnotation annotation is set on the EtcdCluster
//     and holds a valid ordinal of an existing EtcdMember.
//
// Starting means writing Status.QuorumRecovery = {Survivor: <ordinal>} before
// touching anything else; the next reconcile then picks it up through
// continueQuorumRecovery (§4.9 item 2). Without the annotation this is a
// no-op and the loop moves on to the next dispatch step.
//
//nolint:unparam // The Result is always zero only while this is scaffolding; starting recovery will return a non-zero Result to end the loop.
func (r *EtcdClusterReconciler) maybeStartQuorumRecovery(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	value, ok := s.cluster.Annotations[recoverClusterFromAnnotation]
	if !ok {
		return ctrl.Result{}, nil
	}

	// No debounce is needed for now. s.health.Healthy can go false from a
	// single failed MemberList/AlarmList call (healthCheck, pods.go) — a
	// transient blip, not necessarily a real lost quorum — but recovery is
	// only ever triggered manually, so the operator who set the annotation
	// has already judged that quorum is lost. Debouncing (requiring the
	// cluster to stay unhealthy for some minimum duration or number of
	// consecutive reconciles) would only matter for automatic recovery,
	// which isn't supported yet; revisit it if that is ever added.
	if s.health == nil || s.health.Healthy {
		return ctrl.Result{}, nil
	}

	survivor, err := parseRecoverClusterFrom(value)
	if err != nil {
		logger.Error(err, "Ignoring invalid lost-quorum recovery annotation",
			"annotation", recoverClusterFromAnnotation, "value", value)
		return ctrl.Result{}, nil
	}

	// TODO: check that an EtcdMember with this ordinal exists in s.members.

	// TODO: persist Status.QuorumRecovery = &QuorumRecoveryStatus{Survivor:
	// survivor} (before any mutation, §4.6's write-ahead rule), then return
	// a non-zero Result so this loop ends and the next one continues.
	_ = survivor
	return ctrl.Result{}, nil
}

// continueQuorumRecovery continues an already-started lost-quorum recovery
// (§4.8 "Continuing", §4.9 item 2, reconcile_cluster_v0.3.0.png "recover
// cluster from lost-quorum case"). Only called while
// Status.QuorumRecovery != nil.
//
// Any failing step returns its error immediately, and Status.QuorumRecovery
// and the annotation are deliberately left in place so the next reconcile
// resumes from live state.
func (r *EtcdClusterReconciler) continueQuorumRecovery(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	recovery := s.cluster.Status.QuorumRecovery

	// 1. Find the member related to Status.QuorumRecovery.Survivor.
	survivor := findMemberByOrdinal(s.members, recovery.Survivor)
	if survivor == nil {
		return ctrl.Result{}, fmt.Errorf("lost-quorum recovery survivor %d not found", recovery.Survivor)
	}

	// 2. Check live state: is the etcd cluster healthy?
	if s.health == nil || !s.health.Healthy {
		// Not healthy: --force-new-cluster hasn't taken effect yet.
		if err := r.recreateSurvivorWithForceNewCluster(ctx, s, survivor); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.terminateOtherMembers(ctx, s, survivor); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Healthy: the survivor is now a working single-member cluster.
	if err := r.cleanupRecoverClusterFromAnnotation(ctx, s); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.terminateOtherMembers(ctx, s, survivor); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.clearQuorumRecovery(ctx, s); err != nil {
		return ctrl.Result{}, err
	}
	// Recovery never adds members: the cluster is now undersized and the
	// existing scale-out path brings it back to Spec.Size.
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// recreateSurvivorWithForceNewCluster deletes and recreates the survivor's
// Pod with "--force-new-cluster" so it restarts as a single-member cluster.
// The flag is not persisted anywhere: it only applies to that one startup,
// so a later recreate of this Pod naturally starts without it.
//
// It must be idempotent: if the Pod was already recreated with the flag, do
// not recreate it again.
func (r *EtcdClusterReconciler) recreateSurvivorWithForceNewCluster(ctx context.Context, s *reconcileState, survivor *ecv1alpha1.EtcdMember) error {
	// TODO: implement.
	return errQuorumRecoveryNotImplemented
}

// terminateOtherMembers terminates every EtcdMember other than the survivor,
// reusing the existing Terminating flow (§4.6): etcd-side removal (a no-op
// for these members, as they're no longer part of the survivor's cluster),
// alarm disarm, Pod/PVC cleanup and object deletion. Certificate cleanup is
// skipped (§4.6's interim exception). It must be a no-op when no other
// member is left.
func (r *EtcdClusterReconciler) terminateOtherMembers(ctx context.Context, s *reconcileState, survivor *ecv1alpha1.EtcdMember) error {
	// TODO: implement.
	return errQuorumRecoveryNotImplemented
}

// cleanupRecoverClusterFromAnnotation removes recoverClusterFromAnnotation
// from the EtcdCluster if it still exists.
func (r *EtcdClusterReconciler) cleanupRecoverClusterFromAnnotation(ctx context.Context, s *reconcileState) error {
	// TODO: implement.
	return errQuorumRecoveryNotImplemented
}

// clearQuorumRecovery sets Status.QuorumRecovery back to nil, which tells
// §4.9 the recovery is no longer under way. It may also update the
// observational fields (LastRecoveryTime, RecoveryCount, LastRecoveredFrom),
// which must never be used to make decisions.
func (r *EtcdClusterReconciler) clearQuorumRecovery(ctx context.Context, s *reconcileState) error {
	// TODO: implement.
	return errQuorumRecoveryNotImplemented
}

// parseRecoverClusterFrom parses the value of recoverClusterFromAnnotation:
// the ordinal of the member to recover from.
func parseRecoverClusterFrom(value string) (int, error) {
	ordinal, err := strconv.Atoi(value)
	if err != nil || ordinal < 0 {
		return 0, fmt.Errorf("invalid member ordinal %q", value)
	}
	return ordinal, nil
}

// findMemberByOrdinal returns the EtcdMember with the given ordinal, or nil.
func findMemberByOrdinal(members []ecv1alpha1.EtcdMember, ordinal int) *ecv1alpha1.EtcdMember {
	for i := range members {
		if members[i].Spec.Ordinal == ordinal {
			return &members[i]
		}
	}
	return nil
}
