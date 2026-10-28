package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// recoverClusterFromAnnotation is set by a human operator on an EtcdCluster to
// start a lost-quorum recovery (design doc §4.8). The value is the ordinal of
// the member to recover from (the survivor). Automatic recovery is not
// supported, so this annotation is the only way to start one. It is removed
// once the recovery has completed.
const recoverClusterFromAnnotation = "operator.etcd.io/recover-cluster-from"

// forceNewClusterFlag restarts etcd as a single-member cluster from its
// existing data.
const forceNewClusterFlag = "--force-new-cluster"

// Event reasons and action for a lost-quorum recovery (§4.8) and for an
// unhealthy cluster waiting at §4.9 item 7.
const (
	reasonQuorumRecoveryStarted  = "QuorumRecoveryStarted"
	reasonClusterUnhealthy       = "ClusterUnhealthy"
	reasonQuorumRecoveryRejected = "QuorumRecoveryRejected"
	actionRecoverCluster         = "RecoverCluster"
)

// errClusterUnhealthy is logged while an unhealthy cluster waits for quorum
// to come back or for a human operator to request a recovery.
var errClusterUnhealthy = errors.New("the etcd cluster is not healthy")

// clusterHealthy reports whether the cluster is shown to have quorum:
// MemberList and AlarmList succeeded (healthCheck). A cluster that is still
// bootstrapping, with a single EtcdMember and no Pod yet, counts as healthy
// too, since scale-out waits for that member to be Ready. The survivor of a
// lost-quorum recovery is also alone without a Pod while its Pod is
// recreated, so that exception only applies when no recovery is under way.
func clusterHealthy(s *reconcileState) bool {
	if s.health != nil && s.health.Healthy {
		return true
	}

	return s.cluster.Status.QuorumRecovery == nil && len(s.pods) == 0 && len(s.members) <= 1
}

// maybeStartQuorumRecovery decides whether to *start* a lost-quorum recovery
// (§4.8 "Starting", §4.9 item 7, the Start lost-quorum recovery row of §4.12).
//
// It is only reached after CORRUPT/NOSPACE/per-member repair found nothing to
// do. It starts recovery only when all of these hold:
//   - the etcd cluster is still unhealthy (!clusterHealthy);
//   - the recoverClusterFromAnnotation annotation is set on the EtcdCluster
//     and holds a valid ordinal of an existing EtcdMember that
//     checkQuorumRecoverySurvivor accepts.
//
// Starting means writing Status.QuorumRecovery = {Survivor: <ordinal>} before
// touching anything else; the next reconcile then picks it up through
// continueQuorumRecovery (§4.9 item 2). On a healthy cluster this is a no-op
// and the loop moves on to the next dispatch step. An unhealthy cluster
// without a usable annotation ends the loop here, as
// reconcile_loop_v0.3.0.png shows: steps 8 and 9 assume a healthy cluster.
func (r *EtcdClusterReconciler) maybeStartQuorumRecovery(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	// No debounce is needed for now. s.health.Healthy can go false from a
	// single failed MemberList/AlarmList call (healthCheck, pods.go) — a
	// transient blip, not necessarily a real lost quorum — but recovery is
	// only ever triggered manually, so the operator who set the annotation
	// has already judged that quorum is lost. Debouncing (requiring the
	// cluster to stay unhealthy for some minimum duration or number of
	// consecutive reconciles) would only matter for automatic recovery,
	// which isn't supported yet; revisit it if that is ever added.
	if clusterHealthy(s) {
		return ctrl.Result{}, nil
	}
	logger := log.FromContext(ctx)

	value, ok := s.cluster.Annotations[recoverClusterFromAnnotation]
	if !ok {
		logger.Error(errClusterUnhealthy, "Waiting for the cluster to recover, or for a human operator to request a lost-quorum recovery",
			"annotation", recoverClusterFromAnnotation)
		r.Recorder.Eventf(s.cluster, nil, corev1.EventTypeWarning, reasonClusterUnhealthy, actionRecoverCluster,
			"The cluster is not healthy. If it lost quorum, set the %s annotation to the ordinal of the member to recover from",
			recoverClusterFromAnnotation)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// TODO: reject an invalid value when the annotation is set, with a
	// ValidatingAdmissionPolicy or a webhook, rather than only here.
	survivor, err := parseRecoverClusterFrom(value)
	if err == nil {
		err = checkQuorumRecoverySurvivor(s, survivor)
	}
	if err != nil {
		logger.Error(err, "Ignoring invalid lost-quorum recovery annotation",
			"annotation", recoverClusterFromAnnotation, "value", value)
		r.Recorder.Eventf(s.cluster, nil, corev1.EventTypeWarning, reasonQuorumRecoveryRejected, actionRecoverCluster,
			"Cannot recover from %s=%q: %v", recoverClusterFromAnnotation, value, err)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	logger.Info("[Quorum recovery] starting", "survivor", etcdMemberName(s.cluster.Name, survivor))
	s.cluster.Status.QuorumRecovery = &ecv1alpha1.QuorumRecoveryStatus{Survivor: survivor}
	if err := r.Status().Update(ctx, s.cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to start the lost-quorum recovery from member %d: %w", survivor, err)
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// checkQuorumRecoverySurvivor returns why the member with the given ordinal
// can't be the survivor of a lost-quorum recovery, or nil if it can: it must
// be a Ready member that isn't being deleted.
func checkQuorumRecoverySurvivor(s *reconcileState, ordinal int) error {
	member := findMemberByOrdinal(s.members, ordinal)
	switch {
	case member == nil:
		return fmt.Errorf("no EtcdMember has ordinal %d", ordinal)
	case member.DeletionTimestamp != nil:
		return fmt.Errorf("EtcdMember %q is being deleted", member.Name)
	case member.Status.Phase != ecv1alpha1.EtcdMemberReady:
		return fmt.Errorf("EtcdMember %q is %q, not a voting member", member.Name, member.Status.Phase)
	}
	return nil
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
	if !clusterHealthy(s) {
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
// etcd applies the flag on every start of the container, not only the first,
// so the Pod is created with an empty spec hash: once the cluster is healthy,
// updateConfig recreates it without the flag before scale-out adds any member.
//
// It must be idempotent: if the Pod was already recreated with the flag, do
// not recreate it again. Deleting the old Pod and creating the new one take
// separate reconciles, since the new Pod reuses the old one's name.
func (r *EtcdClusterReconciler) recreateSurvivorWithForceNewCluster(ctx context.Context, s *reconcileState, survivor *ecv1alpha1.EtcdMember) error {
	logger := log.FromContext(ctx)

	pod := findPodForEtcdMember(s, survivor)
	switch {
	case pod == nil:
		logger.Info("[Quorum recovery] creating the survivor's Pod with "+forceNewClusterFlag, "member", survivor.Name)
		r.publishQuorumRecoveryStarted(s, survivor)
		// etcd ignores the initial cluster once its data directory exists, so
		// it only lists the survivor, the one member of the new cluster.
		newPod, err := buildMemberPod(s.cluster, survivor, s.dnsDomain, etcdClusterStateExisting, initialClusterForPod(s, survivor, true), r.Scheme)
		if err != nil {
			return err
		}
		newPod.Spec.Containers[0].Args = append(newPod.Spec.Containers[0].Args, forceNewClusterFlag)
		newPod.Annotations[HashMetadataKey] = ""
		if err := r.Create(ctx, newPod); err != nil {
			return fmt.Errorf("failed to create Pod %q with %s: %w", newPod.Name, forceNewClusterFlag, err)
		}
	case podForcesNewCluster(pod):
		// The Pod was recreated with --force-new-cluster in an earlier pass.
		// The cluster stays unhealthy until etcd serves, so we keep re-entering
		// this branch — nothing to do, just wait.
	case pod.DeletionTimestamp != nil:
		// Pod deletion from a previous pass is still in flight.
		logger.Info("[Quorum recovery] waiting for the survivor's Pod to be deleted", "pod", pod.Name)
	default:
		logger.Info("[Quorum recovery] deleting the survivor's Pod to restart it with "+forceNewClusterFlag, "pod", pod.Name)
		r.publishQuorumRecoveryStarted(s, survivor)
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete Pod %q: %w", pod.Name, err)
		}
	}
	return nil
}

// publishQuorumRecoveryStarted publishes the event naming the member the
// recovery started from (§4.8). It is called right before the survivor's Pod
// is deleted or created, so it is recorded even if that fails, and not on a
// pass that only waits.
func (r *EtcdClusterReconciler) publishQuorumRecoveryStarted(s *reconcileState, survivor *ecv1alpha1.EtcdMember) {
	r.Recorder.Eventf(s.cluster, survivor, corev1.EventTypeNormal, reasonQuorumRecoveryStarted, actionRecoverCluster,
		"Recovering the cluster from EtcdMember %s", survivor.Name)
}

// terminateOtherMembers terminates every EtcdMember other than the survivor,
// reusing finalizeClusterMember: the member, its Pod and its PVC are deleted
// and its finalizer is removed. These members are no longer part of the
// survivor's cluster, so there is no etcd membership to change. It is a
// no-op when no other member is left.
func (r *EtcdClusterReconciler) terminateOtherMembers(ctx context.Context, s *reconcileState, survivor *ecv1alpha1.EtcdMember) error {
	logger := log.FromContext(ctx)

	for i := range s.members {
		m := &s.members[i]
		if m.Name == survivor.Name {
			continue
		}
		logger.Info("[Quorum recovery] terminating member", "member", m.Name)
		if err := r.finalizeClusterMember(ctx, m); err != nil {
			return fmt.Errorf("failed to terminate EtcdMember %q: %w", m.Name, err)
		}
	}
	return nil
}

// cleanupRecoverClusterFromAnnotation removes recoverClusterFromAnnotation
// from the EtcdCluster if it still exists.
func (r *EtcdClusterReconciler) cleanupRecoverClusterFromAnnotation(ctx context.Context, s *reconcileState) error {
	if _, ok := s.cluster.Annotations[recoverClusterFromAnnotation]; !ok {
		return nil
	}
	patch := client.MergeFrom(s.cluster.DeepCopy())
	delete(s.cluster.Annotations, recoverClusterFromAnnotation)
	if err := r.Patch(ctx, s.cluster, patch); err != nil {
		return fmt.Errorf("failed to remove annotation %s: %w", recoverClusterFromAnnotation, err)
	}
	return nil
}

// clearQuorumRecovery sets Status.QuorumRecovery back to nil, which tells
// §4.9 the recovery is no longer under way. It may also update the
// observational fields (LastRecoveryTime, RecoveryCount, LastRecoveredFrom),
// which must never be used to make decisions.
func (r *EtcdClusterReconciler) clearQuorumRecovery(ctx context.Context, s *reconcileState) error {
	logger := log.FromContext(ctx)

	survivor := s.cluster.Status.QuorumRecovery.Survivor
	now := metav1.Now()
	s.cluster.Status.QuorumRecovery = nil
	s.cluster.Status.LastRecoveryTime = &now
	s.cluster.Status.RecoveryCount++
	s.cluster.Status.LastRecoveredFrom = strconv.Itoa(survivor)
	if err := r.Status().Update(ctx, s.cluster); err != nil {
		return fmt.Errorf("failed to clear the lost-quorum recovery status: %w", err)
	}
	logger.Info("[Quorum recovery] finished; scale-out adds the other members back", "survivor", etcdMemberName(s.cluster.Name, survivor))
	return nil
}

// podForcesNewCluster reports whether the Pod starts etcd with
// --force-new-cluster.
func podForcesNewCluster(pod *corev1.Pod) bool {
	return len(pod.Spec.Containers) > 0 && slices.Contains(pod.Spec.Containers[0].Args, forceNewClusterFlag)
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
