// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

const (
	// QuorumRecoveryAnnotation is how a human operator starts a lost-quorum
	// recovery (design doc §4.8), since automatic recovery is disabled in
	// v0.3.0. Its value is the ordinal of the member to recover from, or
	// "auto" for the reachable voting member with the highest Raft index.
	// The controller removes it once read, so a request it ignored can't
	// start a recovery later.
	QuorumRecoveryAnnotation = "operator.etcd.io/recover-quorum-from"

	quorumRecoveryAuto  = "auto"
	forceNewClusterFlag = "--force-new-cluster"
)

// clusterUnhealthy reports whether the cluster can't be shown to have
// quorum: MemberList or AlarmList failed, or there are no Pods to ask while
// more than one EtcdMember exists. A cluster that is still bootstrapping has
// a single EtcdMember, since scale-out waits for it to be Ready, so it never
// counts as unhealthy before its first Pod runs.
func clusterUnhealthy(s *reconcileState) bool {
	return (len(s.pods) > 0 || len(s.members) > 1) && (s.health == nil || !s.health.Healthy)
}

// startRequestedQuorumRecovery reads QuorumRecoveryAnnotation and, when the
// cluster is unhealthy and the annotation names a valid survivor, records
// the decision in Status.QuorumRecovery for continueQuorumRecovery to carry
// out. It returns a zero Result when there is no request.
func (r *EtcdClusterReconciler) startRequestedQuorumRecovery(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	value, ok := s.cluster.Annotations[QuorumRecoveryAnnotation]
	if !ok {
		return ctrl.Result{}, nil
	}
	logger := log.FromContext(ctx)

	survivor, err := pickQuorumRecoverySurvivor(logger, s, value)
	switch {
	case err != nil:
		logger.Error(err, "Ignoring the lost-quorum recovery request", "annotation", QuorumRecoveryAnnotation, "value", value)
	case !clusterUnhealthy(s):
		logger.Info("Ignoring the lost-quorum recovery request: the cluster is healthy", "annotation", QuorumRecoveryAnnotation, "value", value)
	default:
		logger.Info("Starting the lost-quorum recovery a human operator requested", "survivor", etcdMemberName(s.cluster.Name, survivor))
		s.cluster.Status.QuorumRecovery = &ecv1alpha1.QuorumRecoveryStatus{Survivor: survivor}
		if err := r.Status().Update(ctx, s.cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("recording the lost-quorum recovery from ordinal %d: %w", survivor, err)
		}
	}

	if err := r.removeQuorumRecoveryAnnotation(ctx, s.cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// pickQuorumRecoverySurvivor resolves a QuorumRecoveryAnnotation value to the
// ordinal of the member to recover from. The survivor must be a voting
// member, and its data must be on a PVC, since recovery recreates its Pod.
func pickQuorumRecoverySurvivor(logger logr.Logger, s *reconcileState, value string) (int, error) {
	if s.cluster.Spec.StorageSpec == nil {
		return 0, errors.New("the cluster has no persistent storage, so recreating the survivor's Pod would lose its data")
	}

	if value == quorumRecoveryAuto {
		member := mostUpToDateMember(logger, s)
		if member == nil {
			return 0, errors.New("no voting member answered the health check; set the annotation to the ordinal of the member to recover from")
		}
		return member.Spec.Ordinal, nil
	}

	// TODO: reject any other value when the annotation is set, with a
	// ValidatingAdmissionPolicy or a webhook.
	ordinal, err := strconv.Atoi(value)
	if err != nil || ordinal < 0 {
		return 0, fmt.Errorf("want a member ordinal or %q, got %q", quorumRecoveryAuto, value)
	}
	member := findMemberByOrdinal(s.members, ordinal)
	if member == nil {
		return 0, fmt.Errorf("no EtcdMember with ordinal %d", ordinal)
	}
	if err := checkQuorumRecoverySurvivor(s, member); err != nil {
		return 0, err
	}
	return ordinal, nil
}

// checkQuorumRecoverySurvivor returns why member can't be the survivor of a
// lost-quorum recovery, or nil if it can: it must be a voting member that
// isn't being deleted.
func checkQuorumRecoverySurvivor(s *reconcileState, member *ecv1alpha1.EtcdMember) error {
	if member.DeletionTimestamp != nil {
		return fmt.Errorf("EtcdMember %q is being deleted", member.Name)
	}
	if p := member.Status.Phase; p != ecv1alpha1.EtcdMemberReady && p != ecv1alpha1.EtcdMemberRecreating {
		return fmt.Errorf("EtcdMember %q is %q, not a voting member", member.Name, p)
	}
	if s.health != nil {
		if h, ok := s.health.Members[member.Name]; ok && h.Status != nil && h.Status.IsLearner {
			return fmt.Errorf("EtcdMember %q is a learner", member.Name)
		}
	}
	return nil
}

// mostUpToDateMember returns the healthy member with the highest Raft commit
// index among those checkQuorumRecoverySurvivor accepts, the lowest ordinal on
// a tie, or nil when none answered the latest health check.
func mostUpToDateMember(logger logr.Logger, s *reconcileState) *ecv1alpha1.EtcdMember {
	if s.health == nil {
		return nil
	}
	var best *ecv1alpha1.EtcdMember
	var bestIndex uint64
	for i := range s.members { // sorted by ordinal
		m := &s.members[i]
		h, ok := s.health.Members[m.Name]
		if !ok || !h.Health || checkQuorumRecoverySurvivor(s, m) != nil {
			continue
		}
		if h.Status == nil {
			// MemberHealth fetches the status on a best-effort basis.
			logger.Info("Skipping a healthy member for lost-quorum recovery: its status is unavailable", "member", m.Name)
			continue
		}
		if best == nil || h.Status.RaftIndex > bestIndex {
			best, bestIndex = m, h.Status.RaftIndex
		}
	}
	return best
}

// continueQuorumRecovery carries out a started lost-quorum recovery (design
// doc §4.8, reconcile_cluster_v0.3.0.png): restart the survivor as a new
// single-member cluster with --force-new-cluster, terminate every other
// member, then clear Status.QuorumRecovery so scale-out rebuilds the cluster.
// Each call takes at most one step and requeues.
func (r *EtcdClusterReconciler) continueQuorumRecovery(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	ordinal := s.cluster.Status.QuorumRecovery.Survivor

	// A request that arrived while this recovery was being recorded must not
	// start a second one later.
	if err := r.removeQuorumRecoveryAnnotation(ctx, s.cluster); err != nil {
		return ctrl.Result{}, err
	}

	survivor := findMemberByOrdinal(s.members, ordinal)
	if survivor == nil || survivor.DeletionTimestamp != nil {
		logger.Info("Lost-quorum recovery survivor is gone; waiting for a human operator", "survivor", etcdMemberName(s.cluster.Name, ordinal))
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	pod := findPodForEtcdMember(s, survivor)
	if pod == nil || !podForcesNewCluster(pod) {
		return r.restartSurvivorAsNewCluster(ctx, s, survivor, pod)
	}

	// From here on refreshClusterState only asks the survivor.
	if !survivorIsAlone(s, survivor) {
		logger.Info("[Quorum recovery] waiting for the survivor to run as a single-member cluster", "member", survivor.Name)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// The survivor no longer lists the other members, so their Terminating
	// cleanup skips MemberRemove and only deletes their Pods, PVCs and
	// EtcdMembers.
	others := 0
	for i := range s.members {
		m := &s.members[i]
		if m.Spec.Ordinal == ordinal {
			continue
		}
		others++
		if m.DeletionTimestamp == nil {
			logger.Info("[Quorum recovery] terminating member", "member", m.Name)
			if err := r.Delete(ctx, m); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("deleting EtcdMember %q for lost-quorum recovery: %w", m.Name, err)
			}
			continue
		}
		if err := r.markMemberTerminating(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
		if _, err := r.cleanupEtcdMember(ctx, s, m); err != nil {
			return ctrl.Result{}, err
		}
	}
	if others > 0 {
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	logger.Info("Lost-quorum recovery finished; scale-out will add the other members back", "survivor", survivor.Name)
	now := metav1.Now()
	s.cluster.Status.QuorumRecovery = nil
	s.cluster.Status.LastRecoveryTime = &now
	s.cluster.Status.RecoveryCount++
	s.cluster.Status.LastRecoveredFrom = strconv.Itoa(ordinal)
	if err := r.Status().Update(ctx, s.cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("recording the end of the lost-quorum recovery: %w", err)
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// restartSurvivorAsNewCluster gets the survivor's Pod running with
// --force-new-cluster: it deletes a Pod that runs without the flag and
// creates a missing one with it. If quorum came back before that happened,
// it cancels the recovery instead, recreating the survivor's Pod if it was
// already deleted.
func (r *EtcdClusterReconciler) restartSurvivorAsNewCluster(
	ctx context.Context,
	s *reconcileState,
	survivor *ecv1alpha1.EtcdMember,
	pod *corev1.Pod,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if s.health != nil && s.health.Healthy {
		logger.Info("Cluster is healthy again; cancelling the lost-quorum recovery", "survivor", survivor.Name)
		if pod == nil {
			if err := r.createPodForEtcdMember(ctx, s, survivor, false); err != nil {
				return ctrl.Result{}, err
			}
		}
		s.cluster.Status.QuorumRecovery = nil
		if err := r.Status().Update(ctx, s.cluster); err != nil {
			return ctrl.Result{}, fmt.Errorf("cancelling the lost-quorum recovery: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	if pod != nil {
		if pod.DeletionTimestamp == nil {
			logger.Info("[Quorum recovery] deleting the survivor's Pod to restart it with "+forceNewClusterFlag, "pod", pod.Name)
			if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("deleting Pod %q for lost-quorum recovery: %w", pod.Name, err)
			}
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	logger.Info("[Quorum recovery] starting the survivor as a new single-member cluster", "member", survivor.Name)
	if err := r.createForceNewClusterPod(ctx, s, survivor); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// createForceNewClusterPod creates the survivor's Pod with
// --force-new-cluster. etcd applies that flag on every start, dropping every
// other member it knows of, so the Pod must not keep it once scale-out adds
// members back. Its spec-hash annotation is left empty for that reason: once
// the cluster is healthy, updateConfig sees the mismatch and recreates the
// Pod without the flag before scale-out runs.
func (r *EtcdClusterReconciler) createForceNewClusterPod(ctx context.Context, s *reconcileState, survivor *ecv1alpha1.EtcdMember) error {
	pod, err := buildMemberPod(s.cluster, survivor, etcdClusterStateExisting, initialClusterForPod(s, survivor, true), r.Scheme)
	if err != nil {
		return err
	}
	pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, forceNewClusterFlag)
	pod.Annotations[HashMetadataKey] = ""
	if err := r.Create(ctx, pod); err != nil {
		return fmt.Errorf("creating Pod %q with %s: %w", pod.Name, forceNewClusterFlag, err)
	}
	return nil
}

// survivorIsAlone reports whether the survivor answers a linearizable
// MemberList as the only member of its cluster, that is, whether
// --force-new-cluster has taken effect.
func survivorIsAlone(s *reconcileState, survivor *ecv1alpha1.EtcdMember) bool {
	if s.health == nil || !s.health.Healthy || s.memberListResp == nil || len(s.memberListResp.Members) != 1 {
		return false
	}
	return findEtcdNodeForEtcdMember(s, survivor) != nil
}

// healthCheckPods returns the Pods refreshClusterState asks about. Once the
// survivor of a lost-quorum recovery runs with --force-new-cluster, only its
// view counts: the other members are being discarded, and asking them could
// fail the linearizable calls.
func healthCheckPods(s *reconcileState) []*corev1.Pod {
	qr := s.cluster.Status.QuorumRecovery
	if qr == nil {
		return s.pods
	}
	for _, p := range s.pods {
		if podOrdinal(p.Name, s.cluster.Name) == qr.Survivor && podForcesNewCluster(p) {
			return []*corev1.Pod{p}
		}
	}
	return s.pods
}

// podForcesNewCluster reports whether the Pod starts etcd with
// --force-new-cluster.
func podForcesNewCluster(pod *corev1.Pod) bool {
	return len(pod.Spec.Containers) > 0 && slices.Contains(pod.Spec.Containers[0].Args, forceNewClusterFlag)
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

// removeQuorumRecoveryAnnotation removes QuorumRecoveryAnnotation from the
// EtcdCluster if it is set.
func (r *EtcdClusterReconciler) removeQuorumRecoveryAnnotation(ctx context.Context, ec *ecv1alpha1.EtcdCluster) error {
	if _, ok := ec.Annotations[QuorumRecoveryAnnotation]; !ok {
		return nil
	}
	patch := client.MergeFrom(ec.DeepCopy())
	delete(ec.Annotations, QuorumRecoveryAnnotation)
	if err := r.Patch(ctx, ec, patch); err != nil {
		return fmt.Errorf("removing annotation %s: %w", QuorumRecoveryAnnotation, err)
	}
	return nil
}
