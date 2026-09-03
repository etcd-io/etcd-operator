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
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// memberCleanupFinalizer is added to every EtcdMember at creation time so the
// Terminating leave-sequence (design doc §4.6, wired up in M3) can run before
// the object is actually removed, whether the deletion came from a
// controller-initiated scale-in or a human operator deleting it directly.
const memberCleanupFinalizer = "operator.etcd.io/member-cleanup"

// reconcileEtcdMember is the shared lifecycle entry point for every normal
// provisioning attempt and its interruption recovery. Future lifecycle
// phases attach focused handlers here without moving provisioning back into
// scaleCluster or dispatch.
func (r *EtcdClusterReconciler) reconcileEtcdMember(
	ctx context.Context,
	state *reconcileState,
	member *ecv1alpha1.EtcdMember,
) (ctrl.Result, error) {
	switch member.Status.Phase {
	// Creating the EtcdMember object and writing its status Phase are two
	// separate calls in createEtcdMember. If the operator crashes in between,
	// the object survives with an empty Phase, which must resume provisioning.
	case "", ecv1alpha1.EtcdMemberPending, ecv1alpha1.EtcdMemberProvisioning:
		return r.reconcileProvisioning(ctx, state, member)
	case ecv1alpha1.EtcdMemberRecreating:
		return r.reconcileRecreating(ctx, state, member)
	case ecv1alpha1.EtcdMemberReplacing:
		return r.reconcileReplacing(ctx, state, member)
	// placeholder for Terminating
	default:
		return ctrl.Result{}, nil
	}
}

// reconcileProvisioning establishes the durable write-before-mutate phase
// boundary and drives normal provisioning as a linear sequence of idempotent
// steps: prerequisites (certificates, PVC), membership registration, Pod
// creation, and health convergence. Ordinal 0 with no live membership is the
// bootstrap voter branch; every other unregistered peer is added as a
// learner.
func (r *EtcdClusterReconciler) reconcileProvisioning(
	ctx context.Context,
	state *reconcileState,
	member *ecv1alpha1.EtcdMember,
) (ctrl.Result, error) {
	// 1. Phase boundary (write-before-mutate): persist Phase=Provisioning
	// before any membership or Pod mutation. On first entry stop here and
	// requeue, so a crash right after this write resumes provisioning instead
	// of restarting it.
	enteringProvisioning := member.Status.Phase == "" || member.Status.Phase == ecv1alpha1.EtcdMemberPending
	if enteringProvisioning {
		if err := r.updateEtcdMemberStatus(ctx, member, func(status *ecv1alpha1.EtcdMemberStatus) {
			status.Phase = ecv1alpha1.EtcdMemberProvisioning
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 2. Idempotent prerequisite: the shared TLS certificates must exist
	// before the member's Pod mounts them.
	if err := r.provisionCertificates(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Idempotent prerequisite: the member's own PVC for ReadWriteOnce
	// storage.
	if err := r.provisionPVC(ctx, state, member); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Initial state and membership registration. Bootstrap — ordinal 0
	// with no live membership — starts the very first voter for the brand-new cluster; this is the only
	// path that starts etcd with cluster-state=new. Every other unregistered
	// peer is added as a learner (etcd admits only one learner at a time, so
	// wait while another learner is still joining). Reaching this again after
	// a crash between MemberAdd and Pod creation is expected — the add is
	// idempotent here because registration is checked against live state.
	bootstrap := member.Spec.Ordinal == 0 &&
		(state.memberListResp == nil || len(state.memberListResp.Members) == 0)

	if !bootstrap {
		if etcdNode := findEtcdNodeForEtcdMember(state, member); etcdNode == nil {
			if err := r.addLearner(state, member); err != nil {
				return ctrl.Result{}, err
			}
			// addLearner updated the membership of etcd cluster, so
			// we need to refresh the reconcileState by requeuing the request.
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	}

	// 5. Pod creation: the member is registered (or is the bootstrap voter)
	// but its Pod is absent (interrupted run, or the Pod was deleted
	// mid-provisioning). Render ETCD_INITIAL_CLUSTER from the authoritative
	// topology, create the Pod, and requeue to give it enough time to
	// start.
	if pod := findPodForEtcdMember(state, member); pod == nil {
		if err := r.createPodForEtcdMember(ctx, state, member, bootstrap); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 6. Convergence: wait until this member's endpoint reports healthy.
	health, err := findHealthStatusForEtcdMember(state, member)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !health.Health {
		// TODO(#472): once a common recovery ladder exists, replace the
		// member once RecreateCount has reached its limit; otherwise
		// recreate the Pod and increment RecreateCount.
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 7. Promotion: if the member is still a learner, wait until the leader
	// considers it caught up, then promote it to a voting member.
	etcdNode := findEtcdNodeForEtcdMember(state, member)
	if etcdNode == nil {
		return ctrl.Result{}, fmt.Errorf("live etcd node for EtcdMember %q is not mapped", member.Name)
	}
	if etcdNode.IsLearner {
		if res, err := r.promoteLearnerForEtcdMember(ctx, state, member, etcdNode); err != nil || !res.IsZero() {
			return res, err
		}
	}

	// 8. Completion: healthy voting member — mark it Ready.
	if err := r.updateEtcdMemberStatus(ctx, member, func(status *ecv1alpha1.EtcdMemberStatus) {
		status.Phase = ecv1alpha1.EtcdMemberReady
		status.RecreateCount = 0
	}); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// podStartTimeout is the maximum time a Pod is allowed to be running but not
// Ready before the reconciler treats it as timed out and replaces it.
const podStartTimeout = 2 * time.Minute

// reconcileRecreating drives the Recreating lifecycle phase for a member whose
// Pod needs to be replaced (e.g. after an upgrade or self-healing). It follows
// the Recreating case in reconcile_member_v0.3.0.png:
//  1. Perform a per-member health check.
//  2. If healthy → reset to Ready.
//  3. If unhealthy and Pod exists but hasn't timed out yet → requeue.
//  4. If unhealthy and Pod is absent or timed out → delete the Pod (if present),
//     create a fresh one and increment RecreateCount, unless RecreateCount >= 3,
//     in which case escalate to Replacing.
func (r *EtcdClusterReconciler) reconcileRecreating(
	ctx context.Context,
	state *reconcileState,
	member *ecv1alpha1.EtcdMember,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Per-member health check.
	health, err := findHealthStatusForEtcdMember(state, member)
	if err != nil {
		// Health data unavailable (e.g. member list empty mid-upgrade) — requeue.
		logger.Info("[Recreating] health status unavailable, requeueing", "member", member.Name, "error", err)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 2. Member is healthy — but only mark Ready if the pod is already running
	// the target version. If it's still on the old image (upgrade in progress),
	// fall through to delete and recreate it.
	var memberPod *corev1.Pod
	if health.Health {
		memberPod = findPodForEtcdMember(state, member)
		onTargetVersion := false
		if memberPod != nil {
			expectedImage := fmt.Sprintf("%s:%s", state.cluster.Spec.ImageRegistry, member.Spec.Version)
			for _, c := range memberPod.Spec.Containers {
				if c.Name == "etcd" && c.Image == expectedImage {
					onTargetVersion = true
					break
				}
			}
		}
		if onTargetVersion {
			logger.Info("[Recreating] member is healthy on target version, marking Ready", "member", member.Name)
			if err := r.updateEtcdMemberStatus(ctx, member, func(status *ecv1alpha1.EtcdMemberStatus) {
				status.Phase = ecv1alpha1.EtcdMemberReady
				status.RecreateCount = 0
			}); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
		// Pod is healthy but still on old image — fall through to replace it.
		logger.Info("[Recreating] member is healthy but on old image, will replace pod",
			"member", member.Name,
		)
	}

	// 3. Member is unhealthy (or healthy but on old image). Check the Pod.
	if memberPod != nil {
		timedOut := memberPod.Status.StartTime != nil &&
			time.Since(memberPod.Status.StartTime.Time) > podStartTimeout
		if !timedOut {
			// Pod is still starting up — give it more time.
			logger.Info("[Recreating] pod not yet timed out, requeueing", "member", member.Name, "pod", memberPod.Name)
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
		// Pod has timed out — fall through to delete + recreate below.
		logger.Info("[Recreating] pod timed out, will replace", "member", member.Name, "pod", memberPod.Name)
	}

	// 4. Escalate to Replacing if RecreateCount is exhausted.
	if member.Status.RecreateCount >= 3 {
		logger.Info("[Recreating] RecreateCount exhausted, escalating to Replacing", "member", member.Name, "recreateCount", member.Status.RecreateCount)
		if err := r.updateEtcdMemberStatus(ctx, member, func(status *ecv1alpha1.EtcdMemberStatus) {
			status.Phase = ecv1alpha1.EtcdMemberReplacing
			status.RecreateCount = 0
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 5. Transfer leadership away before taking this member offline, so the
	// cluster doesn't lose its leader unnecessarily. This is a best-effort
	// operation — if it fails, log and proceed with the pod deletion anyway.
	if member.Status.IsLeader {
		endpoints := clientEndpointsFromPods(state.cluster.Name, state.cluster.Namespace, state.pods, clusterTLSEnabled(state.cluster))
		if len(endpoints) > 0 {
			// Pick the lowest-ordinal member that is not this one as the transfer target.
			var transferTargetID uint64
			for i := range state.members {
				other := &state.members[i]
				if other.Name == member.Name {
					continue
				}
				if node := findEtcdNodeForEtcdMember(state, other); node != nil {
					transferTargetID = node.ID
					break
				}
			}
			if transferTargetID != 0 {
				logger.Info("[Recreating] transferring leadership before pod deletion",
					"member", member.Name,
					"transferTargetID", fmt.Sprintf("%x", transferTargetID),
				)
				if err := etcdutils.MoveLeader(etcdutils.ClientConfig{Endpoints: endpoints, TLS: state.tlsConfig}, transferTargetID); err != nil {
					logger.Info("[Recreating] leader transfer failed, proceeding with pod deletion anyway",
						"member", member.Name,
						"error", err,
					)
				}
			}
		}
	}

	// 6. Delete the timed-out Pod if it still exists.
	if memberPod != nil {
		logger.Info("[Recreating] deleting timed-out pod", "member", member.Name, "pod", memberPod.Name)
		if err := r.Delete(ctx, memberPod); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 6. Pod is absent — create a new one with the current spec and bump RecreateCount.
	logger.Info("[Recreating] creating new pod", "member", member.Name)
	if err := r.ensureProvisioningPod(ctx, state, member, etcdClusterStateExisting, initialClusterForPod(state, member, false)); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}


// reconcileReplacing drives the Replacing lifecycle phase: the member is
// removed from the live etcd cluster, its Pod and PVC are cleaned up, and it
// is reset to Pending so the Provisioning case re-joins it from scratch.
// It follows the Replacing case in reconcile_member_v0.3.0.png:
//  1. Best-effort leader transfer (if this member is the leader).
//  2. Remove the member from the live etcd membership if still registered.
//  3. Delete its Pod if still present.
//  4. Delete its PVC if still present (skipped when no storage spec).
//  5. Certificates are cluster-level (not per-member) — no deletion needed.
//  6. Reset phase to Pending so the Provisioning case re-joins it fresh.
func (r *EtcdClusterReconciler) reconcileReplacing(
	ctx context.Context,
	state *reconcileState,
	member *ecv1alpha1.EtcdMember,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Best-effort leader transfer.
	if member.Status.IsLeader {
		endpoints := clientEndpointsFromPods(state.cluster.Name, state.cluster.Namespace, state.pods, clusterTLSEnabled(state.cluster))
		if len(endpoints) > 0 {
			var transferTargetID uint64
			for i := range state.members {
				other := &state.members[i]
				if other.Name == member.Name {
					continue
				}
				if node := findEtcdNodeForEtcdMember(state, other); node != nil {
					transferTargetID = node.ID
					break
				}
			}
			if transferTargetID != 0 {
				logger.Info("[Replacing] transferring leadership before removal",
					"member", member.Name,
					"transferTargetID", fmt.Sprintf("%x", transferTargetID),
				)
				if err := etcdutils.MoveLeader(etcdutils.ClientConfig{Endpoints: endpoints, TLS: state.tlsConfig}, transferTargetID); err != nil {
					logger.Info("[Replacing] leader transfer failed, proceeding anyway",
						"member", member.Name, "error", err)
				}
			}
		}
	}

	// 2. Remove from live etcd membership if still registered.
	if node := findEtcdNodeForEtcdMember(state, member); node != nil {
		endpoints := clientEndpointsFromPods(state.cluster.Name, state.cluster.Namespace, state.pods, clusterTLSEnabled(state.cluster))
		if len(endpoints) == 0 {
			return ctrl.Result{}, fmt.Errorf("cannot remove EtcdMember %q from cluster: no live client endpoints", member.Name)
		}
		logger.Info("[Replacing] removing member from etcd cluster", "member", member.Name)
		if err := etcdutils.RemoveMember(etcdutils.ClientConfig{Endpoints: endpoints, TLS: state.tlsConfig}, node.ID); err != nil {
			return ctrl.Result{}, fmt.Errorf("removing EtcdMember %q from etcd cluster: %w", member.Name, err)
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 3. Delete the Pod if still present.
	if pod := findPodForEtcdMember(state, member); pod != nil {
		logger.Info("[Replacing] deleting pod", "member", member.Name, "pod", pod.Name)
		if err := r.Delete(ctx, pod); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 4. Delete the PVC if still present (only when storage is configured).
	if state.cluster.Spec.StorageSpec != nil && state.cluster.Spec.StorageSpec.AccessModes != corev1.ReadWriteMany {
		podName := memberPodName(state.cluster.Name, member.Spec.Ordinal)
		pvcName := pvcNameForMember(podName)
		pvc := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: state.cluster.Namespace}, pvc)
		if err == nil {
			logger.Info("[Replacing] deleting PVC", "member", member.Name, "pvc", pvcName)
			if err := r.Delete(ctx, pvc); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		} else if !k8serrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("checking PVC %q for EtcdMember %q: %w", pvcName, member.Name, err)
		}
	}

	// 6. All cleanup done — reset to Pending so Provisioning re-joins fresh.
	logger.Info("[Replacing] cleanup complete, resetting member to Pending", "member", member.Name)
	if err := r.updateEtcdMemberStatus(ctx, member, func(status *ecv1alpha1.EtcdMemberStatus) {
		status.Phase = ecv1alpha1.EtcdMemberPending
		status.RecreateCount = 0
	}); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// provisionCertificates ensures the cluster's shared server/peer TLS
// certificates exist before a member's Pod mounts them. Idempotent.
func (r *EtcdClusterReconciler) provisionCertificates(ctx context.Context, state *reconcileState) error {
	if err := applyEtcdMemberCerts(ctx, state.cluster, r.Client); err != nil {
		return fmt.Errorf("provisioning certificates for EtcdCluster %q: %w", state.cluster.Name, err)
	}
	return nil
}

// provisionPVC ensures the member's own PVC exists. A per-member PVC is only
// needed for ReadWriteOnce storage; clusters without a storage spec or with
// ReadWriteMany storage skip it. Idempotent.
func (r *EtcdClusterReconciler) provisionPVC(ctx context.Context, state *reconcileState, member *ecv1alpha1.EtcdMember) error {
	cluster := state.cluster
	// ReadWriteMany is assumed to be statically provisioned (e.g. a shared
	// NFS-backed PVC) and managed outside the operator, so no per-member PVC
	// is created for it.
	if cluster.Spec.StorageSpec == nil || cluster.Spec.StorageSpec.AccessModes == corev1.ReadWriteMany {
		return nil
	}
	podName := memberPodName(cluster.Name, member.Spec.Ordinal)
	if err := createPVCForMember(ctx, r.Client, cluster, member, podName, r.Scheme); err != nil {
		return fmt.Errorf("provisioning PVC for EtcdMember %q: %w", member.Name, err)
	}
	return nil
}

// addLearner registers the member in the live etcd membership as a learner.
// The member's Pod is created in a later pass that renders
// ETCD_INITIAL_CLUSTER from a fresh membership snapshot.
func (r *EtcdClusterReconciler) addLearner(state *reconcileState, member *ecv1alpha1.EtcdMember) error {
	endpoints := clientEndpointsFromPods(state.cluster.Name, state.cluster.Namespace, state.pods, clusterTLSEnabled(state.cluster))
	if len(endpoints) == 0 {
		return fmt.Errorf("cannot add learner for EtcdMember %q: no live client endpoints", member.Name)
	}

	_, peerURL := peerEndpointForOrdinalIndex(state.cluster, member.Spec.Ordinal)
	if _, err := etcdutils.AddMember(etcdutils.ClientConfig{Endpoints: endpoints, TLS: state.tlsConfig}, []string{peerURL}, true); err != nil {
		return fmt.Errorf("failed to add learner for EtcdMember %q: %w", member.Name, err)
	}

	return nil
}

// promoteLearnerForEtcdMember promotes the member's learner to a voting
// member. A failed promotion is logged and retried on a later pass; the
// absence of live client endpoints is an error because no endpoint exists to
// reach the membership at all.
func (r *EtcdClusterReconciler) promoteLearnerForEtcdMember(
	ctx context.Context,
	state *reconcileState,
	member *ecv1alpha1.EtcdMember,
	etcdNode *etcdserverpb.Member,
) (ctrl.Result, error) {
	endpoints := clientEndpointsFromPods(
		state.cluster.Name,
		state.cluster.Namespace,
		state.pods,
		clusterTLSEnabled(state.cluster),
	)
	if len(endpoints) == 0 {
		return ctrl.Result{}, fmt.Errorf("promoting learner for EtcdMember %q: no live client endpoints", member.Name)
	}
	if err := etcdutils.PromoteLearner(
		etcdutils.ClientConfig{Endpoints: endpoints, TLS: state.tlsConfig},
		etcdNode.ID,
	); err != nil {
		log.FromContext(ctx).Error(err, fmt.Sprintf("failed to promote learner for EtcdMember %q", member.Name))
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}
	return ctrl.Result{}, nil
}

// createPodForEtcdMember creates the Pod for a member that is already
// registered (or is the bootstrap voter): the initial cluster state is new
// only for bootstrap, ETCD_INITIAL_CLUSTER is rendered from the authoritative
// topology, and the Pod is created if absent. Idempotent.
func (r *EtcdClusterReconciler) createPodForEtcdMember(
	ctx context.Context,
	state *reconcileState,
	member *ecv1alpha1.EtcdMember,
	bootstrap bool,
) error {
	initialClusterState := etcdClusterStateExisting
	if bootstrap {
		initialClusterState = etcdClusterStateNew
	}
	return r.ensureProvisioningPod(ctx, state, member, initialClusterState, initialClusterForPod(state, member, bootstrap))
}

// initialClusterForPod renders ETCD_INITIAL_CLUSTER for the member's Pod: a
// self-contained one-member string for bootstrap, otherwise the cluster
// membership rendered from the cluster's EtcdMembers.
func initialClusterForPod(state *reconcileState, member *ecv1alpha1.EtcdMember, bootstrap bool) string {
	if bootstrap {
		name, peerURL := peerEndpointForOrdinalIndex(state.cluster, member.Spec.Ordinal)
		return fmt.Sprintf("%s=%s", name, peerURL)
	}
	return renderInitialCluster(state.cluster, state.members)
}

func (r *EtcdClusterReconciler) ensureProvisioningPod(
	ctx context.Context,
	state *reconcileState,
	member *ecv1alpha1.EtcdMember,
	initialClusterState etcdClusterState,
	initialCluster string,
) error {
	if findPodForEtcdMember(state, member) != nil {
		return nil
	}

	if err := createMemberPod(
		ctx,
		log.FromContext(ctx),
		r.Client,
		state.cluster,
		member,
		initialClusterState,
		initialCluster,
		r.Scheme,
	); err != nil {
		return fmt.Errorf("creating Pod for EtcdMember %q: %w", member.Name, err)
	}

	if err := r.updateEtcdMemberStatus(ctx, member, func(status *ecv1alpha1.EtcdMemberStatus) {
		status.RecreateCount++
	}); err != nil {
		return err
	}
	return nil
}

// findPodForEtcdMember looks up the member's Pod in the reconcile snapshot:
// Pods are listed once per reconcile, so a nil result means the Pod was
// absent at snapshot time.
func findPodForEtcdMember(state *reconcileState, member *ecv1alpha1.EtcdMember) *corev1.Pod {
	podName := memberPodName(state.cluster.Name, member.Spec.Ordinal)
	for _, p := range state.pods {
		if p.Name == podName {
			return p
		}
	}
	return nil
}

// updateEtcdMemberStatus applies a mutation to the latest persisted member
// status, retries optimistic-lock conflicts, and avoids unchanged writes.
func (r *EtcdClusterReconciler) updateEtcdMemberStatus(
	ctx context.Context,
	member *ecv1alpha1.EtcdMember,
	mutate func(*ecv1alpha1.EtcdMemberStatus),
) error {
	key := client.ObjectKeyFromObject(member)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &ecv1alpha1.EtcdMember{}
		if err := r.Get(ctx, key, current); err != nil {
			return err
		}

		desired := current.DeepCopy()
		mutate(&desired.Status)
		if equality.Semantic.DeepEqual(current.Status, desired.Status) {
			member.Status = desired.Status
			member.ResourceVersion = current.ResourceVersion
			return nil
		}

		if err := r.Status().Update(ctx, desired); err != nil {
			return err
		}
		member.Status = desired.Status
		member.ResourceVersion = desired.ResourceVersion
		return nil
	})
	if err != nil {
		return fmt.Errorf("updating status for EtcdMember %q: %w", key.String(), err)
	}
	return nil
}

// findEtcdNodeForEtcdMember correlates an EtcdMember resource with the
// current etcd membership by name: a live node matches when its etcd name —
// or, while a newly added node has not started and is still unnamed, the name
// encoded in its peer URL — equals the EtcdMember's name.
func findEtcdNodeForEtcdMember(state *reconcileState, member *ecv1alpha1.EtcdMember) *etcdserverpb.Member {
	if state.memberListResp == nil {
		return nil
	}

	for _, node := range state.memberListResp.Members {
		for _, peerURL := range node.PeerURLs {
			memberName := node.Name
			if memberName == "" {
				memberName = getMemberNameFromPeerURL(peerURL)
			}
			if memberName == member.Name {
				return node
			}
		}
	}

	return nil
}

// pickNotReadyMember selects from the current live snapshot: a live learner
// mapped to its EtcdMember first, otherwise the lowest-ordinal member that
// isn't Ready yet. state.members is assumed sorted by ordinal, so the first
// match in the second pass is the lowest-ordinal one.
func pickNotReadyMember(
	state *reconcileState,
) *ecv1alpha1.EtcdMember {
	for i := range state.members {
		member := &state.members[i]
		if member.Status.Phase == ecv1alpha1.EtcdMemberReady {
			continue
		}
		if etcdNode := findEtcdNodeForEtcdMember(state, member); etcdNode != nil && etcdNode.IsLearner {
			return member
		}
	}
	for i := range state.members {
		member := &state.members[i]
		if member.Status.Phase != ecv1alpha1.EtcdMemberReady {
			return member
		}
	}
	return nil
}

// pickMemberToUpgrade selects the next member to upgrade to targetVersion.
// It iterates from highest to lowest ordinal and prefers non-leader members first,
// leaving the leader member for last. If all members needing upgrade are leaders
// (or leadership cannot be determined), the highest-ordinal member needing upgrade is returned.
// Returns nil if all members are already at targetVersion.
func pickMemberToUpgrade(members []ecv1alpha1.EtcdMember, targetVersion string) *ecv1alpha1.EtcdMember {
	var leaderCandidate *ecv1alpha1.EtcdMember

	// members is sorted in ascending ordinal order. Iterate in reverse (highest ordinal first).
	for i := len(members) - 1; i >= 0; i-- {
		m := &members[i]
		if m.DeletionTimestamp != nil {
			continue
		}
		if m.Spec.Version == targetVersion {
			continue
		}
		if m.Status.IsLeader {
			if leaderCandidate == nil {
				leaderCandidate = m
			}
			continue
		}
		return m
	}

	return leaderCandidate
}

// findHealthStatusForEtcdMember looks up the member's health entry in the
// reconcile snapshot by its deterministic Pod (= etcd member) name.
func findHealthStatusForEtcdMember(state *reconcileState, member *ecv1alpha1.EtcdMember) (*etcdutils.EpHealth, error) {
	memberName := memberPodName(state.cluster.Name, member.Spec.Ordinal)
	if state.health != nil {
		if memberHealthStatus, ok := state.health.Members[memberName]; ok {
			return &memberHealthStatus, nil
		}
		return nil, fmt.Errorf("health status is not found for member %s", memberName)
	}
	return nil, fmt.Errorf("cannot find the health status for member %s as the health report is empty", memberName)
}

// renderInitialCluster renders ETCD_INITIAL_CLUSTER from the cluster's
// EtcdMembers, assuming they are consistent with the live etcd membership.
// It never infers topology from an ordinal range.
func renderInitialCluster(cluster *ecv1alpha1.EtcdCluster, members []ecv1alpha1.EtcdMember) string {
	entries := make([]string, 0, len(members))
	for i := range members {
		name, peerURL := peerEndpointForOrdinalIndex(cluster, members[i].Spec.Ordinal)
		entries = append(entries, fmt.Sprintf("%s=%s", name, peerURL))
	}

	sort.Strings(entries)
	return strings.Join(entries, ",")
}

// etcdMemberName returns the deterministic name for a member's EtcdMember
// object, matching today's Pod naming so both stay stable and human-readable.
func etcdMemberName(clusterName string, ordinal int) string {
	return fmt.Sprintf("%s-%d", clusterName, ordinal)
}

// listOwnedMembers returns all EtcdMembers owned by ec, sorted in ascending
// ordinal order. The label selector and namespace filter members belonging to the cluster.
func listOwnedMembers(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster) ([]ecv1alpha1.EtcdMember, error) {
	memberList := &ecv1alpha1.EtcdMemberList{}
	if err := c.List(ctx, memberList,
		client.InNamespace(ec.Namespace),
		client.MatchingLabels(clusterNameLabels(ec.Name)),
	); err != nil {
		return nil, fmt.Errorf("failed to list EtcdMembers for cluster %s: %w", ec.Name, err)
	}

	var owned []ecv1alpha1.EtcdMember
	for i := range memberList.Items {
		if metav1.IsControlledBy(&memberList.Items[i], ec) {
			owned = append(owned, memberList.Items[i])
		}
	}

	sort.Slice(owned, func(i, j int) bool {
		return owned[i].Spec.Ordinal < owned[j].Spec.Ordinal
	})
	return owned, nil
}

// memberOrdinals returns the ordinals of the given members.
func memberOrdinals(members []ecv1alpha1.EtcdMember) []int {
	ordinals := make([]int, 0, len(members))
	for _, m := range members {
		ordinals = append(ordinals, m.Spec.Ordinal)
	}
	return ordinals
}

// nextOrdinal picks the ordinal a newly-scaled-out member should use: the
// lowest gap in existing, or max(existing)+1 if there is no gap to reuse.
// Design doc §4.4. This only runs as part of an actual scale-out decision;
// there's no separate action that hunts down and closes a gap on its own.
func nextOrdinal(existing []int) int {
	sort.Ints(existing)
	for i, o := range existing {
		if i != o {
			return i
		}
	}
	return len(existing)
}

// allReady reports whether every member is Phase: Ready. Vacuously true for
// zero members.
func allReady(members []ecv1alpha1.EtcdMember) bool {
	for _, m := range members {
		if m.Status.Phase != ecv1alpha1.EtcdMemberReady {
			return false
		}
	}
	return true
}

// createEtcdMember creates the EtcdMember object for the given ordinal,
// owned by ec, with the cleanup finalizer already attached (design doc §4.3)
// and Phase: Pending. It does not create the member's Pod/PVC/cert or touch
// etcd membership — see design doc §4.6 ("Joining the cluster", M3).
func createEtcdMember(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster, ordinal int, scheme *runtime.Scheme) (*ecv1alpha1.EtcdMember, error) {
	member := &ecv1alpha1.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name:       etcdMemberName(ec.Name, ordinal),
			Namespace:  ec.Namespace,
			Labels:     clusterNameLabels(ec.Name),
			Finalizers: []string{memberCleanupFinalizer},
		},
		Spec: ecv1alpha1.EtcdMemberSpec{
			ClusterName: ec.Name,
			Ordinal:     ordinal,
			Version:     ec.Spec.Version,
		},
	}
	if err := controllerutil.SetControllerReference(ec, member, scheme); err != nil {
		return nil, err
	}
	if err := c.Create(ctx, member); err != nil {
		return nil, err
	}

	// Phase lives on the status subresource, so it has to be written in a
	// separate call after the object exists.
	member.Status.Phase = ecv1alpha1.EtcdMemberPending
	if err := c.Status().Update(ctx, member); err != nil {
		return nil, err
	}
	return member, nil
}
