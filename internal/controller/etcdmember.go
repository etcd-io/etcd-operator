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
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	log.FromContext(ctx).Info("Reconciling an EtcdMember", "EtcdMember", member.Name, "EtcdMemberSpec", member.Spec)
	switch member.Status.Phase {
	// Creating the EtcdMember object and writing its status Phase are two
	// separate calls in createEtcdMember. If the operator crashes in between,
	// the object survives with an empty Phase, which must resume provisioning.
	case "", ecv1alpha1.EtcdMemberPending, ecv1alpha1.EtcdMemberProvisioning:
		return r.reconcileProvisioning(ctx, state, member)
	// Terminating (§4.6): DeletionTimestamp got set the same way regardless
	// of why — scale-in's plain Delete or a human deleting the member
	// directly — and dispatch wrote the Phase before entering here. Clean up
	// the membership and owned resources, end by
	// releasing the finalizer so Kubernetes can finish the deletion.
	case ecv1alpha1.EtcdMemberTerminating:
		return r.cleanupEtcdMember(ctx, state, member)
	// placeholder for Recreating and Replacing
	default:
		return ctrl.Result{}, nil
	}
}

// markMemberTerminating persists Phase=Terminating on a member whose
// deletion has started, if not already written. Idempotent across repeated
// dispatch attempts.
func (r *EtcdClusterReconciler) markMemberTerminating(ctx context.Context, member *ecv1alpha1.EtcdMember) error {
	if member.Status.Phase == ecv1alpha1.EtcdMemberTerminating {
		return nil
	}
	return r.updateEtcdMemberStatus(ctx, member, func(status *ecv1alpha1.EtcdMemberStatus) {
		status.Phase = ecv1alpha1.EtcdMemberTerminating
	})
}

// cleanupEtcdMember is the §4.6 Terminating leave for one member: remove it
// from etcd's live membership, delete its owned Pod and PVC, and finally
// release memberCleanupFinalizer so Kubernetes can finish the deletion.
// Every step is a no-op once done, so re-entering after an interruption
// (operator restart, transient etcd error) resumes harmlessly.
//
// Leadership transfer is intentionally not done here. When etcd's own
// MemberRemove drops this member from the cluster's Membership, the
// member's etcd server is shut down gracefully by the cluster on its
// own, and that graceful shutdown actively hands leadership to a
// remaining peer before it stops — so the implicit transfer is fast
// enough for the simple case. This does not, however, let us pick a
// specific transferee or surface a transfer failure as a distinct
// status; both are reasons §4.6 step 1 calls for an explicit,
// best-effort MoveLeader ahead of RemoveMember, which should be added
// in a follow-up (failure must not stop the sequence).
func (r *EtcdClusterReconciler) cleanupEtcdMember(ctx context.Context, s *reconcileState, member *ecv1alpha1.EtcdMember) (ctrl.Result, error) {
	if err := removeEtcNode(s, member); err != nil {
		return ctrl.Result{}, err
	}

	if err := cleanupMemberResources(ctx, r.Client, s, member); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.clearMemberFinalizer(ctx, member); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// removeEtcNode removes the member from etcd's live membership,
// identified by matching the reconcile snapshot's MemberList against the
// member's deterministic peer URL (never Status.MemberID). No-op when the
// membership snapshot is absent (etcd unreachable — the #463 recovery path)
// or when the member no longer appears in the membership (already removed).
func removeEtcNode(s *reconcileState, member *ecv1alpha1.EtcdMember) error {
	if s.memberListResp == nil {
		return nil
	}

	_, peerURL := peerEndpointForOrdinalIndex(s.cluster, member.Spec.Ordinal)
	var nodeID uint64
	for _, m := range s.memberListResp.Members {
		if slices.Contains(m.PeerURLs, peerURL) {
			nodeID = m.ID
			break
		}
	}
	if nodeID == 0 {
		return nil // already removed from the membership
	}

	endpoints := clientEndpointsFromPods(s.cluster.Name, s.cluster.Namespace, s.pods, clusterTLSEnabled(s.cluster))
	cfg := etcdutils.ClientConfig{Endpoints: endpoints, TLS: s.tlsConfig}
	return etcdutils.RemoveMember(cfg, nodeID)
}

// cleanupMemberResources deletes the member's owned Pod and PVC. The Pod is
// found in this reconcile's snapshot; the PVC is fetched live by its
// deterministic name. Kubernetes' pvc-protection finalizer holds the PVC
// until the Pod is actually gone, so deleting both in one pass is safe.
// Either resource already being gone is not an error.
func cleanupMemberResources(ctx context.Context, c client.Client, s *reconcileState, member *ecv1alpha1.EtcdMember) error {
	podName := memberPodName(s.cluster.Name, member.Spec.Ordinal)
	pvcName := pvcNameForMember(podName)

	for _, pod := range s.pods {
		if pod.Name != podName {
			continue
		}
		if err := c.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting Pod for EtcdMember %q: %w", member.Name, err)
		}
		break
	}

	pvc := &corev1.PersistentVolumeClaim{}
	switch err := c.Get(ctx, types.NamespacedName{Namespace: s.cluster.Namespace, Name: pvcName}, pvc); {
	case err == nil:
		if delErr := c.Delete(ctx, pvc); delErr != nil && !apierrors.IsNotFound(delErr) {
			return fmt.Errorf("deleting PVC for EtcdMember %q: %w", member.Name, delErr)
		}
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("getting PVC for EtcdMember %q: %w", member.Name, err)
	}

	return nil
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
			if err := r.addLearner(ctx, state, member); err != nil {
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
	log.FromContext(ctx).Info("Marking EtcdMember Ready", "EtcdMember", member.Name)
	if err := r.updateEtcdMemberStatus(ctx, member, func(status *ecv1alpha1.EtcdMemberStatus) {
		status.Phase = ecv1alpha1.EtcdMemberReady
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
func (r *EtcdClusterReconciler) addLearner(ctx context.Context, state *reconcileState, member *ecv1alpha1.EtcdMember) error {
	endpoints := clientEndpointsFromPods(state.cluster.Name, state.cluster.Namespace, state.pods, clusterTLSEnabled(state.cluster))
	if len(endpoints) == 0 {
		return fmt.Errorf("cannot add learner for EtcdMember %q: no live client endpoints", member.Name)
	}

	_, peerURL := peerEndpointForOrdinalIndex(state.cluster, member.Spec.Ordinal)
	log.FromContext(ctx).Info("Adding learner for EtcdMember", "EtcdMember", member.Name, "peerURL", peerURL)
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
	log.FromContext(ctx).Info("Promoting learner for EtcdMember", "EtcdMember", member.Name)
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
	if findPodForEtcdMember(state, member) != nil {
		return nil
	}

	initialClusterState := etcdClusterStateExisting
	if bootstrap {
		initialClusterState = etcdClusterStateNew
	}

	if err := createMemberPod(
		ctx,
		log.FromContext(ctx),
		r.Client,
		state.cluster,
		member,
		initialClusterState,
		initialClusterForPod(state, member, bootstrap),
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

// findLeaderName returns the pod name of the current etcd leader by scanning healthInfo
func findLeaderName(healthInfo map[string]etcdutils.EpHealth) string {
	for name, h := range healthInfo {
		if h.Status != nil && h.Status.Header.MemberId == h.Status.Leader {
			return name
		}
	}
	return ""
}

// pickMemberToUpgrade selects the next member to upgrade to targetVersion.
// It iterates from highest to lowest ordinal and prefers non-leader members first,
// leaving the leader member for last.
// Returns nil if all members are already at targetVersion.
func pickMemberToUpgrade(members []ecv1alpha1.EtcdMember, healthInfo map[string]etcdutils.EpHealth, targetVersion string) *ecv1alpha1.EtcdMember {
	// Resolve the leader's pod name once, outside the loop.
	leaderName := findLeaderName(healthInfo)

	var leaderCandidate *ecv1alpha1.EtcdMember

	// members is sorted in ascending ordinal order. Iterate in reverse (highest ordinal first).
	for i := len(members) - 1; i >= 0; i-- {
		m := &members[i]
		if m.Spec.Version == targetVersion {
			continue
		}
		if leaderName != "" && m.Name == leaderName {
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
