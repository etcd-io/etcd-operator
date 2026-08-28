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
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	etcdversions "go.etcd.io/etcd/api/v3/version"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	requeueDuration = 10 * time.Second

	// clusterCleanupFinalizer blocks an EtcdCluster's deletion until
	// finalizeCluster has removed every EtcdMember it owns, mirroring
	// memberCleanupFinalizer (design doc §4.3) one level up. Without it, the
	// EtcdCluster (which the reconciler does not otherwise finalize) would
	// disappear immediately on delete, and the controller would never see it
	// again to clean up its members — Reconcile's first step is Get(cluster),
	// which short-circuits on NotFound before ever inspecting owned
	// EtcdMembers (see fetchAndValidateState).
	clusterCleanupFinalizer = "operator.etcd.io/cluster-cleanup"
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      events.EventRecorder
	ImageRegistry string
}

// reconcileState holds all transient data for a single reconciliation loop.
type reconcileState struct {
	cluster        *ecv1alpha1.EtcdCluster      // cluster CR being reconciled
	members        []ecv1alpha1.EtcdMember      // EtcdMembers owned by this cluster, sorted by ordinal
	pods           []*corev1.Pod                // member pods owned by this cluster, sorted by ordinal
	memberListResp *clientv3.MemberListResponse // member list fetched from the etcd cluster
	health         *etcdutils.ClusterHealth     // cluster/member health and active alarms from the latest health check
	tlsConfig      *tls.Config                  // etcd client TLS config used by every etcdutils call in this loop (nil for non-TLS clusters)
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdmembers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdmembers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdmembers/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;get;list;update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;patch;update;delete
// +kubebuilder:rbac:groups="cert-manager.io",resources=certificates,verbs=get;list;watch;create;patch;update;delete
// +kubebuilder:rbac:groups="cert-manager.io",resources=clusterissuers,verbs=get;list;watch
// +kubebuilder:rbac:groups="cert-manager.io",resources=issuers,verbs=get;list;watch

// Reconcile orchestrates a single reconciliation cycle for an EtcdCluster.
//
// EtcdClusterReconciler reconciles both EtcdCluster and its owned EtcdMember
// objects — there is no second controller (design doc §4.1). Each loop:
//
//  1. Fetch: get the EtcdCluster resource, its owned EtcdMembers and Pods.
//  2. Finalizer EtcdCluster:
//     finalize a cluster with DeletionTimestamp set, or else ensure
//     clusterCleanupFinalizer is present. Checked ahead of
//     Validation/Cluster prerequisites so a paused or spec-invalid cluster
//     can still be deleted.
//  3. Validation: validate EtcdCluster.Spec.
//  4. Cluster prerequisites: certs, TLS config, headless Service — always,
//     independent of anything below.
//  5. Always-on refresh (§4.2): live member list/health, never gated.
//  6. Pause (§4.9 item 1, requirement 15): skip dispatch entirely for this
//     loop. Checked here, right after the always-on refresh and ahead of
//     dispatch, rather than literally ahead of Validation/Cluster
//     prerequisites like reconcile_loop_v0.3.0.png's "Pause Reconciliation"
//     box draws it — the refresh (and buildReconcileClientTLS's TLS config
//     it depends on, built during Cluster prerequisites) must keep running
//     while paused so status doesn't go stale (requirement 15), so pause
//     can't gate those two phases without breaking that guarantee.
//  7. Dispatch: §4.9's priority order (items 2-9) picks at most one
//     mutating action — lost-quorum recovery, Terminating cleanup,
//     CORRUPT/NOSPACE remediation, per-member repair, promote/advance-not-ready,
//     and finally (only once every existing member is Ready)
//     update-config/scale/upgrade.
//
// See docs/design/etcd-member-lifecycle-and-self-healing-v0.3.0.md and
// reconcile_loop_v0.3.0.png for the full workflow this implements. Several
// dispatch branches are intentionally TODO no-ops in this milestone (M2) —
// per-member repair, CORRUPT/NOSPACE remediation, lost-quorum recovery, and
// the EtcdMember/EtcdClusterStatus status roll-up — resolved by M3-M6.
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var (
		state *reconcileState
		res   ctrl.Result
		err   error
	)

	defer func() {
		// Skip once the cluster is being deleted: finalizeCluster may have
		// just removed its last finalizer, in which case it's already gone
		// and a status Update would only fail with NotFound.
		if state != nil && state.cluster.DeletionTimestamp == nil {
			if statusErr := r.updateStatus(ctx, state); statusErr != nil {
				log.FromContext(ctx).Error(statusErr, "Failed to update status")
			}
		}
	}()

	// 1. Fetch: get the EtcdCluster resource and its owned EtcdMembers/Pods.
	state, res, err = r.fetchAndValidateState(ctx, req)
	if state == nil || err != nil {
		return res, err
	}
	log.FromContext(ctx).Info("Reconciling EtcdCluster", "spec", state.cluster.Spec)

	// 2. Finalizer EtcdCluster:
	// checked ahead of Paused/validation/prereqs so a paused or spec-invalid
	// cluster can still be deleted.
	if state.cluster.DeletionTimestamp != nil {
		return r.finalizeCluster(ctx, state)
	}
	if err = r.ensureClusterFinalizer(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Validation: validate EtcdCluster.Spec.
	if err = r.validateSpec(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Cluster prerequisites: certs, TLS config, headless Service.
	if err = r.ensureClusterPrereqs(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	// 5. Always-on refresh: live member list/health (§4.2) — never gated.
	r.refreshClusterState(ctx, state)

	// 6. Pause (requirement 15): do nothing else at all this loop, ahead of
	// dispatch (including an already-started lost-quorum recovery).
	if state.cluster.Spec.Paused {
		log.FromContext(ctx).Info("EtcdCluster is paused; skipping all mutating actions")
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 7. Dispatch: §4.9's priority order (items 2-9).
	return r.dispatch(ctx, state)
}

// fetchAndValidateState retrieves the EtcdCluster and lists the EtcdMembers
// and Pods it owns.
func (r *EtcdClusterReconciler) fetchAndValidateState(ctx context.Context, req ctrl.Request) (*reconcileState, ctrl.Result, error) {
	logger := log.FromContext(ctx)

	ec := &ecv1alpha1.EtcdCluster{}
	if err := r.Get(ctx, req.NamespacedName, ec); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("EtcdCluster resource not found. Ignoring since object may have been deleted")
			return nil, ctrl.Result{}, nil
		}
		return nil, ctrl.Result{}, err
	}

	// Apply operator-side defaults so downstream phases (hash, pod builder,
	// cert wiring) all see consistent values regardless of which fields the
	// user left empty.
	r.PopulateDefaultValues(ec)

	members, err := listOwnedMembers(ctx, r.Client, ec)
	if err != nil {
		logger.Error(err, "Failed to list EtcdMembers. Requesting requeue")
		return nil, ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	pods, err := listOwnedPods(ctx, r.Client, ec, members)
	if err != nil {
		logger.Error(err, "Failed to list pods. Requesting requeue")
		return nil, ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	return &reconcileState{cluster: ec, members: members, pods: pods}, ctrl.Result{}, nil
}

// PopulateDefaultValues mutates `ec` in place to apply operator-side defaults
// for any EtcdClusterSpec field that the user left empty. Defaults are sourced
// from the reconciler instance (e.g. its ImageRegistry), keeping the reconcile
// loop the single owner of how a partially-specified Spec becomes a fully
// resolved one.
func (r *EtcdClusterReconciler) PopulateDefaultValues(ec *ecv1alpha1.EtcdCluster) {
	if ec.Spec.ImageRegistry == "" {
		ec.Spec.ImageRegistry = r.ImageRegistry
	}
}

// buildReconcileClientTLS builds the operator's etcd-client TLS Config for a
// reconcile loop from the cluster's server certificate Secret. Returns a nil
// config (and nil error) when the cluster has no TLS configured.
func (r *EtcdClusterReconciler) buildReconcileClientTLS(ctx context.Context, ec *ecv1alpha1.EtcdCluster) (*tls.Config, error) {
	if ec.Spec.TLS == nil {
		return nil, nil
	}
	return buildClientTLSConfig(ctx, ec, r.Client)
}

// validateSpec validates that all parameters in EtcdCluster.Spec are valid.
//
// TODO: per the workflow diagram's "Validation" phase, this may end up being
// done via an admission webhook instead of here. Add any further spec
// validation as a new check below.
func (r *EtcdClusterReconciler) validateSpec(ctx context.Context, s *reconcileState) error {
	logger := log.FromContext(ctx)

	// Validate the upgrade path using the image tag of the first pod.
	if len(s.pods) > 0 {
		for _, c := range s.pods[0].Spec.Containers {
			if c.Name != "etcd" {
				continue
			}
			idx := strings.LastIndex(c.Image, ":")
			if idx == -1 {
				logger.Info("could not extract image version from pod image",
					"image", c.Image)
				return nil
			}
			currentVersion := c.Image[idx+1:]
			targetVersion := s.cluster.Spec.Version

			if currentVersion != targetVersion {
				canParse, err := validateEtcdUpgradePath(etcdversions.AllVersions, currentVersion, targetVersion)
				if !canParse {
					logger.Info("error when parsing reconcile versions; it is your responsibility "+
						"to validate if the upgrade path is supported",
						"current", currentVersion,
						"target", targetVersion,
						"error", err,
					)
					return nil
				}
				if err != nil {
					logger.Error(err, "unsupported upgrade path between current and target versions",
						"current", currentVersion,
						"target", targetVersion,
					)
					return err
				}
				logger.Info("upgrade path between current and target versions is supported",
					"current", currentVersion,
					"target", targetVersion)
			}
			break
		}
	}

	return nil
}

// ensureClusterPrereqs generates the client/server/peer certificates (if
// enabled), builds the operator's etcd-client TLS config from the server
// certificate, and ensures the headless Service exists. Runs unconditionally
// every reconcile, the same way the always-on health refresh does (§4.2) —
// it isn't a "policy" phase and so is never gated by member readiness.
func (r *EtcdClusterReconciler) ensureClusterPrereqs(ctx context.Context, s *reconcileState) error {
	logger := log.FromContext(ctx)

	if s.cluster.Spec.TLS != nil {
		if err := createClientCertificate(ctx, s.cluster, r.Client); err != nil {
			logger.Error(err, "Failed to create Client Certificate.")
		}
		// Server/peer certs must exist before buildReconcileClientTLS below reads
		// the server cert Secret. reconcileProvisioning also calls this
		// (idempotently) before creating a member's Pod.
		// TODO: buildReconcileClientTLS reuses this member server
		// certificate as the operator's own client identity (see its comment
		// in utils.go) — that's the wrong cert for the operator to depend on;
		// server/peer certs are a member concern. Issue the operator a
		// dedicated client certificate instead, so this phase doesn't need to
		// reach into member-owned certificate state at all.
		if err := applyEtcdMemberCerts(ctx, s.cluster, r.Client); err != nil {
			logger.Error(err, "Failed to create server/peer certificates.")
		}
	} else {
		logger.Info(fmt.Sprintf(
			"missing TLS config for %s,\n running etcd-cluster without TLS protection is NOT recommended for production.",
			s.cluster.Name,
		))
	}

	// Build the operator's etcd-client TLS config (from the server cert Secret).
	// When TLS is unused this stays nil.
	clientTLS, tlsErr := r.buildReconcileClientTLS(ctx, s.cluster)
	if tlsErr != nil {
		logger.Error(tlsErr, "Failed to build client TLS config; will retry next reconcile")
		return tlsErr
	}
	s.tlsConfig = clientTLS

	// Service must exist before pods start so that headless DNS resolves.
	return createHeadlessServiceIfNotExist(ctx, logger, r.Client, s.cluster, r.Scheme)
}

// refreshClusterState fetches the live etcd member list and cluster/member
// health and stores them on reconcileState for the dispatcher below. Always
// runs, never gated by member readiness (§4.2).
func (r *EtcdClusterReconciler) refreshClusterState(ctx context.Context, s *reconcileState) {
	logger := log.FromContext(ctx)
	logger.Info("Now checking health of the cluster members")

	var err error
	s.memberListResp, s.health, err = healthCheck(s.cluster.Name, s.cluster.Namespace, s.pods, clusterTLSEnabled(s.cluster), s.tlsConfig, logger)
	if err != nil {
		logger.Info("health check found errors", "errors", err)
	}

	if s.health != nil {
		logger.Info("Cluster health check complete", "clusterHealthy", s.health.Healthy)

		var unhealthyEndpoints []string
		for _, m := range s.health.Members {
			if !m.Health {
				unhealthyEndpoints = append(unhealthyEndpoints, m.Ep)
			}
		}
		if len(unhealthyEndpoints) > 0 {
			logger.Info("Found one or more unhealthy members", "unhealthyEndpoints", unhealthyEndpoints)
		}

		if len(s.health.Alarms) > 0 {
			logger.Info("Found active etcd alarms", "alarms", s.health.Alarms)
		}
	}
}

// dispatch implements design doc §4.9's priority order, items 2-9:
// at most one mutating action per reconcile. Each numbered step either
// claims the loop (returns a non-zero Result or an error) or falls through
// to the next; only an item that actually takes a mutating action ends the
// loop.
//
// Steps 4-7 are TODO no-ops in this milestone (M2) — the mechanics they'd
// trigger (join/promote/repair/leave, CORRUPT/NOSPACE remediation,
// lost-quorum recovery) land in M3-M5 — but the detection that decides
// *whether* a step claims the loop is real, so the priority order itself is
// reviewable now, ahead of any of that behavior landing.
// Item 8 is partially implemented: the provisioning case reconciles one
// EtcdMember at a time until it is Ready; the replacing case (§4.9 item 8's
// second bullet) lands in a follow-up PR.
func (r *EtcdClusterReconciler) dispatch(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 2. Continue an already-started lost-quorum recovery (§4.8).
	// We should check whether the cluster is healthy first. If yes,
	// cleanup the Status.QuorumRecovery; if no, then check whether
	// it's an already-started lost-quorum recovery, and continue to
	// do it if present.
	if s.cluster.Status.QuorumRecovery != nil {
		// TODO: §4.8/§4.9 item 2 — force-new-cluster the survivor,
		// terminate the rest, let scale-out rebuild (M5).
		logger.Info("Lost-quorum recovery in progress; continuation not implemented yet",
			"survivor", s.cluster.Status.QuorumRecovery.Survivor)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// 3. Clean up any EtcdMember already Terminating (§4.6/§4.9 item 3,
	// reconcile_loop_v0.3.0.png). Ranked here — above CORRUPT/NOSPACE/
	// per-member repair — because a member that's already leaving (from
	// scale-in, or a user manually removing it) is a settled matter: it's
	// never a repair candidate, only ever touches its own Pod/PVC/cert/
	// membership state, and shouldn't linger behind other remediation.
	for i := range s.members {
		m := &s.members[i]
		if m.DeletionTimestamp != nil {
			// TODO: §4.6's real six-step leave sequence (M3); clearing the
			// finalizer directly is an interim workaround.
			logger.Info("Member is Terminating; leave sequence not implemented yet, "+
				"clearing finalizer as an interim workaround", "member", m.Name)
			if err := r.clearMemberFinalizer(ctx, m); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	}

	// 4. CORRUPT alarm on some member (§4.6/§4.7).
	// TODO: §4.9 item 4 — force the tagged member to Phase: Replacing
	// (M4). s.health.Alarms (refreshClusterState) now carries active alarms;
	// this step just doesn't act on them yet.

	// 5. NOSPACE alarm remediation (§4.7).
	// TODO: §4.9 item 5 — compact/defragment/disarm cycle (M4). Same
	// as above, s.health.Alarms already has the data.

	// 6. Per-member repair: continue a member already Recreating, or start
	// fixing exactly one newly-unhealthy Ready member (requirement 6).
	for _, m := range s.members {
		if m.Status.Phase == ecv1alpha1.EtcdMemberRecreating {
			// TODO: §4.9 item 6 — continue the shared Pod-recovery
			// ladder (§4.6, M3).
			logger.Info("Member is Recreating; per-member repair not implemented yet", "member", m.Name)
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	}
	// TODO: §4.9 item 6 — picking a newly-unhealthy Ready member to
	// start Recreating needs the live-health-to-EtcdMember mapping M3/M6
	// add; not wired up yet, so this half of the step never fires either.

	// 7. Decide whether to *start* lost-quorum recovery (§4.8/§4.9 item 7).
	// TODO: opt-in, and only once steps 3-6 above find nothing to do
	// and the cluster is still unhealthy (M5). s.health.Healthy
	// (refreshClusterState) can go false from a single failed
	// MemberList/AlarmList call (healthCheck, pods.go) — a transient blip,
	// not necessarily a real lost quorum — so this step must debounce (e.g.
	// require the cluster to stay unhealthy for some minimum duration or
	// number of consecutive reconciles) before declaring quorum lost,
	// rather than triggering recovery off the first unhealthy reading.

	// 8. Advance whatever's left not-Ready (Pending/Provisioning/Replacing).
	// An existing learner always wins this slot (requirement 11); with more
	// than one not-ready member but no learner, the lowest ordinal wins.
	if notReady := pickNotReadyMember(s); notReady != nil {
		return r.reconcileEtcdMember(ctx, s, notReady)
	}

	// 9. Everything existing is Ready (§4.2's gate), so update-config/scale/upgrade
	// only ever run once every member is Ready.
	if res, err := r.updateConfig(ctx, s); err != nil || !res.IsZero() {
		return res, err
	}

	if res, err := r.scaleCluster(ctx, s); err != nil || !res.IsZero() {
		return res, err
	}

	return r.upgradeCluster(ctx, s)
}

// ensureClusterFinalizer adds clusterCleanupFinalizer to s.cluster if it
// isn't already present.
func (r *EtcdClusterReconciler) ensureClusterFinalizer(ctx context.Context, s *reconcileState) error {
	if !controllerutil.AddFinalizer(s.cluster, clusterCleanupFinalizer) {
		return nil
	}
	return r.Update(ctx, s.cluster)
}

// finalizeCluster handles an EtcdCluster with DeletionTimestamp set: it
// removes every EtcdMember it still owns — reusing the same interim
// finalizer-clearing dispatch()'s Terminating-cleanup step uses for a
// single Terminating member — and, once none remain, releases
// clusterCleanupFinalizer so Kubernetes can finish deleting the EtcdCluster
// itself.
//
// TODO: like that step, this skips the real six-step leave sequence (§4.6,
// M3): members are removed without ever calling etcd's MemberRemove, so
// deleting a cluster with live members can leave stale entries in etcd's
// own membership list. Acceptable for now because the Pods backing those
// members are torn down along with everything else, but M3 should replace
// this loop with the real sequence rather than just deleting faster.
func (r *EtcdClusterReconciler) finalizeCluster(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(s.members) == 0 {
		if controllerutil.RemoveFinalizer(s.cluster, clusterCleanupFinalizer) {
			if err := r.Update(ctx, s.cluster); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	logger.Info("EtcdCluster is Terminating; removing owned EtcdMembers", "remaining", len(s.members))
	for i := range s.members {
		m := &s.members[i]
		if m.DeletionTimestamp == nil {
			if err := r.Delete(ctx, m); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			continue
		}
		if err := r.clearMemberFinalizer(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// clearMemberFinalizer removes memberCleanupFinalizer from m, letting
// Kubernetes finish deleting it. Shared by dispatch()'s Terminating-cleanup
// step (a single Terminating member, cluster otherwise alive) and
// finalizeCluster (every member, cluster itself being deleted).
func (r *EtcdClusterReconciler) clearMemberFinalizer(ctx context.Context, m *ecv1alpha1.EtcdMember) error {
	if !controllerutil.RemoveFinalizer(m, memberCleanupFinalizer) {
		return nil
	}
	return r.Update(ctx, m)
}

// updateConfig compares each member's running configuration against
// EtcdCluster.Spec and recreates the first Pod whose config has drifted.
//
// TODO: §4.6's Pod-recovery ladder (its config-drift branch) covers
// this once M3 lands; recreating one member at a time, highest ordinal
// first, transferring leadership first if needed (§4.5).
func (r *EtcdClusterReconciler) updateConfig(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// scaleCluster grows or shrinks the cluster by one member at a time towards
// EtcdCluster.Spec.Size, picking the ordinal via §4.4's nextOrdinal (reuse
// the lowest gap, else max+1) and always removing the highest ordinal first
// on scale-in (requirement 2).
//
// Scale-out only creates one Pending EtcdMember. The shared member lifecycle
// owns bootstrap, member addition, Pod creation, and learner promotion. Scale-in
// deletes the EtcdMember directly; the finalizer (§4.3) blocks its actual
// removal until the Terminating leave sequence (§4.6, M3) runs.
func (r *EtcdClusterReconciler) scaleCluster(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	currentCount := len(s.members)
	desiredSize := s.cluster.Spec.Size

	if currentCount == desiredSize {
		logger.Info("EtcdCluster is at desired size", "size", desiredSize)
		return ctrl.Result{}, nil
	}

	if currentCount < desiredSize {
		ordinal := nextOrdinal(memberOrdinals(s.members))
		logger.Info("[Scale out] creating a new EtcdMember", "ordinal", ordinal)
		if _, err := createEtcdMember(ctx, r.Client, s.cluster, ordinal, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Scale in: always the highest ordinal (requirement 2).
	highest := s.members[len(s.members)-1]
	logger.Info("[Scale in] deleting the highest-ordinal EtcdMember", "member", highest.Name)
	if err := r.Delete(ctx, &highest); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// upgradeCluster rolls one member to the etcd version requested in
// EtcdCluster.Spec.Version, highest ordinal first.
//
// TODO: not implemented yet. validateSpec already checks, by comparing
// the first Pod's image tag against EtcdCluster.Spec.Version, whether the
// upgrade path is supported when the two differ — but it doesn't persist
// that comparison anywhere on reconcileState. This phase needs to redo the
// same current-vs-target version comparison, then bump one EtcdMember.Spec.Version
// at a time (highest ordinal, non-leader first as a preference); the member
// notices the drift and recreates via §4.6's ladder (M3).
func (r *EtcdClusterReconciler) upgradeCluster(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// updateStatus reflects the current observed state onto EtcdCluster.Status.
//
// TODO: §4.11 (M6) rewrites this to write EtcdMember.Status and
// EtcdClusterStatus.Members from the same live snapshot in one pass, and to
// skip the write when nothing changed. For now this keeps computing
// EtcdClusterStatus directly from live Pods/member-list, as it did before
// EtcdMember existed.
func (r *EtcdClusterReconciler) updateStatus(ctx context.Context, s *reconcileState) error {
	logger := log.FromContext(ctx)

	s.cluster.Status.ObservedGeneration = s.cluster.Generation

	// Pod counts.
	s.cluster.Status.CurrentReplicas = int32(len(s.pods))
	readyCount := int32(0)
	for _, pod := range s.pods {
		if isPodReady(pod) {
			readyCount++
		}
	}
	s.cluster.Status.ReadyReplicas = readyCount

	// etcd membership.
	if s.memberListResp != nil {
		var memberHealth map[string]etcdutils.EpHealth
		if s.health != nil {
			memberHealth = s.health.Members
		}

		s.cluster.Status.MemberCount = int32(len(s.memberListResp.Members))

		s.cluster.Status.Members = make([]ecv1alpha1.MemberStatus, 0, len(s.memberListResp.Members))
		for _, member := range s.memberListResp.Members {
			memberStatus := ecv1alpha1.MemberStatus{
				ID:   fmt.Sprintf("%x", member.ID),
				Name: member.Name,
			}
			if health, ok := memberHealth[member.Name]; ok {
				memberStatus.IsHealthy = health.Health
				if health.Status != nil {
					memberStatus.Version = health.Status.Version
					memberStatus.IsLeader = health.Status.Header.MemberId == health.Status.Leader
				}
			}
			memberStatus.IsLearner = member.IsLearner
			s.cluster.Status.Members = append(s.cluster.Status.Members, memberStatus)
		}

		_, leaderStatus := etcdutils.FindLeaderStatus(memberHealth, logger)
		if leaderStatus != nil {
			s.cluster.Status.LeaderID = fmt.Sprintf("%x", leaderStatus.Leader)
			s.cluster.Status.CurrentVersion = leaderStatus.Version
		} else {
			for _, health := range memberHealth {
				if health.Status != nil {
					s.cluster.Status.CurrentVersion = health.Status.Version
					break
				}
			}
		}
	}

	r.updateConditions(s)

	if err := r.Status().Update(ctx, s.cluster); err != nil {
		logger.Error(err, "Failed to update EtcdCluster status")
		return err
	}
	return nil
}

// updateConditions sets the standard Kubernetes conditions based on observed state.
func (r *EtcdClusterReconciler) updateConditions(s *reconcileState) {
	now := metav1.Now()

	var memberHealth map[string]etcdutils.EpHealth
	if s.health != nil {
		memberHealth = s.health.Members
	}

	availableCondition := metav1.Condition{
		Type:               "Available",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: s.cluster.Generation,
		LastTransitionTime: now,
		Reason:             "ClusterNotReady",
		Message:            "Etcd cluster is not yet available",
	}

	if s.memberListResp != nil && len(s.memberListResp.Members) > 0 {
		healthyCount := 0
		for _, health := range memberHealth {
			if health.Health {
				healthyCount++
			}
		}
		quorum := (len(s.memberListResp.Members) / 2) + 1
		if healthyCount >= quorum {
			availableCondition.Status = metav1.ConditionTrue
			availableCondition.Reason = "ClusterAvailable"
			availableCondition.Message = fmt.Sprintf("Etcd cluster has %d/%d healthy members with quorum",
				healthyCount, len(s.memberListResp.Members))
		} else {
			availableCondition.Message = fmt.Sprintf(
				"Etcd cluster has %d/%d healthy members, quorum requires %d",
				healthyCount, len(s.memberListResp.Members), quorum)
		}
	}

	progressingCondition := metav1.Condition{
		Type:               "Progressing",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: s.cluster.Generation,
		LastTransitionTime: now,
		Reason:             "ClusterStable",
		Message:            "Etcd cluster is stable",
	}

	currentPodCount := int32(len(s.pods))
	desiredSize := int32(s.cluster.Spec.Size)

	if currentPodCount != desiredSize {
		progressingCondition.Status = metav1.ConditionTrue
		progressingCondition.Reason = "ScalingInProgress"
		progressingCondition.Message = fmt.Sprintf("Scaling from %d to %d pods", currentPodCount, desiredSize)
	} else if s.memberListResp != nil && int32(len(s.memberListResp.Members)) != desiredSize {
		progressingCondition.Status = metav1.ConditionTrue
		progressingCondition.Reason = "MembershipChanging"
		progressingCondition.Message = fmt.Sprintf("Etcd membership changing: %d members, target %d",
			len(s.memberListResp.Members), desiredSize)
	}

	if s.memberListResp != nil {
		for _, member := range s.memberListResp.Members {
			if member.IsLearner {
				progressingCondition.Status = metav1.ConditionTrue
				progressingCondition.Reason = "LearnerPromotion"
				progressingCondition.Message = "Waiting for learner member to be promoted"
				break
			}
		}
	}

	degradedCondition := metav1.Condition{
		Type:               "Degraded",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: s.cluster.Generation,
		LastTransitionTime: now,
		Reason:             "ClusterHealthy",
		Message:            "All etcd members are healthy",
	}

	if s.memberListResp != nil && len(memberHealth) > 0 {
		var unhealthyMembers []string
		for _, health := range memberHealth {
			if !health.Health {
				unhealthyMembers = append(unhealthyMembers, health.Ep)
			}
		}
		if len(unhealthyMembers) > 0 {
			degradedCondition.Status = metav1.ConditionTrue
			degradedCondition.Reason = "UnhealthyMembers"
			degradedCondition.Message = fmt.Sprintf("Unhealthy members: %s", strings.Join(unhealthyMembers, ", "))
		}
	}

	meta.SetStatusCondition(&s.cluster.Status.Conditions, availableCondition)
	meta.SetStatusCondition(&s.cluster.Status.Conditions, progressingCondition)
	meta.SetStatusCondition(&s.cluster.Status.Conditions, degradedCondition)
}

// isCertManagerCRDPresent checks if cert-manager CRDs are installed in the cluster.
func isCertManagerCRDPresent(mgr ctrl.Manager) bool {
	gvk := certv1.SchemeGroupVersion.WithKind("Certificate")
	_, err := mgr.GetRESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	return err == nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorder("etcdcluster-controller")
	setupLog := ctrl.Log.WithName("setup")

	builder := ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdCluster{}).
		Owns(&ecv1alpha1.EtcdMember{}).
		Owns(&corev1.Service{})

	if isCertManagerCRDPresent(mgr) {
		builder = builder.Owns(&certv1.Certificate{})
		setupLog.Info("cert-manager CRDs detected, enabling Certificate watches")
	} else {
		setupLog.Info("cert-manager CRDs not detected, only auto provider will be available. Restart the controller after cert-manager CRDs are installed")
	}

	return builder.Complete(r)
}
