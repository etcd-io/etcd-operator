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
	"slices"
	"sort"
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
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcdversions "go.etcd.io/etcd/api/v3/version"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	requeueDuration = 10 * time.Second
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
	pods           []*corev1.Pod                // member pods owned by this cluster, sorted by ordinal
	memberListResp *clientv3.MemberListResponse // member list fetched from the etcd cluster
	memberHealth   []etcdutils.EpHealth         // health information for each etcd member
	tlsConfig      *tls.Config                  // etcd client TLS config used by every etcdutils call in this loop (nil for non-TLS clusters)
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/finalizers,verbs=update
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
// The loop is organized into the phases below (see reconcile_loop_v0.3.0.png
// for the full workflow diagram this mirrors). Each phase returns a non-zero
// ctrl.Result or a non-nil error to end the loop early (typically to requeue
// after a single step, e.g. one Pod created/deleted/promoted); otherwise the
// loop falls through to the next phase.
//
//  0. Fetch: get the EtcdCluster resource and its owned Pods.
//  1. Validation: validate EtcdCluster.Spec.
//  2. Bootstrap: ensure the headless Service and first member Pod exist.
//  3. Health check & fix: check member/Pod health, fix unhealthy member/Pod.
//  4. Exception handling: reconcile Pod-count vs etcd member-count drift left
//     over from a previous loop that was interrupted mid-step.
//  5. Promote learner: promote an in-sync learner to a voting member.
//  6. Update config: recreate Pods whose running config no longer matches spec.
//  7. Scale out & in: grow or shrink the cluster towards the desired size.
//  8. Upgrade: roll one Pod to the desired etcd version.
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var (
		state *reconcileState
		res   ctrl.Result
		err   error
	)

	defer func() {
		if state != nil {
			if statusErr := r.updateStatus(ctx, state); statusErr != nil {
				log.FromContext(ctx).Error(statusErr, "Failed to update status")
			}
		}
	}()

	// 0. Fetch: get the EtcdCluster resource and its owned Pods.
	state, res, err = r.fetchAndValidateState(ctx, req)
	if state == nil || err != nil {
		return res, err
	}
	log.FromContext(ctx).Info("Reconciling EtcdCluster", "spec", state.cluster.Spec)

	// 1. Validation: validate EtcdCluster.Spec.
	if err = r.validateSpec(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Bootstrap: ensure the headless Service and first member Pod exist.
	if res, err = r.bootstrapCluster(ctx, state); err != nil || !res.IsZero() {
		return res, err
	}

	// 3. Health check & fix: check member/Pod health, fix unhealthy member/Pod.
	if res, err = r.healthCheckAndFix(ctx, state); err != nil || !res.IsZero() {
		return res, err
	}

	// 4. Exception handling: reconcile Pod-count vs etcd member-count drift.
	if res, err = r.reconcileExceptions(ctx, state); err != nil || !res.IsZero() {
		return res, err
	}

	// 5. Promote learner.
	if res, err = r.promoteLearner(ctx, state); err != nil || !res.IsZero() {
		return res, err
	}

	// 6. Update config: recreate Pods whose running config drifted from spec.
	if res, err = r.updateConfig(ctx, state); err != nil || !res.IsZero() {
		return res, err
	}

	// 7. Scale out & in.
	if res, err = r.scaleCluster(ctx, state); err != nil || !res.IsZero() {
		return res, err
	}

	// 8. Upgrade.
	return r.upgradeCluster(ctx, state)
}

// fetchAndValidateState retrieves the EtcdCluster and lists the Pods it owns.
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

	if ec.Spec.ImageRegistry == "" {
		ec.Spec.ImageRegistry = r.ImageRegistry
	}

	pods, err := listOwnedPods(ctx, r.Client, ec)
	if err != nil {
		logger.Error(err, "Failed to list pods. Requesting requeue")
		return nil, ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	return &reconcileState{cluster: ec, pods: pods}, ctrl.Result{}, nil
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

// bootstrapCluster generates the client/server/peer certificates (if needed),
// builds the operator's etcd-client TLS config from the server certificate,
// ensures the headless Service exists, and, when no pods are present,
// creates the first member pod (ordinal 0) to bootstrap a new cluster. A
// non-zero ctrl.Result requests a requeue so the next loop observes the new
// pod.
func (r *EtcdClusterReconciler) bootstrapCluster(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if s.cluster.Spec.TLS != nil {
		if err := createClientCertificate(ctx, s.cluster, r.Client); err != nil {
			logger.Error(err, "Failed to create Client Certificate.")
		}
		// Server/peer certs must exist before buildReconcileClientTLS below reads
		// the server cert Secret. createMemberPod also calls this (idempotently)
		// once the first pod is created.
		if err := applyEtcdMemberCerts(ctx, s.cluster, r.Client); err != nil {
			logger.Error(err, "Failed to create server/peer certificates.")
		}
	} else {
		logger.Error(nil, fmt.Sprintf(
			"missing TLS config for %s,\n running etcd-cluster without TLS protection is NOT recommended for production.",
			s.cluster.Name,
		))
	}

	// Build the operator's etcd-client TLS config (from the server cert Secret).
	// When TLS is unused this stays nil.
	clientTLS, tlsErr := r.buildReconcileClientTLS(ctx, s.cluster)
	if tlsErr != nil {
		logger.Error(tlsErr, "Failed to build client TLS config; will retry next reconcile")
		return ctrl.Result{}, tlsErr
	}
	s.tlsConfig = clientTLS

	// Service must exist before pods start so that headless DNS resolves.
	if err := createHeadlessServiceIfNotExist(ctx, logger, r.Client, s.cluster, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	if len(s.pods) > 0 {
		return ctrl.Result{}, nil
	}

	logger.Info("No member pods found, creating first member pod", "expectedSize", s.cluster.Spec.Size)
	if err := createMemberPod(ctx, logger, r.Client, s.cluster, 0, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// healthCheckAndFix obtains the member list and health status from the etcd
// cluster and stores them on reconcileState for later phases. When any
// member/Pod is found unhealthy it should attempt a fix.
//
// ctrl.Result is always zero for now, but the fix TODO below will need to
// return a non-zero Result (e.g. to requeue after restarting a Pod) once
// implemented.
//
//nolint:unparam // see comment above
func (r *EtcdClusterReconciler) healthCheckAndFix(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Now checking health of the cluster members")

	var err error
	s.memberListResp, s.memberHealth, err = healthCheck(s.cluster.Name, s.cluster.Namespace, s.pods, clusterTLSEnabled(s.cluster), s.tlsConfig, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("health check failed: %w", err)
	}

	if !areAllMembersHealthy(s.memberHealth) {
		logger.Info("Found one or more unhealthy members")
		// TODO: not implemented yet. Try to fix one unhealthy member/Pod (e.g.
		// restart it, or remove and re-add it to the etcd cluster). etcd only
		// allows one learner at a time, so if the cluster already has a
		// learner, promote or remove that learner first before adding a new
		// one as part of the fix.
	}

	return ctrl.Result{}, nil
}

// reconcileExceptions reconciles any discrepancy between Pod count and etcd
// member count. This can occur when a previous reconcile was interrupted
// between the etcd member API call and the Pod create/delete.
func (r *EtcdClusterReconciler) reconcileExceptions(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	memberCnt := 0
	if s.memberListResp != nil {
		memberCnt = len(s.memberListResp.Members)
	}
	currentPodCount := int32(len(s.pods))

	if int(currentPodCount) == memberCnt {
		return ctrl.Result{}, nil
	}

	logger.Info("Pod count and etcd member count differ",
		"podCount", currentPodCount, "memberCnt", memberCnt)
	if int(currentPodCount) < memberCnt {
		// A member was added to etcd but the pod was not yet created.
		logger.Info("Creating pod for already-registered etcd member")
		ordinals := make([]int, 0, len(s.pods))
		for _, p := range s.pods {
			ordinals = append(ordinals, podOrdinal(p.Name, s.cluster.Name))
		}
		next := nextPodOrdinal(ordinals, s.cluster.Spec.Size)
		logger.Info("Creating a new member pod", "cluster", s.cluster.Name, "ordinal", next)
		if err := createMemberPod(ctx, logger, r.Client, s.cluster, next, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// A member was removed from etcd but the pod was not yet deleted.
		logger.Info("Deleting pod for already-removed etcd member")
		podToRemove := s.pods[len(s.pods)-1]
		if err := r.Delete(ctx, podToRemove); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// promoteLearner promotes an in-sync learner member to a voting member.
func (r *EtcdClusterReconciler) promoteLearner(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	memberCnt := 0
	if s.memberListResp != nil {
		memberCnt = len(s.memberListResp.Members)
	}
	if memberCnt == 0 {
		return ctrl.Result{}, nil
	}

	_, leaderStatus := etcdutils.FindLeaderStatus(s.memberHealth, logger)
	if leaderStatus == nil {
		return ctrl.Result{}, fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
	}

	learner, learnerStatus := etcdutils.FindLearnerStatus(s.memberHealth, logger)
	if learner == 0 {
		return ctrl.Result{}, nil
	}

	logger.Info("Learner found", "learnerID", learner, "learnerStatus", learnerStatus)
	if !etcdutils.IsLearnerReady(leaderStatus, learnerStatus) {
		logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learner)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	logger.Info("Learner is ready to be promoted to voting member", "learnerID", learner)
	eps := clientEndpointsFromPods(s.cluster.Name, s.cluster.Namespace, s.pods, clusterTLSEnabled(s.cluster))
	// Exclude the learner (last ordinal) from the endpoint list used for promotion.
	eps = eps[:(len(eps) - 1)]
	if err := etcdutils.PromoteLearner(etcdutils.ClientConfig{Endpoints: eps, TLS: s.tlsConfig}, learner); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// updateConfig compares each Pod's running configuration against
// EtcdCluster.Spec and recreates the first Pod whose config has drifted.
//
// TODO: not implemented yet. Per the workflow diagram's "Update config"
// phase, this should hash EtcdCluster.Spec, compare it against each Pod's
// recorded config hash, and recreate mismatched Pods one at a time, starting
// with the highest ordinal and working down. If the Pod being replaced is
// the leader, move leadership to another member (the one with the lowest
// ordinal) first.
func (r *EtcdClusterReconciler) updateConfig(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	hash := EtcdClusterHash(s.cluster)

	sort.Slice(s.pods, func(i, j int) bool {
		return strings.Compare(s.pods[i].Name, s.pods[j].Name) < 0
	})
	sort.Slice(s.memberListResp.Members, func(i, j int) bool {
		return strings.Compare(s.memberListResp.Members[i].Name, s.memberListResp.Members[j].Name) < 0
	})

	var outdated *corev1.Pod
	for _, p := range slices.Backward(s.pods) {
		if p.Annotations != nil && p.Annotations[HashMetadataKey] != hash {
			outdated = p
			break
		}
	}
	if outdated == nil {
		logger.Info("no pods with outdated config found")
		return ctrl.Result{}, nil
	}
	logger.Info("a pod with outdated config found", "pod name", outdated.Name)
	leaderId, leaderStatus := etcdutils.FindLeaderStatus(s.memberHealth, logger)
	if leaderStatus == nil {
		return ctrl.Result{}, fmt.Errorf("couldn't find leader for cluster %s", s.cluster.Name)
	}
	var outdatedMember *etcdserverpb.Member
	for _, m := range s.memberListResp.Members {
		if m.Name == outdated.Name {
			outdatedMember = m
			break
		}
	}
	if outdatedMember.ID == leaderId {
		// move the leader
		var moveTo uint64
		for _, m := range s.memberListResp.Members {
			if m.ID != leaderId {
				moveTo = m.ID
			}
		}
		eps := clientEndpointsFromPods(s.cluster.Name, s.cluster.Namespace, s.pods, clusterTLSEnabled(s.cluster))
		etcdutils.MoveLeader(etcdutils.ClientConfig{Endpoints: eps, TLS: s.tlsConfig}, moveTo)
	}
	if err := r.Delete(ctx, outdated); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// scaleCluster compares the desired cluster size with the observed Pod count
// and grows or shrinks the cluster by one member/Pod at a time towards it.
func (r *EtcdClusterReconciler) scaleCluster(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	currentPodCount := int32(len(s.pods))
	desiredSize := int32(s.cluster.Spec.Size)

	if currentPodCount == desiredSize {
		// Ensure every member is healthy before declaring success.
		if !areAllMembersHealthy(s.memberHealth) {
			logger.Info("EtcdCluster is at desired size but not all members are healthy yet")
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
		logger.Info("EtcdCluster reconciled successfully")
		return ctrl.Result{}, nil
	}

	eps := clientEndpointsFromPods(s.cluster.Name, s.cluster.Namespace, s.pods, clusterTLSEnabled(s.cluster))

	if currentPodCount < desiredSize {
		// Scale out: add a new learner member to etcd, then create its pod.
		nextOrdinal := int(currentPodCount)
		_, peerURL := peerEndpointForOrdinalIndex(s.cluster, nextOrdinal)
		logger.Info("[Scale out] adding a new learner member to etcd cluster", "peerURL", peerURL)
		if _, err := etcdutils.AddMember(etcdutils.ClientConfig{Endpoints: eps, TLS: s.tlsConfig}, []string{peerURL}, true); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("Learner member added successfully", "peerURL", peerURL)

		// gofail: var exceptionAfterMemberAdd struct{}

		if err := createMemberPod(ctx, logger, r.Client, s.cluster, nextOrdinal, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Scale in: remove the last member from etcd, then delete its pod.
	memberCnt := 0
	if s.memberListResp != nil {
		memberCnt = len(s.memberListResp.Members)
	}
	podToRemove := s.pods[len(s.pods)-1]
	memberID := s.memberHealth[memberCnt-1].Status.Header.MemberId
	logger.Info("[Scale in] removing one member", "memberID", memberID, "pod", podToRemove.Name)

	// TODO: not implemented yet. If the member being removed is the leader,
	// move leadership to another member (the one with the lowest ordinal)
	// before removing it.

	epsForRemoval := eps[:len(eps)-1]
	if err := etcdutils.RemoveMember(etcdutils.ClientConfig{Endpoints: epsForRemoval, TLS: s.tlsConfig}, memberID); err != nil {
		return ctrl.Result{}, err
	}

	// gofail: var exceptionAfterMemberDelete struct{}

	if err := r.Delete(ctx, podToRemove); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// upgradeCluster rolls one Pod to the etcd version requested in
// EtcdCluster.Spec.Version, removing the Pod with the old version and the
// highest ordinal (moving leadership first if it is the leader). The next
// loop's reconcileExceptions phase recreates it with the new version.
//
// TODO: not implemented yet. validateSpec already checks, by comparing the
// first Pod's image tag against EtcdCluster.Spec.Version, whether the
// upgrade path is supported when the two differ — but it doesn't persist
// that comparison anywhere on reconcileState. This phase needs to redo the
// same current-vs-target version comparison to decide whether a Pod
// replacement is needed, then perform the replacement described above. Per
// the workflow diagram, consider recording the in-progress ordinal on
// EtcdCluster.Status (e.g. status.currentUpgradeMember) so the correct Pod
// is recreated if this loop is interrupted.
func (r *EtcdClusterReconciler) upgradeCluster(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// updateStatus reflects the current observed state onto EtcdCluster.Status.
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
		s.cluster.Status.MemberCount = int32(len(s.memberListResp.Members))

		s.cluster.Status.Members = make([]ecv1alpha1.MemberStatus, 0, len(s.memberListResp.Members))
		for i, member := range s.memberListResp.Members {
			memberStatus := ecv1alpha1.MemberStatus{
				ID:   fmt.Sprintf("%x", member.ID),
				Name: member.Name,
			}
			if i < len(s.memberHealth) {
				health := s.memberHealth[i]
				memberStatus.IsHealthy = health.Health
				if health.Status != nil {
					memberStatus.Version = health.Status.Version
					memberStatus.IsLeader = health.Status.Header.MemberId == health.Status.Leader
				}
			}
			memberStatus.IsLearner = member.IsLearner
			s.cluster.Status.Members = append(s.cluster.Status.Members, memberStatus)
		}

		_, leaderStatus := etcdutils.FindLeaderStatus(s.memberHealth, logger)
		if leaderStatus != nil {
			s.cluster.Status.LeaderID = fmt.Sprintf("%x", leaderStatus.Leader)
		}

		if leaderStatus != nil {
			s.cluster.Status.CurrentVersion = leaderStatus.Version
		} else if len(s.memberHealth) > 0 && s.memberHealth[0].Status != nil {
			s.cluster.Status.CurrentVersion = s.memberHealth[0].Status.Version
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
		for _, health := range s.memberHealth {
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

	if s.memberListResp != nil && len(s.memberHealth) > 0 {
		var unhealthyMembers []string
		for _, health := range s.memberHealth {
			if !health.Health && health.Status != nil {
				unhealthyMembers = append(unhealthyMembers, fmt.Sprintf("%x", health.Status.Header.MemberId))
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
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Service{})

	if isCertManagerCRDPresent(mgr) {
		builder = builder.Owns(&certv1.Certificate{})
		setupLog.Info("cert-manager CRDs detected, enabling Certificate watches")
	} else {
		setupLog.Info("cert-manager CRDs not detected, only auto provider will be available. Restart the controller after cert-manager CRDs are installed")
	}

	return builder.Complete(r)
}
