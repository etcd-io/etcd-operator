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
	"fmt"
	"strings"
	"time"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	appsv1 "k8s.io/api/apps/v1"
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
// Every phase of Reconcile stores intermediate information here so that
// subsequent phases can operate without additional lookups.
type reconcileState struct {
	cluster        *ecv1alpha1.EtcdCluster      // cluster custom resource currently being reconciled
	sts            *appsv1.StatefulSet          // associated StatefulSet for the cluster
	memberListResp *clientv3.MemberListResponse // member list fetched from the etcd cluster
	memberHealth   []etcdutils.EpHealth         // health information for each etcd member
	tls            tlsReadiness                 // verdict on the cluster's TLS surfaces (drives the TLSReady condition)
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;get;list;update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;patch;update;delete
// +kubebuilder:rbac:groups="cert-manager.io",resources=certificates,verbs=get;list;watch;create;patch;update;delete
// +kubebuilder:rbac:groups="cert-manager.io",resources=clusterissuers,verbs=get;list;watch
// +kubebuilder:rbac:groups="cert-manager.io",resources=issuers,verbs=get;list;watch

// Reconcile orchestrates a single reconciliation cycle for an EtcdCluster. It
// sequentially fetches resources, ensures primitive objects exist, checks the
// health of the etcd cluster and then adjusts its state to match the desired
// specification. Each phase is handled by a dedicated helper method.
//
// For more details on the controller-runtime Reconcile contract see:
// https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var state *reconcileState
	var res ctrl.Result
	var err error

	// Defer status update to ensure it's called regardless of return path
	defer func() {
		if state != nil {
			if statusErr := r.updateStatus(ctx, state); statusErr != nil {
				// Log but don't override the main reconciliation error
				log.FromContext(ctx).Error(statusErr, "Failed to update status")
			}
		}
	}()

	state, res, err = r.fetchAndValidateState(ctx, req)
	if state == nil || err != nil {
		return res, err
	}

	if res, err = r.bootstrapStatefulSet(ctx, state); err != nil || !res.IsZero() {
		return res, err
	}

	if err = r.performHealthChecks(ctx, state); err != nil {
		return ctrl.Result{}, err
	}

	return r.reconcileClusterState(ctx, state)
}

// fetchAndValidateState retrieves the EtcdCluster and its StatefulSet and ensures
// the StatefulSet, if present, is owned by the cluster. It returns a populated
// reconcileState for use in later phases. A non-empty ctrl.Result requests a
// requeue when transient issues occur.
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

	// Determine desired etcd image registry
	if ec.Spec.ImageRegistry == "" {
		ec.Spec.ImageRegistry = r.ImageRegistry
	}

	// Reconcile-time backstop for the apply-time CEL rules (see validateTLS). Reject
	// an incoherent TLS spec by requeuing rather than silently proceeding into cert
	// provisioning, which would otherwise fail deep with a less actionable error.
	if errs := validateTLS(ec); len(errs) > 0 {
		agg := errs.ToAggregate()
		logger.Error(agg, "invalid TLS configuration; not reconciling until fixed",
			"etcdCluster", ec.Name)
		r.Recorder.Eventf(ec, nil, corev1.EventTypeWarning, reasonClientCertificateError,
			"ValidateTLS", "invalid TLS configuration: %v", agg)
		return nil, ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// The operator authenticates to etcd as a client, so it needs its own client
	// identity iff the CLIENT surface is configured (independent of the peer surface).
	if clientTLSEnabled(ec) {
		if err := createClientCertificate(ctx, ec, r.Client); err != nil {
			logger.Error(err, "Failed to create operator client certificate", "etcdCluster", ec.Name)
			r.Recorder.Eventf(ec, nil, corev1.EventTypeWarning, reasonClientCertificateError,
				"CreateClientCertificate", "failed to create operator client certificate: %v", err)
		}
	} else {
		// Cleartext (no client surface) is a supported mode; log at Info, not Error,
		// so legitimately-cleartext clusters don't spam error logs every reconcile.
		logger.Info("client TLS surface not configured; operator dials etcd in cleartext (not recommended for production)",
			"etcdCluster", ec.Name)
	}

	// Evaluate the configured TLS surfaces (Recorder-free verdict), emit any failure
	// Event at this controller boundary, and stash the verdict for the TLSReady
	// condition. This is the single place the runtime peer-CA / issuer / client-cert
	// checks are surfaced as Events; the verdict itself is computed without the
	// Recorder so the util/status helpers stay upstream-clean.
	tlsState := evaluateTLSReadiness(ctx, ec, r.Client)
	r.recordTLSEvent(ec, tlsState)

	logger.Info("Reconciling EtcdCluster", "spec", ec.Spec)

	sts, err := getStatefulSet(ctx, r.Client, ec.Name, ec.Namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			sts = nil
		} else {
			logger.Error(err, "Failed to get StatefulSet. Requesting requeue")
			return nil, ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	}

	if sts != nil {
		if err := checkStatefulSetControlledByEtcdOperator(ec, sts); err != nil {
			logger.Error(err, "StatefulSet is not controlled by this EtcdCluster resource")
			return nil, ctrl.Result{}, err
		}

		// If the version to be reconciled is unsupported, throw an error.
		if len(sts.Spec.Template.Spec.Containers) == 0 {
			logger.Error(err, "StatefulSet has no containers yet")
			return &reconcileState{cluster: ec, sts: sts, tls: tlsState}, ctrl.Result{}, nil
		}
		stsImage := sts.Spec.Template.Spec.Containers[0].Image
		// Note: createOrPatchStatefulSet() only supports the "registry-path:image" format.
		// TODO: switch to using the version from ec.Status eventually.
		//   https://github.com/etcd-io/etcd-operator/pull/278/changes#r2764796805
		idx := strings.Index(stsImage, ":")
		if idx == -1 {
			logger.Error(err, "could not extract image version from StatefulSet image",
				"image", stsImage)
			return &reconcileState{cluster: ec, sts: sts, tls: tlsState}, ctrl.Result{}, nil
		}
		currentVersion := stsImage[idx+1:]
		targetVersion := ec.Spec.Version

		// Only handle cases when there is a version change.
		if currentVersion != targetVersion {
			// TODO: consider adding an option in the CRD called allowCustomImageUpgrade to make
			// the behavior here optional:
			//   https://github.com/etcd-io/etcd-operator/pull/278/changes#r2764717418
			canParse, err := validateEtcdUpgradePath(etcdversions.AllVersions, currentVersion, targetVersion)
			if !canParse {
				logger.Info("error when parsing reconcile versions; it is your responsibility "+
					"to validate if the upgrade path is supported",
					"current", currentVersion,
					"target", targetVersion,
					"error", err,
				)
				return &reconcileState{cluster: ec, sts: sts, tls: tlsState}, ctrl.Result{}, nil
			} else {
				if err != nil {
					logger.Error(err, "unsupported upgrade path between current and target versions",
						"current", currentVersion,
						"target", targetVersion,
					)
					return nil, ctrl.Result{}, err
				}
				logger.Info("upgrade path between current and target versions is supported",
					"current", currentVersion,
					"target", targetVersion)
			}
		}
	}

	return &reconcileState{cluster: ec, sts: sts, tls: tlsState}, ctrl.Result{}, nil
}

// bootstrapStatefulSet ensures that the foundational Kubernetes objects for
// a cluster exist and are correctly initialized. It creates the StatefulSet (initially
// with 0 replicas) and the headless Service if necessary. When either resource
// is created or the StatefulSet is scaled from zero to one replica, the returned
// ctrl.Result requests a requeue so the next reconciliation loop can observe the
// new state. The reconcileState is updated with the current StatefulSet.
func (r *EtcdClusterReconciler) bootstrapStatefulSet(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	requeue := false
	var err error

	switch {
	case s.sts == nil:
		logger.Info("Creating StatefulSet with 0 replica", "expectedSize", s.cluster.Spec.Size)
		s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, 0, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		requeue = true

	case s.sts.Spec.Replicas != nil && *s.sts.Spec.Replicas == 0:
		logger.Info("StatefulSet has 0 replicas. Trying to create a new cluster with 1 member")
		s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, 1, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
		requeue = true
	}

	if err = createHeadlessServiceIfNotExist(ctx, logger, r.Client, s.cluster, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	if requeue {
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}
	return ctrl.Result{}, nil
}

// performHealthChecks obtains the member list and health status from the etcd
// cluster specified in the StatefulSet. Results are stored on the reconcileState
// for later reconciliation steps.
func (r *EtcdClusterReconciler) performHealthChecks(ctx context.Context, s *reconcileState) error {
	logger := log.FromContext(ctx)
	logger.Info("Now checking health of the cluster members")
	var err error
	s.memberListResp, s.memberHealth, err = healthCheck(ctx, s.cluster, r.Client, s.sts, logger)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	return nil
}

// reconcileClusterState compares the desired cluster size with the observed
// etcd member list and StatefulSet replica count. It performs scaling actions
// and handles learner promotion when needed. A ctrl.Result with a requeue
// instructs the controller to retry after adjustments.
func (r *EtcdClusterReconciler) reconcileClusterState(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	memberCnt := 0
	if s.memberListResp != nil {
		memberCnt = len(s.memberListResp.Members)
	}
	targetReplica := *s.sts.Spec.Replicas
	var err error

	// Build the operator's etcd-client TLS config once (nil when the client surface
	// is cleartext) and derive client endpoints from the client surface's scheme.
	clientTLSConfig, err := buildClientTLSConfig(ctx, s.cluster, r.Client)
	if err != nil {
		r.Recorder.Eventf(s.cluster, nil, corev1.EventTypeWarning, reasonClientCertificateError,
			"BuildClientTLSConfig", "failed to build operator client TLS config: %v", err)
		return ctrl.Result{}, err
	}
	cScheme := clientScheme(s.cluster)

	// The number of replicas in the StatefulSet doesn't match the number of etcd members in the cluster.
	if int(targetReplica) != memberCnt {
		logger.Info("The expected number of replicas doesn't match the number of etcd members in the cluster", "targetReplica", targetReplica, "memberCnt", memberCnt)
		if int(targetReplica) < memberCnt {
			logger.Info("An etcd member was added into the cluster, but the StatefulSet hasn't scaled out yet")
			newReplicaCount := targetReplica + 1
			logger.Info("Increasing StatefulSet replicas to match the etcd cluster member count", "oldReplicaCount", targetReplica, "newReplicaCount", newReplicaCount)
			if _, err := reconcileStatefulSet(ctx, logger, s.cluster, r.Client, newReplicaCount, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			logger.Info("An etcd member was removed from the cluster, but the StatefulSet hasn't scaled in yet")
			newReplicaCount := targetReplica - 1
			logger.Info("Decreasing StatefulSet replicas to remove the unneeded Pod.", "oldReplicaCount", targetReplica, "newReplicaCount", newReplicaCount)
			if _, err := reconcileStatefulSet(ctx, logger, s.cluster, r.Client, newReplicaCount, r.Scheme); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	var (
		learnerStatus *clientv3.StatusResponse
		learner       uint64
		leaderStatus  *clientv3.StatusResponse
	)

	if memberCnt > 0 {
		// Find the leader status
		_, leaderStatus = etcdutils.FindLeaderStatus(s.memberHealth, logger)
		if leaderStatus == nil {
			// If the leader is not available, wait for the leader to be elected
			return ctrl.Result{}, fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
		}

		learner, learnerStatus = etcdutils.FindLearnerStatus(s.memberHealth, logger)
		if learner > 0 {
			// There is at least one learner. Try to promote it if it's ready; otherwise requeue and wait.
			logger.Info("Learner found", "learnedID", learner, "learnerStatus", learnerStatus)
			if etcdutils.IsLearnerReady(leaderStatus, learnerStatus) {
				logger.Info("Learner is ready to be promoted to voting member", "learnerID", learner)
				logger.Info("Promoting the learner member", "learnerID", learner)
				eps := clientEndpointsFromStatefulsets(s.sts, cScheme)
				eps = eps[:(len(eps) - 1)]
				if err := etcdutils.PromoteLearner(eps, learner, clientTLSConfig); err != nil {
					// The member is not promoted yet, so we error out and requeue via the caller.
					return ctrl.Result{}, err
				}
			} else {
				// Learner is not yet ready. We can't add another learner or proceed further until this one is promoted.
				logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learner)
				return ctrl.Result{RequeueAfter: requeueDuration}, nil
			}
		}
	}

	if targetReplica == int32(s.cluster.Spec.Size) {
		logger.Info("EtcdCluster is already up-to-date")
		return ctrl.Result{}, nil
	}

	eps := clientEndpointsFromStatefulsets(s.sts, cScheme)

	// If there are no learners left, we can proceed to scale the cluster towards the desired size.
	// When there are no members to add, the controller will requeue above and this block won't execute.
	if targetReplica < int32(s.cluster.Spec.Size) {
		// scale out
		_, peerURL := peerEndpointForOrdinalIndex(s.cluster, int(targetReplica))
		targetReplica++
		logger.Info("[Scale out] adding a new learner member to etcd cluster", "peerURLs", peerURL)
		if _, err := etcdutils.AddMember(eps, []string{peerURL}, true, clientTLSConfig); err != nil {
			return ctrl.Result{}, err
		}

		logger.Info("Learner member added successfully", "peerURLs", peerURL)

		// gofail: var exceptionAfterMemberAdd struct{}

		if s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, targetReplica, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	if targetReplica > int32(s.cluster.Spec.Size) {
		// scale in
		targetReplica--
		logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", s.cluster.Spec.Size)

		memberID := s.memberHealth[memberCnt-1].Status.Header.MemberId

		logger.Info("[Scale in] removing one member", "memberID", memberID)
		eps = eps[:targetReplica]
		if err := etcdutils.RemoveMember(eps, memberID, clientTLSConfig); err != nil {
			return ctrl.Result{}, err
		}

		// gofail: var exceptionAfterMemberDelete struct{}

		if s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, targetReplica, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Ensure every etcd member reports itself healthy before declaring success.
	allMembersHealthy, err := areAllMembersHealthy(ctx, s.cluster, r.Client, s.sts, logger)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !allMembersHealthy {
		// Requeue until the StatefulSet settles and all members are healthy.
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	logger.Info("EtcdCluster reconciled successfully")
	return ctrl.Result{}, nil
}

// updateStatus updates the EtcdCluster status based on observed state.
// It is called at the end of each reconciliation cycle.
func (r *EtcdClusterReconciler) updateStatus(ctx context.Context, s *reconcileState) error {
	logger := log.FromContext(ctx)

	// Update ObservedGeneration
	s.cluster.Status.ObservedGeneration = s.cluster.Generation

	// Update replica counts from StatefulSet
	if s.sts != nil {
		if s.sts.Spec.Replicas != nil {
			s.cluster.Status.CurrentReplicas = *s.sts.Spec.Replicas
		}
		s.cluster.Status.ReadyReplicas = s.sts.Status.ReadyReplicas
	}

	// Update member count from etcd cluster
	if s.memberListResp != nil {
		s.cluster.Status.MemberCount = int32(len(s.memberListResp.Members))

		// Update individual member statuses
		s.cluster.Status.Members = make([]ecv1alpha1.MemberStatus, 0, len(s.memberListResp.Members))
		for i, member := range s.memberListResp.Members {
			memberStatus := ecv1alpha1.MemberStatus{
				ID:   fmt.Sprintf("%x", member.ID),
				Name: member.Name,
			}

			// Find health info for this member
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

		// Update leader ID
		_, leaderStatus := etcdutils.FindLeaderStatus(s.memberHealth, logger)
		if leaderStatus != nil {
			s.cluster.Status.LeaderID = fmt.Sprintf("%x", leaderStatus.Leader)
		}

		// Update current version from leader or first healthy member
		if leaderStatus != nil {
			s.cluster.Status.CurrentVersion = leaderStatus.Version
		} else if len(s.memberHealth) > 0 && s.memberHealth[0].Status != nil {
			s.cluster.Status.CurrentVersion = s.memberHealth[0].Status.Version
		}
	}

	// Update conditions
	r.updateConditions(s)

	// Persist status update
	if err := r.Status().Update(ctx, s.cluster); err != nil {
		logger.Error(err, "Failed to update EtcdCluster status")
		return err
	}

	return nil
}

// updateConditions sets the standard Kubernetes conditions based on observed state
func (r *EtcdClusterReconciler) updateConditions(s *reconcileState) {
	now := metav1.Now()

	// Determine if cluster is available (has quorum and healthy members)
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
			availableCondition.Message = fmt.Sprintf("Etcd cluster has %d/%d healthy members with quorum", healthyCount, len(s.memberListResp.Members))
		} else {
			availableCondition.Message = fmt.Sprintf("Etcd cluster has %d/%d healthy members, quorum requires %d", healthyCount, len(s.memberListResp.Members), quorum)
		}
	}

	// Determine if cluster is progressing (scaling or upgrading)
	progressingCondition := metav1.Condition{
		Type:               "Progressing",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: s.cluster.Generation,
		LastTransitionTime: now,
		Reason:             "ClusterStable",
		Message:            "Etcd cluster is stable",
	}

	if s.sts != nil && s.sts.Spec.Replicas != nil {
		currentReplicas := *s.sts.Spec.Replicas
		desiredSize := int32(s.cluster.Spec.Size)

		if currentReplicas != desiredSize {
			progressingCondition.Status = metav1.ConditionTrue
			progressingCondition.Reason = "ScalingInProgress"
			progressingCondition.Message = fmt.Sprintf("Scaling from %d to %d replicas", currentReplicas, desiredSize)
		} else if s.memberListResp != nil && int32(len(s.memberListResp.Members)) != desiredSize {
			progressingCondition.Status = metav1.ConditionTrue
			progressingCondition.Reason = "MembershipChanging"
			progressingCondition.Message = fmt.Sprintf("Etcd membership changing: %d members, target %d", len(s.memberListResp.Members), desiredSize)
		}

		// Check for learners (indicates scaling in progress)
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
	}

	// Determine if cluster is degraded
	degradedCondition := metav1.Condition{
		Type:               "Degraded",
		Status:             metav1.ConditionFalse,
		ObservedGeneration: s.cluster.Generation,
		LastTransitionTime: now,
		Reason:             "ClusterHealthy",
		Message:            "All etcd members are healthy",
	}

	if s.memberListResp != nil && len(s.memberHealth) > 0 {
		unhealthyMembers := []string{}
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

	// Update or append conditions
	meta.SetStatusCondition(&s.cluster.Status.Conditions, availableCondition)
	meta.SetStatusCondition(&s.cluster.Status.Conditions, progressingCondition)
	meta.SetStatusCondition(&s.cluster.Status.Conditions, degradedCondition)

	// TLSReady reflects the health of the configured TLS surfaces. When no surface
	// is configured (TLSNotConfigured) the condition is omitted entirely -- no TLS,
	// no TLS condition -- and any stale prior condition is removed so flipping a
	// cluster back to cleartext doesn't leave a dangling TLSReady.
	if s.tls.configured {
		meta.SetStatusCondition(&s.cluster.Status.Conditions, s.tls.condition(s.cluster.Generation))
	} else {
		meta.RemoveStatusCondition(&s.cluster.Status.Conditions, tlsReadyConditionType)
	}
}

// recordTLSEvent emits a CR Event reflecting the TLS verdict computed by
// evaluateTLSReadiness. It is the controller boundary that turns the Recorder-free
// verdict into a Kubernetes Event: a Warning (reason == the verdict reason) for any
// failure, and a single Normal "TLSReady" only when a configured cluster transitions
// INTO the ready state (so a steady-state healthy cluster doesn't emit a TLSReady
// event every reconcile). The reason strings match the TLSReady condition reasons.
func (r *EtcdClusterReconciler) recordTLSEvent(ec *ecv1alpha1.EtcdCluster, t tlsReadiness) {
	if !t.configured {
		return
	}
	if !t.ready {
		r.Recorder.Eventf(ec, nil, corev1.EventTypeWarning, t.reason, "TLSReady", "%s", t.message)
		return
	}
	// Ready: only emit on transition (the prior TLSReady condition was not already True).
	prior := meta.FindStatusCondition(ec.Status.Conditions, tlsReadyConditionType)
	if prior == nil || prior.Status != metav1.ConditionTrue {
		r.Recorder.Eventf(ec, nil, corev1.EventTypeNormal, reasonTLSReady, "TLSReady", "%s", t.message)
	}
}

// isCertManagerCRDPresent checks if cert-manager CRDs are installed in the cluster
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
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{})

	// Conditionally watch cert-manager Certificate resources if CRDs are installed
	// This allows the controller to react to Certificate status changes when using cert-manager provider
	if isCertManagerCRDPresent(mgr) {
		// cert-manager CRDs are installed, add Certificate watch
		builder = builder.Owns(&certv1.Certificate{})
		setupLog.Info("cert-manager CRDs detected, enabling Certificate watches")
	} else {
		// cert-manager CRDs not installed, skip Certificate watch
		setupLog.Info("cert-manager CRDs not detected, only auto provider will be available. Restart the controller after cert-manager CRDs are installed")
	}

	return builder.Complete(r)
}
