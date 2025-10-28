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
	"time"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd-operator/pkg/status"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	requeueDuration = 10 * time.Second
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	ImageRegistry string
}

// reconcileState holds all transient data for a single reconciliation loop.
// Every phase of Reconcile stores intermediate information here so that
// subsequent phases can operate without additional lookups.
type reconcileState struct {
	cluster          *ecv1alpha1.EtcdCluster       // cluster custom resource currently being reconciled
	sts              *appsv1.StatefulSet           // associated StatefulSet for the cluster
	memberListResp   *clientv3.MemberListResponse  // member list fetched from the etcd cluster
	memberHealth     []etcdutils.EpHealth          // health information for each etcd member
	calculatedStatus *ecv1alpha1.EtcdClusterStatus // calculated status for this reconcile cycle
	cm               *status.Manager               // condition manager for setting status conditions
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
// Status updates are tracked in state.calculatedStatus and atomically patched
// at the end via a deferred call to ensure status reflects the reconcile outcome.
//
// For more details on the controller-runtime Reconcile contract see:
// https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	state, res, err := r.fetchAndValidateState(ctx, req)
	if state == nil || err != nil {
		return res, err
	}

	r.initializeStatusTracking(state)
	defer r.registerStatusPatch(ctx, state, &res, &err)

	r.setBaselineConditions(state)

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

	// Ensure the operator has TLS credentials when the cluster requests TLS.
	if ec.Spec.TLS != nil {
		if err := createClientCertificate(ctx, ec, r.Client); err != nil {
			logger.Error(err, "Failed to create Client Certificate.")
		}
	} else {
		// TODO: instead of logging error, set default autoConfig
		logger.Error(nil, fmt.Sprintf(
			"missing TLS config for %s,\n running etcd-cluster without TLS protection is NOT recommended for production.",
			ec.Name,
		))
	}

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
	}

	return &reconcileState{cluster: ec, sts: sts}, ctrl.Result{}, nil
}

// initializeStatusTracking makes a deep copy of the CR status so we can compute
// changes without mutating the object fetched from the API server. It also
// prepares a condition manager bound to the current generation.
func (r *EtcdClusterReconciler) initializeStatusTracking(state *reconcileState) {
	if state == nil || state.cluster == nil {
		return
	}

	statusCopy := state.cluster.Status.DeepCopy()
	state.calculatedStatus = statusCopy
	state.cm = status.NewManager(&state.calculatedStatus.Conditions, state.cluster.Generation)
}

// setBaselineConditions ensures we report progress without clobbering established state.
func (r *EtcdClusterReconciler) setBaselineConditions(state *reconcileState) {
	if state == nil || state.cm == nil || state.calculatedStatus == nil {
		return
	}

	updater := state.cm.Update()

	if meta.FindStatusCondition(state.calculatedStatus.Conditions, status.ConditionAvailable) == nil {
		updater = updater.Available(false, status.ReasonReconciling, "Reconciliation started")
	}

	updater = updater.Progressing(true, status.ReasonReconciling, "Reconciliation started")

	if meta.FindStatusCondition(state.calculatedStatus.Conditions, status.ConditionDegraded) == nil {
		updater = updater.Degraded(false, status.ReasonReconciling, "Reconciliation started")
	}

	updater.Apply()
}

// registerStatusPatch defers patching the status subresource using the
// calculated snapshot. Any patch error is surfaced through the main reconcile
// error/result so callers do not have to handle status updates manually.
func (r *EtcdClusterReconciler) registerStatusPatch(
	ctx context.Context,
	state *reconcileState,
	res *ctrl.Result,
	reconcileErr *error,
) {
	if state == nil || state.cluster == nil || state.calculatedStatus == nil {
		return
	}

	logger := log.FromContext(ctx)

	state.calculatedStatus.ObservedGeneration = state.cluster.Generation

	patchErr := status.PatchStatusMutate(ctx, r.Client, state.cluster, func(latest *ecv1alpha1.EtcdCluster) error {
		latest.Status = *state.calculatedStatus
		return nil
	})
	if patchErr != nil {
		logger.Error(patchErr, "Failed to patch EtcdCluster status")
		if reconcileErr != nil && *reconcileErr == nil {
			*reconcileErr = patchErr
			if res != nil {
				*res = ctrl.Result{}
			}
		}
	}
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
		s.cm.Update().
			Progressing(true, status.ReasonCreatingResources, "Creating StatefulSet").
			Available(false, status.ReasonCreatingResources, "StatefulSet does not exist yet").
			Apply()

		s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, 0, r.Scheme)
		if err != nil {
			errMsg := status.FormatError("Failed to create StatefulSet", err)
			s.cm.Update().
				Available(false, status.ReasonResourceCreateFail, errMsg).
				Progressing(false, status.ReasonResourceCreateFail, errMsg).
				Degraded(true, status.ReasonResourceCreateFail, errMsg).
				Apply()
			return ctrl.Result{}, err
		}
		r.updateStatusFromStatefulSet(s.calculatedStatus, s.sts)
		s.cm.Update().
			Progressing(true, status.ReasonInitializingCluster, "StatefulSet created with 0 replicas, requires scaling to 1").
			Apply()
		requeue = true

	case s.sts.Spec.Replicas != nil && *s.sts.Spec.Replicas == 0:
		logger.Info("StatefulSet has 0 replicas. Trying to create a new cluster with 1 member")
		s.cm.Update().
			Progressing(true, status.ReasonInitializingCluster, "Scaling StatefulSet from 0 to 1 replica").
			Apply()

		s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, 1, r.Scheme)
		if err != nil {
			errMsg := status.FormatError("Failed to scale StatefulSet to 1", err)
			s.cm.Update().
				Available(false, status.ReasonResourceUpdateFail, errMsg).
				Progressing(false, status.ReasonResourceUpdateFail, errMsg).
				Degraded(true, status.ReasonResourceUpdateFail, errMsg).
				Apply()
			return ctrl.Result{}, err
		}
		r.updateStatusFromStatefulSet(s.calculatedStatus, s.sts)
		requeue = true
	}

	// Update status from STS if it exists
	if s.sts != nil {
		r.updateStatusFromStatefulSet(s.calculatedStatus, s.sts)
	}

	if err = createHeadlessServiceIfNotExist(ctx, logger, r.Client, s.cluster, r.Scheme); err != nil {
		errMsg := status.FormatError("Failed to ensure Headless Service", err)
		s.cm.Update().
			Available(false, status.ReasonResourceCreateFail, errMsg).
			Progressing(false, status.ReasonResourceCreateFail, errMsg).
			Degraded(true, status.ReasonResourceCreateFail, errMsg).
			Apply()
		return ctrl.Result{}, err
	}

	if requeue {
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}
	return ctrl.Result{}, nil
}

// performHealthChecks obtains the member list and health status from the etcd
// cluster specified in the StatefulSet. Results are stored on the reconcileState
// for later reconciliation steps. Also populates status with member information.
func (r *EtcdClusterReconciler) performHealthChecks(ctx context.Context, s *reconcileState) error {
	logger := log.FromContext(ctx)
	logger.Info("Now checking health of the cluster members")
	var err error
	s.memberListResp, s.memberHealth, err = healthCheck(s.sts, logger)
	if err != nil {
		logger.Error(err, "Health check failed")
		errMsg := status.FormatError("Health check failed", err)
		s.cm.Update().
			Available(false, status.ReasonHealthCheckError, errMsg).
			Degraded(true, status.ReasonHealthCheckError, errMsg).
			Progressing(true, status.ReasonHealthCheckError, "Retrying after health check failure").
			Apply()

		// Clear stale data on error
		if s.calculatedStatus != nil {
			s.calculatedStatus.Members = nil
			s.calculatedStatus.MemberCount = 0
			s.calculatedStatus.LeaderId = ""
			s.calculatedStatus.CurrentVersion = ""
		}
		return fmt.Errorf("health check failed: %w", err)
	}

	// Update member count
	memberCnt := 0
	if s.memberListResp != nil {
		memberCnt = len(s.memberListResp.Members)
	}

	if s.calculatedStatus != nil {
		s.calculatedStatus.MemberCount = int32(memberCnt)

		// Find leader and populate leader info
		var leaderIDHex string
		s.calculatedStatus.LeaderId = ""
		s.calculatedStatus.CurrentVersion = ""

		if memberCnt > 0 {
			_, leaderStatus := etcdutils.FindLeaderStatus(s.memberHealth, logger)
			if leaderStatus != nil && leaderStatus.Header != nil {
				leaderIDHex = fmt.Sprintf("%x", leaderStatus.Header.MemberId)
				s.calculatedStatus.LeaderId = leaderIDHex
				if leaderStatus.Version != "" {
					s.calculatedStatus.CurrentVersion = leaderStatus.Version
				}
			}
		}

		// Populate detailed member statuses
		s.calculatedStatus.Members = r.populateMemberStatuses(s.memberListResp, s.memberHealth, leaderIDHex)
	}

	return nil
}

// reconcileClusterState compares the desired cluster size with the observed
// etcd member list and StatefulSet replica count. It performs scaling actions
// and handles learner promotion when needed. A ctrl.Result with a requeue
// instructs the controller to retry after adjustments.
func (r *EtcdClusterReconciler) reconcileClusterState(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	memberCnt := 0
	if s.memberListResp != nil {
		memberCnt = len(s.memberListResp.Members)
	}
	targetReplica := *s.sts.Spec.Replicas

	if res, handled, err := r.handleReplicaMismatch(ctx, s, memberCnt, targetReplica); handled {
		return res, err
	}

	if res, handled, err := r.handlePendingLearner(ctx, s, memberCnt); handled {
		return res, err
	}

	if targetReplica == int32(s.cluster.Spec.Size) {
		return r.handleStableCluster(ctx, s)
	}

	return r.adjustReplicaTowardsSpec(ctx, s, memberCnt, targetReplica)
}

// handleReplicaMismatch ensures the StatefulSet replica count eventually
// matches the number of etcd members observed in the cluster.
// - If members > replicas: scale STS out by 1 and requeue.
// - If members < replicas: scale STS in by 1 and requeue.
// Conditions:
//   - Progressing=True, Available=False with ReasonMembersMismatch while adjusting
func (r *EtcdClusterReconciler) handleReplicaMismatch(
	ctx context.Context,
	s *reconcileState,
	memberCnt int,
	targetReplica int32,
) (ctrl.Result, bool, error) {
	if int(targetReplica) == memberCnt {
		return ctrl.Result{}, false, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("The expected number of replicas doesn't match the number of etcd members in the cluster", "targetReplica", targetReplica, "memberCnt", memberCnt)
	progressMsg := fmt.Sprintf("StatefulSet replicas (%d) differ from etcd members (%d), adjusting...", targetReplica, memberCnt)
	s.cm.Update().
		Progressing(true, status.ReasonMembersMismatch, progressMsg).
		Available(false, status.ReasonMembersMismatch, "Adjusting StatefulSet replicas to match etcd members").
		Apply()

	var err error
	var newSts *appsv1.StatefulSet
	if int(targetReplica) < memberCnt {
		logger.Info("An etcd member was added into the cluster, but the StatefulSet hasn't scaled out yet")
		newReplicaCount := targetReplica + 1
		logger.Info("Increasing StatefulSet replicas to match the etcd cluster member count", "oldReplicaCount", targetReplica, "newReplicaCount", newReplicaCount)
		if newSts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, newReplicaCount, r.Scheme); err != nil {
			errMsg := status.FormatError("Failed to adjust StatefulSet replicas", err)
			s.cm.Update().
				Available(false, status.ReasonResourceUpdateFail, errMsg).
				Progressing(false, status.ReasonResourceUpdateFail, errMsg).
				Degraded(true, status.ReasonResourceUpdateFail, errMsg).
				Apply()
			return ctrl.Result{}, true, err
		}
	} else {
		logger.Info("An etcd member was removed from the cluster, but the StatefulSet hasn't scaled in yet")
		newReplicaCount := targetReplica - 1
		logger.Info("Decreasing StatefulSet replicas to remove the unneeded Pod.", "oldReplicaCount", targetReplica, "newReplicaCount", newReplicaCount)
		if newSts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, newReplicaCount, r.Scheme); err != nil {
			errMsg := status.FormatError("Failed to adjust StatefulSet replicas", err)
			s.cm.Update().
				Available(false, status.ReasonResourceUpdateFail, errMsg).
				Progressing(false, status.ReasonResourceUpdateFail, errMsg).
				Degraded(true, status.ReasonResourceUpdateFail, errMsg).
				Apply()
			return ctrl.Result{}, true, err
		}
	}
	s.sts = newSts

	if s.calculatedStatus != nil {
		r.updateStatusFromStatefulSet(s.calculatedStatus, s.sts)
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, true, nil
}

// handlePendingLearner promotes a pending learner to a voting member when
// it is sufficiently caught up. If no leader is present, we return and let
// a subsequent loop re-check after election. When a learner is not ready
// yet, we requeue to wait for it to catch up.
// Conditions:
//   - Promoting: Progressing=True, Available=False
//   - Leader missing: Available=False, Progressing=True, Degraded=True
func (r *EtcdClusterReconciler) handlePendingLearner(
	ctx context.Context,
	s *reconcileState,
	memberCnt int,
) (ctrl.Result, bool, error) {
	if memberCnt == 0 {
		return ctrl.Result{}, false, nil
	}

	logger := log.FromContext(ctx)
	_, leaderStatus := etcdutils.FindLeaderStatus(s.memberHealth, logger)
	if leaderStatus == nil {
		s.cm.Update().
			Available(false, status.ReasonLeaderNotFound, "Cluster has no elected leader").
			Progressing(true, status.ReasonLeaderNotFound, "Waiting for leader election").
			Degraded(true, status.ReasonLeaderNotFound, "Cluster has no elected leader").
			Apply()
		return ctrl.Result{}, true, fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
	}

	learnerID, learnerStatus := etcdutils.FindLearnerStatus(s.memberHealth, logger)
	if learnerID == 0 {
		return ctrl.Result{}, false, nil
	}

	logger.Info("Learner found", "learnedID", learnerID, "learnerStatus", learnerStatus)
	if etcdutils.IsLearnerReady(leaderStatus, learnerStatus) {
		logger.Info("Learner is ready to be promoted to voting member", "learnerID", learnerID)
		logger.Info("Promoting the learner member", "learnerID", learnerID)
		s.cm.Update().
			Progressing(true, status.ReasonPromotingLearner, fmt.Sprintf("Promoting learner %d to voting member", learnerID)).
			Available(false, status.ReasonPromotingLearner, "Learner promotion in progress").
			Apply()
		eps := clientEndpointsFromStatefulsets(s.sts)
		eps = eps[:len(eps)-1]
		if err := etcdutils.PromoteLearner(eps, learnerID); err != nil {
			errMsg := status.FormatError("Failed to promote learner", err)
			s.cm.Update().
				Available(false, status.ReasonPromotingLearner, errMsg).
				Progressing(false, status.ReasonPromotingLearner, errMsg).
				Degraded(true, status.ReasonPromotingLearner, errMsg).
				Apply()
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, true, nil
	}

	logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learnerID)
	s.cm.Update().
		Progressing(true, status.ReasonPromotingLearner, fmt.Sprintf("Waiting for learner %d to catch up", learnerID)).
		Available(false, status.ReasonPromotingLearner, "Learner not ready for promotion").
		Apply()
	return ctrl.Result{RequeueAfter: requeueDuration}, true, nil
}

// handleStableCluster runs once the expected replica count matches the spec.
// Ensure every etcd member reports healthy before declaring success.
// Conditions:
//   - All healthy → Available=True, Progressing=False, Degraded=False
//   - Some unhealthy → Available=False, (optionally) Progressing=False, Degraded=True
func (r *EtcdClusterReconciler) handleStableCluster(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("EtcdCluster is already up-to-date")
	// Check if all members are healthy
	allHealthy := true
	if s.memberHealth != nil {
		for _, health := range s.memberHealth {
			if health.Error != "" || health.Status == nil || !health.Health {
				allHealthy = false
				break
			}
		}
	}

	if allHealthy {
		s.cm.Update().
			Available(true, status.ReasonClusterReady, "Cluster is fully available and healthy").
			Progressing(false, status.ReasonReconcileSuccess, "All reconciliation operations complete").
			Degraded(false, status.ReasonClusterHealthy, "All members are healthy").
			Apply()
	} else {
		s.cm.Update().
			Available(false, status.ReasonMembersUnhealthy, "Some cluster members are unhealthy").
			Progressing(false, status.ReasonMembersUnhealthy, "Waiting for members to become healthy").
			Degraded(true, status.ReasonMembersUnhealthy, "Some cluster members are unhealthy").
			Apply()
	}
	return ctrl.Result{}, nil
}

// adjustReplicaTowardsSpec moves the cluster towards the desired size in spec.
// Scale-out path:
//   - Add a learner via etcd API first, then increment desired replica locally
//     and reconcile the StatefulSet to include the new pod. We requeue to allow
//     the learner to be promoted in later loops.
//
// Scale-in path:
//   - Remove the last member via etcd API, then decrease STS by 1 and requeue.
//
// Conditions:
//   - ScalingUp/ScalingDown set Progressing=True, Available=False while in-flight
func (r *EtcdClusterReconciler) adjustReplicaTowardsSpec(
	ctx context.Context,
	s *reconcileState,
	memberCnt int,
	targetReplica int32,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	eps := clientEndpointsFromStatefulsets(s.sts)
	// If there are no learners left, we can proceed to scale the cluster towards the desired size.
	// When there are no members to add, the controller will requeue above and this block won't execute.
	if targetReplica < int32(s.cluster.Spec.Size) {
		// scale out
		_, peerURL := peerEndpointForOrdinalIndex(s.cluster, int(targetReplica))
		logger.Info("[Scale out] adding a new learner member to etcd cluster", "peerURLs", peerURL)
		s.cm.Update().
			Progressing(true, status.ReasonScalingUp, fmt.Sprintf("Adding learner member to scale to %d", s.cluster.Spec.Size)).
			Available(false, status.ReasonScalingUp, "Cluster scaling operation in progress").
			Apply()
		if _, err := etcdutils.AddMember(eps, []string{peerURL}, true); err != nil {
			errMsg := status.FormatError("Failed to add learner member", err)
			s.cm.Update().
				Available(false, status.ReasonScalingUp, errMsg).
				Progressing(false, status.ReasonScalingUp, errMsg).
				Degraded(true, status.ReasonScalingUp, errMsg).
				Apply()
			return ctrl.Result{}, err
		}

		logger.Info("Learner member added successfully", "peerURLs", peerURL)

		// Increment the local desired replica only after AddMember succeeds.
		// We then pass this value to reconcileStatefulSet to scale the STS.
		targetReplica++
		if sts, err := reconcileStatefulSet(ctx, logger, s.cluster, r.Client, targetReplica, r.Scheme); err != nil {
			errMsg := status.FormatError("Failed to update StatefulSet during scale-out", err)
			s.cm.Update().
				Available(false, status.ReasonResourceUpdateFail, errMsg).
				Progressing(false, status.ReasonResourceUpdateFail, errMsg).
				Degraded(true, status.ReasonResourceUpdateFail, errMsg).
				Apply()
			return ctrl.Result{}, err
		} else {
			s.sts = sts
		}

		if s.calculatedStatus != nil {
			r.updateStatusFromStatefulSet(s.calculatedStatus, s.sts)
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// scale in: decrement desired size, remove
	// the etcd member, then shrink the StatefulSet.
	targetReplica--
	logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", s.cluster.Spec.Size)
	memberID := s.memberHealth[memberCnt-1].Status.Header.MemberId

	logger.Info("[Scale in] removing one member", "memberID", memberID)
	s.cm.Update().
		Progressing(true, status.ReasonScalingDown, fmt.Sprintf("Removing member to scale down to %d", s.cluster.Spec.Size)).
		Available(false, status.ReasonScalingDown, "Cluster scaling operation in progress").
		Apply()
	eps = eps[:int(targetReplica)]
	if err := etcdutils.RemoveMember(eps, memberID); err != nil {
		errMsg := status.FormatError("Failed to remove member", err)
		s.cm.Update().
			Available(false, status.ReasonScalingDown, errMsg).
			Progressing(false, status.ReasonScalingDown, errMsg).
			Degraded(true, status.ReasonScalingDown, errMsg).
			Apply()
		return ctrl.Result{}, err
	}

	if sts, err := reconcileStatefulSet(ctx, logger, s.cluster, r.Client, targetReplica, r.Scheme); err != nil {
		errMsg := status.FormatError("Failed to update StatefulSet during scale-in", err)
		s.cm.Update().
			Available(false, status.ReasonResourceUpdateFail, errMsg).
			Progressing(false, status.ReasonResourceUpdateFail, errMsg).
			Degraded(true, status.ReasonResourceUpdateFail, errMsg).
			Apply()
		return ctrl.Result{}, err
	} else {
		s.sts = sts
	}

	if s.calculatedStatus != nil {
		r.updateStatusFromStatefulSet(s.calculatedStatus, s.sts)
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// updateStatusFromStatefulSet updates the status replica counts from the StatefulSet.
func (r *EtcdClusterReconciler) updateStatusFromStatefulSet(
	etcdClusterStatus *ecv1alpha1.EtcdClusterStatus,
	sts *appsv1.StatefulSet,
) {
	if etcdClusterStatus == nil {
		return
	}
	if sts == nil {
		etcdClusterStatus.CurrentReplicas = 0
		etcdClusterStatus.ReadyReplicas = 0
		return
	}
	if sts.Spec.Replicas != nil {
		etcdClusterStatus.CurrentReplicas = *sts.Spec.Replicas
	}
	etcdClusterStatus.ReadyReplicas = sts.Status.ReadyReplicas
}

// populateMemberStatuses builds the Members slice for status from etcd member list and health info.
func (r *EtcdClusterReconciler) populateMemberStatuses(
	memberListResp *clientv3.MemberListResponse,
	memberHealth []etcdutils.EpHealth,
	leaderIDHex string,
) []ecv1alpha1.MemberStatus {
	if memberListResp == nil || len(memberListResp.Members) == 0 {
		return nil
	}

	// Index health responses by member ID so we can safely match even if etcd
	// returns members and health entries in different orders.
	healthByMemberID := make(map[uint64]etcdutils.EpHealth, len(memberHealth))
	for _, mh := range memberHealth {
		if mh.Status != nil && mh.Status.Header != nil {
			healthByMemberID[mh.Status.Header.MemberId] = mh
		}
	}

	members := make([]ecv1alpha1.MemberStatus, 0, len(memberListResp.Members))
	for _, m := range memberListResp.Members {
		memberIDHex := fmt.Sprintf("%x", m.ID)
		memberStatus := ecv1alpha1.MemberStatus{
			Name:      m.Name,
			ID:        memberIDHex,
			IsLearner: m.IsLearner,
		}

		// Set leader flag
		if leaderIDHex != "" && memberIDHex == leaderIDHex {
			memberStatus.IsLeader = true
		}

		// Set details from health check
		if mh, ok := healthByMemberID[m.ID]; ok {
			if mh.Health {
				memberStatus.IsHealthy = true
			}
			if mh.Status != nil && mh.Status.Version != "" {
				memberStatus.Version = mh.Status.Version
			}
		}

		members = append(members, memberStatus)
	}
	return members
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("etcdcluster-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&certv1.Certificate{}).
		Complete(r)
}
