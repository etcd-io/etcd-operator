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

	"go.etcd.io/etcd-operator/pkg/status"

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
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	requeueDuration = 10 * time.Second
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;get;list;update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the EtcdCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.1/pkg/reconcile
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) { // Use named return values
	logger := log.FromContext(ctx)

	// ---------------------------------------------------------------------
	// 1. Fetch EtcdCluster resource
	// ---------------------------------------------------------------------
	etcdCluster := &ecv1alpha1.EtcdCluster{}
	if err = r.Get(ctx, req.NamespacedName, etcdCluster); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("EtcdCluster resource not found. Ignoring since object may have been deleted")
			return ctrl.Result{}, nil // Return nil error for IsNotFound
		}
		logger.Error(err, "Failed to get EtcdCluster")
		// Cannot update status if get fails, just return the error
		return ctrl.Result{}, err
	}

	// ---------------------------------------------------------------------
	// 2. Initialize Status Patch Helper & Defer Status Update
	// ---------------------------------------------------------------------
	// We use PatchStatusMutate. It fetches the latest object inside its retry loop.
	// We prepare the *desired* status modifications in a 'calculatedStatus' variable
	// within the Reconcile scope, and the mutate function applies these calculations.
	var calculatedStatus ecv1alpha1.EtcdClusterStatus = etcdCluster.Status // Start with current status

	// Initialize ConditionManager using the calculatedStatus *copy*.
	// Modifications via cm will only affect this copy until the patch is applied.
	observedGeneration := etcdCluster.Generation
	cm := status.NewManager(&calculatedStatus.Conditions, observedGeneration)

	// Defer the actual patch operation. The mutate function will apply the final 'calculatedStatus'.
	defer func() {
		// Call PatchStatusMutate. The mutate function copies the calculatedStatus
		// onto the latest version fetched inside the patch retry loop.
		patchErr := status.PatchStatusMutate(ctx, r.Client, etcdCluster, func(latest *ecv1alpha1.EtcdCluster) error {
			// Apply the status we calculated during *this* reconcile cycle
			latest.Status = calculatedStatus
			// Ensure Phase is derived *before* patching
			r.derivePhaseFromConditions(latest) // Pass the object being patched
			return nil
		})
		if patchErr != nil {
			logger.Error(patchErr, "Failed to patch EtcdCluster status")
			// If the main reconcile logic didn't return an error, return the patch error
			if err == nil {
				err = patchErr
				res = ctrl.Result{} // Reset result if only status patch failed
			}
			// If the main reconcile *did* return an error, we prefer that original error.
		}
	}()

	// ---------------------------------------------------------------------
	// 3. Initial Status Setup & Handle Size 0
	// ---------------------------------------------------------------------
	// Set initial 'Unknown'/'False' conditions for this reconcile cycle
	cm.SetAvailable(false, status.ReasonReconciling, "Reconciliation started")  // Default to Not Available
	cm.SetProgressing(true, status.ReasonReconciling, "Reconciliation started") // Default to Progressing
	cm.SetDegraded(false, status.ReasonReconciling, "Reconciliation started")   // Default to Not Degraded

	if etcdCluster.Spec.Size == 0 {
		logger.Info("EtcdCluster size is 0..Skipping next steps")
		calculatedStatus.ReadyReplicas = 0
		calculatedStatus.Members = 0
		cm.SetAvailable(false, status.ReasonSizeIsZero, "Desired cluster size is 0")
		cm.SetProgressing(false, status.ReasonSizeIsZero, "Desired cluster size is 0")
		return ctrl.Result{}, nil
	}

	// ---------------------------------------------------------------------
	// 4. Reconcile Core Resources (STS, Service) & Handle Errors
	// ---------------------------------------------------------------------
	logger.Info("Reconciling EtcdCluster", "spec", etcdCluster.Spec)

	var sts *appsv1.StatefulSet
	sts, err = getStatefulSet(ctx, r.Client, etcdCluster.Name, etcdCluster.Namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Creating StatefulSet with 0 replica", "expectedSize", etcdCluster.Spec.Size)
			cm.SetProgressing(true, status.ReasonCreatingResources, "StatefulSet not found, creating...")
			cm.SetAvailable(false, status.ReasonCreatingResources, "StatefulSet does not exist yet")

			sts, err = reconcileStatefulSet(ctx, logger, etcdCluster, r.Client, 0, r.Scheme)
			if err != nil {
				logger.Error(err, "Failed to create StatefulSet")
				cm.SetProgressing(false, status.ReasonResourceCreateFail, status.FormatError("Failed to create StatefulSet", err))
				cm.SetDegraded(true, status.ReasonResourceCreateFail, status.FormatError("Failed to create StatefulSet", err))
				return ctrl.Result{}, err
			}
			// STS created with 0 replicas, needs scaling to 1. Set Initializing.
			cm.SetProgressing(true, status.ReasonInitializingCluster, "StatefulSet created with 0 replicas, requires scaling to 1")
		} else {
			// If an error occurs during Get/Create, we'll requeue the item so we can
			// attempt processing again later. This could have been caused by a
			// temporary network failure, or any other transient reason.
			logger.Error(err, "Failed to get StatefulSet. Requesting requeue")
			cm.SetProgressing(false, status.ReasonStatefulSetGetError, status.FormatError("Failed to get StatefulSet", err)) // Stop progressing on persistent get error
			cm.SetDegraded(true, status.ReasonStatefulSetGetError, status.FormatError("Failed to get StatefulSet", err))
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	}

	// At this point, sts should exist (either found or created)
	if sts == nil {
		// This case should ideally not happen if error handling above is correct
		err = fmt.Errorf("statefulSet is unexpectedly nil after get/create")
		logger.Error(err, "Internal error")
		cm.SetDegraded(true, "InternalError", err.Error())
		return ctrl.Result{}, err
	}

	// Update ReadyReplicas based on current STS status
	calculatedStatus.ReadyReplicas = sts.Status.ReadyReplicas

	// If the Statefulsets is not controlled by this EtcdCluster resource, we should log
	// a warning to the event recorder and return error msg.
	err = checkStatefulSetControlledByEtcdOperator(etcdCluster, sts)
	if err != nil {
		logger.Error(err, "StatefulSet is not controlled by this EtcdCluster resource")
		cm.SetDegraded(true, status.ReasonNotOwnedResource, err.Error())
		return ctrl.Result{}, err
	}

	// If statefulset size is 0. try to instantiate the cluster with 1 member
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas == 0 {
		logger.Info("StatefulSet has 0 replicas. Trying to create a new cluster with 1 member")
		cm.SetProgressing(true, status.ReasonInitializingCluster, "Scaling StatefulSet from 0 to 1 replica")

		sts, err = reconcileStatefulSet(ctx, logger, etcdCluster, r.Client, 1, r.Scheme)
		if err != nil {
			logger.Error(err, "Failed to scale StatefulSet to 1 replica")
			cm.SetProgressing(false, status.ReasonResourceUpdateFail, status.FormatError("Failed to scale StatefulSet to 1", err))
			cm.SetDegraded(true, status.ReasonResourceUpdateFail, status.FormatError("Failed to scale StatefulSet to 1", err))
			return ctrl.Result{}, err
		}
		// return ctrl.Result{RequeueAfter: requeueDuration}, nil // Requeue to check readiness, should we do it?
	}

	// Create Headless Service
	err = createHeadlessServiceIfNotExist(ctx, logger, r.Client, etcdCluster, r.Scheme)
	if err != nil {
		logger.Error(err, "Failed to create Headless Service")
		cm.SetProgressing(false, status.ReasonResourceCreateFail, status.FormatError("Failed to ensure Headless Service", err))
		cm.SetDegraded(true, status.ReasonResourceCreateFail, status.FormatError("Failed to ensure Headless Service", err))
		return ctrl.Result{}, err
	}

	// ---------------------------------------------------------------------
	// 5. Etcd Cluster Health Check & Member Status
	// ---------------------------------------------------------------------
	logger.Info("Now checking health of the cluster members")
	var memberListResp *clientv3.MemberListResponse // Declare other multi-return variables if needed
	var healthInfos []etcdutils.EpHealth
	memberListResp, healthInfos, err = healthCheck(sts, logger)
	if err != nil {
		logger.Error(err, "Health check failed")
		cm.SetAvailable(false, status.ReasonHealthCheckError, status.FormatError("Health check failed", err))
		cm.SetDegraded(true, status.ReasonHealthCheckError, status.FormatError("Health check failed", err))
		// Keep Progressing True as we need to retry health check
		cm.SetProgressing(true, status.ReasonHealthCheckError, "Retrying after health check failure")
		return ctrl.Result{}, fmt.Errorf("health check failed: %w", err)
	}
	// calculatedStatus.LastHealthCheckTime = metav1.Now() // TODO: Add this field
	// TODO: Update CurrentVersion from healthInfos (e.g., from leaderStatus.Version if available)
	// TODO: Populate UnhealthyMembers list based on healthInfos

	// Update member count
	memberCnt := 0
	if memberListResp != nil {
		memberCnt = len(memberListResp.Members)
	}
	calculatedStatus.Members = int32(memberCnt)

	// ---------------------------------------------------------------------
	// 6. Handle Member/Replica Mismatch
	// ---------------------------------------------------------------------
	targetReplica := *sts.Spec.Replicas // Start with the current size of the stateful set
	// The number of replicas in the StatefulSet doesn't match the number of etcd members in the cluster.
	if int(targetReplica) != memberCnt {
		logger.Info("The expected number of replicas doesn't match the number of etcd members in the cluster", "targetReplica", targetReplica, "memberCnt", memberCnt)
		cm.SetProgressing(true, status.ReasonMembersMismatch, fmt.Sprintf("StatefulSet replicas (%d) differ from etcd members (%d), adjusting...", targetReplica, memberCnt))
		cm.SetAvailable(false, status.ReasonMembersMismatch, "Adjusting StatefulSet replicas to match etcd members") // Not fully available during adjustment

		var newReplicaCount int32
		var reason string
		if int(targetReplica) < memberCnt {
			logger.Info("An etcd member was added into the cluster, but the StatefulSet hasn't scaled out yet")
			newReplicaCount = targetReplica + 1
			reason = status.ReasonScalingUp // More specific reason
			logger.Info("Increasing StatefulSet replicas to match the etcd cluster member count", "oldReplicaCount", targetReplica, "newReplicaCount", newReplicaCount)
		} else {
			logger.Info("An etcd member was removed from the cluster, but the StatefulSet hasn't scaled in yet")
			newReplicaCount = targetReplica - 1
			reason = status.ReasonScalingDown // More specific reason
			logger.Info("Decreasing StatefulSet replicas to remove the unneeded Pod.", "oldReplicaCount", targetReplica, "newReplicaCount", newReplicaCount)
		}
		cm.SetProgressing(true, reason, fmt.Sprintf("Adjusting StatefulSet replicas to %d", newReplicaCount)) // Update reason

		sts, err = reconcileStatefulSet(ctx, logger, etcdCluster, r.Client, newReplicaCount, r.Scheme)
		if err != nil {
			logger.Error(err, "Failed to adjust StatefulSet replicas to match member count")
			cm.SetProgressing(false, status.ReasonResourceUpdateFail, status.FormatError("Failed to adjust StatefulSet replicas to match member count", err))
			cm.SetDegraded(true, status.ReasonResourceUpdateFail, status.FormatError("Failed to adjust StatefulSet replicas to match member count", err))
			return ctrl.Result{}, err
		}
		// Requeue to check state after adjustment
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// ---------------------------------------------------------------------
	// 7. Handle Learners
	// ---------------------------------------------------------------------
	var (
		learnerStatus *clientv3.StatusResponse
		learner       uint64
		leaderStatus  *clientv3.StatusResponse
	)
	if memberCnt > 0 {
		_, leaderStatus = etcdutils.FindLeaderStatus(healthInfos, logger)
		if leaderStatus == nil {
			err = fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
			logger.Error(err, "Leader election might be in progress or cluster unhealthy")
			cm.SetAvailable(false, status.ReasonLeaderNotFound, err.Error())
			cm.SetDegraded(true, status.ReasonLeaderNotFound, err.Error())
			cm.SetProgressing(true, status.ReasonLeaderNotFound, "Waiting for leader election") // Still progressing towards stable state
			// If the leader is not available, let's wait for the leader to be elected
			return ctrl.Result{RequeueAfter: requeueDuration}, err
		}
		calculatedStatus.LeaderId = fmt.Sprintf("%x", leaderStatus.Header.MemberId)

		learner, learnerStatus = etcdutils.FindLearnerStatus(healthInfos, logger)
		if learner > 0 {
			// There is at least one learner. Let's try to promote it or wait
			// Find the learner status
			logger.Info("Learner found", "learnedID", learner, "learnerStatus", learnerStatus)
			cm.SetProgressing(true, status.ReasonPromotingLearner, fmt.Sprintf("Learner member %x found", learner))
			cm.SetAvailable(false, status.ReasonPromotingLearner, "Cluster has learner member, not fully available") // Not fully available with learner
			if etcdutils.IsLearnerReady(leaderStatus, learnerStatus) {
				logger.Info("Learner is ready to be promoted to voting member", "learnerID", learner)
				logger.Info("Promoting the learner member", "learnerID", learner)
				cm.SetProgressing(true, status.ReasonPromotingLearner, fmt.Sprintf("Promoting ready learner %x", learner))

				eps := clientEndpointsFromStatefulsets(sts)
				eps = eps[:(len(eps) - 1)]
				err = etcdutils.PromoteLearner(eps, learner)
				if err != nil {
					logger.Error(err, "Failed to promote learner")
					cm.SetProgressing(false, status.ReasonEtcdClientError, status.FormatError(fmt.Sprintf("Failed to promote learner %x", learner), err))
					cm.SetDegraded(true, status.ReasonEtcdClientError, status.FormatError(fmt.Sprintf("Failed to promote learner %x", learner), err))
					return ctrl.Result{}, err
				}
				// CHANGED BEHAVIOR: Promotion initiated, requeue shortly
				return ctrl.Result{RequeueAfter: requeueDuration}, nil
			} else {
				logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learner)
				cm.SetProgressing(true, status.ReasonWaitingForLearner, fmt.Sprintf("Waiting for learner %x to become ready for promotion", learner))
				return ctrl.Result{RequeueAfter: requeueDuration}, nil
			}
		}
	}

	// ---------------------------------------------------------------------
	// 8. Perform Scaling based on Spec.Size (if needed and no learner)
	// ---------------------------------------------------------------------
	if targetReplica != int32(etcdCluster.Spec.Size) {
		eps := clientEndpointsFromStatefulsets(sts)
		var scalingReason string
		var scalingMessage string

		// If there is no more learner, then we can proceed to scale the cluster further.
		// If there is no more member to add, the control will not reach here after the requeue
		if targetReplica < int32(etcdCluster.Spec.Size) {
			// scale out
			scalingReason = status.ReasonScalingUp
			scalingMessage = fmt.Sprintf("Scaling up from %d to %d", targetReplica, targetReplica+1)
			cm.SetProgressing(true, scalingReason, scalingMessage)
			cm.SetAvailable(false, scalingReason, "Cluster is scaling up")

			_, peerURL := peerEndpointForOrdinalIndex(etcdCluster, int(targetReplica)) // The index starts at 0, so we should do this before incrementing targetReplica
			targetReplica++                                                            // This is the new desired STS replica count
			logger.Info("[Scale out] adding a new learner member to etcd cluster", "peerURLs", peerURL)
			if _, err = etcdutils.AddMember(eps, []string{peerURL}, true); err != nil {
				logger.Error(err, "Failed to add learner member")
				cm.SetProgressing(false, status.ReasonEtcdClientError, status.FormatError("Failed to add member", err))
				cm.SetDegraded(true, status.ReasonEtcdClientError, status.FormatError("Failed to add member", err))
				return ctrl.Result{}, err
			}
			logger.Info("Learner member added successfully", "peerURLs", peerURL)
		} else {
			// scale in
			scalingReason = status.ReasonScalingDown
			targetReplica--
			logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", etcdCluster.Spec.Size)
			scalingMessage = fmt.Sprintf("Scaling down from %d to %d", targetReplica+1, targetReplica)
			cm.SetProgressing(true, scalingReason, scalingMessage)
			cm.SetAvailable(false, scalingReason, "Cluster is scaling down")

			memberID := healthInfos[memberCnt-1].Status.Header.MemberId
			logger.Info("[Scale in] removing one member", "memberID", memberID)
			eps = eps[:targetReplica]
			if err = etcdutils.RemoveMember(eps, memberID); err != nil {
				logger.Error(err, "Failed to remove member")
				cm.SetProgressing(false, status.ReasonEtcdClientError, status.FormatError(fmt.Sprintf("Failed to remove member %x", memberID), err))
				cm.SetDegraded(true, status.ReasonEtcdClientError, status.FormatError(fmt.Sprintf("Failed to remove member %x", memberID), err))
				return ctrl.Result{}, err
			}
			logger.Info("Member removed successfully", "memberID", memberID)
		}

		// Update StatefulSet to the new targetReplica count
		sts, err = reconcileStatefulSet(ctx, logger, etcdCluster, r.Client, targetReplica, r.Scheme)
		if err != nil {
			logger.Error(err, "Failed to update StatefulSet during scaling")
			cm.SetProgressing(false, status.ReasonResourceUpdateFail, status.FormatError("Failed to update STS during scaling", err))
			cm.SetDegraded(true, status.ReasonResourceUpdateFail, status.FormatError("Failed to update STS during scaling", err))
			return ctrl.Result{}, err
		}

		// CHANGED BEHAVIOR: Scaling action initiated, requeue to wait for stabilization
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// ---------------------------------------------------------------------
	// 9. Final State Check (Desired size reached, no learners)
	// ---------------------------------------------------------------------
	logger.Info("EtcdCluster is at the desired size and has no learners pending promotion")

	var allMembersHealthy bool
	allMembersHealthy, err = areAllMembersHealthy(sts, logger) // Re-check health
	if err != nil {
		logger.Error(err, "Final health check failed")
		cm.SetAvailable(false, status.ReasonHealthCheckError, status.FormatError("Final health check failed", err))
		cm.SetDegraded(true, status.ReasonHealthCheckError, status.FormatError("Final health check failed", err))
		cm.SetProgressing(true, status.ReasonHealthCheckError, "Retrying after final health check failure") // Keep progressing? Or just degraded?
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	if allMembersHealthy {
		logger.Info("EtcdCluster reconciled successfully and is healthy")
		cm.SetAvailable(true, status.ReasonClusterReady, "Cluster is fully available and healthy")
		cm.SetProgressing(false, status.ReasonReconcileSuccess, "Cluster reconciled to desired state")
		cm.SetDegraded(false, status.ReasonClusterReady, "Cluster is healthy") // Explicitly clear Degraded
		return ctrl.Result{}, nil                                              // Success! Defer will update status.
	} else {
		logger.Info("EtcdCluster reached desired size, but some members are unhealthy")
		cm.SetAvailable(false, status.ReasonMembersUnhealthy, "Cluster reached size, but some members are unhealthy")
		cm.SetProgressing(false, status.ReasonMembersUnhealthy, "Cluster reached size, reconciliation paused pending health")
		cm.SetDegraded(true, status.ReasonMembersUnhealthy, "Some members are unhealthy")
		return ctrl.Result{RequeueAfter: requeueDuration}, nil // Requeue to monitor health
	}

	// Defer function will execute here before returning
}

// derivePhaseFromConditions determines the overall Phase based on the set Conditions.
// NOTE: This logic needs refinement based on the exact conditions being set.
func (r *EtcdClusterReconciler) derivePhaseFromConditions(cluster *ecv1alpha1.EtcdCluster) {
	// Default to Pending if no other condition matches
	phase := "Pending"

	if cluster.Spec.Size == 0 {
		phase = "Idle"
	} else if meta.IsStatusConditionTrue(cluster.Status.Conditions, status.ConditionDegraded) {
		// Check for fatal reasons first
		degradedCondition := meta.FindStatusCondition(cluster.Status.Conditions, status.ConditionDegraded)
		if degradedCondition != nil {
			switch degradedCondition.Reason {
			case status.ReasonResourceCreateFail, status.ReasonResourceUpdateFail, status.ReasonEtcdClientError, status.ReasonNotOwnedResource, "InternalError": // Add more fatal reasons if needed
				phase = "Failed"
			default:
				phase = "Degraded"
			}
		} else {
			phase = "Degraded" // Should have reason if Degraded is True
		}
	} else if meta.IsStatusConditionTrue(cluster.Status.Conditions, status.ConditionProgressing) {
		// Determine specific progressing phase based on reason
		progressingCondition := meta.FindStatusCondition(cluster.Status.Conditions, status.ConditionProgressing)
		if progressingCondition != nil {
			switch progressingCondition.Reason {
			case status.ReasonCreatingResources:
				phase = "Creating"
			case status.ReasonInitializingCluster:
				phase = "Initializing"
			case status.ReasonScalingUp, status.ReasonScalingDown, status.ReasonMemberConfiguration, status.ReasonMembersMismatch:
				phase = "Scaling"
			case status.ReasonPromotingLearner, status.ReasonWaitingForLearner:
				phase = "PromotingLearner"
			default:
				phase = "Progressing" // Generic progressing state if reason not specific
			}
		} else {
			phase = "Progressing" // Should have reason if Progressing is True
		}
	} else if meta.IsStatusConditionTrue(cluster.Status.Conditions, status.ConditionAvailable) {
		phase = "Running"
	}
	// Add logic for Terminating Phase if finalizers are implemented

	cluster.Status.Phase = phase
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("etcdcluster-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}
