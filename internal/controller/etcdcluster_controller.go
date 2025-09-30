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
	calculatedStatus := etcdCluster.Status // Start with current status

	// Initialize ConditionManager using the calculatedStatus *copy*.
	// Modifications via cm will only affect this copy until the patch is applied.
	observedGeneration := etcdCluster.Generation
	cm := status.NewManager(&calculatedStatus.Conditions, observedGeneration)

	// Defer the actual patch operation. The mutate function will apply the final 'calculatedStatus'.
	defer func() {
		// Using the named return value `err`, we can determine if the reconciliation
		// was successful before patching the status.
		// We should only update ObservedGeneration if the reconciliation cycle completed without an error.
		if err == nil {
			// If we are about to exit without an error, it means we have successfully
			// processed the current spec generation.
			calculatedStatus.ObservedGeneration = etcdCluster.Generation
		}

		// Call PatchStatusMutate. The mutate function copies the calculatedStatus
		// onto the latest version fetched inside the patch retry loop.
		patchErr := status.PatchStatusMutate(ctx, r.Client, etcdCluster, func(latest *ecv1alpha1.EtcdCluster) error {
			// Apply the status we calculated during *this* reconcile cycle
			latest.Status = calculatedStatus
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

	// Determine desired etcd image registry
	if etcdCluster.Spec.ImageRegistry == "" {
		etcdCluster.Spec.ImageRegistry = r.ImageRegistry
	}

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
		calculatedStatus.CurrentReplicas = 0
		calculatedStatus.MemberCount = 0
		cm.SetAvailable(false, status.ReasonSizeIsZero, "Desired cluster size is 0")
		cm.SetProgressing(false, status.ReasonSizeIsZero, "Desired cluster size is 0")
		return ctrl.Result{}, nil
	}

	// ---------------------------------------------------------------------
	// 4. Reconcile Core Resources (STS, Service) & Handle Errors
	// ---------------------------------------------------------------------

	// Create Client Certificate for etcd-operator to communicate with the EtcdCluster
	if etcdCluster.Spec.TLS != nil {
		clientCertErr := createClientCertificate(ctx, etcdCluster, r.Client)
		if clientCertErr != nil {
			logger.Error(clientCertErr, "Failed to create Client Certificate.")
		}
	} else {
		// TODO: instead of logging error, set default autoConfig
		logger.Error(nil, fmt.Sprintf("missing TLS config for %s,\n running etcd-cluster without TLS protection is NOT recommended for production.", etcdCluster.Name))
	}
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
				cm.SetAvailable(false, status.ReasonResourceCreateFail, status.FormatError("Failed to create StatefulSet", err))
				cm.SetProgressing(false, status.ReasonResourceCreateFail, status.FormatError("Failed to create StatefulSet", err))
				cm.SetDegraded(true, status.ReasonResourceCreateFail, status.FormatError("Failed to create StatefulSet", err))
				return ctrl.Result{}, err
			}
			r.updateStatusFromStatefulSet(&calculatedStatus, sts) // Update after creation
			// STS created with 0 replicas, needs scaling to 1. Set Initializing.
			cm.SetProgressing(true, status.ReasonInitializingCluster, "StatefulSet created with 0 replicas, requires scaling to 1")
		} else {
			// If an error occurs during Get/Create, we'll requeue the item, so we can
			// attempt processing again later. This could have been caused by a
			// temporary network failure, or any other transient reason.
			logger.Error(err, "Failed to get StatefulSet. Requesting requeue")
			r.updateStatusFromStatefulSet(&calculatedStatus, nil)
			cm.SetAvailable(false, status.ReasonStatefulSetGetError, status.FormatError("Failed to get StatefulSet", err))
			cm.SetProgressing(false, status.ReasonStatefulSetGetError, status.FormatError("Failed to get StatefulSet", err)) // Stop progressing on persistent get error
			cm.SetDegraded(true, status.ReasonStatefulSetGetError, status.FormatError("Failed to get StatefulSet", err))
			r.updateStatusFromStatefulSet(&calculatedStatus, nil)
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	} else { // StatefulSet was found
		r.updateStatusFromStatefulSet(&calculatedStatus, sts)
	}

	// At this point, sts should exist (either found or created)
	if sts == nil {
		// This case should ideally not happen if error handling above is correct
		err = fmt.Errorf("statefulSet is unexpectedly nil after get/create")
		logger.Error(err, "Internal error")
		cm.SetAvailable(false, status.ReasonReconcileError, err.Error())
		cm.SetDegraded(true, "InternalError", err.Error())
		r.updateStatusFromStatefulSet(&calculatedStatus, nil)
		return ctrl.Result{}, err
	}

	// Update ReadyReplicas based on current STS status
	calculatedStatus.ReadyReplicas = sts.Status.ReadyReplicas

	// If the Statefulsets is not controlled by this EtcdCluster resource, we should log
	// a warning to the event recorder and return error msg.
	err = checkStatefulSetControlledByEtcdOperator(etcdCluster, sts)
	if err != nil {
		logger.Error(err, "StatefulSet is not controlled by this EtcdCluster resource")
		cm.SetAvailable(false, status.ReasonNotOwnedResource, err.Error())
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
			cm.SetAvailable(false, status.ReasonResourceUpdateFail, status.FormatError("Failed to scale StatefulSet to 1", err))
			cm.SetProgressing(false, status.ReasonResourceUpdateFail, status.FormatError("Failed to scale StatefulSet to 1", err))
			cm.SetDegraded(true, status.ReasonResourceUpdateFail, status.FormatError("Failed to scale StatefulSet to 1", err))
			return ctrl.Result{}, err
		}
		r.updateStatusFromStatefulSet(&calculatedStatus, sts)
		// return ctrl.Result{RequeueAfter: requeueDuration}, nil // Requeue to check readiness, should we do it?
	}

	// Create Headless Service
	err = createHeadlessServiceIfNotExist(ctx, logger, r.Client, etcdCluster, r.Scheme)
	if err != nil {
		logger.Error(err, "Failed to create Headless Service")
		cm.SetAvailable(false, status.ReasonResourceCreateFail, status.FormatError("Failed to ensure Headless Service", err))
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

		// Clear stale data on error to avoid using stale member status
		calculatedStatus.Members = nil
		calculatedStatus.MemberCount = 0
		calculatedStatus.LeaderId = ""
		calculatedStatus.CurrentVersion = ""
		return ctrl.Result{}, fmt.Errorf("health check failed: %w", err)
	}
	// calculatedStatus.LastHealthCheckTime = metav1.Now() // TODO: Add this field in a future iteration.
	// TODO: Populate UnhealthyMembers list based on healthInfos if exposure becomes necessary.

	// Update member count
	memberCnt := 0
	if memberListResp != nil {
		memberCnt = len(memberListResp.Members)
	}
	calculatedStatus.MemberCount = int32(memberCnt)

	var (
		leaderStatus *clientv3.StatusResponse
		leaderIDHex  string
	)
	// Default to empty leader identity/version. We'll overwrite once a leader is confirmed.
	calculatedStatus.LeaderId = ""
	calculatedStatus.CurrentVersion = ""

	if memberCnt > 0 {
		_, leaderStatus = etcdutils.FindLeaderStatus(healthInfos, logger)
		if leaderStatus != nil && leaderStatus.Header != nil {
			leaderIDHex = fmt.Sprintf("%x", leaderStatus.Header.MemberId)
			calculatedStatus.LeaderId = leaderIDHex
			if leaderStatus.Version != "" {
				calculatedStatus.CurrentVersion = leaderStatus.Version
			}
		}
	}

	// Populate the detailed member status slice using our new helper function.
	calculatedStatus.Members = r.populateMemberStatuses(ctx, memberListResp, healthInfos, leaderIDHex)

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
			cm.SetAvailable(false, status.ReasonResourceUpdateFail, status.FormatError("Failed to adjust StatefulSet replicas to match member count", err))
			cm.SetProgressing(false, status.ReasonResourceUpdateFail, status.FormatError("Failed to adjust StatefulSet replicas to match member count", err))
			cm.SetDegraded(true, status.ReasonResourceUpdateFail, status.FormatError("Failed to adjust StatefulSet replicas to match member count", err))
			return ctrl.Result{}, err
		}
		r.updateStatusFromStatefulSet(&calculatedStatus, sts)
		// Requeue to check state after adjustment
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// ---------------------------------------------------------------------
	// 7. Handle Learners
	// ---------------------------------------------------------------------
	var (
		learnerStatus *clientv3.StatusResponse
		learner       uint64
	)
	if memberCnt > 0 {
		if leaderStatus == nil {
			err = fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
			logger.Error(err, "Leader election might be in progress or cluster unhealthy")
			cm.SetAvailable(false, status.ReasonLeaderNotFound, err.Error())
			cm.SetDegraded(true, status.ReasonLeaderNotFound, err.Error())
			cm.SetProgressing(true, status.ReasonLeaderNotFound, "Waiting for leader election") // Still progressing towards stable state
			// If the leader is not available, let's wait for the leader to be elected
			return ctrl.Result{RequeueAfter: requeueDuration}, err
		}

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

			// TODO: The current logic for selecting a member to remove is not robust.
			// It assumes the last member in `healthInfos` (sorted alphabetically by endpoint)
			// is the member with the highest ordinal index. This can be incorrect under
			// certain naming conventions (e.g., with more than 10 pods) and could lead
			// to removing the wrong member.
			// A robust implementation should find the member to remove by its pod name
			// (e.g., "etcd-cluster-N") instead of relying on slice order.
			// This will be addressed in a future PR to keep this change focused.
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
		r.updateStatusFromStatefulSet(&calculatedStatus, sts)

		// CHANGED BEHAVIOR: Scaling action initiated, requeue to wait for stabilization
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// ---------------------------------------------------------------------
	// 9. Final State Check (Desired size reached, no learners)
	// ---------------------------------------------------------------------
	logger.Info("EtcdCluster is at the desired size and has no learners pending promotion")

	r.updateStatusFromStatefulSet(&calculatedStatus, sts)
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
		cm.SetDegraded(false, status.ReasonClusterHealthy, "Cluster is healthy") // Explicitly clear Degraded
		return ctrl.Result{}, nil                                                // Success! Defer will update status.
	} else {
		logger.Info("EtcdCluster reached desired size, but some members are unhealthy")
		cm.SetAvailable(false, status.ReasonMembersUnhealthy, "Cluster reached size, but some members are unhealthy")
		cm.SetProgressing(false, status.ReasonMembersUnhealthy, "Cluster reached size, reconciliation paused pending health")
		cm.SetDegraded(true, status.ReasonMembersUnhealthy, "Some members are unhealthy")
		return ctrl.Result{RequeueAfter: requeueDuration}, nil // Requeue to monitor health
	}

	// Defer function will execute here before returning
}

func (r *EtcdClusterReconciler) updateStatusFromStatefulSet(
	etcdClusterStatus *ecv1alpha1.EtcdClusterStatus, // Pass the status struct to modify
	sts *appsv1.StatefulSet,
) {
	if sts == nil {
		// If sts is nil, perhaps after a failed creation attempt.
		// Set to 0 or ensure prior logic handles default state.
		etcdClusterStatus.CurrentReplicas = 0
		etcdClusterStatus.ReadyReplicas = 0
		return
	}

	if sts.Spec.Replicas != nil {
		etcdClusterStatus.CurrentReplicas = *sts.Spec.Replicas
	} else {
		etcdClusterStatus.CurrentReplicas = 0 // Should not occur with a valid STS spec
	}
	etcdClusterStatus.ReadyReplicas = sts.Status.ReadyReplicas
}

// populateMemberStatuses translates the raw health and member info from etcd into the
// structured MemberStatus API type.
func (r *EtcdClusterReconciler) populateMemberStatuses(
	ctx context.Context,
	memberListResp *clientv3.MemberListResponse,
	healthInfos []etcdutils.EpHealth,
	leaderID string,
) []ecv1alpha1.MemberStatus {
	logger := log.FromContext(ctx)
	// If there's no member list, there's nothing to populate.
	if memberListResp == nil || len(memberListResp.Members) == 0 {
		return nil
	}

	// Create a map of health info keyed by member ID for efficient lookups.
	healthMap := make(map[uint64]etcdutils.EpHealth)
	for _, hi := range healthInfos {
		if hi.Status != nil && hi.Status.Header != nil {
			healthMap[hi.Status.Header.MemberId] = hi
		}
	}

	// Allocate space for the resulting statuses.
	statuses := make([]ecv1alpha1.MemberStatus, 0, len(memberListResp.Members))

	// Iterate through the canonical member list from etcd.
	for _, etcdMember := range memberListResp.Members {
		// Start building the status for this specific member.
		memberIDHex := fmt.Sprintf("%x", etcdMember.ID)
		ms := ecv1alpha1.MemberStatus{
			ID:        memberIDHex,
			Name:      etcdMember.Name,
			IsLearner: etcdMember.IsLearner,
		}

		if leaderID != "" && memberIDHex == leaderID {
			ms.IsLeader = true
		}

		// Look up the detailed health status for this member in the map.
		if healthInfo, found := healthMap[etcdMember.ID]; found {
			// A health record was found for this member.
			ms.IsHealthy = healthInfo.Health

			// Populate fields from the detailed StatusResponse if it exists.
			if healthInfo.Status != nil {
				ms.Version = healthInfo.Status.Version
			}
		} else {
			// A member was present in the member list but we couldn't get its
			// individual health status (e.g., its endpoint was unreachable during the check).
			ms.IsHealthy = false
			logger.Info("No detailed health status found for etcd member, marking as unhealthy by default", "memberID", ms.ID, "memberName", ms.Name)
		}

		statuses = append(statuses, ms)
	}

	return statuses
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
