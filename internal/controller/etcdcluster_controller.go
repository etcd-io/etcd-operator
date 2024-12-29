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
	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd-operator/internal/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"time"
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

const (
	// ErrResourceExists is used as part of the Event 'reason' when an EtcdCluster fails
	// to sync due to a Statefulsets of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Statefulsets already existing
	MessageResourceExists = "Resource %q already exists and is not managed by EtcdCluster"
)

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the EtcdCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.1/pkg/reconcile
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the EtcdCluster resource
	etcdCluster := &ecv1alpha1.EtcdCluster{}

	err := r.Get(ctx, req.NamespacedName, etcdCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("EtcdCluster resource not found. Ignoring since object may have been deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	/* Implement finalizer logic here
	const etcdClusterFinalizer = "etcdcluster.finalizers.example.com"

	// Finalizer logic for cleanup during deletion
	if !etcdCluster.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&etcdCluster, etcdClusterFinalizer) {
			// Perform cleanup tasks, e.g., delete persistent volumes or other resources
			logger.Info("Performing finalizer cleanup", "name", etcdCluster.Name)

			controllerutil.RemoveFinalizer(&etcdCluster, etcdClusterFinalizer)
			if err := r.Update(ctx, &etcdCluster); err != nil {
				logger.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(&etcdCluster, etcdClusterFinalizer) {
		controllerutil.AddFinalizer(&etcdCluster, etcdClusterFinalizer)
		if err := r.Update(ctx, &etcdCluster); err != nil {
			logger.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}*/

	//logger.Info("Reconciling EtcdCluster", "spec", etcdCluster.Spec)

	// Get the statefulsets which has the same name as the EtcdCluster resource
	stsName := types.NamespacedName{Name: etcdCluster.Name, Namespace: etcdCluster.Namespace}
	sts := &appsv1.StatefulSet{}
	err = r.Get(ctx, stsName, sts)
	if err != nil && errors.IsNotFound(err) {
		if etcdCluster.Spec.Size > 0 {
			logger.Info("Creating StatefulSet with 0 replica", "expectedSize", etcdCluster.Spec.Size)
			// Create a new StatefulSet

			err := utils.CreateOrPatchSS(ctx, logger, etcdCluster, &r.Client, 0, r.Scheme)
			if err != nil {
				return ctrl.Result{}, err
			}

			/*err = controllerutil.SetControllerReference(etcdCluster, sts, r.Scheme)
			if err != nil {
				return ctrl.Result{}, err
			}*/

			logger.Info("Now checking StatefulSet's readiness")
			if !ready(sts, 0) {
				logger.Info("StatefulSet not ready", "readyReplicas", sts.Status.ReadyReplicas, "expected", 0)
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}

			logger.Info("Creating Headless service to target this statefulset")
			// Create a headless service to target the statefulset
			err = utils.CreateHeadlessServiceIfDoesntExist(ctx, logger, r.Client, etcdCluster, r.Scheme)
			if err != nil {
				return ctrl.Result{}, err
			}

		} else {
			// Shouldn't ideally happen, just a safety net
			logger.Info("Skipping creating statefulsets due to the expected cluster size being 0")
			return ctrl.Result{}, nil
		}
	}

	// If an error occurs during Get/Create, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil && !errors.IsNotFound(err) {
		logger.Error(err, "Failed to get StatefulSet..Requesting requeue")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// If the Statefulsets is not controlled by this EtcdCluster resource, we should log
	// a warning to the event recorder and return error msg.
	err = utils.CheckStatefulSetControlledByEtcdOperator(ctx, r.Client, etcdCluster)
	if err != nil {
		logger.Error(err, "StatefulSet is not controlled by this EtcdCluster resource")
		return ctrl.Result{}, err
	}

	// If statefulset size is 0. try to instantiate the cluster with 1 member
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas == 0 && etcdCluster.Spec.Size > 0 {
		logger.Info("StatefulSet has 0 replicas. Trying to create a new cluster with 1 member")

		//err := scaleStatefulSetToDesiredReplicas(ctx, logger, sts, stsName, etcdCluster, 1, r)
		err := utils.CreateOrPatchSS(ctx, logger, etcdCluster, &r.Client, 1, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}

	}

	ssReady, err := utils.WaitForStatefulSetReady(ctx, logger, r.Client, etcdCluster.Name, etcdCluster.Namespace)
	// This will be a loop to check the status of the statefulset and proceed only when the statefulset is ready
	if err != nil {
		return ctrl.Result{}, err
	}

	if !ssReady {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Just a safety net to check service before we do health checks
	err = utils.CreateHeadlessServiceIfDoesntExist(ctx, logger, r.Client, etcdCluster, r.Scheme)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Get latest object before proceeding
	err = r.Get(ctx, stsName, sts)
	if err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Now checking health of the cluster members")
	memberListResp, healthInfos, err := healthCheck(etcdCluster, sts, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("health check failed: %w", err)
	}

	memberCnt := 0
	if memberListResp != nil {
		memberCnt = len(memberListResp.Members)
	}
	/*if memberCnt != len(healthInfos) {
		// Only proceed when all members are healthy
		return ctrl.Result{}, fmt.Errorf("memberCnt (%d) isn't equal to healthy member count (%d)", memberCnt, len(healthInfos))
	}*/

	var learnerStatus *clientv3.StatusResponse
	var learner uint64
	var leader uint64
	var leaderStatus *clientv3.StatusResponse
	if memberCnt > 0 {
		// Find the leader status
		for i := range healthInfos {
			status := healthInfos[i].Status
			if status.Leader == status.Header.MemberId {
				leader = status.Header.MemberId
				leaderStatus = status
				break
			}
		}

		if leaderStatus == nil {
			// If the leader is not available, let's wait for the leader to be elected
			return ctrl.Result{}, fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
		} else {
			logger.Info("Leader found", "leaderID", leader)
		}

		logger.Info("Now checking if there is any pending learner member that needs to be promoted")
		for i := range healthInfos {
			if healthInfos[i].Status.IsLearner {
				learner = healthInfos[i].Status.Header.MemberId
				learnerStatus = healthInfos[i].Status
				logger.Info("Learner member found", "memberID", learner)
				break
			}
		}
	}

	//  Check if the size of the stateful set is less than expected size
	//  Or if there is a pending learner to be promoted
	ssSize, err := utils.IsStatefulSetDesiredSize(ctx, etcdCluster, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}
	if ssSize == utils.SSSmallerThanDesired || learner > 0 {
		// There should be at most one learner
		if memberCnt > 0 {
			if learner == 0 {
				// If there is no more learner, then we can proceed to scale the cluster further.
				// If there is no more member to add, the control will not reach here after the requeue

				targetReplica := int(*sts.Spec.Replicas) + 1
				_, peerURL := utils.PeerEndpointForOrdinalIndex(etcdCluster, int(*sts.Spec.Replicas)) // The index starts at 0, so we do not need to add 1
				eps := clientEndpointsFromStatefulsets(sts)
				logger.Info("[Scale out] adding a new learner member to etcd cluster", "peerURLs", peerURL)
				if _, err := etcdutils.AddMember(eps, []string{peerURL}, true); err != nil {
					return ctrl.Result{}, err
				}

				logger.Info("Learner member added successfully", "peerURLs", peerURL)

				//err := scaleStatefulSetToDesiredReplicas(ctx, logger, sts, stsName, etcdCluster, targetReplica, r)
				err := utils.CreateOrPatchSS(ctx, logger, etcdCluster, &r.Client, int32(targetReplica), r.Scheme)
				if err != nil {
					return ctrl.Result{}, err
				}
			} else {
				// There is at least one learner. Let's try to promote it or wait
				// Find the learner status
				logger.Info("Learner found", "learnedID", learner)
				//logger.Info("There is a learner promotion pending..will wait for it to be promoted before moving further", "learnerID", learner)
				logger.Info("Learner status", "learnerStatus", learnerStatus)
				if isLearnerReady(leaderStatus, learnerStatus) {
					logger.Info("Learner is ready to be promoted to voting member", "learnerID", learner)
					logger.Info("Promoting the learner member", "learnerID", learner)
					eps := clientEndpointsFromStatefulsets(sts)
					eps = eps[:(len(eps) - 1)]
					err = etcdutils.PromoteLearner(eps, learner)
					if err != nil {
						// The member is not promoted yet, so we error out
						return ctrl.Result{}, err
					}
				} else {
					// Learner is not yet ready. We can't add another learner until this one is promoted
					// So let's requeue
					logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learner)
					return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
				}
			}
		}
	}

	if ssSize == utils.SSBiggerThanDesired {
		// scale in
		targetReplica := int(*sts.Spec.Replicas) - 1
		logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", etcdCluster.Spec.Size)

		memberID := healthInfos[memberCnt-1].Status.Header.MemberId

		logger.Info("[Scale in] removing one member", "memberID", memberID)
		eps := clientEndpointsFromStatefulsets(sts)
		eps = eps[:targetReplica]
		if err := etcdutils.RemoveMember(eps, memberID); err != nil {
			return ctrl.Result{}, err
		}
		//err := scaleStatefulSetToDesiredReplicas(ctx, logger, sts, stsName, etcdCluster, targetReplica, r)
		err := utils.CreateOrPatchSS(ctx, logger, etcdCluster, &r.Client, int32(targetReplica), r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	allMembersHealthy, err := areAllMembersHealthy(etcdCluster, sts, logger)
	if err != nil {
		return ctrl.Result{}, err
	}

	ssReady, err = utils.WaitForStatefulSetReady(ctx, logger, r.Client, etcdCluster.Name, etcdCluster.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	ssSize, err = utils.IsStatefulSetDesiredSize(ctx, etcdCluster, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	if ssSize != utils.SSEqualToDesired || !ssReady || !allMembersHealthy {
		// Requeue if the statefulset size is not equal to the expected size of ETCD cluster
		// Or if the statefulset is not ready
		// Or if all members of the cluster are not healthy
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	logger.Info("EtcdCluster reconciled successfully")
	return ctrl.Result{}, nil

	/*
		// Now that it's guaranteed that the statefulset is ready, let's check the health of the members
		memberListResp, healthInfos, err := healthCheck(etcdCluster, sts, logger)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("health check failed: %w", err)
		}

		memberCnt := 0
		if memberListResp != nil {
			memberCnt = len(memberListResp.Members)
		}

		replica := int(*sts.Spec.Replicas)

		if replica != memberCnt {
			// TODO: finish the logic later
			if replica < memberCnt {
				// a new added learner hasn't started yet

				// re-generate configuration for the new learner member;
				// increase statefulsets's replica by 1
			} else {
				// an already removed member hasn't stopped yet.

				// Decrease the statefulsets's replica by 1
			}
			// return
		}

		if memberCnt != len(healthInfos) {
			return ctrl.Result{}, fmt.Errorf("memberCnt (%d) isn't equal to healthy member count (%d)", memberCnt, len(healthInfos))
		}

		// There should be at most one learner, namely the last one
		if memberCnt > 0 && healthInfos[memberCnt-1].Status.IsLearner {
			logger = logger.WithValues("replica", replica, "expectedSize", etcdCluster.Spec.Size)

			learnerStatus := healthInfos[memberCnt-1].Status

			var leaderStatus *clientv3.StatusResponse
			for i := 0; i < memberCnt-1; i++ {
				status := healthInfos[i].Status
				if status.Leader == status.Header.MemberId {
					leaderStatus = status
					break
				}
			}

			if leaderStatus == nil {
				return ctrl.Result{}, fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
			}

			learnerID := healthInfos[memberCnt-1].Status.Header.MemberId
			if isLearnerReady(leaderStatus, learnerStatus) {
				logger.Info("Promoting the learner member", "learnerID", learnerID)
				eps := clientEndpointsFromStatefulsets(sts)
				eps = eps[:(len(eps) - 1)]
				err = etcdutils.PromoteLearner(eps, learnerID)
				if err != nil {
					return ctrl.Result{}, err
				}
			}

			logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learnerID)

			return ctrl.Result{}, nil
		}

		expectedSize := etcdCluster.Spec.Size
		if replica == expectedSize {
			// TODO: check version change, and perform upgrade if needed.
			return ctrl.Result{}, nil
		}

		var targetReplica int32

		if replica < expectedSize {
			// scale out
			targetReplica = int32(replica + 1)
			logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", etcdCluster.Spec.Size)

			// TODO: check PV & PVC for the new member. If they already exist,
			// then it means they haven't been cleaned up yet when scaling in.

			_, peerURL := utils.PeerEndpointForOrdinalIndex(etcdCluster, replica)
			if replica > 0 {
				// if replica == 0, then it's the very first member, then
				// there is no need to add it as a learner; instead we can
				// start it as a voting member directly.
				eps := clientEndpointsFromStatefulsets(sts)
				logger.Info("[Scale out] adding a new learner member", "peerURLs", peerURL)
				if _, err := etcdutils.AddMember(eps, []string{peerURL}, true); err != nil {
					return ctrl.Result{}, err
				}
			} else {
				logger.Info("[Scale out] Starting the very first voting member", "peerURLs", peerURL)
			}
		} else {
			// scale in
			targetReplica = int32(replica - 1)
			logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", etcdCluster.Spec.Size)

			memberID := healthInfos[memberCnt-1].Status.Header.MemberId

			logger.Info("[Scale in] removing one member", "memberID", memberID)
			eps := clientEndpointsFromStatefulsets(sts)
			eps = eps[:targetReplica]
			if err := etcdutils.RemoveMember(eps, memberID); err != nil {
				return ctrl.Result{}, err
			}
		}

		logger.Info("Applying etcd cluster state")
		if err := applyEtcdClusterState(ctx, etcdCluster, int(targetReplica), r.Client); err != nil {
			return ctrl.Result{}, err
		}

		logger.Info("Updating statefulsets")
		// Get latest object before Updating
		err = r.Get(ctx, stsName, sts)
		if err != nil {
			return ctrl.Result{}, err
		}
		// Update the Statefulsets size to the expected size
		sts.Spec.Replicas = &targetReplica
		err = r.Client.Update(ctx, sts)
		if err != nil {
			return ctrl.Result{}, err
		}

		// Emit event for successful reconciliation
		r.Recorder.Eventf(etcdCluster, corev1.EventTypeNormal, "Reconcile", "Reconciled StatefulSet for EtcdCluster %s", etcdCluster.Name)
	*/
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("etcdcluster-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

func ready(sts *appsv1.StatefulSet, replica int32) bool {
	return sts.Status.ReadyReplicas == replica
}

/*// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdCluster{}).
		Named("etcdcluster").
		Complete(r)
}*/

// healthCheck returns a memberList and an error.
// If any member (excluding not yet started or already removed member)
// is unhealthy, the error won't be nil.
func healthCheck(etcdCluster *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet, lg klog.Logger) (*clientv3.MemberListResponse, []etcdutils.EpHealth, error) {
	replica := int(*sts.Spec.Replicas)
	if replica == 0 {
		return nil, nil, nil
	}

	endpoints := clientEndpointsFromStatefulsets(sts)

	memberlistResp, err := etcdutils.MemberList(endpoints)
	if err != nil {
		return nil, nil, err
	}
	memberCnt := len(memberlistResp.Members)

	// Usually replica should be equal to memberCnt. If it isn't, then
	// it means previous reconcile loop somehow interrupted right after
	// adding (replica < memberCnt) or removing (replica > memberCnt)
	// a member from the cluster. In that case, we shouldn't run health
	// check on the not yet started or already removed member.
	cnt := min(replica, memberCnt)

	lg.Info("health checking", "replica", replica, "len(members)", memberCnt)
	endpoints = endpoints[:cnt]

	healthInfos, err := etcdutils.ClusterHealth(endpoints)
	if err != nil {
		return memberlistResp, nil, err
	}

	for _, healthInfo := range healthInfos {
		if !healthInfo.Health {
			// TODO: also update metrics?
			return memberlistResp, healthInfos, fmt.Errorf(healthInfo.String())
		}
		lg.Info(healthInfo.String())
	}

	return memberlistResp, healthInfos, nil
}

func clientEndpointsFromStatefulsets(sts *appsv1.StatefulSet) []string {
	var endpoints []string
	replica := int(*sts.Spec.Replicas)
	if replica > 0 {
		for i := 0; i < replica; i++ {
			endpoints = append(endpoints, clientEndpointForOrdinalIndex(sts, i))
		}
	}
	return endpoints
}

func clientEndpointForOrdinalIndex(sts *appsv1.StatefulSet, index int) string {
	return fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379",
		sts.Name, index, sts.Name, sts.Namespace)
}

func isLearnerReady(leaderStatus, learnerStatus *clientv3.StatusResponse) bool {
	leaderRev := leaderStatus.Header.Revision
	learnerRev := learnerStatus.Header.Revision

	learnerReadyPercent := float64(learnerRev) / float64(leaderRev)
	return learnerReadyPercent >= 0.9
}

/*func scaleStatefulSetToDesiredReplicas(ctx context.Context, logger logr.Logger, sts *appsv1.StatefulSet, stsName types.NamespacedName, etcdCluster *ecv1alpha1.EtcdCluster, targetReplica int, r *EtcdClusterReconciler) error {
	logger.Info("Applying etcd cluster state")
	err := applyEtcdClusterState(ctx, etcdCluster, targetReplica, r.Client)
	if err != nil {
		return err
	}

	// Get latest object before Updating

	err = r.Get(ctx, stsName, sts)
	if err != nil {
		return err
	}

	logger.Info("Patching statefulsets to desired size of %d replicas", targetReplica)
	patch := sts.DeepCopy()
	patch.Spec.Replicas = utils.IntToInt32Ptr(targetReplica)
	// Update the Statefulsets size to the expected size
	err = r.Client.Patch(ctx, patch, client.MergeFrom(sts))
	if err != nil {
		return err
	}
	return nil
}*/

/*func isStatefulSetReady(sts *appsv1.StatefulSet, logger logr.Logger) bool {
	// This will be a loop to check the status of the statefulset and proceed only when the statefulset is ready
	if err := utils.WaitForStatefulSetReady()
	logger.Info("StatefulSet not ready", "readyReplicas", sts.Status.ReadyReplicas, "expected", *sts.Spec.Replicas)
	return false
}*/

func areAllMembersHealthy(etcdCluster *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet, logger klog.Logger) (bool, error) {
	_, health, err := healthCheck(etcdCluster, sts, logger)
	if err != nil {
		return false, err
	}

	for _, h := range health {
		if !h.Health {
			return false, nil
		}
	}
	return true, nil
}
