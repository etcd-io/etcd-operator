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

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	RequeueDuration = 10 * time.Second
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

	if etcdCluster.Spec.Size == 0 {
		logger.Info("EtcdCluster size is 0..Skipping next steps")
		return ctrl.Result{}, nil
	}

	// TODO: Implement finalizer logic here

	logger.Info("Reconciling EtcdCluster", "spec", etcdCluster.Spec)

	// Get the statefulsets which has the same name as the EtcdCluster resource
	sts, err := getStatefulSet(ctx, r.Client, etcdCluster.Name, etcdCluster.Namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Creating StatefulSet with 0 replica", "expectedSize", etcdCluster.Spec.Size)
			// Create a new StatefulSet

			sts, err = reconcileStatefulSet(ctx, logger, etcdCluster, r.Client, 0, r.Scheme)
			if err != nil {
				return ctrl.Result{}, err
			}
		} else {
			// If an error occurs during Get/Create, we'll requeue the item so we can
			// attempt processing again later. This could have been caused by a
			// temporary network failure, or any other transient reason.
			logger.Error(err, "Failed to get StatefulSet. Requesting requeue")
			return ctrl.Result{RequeueAfter: RequeueDuration}, nil
		}
	}

	// If the Statefulsets is not controlled by this EtcdCluster resource, we should log
	// a warning to the event recorder and return error msg.
	err = checkStatefulSetControlledByEtcdOperator(etcdCluster, sts)
	if err != nil {
		logger.Error(err, "StatefulSet is not controlled by this EtcdCluster resource")
		return ctrl.Result{}, err
	}

	// If statefulset size is 0. try to instantiate the cluster with 1 member
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas == 0 {
		logger.Info("StatefulSet has 0 replicas. Trying to create a new cluster with 1 member")

		sts, err = reconcileStatefulSet(ctx, logger, etcdCluster, r.Client, 1, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	err = createHeadlessServiceIfNotExist(ctx, logger, r.Client, etcdCluster, r.Scheme)
	if err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Now checking health of the cluster members")
	memberListResp, healthInfos, err := healthCheck(sts, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("health check failed: %w", err)
	}

	memberCnt := 0
	if memberListResp != nil {
		memberCnt = len(memberListResp.Members)
	}

	if memberCnt != len(healthInfos) {
		// Only proceed when all members are healthy
		return ctrl.Result{}, fmt.Errorf("memberCnt (%d) isn't equal to healthy member count (%d)", memberCnt, len(healthInfos))
	}

	var (
		learnerStatus *clientv3.StatusResponse
		learner       uint64
		leaderStatus  *clientv3.StatusResponse
	)

	if memberCnt > 0 {
		// Find the leader status
		_, leaderStatus = etcdutils.FindLeaderStatus(healthInfos, logger)

		if leaderStatus == nil {
			// If the leader is not available, let's wait for the leader to be elected
			return ctrl.Result{}, fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
		}

		learner, learnerStatus = etcdutils.FindLearnerStatus(healthInfos, logger)

		if learner > 0 {
			// There is at least one learner. Let's try to promote it or wait
			// Find the learner status
			logger.Info("Learner found", "learnedID", learner, "learnerStatus", learnerStatus)
			if etcdutils.IsLearnerReady(leaderStatus, learnerStatus) {
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
				// Learner is not yet ready. We can't add another learner or proceed further until this one is promoted
				// So let's requeue
				logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learner)
				return ctrl.Result{RequeueAfter: RequeueDuration}, nil
			}
		}
	}

	targetReplica := *sts.Spec.Replicas // Start with the current size of the stateful set
	eps := clientEndpointsFromStatefulsets(sts)

	// TODO: finish the logic later
	if int(targetReplica) != memberCnt {
		// TODO: finish the logic later
		if int(targetReplica) < memberCnt {
			// a new added learner hasn't started yet

			// re-generate configuration for the new learner member;
			// increase statefulsets's replica by 1
		} else {
			// an already removed member hasn't stopped yet.

			// Decrease the statefulsets's replica by 1
		}
		// return
	}

	//  Check if the size of the stateful set is less than expected size
	if targetReplica < int32(etcdCluster.Spec.Size) {
		// If there is no more learner, then we can proceed to scale the cluster further.
		// If there is no more member to add, the control will not reach here after the requeue

		_, peerURL := peerEndpointForOrdinalIndex(etcdCluster, int(targetReplica)) // The index starts at 0, so we should do this before incrementing targetReplica
		targetReplica++
		logger.Info("[Scale out] adding a new learner member to etcd cluster", "peerURLs", peerURL)
		if _, err := etcdutils.AddMember(eps, []string{peerURL}, true); err != nil {
			return ctrl.Result{}, err
		}

		logger.Info("Learner member added successfully", "peerURLs", peerURL)
	}

	//  Check if the size of the stateful set is bigger than expected size
	if targetReplica > int32(etcdCluster.Spec.Size) {
		// scale in
		targetReplica--
		logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", etcdCluster.Spec.Size)

		memberID := healthInfos[memberCnt-1].Status.Header.MemberId

		logger.Info("[Scale in] removing one member", "memberID", memberID)
		eps = eps[:targetReplica]
		if err := etcdutils.RemoveMember(eps, memberID); err != nil {
			return ctrl.Result{}, err
		}
	}

	sts, err = reconcileStatefulSet(ctx, logger, etcdCluster, r.Client, targetReplica, r.Scheme)
	if err != nil {
		return ctrl.Result{}, err
	}

	allMembersHealthy, err := areAllMembersHealthy(sts, logger)
	if err != nil {
		return ctrl.Result{}, err
	}

	if *sts.Spec.Replicas != int32(etcdCluster.Spec.Size) || !allMembersHealthy {
		// Requeue if the statefulset size is not equal to the expected size of ETCD cluster
		// Or if all members of the cluster are not healthy
		return ctrl.Result{RequeueAfter: RequeueDuration}, nil
	}

	logger.Info("EtcdCluster reconciled successfully")
	return ctrl.Result{}, nil

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
