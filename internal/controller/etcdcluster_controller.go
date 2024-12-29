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
	"go.etcd.io/etcd-operator/internal/etcd"
	"go.etcd.io/etcd-operator/internal/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"strings"
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
	var etcdCluster ecv1alpha1.EtcdCluster

	err := r.Get(ctx, req.NamespacedName, &etcdCluster)
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

	logger.Info("Reconciling EtcdCluster", "spec", etcdCluster.Spec)

	// Get the statefulsets which has the same name as the EtcdCluster resource
	stsName := types.NamespacedName{Name: etcdCluster.Name, Namespace: etcdCluster.Namespace}
	var sts appsv1.StatefulSet
	err = r.Get(ctx, stsName, &sts)
	if err != nil && errors.IsNotFound(err) {
		if etcdCluster.Spec.Size > 0 {
			logger.Info("Creating StatefulSet with 0 replica", "expectedSize", etcdCluster.Spec.Size)
			status, errCreateSS := utils.CreateOrUpdateStatefulSet(ctx, r.Client, r.Scheme, &etcdCluster, 0)
			if errCreateSS != nil {
				logger.Error(err, "Failed to create StatefulSet")
				return ctrl.Result{}, errCreateSS
			}
			logger.Info(fmt.Sprintf("StatefulSet %s successfully", status))

			logger.Info("Now checking StatefulSet's readiness")
			if !ready(&sts, 0) {
				logger.Info("StatefulSet not ready", "readyReplicas", sts.Status.ReadyReplicas, "expected", 0)
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
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
	if !metav1.IsControlledBy(&sts, &etcdCluster) {
		msg := fmt.Sprintf(MessageResourceExists, sts.Name)
		r.Recorder.Event(&etcdCluster, corev1.EventTypeWarning, ErrResourceExists, msg)
		return ctrl.Result{}, fmt.Errorf("%s", msg)
	}

	memberListResp, healthInfos, err := healthCheck(&etcdCluster, &sts, logger)
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
			eps := clientEndpointsFromStatefulsets(&sts)
			eps = eps[:(len(eps) - 1)]
			err = etcd.PromoteLearner(eps, learnerID)
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

		_, peerURL := utils.PeerEndpointForOrdinalIndex(&etcdCluster, replica)
		if replica > 0 {
			// if replica == 0, then it's the very first member, then
			// there is no need to add it as a learner; instead we can
			// start it as a voting member directly.
			eps := clientEndpointsFromStatefulsets(&sts)
			logger.Info("[Scale out] adding a new learner member", "peerURLs", peerURL)
			if _, err := etcd.AddMember(eps, []string{peerURL}, true); err != nil {
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
		eps := clientEndpointsFromStatefulsets(&sts)
		eps = eps[:targetReplica]
		if err := etcd.RemoveMember(eps, memberID); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("Applying etcd cluster state")
	if err := applyEtcdClusterState(ctx, &etcdCluster, int(targetReplica), r.Client); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Updating statefulsets")
	_, err = utils.CreateOrUpdateStatefulSet(ctx, r.Client, r.Scheme, &etcdCluster, targetReplica)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Emit event for successful reconciliation
	r.Recorder.Eventf(&etcdCluster, corev1.EventTypeNormal, "Reconcile", "Reconciled StatefulSet for EtcdCluster %s", etcdCluster.Name)

	logger.Info("Reconciliation completed successfully", "name", etcdCluster.Name)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
func healthCheck(etcdCluster *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet, lg klog.Logger) (*clientv3.MemberListResponse, []etcd.EpHealth, error) {
	replica := int(*sts.Spec.Replicas)
	if replica == 0 {
		return nil, nil, nil
	}

	endpoints := clientEndpointsFromStatefulsets(sts)

	memberlistResp, err := etcd.MemberList(endpoints)
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

	healthInfos, err := etcd.ClusterHealth(endpoints)
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

func newEtcdClusterState(ec *ecv1alpha1.EtcdCluster, replica int) *corev1.ConfigMap {
	// We always add members one by one, so the state is always
	// "existing" if replica > 1.
	state := "new"
	if replica > 1 {
		state = "existing"
	}

	var initialCluster []string
	for i := 0; i < replica; i++ {
		name, peerURL := utils.PeerEndpointForOrdinalIndex(ec, i)
		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", name, peerURL))
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.ConfigMapNameForEtcdCluster(ec),
			Namespace: ec.Namespace,
		},
		Data: map[string]string{
			"ETCD_INITIAL_CLUSTER_STATE": state,
			"ETCD_INITIAL_CLUSTER":       strings.Join(initialCluster, ","),
		},
	}
}

func isLearnerReady(leaderStatus, learnerStatus *clientv3.StatusResponse) bool {
	leaderRev := leaderStatus.Header.Revision
	learnerRev := learnerStatus.Header.Revision

	learnerReadyPercent := float64(learnerRev) / float64(leaderRev)
	return learnerReadyPercent >= 0.9
}

func applyEtcdClusterState(ctx context.Context, ec *ecv1alpha1.EtcdCluster, replica int, c client.Client) error {
	cm := newEtcdClusterState(ec, replica)

	err := c.Get(ctx, types.NamespacedName{Name: utils.ConfigMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, &corev1.ConfigMap{})
	if err != nil && errors.IsNotFound(err) {
		createErr := c.Create(ctx, cm)
		return createErr
	}

	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("cannot find ConfigMap for EtcdCluster %s: %w", ec.Name, err)
	}

	updateErr := c.Update(ctx, cm)
	return updateErr
}
