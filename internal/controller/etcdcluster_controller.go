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
	cluster        *ecv1alpha1.EtcdCluster      // cluster custom resource currently being reconciled
	sts            *appsv1.StatefulSet          // associated StatefulSet for the cluster
	memberListResp *clientv3.MemberListResponse // member list fetched from the etcd cluster
	memberHealth   []etcdutils.EpHealth         // health information for each etcd member
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
	state, res, err := r.fetchAndValidateState(ctx, req)
	if state == nil || err != nil {
		return res, err
	}

	if bootstrapRes, err := r.bootstrapStatefulSet(ctx, state); err != nil || !bootstrapRes.IsZero() {
		return bootstrapRes, err
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
	s.memberListResp, s.memberHealth, err = healthCheck(s.sts, logger)
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
				eps := clientEndpointsFromStatefulsets(s.sts)
				eps = eps[:(len(eps) - 1)]
				if err := etcdutils.PromoteLearner(eps, learner); err != nil {
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

	eps := clientEndpointsFromStatefulsets(s.sts)

	// If there are no learners left, we can proceed to scale the cluster towards the desired size.
	// When there are no members to add, the controller will requeue above and this block won't execute.
	if targetReplica < int32(s.cluster.Spec.Size) {
		// scale out
		_, peerURL := peerEndpointForOrdinalIndex(s.cluster, int(targetReplica))
		targetReplica++
		logger.Info("[Scale out] adding a new learner member to etcd cluster", "peerURLs", peerURL)
		if _, err := etcdutils.AddMember(eps, []string{peerURL}, true); err != nil {
			return ctrl.Result{}, err
		}

		logger.Info("Learner member added successfully", "peerURLs", peerURL)

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
		if err := etcdutils.RemoveMember(eps, memberID); err != nil {
			return ctrl.Result{}, err
		}

		if s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, targetReplica, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Ensure every etcd member reports itself healthy before declaring success.
	allMembersHealthy, err := areAllMembersHealthy(s.sts, logger)
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
