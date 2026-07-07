package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
)

func desiredEtcdImage(ec *ecv1alpha1.EtcdCluster) string {
	return fmt.Sprintf("%s:%s", ec.Spec.ImageRegistry, ec.Spec.Version)
}

// ordinalFromPeerEp extracts the pod ordinal from a health-report endpoint
// ("http://{stsName}-{i}.{svc}...:2379"). memberHealth is sorted
// lexicographically by endpoint, so slice index != ordinal once i >= 10.
func ordinalFromPeerEp(stsName, ep string) (int, bool) {
	host := ep
	if idx := strings.Index(host, "://"); idx != -1 {
		host = host[idx+len("://"):]
	}
	prefix := stsName + "-"
	if !strings.HasPrefix(host, prefix) {
		return 0, false
	}
	rest := host[len(prefix):]
	if end := strings.IndexAny(rest, ".:"); end != -1 {
		rest = rest[:end]
	}
	ordinal, err := strconv.Atoi(rest)
	if err != nil || ordinal < 0 {
		return 0, false
	}
	return ordinal, true
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// reconcileVersionUpgrade rolls the cluster to spec.version one pod per
// reconcile. The StatefulSet uses the OnDelete update strategy, so
// re-rendering the template never restarts pods by itself; each pod is only
// deleted once every member is healthy, a leader exists, all member revisions
// are within 90% of the leader's, and every pod is Ready. The leader's pod is
// replaced last.
func (r *EtcdClusterReconciler) reconcileVersionUpgrade(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(s.sts.Spec.Template.Spec.Containers) == 0 {
		return ctrl.Result{}, nil
	}

	desiredImage := desiredEtcdImage(s.cluster)
	if s.sts.Spec.Template.Spec.Containers[0].Image != desiredImage {
		logger.Info("Version change detected. Re-rendering StatefulSet template", "desiredImage", desiredImage)
		var err error
		s.sts, err = reconcileStatefulSet(ctx, logger, s.cluster, r.Client, *s.sts.Spec.Replicas, r.Scheme)
		return ctrl.Result{RequeueAfter: requeueDuration}, err
	}

	type ordinalPod struct {
		ordinal int
		pod     *corev1.Pod
	}

	replicas := int(*s.sts.Spec.Replicas)
	pods := make([]ordinalPod, 0, replicas)
	for i := 0; i < replicas; i++ {
		pod := &corev1.Pod{}
		podName := fmt.Sprintf("%s-%d", s.cluster.Name, i)
		if err := r.Get(ctx, client.ObjectKey{Name: podName, Namespace: s.cluster.Namespace}, pod); err != nil {
			if errors.IsNotFound(err) {
				// A replacement is in flight; wait for the StatefulSet controller.
				logger.Info("Pod not found. Waiting for it to be recreated", "pod", podName)
				return ctrl.Result{RequeueAfter: requeueDuration}, nil
			}
			return ctrl.Result{}, err
		}
		if pod.DeletionTimestamp != nil {
			logger.Info("Pod is terminating. Waiting for its replacement", "pod", podName)
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
		pods = append(pods, ordinalPod{ordinal: i, pod: pod})
	}

	var outdated []ordinalPod
	for _, p := range pods {
		if len(p.pod.Spec.Containers) == 0 || p.pod.Spec.Containers[0].Image != desiredImage {
			outdated = append(outdated, p)
		}
	}
	if len(outdated) == 0 {
		logger.Info("EtcdCluster is already up-to-date")
		return ctrl.Result{}, nil
	}

	for _, p := range pods {
		if !isPodReady(p.pod) {
			logger.Info("Pod is not ready. Deferring upgrade step", "pod", p.pod.Name)
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	}

	_, leaderStatus := etcdutils.FindLeaderStatus(s.memberHealth, logger)
	if leaderStatus == nil {
		logger.Info("No leader found. Deferring upgrade step")
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}
	for i := range s.memberHealth {
		if s.memberHealth[i].Status == nil {
			continue
		}
		if !etcdutils.IsLearnerReady(leaderStatus, s.memberHealth[i].Status) {
			logger.Info("Member is lagging behind the leader. Deferring upgrade step", "endpoint", s.memberHealth[i].Ep)
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
	}

	leaderOrdinal := -1
	for i := range s.memberHealth {
		status := s.memberHealth[i].Status
		if status != nil && status.Header != nil && status.Header.MemberId == status.Leader {
			if ordinal, ok := ordinalFromPeerEp(s.sts.Name, s.memberHealth[i].Ep); ok {
				leaderOrdinal = ordinal
			}
			break
		}
	}

	// Lowest-ordinal outdated pod first; the leader's pod goes last.
	victim := outdated[0]
	if victim.ordinal == leaderOrdinal && len(outdated) > 1 {
		victim = outdated[1]
	}

	logger.Info("Deleting pod so the StatefulSet recreates it with the new image",
		"pod", victim.pod.Name, "desiredImage", desiredImage, "leaderOrdinal", leaderOrdinal)
	if err := r.Delete(ctx, victim.pod); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}
