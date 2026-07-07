package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
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

// isPodOutdated reports whether the pod predates the StatefulSet's current
// template. The controller-revision-hash label catches any template drift
// (args, labels, volumes, ...) and stays valid when admission webhooks
// rewrite pod images; the image comparison is only a fallback for the window
// before the StatefulSet controller publishes an update revision.
func isPodOutdated(pod *corev1.Pod, sts *appsv1.StatefulSet, desiredImage string) bool {
	if rev := sts.Status.UpdateRevision; rev != "" {
		return pod.Labels[appsv1.ControllerRevisionHashLabelKey] != rev
	}
	return len(pod.Spec.Containers) == 0 || pod.Spec.Containers[0].Image != desiredImage
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// reconcileVersionUpgrade rolls the cluster to the current template one pod
// per reconcile. The StatefulSet uses the OnDelete update strategy, so
// re-rendering the template never restarts pods by itself; each pod is only
// deleted once every member is healthy, a leader exists, all member revisions
// are within 90% of the leader's, and every up-to-date pod is Ready. The
// leader's pod is replaced last.
func (r *EtcdClusterReconciler) reconcileVersionUpgrade(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if len(s.sts.Spec.Template.Spec.Containers) == 0 {
		return ctrl.Result{}, nil
	}

	// Re-render on image drift or on any spec edit not yet observed, so
	// non-image template changes (etcdOptions, podTemplate, TLS) roll out too.
	desiredImage := desiredEtcdImage(s.cluster)
	if s.sts.Spec.Template.Spec.Containers[0].Image != desiredImage ||
		s.cluster.Generation != s.cluster.Status.ObservedGeneration {
		logger.Info("Spec change detected. Re-rendering StatefulSet template", "desiredImage", desiredImage)
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
		if err := r.PodReader.Get(ctx, client.ObjectKey{Name: podName, Namespace: s.cluster.Namespace}, pod); err != nil {
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
		if isPodOutdated(p.pod, s.sts, desiredImage) {
			outdated = append(outdated, p)
		}
	}
	if len(outdated) == 0 {
		logger.Info("EtcdCluster is already up-to-date")
		return ctrl.Result{}, nil
	}

	// Every up-to-date pod must be Ready before touching anything. A not-Ready
	// pod that is itself outdated is exempt: requiring it Ready would block
	// its own replacement (rollback from a version that never becomes Ready).
	outdatedOrdinals := make(map[int]bool, len(outdated))
	for _, p := range outdated {
		outdatedOrdinals[p.ordinal] = true
	}
	for _, p := range pods {
		if !outdatedOrdinals[p.ordinal] && !isPodReady(p.pod) {
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

	// Not-Ready outdated pods go first (they hold no quorum weight), then
	// lowest ordinal; the leader's pod goes last.
	candidates := make([]ordinalPod, 0, len(outdated))
	for _, p := range outdated {
		if !isPodReady(p.pod) {
			candidates = append(candidates, p)
		}
	}
	for _, p := range outdated {
		if isPodReady(p.pod) {
			candidates = append(candidates, p)
		}
	}
	victim := candidates[0]
	if victim.ordinal == leaderOrdinal && len(candidates) > 1 {
		victim = candidates[1]
	}

	logger.Info("Deleting pod so the StatefulSet recreates it with the new image",
		"pod", victim.pod.Name, "desiredImage", desiredImage, "leaderOrdinal", leaderOrdinal)
	if err := r.Delete(ctx, victim.pod); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// recoverDegradedUpgrade runs when the member health check fails, which
// otherwise blocks reconciliation before any template or pod convergence: a
// pod replaced with a broken image (bad tag, incompatible flags) would wedge
// the cluster at N-1 members forever. It re-syncs the StatefulSet template
// from the spec so a version rollback or option fix can land, and, while the
// remaining members still hold quorum, deletes one not-Ready outdated pod so
// the StatefulSet recreates it from the fixed template. Returns handled=false
// when the degradation is not something template convergence can repair.
func (r *EtcdClusterReconciler) recoverDegradedUpgrade(ctx context.Context, s *reconcileState) (ctrl.Result, bool) {
	logger := log.FromContext(ctx)

	if s.sts == nil || s.sts.Spec.Replicas == nil || len(s.sts.Spec.Template.Spec.Containers) == 0 {
		return ctrl.Result{}, false
	}

	// waitForStatefulSetReady can never succeed with a member down, so sync
	// without it.
	sts, err := syncStatefulSet(ctx, logger, s.cluster, r.Client, *s.sts.Spec.Replicas, r.Scheme)
	if err != nil {
		logger.Error(err, "Failed to re-sync StatefulSet template while degraded")
		return ctrl.Result{}, false
	}
	s.sts = sts

	healthy := 0
	for i := range s.memberHealth {
		if s.memberHealth[i].Health {
			healthy++
		}
	}
	replicas := int(*sts.Spec.Replicas)
	if healthy < replicas/2+1 {
		// Quorum is already lost; deleting anything can only make it worse.
		return ctrl.Result{}, false
	}

	desiredImage := desiredEtcdImage(s.cluster)
	var victim *corev1.Pod
	for i := 0; i < replicas; i++ {
		pod := &corev1.Pod{}
		podName := fmt.Sprintf("%s-%d", s.cluster.Name, i)
		if err := r.PodReader.Get(ctx, client.ObjectKey{Name: podName, Namespace: s.cluster.Namespace}, pod); err != nil {
			if errors.IsNotFound(err) {
				// A replacement is already in flight.
				return ctrl.Result{RequeueAfter: requeueDuration}, true
			}
			return ctrl.Result{}, false
		}
		if pod.DeletionTimestamp != nil {
			return ctrl.Result{RequeueAfter: requeueDuration}, true
		}
		if isPodReady(pod) {
			continue
		}
		if !isPodOutdated(pod, sts, desiredImage) {
			// Broken but already on the current template: recreating it would
			// change nothing.
			return ctrl.Result{}, false
		}
		if victim == nil {
			victim = pod
		}
	}
	if victim == nil {
		return ctrl.Result{}, false
	}

	logger.Info("Replacing broken outdated pod while quorum holds", "pod", victim.Name)
	if err := r.Delete(ctx, victim); err != nil {
		logger.Error(err, "Failed to delete broken outdated pod", "pod", victim.Name)
		return ctrl.Result{}, false
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, true
}
