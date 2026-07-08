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
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

const (
	// checkpointCleanupFinalizer guards deletion of the reserved checkpoint
	// key in the TARGET etcd (the CR's only externally-persisted state).
	checkpointCleanupFinalizer = "operator.etcd.io/checkpoint-cleanup"
	// skipCheckpointCleanupAnnotation is the documented escape hatch: set to
	// "true" to let deletion proceed without reaching the target (the
	// reserved key is orphaned and must be removed by hand).
	skipCheckpointCleanupAnnotation = "operator.etcd.io/skip-checkpoint-cleanup"

	// certExpiryLeadWindow: a referenced certificate expiring within this
	// window draws a Warning event. cert-manager renews ~30d out, so <14d
	// remaining means the renewal machinery has already failed.
	certExpiryLeadWindow = 14 * 24 * time.Hour
	// warningDampInterval bounds re-emission of standing warnings
	// (cert expiry, insecureSkipVerify, cleanup failures) per CR. In-memory:
	// a controller restart re-emits early, which is the conservative side.
	warningDampInterval = 12 * time.Hour

	// finalizer pacing: wait for agent pods to terminate, then retry failed
	// checkpoint deletes with a bounded exponential backoff.
	finalizerPodWait       = 5 * time.Second
	finalizerRetryInitial  = 30 * time.Second
	finalizerRetryMax      = 5 * time.Minute
	validationRetryBackoff = 30 * time.Second

	eventActionReconcile = "Reconcile"
	eventActionFinalize  = "Finalize"
)

// EtcdMirrorReconciler reconciles an EtcdMirror into a size-1 stateless
// mirror-agent Deployment and mirrors the agent's /statusz snapshot into
// status. StatusClient and Cleaner are injectable seams: envtest has no
// kubelet, so agent pods never run there and neither /statusz nor the target
// etcd is reachable — tests substitute fakes and drive Reconcile directly.
type EtcdMirrorReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
	// AgentImage is the image agent Deployments run (the operator image
	// itself; the binary ships at /mirror-agent). Unset leaves EtcdMirror CRs
	// Pending with reason AgentImageNotConfigured.
	AgentImage string
	// StatusClient polls agent pods' /statusz. Defaulted in SetupWithManager.
	StatusClient AgentStatusClient
	// Cleaner deletes the reserved checkpoint key during finalization.
	// Defaulted in SetupWithManager.
	Cleaner CheckpointCleaner

	// In-memory ledgers (lost on restart, which is acceptable: the lag window
	// restarts, standing warnings conservatively re-emit, and counter bases
	// re-derive from persisted status).
	mu sync.Mutex
	// lagSince: when the watermark gap first exceeded the lag threshold.
	lagSince map[types.UID]time.Time
	// warnedAt: last emission per damped-warning key ("<cr-uid>/<key>").
	warnedAt map[string]time.Time
	// counterBases: per-CR offsets rebasing the agent's process-local
	// monotonic counters onto persisted status across pod restarts.
	counterBases map[types.UID]agentCounterBase
}

// agentCounterBase carries the persisted-counter offset for one agent pod so
// forcedResyncCount/scanRestartCount never regress when the pod restarts (the
// status contract declares both "Monotonic, never reset"; the agent's copies
// are process memory).
type agentCounterBase struct {
	podUID       types.UID
	forcedResync int64
	scanRestart  int64
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdmirrors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdmirrors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdmirrors/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// The manager's recorder emits events.k8s.io/v1 Events; the core-group grant
// alone silently drops them on a real apiserver (invisible in envtest).
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile drives one EtcdMirror: finalize on delete, otherwise validate,
// guard, render the agent Deployment, poll /statusz, and mirror the snapshot
// into status.
func (r *EtcdMirrorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	em := &ecv1alpha1.EtcdMirror{}
	if err := r.Get(ctx, req.NamespacedName, em); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !em.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, em)
	}

	if !controllerutil.ContainsFinalizer(em, checkpointCleanupFinalizer) {
		controllerutil.AddFinalizer(em, checkpointCleanupFinalizer)
		if err := r.Update(ctx, em); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	prior := em.Status.DeepCopy()
	res, err := r.reconcileMirror(ctx, em, prior)
	if !apiequality.Semantic.DeepEqual(prior, &em.Status) {
		if statusErr := r.Status().Update(ctx, em); statusErr != nil {
			logger.Error(statusErr, "failed to update EtcdMirror status")
			if err == nil {
				err = statusErr
			}
		}
	}
	return res, err
}

// reconcileMirror is the non-deleting path. It mutates em.Status; the caller
// persists it.
func (r *EtcdMirrorReconciler) reconcileMirror(
	ctx context.Context, em *ecv1alpha1.EtcdMirror, prior *ecv1alpha1.EtcdMirrorStatus,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Standing spec warnings (cert expiry lead window, insecureSkipVerify)
	// are spec/Secret-derived and independent of agent state: emit on every
	// non-finalizing reconcile so Pending/Paused/guard-blocked mirrors warn
	// too (the InsecureSkipVerify contract has no reachability qualifier).
	r.emitSpecWarnings(ctx, em, time.Now())

	// Validate: agent image, credential Secrets, serviceRef resolution. Any
	// failure is an environment/spec problem, not an agent failure: Pending.
	if r.AgentImage == "" {
		r.blockValidation(ctx, em, prior, reasonAgentImageNotConfigured,
			"the operator was started without --mirror-agent-image; EtcdMirror CRs stay Pending until it is set")
		return ctrl.Result{RequeueAfter: validationRetryBackoff}, nil
	}
	in := agentWorkloadInput{image: r.AgentImage}
	var err error
	if in.sourceCreds, err = resolveSideCreds(ctx, r.Client, logger, em.Namespace, sideSourceName, em.Spec.Source); err == nil {
		in.targetCreds, err = resolveSideCreds(ctx, r.Client, logger, em.Namespace, sideTargetName, em.Spec.Target)
	}
	if err == nil {
		if in.sourceEndpoints, err = r.endpointsForSide(ctx, em, em.Spec.Source); err == nil {
			in.targetEndpoints, err = r.endpointsForSide(ctx, em, em.Spec.Target)
		}
	}
	if err != nil {
		var ce *credsError
		if errors.As(err, &ce) {
			r.blockValidation(ctx, em, prior, ce.Reason, ce.Error())
			return ctrl.Result{RequeueAfter: validationRetryBackoff}, nil
		}
		return ctrl.Result{}, err
	}

	// Guards: overlapping destination ranges and two-way loops. The loser
	// (the newer CR) is stopped; the conflict clears when the sibling goes.
	blocked, res, err := r.applyGuards(ctx, em, prior, in)
	if err != nil || blocked {
		return res, err
	}

	// Paused: scale to zero, keep the rest of status honest (untouched).
	if em.Spec.Paused {
		if _, err := r.applyAgentDeployment(ctx, em, in, 0); err != nil {
			return ctrl.Result{}, err
		}
		em.Status.Phase = ecv1alpha1.EtcdMirrorPhasePaused
		em.Status.ObservedGeneration = em.Generation
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse,
			reasonPaused, "spec.paused is true; the agent Deployment is scaled to zero (the checkpoint is retained)")
		return ctrl.Result{RequeueAfter: slowRequeueInterval}, nil
	}

	// Render and apply the size-1 Deployment.
	created, err := r.applyAgentDeployment(ctx, em, in, 1)
	if err != nil {
		return ctrl.Result{}, err
	}
	if created {
		em.Status.Phase = ecv1alpha1.EtcdMirrorPhasePending
		em.Status.ObservedGeneration = em.Generation
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse,
			reasonAgentPodNotReady, "agent Deployment created; waiting for its pod")
		return ctrl.Result{RequeueAfter: statusPollInterval}, nil
	}

	// Locate the agent pod. Never poll /statusz without a running pod IP.
	pod, err := r.findAgentPod(ctx, em)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pod == nil || pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
		if pod != nil {
			em.Status.AgentPod = pod.Name
		}
		if em.Status.Phase == "" {
			em.Status.Phase = ecv1alpha1.EtcdMirrorPhasePending
		}
		em.Status.ObservedGeneration = em.Generation
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse,
			reasonAgentPodNotReady, "agent pod is not running yet")
		return ctrl.Result{RequeueAfter: statusPollInterval}, nil
	}
	em.Status.AgentPod = pod.Name

	// Poll /statusz through the seam.
	snap, err := r.StatusClient.Snapshot(ctx, net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(agentHTTPPort)))
	if err != nil {
		// ALL prior status fields retained — including phase: Degraded is
		// reserved for the agent's own retry/backoff loop and a poll failure
		// says nothing about the agent (a controller-side route blip must not
		// flip a healthy mirror, nor mask a terminal Failed). Staleness stays
		// observable via LastStatusSyncTime not advancing.
		reason := reasonAgentStatusUnreachable
		var decodeErr *snapshotDecodeError
		if errors.As(err, &decodeErr) {
			reason = reasonSnapshotDecodeFailed
		}
		em.Status.ObservedGeneration = em.Generation
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionUnknown,
			reason, fmt.Sprintf("polling agent /statusz: %v", err))
		return ctrl.Result{RequeueAfter: statusPollInterval}, nil
	}

	// Rebase the process-local monotonic counters before anything reads them.
	r.adjustSnapshotCounters(em, prior, pod.UID, snap)

	// Events first (the persisted counters/conditions are the dedup ledger,
	// so emit before the snapshot overwrites them), then the mapping.
	r.emitSnapshotEvents(em, prior, snap)
	now := time.Now()
	r.mu.Lock()
	if r.lagSince == nil {
		r.lagSince = make(map[types.UID]time.Time)
	}
	lagSince := r.lagSince[em.UID]
	r.mu.Unlock()
	lagSince = applySnapshotToStatus(em, snap, now, lagSince)
	r.mu.Lock()
	if lagSince.IsZero() {
		delete(r.lagSince, em.UID)
	} else {
		r.lagSince[em.UID] = lagSince
	}
	r.mu.Unlock()

	if em.Status.Phase == ecv1alpha1.EtcdMirrorPhaseFailed {
		return ctrl.Result{RequeueAfter: slowRequeueInterval}, nil
	}
	return ctrl.Result{RequeueAfter: statusPollInterval}, nil
}

// blockValidation parks the CR on a validation failure with a specific
// Available reason, emitting one Warning per transition into that reason.
// Phase drops to Pending ("the agent workload has not been created yet") only
// when that is true: a Deployment rendered before the failure (a referenced
// Secret/Service deleted later, an operator restart without the image flag)
// keeps its pod running on mounted material, so the prior phase is retained
// and only the condition carries the failure — status is stale until
// validation passes, observable via LastStatusSyncTime.
func (r *EtcdMirrorReconciler) blockValidation(
	ctx context.Context, em *ecv1alpha1.EtcdMirror, prior *ecv1alpha1.EtcdMirrorStatus, reason, message string,
) {
	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Namespace: em.Namespace, Name: deploymentNameForEtcdMirror(em)}, dep)
	if err != nil || em.Status.Phase == "" {
		em.Status.Phase = ecv1alpha1.EtcdMirrorPhasePending
	} else {
		message += "; the existing agent Deployment keeps running on previously mounted material" +
			" and its status mirror is stale until validation passes"
	}
	em.Status.ObservedGeneration = em.Generation
	priorCond := meta.FindStatusCondition(prior.Conditions, ecv1alpha1.EtcdMirrorConditionAvailable)
	if priorCond == nil || priorCond.Reason != reason {
		r.Recorder.Eventf(em, nil, corev1.EventTypeWarning, reason, eventActionReconcile, "%s", message)
	}
	setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reason, message)
}

// adjustSnapshotCounters rebases the agent's process-local monotonic counters
// onto the persisted status so a pod restart never regresses them (nor
// re-fires the counter-keyed events). First sight of a pod assumes the
// persisted counters already include the snapshot's — true after a controller
// restart, and a fresh pod reports 0; increments that land between pod start
// and its first poll are absorbed into the base. The max clamps cover a
// container restart inside one pod.
func (r *EtcdMirrorReconciler) adjustSnapshotCounters(
	em *ecv1alpha1.EtcdMirror, prior *ecv1alpha1.EtcdMirrorStatus, podUID types.UID, snap *mirroragent.Snapshot,
) {
	r.mu.Lock()
	if r.counterBases == nil {
		r.counterBases = make(map[types.UID]agentCounterBase)
	}
	b, ok := r.counterBases[em.UID]
	if !ok || b.podUID != podUID {
		b = agentCounterBase{
			podUID:       podUID,
			forcedResync: max(int64(prior.ForcedResyncCount)-snap.ForcedResyncCount, 0),
			scanRestart:  max(prior.ScanRestartCount-snap.ScanRestartCount, 0),
		}
		r.counterBases[em.UID] = b
	}
	r.mu.Unlock()
	snap.ForcedResyncCount = max(b.forcedResync+snap.ForcedResyncCount, int64(prior.ForcedResyncCount))
	snap.ScanRestartCount = max(b.scanRestart+snap.ScanRestartCount, prior.ScanRestartCount)
}

// endpointsForSide resolves one side to the comma-joined --<side>-endpoints
// value: the endpointList verbatim, or the serviceRef's DNS name with a
// resolved numeric port.
func (r *EtcdMirrorReconciler) endpointsForSide(
	ctx context.Context, em *ecv1alpha1.EtcdMirror, ep ecv1alpha1.EtcdMirrorEndpoint,
) (string, error) {
	if len(ep.EndpointList) > 0 {
		return strings.Join(ep.EndpointList, ","), nil
	}
	if ep.ServiceRef != nil {
		return serviceRefEndpoints(ctx, r.Client, em.Namespace, ep.ServiceRef)
	}
	return "", &credsError{Reason: reasonInvalidConfig, msg: "endpoint has neither endpointList nor serviceRef"}
}

// applyGuards evaluates PrefixConflict and DirectionConflict against every
// other EtcdMirror. Returns blocked=true when em is the conflict loser (its
// Deployment is scaled to zero and the CR parks in Pending).
func (r *EtcdMirrorReconciler) applyGuards(
	ctx context.Context, em *ecv1alpha1.EtcdMirror, prior *ecv1alpha1.EtcdMirrorStatus, in agentWorkloadInput,
) (bool, ctrl.Result, error) {
	all := &ecv1alpha1.EtcdMirrorList{}
	if err := r.List(ctx, all); err != nil {
		return false, ctrl.Result{}, err
	}

	prefixConflict := findPrefixConflict(em, all.Items)
	directionConflict := findDirectionConflict(em, all.Items)

	// Both conditions are (re)evaluated on every reconcile — even when the
	// other guard blocks — so a True left by a since-deleted sibling clears.
	// DirectionConflict is a mutual property: True on both CRs.
	if directionConflict != nil {
		r.setConflictCondition(em, prior, directionConflict)
	} else {
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionDirectionConflict, metav1.ConditionFalse,
			reasonNoConflict, "no other EtcdMirror mirrors the opposite direction between these clusters")
	}
	// PrefixConflict: only the loser reports True (the winner keeps its
	// range), but the winner's False names the parked sibling honestly.
	switch {
	case prefixConflict == nil:
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionPrefixConflict, metav1.ConditionFalse,
			reasonNoConflict, "no other EtcdMirror overlaps this effective destination range on the same target cluster")
	case !isConflictLoser(em, prefixConflict.sibling):
		setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionPrefixConflict, metav1.ConditionFalse,
			reasonConflictWinner, fmt.Sprintf(
				"EtcdMirror %s/%s overlaps this effective destination range but is the newer CR: it is parked, this mirror keeps the range",
				prefixConflict.sibling.Namespace, prefixConflict.sibling.Name))
		prefixConflict = nil
	}

	if prefixConflict != nil {
		return true, r.blockOnConflict(ctx, em, prior, in, prefixConflict), nil
	}
	if directionConflict != nil && isConflictLoser(em, directionConflict.sibling) {
		return true, r.blockOnConflict(ctx, em, prior, in, nil), nil
	}
	return false, ctrl.Result{}, nil
}

// setConflictCondition raises a guard condition, emitting one Warning per
// False->True transition.
func (r *EtcdMirrorReconciler) setConflictCondition(
	em *ecv1alpha1.EtcdMirror, prior *ecv1alpha1.EtcdMirrorStatus, c *mirrorConflict,
) {
	if !meta.IsStatusConditionTrue(prior.Conditions, c.conditionType) {
		r.Recorder.Eventf(em, nil, corev1.EventTypeWarning, c.conditionType, eventActionReconcile, "%s", c.message)
	}
	setMirrorCondition(em, c.conditionType, metav1.ConditionTrue, reasonConflict, c.message)
}

// blockOnConflict stops the conflict loser: condition True, Deployment scaled
// to zero, Phase=Pending (the conflict clears when the sibling is deleted, so
// this is not Failed).
func (r *EtcdMirrorReconciler) blockOnConflict(
	ctx context.Context, em *ecv1alpha1.EtcdMirror, prior *ecv1alpha1.EtcdMirrorStatus,
	in agentWorkloadInput, c *mirrorConflict,
) ctrl.Result {
	condType := ecv1alpha1.EtcdMirrorConditionDirectionConflict
	if c != nil {
		r.setConflictCondition(em, prior, c)
		condType = c.conditionType
	}
	if _, err := r.applyAgentDeployment(ctx, em, in, 0); err != nil {
		log.FromContext(ctx).Error(err, "failed to scale conflicting agent Deployment to zero")
	}
	em.Status.Phase = ecv1alpha1.EtcdMirrorPhasePending
	em.Status.ObservedGeneration = em.Generation
	setMirrorCondition(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse,
		condType, "stopped by a "+condType+" with another EtcdMirror; see that condition")
	return ctrl.Result{RequeueAfter: slowRequeueInterval}
}

// applyAgentDeployment renders and CreateOrPatches the agent Deployment.
// Returns whether it was created this reconcile.
func (r *EtcdMirrorReconciler) applyAgentDeployment(
	ctx context.Context, em *ecv1alpha1.EtcdMirror, in agentWorkloadInput, replicas int32,
) (bool, error) {
	desired := renderAgentDeployment(em, in, replicas)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace},
	}
	op, err := controllerutil.CreateOrPatch(ctx, r.Client, dep, func() error {
		dep.Labels = desired.Labels
		dep.Spec = desired.Spec
		return controllerutil.SetControllerReference(em, dep, r.Scheme)
	})
	if err != nil {
		return false, fmt.Errorf("applying agent Deployment: %w", err)
	}
	return op == controllerutil.OperationResultCreated, nil
}

// findAgentPod returns the newest non-terminating pod of the agent
// Deployment, or nil.
func (r *EtcdMirrorReconciler) findAgentPod(ctx context.Context, em *ecv1alpha1.EtcdMirror) (*corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(em.Namespace),
		client.MatchingLabels(etcdMirrorAgentLabels(em))); err != nil {
		return nil, err
	}
	var newest *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if !p.DeletionTimestamp.IsZero() {
			continue
		}
		if newest == nil || newest.CreationTimestamp.Before(&p.CreationTimestamp) {
			newest = p
		}
	}
	return newest, nil
}

// emitSnapshotEvents emits the operator-facing events whose dedup ledger is
// the persisted status (counters and prior conditions) — must run BEFORE
// applySnapshotToStatus overwrites those fields. Steady state emits nothing.
func (r *EtcdMirrorReconciler) emitSnapshotEvents(
	em *ecv1alpha1.EtcdMirror, prior *ecv1alpha1.EtcdMirrorStatus, snap *mirroragent.Snapshot,
) {
	// Forced resyncs advanced since the last persisted count. Multiple
	// resyncs between polls collapse into one event pair — accepted: the
	// counter still records them all.
	if snap.ForcedResyncCount > int64(prior.ForcedResyncCount) {
		r.Recorder.Eventf(em, nil, corev1.EventTypeWarning,
			ecv1alpha1.EtcdMirrorEventForcedResyncStarted, eventActionReconcile,
			"forced resync #%d started (trigger: %s)", snap.ForcedResyncCount, snap.LastResyncReason)
		if snap.LastResyncReason == mirroragent.ResyncReasonClusterIDMismatch {
			r.Recorder.Eventf(em, nil, corev1.EventTypeWarning,
				ecv1alpha1.EtcdMirrorEventCheckpointInvalidated, eventActionReconcile,
				"checkpoint discarded: a bound cluster ID no longer matches; forcing genesis and re-arming the RequireEmpty check")
		}
	}
	if meta.IsStatusConditionTrue(prior.Conditions, ecv1alpha1.EtcdMirrorConditionCompacted) &&
		!snap.Compacted && snap.Phase == mirroragent.PhaseSyncing {
		r.Recorder.Eventf(em, nil, corev1.EventTypeNormal,
			ecv1alpha1.EtcdMirrorEventForcedResyncCompleted, eventActionReconcile,
			"forced resync completed; steady-state syncing resumed")
	}
	if snap.ScanRestartCount > prior.ScanRestartCount {
		r.Recorder.Eventf(em, nil, corev1.EventTypeWarning,
			ecv1alpha1.EtcdMirrorEventInitialSyncCompactionRaced, eventActionReconcile,
			"genesis scan attempt aborted and restarted from a fresh R0 (restart #%d, cause: %s)",
			snap.ScanRestartCount, snap.LastScanRestartCause)
	}
	newViolation := snap.Phase == mirroragent.PhaseFailed && snap.LastErrorReason == "EmptyTargetViolation"
	if newViolation && !meta.IsStatusConditionTrue(prior.Conditions, ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation) {
		r.Recorder.Eventf(em, nil, corev1.EventTypeWarning,
			ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation, eventActionReconcile,
			"destination prefix was non-empty at genesis; to clear it run: %s",
			etcdctlDelCommand(effectiveDestPrefix(em)))
	}
}

// emitSpecWarnings emits the standing (12h-damped) warnings derived from the
// spec and referenced Secrets: certificates in the expiry lead window and
// insecureSkipVerify.
func (r *EtcdMirrorReconciler) emitSpecWarnings(ctx context.Context, em *ecv1alpha1.EtcdMirror, now time.Time) {
	for _, side := range []struct {
		name string
		ep   ecv1alpha1.EtcdMirrorEndpoint
	}{{sideSourceName, em.Spec.Source}, {sideTargetName, em.Spec.Target}} {
		for _, exp := range sideCertExpiries(ctx, r.Client, em.Namespace, side.name, side.ep) {
			if exp.NotAfter.Sub(now) < certExpiryLeadWindow {
				r.dampedEventf(em, now, "certexpiry/"+exp.Side+"/"+exp.Kind, corev1.EventTypeWarning,
					ecv1alpha1.EtcdMirrorEventCertificateExpiringSoon,
					"%s %s expires %s (within the %s lead window); the renewal machinery appears to have failed",
					exp.Side, exp.Kind, exp.NotAfter.UTC().Format(time.RFC3339), certExpiryLeadWindow)
			}
		}
		if side.ep.TLS != nil && side.ep.TLS.InsecureSkipVerify {
			r.dampedEventf(em, now, "insecureskipverify/"+side.name, corev1.EventTypeWarning,
				ecv1alpha1.EtcdMirrorEventInsecureSkipVerifyEnabled,
				"%s TLS verification is disabled (insecureSkipVerify: true) — strongly discouraged", side.name)
		}
	}
}

// dampedEventf emits at most one event per (CR, key) per warningDampInterval.
func (r *EtcdMirrorReconciler) dampedEventf(
	em *ecv1alpha1.EtcdMirror, now time.Time, key, eventtype, reason, note string, args ...any,
) {
	ledgerKey := string(em.UID) + "/" + key
	r.mu.Lock()
	if r.warnedAt == nil {
		r.warnedAt = make(map[string]time.Time)
	}
	if last, ok := r.warnedAt[ledgerKey]; ok && now.Sub(last) < warningDampInterval {
		r.mu.Unlock()
		return
	}
	r.warnedAt[ledgerKey] = now
	r.mu.Unlock()
	r.Recorder.Eventf(em, nil, eventtype, reason, eventActionReconcile, note, args...)
}

// finalize handles CR deletion: stop the agent, delete the reserved
// checkpoint key from the target (through the Cleaner seam), then release the
// finalizer. The skip-checkpoint-cleanup annotation is the escape hatch for
// permanently unreachable targets.
func (r *EtcdMirrorReconciler) finalize(ctx context.Context, em *ecv1alpha1.EtcdMirror) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(em, checkpointCleanupFinalizer) {
		return ctrl.Result{}, nil
	}

	if em.Annotations[skipCheckpointCleanupAnnotation] == "true" {
		r.Recorder.Eventf(em, nil, corev1.EventTypeNormal, "CheckpointCleanupSkipped", eventActionFinalize,
			"checkpoint cleanup skipped by the %s annotation; reserved key %q remains in the target etcd",
			skipCheckpointCleanupAnnotation, checkpointKeyForMirror(em))
		return r.removeFinalizer(ctx, em)
	}

	// The agent must stop before the key is deleted, or its next fenced Txn
	// recreates it: delete the Deployment, wait for its pods to be gone.
	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Namespace: em.Namespace, Name: deploymentNameForEtcdMirror(em)}, dep)
	switch {
	case err == nil:
		if dep.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, dep); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
	case !apierrors.IsNotFound(err):
		return ctrl.Result{}, err
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(em.Namespace),
		client.MatchingLabels(etcdMirrorAgentLabels(em))); err != nil {
		return ctrl.Result{}, err
	}
	if len(pods.Items) > 0 {
		logger.Info("waiting for agent pods to terminate before checkpoint cleanup", "pods", len(pods.Items))
		return ctrl.Result{RequeueAfter: finalizerPodWait}, nil
	}

	var skipReason string
	tgt, err := resolveFinalizerTarget(ctx, r.Client, logger, em)
	if err == nil {
		skipReason, err = r.Cleaner.DeleteCheckpoint(ctx, tgt)
	}
	if err != nil {
		// Bounded-backoff retries forever; the annotation is the exit.
		r.dampedEventf(em, time.Now(), "checkpointcleanup", corev1.EventTypeWarning,
			"CheckpointCleanupFailed",
			"deleting reserved checkpoint key from the target failed (will retry; set the %s annotation to skip): %v",
			skipCheckpointCleanupAnnotation, err)
		backoff := finalizerRetryBackoff(time.Since(em.DeletionTimestamp.Time))
		logger.Error(err, "checkpoint cleanup failed", "retryAfter", backoff)
		return ctrl.Result{RequeueAfter: backoff}, nil
	}
	if skipReason != "" {
		// A foreign or undecodable fence is provably not this CR's state
		// (e.g. this CR was a parked PrefixConflict loser and the key is the
		// winner's live fence): nothing of ours to clean, deletion proceeds.
		r.Recorder.Eventf(em, nil, corev1.EventTypeNormal, "CheckpointNotOwned", eventActionFinalize,
			"reserved key %q left in place: %s", tgt.Key, skipReason)
	}
	return r.removeFinalizer(ctx, em)
}

func (r *EtcdMirrorReconciler) removeFinalizer(ctx context.Context, em *ecv1alpha1.EtcdMirror) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(em, checkpointCleanupFinalizer)
	if err := r.Update(ctx, em); err != nil {
		return ctrl.Result{}, err
	}
	r.mu.Lock()
	delete(r.lagSince, em.UID)
	delete(r.counterBases, em.UID)
	prefix := string(em.UID) + "/"
	for k := range r.warnedAt {
		if strings.HasPrefix(k, prefix) {
			delete(r.warnedAt, k)
		}
	}
	r.mu.Unlock()
	return ctrl.Result{}, nil
}

// finalizerRetryBackoff doubles from finalizerRetryInitial as elapsed
// deletion time accumulates, capped at finalizerRetryMax — bounded frequency,
// never gives up silently.
func finalizerRetryBackoff(elapsed time.Duration) time.Duration {
	backoff := finalizerRetryInitial
	remaining := elapsed
	for backoff < finalizerRetryMax && remaining >= backoff {
		remaining -= backoff
		backoff *= 2
	}
	return min(backoff, finalizerRetryMax)
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdMirrorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorder("etcdmirror-controller")
	if r.StatusClient == nil {
		r.StatusClient = newHTTPAgentStatusClient()
	}
	if r.Cleaner == nil {
		r.Cleaner = etcdCheckpointCleaner{}
	}
	// The controller's own status writes must not re-enqueue the CR: every
	// healthy poll advances LastStatusSyncTime, so an unfiltered watch turns
	// the poll cadence into a self-triggering hot loop. Generation covers
	// spec changes and deletion; annotations keep the skip-checkpoint-cleanup
	// escape hatch responsive. Poll cadence rests on the returned
	// RequeueAfter values, which is the design.
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdMirror{}, builder.WithPredicates(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
		))).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}
