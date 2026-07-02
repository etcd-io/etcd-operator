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
	"io"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/objectstore"
)

const (
	// defaultRestoreTimeout bounds the download+restore step when the spec omits one.
	defaultRestoreTimeout = 10 * time.Minute
	// restoreRequeueInterval is how often the reconciler re-checks a stamped
	// restore-target cluster while waiting for its genesis member to boot from
	// the restored data dir.
	restoreRequeueInterval = 5 * time.Second
)

// EtcdRestoreReconciler reconciles an EtcdRestore object: it downloads a
// snapshot from object storage and bootstraps a new EtcdCluster from it.
//
// Like EtcdBackupReconciler, the reconciler is intentionally thin: the
// cloud-specific behaviour lives behind two seams — objectstore.Factory (read
// the snapshot from a bucket) and Restorer (write it into a fresh member and
// bootstrap). Both are injectable so the orchestration is exercised by unit
// tests with no cloud creds and no live etcd.
type EtcdRestoreReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// RESTConfig is retained for parity with the other controllers (and any
	// future in-pod operations); the restore data path no longer execs into pods.
	RESTConfig *rest.Config

	// OperatorImage is the operator's own image, stamped onto the restore-target
	// EtcdCluster so the cluster controller can inject a restore init-container
	// (the operator image, `manager restore-localize`) that downloads the
	// snapshot and lays the genesis data dir before etcd starts. Required at
	// runtime; tests set it explicitly.
	OperatorImage string

	// NewStore builds an object store from a destination and credentials.
	// Defaults to objectstore.New; overridden in tests with a fake. It is used
	// only to eagerly verify the snapshot object is reachable (so a missing
	// object fails the restore terminally and promptly), not to stream bytes —
	// the member's init-container performs the actual download+restore.
	NewStore objectstore.Factory
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdbackups,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile drives a single EtcdRestore to completion. A restore is a one-shot
// job, so a terminal phase (Completed/Failed) short-circuits further work.
func (r *EtcdRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var restore ecv1alpha1.EtcdRestore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal restores are immutable records; nothing more to do.
	if restore.Status.Phase == ecv1alpha1.RestorePhaseCompleted ||
		restore.Status.Phase == ecv1alpha1.RestorePhaseFailed {
		return ctrl.Result{}, nil
	}

	restore.Status.ObservedGeneration = restore.Generation

	result, err := r.runRestore(ctx, &restore)
	if err != nil {
		logger.Error(err, "restore failed", "restore", req.NamespacedName)
		r.markFailed(&restore, err)
	}

	if statusErr := r.Status().Update(ctx, &restore); statusErr != nil {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, statusErr
	}
	return result, err
}

// runRestore drives the restore as: validate -> resolve source -> verify the
// snapshot object is reachable -> ensure+STAMP the target cluster -> WATCH the
// genesis member boot from the restored data.
//
// This is the init-container (pull) model: the controller no longer streams
// snapshot bytes into a member. Instead it stamps the snapshot source onto the
// target EtcdCluster (restore-source annotation); the cluster controller injects
// a restore init-container that downloads the snapshot and lays the genesis data
// dir before etcd starts. The controller's job shrinks to stamping the source
// and observing the member become Ready (or fail), so Completed truthfully means
// "a member booted from the restored data", not merely "the spec was injected".
func (r *EtcdRestoreReconciler) runRestore(ctx context.Context, restore *ecv1alpha1.EtcdRestore) (ctrl.Result, error) {
	// 1. Validate the spec up front so misconfiguration is a clean, terminal
	//    failure rather than a partial restore.
	if err := validateRestoreSpec(&restore.Spec); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Resolve the snapshot source into a concrete destination + object key +
	//    cluster version (the latter from the referenced backup, when used).
	src, err := r.resolveSource(ctx, restore)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. Build the object store and eagerly verify the snapshot object is
	//    reachable. This surfaces a missing object as a terminal, prompt failure
	//    here (the message contains "download"/"not found") rather than letting a
	//    member pod crash-loop on a 404 download. We open the body and close it
	//    immediately — the member's init-container performs the real download.
	store, err := r.buildStore(ctx, restore.Namespace, src.destination)
	if err != nil {
		return ctrl.Result{}, err
	}
	timeout := defaultRestoreTimeout
	if restore.Spec.RestoreTimeout != nil {
		timeout = restore.Spec.RestoreTimeout.Duration
	}
	// Verify reachability only on the first pass (before we enter Restoring and
	// stamp the target); subsequent requeues are just watching the member boot,
	// so re-opening the object every interval would be wasteful.
	if restore.Status.Phase != ecv1alpha1.RestorePhaseRestoring {
		vctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if err := r.verifySnapshotReachable(vctx, store, src.key); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 4. Ensure the target is a fresh, empty cluster (create if absent, reject if
	//    it already has members) and STAMP the restore-source annotation on it so
	//    the cluster controller injects the restore init-container.
	if restore.Status.Phase == "" || restore.Status.Phase == ecv1alpha1.RestorePhasePending {
		restore.Status.Phase = ecv1alpha1.RestorePhaseDownloading
	}
	target, err := r.ensureStampedTargetCluster(ctx, restore, src, store)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 5. WATCH: the genesis member must boot from the restored data dir. Detect a
	//    wedged restore init-container (corrupt snapshot, bad creds) as a terminal
	//    failure; mark Completed once the StatefulSet reports a ready member.
	restore.Status.Phase = ecv1alpha1.RestorePhaseRestoring
	ready, failErr, err := r.observeRestoreProgress(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failErr != nil {
		return ctrl.Result{}, failErr
	}
	if !ready {
		// Still booting; requeue without churning the status.
		return ctrl.Result{RequeueAfter: restoreRequeueInterval}, nil
	}

	// 6. Record success — a member booted from the restored data.
	now := metav1.Now()
	restore.Status.Phase = ecv1alpha1.RestorePhaseCompleted
	restore.Status.SnapshotLocation = fmt.Sprintf("%s://%s", store.Scheme(),
		objectstore.JoinKey(src.destination.Prefix, src.key))
	restore.Status.RestoredCluster = target.Name
	restore.Status.CompletionTime = &now
	meta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
		Type:               ecv1alpha1.RestoreConditionSucceeded,
		Status:             metav1.ConditionTrue,
		Reason:             "SnapshotRestored",
		Message:            fmt.Sprintf("snapshot %q restored into cluster %q", src.key, target.Name),
		ObservedGeneration: restore.Generation,
	})
	r.event(restore, corev1.EventTypeNormal, "Restored",
		fmt.Sprintf("restored snapshot into EtcdCluster %q", target.Name))

	return ctrl.Result{}, nil
}

// verifySnapshotReachable opens the snapshot object and reads a byte to confirm
// it exists and is fetchable, then closes it. A missing object surfaces as a
// terminal failure whose message contains "download"/"not found", satisfying the
// missing-object contract without waiting for a member pod to crash-loop.
func (r *EtcdRestoreReconciler) verifySnapshotReachable(
	ctx context.Context, store objectstore.Store, key string,
) error {
	body, err := store.Download(ctx, key)
	if err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return fmt.Errorf("download snapshot %q: not found: %w", key, err)
		}
		return fmt.Errorf("download snapshot %q: %w", key, err)
	}
	defer func() { _ = body.Close() }()
	buf := make([]byte, 1)
	if _, rerr := body.Read(buf); rerr != nil && !errors.Is(rerr, io.EOF) {
		return fmt.Errorf("download snapshot %q: %w", key, rerr)
	}
	return nil
}

// resolvedSource is the flattened source of a restore: where the snapshot lives
// and the etcd version to stamp the target cluster with.
type resolvedSource struct {
	destination ecv1alpha1.BackupDestination
	key         string
	version     string
}

// resolveSource turns the spec's Source (backupRef | location) into a concrete
// destination + object key. For a backupRef it reads the referenced EtcdBackup,
// requiring it to have completed, and derives the key from its recorded
// snapshot location relative to the destination prefix.
func (r *EtcdRestoreReconciler) resolveSource(ctx context.Context, restore *ecv1alpha1.EtcdRestore) (resolvedSource, error) {
	src := restore.Spec.Source
	if src.Location != nil {
		return resolvedSource{
			destination: src.Location.Destination,
			key:         src.Location.Key,
		}, nil
	}

	// backupRef path.
	var backup ecv1alpha1.EtcdBackup
	key := types.NamespacedName{Namespace: restore.Namespace, Name: src.BackupRef.Name}
	if err := r.Get(ctx, key, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return resolvedSource{}, fmt.Errorf("referenced EtcdBackup %q not found", src.BackupRef.Name)
		}
		return resolvedSource{}, fmt.Errorf("get EtcdBackup %q: %w", src.BackupRef.Name, err)
	}
	if backup.Status.Phase != ecv1alpha1.BackupPhaseCompleted {
		return resolvedSource{}, fmt.Errorf(
			"referenced EtcdBackup %q has not completed (phase %q)", src.BackupRef.Name, backup.Status.Phase)
	}

	objKey, err := snapshotKeyFromBackup(&backup)
	if err != nil {
		return resolvedSource{}, err
	}
	return resolvedSource{
		destination: backup.Spec.Destination,
		key:         objKey,
		// Best-effort version hint: if the source EtcdCluster the backup was
		// taken from still exists, inherit its version so the operator need not
		// restate it. The caller falls back to target.Version (and errors if
		// neither is available).
		version: r.sourceClusterVersion(ctx, restore.Namespace, backup.Spec.ClusterRef),
	}, nil
}

// sourceClusterVersion returns the etcd version of the named cluster if it
// still exists, or "" otherwise. It is a best-effort hint, never an error: the
// originating cluster is commonly gone by restore time.
func (r *EtcdRestoreReconciler) sourceClusterVersion(ctx context.Context, namespace, name string) string {
	var cluster ecv1alpha1.EtcdCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &cluster); err != nil {
		return ""
	}
	return cluster.Spec.Version
}

// buildStore constructs an objectstore.Store from a destination, resolving and
// audit-logging credentials.
func (r *EtcdRestoreReconciler) buildStore(
	ctx context.Context, namespace string, dst ecv1alpha1.BackupDestination,
) (objectstore.Store, error) {
	osDst, err := toObjectStoreDestination(dst)
	if err != nil {
		return nil, err
	}
	creds, err := resolveStoreCredentials(ctx, r.Client, log.FromContext(ctx), namespace, dst)
	if err != nil {
		return nil, err
	}
	store, err := r.NewStore(ctx, osDst, creds)
	if err != nil {
		return nil, fmt.Errorf("build object store: %w", err)
	}
	return store, nil
}

// ensureStampedTargetCluster creates the target EtcdCluster if it does not
// exist, or verifies that an existing one is empty (no ready members), and in
// both cases ensures it carries the restore-source annotation so the cluster
// controller injects the restore init-container. It returns the cluster.
//
// The annotation is the single gate that turns a normal cluster into a
// restore-target; it is set at creation (so the very first StatefulSet is a
// restore pod template) and reconciled onto an existing empty shell.
func (r *EtcdRestoreReconciler) ensureStampedTargetCluster(
	ctx context.Context, restore *ecv1alpha1.EtcdRestore, src resolvedSource, store objectstore.Store,
) (*ecv1alpha1.EtcdCluster, error) {
	t := restore.Spec.Target
	clusterKey := types.NamespacedName{Namespace: restore.Namespace, Name: t.Name}

	annotationValue, err := r.buildRestoreAnnotation(src, store)
	if err != nil {
		return nil, err
	}

	var existing ecv1alpha1.EtcdCluster
	getErr := r.Get(ctx, clusterKey, &existing)
	switch {
	case getErr == nil:
		// A cluster already exists: it must be an empty shell, never an active
		// cluster with data, or the restore would silently diverge from it.
		if err := r.assertClusterEmpty(ctx, &existing); err != nil {
			return nil, err
		}
		// Reconcile the restore annotation onto the empty shell if missing.
		if existing.Annotations[restoreSourceAnnotation] != annotationValue {
			if existing.Annotations == nil {
				existing.Annotations = map[string]string{}
			}
			existing.Annotations[restoreSourceAnnotation] = annotationValue
			if err := r.Update(ctx, &existing); err != nil {
				return nil, fmt.Errorf("stamp restore annotation on EtcdCluster %q: %w", t.Name, err)
			}
		}
		return &existing, nil
	case apierrors.IsNotFound(getErr):
		// Fall through to create a fresh, stamped cluster.
	default:
		return nil, fmt.Errorf("get target EtcdCluster %q: %w", t.Name, getErr)
	}

	version := t.Version
	if version == "" {
		version = src.version
	}
	if version == "" {
		return nil, fmt.Errorf("target.version must be set when restoring from an explicit location")
	}
	size := t.Size
	if size == 0 {
		size = 1
	}

	cluster := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        t.Name,
			Namespace:   restore.Namespace,
			Annotations: map[string]string{restoreSourceAnnotation: annotationValue},
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:        size,
			Version:     version,
			StorageSpec: t.StorageSpec,
		},
	}
	// Own the created cluster so deleting the EtcdRestore can GC the restored
	// cluster if desired, and so the lineage is discoverable.
	if err := ctrl.SetControllerReference(restore, cluster, r.Scheme); err != nil {
		return nil, fmt.Errorf("set owner on target cluster: %w", err)
	}
	if err := r.Create(ctx, cluster); err != nil {
		return nil, fmt.Errorf("create target EtcdCluster %q: %w", t.Name, err)
	}
	r.event(restore, corev1.EventTypeNormal, "ClusterCreated",
		fmt.Sprintf("created EtcdCluster %q for restore", t.Name))
	return cluster, nil
}

// buildRestoreAnnotation assembles the JSON restore-source annotation the
// cluster controller reads to inject the restore init-container. It carries the
// snapshot addressing, the creds Secret *name* (never values), the operator
// image, and a generation token (the snapshot key) for the init-container's
// idempotency marker.
func (r *EtcdRestoreReconciler) buildRestoreAnnotation(
	src resolvedSource, store objectstore.Store,
) (string, error) {
	if r.OperatorImage == "" {
		return "", fmt.Errorf("operator image is not configured; cannot build restore init-container " +
			"(set --operator-image / OPERATOR_IMAGE)")
	}
	osDst, err := toObjectStoreDestination(src.destination)
	if err != nil {
		return "", err
	}
	credsName := ""
	if src.destination.SecretRef != nil {
		credsName = src.destination.SecretRef.Name
	}
	rs := restoreSource{
		Provider:        string(osDst.Provider),
		Bucket:          osDst.Bucket,
		Prefix:          osDst.Prefix,
		Key:             src.key,
		Region:          osDst.Region,
		Endpoint:        osDst.Endpoint,
		ForcePathStyle:  osDst.ForcePathStyle,
		CredsSecretName: credsName,
		OperatorImage:   r.OperatorImage,
		Generation:      src.key,
	}
	return encodeRestoreSource(rs)
}

// observeRestoreProgress inspects the restore-target cluster's StatefulSet and
// genesis pod. It returns (ready=true) once a member reports Ready (the restore
// init-container ran and etcd booted from the restored data); a non-nil failErr
// when the restore init-container is wedged (corrupt snapshot, bad creds,
// CrashLoopBackOff) so the restore is marked Failed for the right reason; and
// (false,nil,nil) while the member is still coming up (caller requeues).
func (r *EtcdRestoreReconciler) observeRestoreProgress(
	ctx context.Context, target *ecv1alpha1.EtcdCluster,
) (ready bool, failErr error, err error) {
	var sts appsv1.StatefulSet
	stsKey := types.NamespacedName{Namespace: target.Namespace, Name: target.Name}
	if getErr := r.Get(ctx, stsKey, &sts); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			// StatefulSet not created yet; keep waiting.
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("get StatefulSet for restore target %q: %w", target.Name, getErr)
	}
	if sts.Status.ReadyReplicas > 0 {
		return true, nil, nil
	}

	// Not ready yet: check the genesis pod for a wedged restore init-container so
	// a corrupt snapshot / bad creds fails terminally instead of looping forever.
	genesisPod := fmt.Sprintf("%s-0", target.Name)
	var pod corev1.Pod
	if getErr := r.Get(ctx, types.NamespacedName{Namespace: target.Namespace, Name: genesisPod}, &pod); getErr != nil {
		// No pod yet (or transient): keep waiting.
		return false, nil, nil
	}
	if reason := restoreInitContainerFailure(&pod); reason != "" {
		return false, fmt.Errorf("restore init-container failed: %s", reason), nil
	}
	return false, nil, nil
}

// restoreInitContainerFailure returns a non-empty diagnosis if the restore
// init-container has terminated non-zero or is stuck in a known-fatal waiting
// state. A snapshot that is corrupt makes `restore-localize`'s in-process
// snapshot.Restore exit non-zero, surfacing here.
func restoreInitContainerFailure(pod *corev1.Pod) string {
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Name != restoreInitContainerName {
			continue
		}
		if t := cs.LastTerminationState.Terminated; t != nil && t.ExitCode != 0 {
			return fmt.Sprintf("%s exited %d: %s", cs.Name, t.ExitCode, t.Reason)
		}
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
			return fmt.Sprintf("%s exited %d: %s", cs.Name, t.ExitCode, t.Reason)
		}
		if w := cs.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" {
			return fmt.Sprintf("%s in CrashLoopBackOff: %s", cs.Name, w.Message)
		}
	}
	return ""
}

// assertClusterEmpty rejects restoring into a cluster that already has members,
// UNLESS that cluster is already stamped as this restore's target. Emptiness is
// judged by the backing StatefulSet's ready replicas: a restore must be the
// cluster's genesis, so a ready member on an UNSTAMPED cluster means foreign data
// already exists (reject). A ready member on an already-STAMPED cluster is our
// own restored genesis booting — the success signal, not a populated-target
// rejection — so it is allowed (this reconciles the controller's "empty only"
// guard with the init-container's "skip if already restored" idempotency).
func (r *EtcdRestoreReconciler) assertClusterEmpty(ctx context.Context, cluster *ecv1alpha1.EtcdCluster) error {
	if _, alreadyStamped := cluster.Annotations[restoreSourceAnnotation]; alreadyStamped {
		// Restore already in progress against this cluster; ready members are our
		// own restored member, not a foreign populated cluster.
		return nil
	}
	var sts appsv1.StatefulSet
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.Get(ctx, key, &sts); err != nil {
		if apierrors.IsNotFound(err) {
			// Cluster object exists but no StatefulSet yet: an empty shell, OK.
			return nil
		}
		return fmt.Errorf("get StatefulSet for target cluster %q: %w", cluster.Name, err)
	}
	if sts.Status.ReadyReplicas > 0 {
		return fmt.Errorf(
			"target EtcdCluster %q already has %d ready member(s); refusing to restore over a non-empty cluster",
			cluster.Name, sts.Status.ReadyReplicas)
	}
	return nil
}

func (r *EtcdRestoreReconciler) markFailed(restore *ecv1alpha1.EtcdRestore, err error) {
	restore.Status.Phase = ecv1alpha1.RestorePhaseFailed
	meta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
		Type:               ecv1alpha1.RestoreConditionSucceeded,
		Status:             metav1.ConditionFalse,
		Reason:             "RestoreFailed",
		Message:            err.Error(),
		ObservedGeneration: restore.Generation,
	})
	r.event(restore, corev1.EventTypeWarning, "RestoreFailed", err.Error())
}

// event records a Kubernetes Event when a Recorder is wired (it is nil in unit
// tests that do not exercise events).
func (r *EtcdRestoreReconciler) event(restore *ecv1alpha1.EtcdRestore, eventtype, reason, message string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(restore, eventtype, reason, message)
}

// SetupWithManager registers the reconciler and wires the default object-store
// factory if the caller did not inject one. The reconciler owns the target
// EtcdCluster, so a periodic requeue (RequeueAfter) drives the watch for the
// genesis member becoming Ready; no extra StatefulSet watch is required for
// correctness.
func (r *EtcdRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.NewStore == nil {
		r.NewStore = objectstore.New
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("etcdrestore-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdRestore{}).
		Owns(&ecv1alpha1.EtcdCluster{}).
		Complete(r)
}
