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

	// RESTConfig is used to build the in-pod exec restorer at runtime.
	RESTConfig *rest.Config

	// Restorer writes the snapshot into the target member. Defaults to an
	// exec-based implementation in SetupWithManager; overridden in tests.
	Restorer Restorer

	// NewStore builds an object store from a destination and credentials.
	// Defaults to objectstore.New; overridden in tests with a fake.
	NewStore objectstore.Factory
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdbackups,verbs=get;list;watch

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

// runRestore performs validate -> resolve source -> download -> ensure target
// -> restore, mutating restore.Status as it progresses.
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

	// 3. Build the object store (resolving credentials from the optional
	//    secretRef, audit-logging only the secret *name*).
	store, err := r.buildStore(ctx, restore.Namespace, src.destination)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 4. Ensure the target is a fresh, empty cluster: create it if absent,
	//    reject it if it already has members (a restore must be a genesis).
	target, err := r.ensureEmptyTargetCluster(ctx, restore, src.version)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 5. Download the snapshot and stream it into the target member's restore.
	timeout := defaultRestoreTimeout
	if restore.Spec.RestoreTimeout != nil {
		timeout = restore.Spec.RestoreTimeout.Duration
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	restore.Status.Phase = ecv1alpha1.RestorePhaseDownloading
	body, err := store.Download(rctx, src.key)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("download snapshot %q: %w", src.key, err)
	}
	defer func() { _ = body.Close() }()

	restore.Status.Phase = ecv1alpha1.RestorePhaseRestoring
	params := restoreParamsForCluster(target)
	if _, err := r.Restorer.Restore(rctx, params, body); err != nil {
		return ctrl.Result{}, fmt.Errorf("restore snapshot into %q: %w", target.Name, err)
	}

	// 6. Record success.
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

// ensureEmptyTargetCluster creates the target EtcdCluster if it does not exist,
// or verifies that an existing one is empty (no ready members). It returns the
// cluster to restore into.
func (r *EtcdRestoreReconciler) ensureEmptyTargetCluster(
	ctx context.Context, restore *ecv1alpha1.EtcdRestore, versionHint string,
) (*ecv1alpha1.EtcdCluster, error) {
	t := restore.Spec.Target
	clusterKey := types.NamespacedName{Namespace: restore.Namespace, Name: t.Name}

	var existing ecv1alpha1.EtcdCluster
	err := r.Get(ctx, clusterKey, &existing)
	switch {
	case err == nil:
		// A cluster already exists: it must be an empty shell, never an active
		// cluster with data, or the restore would silently diverge from it.
		if err := r.assertClusterEmpty(ctx, &existing); err != nil {
			return nil, err
		}
		return &existing, nil
	case apierrors.IsNotFound(err):
		// Fall through to create a fresh cluster.
	default:
		return nil, fmt.Errorf("get target EtcdCluster %q: %w", t.Name, err)
	}

	version := t.Version
	if version == "" {
		version = versionHint
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
			Name:      t.Name,
			Namespace: restore.Namespace,
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
		fmt.Sprintf("created empty EtcdCluster %q for restore", t.Name))
	return cluster, nil
}

// assertClusterEmpty rejects restoring into a cluster that already has members.
// Emptiness is judged by the backing StatefulSet's ready replicas: a restore
// must be the cluster's genesis, so any ready member means data already exists.
func (r *EtcdRestoreReconciler) assertClusterEmpty(ctx context.Context, cluster *ecv1alpha1.EtcdCluster) error {
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

// SetupWithManager registers the reconciler and wires the default Restorer and
// object-store factory if the caller did not inject them.
func (r *EtcdRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.NewStore == nil {
		r.NewStore = objectstore.New
	}
	if r.Restorer == nil {
		cfg := r.RESTConfig
		if cfg == nil {
			cfg = mgr.GetConfig()
		}
		r.Restorer = newExecRestorer(cfg, backupContainerName)
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("etcdrestore-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdRestore{}).
		Complete(r)
}
