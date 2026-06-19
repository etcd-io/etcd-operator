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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/objectstore"
)

const (
	// defaultSnapshotTimeout bounds the snapshot-save step when the spec omits one.
	defaultSnapshotTimeout = 10 * time.Minute
	// backupContainerName is the etcd container name in member pods.
	backupContainerName = "etcd"
)

// EtcdBackupReconciler reconciles an EtcdBackup object: it takes a point-in-time
// snapshot of the referenced EtcdCluster and uploads it to object storage.
//
// The reconciler is intentionally thin: all cloud-specific behavior lives
// behind two seams — Snapshotter (produce the snapshot stream) and
// objectstore.Factory (write it to a bucket). Both are injectable so the
// orchestration is exercised by unit tests with no cloud creds and no live
// etcd.
type EtcdBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// RESTConfig is used to build the in-pod exec snapshotter at runtime.
	RESTConfig *rest.Config

	// Snapshotter produces the snapshot stream. Defaults to an exec-based
	// implementation in SetupWithManager; overridden in tests.
	Snapshotter Snapshotter

	// NewStore builds an object store from a destination and credentials.
	// Defaults to objectstore.New; overridden in tests with a fake.
	NewStore objectstore.Factory
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdbackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

// Reconcile drives a single EtcdBackup to completion. Because a backup is a
// one-shot job rather than a continuously-reconciled desired state, a terminal
// phase (Completed/Failed) short-circuits further work; the resource is the
// audit record of that snapshot.
func (r *EtcdBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var backup ecv1alpha1.EtcdBackup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal backups are immutable records; nothing more to do.
	if backup.Status.Phase == ecv1alpha1.BackupPhaseCompleted ||
		backup.Status.Phase == ecv1alpha1.BackupPhaseFailed {
		return ctrl.Result{}, nil
	}

	backup.Status.ObservedGeneration = backup.Generation

	result, err := r.runBackup(ctx, &backup)
	if err != nil {
		logger.Error(err, "backup failed", "backup", req.NamespacedName)
		r.markFailed(&backup, err)
	}

	if statusErr := r.Status().Update(ctx, &backup); statusErr != nil {
		// Prefer surfacing the original error if both occurred.
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, statusErr
	}
	return result, err
}

// runBackup performs the snapshot+upload, mutating backup.Status as it
// progresses. It returns a non-nil error on any failure; the caller records it.
func (r *EtcdBackupReconciler) runBackup(ctx context.Context, backup *ecv1alpha1.EtcdBackup) (ctrl.Result, error) {
	// 1. Resolve the target cluster and a member pod to snapshot from.
	pod, err := r.selectMemberPod(ctx, backup)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 2. Build the object store (resolving credentials from the optional secret).
	store, err := r.buildStore(ctx, backup)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. Take the snapshot and stream it into the uploader via a pipe. The GCS
	//    provider streams straight through; the S3 provider spools the stream to
	//    an ephemeral temp file first (PutObject needs a known length), so for
	//    S3 the snapshot does briefly land in the operator pod's scratch space.
	backup.Status.Phase = ecv1alpha1.BackupPhaseSnapshotting

	timeout := defaultSnapshotTimeout
	if backup.Spec.SnapshotTimeout != nil {
		timeout = backup.Spec.SnapshotTimeout.Duration
	}
	snapCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	key := snapshotObjectKey(backup)
	res, err := r.snapshotAndUpload(snapCtx, backup, store, pod, key)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 4. Record success.
	now := metav1.Now()
	backup.Status.Phase = ecv1alpha1.BackupPhaseCompleted
	backup.Status.SnapshotLocation = res.URI
	backup.Status.SnapshotSizeBytes = res.Size
	backup.Status.CompletionTime = &now
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               ecv1alpha1.BackupConditionSucceeded,
		Status:             metav1.ConditionTrue,
		Reason:             "SnapshotUploaded",
		Message:            fmt.Sprintf("snapshot uploaded to %s (%d bytes)", res.URI, res.Size),
		ObservedGeneration: backup.Generation,
	})

	// 5. Best-effort retention pruning. A pruning failure does not fail the
	//    backup itself — the snapshot is already safely uploaded.
	if err := r.applyRetention(ctx, backup, store); err != nil {
		log.FromContext(ctx).Error(err, "retention pruning failed (snapshot upload still succeeded)")
	}

	return ctrl.Result{}, nil
}

// snapshotAndUpload wires the snapshotter's stdout pipe into the store's
// Upload reader. The pipe gives backpressure: the snapshot only advances as
// fast as the upload drains it, bounding memory to the io.Copy buffer.
func (r *EtcdBackupReconciler) snapshotAndUpload(
	ctx context.Context,
	backup *ecv1alpha1.EtcdBackup,
	store objectstore.Store,
	pod types.NamespacedName,
	key string,
) (objectstore.UploadResult, error) {
	pr, pw := io.Pipe()

	// Producer: snapshot -> pipe writer.
	go func() {
		_, serr := r.Snapshotter.Snapshot(ctx, pod, pw)
		// Closing with the error propagates it to the uploader's Read.
		_ = pw.CloseWithError(serr)
	}()

	// Consumer: pipe reader -> object store. Drives both goroutines.
	backup.Status.Phase = ecv1alpha1.BackupPhaseUploading
	res, err := store.Upload(ctx, key, pr, -1)
	// Ensure the producer is unblocked if Upload returned early.
	_ = pr.CloseWithError(err)
	if err != nil {
		return objectstore.UploadResult{}, fmt.Errorf("snapshot/upload: %w", err)
	}
	return res, nil
}

// selectMemberPod resolves the referenced EtcdCluster's StatefulSet and returns
// a member pod to snapshot from. Any ready member yields a consistent
// cluster-wide snapshot, so the lowest-ordinal ready pod is chosen for
// determinism.
func (r *EtcdBackupReconciler) selectMemberPod(ctx context.Context, backup *ecv1alpha1.EtcdBackup) (types.NamespacedName, error) {
	var cluster ecv1alpha1.EtcdCluster
	clusterKey := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.ClusterRef}
	if err := r.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return types.NamespacedName{}, fmt.Errorf("referenced EtcdCluster %q not found", backup.Spec.ClusterRef)
		}
		return types.NamespacedName{}, fmt.Errorf("get EtcdCluster %q: %w", backup.Spec.ClusterRef, err)
	}

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, clusterKey, &sts); err != nil {
		return types.NamespacedName{}, fmt.Errorf("get StatefulSet for cluster %q: %w", backup.Spec.ClusterRef, err)
	}
	if sts.Status.ReadyReplicas < 1 {
		return types.NamespacedName{}, fmt.Errorf("cluster %q has no ready members to snapshot", backup.Spec.ClusterRef)
	}

	// StatefulSet pods are deterministically named <sts>-<ordinal>.
	return types.NamespacedName{
		Namespace: backup.Namespace,
		Name:      fmt.Sprintf("%s-0", sts.Name),
	}, nil
}

// buildStore constructs an objectstore.Store from the backup's destination,
// resolving credentials from the optional secretRef.
func (r *EtcdBackupReconciler) buildStore(ctx context.Context, backup *ecv1alpha1.EtcdBackup) (objectstore.Store, error) {
	dst := backup.Spec.Destination

	osDst, err := toObjectStoreDestination(dst)
	if err != nil {
		return nil, err
	}

	creds, err := r.resolveCredentials(ctx, backup.Namespace, dst)
	if err != nil {
		return nil, err
	}

	store, err := r.NewStore(ctx, osDst, creds)
	if err != nil {
		return nil, fmt.Errorf("build object store: %w", err)
	}
	return store, nil
}

// resolveCredentials reads the provider credentials from the referenced secret,
// if any. With no secretRef the returned Credentials is empty and providers
// fall back to ambient credentials (IRSA/Workload Identity).
func (r *EtcdBackupReconciler) resolveCredentials(
	ctx context.Context, namespace string, dst ecv1alpha1.BackupDestination,
) (objectstore.Credentials, error) {
	var creds objectstore.Credentials
	if dst.SecretRef == nil {
		return creds, nil
	}

	var secret corev1.Secret
	key := types.NamespacedName{Namespace: namespace, Name: dst.SecretRef.Name}
	if err := r.Get(ctx, key, &secret); err != nil {
		return creds, fmt.Errorf("get credentials secret %q: %w", dst.SecretRef.Name, err)
	}

	switch dst.Provider {
	case ecv1alpha1.BackupProviderS3:
		creds.AccessKeyID = string(secret.Data["accessKeyID"])
		creds.SecretAccessKey = string(secret.Data["secretAccessKey"])
		creds.SessionToken = string(secret.Data["sessionToken"])
	case ecv1alpha1.BackupProviderGCS:
		creds.ServiceAccountJSON = secret.Data["serviceAccountJSON"]
	}
	return creds, nil
}

// applyRetention deletes snapshots beyond the configured RetainCount under the
// backup's bucket/prefix, newest-first.
func (r *EtcdBackupReconciler) applyRetention(ctx context.Context, backup *ecv1alpha1.EtcdBackup, store objectstore.Store) error {
	pol := backup.Spec.Retention
	if pol == nil || pol.RetainCount <= 0 {
		return nil
	}

	objs, err := store.List(ctx, snapshotKeyPrefix(backup))
	if err != nil {
		return fmt.Errorf("list for retention: %w", err)
	}
	if int32(len(objs)) <= pol.RetainCount {
		return nil
	}

	// Listed keys are absolute (they include the destination prefix). Delete
	// joins its argument with the destination prefix again, so strip the prefix
	// to address each object relative to it.
	prefix := backup.Spec.Destination.Prefix
	for _, o := range objs[pol.RetainCount:] {
		relKey := relativeKey(prefix, o.Key)
		if err := store.Delete(ctx, relKey); err != nil {
			return fmt.Errorf("delete %q: %w", o.Key, err)
		}
	}
	return nil
}

func (r *EtcdBackupReconciler) markFailed(backup *ecv1alpha1.EtcdBackup, err error) {
	backup.Status.Phase = ecv1alpha1.BackupPhaseFailed
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               ecv1alpha1.BackupConditionSucceeded,
		Status:             metav1.ConditionFalse,
		Reason:             "BackupFailed",
		Message:            err.Error(),
		ObservedGeneration: backup.Generation,
	})
}

// SetupWithManager registers the reconciler and wires the default Snapshotter
// and object-store factory if the caller did not inject them.
func (r *EtcdBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.NewStore == nil {
		r.NewStore = objectstore.New
	}
	if r.Snapshotter == nil {
		cfg := r.RESTConfig
		if cfg == nil {
			cfg = mgr.GetConfig()
		}
		r.Snapshotter = newExecSnapshotter(cfg, backupContainerName)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecv1alpha1.EtcdBackup{}).
		Complete(r)
}
