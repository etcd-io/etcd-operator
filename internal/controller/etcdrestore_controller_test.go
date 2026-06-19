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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/objectstore"
)

// --- fixtures ---------------------------------------------------------------

const (
	testRestoreName  = "restore-1"
	testOperatorImg  = "ghcr.io/etcd/etcd-operator:test"
	restoreReadySize = 1
)

func newRestoreReconciler(
	t *testing.T, store *fakeStore, objs ...client.Object,
) *EtcdRestoreReconciler {
	t.Helper()
	s := backupScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&ecv1alpha1.EtcdRestore{}).
		Build()
	return &EtcdRestoreReconciler{
		Client:        cl,
		Scheme:        s,
		OperatorImage: testOperatorImg,
		NewStore: func(_ context.Context, _ objectstore.Destination, _ objectstore.Credentials) (objectstore.Store, error) {
			return store, nil
		},
	}
}

// locationRestore builds an EtcdRestore that restores an explicit object key.
func locationRestore(targetName, version string) *ecv1alpha1.EtcdRestore {
	return &ecv1alpha1.EtcdRestore{
		ObjectMeta: metav1.ObjectMeta{Name: testRestoreName, Namespace: testNS},
		Spec: ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{
				Location: &ecv1alpha1.SnapshotLocation{
					Destination: ecv1alpha1.BackupDestination{
						Provider: ecv1alpha1.BackupProviderS3,
						Prefix:   "etcd/backups",
						S3:       &ecv1alpha1.S3DestinationSpec{Bucket: "b", Region: "us-east-1"},
					},
					Key: "etcd-a/snap.db",
				},
			},
			Target: ecv1alpha1.RestoreTarget{Name: targetName, Size: 1, Version: version},
		},
	}
}

func reconcileRestore(t *testing.T, r *EtcdRestoreReconciler) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testRestoreName, Namespace: testNS},
	})
}

func getRestore(t *testing.T, r *EtcdRestoreReconciler) ecv1alpha1.EtcdRestore {
	t.Helper()
	var got ecv1alpha1.EtcdRestore
	require.NoError(t, r.Get(context.Background(),
		types.NamespacedName{Name: testRestoreName, Namespace: testNS}, &got))
	return got
}

func getCluster(t *testing.T, r *EtcdRestoreReconciler, name string) ecv1alpha1.EtcdCluster {
	t.Helper()
	var c ecv1alpha1.EtcdCluster
	require.NoError(t, r.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: testNS}, &c))
	return c
}

// --- tests ------------------------------------------------------------------

// TestRestore_StampsTargetAndRequeues proves the controller's core init-container
// model: it creates the target EtcdCluster, stamps it with the restore-source
// annotation (so the cluster controller injects the restore init-container), and
// requeues in Restoring (NOT Completed) until the genesis member boots.
func TestRestore_StampsTargetAndRequeues(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("SNAPSHOT"), -1)
	require.NoError(t, err)

	restore := locationRestore("restored-a", "v3.6.1")
	r := newRestoreReconciler(t, store, restore)

	res, err := reconcileRestore(t, r)
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "should requeue while the member boots")

	got := getRestore(t, r)
	assert.Equal(t, ecv1alpha1.RestorePhaseRestoring, got.Status.Phase)
	assert.NotEqual(t, ecv1alpha1.RestorePhaseCompleted, got.Status.Phase)

	// Target cluster created, owned by the restore, and stamped.
	cluster := getCluster(t, r, "restored-a")
	assert.Equal(t, "v3.6.1", cluster.Spec.Version)
	assert.Equal(t, 1, cluster.Spec.Size)
	require.Len(t, cluster.OwnerReferences, 1)
	assert.Equal(t, "EtcdRestore", cluster.OwnerReferences[0].Kind)

	// The annotation must decode to a valid restore source carrying our snapshot
	// addressing, the operator image, and a generation token.
	raw := cluster.Annotations[restoreSourceAnnotation]
	require.NotEmpty(t, raw, "target cluster must carry the restore-source annotation")
	src, decErr := decodeRestoreSource(raw)
	require.NoError(t, decErr)
	assert.Equal(t, "s3", src.Provider)
	assert.Equal(t, "b", src.Bucket)
	assert.Equal(t, "etcd-a/snap.db", src.Key)
	assert.Equal(t, "etcd/backups", src.Prefix)
	assert.Equal(t, testOperatorImg, src.OperatorImage)
	assert.NotEmpty(t, src.Generation)
}

// TestRestore_CompletesWhenMemberReady proves the controller marks Completed
// only once the restore-target StatefulSet reports a ready member — Completed
// truthfully means "a member booted from the restored data".
func TestRestore_CompletesWhenMemberReady(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("SNAPSHOT"), -1)
	require.NoError(t, err)

	restore := locationRestore("restored-ready", "v3.6.1")
	r := newRestoreReconciler(t, store, restore)

	// First reconcile: stamps + requeues.
	_, err = reconcileRestore(t, r)
	require.NoError(t, err)
	require.Equal(t, ecv1alpha1.RestorePhaseRestoring, getRestore(t, r).Status.Phase)

	// Simulate the cluster controller bringing a member up: create a ready STS.
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-ready", Namespace: testNS},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
	require.NoError(t, r.Create(context.Background(), sts))

	// Second reconcile: observes the ready member and completes.
	_, err = reconcileRestore(t, r)
	require.NoError(t, err)
	got := getRestore(t, r)
	assert.Equal(t, ecv1alpha1.RestorePhaseCompleted, got.Status.Phase)
	assert.Equal(t, "restored-ready", got.Status.RestoredCluster)
	assert.NotNil(t, got.Status.CompletionTime)
	assert.Contains(t, got.Status.SnapshotLocation, "etcd/backups/etcd-a/snap.db")
	cond := meta.FindStatusCondition(got.Status.Conditions, ecv1alpha1.RestoreConditionSucceeded)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
}

// TestRestore_BackupRefStampsInheritedVersion proves the backupRef path resolves
// the snapshot key + inherits the source cluster's version into the stamped
// target.
func TestRestore_BackupRefStampsInheritedVersion(t *testing.T) {
	cluster, _ := readyCluster() // source cluster still exists -> version hint
	backup := s3Backup("etcd-a")
	backup.Status.Phase = ecv1alpha1.BackupPhaseCompleted
	backup.Status.SnapshotLocation = "test://etcd/backups/etcd-a/backup-1-...db"

	store := newFakeStore("etcd/backups")
	objKey := snapshotObjectKey(backup)
	_, err := store.Upload(context.Background(), objKey, readerOf("FROMBACKUP"), -1)
	require.NoError(t, err)

	restore := &ecv1alpha1.EtcdRestore{
		ObjectMeta: metav1.ObjectMeta{Name: testRestoreName, Namespace: testNS},
		Spec: ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{
				BackupRef: &ecv1alpha1.BackupReference{Name: testBackupName},
			},
			Target: ecv1alpha1.RestoreTarget{Name: "restored-b"}, // no version -> inherit
		},
	}
	r := newRestoreReconciler(t, store, cluster, backup, restore)

	_, err = reconcileRestore(t, r)
	require.NoError(t, err)

	created := getCluster(t, r, "restored-b")
	assert.Equal(t, cluster.Spec.Version, created.Spec.Version)
	raw := created.Annotations[restoreSourceAnnotation]
	require.NotEmpty(t, raw)
	src, decErr := decodeRestoreSource(raw)
	require.NoError(t, decErr)
	assert.Equal(t, objKey, src.Key)
}

func TestRestore_BackupRefNotCompletedFails(t *testing.T) {
	backup := s3Backup("etcd-a") // phase is empty (Pending)
	store := newFakeStore("etcd/backups")
	restore := &ecv1alpha1.EtcdRestore{
		ObjectMeta: metav1.ObjectMeta{Name: testRestoreName, Namespace: testNS},
		Spec: ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{BackupRef: &ecv1alpha1.BackupReference{Name: testBackupName}},
			Target: ecv1alpha1.RestoreTarget{Name: "restored-c", Version: "v3.6.1"},
		},
	}
	r := newRestoreReconciler(t, store, backup, restore)

	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_MissingBackupRefFails(t *testing.T) {
	store := newFakeStore("etcd/backups")
	restore := &ecv1alpha1.EtcdRestore{
		ObjectMeta: metav1.ObjectMeta{Name: testRestoreName, Namespace: testNS},
		Spec: ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{BackupRef: &ecv1alpha1.BackupReference{Name: "absent"}},
			Target: ecv1alpha1.RestoreTarget{Name: "restored-d", Version: "v3.6.1"},
		},
	}
	r := newRestoreReconciler(t, store, restore)
	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_BothSourcesFails(t *testing.T) {
	restore := locationRestore("restored-e", "v3.6.1")
	restore.Spec.Source.BackupRef = &ecv1alpha1.BackupReference{Name: "x"}
	r := newRestoreReconciler(t, newFakeStore(""), restore)
	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_NoSourceFails(t *testing.T) {
	restore := &ecv1alpha1.EtcdRestore{
		ObjectMeta: metav1.ObjectMeta{Name: testRestoreName, Namespace: testNS},
		Spec: ecv1alpha1.EtcdRestoreSpec{
			Target: ecv1alpha1.RestoreTarget{Name: "restored-f", Version: "v3.6.1"},
		},
	}
	r := newRestoreReconciler(t, newFakeStore(""), restore)
	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_VersionRequiredForExplicitLocation(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("S"), -1)
	require.NoError(t, err)
	restore := locationRestore("restored-g", "") // no version, no source cluster -> error
	r := newRestoreReconciler(t, store, restore)
	_, err = reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

// TestRestore_NonEmptyUnstampedTargetRejected proves a restore into a cluster
// that already has a ready member AND is NOT already a restore target is
// rejected, and is never stamped (so the cluster controller never mutates that
// live cluster's pod template).
func TestRestore_NonEmptyUnstampedTargetRejected(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("S"), -1)
	require.NoError(t, err)

	existing := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-h", Namespace: testNS},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "v3.6.1"},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-h", Namespace: testNS},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
	restore := locationRestore("restored-h", "v3.6.1")
	r := newRestoreReconciler(t, store, existing, sts, restore)

	_, err = reconcileRestore(t, r)
	require.Error(t, err)
	got := getRestore(t, r)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, got.Status.Phase)
	// The live cluster must NOT have been stamped.
	cluster := getCluster(t, r, "restored-h")
	_, stamped := cluster.Annotations[restoreSourceAnnotation]
	assert.False(t, stamped, "must not stamp (and thus mutate) a non-empty live cluster")
}

// TestRestore_EmptyExistingTargetStamped proves an existing empty shell cluster
// is accepted and gets stamped.
func TestRestore_EmptyExistingTargetStamped(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("S"), -1)
	require.NoError(t, err)

	existing := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-i", Namespace: testNS},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "v3.6.1"},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-i", Namespace: testNS},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 0},
	}
	restore := locationRestore("restored-i", "v3.6.1")
	r := newRestoreReconciler(t, store, existing, sts, restore)

	_, err = reconcileRestore(t, r)
	require.NoError(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseRestoring, getRestore(t, r).Status.Phase)
	cluster := getCluster(t, r, "restored-i")
	_, stamped := cluster.Annotations[restoreSourceAnnotation]
	assert.True(t, stamped)
}

func TestRestore_SnapshotNotFoundFails(t *testing.T) {
	store := newFakeStore("etcd/backups") // empty: object absent
	restore := locationRestore("restored-j", "v3.6.1")
	r := newRestoreReconciler(t, store, restore)

	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	got := getRestore(t, r)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, got.Status.Phase)
	// The target cluster must not have been created/stamped — verification fails
	// before the cluster is touched.
	var cluster ecv1alpha1.EtcdCluster
	getErr := r.Get(context.Background(), types.NamespacedName{Name: "restored-j", Namespace: testNS}, &cluster)
	assert.Error(t, getErr, "no cluster should be created when the snapshot is missing")
}

func TestRestore_DownloadErrorFails(t *testing.T) {
	store := newFakeStore("etcd/backups")
	store.downloadErr = fmt.Errorf("network down")
	restore := locationRestore("restored-k", "v3.6.1")
	r := newRestoreReconciler(t, store, restore)

	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

// TestRestore_NoOperatorImageFails proves the controller refuses to stamp a
// restore target when it does not know its own image (it could not build a
// working restore init-container).
func TestRestore_NoOperatorImageFails(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("S"), -1)
	require.NoError(t, err)
	restore := locationRestore("restored-noimg", "v3.6.1")
	r := newRestoreReconciler(t, store, restore)
	r.OperatorImage = "" // simulate a misconfigured operator
	_, err = reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_TerminalIsNoOp(t *testing.T) {
	store := newFakeStore("etcd/backups")
	restore := locationRestore("restored-m", "v3.6.1")
	restore.Status.Phase = ecv1alpha1.RestorePhaseCompleted
	r := newRestoreReconciler(t, store, restore)

	_, err := reconcileRestore(t, r)
	require.NoError(t, err)
	// No target cluster should have been created by a terminal restore.
	var cluster ecv1alpha1.EtcdCluster
	getErr := r.Get(context.Background(), types.NamespacedName{Name: "restored-m", Namespace: testNS}, &cluster)
	assert.Error(t, getErr)
}

func TestValidateRestoreSpec(t *testing.T) {
	loc := &ecv1alpha1.SnapshotLocation{
		Destination: ecv1alpha1.BackupDestination{
			Provider: ecv1alpha1.BackupProviderS3,
			S3:       &ecv1alpha1.S3DestinationSpec{Bucket: "b"},
		},
		Key: "k.db",
	}
	cases := []struct {
		name    string
		spec    ecv1alpha1.EtcdRestoreSpec
		wantErr bool
	}{
		{"location ok", ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{Location: loc},
			Target: ecv1alpha1.RestoreTarget{Name: "c"},
		}, false},
		{"backupRef ok", ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{BackupRef: &ecv1alpha1.BackupReference{Name: "b"}},
			Target: ecv1alpha1.RestoreTarget{Name: "c"},
		}, false},
		{"both sources", ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{Location: loc, BackupRef: &ecv1alpha1.BackupReference{Name: "b"}},
			Target: ecv1alpha1.RestoreTarget{Name: "c"},
		}, true},
		{"no source", ecv1alpha1.EtcdRestoreSpec{
			Target: ecv1alpha1.RestoreTarget{Name: "c"},
		}, true},
		{"no target name", ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{Location: loc},
		}, true},
		{"location missing key", ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{Location: &ecv1alpha1.SnapshotLocation{Destination: loc.Destination}},
			Target: ecv1alpha1.RestoreTarget{Name: "c"},
		}, true},
		{"location bad destination", ecv1alpha1.EtcdRestoreSpec{
			Source: ecv1alpha1.SnapshotSource{Location: &ecv1alpha1.SnapshotLocation{
				Destination: ecv1alpha1.BackupDestination{Provider: ecv1alpha1.BackupProviderS3}, // no S3 block
				Key:         "k",
			}},
			Target: ecv1alpha1.RestoreTarget{Name: "c"},
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateRestoreSpec(&c.spec)
			if c.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestRestoreSourceAnnotationRoundTrip proves encode/decode of the annotation is
// lossless and that decode rejects incomplete payloads (which would otherwise
// build a data-less restore pod).
func TestRestoreSourceAnnotationRoundTrip(t *testing.T) {
	src := restoreSource{
		Provider: "s3", Bucket: "b", Prefix: "p", Key: "k.db",
		Region: "us-east-1", Endpoint: "http://minio:9000", ForcePathStyle: true,
		CredsSecretName: "creds", OperatorImage: testOperatorImg, Generation: "k.db",
	}
	raw, err := encodeRestoreSource(src)
	require.NoError(t, err)
	got, err := decodeRestoreSource(raw)
	require.NoError(t, err)
	assert.Equal(t, src, got)

	_, err = decodeRestoreSource(`{"provider":"s3","bucket":"b"}`) // missing key + image
	require.Error(t, err)
}
