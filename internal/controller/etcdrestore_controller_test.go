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
	"sync"
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

// --- test doubles -----------------------------------------------------------

// fakeRestorer records what it was asked to restore and returns canned bytes
// read (or an error) instead of exec'ing into a pod.
type fakeRestorer struct {
	mu        sync.Mutex
	gotParams RestoreParams
	gotBytes  []byte
	calls     int
	err       error
}

func (f *fakeRestorer) Restore(_ context.Context, params RestoreParams, r io.Reader) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.gotParams = params
	if f.err != nil {
		return 0, f.err
	}
	data, err := io.ReadAll(r)
	f.gotBytes = data
	return int64(len(data)), err
}

// --- fixtures ---------------------------------------------------------------

const testRestoreName = "restore-1"

func newRestoreReconciler(
	t *testing.T, store *fakeStore, restorer *fakeRestorer, objs ...client.Object,
) *EtcdRestoreReconciler {
	t.Helper()
	s := backupScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&ecv1alpha1.EtcdRestore{}).
		Build()
	return &EtcdRestoreReconciler{
		Client:   cl,
		Scheme:   s,
		Restorer: restorer,
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

// --- tests ------------------------------------------------------------------

func TestRestore_HappyPath_ExplicitLocation(t *testing.T) {
	store := newFakeStore("etcd/backups")
	// Seed the snapshot object at the requested key.
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("SNAPSHOT"), -1)
	require.NoError(t, err)

	restorer := &fakeRestorer{}
	restore := locationRestore("restored-a", "v3.6.1")
	r := newRestoreReconciler(t, store, restorer, restore)

	_, err = reconcileRestore(t, r)
	require.NoError(t, err)

	got := getRestore(t, r)
	assert.Equal(t, ecv1alpha1.RestorePhaseCompleted, got.Status.Phase)
	assert.Equal(t, "restored-a", got.Status.RestoredCluster)
	assert.NotNil(t, got.Status.CompletionTime)
	assert.Contains(t, got.Status.SnapshotLocation, "etcd/backups/etcd-a/snap.db")
	cond := meta.FindStatusCondition(got.Status.Conditions, ecv1alpha1.RestoreConditionSucceeded)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)

	// The restorer got the snapshot bytes and the genesis member identity.
	assert.Equal(t, 1, restorer.calls)
	assert.Equal(t, []byte("SNAPSHOT"), restorer.gotBytes)
	assert.Equal(t, "restored-a-0", restorer.gotParams.MemberName)
	assert.Equal(t, "restored-a-0", restorer.gotParams.Pod.Name)
	assert.Contains(t, restorer.gotParams.PeerURL, "restored-a-0.restored-a.ns1.svc.cluster.local:2380")

	// The target EtcdCluster was created, owned by the restore, with the spec.
	var cluster ecv1alpha1.EtcdCluster
	require.NoError(t, r.Get(context.Background(),
		types.NamespacedName{Name: "restored-a", Namespace: testNS}, &cluster))
	assert.Equal(t, "v3.6.1", cluster.Spec.Version)
	assert.Equal(t, 1, cluster.Spec.Size)
	require.Len(t, cluster.OwnerReferences, 1)
	assert.Equal(t, "EtcdRestore", cluster.OwnerReferences[0].Kind)
}

func TestRestore_HappyPath_BackupRef(t *testing.T) {
	// A completed EtcdBackup of cluster etcd-a; the restore references it.
	cluster, _ := readyCluster() // source cluster still exists -> version hint
	backup := s3Backup("etcd-a")
	backup.Status.Phase = ecv1alpha1.BackupPhaseCompleted
	backup.Status.SnapshotLocation = "test://etcd/backups/etcd-a/backup-1-...db"

	// Seed the object at the key the backup controller would have written.
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
			Target: ecv1alpha1.RestoreTarget{Name: "restored-b"}, // no version -> inherit from source cluster
		},
	}
	restorer := &fakeRestorer{}
	r := newRestoreReconciler(t, store, restorer, cluster, backup, restore)

	_, err = reconcileRestore(t, r)
	require.NoError(t, err)

	got := getRestore(t, r)
	assert.Equal(t, ecv1alpha1.RestorePhaseCompleted, got.Status.Phase)
	assert.Equal(t, []byte("FROMBACKUP"), restorer.gotBytes)

	// Version was inherited from the still-existing source cluster.
	var created ecv1alpha1.EtcdCluster
	require.NoError(t, r.Get(context.Background(),
		types.NamespacedName{Name: "restored-b", Namespace: testNS}, &created))
	assert.Equal(t, cluster.Spec.Version, created.Spec.Version)
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
	r := newRestoreReconciler(t, store, &fakeRestorer{}, backup, restore)

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
	r := newRestoreReconciler(t, store, &fakeRestorer{}, restore)
	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_BothSourcesFails(t *testing.T) {
	restore := locationRestore("restored-e", "v3.6.1")
	restore.Spec.Source.BackupRef = &ecv1alpha1.BackupReference{Name: "x"}
	r := newRestoreReconciler(t, newFakeStore(""), &fakeRestorer{}, restore)
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
	r := newRestoreReconciler(t, newFakeStore(""), &fakeRestorer{}, restore)
	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_VersionRequiredForExplicitLocation(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("S"), -1)
	require.NoError(t, err)
	restore := locationRestore("restored-g", "") // no version, no source cluster -> error
	r := newRestoreReconciler(t, store, &fakeRestorer{}, restore)
	_, err = reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_NonEmptyTargetRejected(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("S"), -1)
	require.NoError(t, err)

	// A target cluster that already exists AND has a ready StatefulSet member.
	existing := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-h", Namespace: testNS},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "v3.6.1"},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-h", Namespace: testNS},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
	restore := locationRestore("restored-h", "v3.6.1")
	restorer := &fakeRestorer{}
	r := newRestoreReconciler(t, store, restorer, existing, sts, restore)

	_, err = reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
	assert.Equal(t, 0, restorer.calls, "must not restore over a non-empty cluster")
}

func TestRestore_EmptyExistingTargetAccepted(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("S"), -1)
	require.NoError(t, err)

	// Target exists but its StatefulSet reports zero ready members (empty shell).
	existing := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-i", Namespace: testNS},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "v3.6.1"},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-i", Namespace: testNS},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 0},
	}
	restore := locationRestore("restored-i", "v3.6.1")
	restorer := &fakeRestorer{}
	r := newRestoreReconciler(t, store, restorer, existing, sts, restore)

	_, err = reconcileRestore(t, r)
	require.NoError(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseCompleted, getRestore(t, r).Status.Phase)
	assert.Equal(t, 1, restorer.calls)
}

func TestRestore_SnapshotNotFoundFails(t *testing.T) {
	store := newFakeStore("etcd/backups") // empty: object absent
	restore := locationRestore("restored-j", "v3.6.1")
	r := newRestoreReconciler(t, store, &fakeRestorer{}, restore)

	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_DownloadErrorFails(t *testing.T) {
	store := newFakeStore("etcd/backups")
	store.downloadErr = fmt.Errorf("network down")
	restore := locationRestore("restored-k", "v3.6.1")
	r := newRestoreReconciler(t, store, &fakeRestorer{}, restore)

	_, err := reconcileRestore(t, r)
	require.Error(t, err)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, getRestore(t, r).Status.Phase)
}

func TestRestore_RestorerErrorFails(t *testing.T) {
	store := newFakeStore("etcd/backups")
	_, err := store.Upload(context.Background(), "etcd-a/snap.db", readerOf("S"), -1)
	require.NoError(t, err)
	restorer := &fakeRestorer{err: fmt.Errorf("etcdctl restore boom")}
	restore := locationRestore("restored-l", "v3.6.1")
	r := newRestoreReconciler(t, store, restorer, restore)

	_, err = reconcileRestore(t, r)
	require.Error(t, err)
	got := getRestore(t, r)
	assert.Equal(t, ecv1alpha1.RestorePhaseFailed, got.Status.Phase)
	cond := meta.FindStatusCondition(got.Status.Conditions, ecv1alpha1.RestoreConditionSucceeded)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
}

func TestRestore_TerminalIsNoOp(t *testing.T) {
	store := newFakeStore("etcd/backups")
	restore := locationRestore("restored-m", "v3.6.1")
	restore.Status.Phase = ecv1alpha1.RestorePhaseCompleted
	restorer := &fakeRestorer{}
	r := newRestoreReconciler(t, store, restorer, restore)

	_, err := reconcileRestore(t, r)
	require.NoError(t, err)
	assert.Equal(t, 0, restorer.calls)
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

func TestRestoreParamsForCluster(t *testing.T) {
	cluster := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd-z", Namespace: "nsz"},
	}
	p := restoreParamsForCluster(cluster)
	assert.Equal(t, "etcd-z-0", p.MemberName)
	assert.Equal(t, "etcd-z-0", p.Pod.Name)
	assert.Equal(t, "nsz", p.Pod.Namespace)
	assert.Contains(t, p.PeerURL, "etcd-z-0.etcd-z.nsz.svc.cluster.local:2380")
	assert.Equal(t, defaultRestoreDataDir, p.DataDir)
}
