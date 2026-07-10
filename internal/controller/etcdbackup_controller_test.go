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
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/objectstore"
)

// --- test doubles -----------------------------------------------------------

// fakeSnapshotter returns canned snapshot bytes (or an error) instead of
// exec'ing into a pod.
type fakeSnapshotter struct {
	data   []byte
	err    error
	gotPod types.NamespacedName
	calls  int
	mu     sync.Mutex
	delay  time.Duration
}

func (f *fakeSnapshotter) Snapshot(ctx context.Context, pod types.NamespacedName, w io.Writer) (int64, error) {
	f.mu.Lock()
	f.calls++
	f.gotPod = pod
	f.mu.Unlock()
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if f.err != nil {
		return 0, f.err
	}
	n, err := w.Write(f.data)
	return int64(n), err
}

// fakeStore is an in-memory objectstore.Store for controller tests.
type fakeStore struct {
	mu          sync.Mutex
	prefix      string
	objects     map[string][]byte
	times       map[string]time.Time
	clock       time.Time
	uploadErr   error
	downloadErr error
	deleted     []string
}

func newFakeStore(prefix string) *fakeStore {
	return &fakeStore{
		prefix:  prefix,
		objects: map[string][]byte{},
		times:   map[string]time.Time{},
		clock:   time.Unix(0, 0),
	}
}

func (s *fakeStore) key(k string) string { return objectstore.JoinKey(s.prefix, k) }

func (s *fakeStore) Upload(_ context.Context, key string, r io.Reader, _ int64) (objectstore.UploadResult, error) {
	if s.uploadErr != nil {
		return objectstore.UploadResult{}, s.uploadErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return objectstore.UploadResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	full := s.key(key)
	s.objects[full] = data
	s.clock = s.clock.Add(time.Second)
	s.times[full] = s.clock
	return objectstore.UploadResult{URI: "test://" + full, Size: int64(len(data))}, nil
}

func (s *fakeStore) Download(_ context.Context, key string) (io.ReadCloser, error) {
	if s.downloadErr != nil {
		return nil, s.downloadErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	full := s.key(key)
	data, ok := s.objects[full]
	if !ok {
		return nil, fmt.Errorf("fakeStore: %q: %w", full, objectstore.ErrNotFound)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *fakeStore) List(_ context.Context, keyPrefix string) ([]objectstore.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	full := s.key(keyPrefix)
	var out []objectstore.ObjectInfo
	for k, v := range s.objects {
		if len(full) == 0 || hasStorePrefix(k, full) {
			out = append(out, objectstore.ObjectInfo{Key: k, Size: int64(len(v)), LastModified: s.times[k]})
		}
	}
	// newest first
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].LastModified.After(out[i].LastModified) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

func (s *fakeStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	full := s.key(key)
	s.deleted = append(s.deleted, full)
	delete(s.objects, full)
	delete(s.times, full)
	return nil
}

func (s *fakeStore) Scheme() string { return "test" }

func hasStorePrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

// --- fixtures ---------------------------------------------------------------

func backupScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, ecv1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, appsv1.AddToScheme(s))
	return s
}

// Shared identifiers for the controller test fixtures.
const (
	testNS          = "ns1"
	testClusterName = "etcd-a"
	testBackupName  = "backup-1"
)

func readyCluster() (*ecv1alpha1.EtcdCluster, *appsv1.StatefulSet) {
	c := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNS},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "v3.6.1"},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testNS},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
	return c, sts
}

func s3Backup(clusterRef string) *ecv1alpha1.EtcdBackup {
	return &ecv1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testBackupName,
			Namespace:         testNS,
			CreationTimestamp: metav1.NewTime(time.Date(2026, 6, 17, 1, 2, 3, 0, time.UTC)),
		},
		Spec: ecv1alpha1.EtcdBackupSpec{
			ClusterRef: clusterRef,
			Destination: ecv1alpha1.BackupDestination{
				Provider: ecv1alpha1.BackupProviderS3,
				Prefix:   "etcd/backups",
				S3:       &ecv1alpha1.S3DestinationSpec{Bucket: "b", Region: "us-east-1"},
			},
		},
	}
}

func newReconciler(t *testing.T, store *fakeStore, snap *fakeSnapshotter, objs ...client.Object) *EtcdBackupReconciler {
	t.Helper()
	s := backupScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&ecv1alpha1.EtcdBackup{}).
		Build()
	return &EtcdBackupReconciler{
		Client:      cl,
		Scheme:      s,
		Snapshotter: snap,
		NewStore: func(_ context.Context, _ objectstore.Destination, _ objectstore.Credentials) (objectstore.Store, error) {
			return store, nil
		},
	}
}

// --- tests ------------------------------------------------------------------

func TestReconcile_HappyPath(t *testing.T) {
	cluster, sts := readyCluster()
	backup := s3Backup("etcd-a")
	store := newFakeStore("etcd/backups")
	snap := &fakeSnapshotter{data: []byte("SNAPSHOTDATA")}

	r := newReconciler(t, store, snap, cluster, sts, backup)

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "backup-1", Namespace: "ns1"},
	})
	require.NoError(t, err)
	assert.True(t, res.IsZero())

	// Status reflects success.
	var got ecv1alpha1.EtcdBackup
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "backup-1", Namespace: "ns1"}, &got))
	assert.Equal(t, ecv1alpha1.BackupPhaseCompleted, got.Status.Phase)
	assert.Equal(t, int64(len("SNAPSHOTDATA")), got.Status.SnapshotSizeBytes)
	assert.NotNil(t, got.Status.CompletionTime)
	assert.Contains(t, got.Status.SnapshotLocation, "etcd/backups/etcd-a/backup-1-")
	cond := meta.FindStatusCondition(got.Status.Conditions, ecv1alpha1.BackupConditionSucceeded)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)

	// Snapshotter was asked for the lowest-ordinal pod.
	assert.Equal(t, "etcd-a-0", snap.gotPod.Name)
	assert.Equal(t, "ns1", snap.gotPod.Namespace)

	// The bytes actually landed in the store under the joined key.
	require.Len(t, store.objects, 1)
	for k, v := range store.objects {
		assert.Contains(t, k, "etcd/backups/etcd-a/backup-1-")
		assert.Equal(t, []byte("SNAPSHOTDATA"), v)
	}
}

func TestReconcile_RetentionKeepsJustUploadedSnapshot(t *testing.T) {
	// End-to-end: with RetainCount=1, after a successful snapshot the only
	// surviving object must be the one this reconcile just uploaded — never an
	// older pre-existing snapshot, and the new one must not be pruned.
	cluster, sts := readyCluster()
	backup := s3Backup("etcd-a")
	backup.Spec.Retention = &ecv1alpha1.RetentionPolicy{RetainCount: 1}
	store := newFakeStore("etcd/backups")

	// Seed two older snapshots (clock advances per upload, so these predate the
	// reconcile's upload).
	for _, k := range []string{"etcd-a/backup-1-20200101T000000Z.db", "etcd-a/backup-1-20210101T000000Z.db"} {
		_, err := store.Upload(context.Background(), k, readerOf("old"), -1)
		require.NoError(t, err)
	}

	snap := &fakeSnapshotter{data: []byte("FRESH")}
	r := newReconciler(t, store, snap, cluster, sts, backup)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "backup-1", Namespace: "ns1"},
	})
	require.NoError(t, err)

	require.Len(t, store.objects, 1, "RetainCount=1 keeps exactly one snapshot")
	for k, v := range store.objects {
		assert.Equal(t, []byte("FRESH"), v, "the surviving snapshot must be the just-uploaded one")
		assert.NotContains(t, k, "20200101")
		assert.NotContains(t, k, "20210101")
	}
}

func TestReconcile_MissingClusterFails(t *testing.T) {
	backup := s3Backup("absent")
	store := newFakeStore("")
	snap := &fakeSnapshotter{data: []byte("x")}
	r := newReconciler(t, store, snap, backup)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "backup-1", Namespace: "ns1"},
	})
	require.Error(t, err)

	var got ecv1alpha1.EtcdBackup
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "backup-1", Namespace: "ns1"}, &got))
	assert.Equal(t, ecv1alpha1.BackupPhaseFailed, got.Status.Phase)
	assert.Equal(t, 0, snap.calls, "snapshot must not run when cluster is missing")
}

func TestReconcile_NoReadyMembersFails(t *testing.T) {
	cluster, sts := readyCluster()
	sts.Status.ReadyReplicas = 0
	backup := s3Backup("etcd-a")
	store := newFakeStore("")
	snap := &fakeSnapshotter{data: []byte("x")}
	r := newReconciler(t, store, snap, cluster, sts, backup)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "backup-1", Namespace: "ns1"},
	})
	require.Error(t, err)
	assert.Equal(t, 0, snap.calls)
}

func TestReconcile_SnapshotErrorMarksFailed(t *testing.T) {
	cluster, sts := readyCluster()
	backup := s3Backup("etcd-a")
	store := newFakeStore("etcd/backups")
	snap := &fakeSnapshotter{err: fmt.Errorf("etcdctl boom")}
	r := newReconciler(t, store, snap, cluster, sts, backup)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "backup-1", Namespace: "ns1"},
	})
	require.Error(t, err)

	var got ecv1alpha1.EtcdBackup
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "backup-1", Namespace: "ns1"}, &got))
	assert.Equal(t, ecv1alpha1.BackupPhaseFailed, got.Status.Phase)
	cond := meta.FindStatusCondition(got.Status.Conditions, ecv1alpha1.BackupConditionSucceeded)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Empty(t, store.objects, "nothing should be uploaded on snapshot failure")
}

func TestReconcile_UploadErrorMarksFailed(t *testing.T) {
	cluster, sts := readyCluster()
	backup := s3Backup("etcd-a")
	store := newFakeStore("etcd/backups")
	store.uploadErr = fmt.Errorf("s3 unavailable")
	snap := &fakeSnapshotter{data: []byte("data")}
	r := newReconciler(t, store, snap, cluster, sts, backup)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "backup-1", Namespace: "ns1"},
	})
	require.Error(t, err)

	var got ecv1alpha1.EtcdBackup
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "backup-1", Namespace: "ns1"}, &got))
	assert.Equal(t, ecv1alpha1.BackupPhaseFailed, got.Status.Phase)
}

func TestReconcile_TerminalIsNoOp(t *testing.T) {
	cluster, sts := readyCluster()
	backup := s3Backup("etcd-a")
	backup.Status.Phase = ecv1alpha1.BackupPhaseCompleted
	store := newFakeStore("")
	snap := &fakeSnapshotter{data: []byte("x")}
	r := newReconciler(t, store, snap, cluster, sts, backup)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "backup-1", Namespace: "ns1"},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, snap.calls, "completed backup must not re-run")
}

func TestReconcile_SnapshotTimeout(t *testing.T) {
	cluster, sts := readyCluster()
	backup := s3Backup("etcd-a")
	backup.Spec.SnapshotTimeout = &metav1.Duration{Duration: 20 * time.Millisecond}
	store := newFakeStore("etcd/backups")
	snap := &fakeSnapshotter{data: []byte("data"), delay: 500 * time.Millisecond}
	r := newReconciler(t, store, snap, cluster, sts, backup)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "backup-1", Namespace: "ns1"},
	})
	require.Error(t, err)
	var got ecv1alpha1.EtcdBackup
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "backup-1", Namespace: "ns1"}, &got))
	assert.Equal(t, ecv1alpha1.BackupPhaseFailed, got.Status.Phase)
}

func TestResolveCredentials_S3FromSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "ns1"},
		Data: map[string][]byte{
			"accessKeyID":     []byte("AKID"),
			"secretAccessKey": []byte("SECRET"),
			"sessionToken":    []byte("TOKEN"),
		},
	}
	r := newReconciler(t, newFakeStore(""), &fakeSnapshotter{}, secret)

	creds, err := r.resolveCredentials(context.Background(), "ns1", ecv1alpha1.BackupDestination{
		Provider:  ecv1alpha1.BackupProviderS3,
		SecretRef: &corev1.LocalObjectReference{Name: "creds"},
	})
	require.NoError(t, err)
	assert.Equal(t, "AKID", creds.AccessKeyID)
	assert.Equal(t, "SECRET", creds.SecretAccessKey)
	assert.Equal(t, "TOKEN", creds.SessionToken)
}

func TestResolveCredentials_GCSFromSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "gcskey", Namespace: "ns1"},
		Data:       map[string][]byte{"serviceAccountJSON": []byte(`{"type":"service_account"}`)},
	}
	r := newReconciler(t, newFakeStore(""), &fakeSnapshotter{}, secret)

	creds, err := r.resolveCredentials(context.Background(), "ns1", ecv1alpha1.BackupDestination{
		Provider:  ecv1alpha1.BackupProviderGCS,
		SecretRef: &corev1.LocalObjectReference{Name: "gcskey"},
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"service_account"}`, string(creds.ServiceAccountJSON))
}

func TestResolveCredentials_NoSecretIsAmbient(t *testing.T) {
	r := newReconciler(t, newFakeStore(""), &fakeSnapshotter{})
	creds, err := r.resolveCredentials(context.Background(), "ns1", ecv1alpha1.BackupDestination{
		Provider: ecv1alpha1.BackupProviderS3,
	})
	require.NoError(t, err)
	assert.Empty(t, creds.AccessKeyID)
}

func TestResolveCredentials_MissingSecretErrors(t *testing.T) {
	r := newReconciler(t, newFakeStore(""), &fakeSnapshotter{})
	_, err := r.resolveCredentials(context.Background(), "ns1", ecv1alpha1.BackupDestination{
		Provider:  ecv1alpha1.BackupProviderS3,
		SecretRef: &corev1.LocalObjectReference{Name: "nope"},
	})
	assert.Error(t, err)
}

func TestApplyRetention_PrunesOldest(t *testing.T) {
	backup := s3Backup("etcd-a")
	backup.Spec.Retention = &ecv1alpha1.RetentionPolicy{RetainCount: 2}
	store := newFakeStore("etcd/backups")

	// Seed three existing snapshots under the cluster prefix with increasing
	// modtimes (store advances its clock per upload).
	for _, k := range []string{"etcd-a/old1.db", "etcd-a/old2.db", "etcd-a/old3.db"} {
		_, err := store.Upload(context.Background(), k, readerOf("x"), -1)
		require.NoError(t, err)
	}

	r := &EtcdBackupReconciler{}
	require.NoError(t, r.applyRetention(context.Background(), backup, store))

	// RetainCount=2 keeps the two newest; exactly one (the oldest) deleted.
	require.Len(t, store.deleted, 1)
	assert.Equal(t, "etcd/backups/etcd-a/old1.db", store.deleted[0])
	assert.Len(t, store.objects, 2)
}

func TestApplyRetention_DisabledByDefault(t *testing.T) {
	backup := s3Backup("etcd-a")
	store := newFakeStore("etcd/backups")
	for _, k := range []string{"etcd-a/a.db", "etcd-a/b.db"} {
		_, err := store.Upload(context.Background(), k, readerOf("x"), -1)
		require.NoError(t, err)
	}
	r := &EtcdBackupReconciler{}
	require.NoError(t, r.applyRetention(context.Background(), backup, store))
	assert.Empty(t, store.deleted)
}

func TestToObjectStoreDestination(t *testing.T) {
	t.Run("s3 ok", func(t *testing.T) {
		dst, err := toObjectStoreDestination(ecv1alpha1.BackupDestination{
			Provider: ecv1alpha1.BackupProviderS3,
			Prefix:   "p",
			S3:       &ecv1alpha1.S3DestinationSpec{Bucket: "b", Region: "r", Endpoint: "http://e", ForcePathStyle: true},
		})
		require.NoError(t, err)
		assert.Equal(t, objectstore.ProviderS3, dst.Provider)
		assert.Equal(t, "b", dst.Bucket)
		assert.Equal(t, "r", dst.Region)
		assert.True(t, dst.ForcePathStyle)
	})
	t.Run("s3 missing block", func(t *testing.T) {
		_, err := toObjectStoreDestination(ecv1alpha1.BackupDestination{Provider: ecv1alpha1.BackupProviderS3})
		assert.Error(t, err)
	})
	t.Run("gcs ok", func(t *testing.T) {
		dst, err := toObjectStoreDestination(ecv1alpha1.BackupDestination{
			Provider: ecv1alpha1.BackupProviderGCS,
			GCS:      &ecv1alpha1.GCSDestinationSpec{Bucket: "gb"},
		})
		require.NoError(t, err)
		assert.Equal(t, objectstore.ProviderGCS, dst.Provider)
		assert.Equal(t, "gb", dst.Bucket)
	})
	t.Run("gcs missing block", func(t *testing.T) {
		_, err := toObjectStoreDestination(ecv1alpha1.BackupDestination{Provider: ecv1alpha1.BackupProviderGCS})
		assert.Error(t, err)
	})
	t.Run("unknown provider", func(t *testing.T) {
		_, err := toObjectStoreDestination(ecv1alpha1.BackupDestination{Provider: "azure"})
		assert.Error(t, err)
	})
}

func TestRelativeKey(t *testing.T) {
	assert.Equal(t, "etcd-a/x.db", relativeKey("etcd/backups", "etcd/backups/etcd-a/x.db"))
	assert.Equal(t, "etcd-a/x.db", relativeKey("/etcd/backups/", "etcd/backups/etcd-a/x.db"))
	assert.Equal(t, "etcd-a/x.db", relativeKey("", "etcd-a/x.db"))
	// key not under prefix returns as-is (trimmed)
	assert.Equal(t, "other/x.db", relativeKey("etcd", "other/x.db"))
}

// readerOf is a tiny helper returning an io.Reader over a string.
func readerOf(s string) io.Reader { return &stringReader{s: s} }

type stringReader struct {
	s string
	i int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}
