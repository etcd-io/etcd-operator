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

package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeGCSBucket implements gcsBucketHandle in memory.
type fakeGCSBucket struct {
	objects  map[string][]byte
	modTimes map[string]time.Time
	clock    time.Time
	delCalls int
}

func newFakeGCSBucket() *fakeGCSBucket {
	return &fakeGCSBucket{
		objects:  map[string][]byte{},
		modTimes: map[string]time.Time{},
		clock:    time.Unix(2000, 0),
	}
}

type fakeGCSWriter struct {
	bucket *fakeGCSBucket
	key    string
	buf    bytes.Buffer
}

func (w *fakeGCSWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *fakeGCSWriter) Close() error {
	w.bucket.objects[w.key] = append([]byte(nil), w.buf.Bytes()...)
	w.bucket.clock = w.bucket.clock.Add(time.Second)
	w.bucket.modTimes[w.key] = w.bucket.clock
	return nil
}

func (b *fakeGCSBucket) NewWriter(_ context.Context, key string) io.WriteCloser {
	return &fakeGCSWriter{bucket: b, key: key}
}

func (b *fakeGCSBucket) NewReader(_ context.Context, key string) (io.ReadCloser, error) {
	data, ok := b.objects[key]
	if !ok {
		// Mirror the real client: a missing object surfaces ErrObjectNotExist.
		return nil, gcs.ErrObjectNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *fakeGCSBucket) Delete(_ context.Context, key string) error {
	b.delCalls++
	delete(b.objects, key)
	return nil
}

func (b *fakeGCSBucket) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	for k, v := range b.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, ObjectInfo{Key: k, Size: int64(len(v)), LastModified: b.modTimes[k]})
		}
	}
	return out, nil
}

func newTestGCSStore(bucket gcsBucketHandle, prefix string) *gcsStore {
	return &gcsStore{bucket: bucket, name: "test-bucket", prefix: prefix, closer: func() error { return nil }}
}

func TestGCSStore_UploadJoinsPrefixAndReportsURI(t *testing.T) {
	b := newFakeGCSBucket()
	store := newTestGCSStore(b, "etcd")

	res, err := store.Upload(context.Background(), "cluster/snap.db", strings.NewReader("payload"), -1)
	require.NoError(t, err)
	assert.Equal(t, "gs://test-bucket/etcd/cluster/snap.db", res.URI)
	assert.Equal(t, int64(7), res.Size)
	assert.Equal(t, []byte("payload"), b.objects["etcd/cluster/snap.db"])
}

func TestGCSStore_ListNewestFirst(t *testing.T) {
	b := newFakeGCSBucket()
	store := newTestGCSStore(b, "p")
	for _, k := range []string{"c/a.db", "c/b.db", "c/c.db"} {
		_, err := store.Upload(context.Background(), k, strings.NewReader(k), -1)
		require.NoError(t, err)
	}
	infos, err := store.List(context.Background(), "c")
	require.NoError(t, err)
	require.Len(t, infos, 3)
	assert.Equal(t, "p/c/c.db", infos[0].Key)
}

func TestGCSStore_Delete(t *testing.T) {
	b := newFakeGCSBucket()
	store := newTestGCSStore(b, "p")
	_, err := store.Upload(context.Background(), "c/a.db", strings.NewReader("x"), -1)
	require.NoError(t, err)
	require.NoError(t, store.Delete(context.Background(), "c/a.db"))
	assert.Equal(t, 1, b.delCalls)
	_, ok := b.objects["p/c/a.db"]
	assert.False(t, ok)
}

func TestGCSStore_DownloadJoinsPrefixAndStreamsBody(t *testing.T) {
	b := newFakeGCSBucket()
	store := newTestGCSStore(b, "etcd")
	_, err := store.Upload(context.Background(), "cluster/snap.db", strings.NewReader("payload"), -1)
	require.NoError(t, err)

	rc, err := store.Download(context.Background(), "cluster/snap.db")
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(got))
}

func TestGCSStore_DownloadMissingIsErrNotFound(t *testing.T) {
	b := newFakeGCSBucket()
	store := newTestGCSStore(b, "p")
	_, err := store.Download(context.Background(), "c/absent.db")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound), "missing object must map to ErrNotFound, got %v", err)
}

func TestGCSStore_Scheme(t *testing.T) {
	assert.Equal(t, "gs", newTestGCSStore(newFakeGCSBucket(), "").Scheme())
}
