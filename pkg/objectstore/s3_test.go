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
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeS3 implements s3API in memory so the s3Store logic (key joining, URI
// construction, list pagination, retention deletes) is testable without AWS.
type fakeS3 struct {
	objects  map[string][]byte
	modTimes map[string]time.Time
	clock    time.Time
	putCalls int
	delCalls int
	failPut  bool
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		objects:  map[string][]byte{},
		modTimes: map[string]time.Time{},
		clock:    time.Unix(1000, 0),
	}
}

func (f *fakeS3) PutObject(
	_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	f.putCalls++
	if f.failPut {
		return nil, io.ErrClosedPipe
	}
	data, _ := io.ReadAll(in.Body)
	key := aws.ToString(in.Key)
	f.objects[key] = data
	f.clock = f.clock.Add(time.Second)
	f.modTimes[key] = f.clock
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) ListObjectsV2(
	_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(in.Prefix)
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	// deterministic order
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	out := &s3.ListObjectsV2Output{}
	for _, k := range keys {
		sz := int64(len(f.objects[k]))
		mt := f.modTimes[k]
		out.Contents = append(out.Contents, s3types.Object{
			Key:          aws.String(k),
			Size:         &sz,
			LastModified: &mt,
		})
	}
	return out, nil
}

func (f *fakeS3) DeleteObject(
	_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	f.delCalls++
	delete(f.objects, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func newTestS3Store(api s3API, prefix string) *s3Store {
	return &s3Store{client: api, bucket: "test-bucket", prefix: prefix}
}

func TestS3Store_UploadJoinsPrefixAndReportsURI(t *testing.T) {
	api := newFakeS3()
	store := newTestS3Store(api, "etcd/backups")

	res, err := store.Upload(context.Background(), "cluster-a/snap.db", strings.NewReader("hello"), -1)
	require.NoError(t, err)
	assert.Equal(t, "s3://test-bucket/etcd/backups/cluster-a/snap.db", res.URI)
	assert.Equal(t, int64(5), res.Size)
	assert.Equal(t, 1, api.putCalls)
	_, ok := api.objects["etcd/backups/cluster-a/snap.db"]
	assert.True(t, ok, "object stored under joined key")
}

func TestS3Store_UploadError(t *testing.T) {
	api := newFakeS3()
	api.failPut = true
	store := newTestS3Store(api, "")
	_, err := store.Upload(context.Background(), "x.db", strings.NewReader("data"), -1)
	assert.Error(t, err)
}

func TestS3Store_ListNewestFirst(t *testing.T) {
	api := newFakeS3()
	store := newTestS3Store(api, "p")
	for _, k := range []string{"c/1.db", "c/2.db", "c/3.db"} {
		_, err := store.Upload(context.Background(), k, strings.NewReader(k), -1)
		require.NoError(t, err)
	}
	infos, err := store.List(context.Background(), "c")
	require.NoError(t, err)
	require.Len(t, infos, 3)
	// modtime increases per upload; newest (3.db) must be first.
	assert.Equal(t, "p/c/3.db", infos[0].Key)
	assert.True(t, infos[0].LastModified.After(infos[2].LastModified))
}

func TestS3Store_ListPrefixIsDirectoryBoundary(t *testing.T) {
	// Two clusters whose names share a string prefix ("etcd-a" is a prefix of
	// "etcd-a-2") live under the same destination prefix. Listing one cluster's
	// snapshots must not return the other's, or retention would delete a live
	// cluster's valid backups.
	api := newFakeS3()
	store := newTestS3Store(api, "etcd/backups")
	for _, k := range []string{"etcd-a/1.db", "etcd-a/2.db", "etcd-a-2/1.db"} {
		_, err := store.Upload(context.Background(), k, strings.NewReader(k), -1)
		require.NoError(t, err)
	}

	infos, err := store.List(context.Background(), "etcd-a")
	require.NoError(t, err)
	require.Len(t, infos, 2, "only etcd-a/* must be listed, not etcd-a-2/*")
	for _, info := range infos {
		assert.True(t, strings.HasPrefix(info.Key, "etcd/backups/etcd-a/"),
			"unexpected key crossed the directory boundary: %s", info.Key)
	}
}

func TestS3Store_ListStableOnEqualModTimes(t *testing.T) {
	// When several objects share an identical LastModified (S3's 1s
	// granularity), the order must be deterministic (key descending) so the
	// newest key is first and never falls into a retention deletion set.
	api := newFakeS3()
	store := newTestS3Store(api, "p")
	tied := time.Unix(2000, 0)
	for _, k := range []string{"c/a-20260617T000001Z.db", "c/a-20260617T000003Z.db", "c/a-20260617T000002Z.db"} {
		api.objects["p/"+k] = []byte(k)
		api.modTimes["p/"+k] = tied
	}

	infos, err := store.List(context.Background(), "c")
	require.NoError(t, err)
	require.Len(t, infos, 3)
	// Newest timestamp (…000003Z) sorts first by key on the modtime tie.
	assert.Equal(t, "p/c/a-20260617T000003Z.db", infos[0].Key)
	assert.Equal(t, "p/c/a-20260617T000001Z.db", infos[2].Key)
}

func TestS3Store_Delete(t *testing.T) {
	api := newFakeS3()
	store := newTestS3Store(api, "p")
	_, err := store.Upload(context.Background(), "c/1.db", strings.NewReader("x"), -1)
	require.NoError(t, err)
	require.NoError(t, store.Delete(context.Background(), "c/1.db"))
	assert.Equal(t, 1, api.delCalls)
	_, ok := api.objects["p/c/1.db"]
	assert.False(t, ok)
}
