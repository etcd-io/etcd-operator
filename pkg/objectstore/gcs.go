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
	"errors"
	"fmt"
	"io"
	"sort"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	Register(ProviderGCS, newGCSStore)
}

// gcsBucketHandle is the subset of *storage.BucketHandle the store uses,
// narrowed so the orchestration is testable with a fake (see gcs_test.go).
type gcsBucketHandle interface {
	NewWriter(ctx context.Context, key string) io.WriteCloser
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// gcsStore implements Store against Google Cloud Storage.
type gcsStore struct {
	bucket gcsBucketHandle
	name   string
	prefix string
	closer func() error
}

// newGCSStore builds a gcsStore. With an explicit service-account JSON it uses
// those credentials; otherwise it relies on Application Default Credentials
// (Workload Identity in-cluster), the recommended production posture.
func newGCSStore(ctx context.Context, dst Destination, creds Credentials) (Store, error) {
	var opts []option.ClientOption
	if len(creds.ServiceAccountJSON) > 0 {
		// WithCredentialsJSON is the documented way to pass an explicit GCP
		// service-account key. The deprecation note concerns the general risk
		// of handling raw key material; here the key originates from a
		// user-provided Secret and explicit credentials are an intentional,
		// supported mode (the recommended path remains Workload Identity, i.e.
		// no secret at all).
		//nolint:staticcheck // SA1019: explicit-credentials mode is intentional
		opts = append(opts, option.WithCredentialsJSON(creds.ServiceAccountJSON))
	}

	client, err := gcs.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("objectstore/gcs: new client: %w", err)
	}

	return &gcsStore{
		bucket: &realGCSBucket{handle: client.Bucket(dst.Bucket)},
		name:   dst.Bucket,
		prefix: dst.Prefix,
		closer: client.Close,
	}, nil
}

func (g *gcsStore) Scheme() string { return "gs" }

func (g *gcsStore) Upload(ctx context.Context, key string, r io.Reader, _ int64) (UploadResult, error) {
	fullKey := JoinKey(g.prefix, key)

	w := g.bucket.NewWriter(ctx, fullKey)
	n, err := io.Copy(w, r)
	if err != nil {
		// Close to release resources; ignore its error in favor of the copy error.
		_ = w.Close()
		return UploadResult{}, fmt.Errorf("objectstore/gcs: upload %s: %w", fullKey, err)
	}
	if err := w.Close(); err != nil {
		return UploadResult{}, fmt.Errorf("objectstore/gcs: finalize %s: %w", fullKey, err)
	}

	return UploadResult{
		URI:  fmt.Sprintf("gs://%s/%s", g.name, fullKey),
		Size: n,
	}, nil
}

func (g *gcsStore) List(ctx context.Context, keyPrefix string) ([]ObjectInfo, error) {
	infos, err := g.bucket.List(ctx, JoinKey(g.prefix, keyPrefix))
	if err != nil {
		return nil, err
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].LastModified.After(infos[j].LastModified)
	})
	return infos, nil
}

func (g *gcsStore) Delete(ctx context.Context, key string) error {
	return g.bucket.Delete(ctx, JoinKey(g.prefix, key))
}

// realGCSBucket adapts the concrete *storage.BucketHandle to gcsBucketHandle.
type realGCSBucket struct {
	handle *gcs.BucketHandle
}

func (b *realGCSBucket) NewWriter(ctx context.Context, key string) io.WriteCloser {
	return b.handle.Object(key).NewWriter(ctx)
}

func (b *realGCSBucket) Delete(ctx context.Context, key string) error {
	if err := b.handle.Object(key).Delete(ctx); err != nil {
		return fmt.Errorf("objectstore/gcs: delete %s: %w", key, err)
	}
	return nil
}

func (b *realGCSBucket) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	it := b.handle.Objects(ctx, &gcs.Query{Prefix: prefix})
	var out []ObjectInfo
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("objectstore/gcs: list %s: %w", prefix, err)
		}
		out = append(out, ObjectInfo{
			Key:          attrs.Name,
			Size:         attrs.Size,
			LastModified: attrs.Updated,
		})
	}
	return out, nil
}
