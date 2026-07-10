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
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func init() {
	Register(ProviderS3, newS3Store)
}

// s3API is the subset of the S3 client the store uses. Narrowing the surface
// keeps the store testable in isolation (see s3_test.go) without spinning up
// the real client.
type s3API interface {
	PutObject(
		ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options),
	) (*s3.PutObjectOutput, error)
	GetObject(
		ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
	ListObjectsV2(
		ctx context.Context, in *s3.ListObjectsV2Input, optFns ...func(*s3.Options),
	) (*s3.ListObjectsV2Output, error)
	DeleteObject(
		ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options),
	) (*s3.DeleteObjectOutput, error)
}

// s3Store implements Store against AWS S3 (and S3-compatible endpoints).
type s3Store struct {
	client s3API
	bucket string
	prefix string
}

// newS3Store builds an s3Store from a Destination and Credentials. When
// explicit credentials are absent it relies on the default AWS credential
// chain (env, shared config, IRSA, instance profile), which is the
// recommended production posture.
func newS3Store(ctx context.Context, dst Destination, creds Credentials) (Store, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if dst.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(dst.Region))
	}
	if creds.AccessKeyID != "" && creds.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("objectstore/s3: load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if dst.Endpoint != "" {
			o.BaseEndpoint = aws.String(dst.Endpoint)
		}
		o.UsePathStyle = dst.ForcePathStyle
	})

	return &s3Store{client: client, bucket: dst.Bucket, prefix: dst.Prefix}, nil
}

func (s *s3Store) Scheme() string { return "s3" }

// Upload writes r to the destination. S3 PutObject requires a seekable body
// with a known length; an etcd snapshot stream has neither, so it is spooled
// to an ephemeral temp file first (snapshots are bounded and the operator pod
// has scratch space). The temp file is removed before returning.
func (s *s3Store) Upload(ctx context.Context, key string, r io.Reader, _ int64) (UploadResult, error) {
	fullKey := JoinKey(s.prefix, key)

	tmp, err := os.CreateTemp("", "etcd-snapshot-*.db")
	if err != nil {
		return UploadResult{}, fmt.Errorf("objectstore/s3: create temp file: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	size, err := io.Copy(tmp, r)
	if err != nil {
		return UploadResult{}, fmt.Errorf("objectstore/s3: buffer snapshot: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return UploadResult{}, fmt.Errorf("objectstore/s3: rewind snapshot: %w", err)
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(fullKey),
		Body:          tmp,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return UploadResult{}, fmt.Errorf("objectstore/s3: put %s: %w", fullKey, err)
	}

	return UploadResult{
		URI:  fmt.Sprintf("s3://%s/%s", s.bucket, fullKey),
		Size: size,
	}, nil
}

// Download opens the object for streaming reads. Unlike Upload (which must spool
// to a temp file because PutObject needs a known length), GetObject returns a
// streaming body, so the snapshot is never staged on the operator's disk on the
// read path. A NoSuchKey response is mapped to ErrNotFound so the restore
// controller can report a terminal "snapshot not found" rather than retrying.
func (s *s3Store) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	fullKey := JoinKey(s.prefix, key)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("objectstore/s3: get %s: %w", fullKey, ErrNotFound)
		}
		return nil, fmt.Errorf("objectstore/s3: get %s: %w", fullKey, err)
	}
	return out.Body, nil
}

func (s *s3Store) List(ctx context.Context, keyPrefix string) ([]ObjectInfo, error) {
	// Append a trailing slash so the prefix matches a directory boundary rather
	// than a raw string prefix. Without it, listing "etcd/backups/etcd-a" also
	// returns (and retention would delete) objects under "etcd/backups/etcd-a-2".
	// JoinKey trims trailing slashes, so the boundary is added here after the
	// join. An empty prefix lists the whole bucket and must stay empty.
	fullPrefix := JoinKey(s.prefix, keyPrefix)
	if fullPrefix != "" {
		fullPrefix += "/"
	}
	return s.listExact(ctx, fullPrefix)
}

func (s *s3Store) listExact(ctx context.Context, fullPrefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	var token *string
	for {
		resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("objectstore/s3: list %s: %w", fullPrefix, err)
		}
		for _, o := range resp.Contents {
			info := ObjectInfo{Key: aws.ToString(o.Key)}
			if o.Size != nil {
				info.Size = *o.Size
			}
			if o.LastModified != nil {
				info.LastModified = *o.LastModified
			}
			out = append(out, info)
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		token = resp.NextContinuationToken
	}

	// Most-recent first so callers can apply retention trivially. S3
	// LastModified has one-second granularity, so a stable sort with a
	// deterministic tiebreaker on the key (which embeds a sortable UTC
	// timestamp) is required; otherwise retention could non-deterministically
	// delete the just-uploaded snapshot when its modtime ties an older one.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].LastModified.Equal(out[j].LastModified) {
			return out[i].Key > out[j].Key
		}
		return out[i].LastModified.After(out[j].LastModified)
	})
	return out, nil
}

func (s *s3Store) Delete(ctx context.Context, key string) error {
	fullKey := JoinKey(s.prefix, key)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return fmt.Errorf("objectstore/s3: delete %s: %w", fullKey, err)
	}
	return nil
}
