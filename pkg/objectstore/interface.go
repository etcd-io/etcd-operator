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

// Package objectstore defines a pluggable abstraction over object-storage
// backends (S3, GCS, ...) used to upload etcd snapshots.
//
// The package is deliberately decoupled from the Kubernetes API types and the
// controller: a Provider receives plain credentials and a Destination, and
// exposes a minimal Upload/List/Delete surface. This keeps the heavy cloud
// SDKs (aws-sdk-go-v2, cloud.google.com/go/storage) behind a single seam so
// that:
//
//   - the rest of the operator can be unit-tested against a fake Provider
//     without any cloud credentials, and
//   - the implementation could be lifted into a standalone backup-manager
//     binary later without touching its callers.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrNotFound is returned by Download when the requested object does not exist.
// Callers (e.g. the restore controller) use errors.Is to distinguish a missing
// snapshot from a transport error so they can report an actionable, terminal
// failure rather than retrying forever.
var ErrNotFound = errors.New("objectstore: object not found")

// Provider is the name of an object-storage backend. It mirrors the
// v1alpha1.BackupProvider enum but is redeclared here so the package carries
// no dependency on the API types.
type Provider string

const (
	// ProviderS3 is the AWS S3 (and S3-compatible) backend.
	ProviderS3 Provider = "s3"
	// ProviderGCS is the Google Cloud Storage backend.
	ProviderGCS Provider = "gcs"
)

// Credentials carries the secret material a provider needs to authenticate.
// All fields are optional; a provider falls back to its ambient credential
// chain (IRSA, Workload Identity, instance metadata, ...) when the relevant
// fields are empty. This lets operators run credential-free in production
// while still supporting explicit secrets for portability and tests.
type Credentials struct {
	// S3 credentials.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// GCS credentials: the raw JSON of a GCP service-account key.
	ServiceAccountJSON []byte
}

// Destination fully describes where an object should be written. It is the
// flattened, provider-agnostic union of the per-provider destination specs in
// the API. The factory validates that the fields relevant to Provider are set.
type Destination struct {
	Provider Provider
	Bucket   string
	// Prefix is an optional key prefix; it is joined with the object key.
	Prefix string

	// S3-only knobs.
	Region         string
	Endpoint       string
	ForcePathStyle bool
}

// ObjectInfo describes a stored object, returned by List.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// UploadResult reports the outcome of a successful upload.
type UploadResult struct {
	// URI is the canonical address of the uploaded object, e.g.
	// "s3://bucket/key" or "gs://bucket/key".
	URI string
	// Size is the number of bytes written.
	Size int64
}

// Store is the minimal object-storage surface the backup controller needs.
// Implementations must be safe for concurrent use by multiple goroutines.
type Store interface {
	// Upload streams r to key under the configured bucket/prefix and returns
	// the canonical URI and byte count. size may be -1 if unknown; providers
	// that require a known length will buffer as needed.
	Upload(ctx context.Context, key string, r io.Reader, size int64) (UploadResult, error)

	// Download opens the object at key (joined with the destination Prefix) for
	// reading. The caller owns the returned ReadCloser and must Close it. It is
	// the read-side counterpart of Upload, used by the restore path to stream a
	// snapshot back out of object storage. Implementations stream the body
	// rather than buffering it, so an arbitrarily large snapshot stays bounded
	// to the consumer's read buffer. A missing object surfaces as ErrNotFound.
	Download(ctx context.Context, key string) (io.ReadCloser, error)

	// List returns objects under the configured bucket/prefix whose key begins
	// with keyPrefix (joined with the destination Prefix), most-recent first.
	List(ctx context.Context, keyPrefix string) ([]ObjectInfo, error)

	// Delete removes the object at key (joined with the destination Prefix).
	Delete(ctx context.Context, key string) error

	// Scheme returns the URI scheme for this store ("s3" or "gs").
	Scheme() string
}

// Factory builds a Store for a given Destination and Credentials. It is the
// single dispatch point on Destination.Provider; new backends are added by
// extending this switch and dropping a new implementation file in the package.
type Factory func(ctx context.Context, dst Destination, creds Credentials) (Store, error)

// registry holds the provider constructors. It is package-private and
// populated by each provider's init via Register; New dispatches through it so
// that tests can register fakes without importing cloud SDKs.
var registry = map[Provider]Factory{}

// Register associates a Factory with a Provider. It is intended to be called
// from provider init functions (see s3.go, gcs.go). Calling it twice for the
// same provider panics, surfacing accidental duplicate registration at startup.
func Register(p Provider, f Factory) {
	if _, exists := registry[p]; exists {
		panic(fmt.Sprintf("objectstore: provider %q already registered", p))
	}
	registry[p] = f
}

// New constructs a Store for dst.Provider, validating that the destination is
// internally consistent before dispatching to the registered factory.
func New(ctx context.Context, dst Destination, creds Credentials) (Store, error) {
	if err := dst.validate(); err != nil {
		return nil, err
	}
	f, ok := registry[dst.Provider]
	if !ok {
		return nil, fmt.Errorf("objectstore: no provider registered for %q", dst.Provider)
	}
	return f(ctx, dst, creds)
}

// isRegistered reports whether a provider has a factory registered.
func isRegistered(p Provider) bool {
	_, ok := registry[p]
	return ok
}

// SupportedProviders returns the set of registered providers, sorted is not
// guaranteed; primarily useful for diagnostics and tests.
func SupportedProviders() []Provider {
	out := make([]Provider, 0, len(registry))
	for p := range registry {
		out = append(out, p)
	}
	return out
}

// validate performs provider-agnostic and provider-specific sanity checks on a
// Destination. It is intentionally strict so that misconfiguration surfaces
// before any network I/O.
func (d Destination) validate() error {
	if d.Bucket == "" {
		return fmt.Errorf("objectstore: destination bucket must not be empty")
	}
	switch d.Provider {
	case ProviderS3:
		// Region/Endpoint are optional (endpoint covers S3-compatible stores).
	case ProviderGCS:
		if d.Endpoint != "" || d.ForcePathStyle {
			return fmt.Errorf("objectstore: s3-only fields set for gcs destination")
		}
	case "":
		return fmt.Errorf("objectstore: destination provider must not be empty")
	default:
		// Unknown to the built-in switch: accept it only if a provider was
		// registered for it (the package is intentionally pluggable), reject
		// otherwise.
		if !isRegistered(d.Provider) {
			return fmt.Errorf("objectstore: unsupported provider %q", d.Provider)
		}
	}
	return nil
}

// JoinKey joins an optional prefix and a key into a normalized object key.
// Exported so providers and callers share identical key construction.
func JoinKey(prefix, key string) string {
	prefix = trimSlashes(prefix)
	key = trimSlashes(key)
	switch {
	case prefix == "":
		return key
	case key == "":
		return prefix
	default:
		return prefix + "/" + key
	}
}

func trimSlashes(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
