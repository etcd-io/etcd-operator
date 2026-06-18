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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoinKey(t *testing.T) {
	cases := []struct {
		prefix, key, want string
	}{
		{"", "a.db", "a.db"},
		{"etcd", "a.db", "etcd/a.db"},
		{"etcd/", "/a.db", "etcd/a.db"},
		{"/etcd/backups/", "cluster/a.db", "etcd/backups/cluster/a.db"},
		{"etcd", "", "etcd"},
		{"", "", ""},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, JoinKey(c.prefix, c.key), "JoinKey(%q,%q)", c.prefix, c.key)
	}
}

func TestDestinationValidate(t *testing.T) {
	cases := []struct {
		name    string
		dst     Destination
		wantErr bool
	}{
		{"s3 ok", Destination{Provider: ProviderS3, Bucket: "b", Region: "us-east-1"}, false},
		{
			"s3 compatible endpoint ok",
			Destination{Provider: ProviderS3, Bucket: "b", Endpoint: "http://minio:9000", ForcePathStyle: true},
			false,
		},
		{"gcs ok", Destination{Provider: ProviderGCS, Bucket: "b"}, false},
		{"empty bucket", Destination{Provider: ProviderS3}, true},
		{"empty provider", Destination{Bucket: "b"}, true},
		{"unknown provider", Destination{Provider: "azure", Bucket: "b"}, true},
		{"gcs with s3 fields", Destination{Provider: ProviderGCS, Bucket: "b", ForcePathStyle: true}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.dst.validate()
			if c.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// capture records what the factory was handed, to assert dispatch wiring.
type capture struct {
	dst   Destination
	creds Credentials
}

func TestNew_DispatchAndValidation(t *testing.T) {
	const fakeProvider Provider = "fake-test-provider"

	var captured *capture
	Register(fakeProvider, func(_ context.Context, dst Destination, creds Credentials) (Store, error) {
		captured = &capture{dst: dst, creds: creds}
		return newMemStore("fake"), nil
	})

	t.Run("dispatches to registered factory", func(t *testing.T) {
		s, err := New(context.Background(), Destination{
			Provider: fakeProvider,
			Bucket:   "bucket",
		}, Credentials{AccessKeyID: "AK"})
		require.NoError(t, err)
		require.NotNil(t, s)
		require.NotNil(t, captured)
		assert.Equal(t, "bucket", captured.dst.Bucket)
		assert.Equal(t, "AK", captured.creds.AccessKeyID)
	})

	t.Run("validation runs before dispatch", func(t *testing.T) {
		_, err := New(context.Background(), Destination{Provider: fakeProvider}, Credentials{})
		assert.Error(t, err, "empty bucket must fail validation")
	})

	t.Run("unregistered provider errors", func(t *testing.T) {
		_, err := New(context.Background(), Destination{Provider: "nope", Bucket: "b"}, Credentials{})
		assert.Error(t, err)
	})
}

func TestRegister_DuplicatePanics(t *testing.T) {
	const p Provider = "dup-test-provider"
	Register(p, func(context.Context, Destination, Credentials) (Store, error) { return nil, nil })
	assert.Panics(t, func() {
		Register(p, func(context.Context, Destination, Credentials) (Store, error) { return nil, nil })
	})
}

func TestSupportedProviders_IncludesBuiltins(t *testing.T) {
	got := SupportedProviders()
	assert.Contains(t, got, ProviderS3)
	assert.Contains(t, got, ProviderGCS)
}
