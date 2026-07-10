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
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// capturingLogger records every formatted log line so a test can assert that
// secret material never appears in any of them.
type capturingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (c *capturingLogger) sink() logr.Logger {
	return funcr.New(func(prefix, args string) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.lines = append(c.lines, prefix+" "+args)
	}, funcr.Options{})
}

func (c *capturingLogger) all() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

// These are the secret values that must NEVER be logged.
const (
	secretAK   = "AKIAEXAMPLESENSITIVE"
	secretSK   = "sUperSecret/Key+Value=="
	secretTok  = "FwoGZXIvSESSIONTOKEN"
	secretJSON = `{"type":"service_account","private_key":"-----BEGIN PRIVATE KEY-----TOPSECRET"}`
)

func credSecret(name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Data:       data,
	}
}

// TestResolveStoreCredentials_NeverLogsSecretValues is the core security
// guarantee: regardless of provider, the resolver may log the secret *name* but
// never any credential value.
func TestResolveStoreCredentials_NeverLogsSecretValues(t *testing.T) {
	s := backupScheme(t)

	t.Run("s3", func(t *testing.T) {
		secret := credSecret("s3creds", map[string][]byte{
			"accessKeyID":     []byte(secretAK),
			"secretAccessKey": []byte(secretSK),
			"sessionToken":    []byte(secretTok),
		})
		cl := fake.NewClientBuilder().WithScheme(s).WithObjects(secret).Build()
		cap := &capturingLogger{}

		creds, err := resolveStoreCredentials(context.Background(), cl, cap.sink(), testNS,
			ecv1alpha1.BackupDestination{
				Provider:  ecv1alpha1.BackupProviderS3,
				SecretRef: &corev1.LocalObjectReference{Name: "s3creds"},
			})
		require.NoError(t, err)
		// Sanity: the values were actually read (so the test is meaningful).
		require.Equal(t, secretAK, creds.AccessKeyID)
		require.Equal(t, secretSK, creds.SecretAccessKey)

		logged := cap.all()
		assert.Contains(t, logged, "s3creds", "the secret name is allowed (and expected) in audit logs")
		assert.NotContains(t, logged, secretAK, "access key ID must never be logged")
		assert.NotContains(t, logged, secretSK, "secret access key must never be logged")
		assert.NotContains(t, logged, secretTok, "session token must never be logged")
	})

	t.Run("gcs", func(t *testing.T) {
		secret := credSecret("gcskey", map[string][]byte{"serviceAccountJSON": []byte(secretJSON)})
		cl := fake.NewClientBuilder().WithScheme(s).WithObjects(secret).Build()
		cap := &capturingLogger{}

		creds, err := resolveStoreCredentials(context.Background(), cl, cap.sink(), testNS,
			ecv1alpha1.BackupDestination{
				Provider:  ecv1alpha1.BackupProviderGCS,
				SecretRef: &corev1.LocalObjectReference{Name: "gcskey"},
			})
		require.NoError(t, err)
		require.Equal(t, secretJSON, string(creds.ServiceAccountJSON))

		logged := cap.all()
		assert.Contains(t, logged, "gcskey")
		assert.NotContains(t, logged, "TOPSECRET", "service-account key material must never be logged")
		assert.NotContains(t, logged, "private_key")
	})
}

// TestResolveStoreCredentials_Validation covers the presence/format checks the
// resolver performs so misconfiguration surfaces before any network I/O.
func TestResolveStoreCredentials_Validation(t *testing.T) {
	s := backupScheme(t)

	t.Run("ambient when no secretRef", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(s).Build()
		creds, err := resolveStoreCredentials(context.Background(), cl, logr.Discard(), testNS,
			ecv1alpha1.BackupDestination{Provider: ecv1alpha1.BackupProviderS3})
		require.NoError(t, err)
		assert.Empty(t, creds.AccessKeyID)
	})

	t.Run("missing secret errors without leaking", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(s).Build()
		_, err := resolveStoreCredentials(context.Background(), cl, logr.Discard(), testNS,
			ecv1alpha1.BackupDestination{
				Provider:  ecv1alpha1.BackupProviderS3,
				SecretRef: &corev1.LocalObjectReference{Name: "absent"},
			})
		require.Error(t, err)
	})

	t.Run("s3 secret missing required keys errors", func(t *testing.T) {
		secret := credSecret("partial", map[string][]byte{"accessKeyID": []byte("only-ak")})
		cl := fake.NewClientBuilder().WithScheme(s).WithObjects(secret).Build()
		_, err := resolveStoreCredentials(context.Background(), cl, logr.Discard(), testNS,
			ecv1alpha1.BackupDestination{
				Provider:  ecv1alpha1.BackupProviderS3,
				SecretRef: &corev1.LocalObjectReference{Name: "partial"},
			})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "only-ak", "error must not echo secret values")
	})

	t.Run("gcs secret missing key errors", func(t *testing.T) {
		secret := credSecret("emptygcs", map[string][]byte{})
		cl := fake.NewClientBuilder().WithScheme(s).WithObjects(secret).Build()
		_, err := resolveStoreCredentials(context.Background(), cl, logr.Discard(), testNS,
			ecv1alpha1.BackupDestination{
				Provider:  ecv1alpha1.BackupProviderGCS,
				SecretRef: &corev1.LocalObjectReference{Name: "emptygcs"},
			})
		require.Error(t, err)
	})
}

func TestValidateDestination(t *testing.T) {
	cases := []struct {
		name    string
		dst     ecv1alpha1.BackupDestination
		wantErr bool
	}{
		{"s3 ok", ecv1alpha1.BackupDestination{
			Provider: ecv1alpha1.BackupProviderS3,
			S3:       &ecv1alpha1.S3DestinationSpec{Bucket: "b"},
		}, false},
		{"s3 missing block", ecv1alpha1.BackupDestination{Provider: ecv1alpha1.BackupProviderS3}, true},
		{"gcs ok", ecv1alpha1.BackupDestination{
			Provider: ecv1alpha1.BackupProviderGCS,
			GCS:      &ecv1alpha1.GCSDestinationSpec{Bucket: "b"},
		}, false},
		{"empty secretRef name", ecv1alpha1.BackupDestination{
			Provider:  ecv1alpha1.BackupProviderS3,
			S3:        &ecv1alpha1.S3DestinationSpec{Bucket: "b"},
			SecretRef: &corev1.LocalObjectReference{Name: ""},
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateDestination(c.dst)
			if c.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
