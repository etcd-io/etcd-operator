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
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// Sentinel byte sequences that must never leak into logs or error strings.
const (
	sentinelCert     = "SENTINEL-CERT-BYTES-b64f00"
	sentinelKey      = "SENTINEL-KEY-BYTES-deadbeef"
	sentinelCA       = "SENTINEL-CA-BYTES-cafe"
	sentinelUsername = "SENTINEL-USERNAME-root"
	sentinelPassword = "SENTINEL-PASSWORD-hunter2"
)

func TestResolveSideCreds_NeverLogsValues(t *testing.T) {
	tlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "side-tls", Namespace: "default"},
		Data: map[string][]byte{
			"tls.crt": []byte(sentinelCert),
			"tls.key": []byte(sentinelKey),
			"ca.crt":  []byte(sentinelCA),
		},
	}
	authSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "side-auth", Namespace: "default"},
		Data: map[string][]byte{
			"username": []byte(sentinelUsername),
			"password": []byte(sentinelPassword),
		},
	}
	c := fake.NewClientBuilder().WithObjects(tlsSecret, authSecret).Build()

	var logLines []string
	logger := funcr.New(func(prefix, args string) {
		logLines = append(logLines, prefix+" "+args)
	}, funcr.Options{Verbosity: 10})

	ep := ecv1alpha1.EtcdMirrorEndpoint{
		TLS:  &ecv1alpha1.EtcdMirrorTLS{SecretRef: &corev1.LocalObjectReference{Name: "side-tls"}},
		Auth: &ecv1alpha1.EtcdMirrorAuth{SecretRef: corev1.LocalObjectReference{Name: "side-auth"}},
	}

	layout, err := resolveSideCreds(t.Context(), c, logger, "default", "source", ep)
	require.NoError(t, err)
	assert.True(t, layout.HasClientCert)
	assert.True(t, layout.HasCA)
	assert.True(t, layout.HasAuth)
	assert.NotEmpty(t, layout.AuthSecretRV)

	sentinels := []string{sentinelCert, sentinelKey, sentinelCA, sentinelUsername, sentinelPassword}
	for _, line := range logLines {
		for _, s := range sentinels {
			assert.NotContains(t, line, s, "secret value leaked into a log line")
		}
	}

	t.Run("missing key errors name objects and keys, never data", func(t *testing.T) {
		lopsided := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "lopsided-tls", Namespace: "default"},
			Data:       map[string][]byte{"tls.crt": []byte(sentinelCert)},
		}
		c := fake.NewClientBuilder().WithObjects(lopsided).Build()
		ep := ecv1alpha1.EtcdMirrorEndpoint{
			TLS: &ecv1alpha1.EtcdMirrorTLS{SecretRef: &corev1.LocalObjectReference{Name: "lopsided-tls"}},
		}
		_, err := resolveSideCreds(t.Context(), c, logger, "default", "source", ep)
		require.Error(t, err)
		var ce *credsError
		require.ErrorAs(t, err, &ce)
		assert.Equal(t, "InvalidTLSSecret", ce.Reason)
		assert.NotContains(t, err.Error(), sentinelCert)
		assert.Contains(t, err.Error(), "lopsided-tls")
	})

	t.Run("empty auth values are rejected without leaking", func(t *testing.T) {
		emptyAuth := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "empty-auth", Namespace: "default"},
			Data:       map[string][]byte{"username": []byte(sentinelUsername)},
		}
		c := fake.NewClientBuilder().WithObjects(emptyAuth).Build()
		ep := ecv1alpha1.EtcdMirrorEndpoint{
			Auth: &ecv1alpha1.EtcdMirrorAuth{SecretRef: corev1.LocalObjectReference{Name: "empty-auth"}},
		}
		_, err := resolveSideCreds(t.Context(), c, logger, "default", "target", ep)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), sentinelUsername)
	})

	t.Run("missing secret is SecretNotFound", func(t *testing.T) {
		c := fake.NewClientBuilder().Build()
		ep := ecv1alpha1.EtcdMirrorEndpoint{
			TLS: &ecv1alpha1.EtcdMirrorTLS{SecretRef: &corev1.LocalObjectReference{Name: "absent"}},
		}
		_, err := resolveSideCreds(t.Context(), c, logger, "default", "source", ep)
		var ce *credsError
		require.ErrorAs(t, err, &ce)
		assert.Equal(t, "SecretNotFound", ce.Reason)
	})
}

func TestResolveSideCredsFinalizerTarget(t *testing.T) {
	authSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tgt-auth", Namespace: "default"},
		Data: map[string][]byte{
			"username": []byte(sentinelUsername + "\n"),
			"password": []byte(sentinelPassword),
		},
	}
	c := fake.NewClientBuilder().WithObjects(authSecret).Build()
	logger := funcr.New(func(string, string) {}, funcr.Options{})

	em := minimalMirror()
	em.Spec.Target.Prefix = "/mirrored/"
	em.Spec.Target.Auth = &ecv1alpha1.EtcdMirrorAuth{SecretRef: corev1.LocalObjectReference{Name: "tgt-auth"}}

	tgt, err := resolveFinalizerTarget(t.Context(), c, logger, em)
	require.NoError(t, err)
	assert.Equal(t, []string{"tgt:2379"}, tgt.Endpoints)
	assert.Nil(t, tgt.TLS)
	assert.Equal(t, sentinelUsername, tgt.Username, "trailing newline must be stripped")
	assert.Equal(t, sentinelPassword, tgt.Password)
	assert.Equal(t, "/mirrored/\x00etcdmirror-checkpoint", tgt.Key)

	t.Run("explicit checkpoint key wins", func(t *testing.T) {
		em := em.DeepCopy()
		em.Spec.Checkpoint = &ecv1alpha1.EtcdMirrorCheckpointSpec{Key: "/mirrored/\x00custom"}
		tgt, err := resolveFinalizerTarget(t.Context(), c, logger, em)
		require.NoError(t, err)
		assert.Equal(t, "/mirrored/\x00custom", tgt.Key)
	})
}

// Guard against message drift: the etcdctl del command in the
// EmptyTargetViolation message must quote both range ends.
func TestEtcdctlDelRangeQuoting(t *testing.T) {
	cmd := etcdctlDelCommand("/mirrored/")
	assert.Equal(t, 2, strings.Count(cmd, `"/`))
}
