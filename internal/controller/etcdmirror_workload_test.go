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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

func minimalMirror() *ecv1alpha1.EtcdMirror {
	return &ecv1alpha1.EtcdMirror{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "m1",
			Namespace:  "default",
			UID:        types.UID("uid-1"),
			Generation: 3,
		},
		Spec: ecv1alpha1.EtcdMirrorSpec{
			Source: ecv1alpha1.EtcdMirrorEndpoint{EndpointList: []string{"src:2379"}},
			Target: ecv1alpha1.EtcdMirrorEndpoint{EndpointList: []string{"tgt:2379"}},
		},
	}
}

func minimalInput() agentWorkloadInput {
	return agentWorkloadInput{
		image:           testAgentImage,
		sourceEndpoints: "src:2379",
		targetEndpoints: "tgt:2379",
	}
}

func TestMirrorAgentArgs(t *testing.T) {
	t.Run("minimal spec emits only always-flags", func(t *testing.T) {
		got := agentArgs(minimalMirror(), minimalInput())
		want := []string{
			"--link-uid=uid-1",
			"--epoch=3",
			"--mode=Sync",
			"--http-bind-address=:8080",
			"--source-endpoints=src:2379",
			"--target-endpoints=tgt:2379",
		}
		assert.Equal(t, want, got)
	})

	t.Run("drain mode is explicit", func(t *testing.T) {
		em := minimalMirror()
		em.Spec.Mode = ecv1alpha1.EtcdMirrorModeDrain
		assert.Contains(t, agentArgs(em, minimalInput()), "--mode=Drain")
	})

	t.Run("full spec emits the exact ordered slice", func(t *testing.T) {
		em := minimalMirror()
		em.Spec.Source.Prefix = "/registry/"
		em.Spec.Target.Prefix = "/mirrored/"
		em.Spec.Source.TLS = &ecv1alpha1.EtcdMirrorTLS{
			SecretRef:   &corev1.LocalObjectReference{Name: "src-tls"},
			CABundleRef: &ecv1alpha1.EtcdMirrorCABundleRef{Name: "trust", Key: "bundle.pem"},
			ServerName:  "etcd.example.com",
		}
		em.Spec.Source.Auth = &ecv1alpha1.EtcdMirrorAuth{
			SecretRef: corev1.LocalObjectReference{Name: "src-auth"},
		}
		em.Spec.Target.TLS = &ecv1alpha1.EtcdMirrorTLS{
			InsecureSkipVerify:                true,
			InsecureSkipVerifyAcknowledgeRisk: true,
		}
		em.Spec.Sync = ecv1alpha1.EtcdMirrorSyncSpec{
			DestPrefix:       "sub/",
			ExcludePrefixes:  []string{"/registry/events/", "/registry/leases/"},
			MaxTxnOps:        64,
			TxnFlushBytes:    resource.NewQuantity(512*1024, resource.BinarySI),
			PageKeyLimit:     256,
			PageBytes:        ptrQuantity("2Mi"),
			WatchBufferBytes: ptrQuantity("32Mi"),
			MaxOpsPerSecond:  1000,
			RequestTimeout:   &metav1.Duration{Duration: 20 * time.Second},
			DialTimeout:      &metav1.Duration{Duration: 5 * time.Second},
			ReconnectBackoff: &ecv1alpha1.EtcdMirrorBackoffSpec{
				InitialDelay: &metav1.Duration{Duration: 2 * time.Second},
				MaxDelay:     &metav1.Duration{Duration: time.Minute},
			},
		}
		em.Spec.InitialSync = &ecv1alpha1.EtcdMirrorInitialSyncSpec{
			Mode:          ecv1alpha1.EtcdMirrorInitialSyncOverwrite,
			StartRevision: 42,
		}
		em.Spec.Checkpoint = &ecv1alpha1.EtcdMirrorCheckpointSpec{Key: "/mirrored/sub/\x00cp"}
		em.Spec.Reconciliation = &ecv1alpha1.EtcdMirrorReconciliationSpec{
			Enabled:       true,
			Interval:      &metav1.Duration{Duration: 30 * time.Minute},
			DeleteOrphans: true,
		}
		in := minimalInput()
		in.sourceCreds = sideCredsLayout{HasClientCert: true, HasCA: true, HasAuth: true, AuthSecretRV: "7"}

		want := []string{
			"--link-uid=uid-1",
			"--epoch=3",
			"--mode=Sync",
			"--source-prefix=/registry/",
			"--target-prefix=/mirrored/",
			"--dest-prefix=sub/",
			"--exclude-prefix=/registry/events/",
			"--exclude-prefix=/registry/leases/",
			"--initial-sync-mode=Overwrite",
			"--start-revision=42",
			"--checkpoint-key=/mirrored/sub/\x00cp",
			"--max-txn-ops=64",
			"--txn-flush-bytes=512Ki",
			"--page-key-limit=256",
			"--page-bytes=2Mi",
			"--watch-buffer-bytes=32Mi",
			"--max-ops-per-second=1000",
			"--request-timeout=20s",
			"--dial-timeout=5s",
			"--backoff-initial-delay=2s",
			"--backoff-max-delay=1m0s",
			"--reconcile-enabled",
			"--reconcile-interval=30m0s",
			"--reconcile-delete-orphans",
			"--http-bind-address=:8080",
			"--source-endpoints=src:2379",
			"--source-tls",
			"--source-cert-file=/etc/mirror-agent/source/tls/tls.crt",
			"--source-key-file=/etc/mirror-agent/source/tls/tls.key",
			"--source-ca-file=/etc/mirror-agent/source/tls/ca.crt",
			"--source-ca-bundle-file=/etc/mirror-agent/source/ca/bundle.pem",
			"--source-server-name=etcd.example.com",
			"--source-username-file=/etc/mirror-agent/source/auth/username",
			"--source-password-file=/etc/mirror-agent/source/auth/password",
			"--target-endpoints=tgt:2379",
			"--target-tls",
			"--target-insecure-skip-verify",
		}
		assert.Equal(t, want, agentArgs(em, in))
	})

	t.Run("checkpoint key flag only when set", func(t *testing.T) {
		em := minimalMirror()
		em.Spec.Checkpoint = &ecv1alpha1.EtcdMirrorCheckpointSpec{}
		for _, a := range agentArgs(em, minimalInput()) {
			assert.NotContains(t, a, "--checkpoint-key")
		}
	})

	t.Run("start revision only when positive", func(t *testing.T) {
		em := minimalMirror()
		em.Spec.InitialSync = &ecv1alpha1.EtcdMirrorInitialSyncSpec{Mode: ecv1alpha1.EtcdMirrorInitialSyncRequireEmpty}
		for _, a := range agentArgs(em, minimalInput()) {
			assert.NotContains(t, a, "--start-revision")
		}
	})

	t.Run("deterministic across renders", func(t *testing.T) {
		em := minimalMirror()
		em.Spec.Sync.ExcludePrefixes = []string{"/b/", "/a/"}
		first := agentArgs(em, minimalInput())
		second := agentArgs(em, minimalInput())
		assert.Equal(t, first, second)
		// excludePrefixes keep spec order, not sorted order
		assert.Contains(t, first, "--exclude-prefix=/b/")
		i1, i2 := indexOf(first, "--exclude-prefix=/b/"), indexOf(first, "--exclude-prefix=/a/")
		assert.Less(t, i1, i2)
	})
}

func ptrQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

func indexOf(ss []string, want string) int {
	for i, s := range ss {
		if s == want {
			return i
		}
	}
	return -1
}

func TestMirrorDeploymentRender(t *testing.T) {
	em := minimalMirror()
	em.Spec.Source.TLS = &ecv1alpha1.EtcdMirrorTLS{SecretRef: &corev1.LocalObjectReference{Name: "src-tls"}}
	em.Spec.Source.Auth = &ecv1alpha1.EtcdMirrorAuth{SecretRef: corev1.LocalObjectReference{Name: "src-auth"}}
	em.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
	}
	em.Spec.PodTemplate = &ecv1alpha1.PodTemplate{
		Metadata: &ecv1alpha1.PodMetadata{
			Labels:      map[string]string{"team": "storage"},
			Annotations: map[string]string{"custom": "anno"},
		},
		Spec: &ecv1alpha1.PodSpec{
			NodeSelector: map[string]string{"zone": "z1"},
			Tolerations:  []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
		},
	}
	in := minimalInput()
	in.sourceCreds = sideCredsLayout{HasClientCert: false, HasCA: true, HasAuth: true, AuthSecretRV: "41"}

	dep := renderAgentDeployment(em, in, 1)

	assert.Equal(t, "m1-mirror-agent", dep.Name)
	assert.Equal(t, int32(1), *dep.Spec.Replicas)
	assert.Equal(t, "Recreate", string(dep.Spec.Strategy.Type))
	assert.Equal(t, map[string]string{"app": "m1-mirror-agent", "controller": "m1"}, dep.Spec.Selector.MatchLabels)

	pod := dep.Spec.Template
	require.Len(t, pod.Spec.Containers, 1)
	c := pod.Spec.Containers[0]
	assert.Equal(t, []string{"/mirror-agent"}, c.Command)
	assert.Equal(t, testAgentImage, c.Image)
	require.Len(t, c.Ports, 1)
	assert.Equal(t, int32(8080), c.Ports[0].ContainerPort)
	assert.Equal(t, "/readyz", c.ReadinessProbe.HTTPGet.Path)
	assert.Equal(t, "/healthz", c.LivenessProbe.HTTPGet.Path)
	assert.Equal(t, resource.MustParse("128Mi"), c.Resources.Requests[corev1.ResourceMemory])

	// The secret lacks tls.crt: no cert-file flags, but the ca-file flag stays.
	assert.NotContains(t, c.Args, "--source-cert-file=/etc/mirror-agent/source/tls/tls.crt")
	assert.Contains(t, c.Args, "--source-ca-file=/etc/mirror-agent/source/tls/ca.crt")

	require.NotNil(t, pod.Spec.AutomountServiceAccountToken)
	assert.False(t, *pod.Spec.AutomountServiceAccountToken)

	// podTemplate propagation plus reserved labels winning
	assert.Equal(t, "storage", pod.Labels["team"])
	assert.Equal(t, "m1-mirror-agent", pod.Labels["app"])
	assert.Equal(t, "anno", pod.Annotations["custom"])
	assert.Equal(t, map[string]string{"zone": "z1"}, pod.Spec.NodeSelector)
	require.Len(t, pod.Spec.Tolerations, 1)

	// auth rotation annotation (source RV, empty target RV)
	assert.Equal(t, "41/", pod.Annotations[authSecretsRVAnnotation])

	// volumes: source tls + source auth, 0400
	names := map[string]corev1.Volume{}
	for _, v := range pod.Spec.Volumes {
		names[v.Name] = v
	}
	require.Contains(t, names, "source-tls")
	require.Contains(t, names, "source-auth")
	assert.NotContains(t, names, "target-tls")
	assert.Equal(t, int32(0o400), *names["source-tls"].Secret.DefaultMode)
	assert.Equal(t, "src-tls", names["source-tls"].Secret.SecretName)

	// paused render
	pausedDep := renderAgentDeployment(em, in, 0)
	assert.Equal(t, int32(0), *pausedDep.Spec.Replicas)

	// no auth annotation when neither side uses auth
	em2 := minimalMirror()
	dep2 := renderAgentDeployment(em2, minimalInput(), 1)
	assert.NotContains(t, dep2.Spec.Template.Annotations, authSecretsRVAnnotation)
}

func TestEtcdctlDelCommandMessage(t *testing.T) {
	assert.Equal(t, `etcdctl del "/mirrored/" "/mirrored0"`, etcdctlDelCommand("/mirrored/"))
	assert.Equal(t, `etcdctl del "" --from-key`, etcdctlDelCommand(""))
}
