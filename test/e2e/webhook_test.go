/*
Copyright 2025.

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

package e2e

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// newWebhookTestCluster builds an EtcdCluster with the given spec parameters.
// It is local to this file so the suite does not depend on helpers added by
// other in-flight e2e PRs.
func newWebhookTestCluster(name string, size int, version string) *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{Size: size, Version: version},
	}
}

// TestAdmissionWebhooks verifies that the validating and defaulting admission
// webhooks are registered and enforce the documented invariants at apply time.
// Each invalid apply must be rejected synchronously with an actionable message.
func TestAdmissionWebhooks(t *testing.T) {
	feature := features.New("admission-webhooks")

	feature.Assess("rejects even cluster size with quorum guidance",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			err := c.Resources().Create(ctx, newWebhookTestCluster("wh-even", 2, etcdVersion))
			if err == nil {
				t.Fatal("expected even-sized EtcdCluster to be rejected by the validating webhook")
			}
			if !strings.Contains(err.Error(), "size must be an odd number") {
				t.Fatalf("expected quorum guidance in rejection, got: %v", err)
			}
			return ctx
		})

	feature.Assess("rejects non-semver version",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			err := c.Resources().Create(ctx, newWebhookTestCluster("wh-badver", 3, "not-a-version"))
			if err == nil {
				t.Fatal("expected non-semver version to be rejected")
			}
			if !strings.Contains(err.Error(), "is not a valid semantic version") {
				t.Fatalf("expected semver guidance in rejection, got: %v", err)
			}
			return ctx
		})

	feature.Assess("rejects unknown TLS provider",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			cluster := newWebhookTestCluster("wh-badtls", 3, etcdVersion)
			cluster.Spec.TLS = &ecv1alpha1.TLSCertificate{Provider: "vault"}
			err := c.Resources().Create(ctx, cluster)
			if err == nil {
				t.Fatal("expected unknown TLS provider to be rejected")
			}
			if !strings.Contains(err.Error(), "auto") || !strings.Contains(err.Error(), "cert-manager") {
				t.Fatalf("expected supported-provider list in rejection, got: %v", err)
			}
			return ctx
		})

	feature.Assess("admits valid cluster and defaults tls.provider to auto",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			cluster := newWebhookTestCluster("wh-valid", 3, etcdVersion)
			cluster.Spec.TLS = &ecv1alpha1.TLSCertificate{} // empty -> defaulted
			if err := c.Resources().Create(ctx, cluster); err != nil {
				t.Fatalf("expected valid cluster to be admitted, got: %v", err)
			}
			defer func() { _ = c.Resources().Delete(ctx, cluster) }()

			var got ecv1alpha1.EtcdCluster
			if err := c.Resources().Get(ctx, cluster.Name, namespace, &got); err != nil {
				t.Fatalf("failed to read back cluster: %v", err)
			}
			if got.Spec.TLS == nil || got.Spec.TLS.Provider != "auto" {
				t.Fatalf("expected defaulting webhook to set tls.provider=auto, got: %+v", got.Spec.TLS)
			}
			return ctx
		})

	feature.Assess("rejects skip-minor upgrade on update",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			cluster := newWebhookTestCluster("wh-upgrade", 3, "3.5.1")
			if err := c.Resources().Create(ctx, cluster); err != nil {
				t.Fatalf("failed to create initial cluster: %v", err)
			}
			defer func() { _ = c.Resources().Delete(ctx, cluster) }()

			var got ecv1alpha1.EtcdCluster
			if err := c.Resources().Get(ctx, cluster.Name, namespace, &got); err != nil {
				t.Fatalf("failed to read back cluster: %v", err)
			}
			got.Spec.Version = "3.7.1" // skips 3.6
			if err := c.Resources().Update(ctx, &got); err == nil {
				t.Fatal("expected skip-minor upgrade to be rejected on update")
			} else if !strings.Contains(err.Error(), "skips a minor version") {
				t.Fatalf("expected skip-minor guidance in rejection, got: %v", err)
			}
			return ctx
		})

	_ = testEnv.Test(t, feature.Feature())
}
