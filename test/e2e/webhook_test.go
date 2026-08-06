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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// webhookValidVersion is a hardcoded, well-formed semantic version used by every
// webhook test that is NOT specifically exercising version validation.
//
// It deliberately does NOT use the suite-wide etcdVersion (sourced from
// `go list -m {{.Version}} go.etcd.io/etcd/api/v3`), because that value is
// v-prefixed (e.g. "v3.6.12"). semver.NewVersion("v3.6.12") errors, so the new
// validateVersionFormat webhook rejects it. Feeding etcdVersion into these cases
// would (a) make the happy-path "admits valid cluster" assertion fail outright,
// and (b) cause the size/tls/storage reject cases to pass for the WRONG reason --
// the aggregated rejection would also carry a version error, so a substring match
// could no longer isolate the invariant actually under test. A fixed valid semver
// keeps each assertion proving exactly the one thing it claims to prove.
const webhookValidVersion = "3.6.1"

// skipWebhooksLabel is the break-glass opt-out label injected as an objectSelector
// by config/default/webhook_selector_patch.yaml. An EtcdCluster carrying this label
// bypasses BOTH admission webhooks (the objectSelector matches every object that
// does NOT have the label set to "true").
const skipWebhooksLabel = "etcd.operator.etcd.io/skip-webhooks"

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

// requireWebhookRejection asserts that err is a rejection produced by the
// validating webhook (a structured 422 Invalid), that it names the expected
// spec field path, carries the expected actionable detail, and -- for isolation --
// does NOT also carry any of the forbidden substrings (errors that belong to a
// different invariant). The isolation check is what catches the "passes for the
// wrong reason" failure mode where an aggregated NewInvalid happens to contain the
// asserted substring while really failing on an unrelated field (e.g. a bad
// version riding along on a size test).
func requireWebhookRejection(t *testing.T, err error, wantField, wantDetail string, forbidden ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected the validating webhook to reject the request (field %q), but Create/Update succeeded", wantField)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected an apierrors.IsInvalid (422) rejection from the validating webhook, got %T: %v", err, err)
	}
	msg := err.Error()
	if !strings.Contains(msg, wantField) {
		t.Fatalf("expected rejection to name field %q, got: %v", wantField, err)
	}
	if !strings.Contains(msg, wantDetail) {
		t.Fatalf("expected rejection detail %q, got: %v", wantDetail, err)
	}
	for _, f := range forbidden {
		if strings.Contains(msg, f) {
			t.Fatalf("rejection leaked an unrelated invariant %q (test is not isolating %q); got: %v", f, wantField, err)
		}
	}
}

// TestAdmissionWebhooks verifies that the validating and defaulting admission
// webhooks are registered behind a real cert-manager-issued serving cert and
// enforce the documented invariants at apply time through the live API server.
// Each invalid apply must be rejected synchronously, as a structured Invalid
// (422), with an actionable, field-scoped message; each valid apply must be
// admitted (and defaulted). It additionally proves the two behaviors that only an
// e2e against a live API server can prove: the kustomize CA-injection chain (every
// rejection here is a real API-server -> webhook-pod call over TLS) and the
// break-glass objectSelector opt-out.
//
// Admission is synchronous, so the test body contains no sleeps or polling: a
// successful Create/Update return IS the admission decision.
func TestAdmissionWebhooks(t *testing.T) {
	feature := features.New("admission-webhooks")

	// --- Negative cases: each isolates exactly one invariant. -----------------

	feature.Assess("rejects even cluster size with quorum guidance",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			err := c.Resources().Create(ctx, newWebhookTestCluster("wh-even", 2, webhookValidVersion))
			requireWebhookRejection(t, err,
				"spec.size",
				"size must be an odd number",
				// isolation + remediation: must not be failing on the version, and
				// must carry the concrete quorum remediation the envtest twin asserts.
				"is not a valid semantic version")
			if !strings.Contains(err.Error(), "Use 1 or 3 instead") {
				t.Fatalf("expected concrete quorum remediation %q, got: %v", "Use 1 or 3 instead", err)
			}
			return ctx
		})

	feature.Assess("rejects non-semver version",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			err := c.Resources().Create(ctx, newWebhookTestCluster("wh-badver", 3, "not-a-version"))
			requireWebhookRejection(t, err,
				"spec.version",
				"is not a valid semantic version",
				// size is valid (3) here, so a size error would mean the wrong path fired.
				"size must be an odd number")
			return ctx
		})

	feature.Assess("rejects unknown TLS provider",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			cluster := newWebhookTestCluster("wh-badtls", 3, webhookValidVersion)
			cluster.Spec.TLS = &ecv1alpha1.TLSCertificate{Provider: "vault"}
			err := c.Resources().Create(ctx, cluster)
			requireWebhookRejection(t, err,
				"spec.tls.provider",
				// exact NotSupported rendering -- catches a garbled supported-values list
				// that a loose Contains("auto") && Contains("cert-manager") would miss.
				`supported values: "auto", "cert-manager"`,
				"size must be an odd number", "is not a valid semantic version")
			return ctx
		})

	feature.Assess("rejects undersized storageSpec volumeSizeRequest",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			cluster := newWebhookTestCluster("wh-badstorage", 3, webhookValidVersion)
			cluster.Spec.StorageSpec = &ecv1alpha1.StorageSpec{
				VolumeSizeRequest: resource.MustParse("512Ki"), // below the 1Mi floor
			}
			err := c.Resources().Create(ctx, cluster)
			requireWebhookRejection(t, err,
				"spec.storageSpec.volumeSizeRequest",
				"volumeSizeRequest must be at least 1Mi",
				"size must be an odd number", "is not a valid semantic version")
			return ctx
		})

	// --- Positive case: admitted + defaulted. ---------------------------------

	feature.Assess("admits valid cluster and defaults tls.provider to auto",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			// size 1 (still odd/valid) minimizes the reconcile load this otherwise
			// pure-admission test imposes on the kind node.
			cluster := newWebhookTestCluster("wh-valid", 1, webhookValidVersion)
			cluster.Spec.TLS = &ecv1alpha1.TLSCertificate{} // empty provider -> defaulted
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

	// --- Update path: rejects skip-minor, admits a single-minor upgrade. ------

	feature.Assess("guards version upgrades on update (rejects skip-minor, admits single-minor)",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()
			cluster := newWebhookTestCluster("wh-upgrade", 1, "3.5.1")
			if err := c.Resources().Create(ctx, cluster); err != nil {
				t.Fatalf("failed to create initial cluster: %v", err)
			}
			defer func() { _ = c.Resources().Delete(ctx, cluster) }()

			// Skip-minor (3.5 -> 3.7) must be rejected.
			var got ecv1alpha1.EtcdCluster
			if err := c.Resources().Get(ctx, cluster.Name, namespace, &got); err != nil {
				t.Fatalf("failed to read back cluster: %v", err)
			}
			got.Spec.Version = "3.7.1" // skips 3.6
			err := c.Resources().Update(ctx, &got)
			requireWebhookRejection(t, err, "spec.version", "skips a minor version")

			// Single-minor (3.5 -> 3.6) must be admitted. This proves the validator
			// is not simply over-blocking ALL updates -- the negative case alone
			// cannot distinguish a correct guard from a reject-everything bug.
			var cur ecv1alpha1.EtcdCluster
			if err := c.Resources().Get(ctx, cluster.Name, namespace, &cur); err != nil {
				t.Fatalf("failed to re-read cluster before single-minor update: %v", err)
			}
			cur.Spec.Version = "3.6.1"
			if err := c.Resources().Update(ctx, &cur); err != nil {
				t.Fatalf("expected single-minor upgrade 3.5.1 -> 3.6.1 to be admitted, got: %v", err)
			}
			return ctx
		})

	// --- Break-glass: the objectSelector opt-out. -----------------------------
	//
	// This is the assertion that justifies paying the kind+build+deploy cost: it
	// cannot be exercised by envtest or the static render test. A wrong operator
	// (In vs NotIn) or a wrong value in webhook_selector_patch.yaml would silently
	// disable admission fleet-wide; here we prove against a LIVE API server that
	// (a) the label bypasses the otherwise-fatal even-size rejection, and
	// (b) the very same invalid spec WITHOUT the label is still rejected -- so the
	// bypass is genuinely scoped to the labeled object, not globally off.

	feature.Assess("break-glass skip-webhooks label bypasses admission for that object only",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			c := cfg.Client()

			// Labeled: an even size (2) that the webhook would normally reject must be
			// ADMITTED because the objectSelector excludes it.
			labeled := newWebhookTestCluster("wh-skip-labeled", 2, webhookValidVersion)
			labeled.Labels = map[string]string{skipWebhooksLabel: "true"}
			if err := c.Resources().Create(ctx, labeled); err != nil {
				t.Fatalf("expected break-glass-labeled even-sized cluster to bypass the webhook and be admitted, got: %v", err)
			}
			defer func() { _ = c.Resources().Delete(ctx, labeled) }()

			// Unlabeled control: the SAME invalid spec must still be rejected, proving
			// the bypass is per-object and admission is otherwise live.
			unlabeled := newWebhookTestCluster("wh-skip-unlabeled", 2, webhookValidVersion)
			err := c.Resources().Create(ctx, unlabeled)
			requireWebhookRejection(t, err, "spec.size", "size must be an odd number")
			return ctx
		})

	_ = testEnv.Test(t, feature.Feature())
}
