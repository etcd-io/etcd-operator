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

// Package config_test renders the deployment kustomization and asserts the
// production-hardening properties of the admission webhook wiring that are easy
// to break silently in YAML: the fail-closed policy and timeouts, the
// break-glass objectSelector, and the cert-manager caBundle injection chain
// (inject-ca-from annotation -> serving Certificate -> webhook Service DNS ->
// mounted secret). These are verified against the actual `kustomize build`
// output rather than the hand-maintained source files so a future edit to any
// link in the chain is caught.
package config_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

const skipLabelKey = "etcd.operator.etcd.io/skip-webhooks"

// repoRoot returns the repository root (two levels up from test/config).
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

// kustomizeBin resolves a kustomize binary: the repo-local bin/kustomize that
// `make kustomize` installs, otherwise one on PATH. Returns "" when neither is
// available so the test can skip rather than fail on a bare checkout.
func kustomizeBin(t *testing.T) string {
	t.Helper()
	local := filepath.Join(repoRoot(t), "bin", "kustomize")
	if fi, err := os.Stat(local); err == nil && !fi.IsDir() {
		return local
	}
	if p, err := exec.LookPath("kustomize"); err == nil {
		return p
	}
	return ""
}

// renderDefault runs `kustomize build config/default` and splits the multi-doc
// output, returning the decoded webhook configurations and serving Certificate.
func renderDefault(t *testing.T) (
	[]admissionregv1.ValidatingWebhookConfiguration,
	[]admissionregv1.MutatingWebhookConfiguration,
	[]certmanagerv1.Certificate,
) {
	t.Helper()
	bin := kustomizeBin(t)
	if bin == "" {
		t.Skip("kustomize not found (run `make kustomize`); skipping config render assertions")
	}
	root := repoRoot(t)
	cmd := exec.Command(bin, "build", filepath.Join(root, "config", "default"))
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("kustomize build failed: %v\nstderr:\n%s", err, ee.Stderr)
		}
		t.Fatalf("kustomize build failed: %v", err)
	}

	var (
		vwcs  []admissionregv1.ValidatingWebhookConfiguration
		mwcs  []admissionregv1.MutatingWebhookConfiguration
		certs []certmanagerv1.Certificate
	)
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	for {
		// Decode each document to canonical JSON, peek at its kind, then unmarshal
		// into the matching typed object. yaml.Unmarshal on a generic map already
		// produces JSON-compatible values, so encoding/json round-trips cleanly.
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break // io.EOF (or a malformed trailing doc) ends the stream.
		}
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		var meta metav1.TypeMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			continue
		}
		switch meta.Kind {
		case "ValidatingWebhookConfiguration":
			var o admissionregv1.ValidatingWebhookConfiguration
			mustJSON(t, raw, &o)
			vwcs = append(vwcs, o)
		case "MutatingWebhookConfiguration":
			var o admissionregv1.MutatingWebhookConfiguration
			mustJSON(t, raw, &o)
			mwcs = append(mwcs, o)
		case "Certificate":
			var o certmanagerv1.Certificate
			mustJSON(t, raw, &o)
			certs = append(certs, o)
		}
	}
	return vwcs, mwcs, certs
}

// mustJSON unmarshals a JSON document into out, failing the test on error.
func mustJSON(t *testing.T, raw json.RawMessage, out any) {
	t.Helper()
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("decoding %T: %v", out, err)
	}
}

// assertSkipSelector verifies the break-glass objectSelector: absence of the
// skip label must still select the object (NotIn "true").
func assertSkipSelector(t *testing.T, sel *metav1.LabelSelector, who string) {
	t.Helper()
	if sel == nil {
		t.Fatalf("%s: expected an objectSelector (break-glass escape hatch) but it was nil", who)
	}
	for _, expr := range sel.MatchExpressions {
		if expr.Key != skipLabelKey {
			continue
		}
		if expr.Operator != metav1.LabelSelectorOpNotIn {
			t.Fatalf("%s: objectSelector on %q must use NotIn so an unlabeled CR is still validated; got %q",
				who, skipLabelKey, expr.Operator)
		}
		if len(expr.Values) != 1 || expr.Values[0] != "true" {
			t.Fatalf("%s: objectSelector on %q must exclude value \"true\"; got %v",
				who, skipLabelKey, expr.Values)
		}
		return
	}
	t.Fatalf("%s: objectSelector is missing the break-glass key %q", who, skipLabelKey)
}

func TestWebhookConfigsAreProductionHardened(t *testing.T) {
	vwcs, mwcs, _ := renderDefault(t)

	if len(vwcs) != 1 {
		t.Fatalf("expected exactly one ValidatingWebhookConfiguration, got %d", len(vwcs))
	}
	if len(mwcs) != 1 {
		t.Fatalf("expected exactly one MutatingWebhookConfiguration, got %d", len(mwcs))
	}

	check := func(who string, wh admissionregv1.ValidatingWebhook) {
		if wh.FailurePolicy == nil || *wh.FailurePolicy != admissionregv1.Fail {
			t.Errorf("%s: failurePolicy must be Fail (fail closed for correctness-critical validation); got %v",
				who, wh.FailurePolicy)
		}
		if wh.MatchPolicy == nil || *wh.MatchPolicy != admissionregv1.Equivalent {
			t.Errorf("%s: matchPolicy must be Equivalent; got %v", who, wh.MatchPolicy)
		}
		if wh.TimeoutSeconds == nil || *wh.TimeoutSeconds != 10 {
			t.Errorf("%s: timeoutSeconds must be 10; got %v", who, wh.TimeoutSeconds)
		}
		if wh.SideEffects == nil || *wh.SideEffects != admissionregv1.SideEffectClassNone {
			t.Errorf("%s: sideEffects must be None; got %v", who, wh.SideEffects)
		}
		assertSkipSelector(t, wh.ObjectSelector, who)
	}

	for _, wh := range vwcs[0].Webhooks {
		check("validating/"+wh.Name, wh)
	}
	for _, wh := range mwcs[0].Webhooks {
		// MutatingWebhook shares the relevant fields; adapt via a tiny shim.
		check("mutating/"+wh.Name, admissionregv1.ValidatingWebhook{
			Name:           wh.Name,
			FailurePolicy:  wh.FailurePolicy,
			MatchPolicy:    wh.MatchPolicy,
			TimeoutSeconds: wh.TimeoutSeconds,
			SideEffects:    wh.SideEffects,
			ObjectSelector: wh.ObjectSelector,
		})
	}
}

func TestCertManagerCABundleInjectionWiring(t *testing.T) {
	vwcs, mwcs, certs := renderDefault(t)

	if len(certs) != 1 {
		t.Fatalf("expected exactly one serving Certificate, got %d", len(certs))
	}
	cert := certs[0]

	// The inject-ca-from annotation on each webhook config must point at the
	// rendered serving Certificate as namespace/name.
	wantRef := cert.Namespace + "/" + cert.Name
	const injectAnnotation = "cert-manager.io/inject-ca-from"

	assertInject := func(who string, ann map[string]string) {
		got, ok := ann[injectAnnotation]
		if !ok {
			t.Fatalf("%s: missing %s annotation; cert-manager will not inject the caBundle", who, injectAnnotation)
		}
		if got != wantRef {
			t.Fatalf("%s: %s = %q but the serving Certificate is %q; the CA will be injected from the wrong object",
				who, injectAnnotation, got, wantRef)
		}
	}
	assertInject("ValidatingWebhookConfiguration", vwcs[0].Annotations)
	assertInject("MutatingWebhookConfiguration", mwcs[0].Annotations)

	// The Certificate must be issued by the rendered self-signed Issuer and write
	// the secret the manager mounts (webhook-server-cert).
	if cert.Spec.SecretName != "webhook-server-cert" {
		t.Errorf("serving Certificate secretName must be webhook-server-cert (mounted by manager_webhook_patch); got %q",
			cert.Spec.SecretName)
	}
	if cert.Spec.IssuerRef.Name == "" || cert.Spec.IssuerRef.Kind != "Issuer" {
		t.Errorf("serving Certificate issuerRef must reference the self-signed Issuer; got %+v", cert.Spec.IssuerRef)
	}

	// dnsNames must resolve to the actual webhook Service FQDN so the served cert
	// validates against the clientConfig.service the API server dials. The
	// replacements substitute SERVICE_NAME/SERVICE_NAMESPACE, so no placeholder
	// must survive and the svc form must be present.
	var sawSvc, sawClusterLocal bool
	for _, dn := range cert.Spec.DNSNames {
		if dn == "SERVICE_NAME.SERVICE_NAMESPACE.svc" || dn == "SERVICE_NAME.SERVICE_NAMESPACE.svc.cluster.local" {
			t.Fatalf("serving Certificate dnsNames still contains an unsubstituted placeholder %q", dn)
		}
		if strings.HasSuffix(dn, ".svc") {
			sawSvc = true
		}
		if strings.HasSuffix(dn, ".svc.cluster.local") {
			sawClusterLocal = true
		}
	}
	if !sawSvc || !sawClusterLocal {
		t.Fatalf("serving Certificate dnsNames must include both <svc>.<ns>.svc and .svc.cluster.local; got %v",
			cert.Spec.DNSNames)
	}

	// Cross-check the dnsNames name/namespace against the webhook clientConfig the
	// API server will dial, so a future rename of the Service can't silently break
	// TLS SAN matching.
	svcName := vwcs[0].Webhooks[0].ClientConfig.Service.Name
	svcNS := vwcs[0].Webhooks[0].ClientConfig.Service.Namespace
	wantFQDN := svcName + "." + svcNS + ".svc"
	found := false
	for _, dn := range cert.Spec.DNSNames {
		if dn == wantFQDN {
			found = true
		}
	}
	if !found {
		t.Fatalf("serving Certificate dnsNames must include the webhook Service FQDN %q so the served cert's "+
			"SAN matches the dialed service; got %v", wantFQDN, cert.Spec.DNSNames)
	}
}
