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

package v1alpha1_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	operatorv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// This file exercises the validating + defaulting webhooks against a real
// (envtest) API server: the generated webhook configurations are installed, a
// manager serves the webhook endpoints over the envtest-provisioned serving
// cert, and invalid EtcdCluster objects are applied to assert they are rejected
// with the exact, actionable messages produced by the webhook.

var (
	whCfg     *rest.Config
	whEnv     *envtest.Environment
	whClient  client.Client
	whTestNS  = "default"
	whStarted bool
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		// Without envtest binaries we cannot stand up an API server; the pure-unit
		// tests in this package still run via the normal `go test` path. Skipping
		// here keeps `go vet` / `go build` green on machines without assets.
		fmt.Fprintln(os.Stderr, "KUBEBUILDER_ASSETS not set; skipping webhook envtest setup")
		os.Exit(m.Run())
	}

	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	envTestK8sVersion := os.Getenv("ENVTEST_K8S_VERSION")
	if envTestK8sVersion == "" {
		envTestK8sVersion = "1.31.0"
	}

	whEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join(
			"..", "..", "bin", "k8s",
			fmt.Sprintf("%s-%s-%s", envTestK8sVersion, runtime.GOOS, runtime.GOARCH),
		),
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("..", "..", "config", "webhook", "manifests.yaml")},
		},
	}

	var err error
	whCfg, err = whEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start webhook envtest: %v\n", err)
		os.Exit(1)
	}
	whStarted = true

	if err := operatorv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add scheme: %v\n", err)
		os.Exit(1)
	}

	whClient, err = client.New(whCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create client: %v\n", err)
		os.Exit(1)
	}

	// Start a manager serving the webhooks on the envtest-provisioned cert.
	wo := whEnv.WebhookInstallOptions
	mgr, err := manager.New(whCfg, manager.Options{
		Scheme:  scheme.Scheme,
		Metrics: server.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    wo.LocalServingHost,
			Port:    wo.LocalServingPort,
			CertDir: wo.LocalServingCertDir,
			TLSOpts: []func(*tls.Config){func(c *tls.Config) { c.MinVersion = tls.VersionTLS12 }},
		}),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create manager: %v\n", err)
		os.Exit(1)
	}
	if err := (&operatorv1alpha1.EtcdCluster{}).SetupWebhookWithManager(mgr); err != nil {
		fmt.Fprintf(os.Stderr, "failed to register webhook: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := mgr.Start(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "manager exited: %v\n", err)
		}
	}()

	// Wait for the webhook server to accept connections before running tests.
	if err := waitForWebhookServer(wo.LocalServingHost, wo.LocalServingPort); err != nil {
		fmt.Fprintf(os.Stderr, "webhook server never became ready: %v\n", err)
		cancel()
		_ = whEnv.Stop()
		os.Exit(1)
	}

	code := m.Run()

	cancel()
	if err := whEnv.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to stop envtest: %v\n", err)
	}
	os.Exit(code)
}

func waitForWebhookServer(host string, port int) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	dialer := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // test-only
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := dialer.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timed out dialing %s", addr)
}

func skipIfNoEnvtest(t *testing.T) {
	t.Helper()
	if !whStarted {
		t.Skip("webhook envtest not started (KUBEBUILDER_ASSETS unset)")
	}
}

func mkCluster(name string, size int, version string) *operatorv1alpha1.EtcdCluster {
	return &operatorv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: whTestNS},
		Spec:       operatorv1alpha1.EtcdClusterSpec{Size: size, Version: version},
	}
}

func TestEnvtest_RejectsEvenSize(t *testing.T) {
	skipIfNoEnvtest(t)
	err := whClient.Create(context.Background(), mkCluster("even-size", 2, "3.6.1"))
	if err == nil {
		t.Fatal("expected even size to be rejected by the validating webhook")
	}
	if !strings.Contains(err.Error(), "size must be an odd number") {
		t.Fatalf("unexpected rejection message: %v", err)
	}
	if !strings.Contains(err.Error(), "Use 1 or 3 instead") {
		t.Fatalf("expected actionable remediation in message, got: %v", err)
	}
}

func TestEnvtest_RejectsBadVersion(t *testing.T) {
	skipIfNoEnvtest(t)
	err := whClient.Create(context.Background(), mkCluster("bad-version", 3, "not-semver"))
	if err == nil {
		t.Fatal("expected non-semver version to be rejected")
	}
	if !strings.Contains(err.Error(), "is not a valid semantic version") {
		t.Fatalf("unexpected rejection message: %v", err)
	}
}

func TestEnvtest_RejectsUnknownTLSProvider(t *testing.T) {
	skipIfNoEnvtest(t)
	c := mkCluster("bad-tls", 3, "3.6.1")
	c.Spec.TLS = &operatorv1alpha1.TLSCertificate{Provider: "vault"}
	err := whClient.Create(context.Background(), c)
	if err == nil {
		t.Fatal("expected unknown TLS provider to be rejected")
	}
	if !strings.Contains(err.Error(), "supported values: \"auto\", \"cert-manager\"") {
		t.Fatalf("unexpected rejection message: %v", err)
	}
}

func TestEnvtest_AcceptsValidAndDefaultsTLSProvider(t *testing.T) {
	skipIfNoEnvtest(t)
	c := mkCluster("valid-defaults", 3, "3.6.1")
	c.Spec.TLS = &operatorv1alpha1.TLSCertificate{} // empty provider -> defaulted to "auto"
	if err := whClient.Create(context.Background(), c); err != nil {
		t.Fatalf("expected valid cluster to be admitted, got: %v", err)
	}
	t.Cleanup(func() { _ = whClient.Delete(context.Background(), c) })

	var got operatorv1alpha1.EtcdCluster
	if err := whClient.Get(context.Background(),
		client.ObjectKey{Name: c.Name, Namespace: c.Namespace}, &got); err != nil {
		t.Fatalf("failed to read back cluster: %v", err)
	}
	if got.Spec.TLS == nil || got.Spec.TLS.Provider != "auto" {
		t.Fatalf("expected defaulting webhook to set tls.provider=auto, got: %+v", got.Spec.TLS)
	}
}

func TestEnvtest_RejectsSkipMinorUpgrade(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()
	c := mkCluster("upgrade-guard", 3, "3.5.1")
	if err := whClient.Create(ctx, c); err != nil {
		t.Fatalf("failed to create initial cluster: %v", err)
	}
	t.Cleanup(func() { _ = whClient.Delete(ctx, c) })

	var got operatorv1alpha1.EtcdCluster
	if err := whClient.Get(ctx, client.ObjectKey{Name: c.Name, Namespace: c.Namespace}, &got); err != nil {
		t.Fatalf("failed to read back cluster: %v", err)
	}
	got.Spec.Version = "3.7.1" // skips 3.6
	err := whClient.Update(ctx, &got)
	if err == nil {
		t.Fatal("expected skip-minor upgrade to be rejected on update")
	}
	if !strings.Contains(err.Error(), "skips a minor version") {
		t.Fatalf("unexpected upgrade rejection message: %v", err)
	}

	// A single-minor upgrade must be accepted.
	if err := whClient.Get(ctx, client.ObjectKey{Name: c.Name, Namespace: c.Namespace}, &got); err != nil {
		t.Fatalf("failed to re-read cluster: %v", err)
	}
	got.Spec.Version = "3.6.1"
	if err := whClient.Update(ctx, &got); err != nil {
		t.Fatalf("expected single-minor upgrade to be accepted, got: %v", err)
	}
}

// keep ctrl import used even if the manager construction changes shape.
var _ = ctrl.Log
