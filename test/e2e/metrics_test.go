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
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// metricNamesToAssert lists the custom domain metrics this feature expects to
// find on the operator's /metrics endpoint once at least one EtcdCluster has
// been reconciled.
var metricNamesToAssert = []string{
	"etcd_operator_cluster_member_count_desired",
	"etcd_operator_cluster_member_count",
	"etcd_operator_cluster_member_count_ready",
	"etcd_operator_cluster_member_count_healthy",
	"etcd_operator_cluster_learner_count",
	"etcd_operator_cluster_has_quorum",
	"etcd_operator_cluster_has_leader",
	"etcd_operator_cluster_tls_enabled",
	"etcd_operator_cluster_reconcile_duration_seconds",
}

// TestDomainMetricsExposed creates an EtcdCluster with metrics + a PodMonitor
// configured via spec.metrics, waits for it to become ready, and asserts that
// the operator's Prometheus /metrics endpoint exposes the custom per-cluster
// domain metrics labelled with this cluster's namespace and name.
//
// NOTE: This test is authored primarily for compile-time verification in CI
// alongside the unit/envtest coverage of the metrics package; it only executes
// against a live KinD cluster as part of the e2e suite (make test-e2e).
func TestDomainMetricsExposed(t *testing.T) {
	clusterName := "etcd-metrics"
	feature := features.New("domain-metrics")

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		metricsCreateCluster(ctx, t, c, clusterName, 3)
		metricsWaitForStatefulSetReady(ctx, t, c, clusterName, 3)
		return ctx
	})

	feature.Assess("operator /metrics exposes per-cluster domain metrics",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			body := metricsScrapeOperator(ctx, t, c)
			for _, name := range metricNamesToAssert {
				if !strings.Contains(body, name) {
					t.Errorf("expected /metrics to contain %q, but it did not", name)
				}
			}
			// At least one series must be labelled with this cluster's name.
			if !strings.Contains(body, fmt.Sprintf("name=%q", clusterName)) {
				t.Errorf("expected /metrics to contain a series labelled name=%q", clusterName)
			}
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		metricsDeleteCluster(ctx, t, c, clusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// metricsCreateCluster creates an EtcdCluster with metrics + PodMonitor enabled.
// It is a self-contained helper (not shared with other e2e files) to keep this
// PR independent of in-flight e2e changes.
func metricsCreateCluster(ctx context.Context, t *testing.T, c *envconf.Config, name string, size int) {
	t.Helper()
	enabled := true
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: "v3.5.21",
			Metrics: &ecv1alpha1.MetricsSpec{
				Enabled: &enabled,
				PodMonitor: &ecv1alpha1.PodMonitorSpec{
					Enabled:  true,
					Interval: "30s",
					Port:     "client",
				},
			},
		},
	}
	if err := c.Client().Resources().Create(ctx, ec); err != nil {
		t.Fatalf("failed to create EtcdCluster %q: %v", name, err)
	}
}

// metricsDeleteCluster removes the EtcdCluster created for this feature.
func metricsDeleteCluster(ctx context.Context, t *testing.T, c *envconf.Config, name string) {
	t.Helper()
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if err := c.Client().Resources().Delete(ctx, ec); err != nil {
		t.Logf("failed to delete EtcdCluster %q: %v", name, err)
	}
}

// metricsWaitForStatefulSetReady blocks until the cluster's StatefulSet reports
// the expected number of ready replicas.
func metricsWaitForStatefulSetReady(ctx context.Context, t *testing.T, c *envconf.Config, name string, expectedReplicas int) {
	t.Helper()
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if err := wait.For(
		conditions.New(c.Client().Resources()).ResourceMatch(sts, func(object k8s.Object) bool {
			s, ok := object.(*appsv1.StatefulSet)
			return ok && int(s.Status.ReadyReplicas) == expectedReplicas
		}),
		wait.WithTimeout(5*time.Minute),
		wait.WithInterval(10*time.Second),
		wait.WithContext(ctx),
	); err != nil {
		t.Fatalf("statefulset %q did not reach %d ready replicas: %v", name, expectedReplicas, err)
	}
}

// metricsScrapeOperator scrapes the operator pod's metrics endpoint through the
// API server pod-proxy and returns the raw Prometheus exposition text. The e2e
// manager is expected to serve plain-HTTP metrics on :8080
// (--metrics-bind-address=:8080 --metrics-secure=false) for scrape simplicity.
func metricsScrapeOperator(ctx context.Context, t *testing.T, c *envconf.Config) string {
	t.Helper()
	pod, err := getEtcdOperatorPod(t, c.Client())
	if err != nil {
		t.Fatalf("failed to find operator pod: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(c.Client().RESTConfig())
	if err != nil {
		t.Fatalf("failed to build clientset: %v", err)
	}

	res := clientset.CoreV1().RESTClient().Get().
		Namespace(pod.Namespace).
		Resource("pods").
		SubResource("proxy").
		Name(fmt.Sprintf("%s:8080", pod.Name)).
		Suffix("metrics").
		Do(ctx)
	if err := res.Error(); err != nil {
		t.Fatalf("failed to scrape /metrics via pod proxy: %v", err)
	}
	raw, err := res.Raw()
	if err != nil {
		t.Fatalf("failed to read /metrics response: %v", err)
	}
	return string(raw)
}

var _ = corev1.Pod{}
