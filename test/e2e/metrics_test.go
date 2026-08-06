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
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
// been reconciled. A bare name-substring check only proves the collector was
// registered (the # HELP/# TYPE lines are emitted even with zero series), so it
// is a necessary-but-not-sufficient guard. The value assertions in the Assess
// block below are what actually prove the per-cluster series were recorded.
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
// configured via spec.metrics, waits for it to become ready, and proves
// end-to-end that the operator's Prometheus /metrics endpoint exposes the
// custom per-cluster domain metrics with the CORRECT derived values for a
// healthy cluster -- not merely that the collectors are registered.
//
// Specifically it asserts, by polling the live /metrics scrape until the
// operator's asynchronous reconcile has populated status:
//   - member_count_desired / member_count / member_count_ready /
//     member_count_healthy all equal the cluster size (the operator actually
//     talked to etcd and counted real members),
//   - has_quorum == 1 and has_leader == 1 (the dangerous derived booleans that a
//     real outage dashboard reads; these require a real etcd round-trip and a
//     populated Status.Members/LeaderID, so they distinguish "operator fully
//     reconciled etcd" from "operator never talked to etcd"),
//   - learner_count == 0 and tls_enabled == 0 for a steady plaintext cluster,
//   - reconcile_duration_seconds_count >= 1 (the reconcile-timing hook fired),
//   - tls_ready is ABSENT for this plaintext cluster (the documented invariant
//     so alerts never fire on plaintext clusters),
//   - a PodMonitor object was actually created for the cluster with endpoint
//     port "client" and interval "30s".
//
// A single-member cluster is sufficient: quorum (1/1), a leader, healthy
// members, the PodMonitor path, and every derived gauge are all exercised, and
// the quorum-over-voting-members arithmetic is covered by the metrics package
// unit tests. Using size 1 avoids the dominant cost of sequential 3-node
// StatefulSet bootstrap + member-join.
func TestDomainMetricsExposed(t *testing.T) {
	clusterName := "etcd-metrics"
	const clusterSize = 1
	feature := features.New("domain-metrics")

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		metricsCreateCluster(ctx, t, c, clusterName, clusterSize)
		metricsWaitForStatefulSetReady(ctx, t, c, clusterName, clusterSize)
		return ctx
	})

	feature.Assess("operator /metrics exposes correct per-cluster domain values",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// The derived gauges are written by the operator's reconcile loop,
			// which is asynchronous to StatefulSet readiness: there is a race
			// window where pods are Ready but the operator has not yet run the
			// reconcile that populates Status.Members / quorum / leader. Poll the
			// scrape until every expected sample is present rather than reading
			// once and racing. No fixed sleeps anywhere.
			lvs := fmt.Sprintf("{name=%q,namespace=%q}", clusterName, namespace)
			want := map[string]string{
				"etcd_operator_cluster_member_count_desired" + lvs: strconv.Itoa(clusterSize),
				"etcd_operator_cluster_member_count" + lvs:         strconv.Itoa(clusterSize),
				"etcd_operator_cluster_member_count_ready" + lvs:   strconv.Itoa(clusterSize),
				"etcd_operator_cluster_member_count_healthy" + lvs: strconv.Itoa(clusterSize),
				"etcd_operator_cluster_learner_count" + lvs:        "0",
				// The two strongest, most dangerous assertions: a regression that
				// leaves Status.Members/LeaderID unpopulated (e.g. status-update
				// ordering) would ship has_quorum 0 / has_leader 0 green under a
				// name-only check. These require the operator to have actually
				// reconciled etcd.
				"etcd_operator_cluster_has_quorum" + lvs:  "1",
				"etcd_operator_cluster_has_leader" + lvs:  "1",
				"etcd_operator_cluster_tls_enabled" + lvs: "0",
			}

			var lastBody string
			err := wait.For(func(ctx context.Context) (bool, error) {
				lastBody = metricsScrapeOperator(ctx, t, c)
				for metric, val := range want {
					if !metricsContainsSample(lastBody, metric+" "+val) {
						return false, nil
					}
				}
				// The reconcile-timing histogram must have observed at least one
				// reconcile for this cluster. A histogram's # HELP line exists with
				// zero observations, so assert the _count child series is >= 1.
				if !metricsReconcileObserved(lastBody, clusterName, namespace) {
					return false, nil
				}
				return true, nil
			}, wait.WithTimeout(2*time.Minute), wait.WithInterval(5*time.Second), wait.WithContext(ctx))
			if err != nil {
				// Surface the metric names + the offending body for diagnosis.
				for _, name := range metricNamesToAssert {
					if !strings.Contains(lastBody, name) {
						t.Errorf("metric family %q missing from /metrics (collector not registered?)", name)
					}
				}
				for metric, val := range want {
					if !metricsContainsSample(lastBody, metric+" "+val) {
						t.Errorf("expected sample %q not found in /metrics", metric+" "+val)
					}
				}
				if !metricsReconcileObserved(lastBody, clusterName, namespace) {
					t.Errorf("reconcile_duration_seconds_count for %s/%s was not >= 1", namespace, clusterName)
				}
				t.Fatalf("timed out waiting for correct domain-metric values: %v", err)
			}

			// tls_ready must be ABSENT for a plaintext cluster: the metrics package
			// DeleteLabelValues()-es it for non-TLS clusters so "tls configured but
			// not ready" alerts never fire on plaintext. A present series here is a
			// real regression.
			tlsReadyPrefix := fmt.Sprintf(
				"etcd_operator_cluster_tls_ready{name=%q,namespace=%q}", clusterName, namespace)
			for _, line := range strings.Split(lastBody, "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), tlsReadyPrefix) {
					t.Errorf("expected NO tls_ready series for plaintext cluster, found: %q", strings.TrimSpace(line))
				}
			}
			return ctx
		})

	feature.Assess("operator reconciles a PodMonitor with the configured endpoint",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// The PodMonitor branch swallows all errors (CRD-absent is a silent
			// skip), so without observing the object the whole podMonitor half of
			// the feature is dead setup. The e2e suite installs the
			// prometheus-operator CRDs, so the object MUST exist. Poll because
			// PodMonitor reconcile is part of the same async reconcile loop.
			pm := newPodMonitorUnstructured(clusterName, namespace)
			if err := wait.For(
				conditions.New(c.Client().Resources()).ResourceMatch(pm, func(object k8s.Object) bool {
					u, ok := object.(*unstructured.Unstructured)
					if !ok {
						return false
					}
					port, interval, found := podMonitorEndpoint0(u)
					return found && port == "client" && interval == "30s"
				}),
				wait.WithTimeout(90*time.Second),
				wait.WithInterval(5*time.Second),
				wait.WithContext(ctx),
			); err != nil {
				// Re-Get for a precise diagnosis of what was (or wasn't) there.
				got := newPodMonitorUnstructured(clusterName, namespace)
				if gerr := c.Client().Resources().Get(ctx, clusterName, namespace, got); gerr != nil {
					t.Fatalf("PodMonitor %s/%s was never created (operator silently skipped it?): %v",
						namespace, clusterName, gerr)
				}
				port, interval, found := podMonitorEndpoint0(got)
				t.Fatalf("PodMonitor %s/%s endpoint mismatch (found=%v port=%q interval=%q), want port=client interval=30s: %v",
					namespace, clusterName, found, port, interval, err)
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

// metricsContainsSample reports whether the Prometheus exposition text contains
// a line equal to want after trimming surrounding whitespace. Sample lines are
// compared line-by-line so a stray prefix/suffix elsewhere in the body cannot
// produce a false positive.
func metricsContainsSample(body, want string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

// metricsReconcileObserved reports whether the reconcile-duration histogram has
// observed at least one reconcile for the given cluster, by parsing the
// _count child series. The _count line looks like:
//
//	etcd_operator_cluster_reconcile_duration_seconds_count{name="x",namespace="y"} 3
//
// We match on the label set (order-independent within the {...}) and require the
// trailing value to parse as a number >= 1.
func metricsReconcileObserved(body, name, ns string) bool {
	const prefix = "etcd_operator_cluster_reconcile_duration_seconds_count{"
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		brace := strings.IndexByte(line, '}')
		if brace < 0 {
			continue
		}
		labels := line[len(prefix):brace]
		if !strings.Contains(labels, fmt.Sprintf("name=%q", name)) ||
			!strings.Contains(labels, fmt.Sprintf("namespace=%q", ns)) {
			continue
		}
		valStr := strings.TrimSpace(line[brace+1:])
		v, err := strconv.ParseFloat(valStr, 64)
		if err == nil && v >= 1 {
			return true
		}
	}
	return false
}

// podMonitorGVK addresses the prometheus-operator PodMonitor type via
// unstructured so the test takes no hard dependency on the prometheus-operator
// Go module (mirroring the controller).
var podMonitorGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "PodMonitor",
}

// newPodMonitorUnstructured returns an empty unstructured object addressing the
// PodMonitor the operator creates for the cluster (same namespace/name).
func newPodMonitorUnstructured(name, ns string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(podMonitorGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	return u
}

// podMonitorEndpoint0 extracts the port and interval of the first
// podMetricsEndpoints entry from a PodMonitor unstructured object. found is
// false if the spec/endpoint is missing or malformed.
func podMonitorEndpoint0(u *unstructured.Unstructured) (port, interval string, found bool) {
	eps, ok, err := unstructured.NestedSlice(u.Object, "spec", "podMetricsEndpoints")
	if err != nil || !ok || len(eps) == 0 {
		return "", "", false
	}
	ep, ok := eps[0].(map[string]any)
	if !ok {
		return "", "", false
	}
	port, _ = ep["port"].(string)
	interval, _ = ep["interval"].(string)
	return port, interval, true
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
