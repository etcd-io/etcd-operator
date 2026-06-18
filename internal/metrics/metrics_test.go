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

package metrics

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// resetState clears all collectors and the leader tracker so each test starts
// from a clean slate even though the collectors are package-level globals.
func resetState() {
	MemberCountDesired.Reset()
	MemberCount.Reset()
	MemberCountReady.Reset()
	MemberCountHealthy.Reset()
	LearnerCount.Reset()
	HasQuorum.Reset()
	HasLeader.Reset()
	TLSEnabled.Reset()
	LeaderChangesTotal.Reset()
	ReconcileErrorsTotal.Reset()
	ReconcileDurationSeconds.Reset()
	tracker.mu.Lock()
	tracker.leaders = map[string]string{}
	tracker.mu.Unlock()
}

func newCluster(ns, name string, size int) *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: size},
	}
}

// newRegistry returns an isolated registry with all collectors registered, so
// scrape values are deterministic and independent of process/global state.
func newRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	resetState()
	reg := prometheus.NewRegistry()
	if err := RegisterTo(reg); err != nil {
		t.Fatalf("RegisterTo failed: %v", err)
	}
	return reg
}

func TestRegisterToIsComplete(t *testing.T) {
	reg := newRegistry(t)
	// Every collector defined by the package must register without conflict.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	if len(mfs) == 0 {
		// Counters/histograms with no observations may not appear until used,
		// but gauges with no series also won't. This just ensures Gather works.
		t.Log("no metric families yet (expected before any values are set)")
	}
}

func TestRecordClusterMetrics_HealthyQuorate(t *testing.T) {
	reg := newRegistry(t)

	c := newCluster("ns1", "etcd-a", 3)
	c.Spec.TLS = &ecv1alpha1.TLSCertificate{Provider: "auto"}
	c.Status.MemberCount = 3
	c.Status.ReadyReplicas = 3
	c.Status.LeaderID = "abc123"
	c.Status.Members = []ecv1alpha1.MemberStatus{
		{ID: "1", IsHealthy: true, IsLeader: true},
		{ID: "2", IsHealthy: true},
		{ID: "3", IsHealthy: true},
	}

	RecordClusterMetrics(c)

	assertGauge(t, reg, "etcd_operator_cluster_member_count_desired", 3, "ns1", "etcd-a")
	assertGauge(t, reg, "etcd_operator_cluster_member_count", 3, "ns1", "etcd-a")
	assertGauge(t, reg, "etcd_operator_cluster_member_count_ready", 3, "ns1", "etcd-a")
	assertGauge(t, reg, "etcd_operator_cluster_member_count_healthy", 3, "ns1", "etcd-a")
	assertGauge(t, reg, "etcd_operator_cluster_learner_count", 0, "ns1", "etcd-a")
	assertGauge(t, reg, "etcd_operator_cluster_has_quorum", 1, "ns1", "etcd-a")
	assertGauge(t, reg, "etcd_operator_cluster_has_leader", 1, "ns1", "etcd-a")
	assertGauge(t, reg, "etcd_operator_cluster_tls_enabled", 1, "ns1", "etcd-a")
}

func TestRecordClusterMetrics_DegradedNoQuorumNoLeader(t *testing.T) {
	reg := newRegistry(t)

	c := newCluster("ns1", "etcd-b", 3)
	c.Status.MemberCount = 3
	c.Status.ReadyReplicas = 1
	c.Status.LeaderID = "" // no leader known
	c.Status.Members = []ecv1alpha1.MemberStatus{
		{ID: "1", IsHealthy: true},
		{ID: "2", IsHealthy: false},
		{ID: "3", IsHealthy: false},
	}

	RecordClusterMetrics(c)

	// 1/3 healthy -> below quorum of 2.
	assertGauge(t, reg, "etcd_operator_cluster_member_count_healthy", 1, "ns1", "etcd-b")
	assertGauge(t, reg, "etcd_operator_cluster_has_quorum", 0, "ns1", "etcd-b")
	assertGauge(t, reg, "etcd_operator_cluster_has_leader", 0, "ns1", "etcd-b")
	assertGauge(t, reg, "etcd_operator_cluster_tls_enabled", 0, "ns1", "etcd-b")
}

func TestRecordClusterMetrics_LearnerCounted(t *testing.T) {
	reg := newRegistry(t)

	c := newCluster("ns1", "etcd-c", 4)
	c.Status.MemberCount = 4
	c.Status.Members = []ecv1alpha1.MemberStatus{
		{ID: "1", IsHealthy: true, IsLeader: true},
		{ID: "2", IsHealthy: true},
		{ID: "3", IsHealthy: true},
		{ID: "4", IsHealthy: true, IsLearner: true},
	}
	c.Status.LeaderID = "1"

	RecordClusterMetrics(c)

	assertGauge(t, reg, "etcd_operator_cluster_learner_count", 1, "ns1", "etcd-c")
	// 4 healthy of 4 members -> quorum (>=3) satisfied.
	assertGauge(t, reg, "etcd_operator_cluster_has_quorum", 1, "ns1", "etcd-c")
}

func TestLeaderChangesCounter(t *testing.T) {
	reg := newRegistry(t)
	c := newCluster("ns1", "etcd-d", 3)
	c.Status.MemberCount = 3
	c.Status.Members = []ecv1alpha1.MemberStatus{{ID: "1", IsHealthy: true}}

	// First observation: no change counted.
	c.Status.LeaderID = "leader1"
	RecordClusterMetrics(c)
	assertCounter(t, reg, "etcd_operator_cluster_leader_changes_total", 0, "ns1", "etcd-d")

	// Same leader again: still no change.
	RecordClusterMetrics(c)
	assertCounter(t, reg, "etcd_operator_cluster_leader_changes_total", 0, "ns1", "etcd-d")

	// Leader changes: counted once.
	c.Status.LeaderID = "leader2"
	RecordClusterMetrics(c)
	assertCounter(t, reg, "etcd_operator_cluster_leader_changes_total", 1, "ns1", "etcd-d")

	// Leaderless window does not count, and recovery to a new leader counts again.
	c.Status.LeaderID = ""
	RecordClusterMetrics(c)
	assertCounter(t, reg, "etcd_operator_cluster_leader_changes_total", 1, "ns1", "etcd-d")
	c.Status.LeaderID = "leader3"
	RecordClusterMetrics(c)
	assertCounter(t, reg, "etcd_operator_cluster_leader_changes_total", 2, "ns1", "etcd-d")
}

func TestObserveReconcile(t *testing.T) {
	reg := newRegistry(t)

	ObserveReconcile("ns1", "etcd-e", 0.5, nil)
	ObserveReconcile("ns1", "etcd-e", 0.7, errors.New("boom"))

	// Two observations recorded in the histogram.
	if got := testutil.CollectAndCount(ReconcileDurationSeconds); got == 0 {
		t.Fatalf("expected reconcile duration observations, got none")
	}
	assertCounter(t, reg, "etcd_operator_cluster_reconcile_errors_total", 1, "ns1", "etcd-e")
}

func TestDeleteClusterMetrics(t *testing.T) {
	reg := newRegistry(t)

	c := newCluster("ns1", "etcd-f", 1)
	c.Status.MemberCount = 1
	c.Status.LeaderID = "x"
	c.Status.Members = []ecv1alpha1.MemberStatus{{ID: "1", IsHealthy: true}}
	RecordClusterMetrics(c)
	ObserveReconcile("ns1", "etcd-f", 0.1, errors.New("e"))

	if c := testutil.CollectAndCount(MemberCount); c == 0 {
		t.Fatalf("expected member_count series present before delete")
	}

	DeleteClusterMetrics("ns1", "etcd-f")

	for _, coll := range []prometheus.Collector{
		MemberCount, MemberCountDesired, HasQuorum, HasLeader, TLSEnabled,
		LeaderChangesTotal, ReconcileErrorsTotal,
	} {
		if got := testutil.CollectAndCount(coll); got != 0 {
			t.Errorf("expected 0 series after delete, got %d", got)
		}
	}
	_ = reg

	// Leader tracker must also be reset, so a re-created cluster does not
	// spuriously count a leader change on its first observation.
	c.Status.LeaderID = "y"
	RecordClusterMetrics(c)
	assertCounter(t, reg, "etcd_operator_cluster_leader_changes_total", 0, "ns1", "etcd-f")
}

func TestMetricsExposedWithExpectedLabels(t *testing.T) {
	reg := newRegistry(t)
	c := newCluster("prod", "etcd-1", 3)
	c.Status.MemberCount = 3
	c.Status.Members = []ecv1alpha1.MemberStatus{{ID: "1", IsHealthy: true}}
	c.Status.LeaderID = "1"
	RecordClusterMetrics(c)

	expected := `
# HELP etcd_operator_cluster_member_count_desired Desired number of etcd members for the cluster (spec.size).
# TYPE etcd_operator_cluster_member_count_desired gauge
etcd_operator_cluster_member_count_desired{name="etcd-1",namespace="prod"} 3
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"etcd_operator_cluster_member_count_desired"); err != nil {
		t.Errorf("unexpected metric output: %v", err)
	}
}

func TestSpecHelpers(t *testing.T) {
	// Default (nil Metrics) -> metrics on, podmonitor off.
	s := &ecv1alpha1.EtcdClusterSpec{}
	if !s.MetricsEnabled() {
		t.Error("expected metrics enabled by default")
	}
	if s.PodMonitorEnabled() {
		t.Error("expected podmonitor disabled by default")
	}

	// Explicit disable.
	off := false
	s.Metrics = &ecv1alpha1.MetricsSpec{Enabled: &off}
	if s.MetricsEnabled() {
		t.Error("expected metrics disabled when enabled=false")
	}
	if s.PodMonitorEnabled() {
		t.Error("podmonitor must follow metrics disable")
	}

	// Explicit enable + podmonitor on.
	on := true
	s.Metrics = &ecv1alpha1.MetricsSpec{Enabled: &on, PodMonitor: &ecv1alpha1.PodMonitorSpec{Enabled: true}}
	if !s.PodMonitorEnabled() {
		t.Error("expected podmonitor enabled")
	}

	// PodMonitor enabled but metrics disabled -> podmonitor must be off.
	s.Metrics.Enabled = &off
	if s.PodMonitorEnabled() {
		t.Error("podmonitor must be off when metrics disabled")
	}
}

func assertGauge(t *testing.T, reg *prometheus.Registry, name string, want float64, ns, clusterName string) {
	t.Helper()
	got := readSample(t, reg, name, ns, clusterName)
	if got != want {
		t.Errorf("%s{namespace=%q,name=%q}=%v, want %v", name, ns, clusterName, got, want)
	}
}

func assertCounter(t *testing.T, reg *prometheus.Registry, name string, want float64, ns, clusterName string) {
	t.Helper()
	got := readSample(t, reg, name, ns, clusterName)
	if got != want {
		t.Errorf("%s{namespace=%q,name=%q}=%v, want %v", name, ns, clusterName, got, want)
	}
}

// readSample gathers the registry and returns the value of the named metric for
// the given (namespace,name) label pair, or 0 if no such series exists.
func readSample(t *testing.T, reg *prometheus.Registry, name, ns, clusterName string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			var gotNS, gotName string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case labelNamespace:
					gotNS = l.GetValue()
				case labelName:
					gotName = l.GetValue()
				}
			}
			if gotNS != ns || gotName != clusterName {
				continue
			}
			if m.Gauge != nil {
				return m.Gauge.GetValue()
			}
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
		}
	}
	return 0
}
