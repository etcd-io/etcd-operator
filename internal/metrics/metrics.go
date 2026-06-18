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

// Package metrics defines and registers the custom domain metrics exposed by
// the etcd-operator for every reconciled EtcdCluster. The collectors are
// registered against the controller-runtime global Prometheus registry
// (sigs.k8s.io/controller-runtime/pkg/metrics) so that they are served on the
// operator's existing /metrics endpoint with no additional HTTP plumbing.
//
// All per-cluster gauges are labelled with the cluster's namespace and name so
// that a single operator instance managing many clusters produces one time
// series per (namespace,name). When a cluster is deleted the operator should
// call DeleteClusterMetrics to drop its series and avoid unbounded cardinality
// growth from stale clusters.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

const (
	subsystem = "etcd_operator_cluster"

	// labelNamespace and labelName identify the EtcdCluster a series belongs to.
	labelNamespace = "namespace"
	labelName      = "name"
)

var (
	// MemberCountDesired is the size requested in EtcdCluster.Spec.Size.
	MemberCountDesired = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystem,
		Name:      "member_count_desired",
		Help:      "Desired number of etcd members for the cluster (spec.size).",
	}, []string{labelNamespace, labelName})

	// MemberCount is the number of members reported by the etcd member list API.
	MemberCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystem,
		Name:      "member_count",
		Help:      "Number of members currently registered in the etcd cluster (member list API).",
	}, []string{labelNamespace, labelName})

	// MemberCountReady is the number of ready etcd pods (StatefulSet readyReplicas).
	MemberCountReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystem,
		Name:      "member_count_ready",
		Help:      "Number of ready etcd members (StatefulSet readyReplicas).",
	}, []string{labelNamespace, labelName})

	// MemberCountHealthy is the number of members reporting healthy.
	MemberCountHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystem,
		Name:      "member_count_healthy",
		Help:      "Number of etcd members reported healthy in the cluster status.",
	}, []string{labelNamespace, labelName})

	// LearnerCount is the number of members that are currently learners.
	LearnerCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystem,
		Name:      "learner_count",
		Help:      "Number of etcd members that are currently learners (non-voting).",
	}, []string{labelNamespace, labelName})

	// HasQuorum is 1 when a quorum of members is healthy, 0 otherwise.
	HasQuorum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystem,
		Name:      "has_quorum",
		Help:      "Whether the etcd cluster currently has quorum (1) or not (0).",
	}, []string{labelNamespace, labelName})

	// HasLeader is 1 when a leader is known, 0 otherwise.
	HasLeader = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystem,
		Name:      "has_leader",
		Help:      "Whether the etcd cluster currently has a known leader (1) or not (0).",
	}, []string{labelNamespace, labelName})

	// TLSEnabled is 1 when the cluster is configured for TLS, 0 otherwise.
	TLSEnabled = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: subsystem,
		Name:      "tls_enabled",
		Help:      "Whether TLS is configured for the etcd cluster (1) or not (0).",
	}, []string{labelNamespace, labelName})

	// LeaderChangesTotal counts observed changes of the cluster leader ID.
	LeaderChangesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: subsystem,
		Name:      "leader_changes_total",
		Help:      "Total number of etcd leader changes observed by the operator for the cluster.",
	}, []string{labelNamespace, labelName})

	// ReconcileDurationSeconds observes the wall-clock duration of a reconcile.
	ReconcileDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: subsystem,
		Name:      "reconcile_duration_seconds",
		Help:      "Duration of EtcdCluster reconcile loops in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{labelNamespace, labelName})

	// ReconcileErrorsTotal counts reconcile loops that returned an error.
	ReconcileErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: subsystem,
		Name:      "reconcile_errors_total",
		Help:      "Total number of EtcdCluster reconcile loops that returned an error.",
	}, []string{labelNamespace, labelName})
)

// allCollectors returns every collector defined by this package. It is the
// single source of truth used by both registration and per-cluster deletion.
func allCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		MemberCountDesired,
		MemberCount,
		MemberCountReady,
		MemberCountHealthy,
		LearnerCount,
		HasQuorum,
		HasLeader,
		TLSEnabled,
		LeaderChangesTotal,
		ReconcileDurationSeconds,
		ReconcileErrorsTotal,
	}
}

var registerOnce sync.Once

// MustRegister registers all custom collectors against the controller-runtime
// global registry. It is safe to call more than once; only the first call has
// an effect, which keeps it usable from both cmd/main.go and from tests that
// share the global registry.
func MustRegister() {
	registerOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(allCollectors()...)
	})
}

// RegisterTo registers all custom collectors against an arbitrary registry.
// This is primarily intended for unit tests that want an isolated registry so
// that scraped values are deterministic and independent of process state.
func RegisterTo(r prometheus.Registerer) error {
	for _, c := range allCollectors() {
		if err := r.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// leaderTracker remembers the last observed leader ID per cluster so that
// transitions can be counted as leader changes. It is keyed by namespace/name.
type leaderTracker struct {
	mu      sync.Mutex
	leaders map[string]string
}

var tracker = &leaderTracker{leaders: map[string]string{}}

func clusterKey(namespace, name string) string {
	return namespace + "/" + name
}

// observeLeader records the current leader for a cluster and returns true if it
// changed from a previously-known, non-empty leader. The first observation (or
// any observation following an unknown/empty leader) does not count as a change
// so that operator restarts and transient leaderless windows do not inflate the
// leader_changes_total counter.
func (t *leaderTracker) observeLeader(namespace, name, leaderID string) bool {
	if leaderID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := clusterKey(namespace, name)
	prev, ok := t.leaders[key]
	t.leaders[key] = leaderID
	return ok && prev != "" && prev != leaderID
}

// forget drops any tracked leader for the cluster.
func (t *leaderTracker) forget(namespace, name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.leaders, clusterKey(namespace, name))
}

// RecordClusterMetrics derives all per-cluster gauge values from the cluster's
// spec and (already-populated) status, and counts a leader change if the
// observed leader differs from the previous reconcile. It is intended to be
// called once per reconcile, after the controller has computed status, with the
// in-memory EtcdCluster object (no extra API calls required).
func RecordClusterMetrics(cluster *ecv1alpha1.EtcdCluster) {
	ns, name := cluster.Namespace, cluster.Name

	MemberCountDesired.WithLabelValues(ns, name).Set(float64(cluster.Spec.Size))
	MemberCount.WithLabelValues(ns, name).Set(float64(cluster.Status.MemberCount))
	MemberCountReady.WithLabelValues(ns, name).Set(float64(cluster.Status.ReadyReplicas))

	healthy := 0
	learners := 0
	for _, m := range cluster.Status.Members {
		if m.IsHealthy {
			healthy++
		}
		if m.IsLearner {
			learners++
		}
	}
	MemberCountHealthy.WithLabelValues(ns, name).Set(float64(healthy))
	LearnerCount.WithLabelValues(ns, name).Set(float64(learners))

	// Quorum: a strict majority of the registered members must be healthy.
	HasQuorum.WithLabelValues(ns, name).Set(boolToFloat(hasQuorum(int(cluster.Status.MemberCount), healthy)))

	HasLeader.WithLabelValues(ns, name).Set(boolToFloat(cluster.Status.LeaderID != ""))
	TLSEnabled.WithLabelValues(ns, name).Set(boolToFloat(cluster.Spec.TLS != nil))

	if tracker.observeLeader(ns, name, cluster.Status.LeaderID) {
		LeaderChangesTotal.WithLabelValues(ns, name).Inc()
	}
}

// ObserveReconcile records the duration of a reconcile loop and increments the
// error counter when the reconcile returned an error.
func ObserveReconcile(namespace, name string, seconds float64, reconcileErr error) {
	ReconcileDurationSeconds.WithLabelValues(namespace, name).Observe(seconds)
	if reconcileErr != nil {
		ReconcileErrorsTotal.WithLabelValues(namespace, name).Inc()
	}
}

// DeleteClusterMetrics removes every per-cluster series for the given cluster.
// It should be called when an EtcdCluster is deleted to bound metric
// cardinality. Deleting a label set that was never set is a no-op.
func DeleteClusterMetrics(namespace, name string) {
	lvs := []string{namespace, name}
	MemberCountDesired.DeleteLabelValues(lvs...)
	MemberCount.DeleteLabelValues(lvs...)
	MemberCountReady.DeleteLabelValues(lvs...)
	MemberCountHealthy.DeleteLabelValues(lvs...)
	LearnerCount.DeleteLabelValues(lvs...)
	HasQuorum.DeleteLabelValues(lvs...)
	HasLeader.DeleteLabelValues(lvs...)
	TLSEnabled.DeleteLabelValues(lvs...)
	LeaderChangesTotal.DeleteLabelValues(lvs...)
	ReconcileErrorsTotal.DeleteLabelValues(lvs...)
	ReconcileDurationSeconds.DeleteLabelValues(lvs...)
	tracker.forget(namespace, name)
}

// hasQuorum reports whether healthy members constitute a strict majority of the
// registered members. With zero members there is no quorum.
func hasQuorum(members, healthy int) bool {
	if members <= 0 {
		return false
	}
	return healthy >= (members/2)+1
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
