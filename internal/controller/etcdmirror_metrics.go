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

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

const (
	metricLabelNamespace = "namespace"
	metricLabelName      = "name"
)

// Controller-side gauges mirroring EtcdMirror status. They exist so an agent
// that never scheduled (image typo, unschedulable pod) still produces series
// to alert on — the etcd_mirror_agent_* family only exists once the agent pod
// runs and is scraped. Served by the manager's /metrics endpoint.
var (
	// allMirrorPhases is the full one-hot domain of etcdMirrorPhaseGauge.
	// Pre-emitting every phase at 0/1 means a phase flip never leaves a stale
	// 1 series and `== 1` alert expressions are absence-safe once the CR has
	// reconciled once. A CR with an empty status.phase emits all zeros.
	allMirrorPhases = []ecv1alpha1.EtcdMirrorPhase{
		ecv1alpha1.EtcdMirrorPhasePending,
		ecv1alpha1.EtcdMirrorPhaseConnecting,
		ecv1alpha1.EtcdMirrorPhaseInitialSync,
		ecv1alpha1.EtcdMirrorPhaseSyncing,
		ecv1alpha1.EtcdMirrorPhaseDegraded,
		ecv1alpha1.EtcdMirrorPhasePaused,
		ecv1alpha1.EtcdMirrorPhaseFailed,
	}
	// conditionStatusValues one-hots each condition across the three
	// metav1.ConditionStatus values (lowercase, kube-state-metrics
	// convention), so `status="true"} == 0` distinguishes False/Unknown from
	// series absence.
	conditionStatusValues = []string{"true", "false", "unknown"}

	etcdMirrorPhaseGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "etcd_mirror_phase",
		Help: "One-hot mirror of EtcdMirror status.phase (1 on the current phase). " +
			"Controller-side: present even when the agent pod never scheduled.",
	}, []string{metricLabelNamespace, metricLabelName, "phase"})

	etcdMirrorConditionGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "etcd_mirror_condition",
		Help: "One-hot mirror of EtcdMirror status.conditions per type and status. " +
			"Controller-side: updated on every reconcile, including failed agent /statusz polls.",
	}, []string{metricLabelNamespace, metricLabelName, "type", "status"})
)

func init() {
	metrics.Registry.MustRegister(etcdMirrorPhaseGauge, etcdMirrorConditionGauge)
}

// updateEtcdMirrorMetrics re-emits both gauges from em's current status.
// Conditions iterate what is actually present (types are never removed from
// status once set), so future condition types are covered automatically.
func updateEtcdMirrorMetrics(em *ecv1alpha1.EtcdMirror) {
	for _, p := range allMirrorPhases {
		var v float64
		if em.Status.Phase == p {
			v = 1
		}
		etcdMirrorPhaseGauge.WithLabelValues(em.Namespace, em.Name, string(p)).Set(v)
	}
	for _, c := range em.Status.Conditions {
		current := strings.ToLower(string(c.Status))
		for _, s := range conditionStatusValues {
			var v float64
			if s == current {
				v = 1
			}
			etcdMirrorConditionGauge.WithLabelValues(em.Namespace, em.Name, c.Type, s).Set(v)
		}
	}
}

// deleteEtcdMirrorMetrics drops every series for one CR — called at CR
// deletion so no stale series outlive it.
func deleteEtcdMirrorMetrics(namespace, name string) {
	labels := prometheus.Labels{metricLabelNamespace: namespace, metricLabelName: name}
	etcdMirrorPhaseGauge.DeletePartialMatch(labels)
	etcdMirrorConditionGauge.DeletePartialMatch(labels)
}
