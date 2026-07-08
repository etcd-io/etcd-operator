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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

func phaseValue(em *ecv1alpha1.EtcdMirror, phase ecv1alpha1.EtcdMirrorPhase) float64 {
	return testutil.ToFloat64(etcdMirrorPhaseGauge.WithLabelValues(em.Namespace, em.Name, string(phase)))
}

func conditionValue(em *ecv1alpha1.EtcdMirror, condType, status string) float64 {
	return testutil.ToFloat64(etcdMirrorConditionGauge.WithLabelValues(em.Namespace, em.Name, condType, status))
}

// mirrorSeriesCount counts series of one metric carrying this CR's
// namespace/name labels, straight from the manager registry (the registry is
// process-global and other tests emit series for other CRs).
func mirrorSeriesCount(t *testing.T, metricName, namespace, name string) int {
	t.Helper()
	mfs, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)
	count := 0
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			var nsOK, nameOK bool
			for _, lp := range m.GetLabel() {
				nsOK = nsOK || (lp.GetName() == "namespace" && lp.GetValue() == namespace)
				nameOK = nameOK || (lp.GetName() == "name" && lp.GetValue() == name)
			}
			if nsOK && nameOK {
				count++
			}
		}
	}
	return count
}

func TestEtcdMirrorMetrics_Lifecycle(t *testing.T) {
	em := &ecv1alpha1.EtcdMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "metrics-unit-ns", Name: "unit-mirror"},
	}
	em.Status.Phase = ecv1alpha1.EtcdMirrorPhaseSyncing
	em.Status.Conditions = []metav1.Condition{
		{Type: ecv1alpha1.EtcdMirrorConditionAvailable, Status: metav1.ConditionTrue, Reason: "x"},
		{Type: ecv1alpha1.EtcdMirrorConditionCompacted, Status: metav1.ConditionFalse, Reason: "x"},
	}
	updateEtcdMirrorMetrics(em)

	// Phase is one-hot across the full domain.
	assert.Equal(t, 1.0, phaseValue(em, ecv1alpha1.EtcdMirrorPhaseSyncing))
	for _, p := range allMirrorPhases {
		if p != ecv1alpha1.EtcdMirrorPhaseSyncing {
			assert.Zero(t, phaseValue(em, p), "phase %s must be 0", p)
		}
	}
	// Each condition is one-hot across true/false/unknown.
	assert.Equal(t, 1.0, conditionValue(em, ecv1alpha1.EtcdMirrorConditionAvailable, "true"))
	assert.Zero(t, conditionValue(em, ecv1alpha1.EtcdMirrorConditionAvailable, "false"))
	assert.Zero(t, conditionValue(em, ecv1alpha1.EtcdMirrorConditionAvailable, "unknown"))
	assert.Equal(t, 1.0, conditionValue(em, ecv1alpha1.EtcdMirrorConditionCompacted, "false"))
	assert.Zero(t, conditionValue(em, ecv1alpha1.EtcdMirrorConditionCompacted, "true"))

	// Phase flip leaves no stale 1s.
	em.Status.Phase = ecv1alpha1.EtcdMirrorPhaseDegraded
	em.Status.Conditions[0].Status = metav1.ConditionUnknown
	updateEtcdMirrorMetrics(em)
	assert.Zero(t, phaseValue(em, ecv1alpha1.EtcdMirrorPhaseSyncing))
	assert.Equal(t, 1.0, phaseValue(em, ecv1alpha1.EtcdMirrorPhaseDegraded))
	assert.Zero(t, conditionValue(em, ecv1alpha1.EtcdMirrorConditionAvailable, "true"))
	assert.Equal(t, 1.0, conditionValue(em, ecv1alpha1.EtcdMirrorConditionAvailable, "unknown"))

	require.Equal(t, len(allMirrorPhases), mirrorSeriesCount(t, "etcd_mirror_phase", em.Namespace, em.Name))
	require.Equal(t, 2*len(conditionStatusValues), mirrorSeriesCount(t, "etcd_mirror_condition", em.Namespace, em.Name))

	// Deletion drops every series for the CR.
	deleteEtcdMirrorMetrics(em.Namespace, em.Name)
	assert.Zero(t, mirrorSeriesCount(t, "etcd_mirror_phase", em.Namespace, em.Name))
	assert.Zero(t, mirrorSeriesCount(t, "etcd_mirror_condition", em.Namespace, em.Name))
}

func TestEtcdMirrorMetrics_EmptyPhaseAllZeros(t *testing.T) {
	em := &ecv1alpha1.EtcdMirror{
		ObjectMeta: metav1.ObjectMeta{Namespace: "metrics-unit-ns", Name: "unreconciled-mirror"},
	}
	updateEtcdMirrorMetrics(em)
	for _, p := range allMirrorPhases {
		assert.Zero(t, phaseValue(em, p))
	}
	assert.Zero(t, mirrorSeriesCount(t, "etcd_mirror_condition", em.Namespace, em.Name))
	deleteEtcdMirrorMetrics(em.Namespace, em.Name)
}

// Registration sanity: both vecs are on the manager registry (linting the
// init() wiring, not prometheus itself).
func TestEtcdMirrorMetrics_Registered(t *testing.T) {
	for _, vec := range []*prometheus.GaugeVec{etcdMirrorPhaseGauge, etcdMirrorConditionGauge} {
		err := ctrlmetrics.Registry.Register(vec)
		var are prometheus.AlreadyRegisteredError
		require.ErrorAs(t, err, &are)
	}
}
