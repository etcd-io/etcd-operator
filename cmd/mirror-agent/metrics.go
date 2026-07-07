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

package main

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

// metricPrefix is the agent metric namespace (the operator's controller
// metrics live elsewhere; the agent is not a controller-runtime manager).
const metricPrefix = "etcd_mirror_agent_"

var (
	allPhases = []mirroragent.Phase{
		mirroragent.PhaseConnecting, mirroragent.PhaseInitialSync,
		mirroragent.PhaseSyncing, mirroragent.PhaseDegraded,
		mirroragent.PhaseFailed, mirroragent.PhaseDrained,
	}
	allClasses = []mirroragent.Class{
		mirroragent.ClassTransient, mirroragent.ClassThrottle,
		mirroragent.ClassResync, mirroragent.ClassQuota,
		mirroragent.ClassPermanent,
	}
)

func newDesc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(metricPrefix+name, help, labels, nil)
}

var (
	descPhase = newDesc("phase",
		"One-hot agent phase (1 on the current phase).", "phase")
	descWatermark = newDesc("watermark_revision",
		"Checkpoint watermark: the source revision through which the target is caught up.")
	descSourceRevision = newDesc("source_revision",
		"Source cluster revision as of the last watch header.")
	descLag = newDesc("lag_revisions",
		"max(0, source_revision - watermark). Overstates lag for prefix-scoped mirrors "+
			"(the source revision is cluster-global). Absent until the source revision is known.")
	descSinceProgress = newDesc("seconds_since_last_progress",
		"Seconds since the watermark last advanced. Absent until first progress.")
	descKeysApplied = newDesc("keys_applied_total",
		"Data operations (puts and deletes) committed to the target in fenced Txns.")
	descInitialSyncKeys = newDesc("initial_sync_keys",
		"Keys copied so far by the genesis scan.")
	descInitialSyncExpected = newDesc("initial_sync_expected_keys",
		"Expected key total for the genesis scan (the progress denominator).")
	descLeaseBackedKeys = newDesc("lease_backed_keys",
		"Mirrored keys whose source copy is lease-backed.")
	descForcedResync = newDesc("forced_resync_total",
		"Forced resyncs by trigger reason.", "reason")
	descScanRestart = newDesc("scan_restart_total",
		"Genesis scan attempts aborted and restarted from a fresh R0 "+
			"(cause in /statusz lastScanRestartCause).")
	descResyncLoop = newDesc("resync_loop_detected",
		"1 while the resync-loop livelock detector is latched.")
	descThrottled = newDesc("throttled",
		"1 while the target is rejecting the write rate (backoff engaged).")
	descQuotaExhausted = newDesc("quota_exhausted",
		"1 while the target reports NOSPACE (parked on the quota probe).")
	descCompacted = newDesc("compacted",
		"1 while recovering from source compaction outrunning the watch.")
	descCutoverReady = newDesc("cutover_ready",
		"1 once the drain completed and the fence role is Primary.")
	descLastErrorClass = newDesc("last_error_class",
		"One-hot class of the most recent failure (all 0 when the last attempt succeeded).",
		"class")
	descSourceKeys = newDesc("source_keys",
		"In-scope source key count from the most recent reconciliation/verification pass.")
	descTargetKeys = newDesc("target_keys",
		"In-scope target key count from the most recent reconciliation/verification pass.")
	descDriftMissing = newDesc("drift_missing_keys",
		"Keys missing on the target per the last full reconciliation diff.")
	descDriftDivergent = newDesc("drift_divergent_keys",
		"Keys with divergent values per the last full reconciliation diff.")
	descDriftOrphan = newDesc("drift_orphan_keys",
		"Target keys with no source counterpart per the last full reconciliation diff.")
	descLastReconcile = newDesc("last_reconcile_timestamp_seconds",
		"Unix time the most recent reconciliation/verification pass completed.")
)

var allDescs = []*prometheus.Desc{
	descPhase, descWatermark, descSourceRevision, descLag, descSinceProgress,
	descKeysApplied, descInitialSyncKeys, descInitialSyncExpected,
	descLeaseBackedKeys, descForcedResync, descScanRestart, descResyncLoop,
	descThrottled, descQuotaExhausted, descCompacted, descCutoverReady,
	descLastErrorClass, descSourceKeys, descTargetKeys, descDriftMissing,
	descDriftDivergent, descDriftOrphan, descLastReconcile,
}

// snapshotCollector adapts Agent.Snapshot to prometheus.Collector: every
// Collect reads one fresh snapshot and emits const metrics — no ticker, no
// staleness window.
type snapshotCollector struct {
	snapshot snapshotFn
	now      func() time.Time
}

func (c *snapshotCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range allDescs {
		ch <- d
	}
}

func (c *snapshotCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.snapshot()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}
	for _, p := range allPhases {
		gauge(descPhase, boolGauge(s.Phase == p), string(p))
	}
	gauge(descWatermark, float64(s.Watermark))
	gauge(descSourceRevision, float64(s.SourceRevision))
	if s.SourceRevision > 0 {
		gauge(descLag, float64(max(s.SourceRevision-s.Watermark, 0)))
	}
	if !s.LastProgressTime.IsZero() {
		gauge(descSinceProgress, c.now().Sub(s.LastProgressTime).Seconds())
	}
	counter(descKeysApplied, float64(s.KeysAppliedTotal))
	gauge(descInitialSyncKeys, float64(s.InitialSyncKeyCount))
	gauge(descInitialSyncExpected, float64(s.InitialSyncTotalKeyCount))
	gauge(descLeaseBackedKeys, float64(s.LeaseBackedKeyCount))
	for reason, n := range s.ForcedResyncCountByReason {
		counter(descForcedResync, float64(n), string(reason))
	}
	counter(descScanRestart, float64(s.ScanRestartCount))
	gauge(descResyncLoop, boolGauge(s.ResyncLoopDetected))
	gauge(descThrottled, boolGauge(s.Throttled))
	gauge(descQuotaExhausted, boolGauge(s.QuotaExhausted))
	gauge(descCompacted, boolGauge(s.Compacted))
	gauge(descCutoverReady, boolGauge(s.CutoverReady))
	for _, class := range allClasses {
		gauge(descLastErrorClass, boolGauge(s.LastErrorClass == class), string(class))
	}
	gauge(descSourceKeys, float64(s.SourceKeyCount))
	gauge(descTargetKeys, float64(s.TargetKeyCount))
	if d := s.LastReconcileDrift; d != nil {
		gauge(descDriftMissing, float64(d.MissingKeys))
		gauge(descDriftDivergent, float64(d.DivergentKeys))
		gauge(descDriftOrphan, float64(d.OrphanKeys))
	}
	if !s.LastReconcileTime.IsZero() {
		gauge(descLastReconcile, float64(s.LastReconcileTime.Unix()))
	}
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// newRegistry builds the agent's standalone Prometheus registry: process/Go
// runtime collectors, the snapshot collector, and the cert-expiry gauges.
// Deliberately NOT controller-runtime's global registry — that one is
// manager-coupled and the agent is not a manager.
func newRegistry(snapshot snapshotFn, certExpiry *prometheus.GaugeVec) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		&snapshotCollector{snapshot: snapshot, now: time.Now},
		certExpiry,
	)
	return reg
}
