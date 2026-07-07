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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

func TestSnapshotCollector(t *testing.T) {
	snap := fullSnapshot()
	c := &snapshotCollector{
		snapshot: func() mirroragent.Snapshot { return snap },
		now:      func() time.Time { return snap.LastProgressTime.Add(5 * time.Second) },
	}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)

	// One entry per family: name, type, help (must match metrics.go
	// verbatim), samples (metricPrefix is prepended to each).
	type family struct {
		name, typ, help string
		samples         []string
	}
	families := []family{
		{"phase", "gauge", "One-hot agent phase (1 on the current phase).", []string{
			`phase{phase="Connecting"} 0`,
			`phase{phase="Degraded"} 0`,
			`phase{phase="Drained"} 0`,
			`phase{phase="Failed"} 0`,
			`phase{phase="InitialSync"} 0`,
			`phase{phase="Syncing"} 1`,
		}},
		{"watermark_revision", "gauge",
			"Checkpoint watermark: the source revision through which the target is caught up.",
			[]string{"watermark_revision 90"}},
		{"source_revision", "gauge",
			"Source cluster revision as of the last watch header.",
			[]string{"source_revision 100"}},
		{"lag_revisions", "gauge",
			"max(0, source_revision - watermark). Overstates lag for prefix-scoped mirrors " +
				"(the source revision is cluster-global). Absent until the source revision is known.",
			[]string{"lag_revisions 10"}},
		{"seconds_since_last_progress", "gauge",
			"Seconds since the watermark last advanced. Absent until first progress.",
			[]string{"seconds_since_last_progress 5"}},
		{"keys_applied_total", "counter",
			"Data operations (puts and deletes) committed to the target in fenced Txns.",
			[]string{"keys_applied_total 123"}},
		{"initial_sync_keys", "gauge",
			"Keys copied so far by the genesis scan.",
			[]string{"initial_sync_keys 40"}},
		{"initial_sync_expected_keys", "gauge",
			"Expected key total for the genesis scan (the progress denominator).",
			[]string{"initial_sync_expected_keys 50"}},
		{"lease_backed_keys", "gauge",
			"Mirrored keys whose source copy is lease-backed.",
			[]string{"lease_backed_keys 3"}},
		{"forced_resync_total", "counter",
			"Forced resyncs by trigger reason.",
			[]string{
				`forced_resync_total{reason="ClusterIDMismatch"} 1`,
				`forced_resync_total{reason="Compacted"} 2`,
			}},
		{"scan_restart_total", "counter",
			"Genesis scan attempts aborted and restarted from a fresh R0 " +
				"(cause in /statusz lastScanRestartCause).",
			[]string{"scan_restart_total 4"}},
		{"resync_loop_detected", "gauge",
			"1 while the resync-loop livelock detector is latched.",
			[]string{"resync_loop_detected 1"}},
		{"throttled", "gauge",
			"1 while the target is rejecting the write rate (backoff engaged).",
			[]string{"throttled 1"}},
		{"quota_exhausted", "gauge",
			"1 while the target reports NOSPACE (parked on the quota probe).",
			[]string{"quota_exhausted 0"}},
		{"compacted", "gauge",
			"1 while recovering from source compaction outrunning the watch.",
			[]string{"compacted 1"}},
		{"cutover_ready", "gauge",
			"1 once the drain completed and the fence role is Primary.",
			[]string{"cutover_ready 1"}},
		{"last_error_class", "gauge",
			"One-hot class of the most recent failure (all 0 when the last attempt succeeded).",
			[]string{
				`last_error_class{class="Permanent"} 0`,
				`last_error_class{class="Quota"} 0`,
				`last_error_class{class="Resync"} 0`,
				`last_error_class{class="Throttle"} 1`,
				`last_error_class{class="Transient"} 0`,
			}},
		{"source_keys", "gauge",
			"In-scope source key count from the most recent reconciliation/verification pass.",
			[]string{"source_keys 10"}},
		{"target_keys", "gauge",
			"In-scope target key count from the most recent reconciliation/verification pass.",
			[]string{"target_keys 9"}},
		{"drift_missing_keys", "gauge",
			"Keys missing on the target per the last full reconciliation diff.",
			[]string{"drift_missing_keys 1"}},
		{"drift_divergent_keys", "gauge",
			"Keys with divergent values per the last full reconciliation diff.",
			[]string{"drift_divergent_keys 2"}},
		{"drift_orphan_keys", "gauge",
			"Target keys with no source counterpart per the last full reconciliation diff.",
			[]string{"drift_orphan_keys 3"}},
		{"last_reconcile_timestamp_seconds", "gauge",
			"Unix time the most recent reconciliation/verification pass completed.",
			[]string{fmt.Sprintf("last_reconcile_timestamp_seconds %d", snap.LastReconcileTime.Unix())}},
	}
	var expected strings.Builder
	for _, f := range families {
		fmt.Fprintf(&expected, "# HELP %[1]s%[2]s %[3]s\n# TYPE %[1]s%[2]s %[4]s\n",
			metricPrefix, f.name, f.help, f.typ)
		for _, s := range f.samples {
			expected.WriteString(metricPrefix + s + "\n")
		}
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected.String())))
}

// TestSnapshotCollectorConditionalAbsence pins which families are omitted
// while their inputs are unknown: lag before the source revision is probed,
// progress age before first progress, drift gauges before a full diff, and
// the reconcile timestamp before any pass.
func TestSnapshotCollectorConditionalAbsence(t *testing.T) {
	c := &snapshotCollector{
		snapshot: func() mirroragent.Snapshot { return mirroragent.Snapshot{} },
		now:      time.Now,
	}
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(""),
		metricPrefix+"lag_revisions",
		metricPrefix+"seconds_since_last_progress",
		metricPrefix+"drift_missing_keys",
		metricPrefix+"drift_divergent_keys",
		metricPrefix+"drift_orphan_keys",
		metricPrefix+"last_reconcile_timestamp_seconds",
	))
	// The one-hots are always fully emitted, even before Run sets a phase.
	require.Equal(t, 6, testutil.CollectAndCount(c, metricPrefix+"phase"))
	require.Equal(t, 5, testutil.CollectAndCount(c, metricPrefix+"last_error_class"))
	// forced_resync_total is born at 0 for every known reason so increase()
	// can see the first resync (a counter's first sample yields no delta).
	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(fmt.Sprintf(`
# HELP %[1]sforced_resync_total Forced resyncs by trigger reason.
# TYPE %[1]sforced_resync_total counter
%[1]sforced_resync_total{reason="ClusterIDMismatch"} 0
%[1]sforced_resync_total{reason="Compacted"} 0
`, metricPrefix)), metricPrefix+"forced_resync_total"))
}
