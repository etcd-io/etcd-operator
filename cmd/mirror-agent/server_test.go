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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

// fullSnapshot fabricates a Snapshot with every field populated. Fixed UTC
// times (no monotonic clock) so JSON round-trips compare equal.
func fullSnapshot() mirroragent.Snapshot {
	at := func(min int) time.Time { return time.Date(2026, 7, 7, 12, min, 0, 0, time.UTC) }
	return mirroragent.Snapshot{
		Phase:                     mirroragent.PhaseSyncing,
		SourceVersion:             "3.5.9",
		TargetVersion:             "3.6.0",
		SourceClusterID:           11,
		TargetClusterID:           22,
		Watermark:                 90,
		SourceRevision:            100,
		LastProgressTime:          at(1),
		InitialSyncKeyCount:       40,
		InitialSyncTotalKeyCount:  50,
		InitialSyncStartTime:      at(2),
		InitialSyncCompletionTime: at(3),
		KeysAppliedTotal:          123,
		LeaseBackedKeyCount:       3,
		ForcedResyncCount:         3,
		ForcedResyncCountByReason: map[mirroragent.ResyncReason]int64{
			mirroragent.ResyncReasonCompacted:         2,
			mirroragent.ResyncReasonClusterIDMismatch: 1,
		},
		LastResyncReason:     mirroragent.ResyncReasonCompacted,
		ResyncLoopDetected:   true,
		ScanRestartCount:     4,
		LastScanRestartCause: mirroragent.ScanRestartWatchBufferOverflow,
		SourceKeyCount:       10,
		TargetKeyCount:       9,
		Throttled:            true,
		QuotaExhausted:       false,
		Compacted:            true,
		LastReconcileTime:    at(4),
		LastReconcileDrift:   &mirroragent.Drift{MissingKeys: 1, DivergentKeys: 2, OrphanKeys: 3, Repaired: true},
		LastError:            "boom",
		LastErrorClass:       mirroragent.ClassThrottle,
		CutoverReady:         true,
		Cutover: &mirroragent.CutoverStatus{
			DrainTargetRevision: 100, DrainedRevision: 100, VerifiedTime: at(5),
			SourceKeyCount: 10, TargetKeyCount: 10, LeasedKeyCount: 3,
		},
	}
}

func get(t *testing.T, mux *http.ServeMux, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestStatuszHandler(t *testing.T) {
	want := fullSnapshot()
	mux := newMux(func() mirroragent.Snapshot { return want }, prometheus.NewRegistry())

	rec := get(t, mux, "/statusz")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))

	var got mirroragent.Snapshot
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, want, got, "the wire shape round-trips into mirroragent.Snapshot losslessly")
}

func TestReadyzPhases(t *testing.T) {
	cases := []struct {
		phase mirroragent.Phase
		want  int
	}{
		{"", http.StatusServiceUnavailable},
		{mirroragent.PhaseConnecting, http.StatusServiceUnavailable},
		{mirroragent.PhaseFailed, http.StatusServiceUnavailable},
		{mirroragent.PhaseInitialSync, http.StatusOK},
		{mirroragent.PhaseSyncing, http.StatusOK},
		{mirroragent.PhaseDegraded, http.StatusOK},
		{mirroragent.PhaseDrained, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(string(tc.phase), func(t *testing.T) {
			mux := newMux(func() mirroragent.Snapshot {
				return mirroragent.Snapshot{Phase: tc.phase}
			}, prometheus.NewRegistry())
			require.Equal(t, tc.want, get(t, mux, "/readyz").Code)
		})
	}
}

func TestHealthzAlwaysOK(t *testing.T) {
	mux := newMux(func() mirroragent.Snapshot {
		return mirroragent.Snapshot{Phase: mirroragent.PhaseFailed}
	}, prometheus.NewRegistry())
	rec := get(t, mux, "/healthz")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok", rec.Body.String())
}
