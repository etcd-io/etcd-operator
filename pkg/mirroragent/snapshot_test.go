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

package mirroragent

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestAgent(t *testing.T) *Agent {
	t.Helper()
	a, err := New(Config{LinkUID: "link", Epoch: 1}, nil, nil)
	require.NoError(t, err)
	return a
}

func TestSnapshotForcedResyncCountByReason(t *testing.T) {
	a := newTestAgent(t)
	a.noteResync(&ResyncError{Reason: ResyncReasonCompacted, Cause: errors.New("x")})
	a.noteResync(&ResyncError{Reason: ResyncReasonClusterIDMismatch, Cause: errors.New("y")})
	a.noteResync(errors.New("bare errors default to Compacted"))

	s := a.Snapshot()
	require.Equal(t, int64(3), s.ForcedResyncCount)
	require.Equal(t, map[ResyncReason]int64{
		ResyncReasonCompacted:         2,
		ResyncReasonClusterIDMismatch: 1,
	}, s.ForcedResyncCountByReason)
	require.Equal(t, ResyncReasonCompacted, s.LastResyncReason)
}

func TestSnapshotDeepCopiesReasonMap(t *testing.T) {
	a := newTestAgent(t)
	a.noteResync(&ResyncError{Reason: ResyncReasonCompacted, Cause: errors.New("x")})
	a.update(func(s *Snapshot) {
		s.Cutover = &CutoverStatus{DrainTargetRevision: 7}
		s.LastReconcileDrift = &Drift{MissingKeys: 1}
	})

	got := a.Snapshot()
	got.ForcedResyncCountByReason[ResyncReasonClusterIDMismatch] = 99
	got.Cutover.DrainTargetRevision = 99
	got.LastReconcileDrift.MissingKeys = 99

	fresh := a.Snapshot()
	require.Equal(t, map[ResyncReason]int64{ResyncReasonCompacted: 1}, fresh.ForcedResyncCountByReason)
	require.Equal(t, int64(7), fresh.Cutover.DrainTargetRevision)
	require.Equal(t, int64(1), fresh.LastReconcileDrift.MissingKeys)
}

// TestSnapshotJSONWireNames pins the /statusz wire contract: the exact
// lowerCamel key set, zero times absent (omitzero), nil pointers absent.
func TestSnapshotJSONWireNames(t *testing.T) {
	now := time.Now()
	full := Snapshot{
		Phase:                     PhaseSyncing,
		SourceVersion:             "3.5.9",
		TargetVersion:             "3.6.0",
		SourceClusterID:           1,
		TargetClusterID:           2,
		Watermark:                 3,
		SourceRevision:            4,
		LastProgressTime:          now,
		InitialSyncKeyCount:       5,
		InitialSyncTotalKeyCount:  6,
		InitialSyncStartTime:      now,
		InitialSyncCompletionTime: now,
		KeysAppliedTotal:          7,
		LeaseBackedKeyCount:       8,
		ForcedResyncCount:         9,
		ForcedResyncCountByReason: map[ResyncReason]int64{ResyncReasonCompacted: 9},
		LastResyncReason:          ResyncReasonCompacted,
		ResyncLoopDetected:        true,
		ScanRestartCount:          10,
		LastScanRestartCause:      ScanRestartWatchBufferOverflow,
		SourceKeyCount:            11,
		TargetKeyCount:            12,
		Throttled:                 true,
		QuotaExhausted:            true,
		Compacted:                 true,
		LastReconcileTime:         now,
		LastReconcileDrift:        &Drift{MissingKeys: 1, DivergentKeys: 2, OrphanKeys: 3, Repaired: true},
		LastError:                 "boom",
		LastErrorClass:            ClassTransient,
		CutoverReady:              true,
		Cutover: &CutoverStatus{
			DrainTargetRevision: 1, DrainedRevision: 2, VerifiedTime: now,
			SourceKeyCount: 3, TargetKeyCount: 4, LeasedKeyCount: 5,
		},
	}

	keys := func(v any) map[string]any {
		t.Helper()
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		var m map[string]any
		require.NoError(t, json.Unmarshal(raw, &m))
		return m
	}

	fullKeys := keys(full)
	// Cluster IDs are JSON strings: as uint64s beyond 2^53 they would round
	// through float64-based consumers (jq, JavaScript) as plain numbers.
	require.Equal(t, "1", fullKeys["sourceClusterID"])
	require.Equal(t, "2", fullKeys["targetClusterID"])
	wantFull := []string{
		"phase", "sourceVersion", "targetVersion", "sourceClusterID", "targetClusterID",
		"watermark", "sourceRevision", "lastProgressTime",
		"initialSyncKeyCount", "initialSyncTotalKeyCount", "initialSyncStartTime",
		"initialSyncCompletionTime", "keysAppliedTotal", "leaseBackedKeyCount",
		"forcedResyncCount", "forcedResyncCountByReason", "lastResyncReason",
		"resyncLoopDetected", "scanRestartCount", "lastScanRestartCause",
		"sourceKeyCount", "targetKeyCount", "throttled", "quotaExhausted", "compacted",
		"lastReconcileTime", "lastReconcileDrift", "lastError", "lastErrorClass",
		"cutoverReady", "cutover",
	}
	require.ElementsMatch(t, wantFull, mapKeys(fullKeys))
	require.ElementsMatch(t,
		[]string{"missingKeys", "divergentKeys", "orphanKeys", "repaired"},
		mapKeys(fullKeys["lastReconcileDrift"].(map[string]any)))
	require.ElementsMatch(t,
		[]string{"drainTargetRevision", "drainedRevision", "verifiedTime",
			"sourceKeyCount", "targetKeyCount", "leasedKeyCount"},
		mapKeys(fullKeys["cutover"].(map[string]any)))

	// Zero value: zero times, the nil map, and nil pointers are absent.
	zeroKeys := keys(Snapshot{})
	for _, absent := range []string{
		"lastProgressTime", "initialSyncStartTime", "initialSyncCompletionTime",
		"lastReconcileTime", "forcedResyncCountByReason", "lastReconcileDrift", "cutover",
	} {
		require.NotContains(t, zeroKeys, absent)
	}
	require.Contains(t, zeroKeys, "phase")

	// omitzero on CutoverStatus.VerifiedTime.
	cutKeys := keys(CutoverStatus{})
	require.NotContains(t, cutKeys, "verifiedTime")
}

func mapKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
