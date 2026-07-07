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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestReconcileDue: the periodic pass's pure gating logic — disabled
// interval, unarmed deadline, a not-yet-due deadline, and a requested drain
// (whose own verification pass supersedes the periodic one) all veto it.
func TestReconcileDue(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		make func() *Agent
		want bool
	}{
		{name: "due", make: func() *Agent {
			a := &Agent{cfg: Config{ReconcileInterval: time.Minute, Mode: ModeSync}}
			a.nextReconcile = now.Add(-time.Second)
			return a
		}, want: true},
		{name: "due exactly at deadline", make: func() *Agent {
			a := &Agent{cfg: Config{ReconcileInterval: time.Minute, Mode: ModeSync}}
			a.nextReconcile = now
			return a
		}, want: true},
		{name: "disabled interval", make: func() *Agent {
			a := &Agent{cfg: Config{Mode: ModeSync}}
			a.nextReconcile = now.Add(-time.Second)
			return a
		}, want: false},
		{name: "deadline not armed", make: func() *Agent {
			return &Agent{cfg: Config{ReconcileInterval: time.Minute, Mode: ModeSync}}
		}, want: false},
		{name: "before deadline", make: func() *Agent {
			a := &Agent{cfg: Config{ReconcileInterval: time.Minute, Mode: ModeSync}}
			a.nextReconcile = now.Add(time.Second)
			return a
		}, want: false},
		{name: "drain mode", make: func() *Agent {
			a := &Agent{cfg: Config{ReconcileInterval: time.Minute, Mode: ModeDrain}}
			a.nextReconcile = now.Add(-time.Second)
			return a
		}, want: false},
		{name: "drain requested", make: func() *Agent {
			a := &Agent{cfg: Config{ReconcileInterval: time.Minute, Mode: ModeSync}}
			a.nextReconcile = now.Add(-time.Second)
			a.drainReq.Store(true)
			return a
		}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.make().reconcileDue(now))
		})
	}
}

// TestScheduleNextReconcile: a no-op when disabled; otherwise the deadline
// lands one interval out, and re-scheduling pushes it further (the
// mandatory-sweep re-arm semantics — a sweep just produced the same signal).
func TestScheduleNextReconcile(t *testing.T) {
	disabled := &Agent{cfg: Config{}}
	disabled.scheduleNextReconcile()
	assert.True(t, disabled.nextReconcile.IsZero(), "disabled scheduler must not arm a deadline")

	a := &Agent{cfg: Config{ReconcileInterval: time.Hour}}
	before := time.Now()
	a.scheduleNextReconcile()
	first := a.nextReconcile
	assert.False(t, first.Before(before.Add(time.Hour)), "deadline must be at least one interval out")
	assert.False(t, first.After(time.Now().Add(time.Hour)), "deadline must be at most one interval out")

	a.scheduleNextReconcile()
	assert.False(t, a.nextReconcile.Before(first), "re-scheduling must push the deadline out")
}
