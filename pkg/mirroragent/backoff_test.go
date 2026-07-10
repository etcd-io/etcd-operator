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

func TestBackoffTransientCurve(t *testing.T) {
	b := newBackoff(time.Second, 8*time.Second)
	assert.Equal(t, 1*time.Second, b.next(ClassTransient))
	assert.Equal(t, 2*time.Second, b.next(ClassTransient))
	assert.Equal(t, 4*time.Second, b.next(ClassTransient))
	assert.Equal(t, 8*time.Second, b.next(ClassTransient))
	assert.Equal(t, 8*time.Second, b.next(ClassTransient), "the transient curve caps at max")
}

// TestBackoffThrottleCurveDistinct: the target asked us to slow down, so the
// throttle curve starts higher (4x) and caps higher (2x) than the transient
// curve — and the two advance independently.
func TestBackoffThrottleCurveDistinct(t *testing.T) {
	b := newBackoff(time.Second, 8*time.Second)
	assert.Equal(t, 4*time.Second, b.next(ClassThrottle))
	assert.Equal(t, 8*time.Second, b.next(ClassThrottle))
	assert.Equal(t, 16*time.Second, b.next(ClassThrottle))
	assert.Equal(t, 16*time.Second, b.next(ClassThrottle), "the throttle curve caps at 2x max")

	assert.Equal(t, 1*time.Second, b.next(ClassTransient),
		"throttle advancement must not move the transient curve")
}

func TestBackoffThrottleStartCappedForTightBounds(t *testing.T) {
	b := newBackoff(time.Second, time.Second)
	assert.Equal(t, 2*time.Second, b.next(ClassThrottle),
		"4x initial is capped at 2x max when the bounds are tight")
}

func TestBackoffReset(t *testing.T) {
	b := newBackoff(time.Second, 8*time.Second)
	_ = b.next(ClassTransient)
	_ = b.next(ClassTransient)
	_ = b.next(ClassThrottle)
	b.reset()
	assert.Equal(t, 1*time.Second, b.next(ClassTransient), "reset restarts the transient curve")
	assert.Equal(t, 4*time.Second, b.next(ClassThrottle), "reset restarts the throttle curve")
}

// TestBackoffNoteSuccessPreservesThrottleCurve: a success right after a
// throttle delay resets only the transient curve — the throttle curve keeps
// escalating across intermittent successes and resets only after a full
// max-delay interval without throttle errors.
func TestBackoffNoteSuccessPreservesThrottleCurve(t *testing.T) {
	b := newBackoff(time.Millisecond, 20*time.Millisecond)
	_ = b.next(ClassTransient)
	_ = b.next(ClassTransient)
	first := b.next(ClassThrottle)
	b.noteSuccess()
	assert.Equal(t, time.Millisecond, b.next(ClassTransient),
		"noteSuccess restarts the transient curve immediately")
	assert.Greater(t, b.next(ClassThrottle), first,
		"the throttle curve must keep escalating across an immediate success")

	time.Sleep(25 * time.Millisecond) // > max: a genuinely healthy stretch
	b.noteSuccess()
	assert.Equal(t, 4*time.Millisecond, b.next(ClassThrottle),
		"a max-delay-long throttle-free stretch restarts the throttle curve")
}

// TestBackoffNonRetryClassesUseTransientCurve documents the contract that
// resync/quota/permanent never legitimately reach backoff: callers handle
// them first (quota parks on the flat QuotaProbeInterval instead — pinned by
// the TestTargetQuotaExhausted integration test). If one slips through, it
// falls back to the standard curve rather than spinning.
func TestBackoffNonRetryClassesUseTransientCurve(t *testing.T) {
	b := newBackoff(time.Second, 8*time.Second)
	assert.Equal(t, 1*time.Second, b.next(ClassQuota))
	assert.Equal(t, 2*time.Second, b.next(ClassPermanent))
	assert.Equal(t, 4*time.Second, b.next(ClassResync))
}
