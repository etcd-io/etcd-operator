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

import "time"

// backoff produces class-specific retry delays. Connection-class errors get
// a standard exponential curve within [initial, max]; throttling-class
// errors get a more conservative curve derived from the same bounds (start
// 4x higher, cap 2x higher) — the target asked us to slow down, so we slow
// down harder and longer. Quota and permanent classes never route here.
//
// Retry ownership: clientv3 auto-retries Get only on codes.Unavailable;
// Txn/Put/Delete are write-at-most-once (client-retried only when no
// connection was ever established). The engine owns 100% of write-path
// retry/backoff, driven by [Classify] and these curves. A fenced-Txn retry
// after an ambiguous timeout must re-read the fence first — the Txn's own
// success bumped the fence ModRevision.
type backoff struct {
	initial time.Duration
	max     time.Duration

	transientNext time.Duration
	throttleNext  time.Duration
	lastThrottle  time.Time
}

func newBackoff(initial, maxDelay time.Duration) *backoff {
	return &backoff{initial: initial, max: maxDelay}
}

// next returns the delay before the next attempt for the given class and
// advances that class's curve. Classes without a backoff policy (resync,
// quota, permanent) fall back to the transient curve — callers are expected
// to handle them before consulting backoff.
func (b *backoff) next(c Class) time.Duration {
	if c == ClassThrottle {
		b.lastThrottle = time.Now()
		if b.throttleNext == 0 {
			b.throttleNext = minDuration(4*b.initial, 2*b.max)
		}
		d := b.throttleNext
		b.throttleNext = minDuration(2*b.throttleNext, 2*b.max)
		return d
	}
	if b.transientNext == 0 {
		b.transientNext = b.initial
	}
	d := b.transientNext
	b.transientNext = minDuration(2*b.transientNext, b.max)
	return d
}

// reset clears both curves unconditionally.
func (b *backoff) reset() {
	b.transientNext = 0
	b.throttleNext = 0
}

// noteSuccess resets the transient curve immediately but the throttle curve
// only after a full max-delay interval without throttle errors: a target
// still intermittently rejecting the write rate must keep escalating instead
// of restarting from the floor after every successful batch.
func (b *backoff) noteSuccess() {
	b.transientNext = 0
	if b.throttleNext != 0 && time.Since(b.lastThrottle) >= b.max {
		b.throttleNext = 0
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
