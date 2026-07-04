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
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// rewriter implements the mirror's single key-rewrite formula:
//
//	key' = target.prefix + destPrefix + TrimPrefix(key, source.prefix)
//
// It is an anchored strip-and-reprefix — never a substring replace — and it
// is order-preserving within the source scope, which the prune pass's merge
// scan relies on.
type rewriter struct {
	srcPrefix string
	// dstPrefix is the effective destination prefix (target.prefix +
	// destPrefix).
	dstPrefix string
	exclude   []string
	// reservedKey is never produced by any apply path, even if a source key
	// happens to map onto it.
	reservedKey string
}

func newRewriter(cfg Config) *rewriter {
	return &rewriter{
		srcPrefix:   cfg.SourcePrefix,
		dstPrefix:   cfg.EffectiveDestPrefix(),
		exclude:     cfg.ExcludePrefixes,
		reservedKey: cfg.CheckpointKey,
	}
}

// inScope reports whether a source key is mirrored: under the source prefix
// and not excluded.
func (r *rewriter) inScope(srcKey string) bool {
	if !strings.HasPrefix(srcKey, r.srcPrefix) {
		return false
	}
	return !r.excluded(srcKey)
}

// excluded reports whether a source key falls under any exclude prefix.
func (r *rewriter) excluded(srcKey string) bool {
	for _, p := range r.exclude {
		if strings.HasPrefix(srcKey, p) {
			return true
		}
	}
	return false
}

// rewrite maps an in-scope source key to its target key. ok is false when
// the key is out of scope, excluded, or would collide with the reserved
// checkpoint key.
func (r *rewriter) rewrite(srcKey string) (string, bool) {
	if !r.inScope(srcKey) {
		return "", false
	}
	dst := r.dstPrefix + strings.TrimPrefix(srcKey, r.srcPrefix)
	if dst == r.reservedKey {
		return "", false
	}
	return dst, true
}

// image maps ANY source key under the source prefix (including excluded
// ones) to its would-be target key, for cursor/window arithmetic in the
// merge scan. Callers must ensure srcKey has the source prefix.
func (r *rewriter) image(srcKey string) string {
	return r.dstPrefix + strings.TrimPrefix(srcKey, r.srcPrefix)
}

// excludedImage reports whether a TARGET key falls under the image of an
// exclude prefix: such keys are never treated as orphans by prune passes.
func (r *rewriter) excludedImage(dstKey string) bool {
	for _, p := range r.exclude {
		if !strings.HasPrefix(p, r.srcPrefix) {
			continue // excluded range is outside the mirrored scope
		}
		if strings.HasPrefix(dstKey, r.image(p)) {
			return true
		}
	}
	return false
}

// sourceRange returns the [start, end) watch/scan range for the source
// prefix. An empty prefix means the whole keyspace.
func (r *rewriter) sourceRange() (string, string) {
	return keyRange(r.srcPrefix)
}

// scanRange is one [Start, End) source read window. End == rangeEndInf is
// etcd's ">= Start" sentinel (unbounded).
type scanRange struct {
	start, end string
}

// rangeEndInf is etcd's unbounded range-end sentinel ("all keys >= start").
const rangeEndInf = "\x00"

// endAfter reports whether range end e lies strictly after key k, treating
// the sentinel as +inf.
func endAfter(e, k string) bool {
	return e == rangeEndInf || e > k
}

// scanRanges returns the source range minus every excluded range: the
// sorted, disjoint windows the genesis scan reads. Range Gets are issued
// ONLY over these windows, so excluded data is elided server-side — never
// transferred and filtered client-side. (The watch still spans the whole
// source range: watches cannot be decomposed without multiplying streams,
// and its events are filtered through rewrite.)
func (r *rewriter) scanRanges() []scanRange {
	start, end := r.sourceRange()
	out := []scanRange{{start: start, end: end}}
	for _, p := range r.exclude {
		es, ee := keyRange(p)
		next := make([]scanRange, 0, len(out)+1)
		for _, w := range out {
			next = append(next, subtractRange(w, es, ee)...)
		}
		out = next
	}
	return out
}

// subtractRange removes the [es, ee) slice from window w, yielding 0, 1, or
// 2 remaining windows.
func subtractRange(w scanRange, es, ee string) []scanRange {
	// No overlap: the excluded range ends at/before the window starts, or
	// starts at/after the window ends.
	if !endAfter(ee, w.start) || !endAfter(w.end, es) {
		return []scanRange{w}
	}
	out := make([]scanRange, 0, 2)
	if es > w.start {
		out = append(out, scanRange{start: w.start, end: es})
	}
	if ee != rangeEndInf && endAfter(w.end, ee) {
		out = append(out, scanRange{start: ee, end: w.end})
	}
	return out
}

// destRange returns the [start, end) range covering the effective
// destination prefix on the target.
func (r *rewriter) destRange() (string, string) {
	return keyRange(r.dstPrefix)
}

// keyRange converts a prefix to an etcd [start, end) range. The empty prefix
// maps to the whole keyspace ("\x00" with the >=-key range end "\x00").
func keyRange(prefix string) (string, string) {
	if prefix == "" {
		return "\x00", "\x00"
	}
	return prefix, clientv3.GetPrefixRangeEnd(prefix)
}

// nextKey returns the smallest key strictly greater than k, for cursor
// advancement.
func nextKey(k string) string {
	return k + "\x00"
}
