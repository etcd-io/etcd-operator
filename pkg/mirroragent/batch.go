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

// kvOp is one target-side write. Keys are already rewritten; srcKey is
// retained for scan-cursor bookkeeping only.
type kvOp struct {
	key      string
	value    string
	isDelete bool
	srcKey   string
}

func (o kvOp) bytes() int64 { return int64(len(o.key) + len(o.value)) }

// revGroup is the complete set of in-scope ops of ONE source revision — the
// atom the batcher never splits. Scan pages use synthetic groups (one key
// per group, rev = the scan base) since a snapshot has no per-key revision
// boundaries to preserve.
type revGroup struct {
	rev int64
	ops []kvOp
}

func (g revGroup) bytes() int64 {
	var n int64
	for _, o := range g.ops {
		n += o.bytes()
	}
	return n
}

// flushSet is one target Txn's worth of ops plus the checkpoint metadata
// that rides in the same Txn's reserved op slot.
type flushSet struct {
	ops []kvOp
	// groups preserves the revision boundaries inside ops, so a set the
	// target rejects for its Txn limits can be re-committed at revision
	// granularity (one shrink attempt) instead of failing permanently.
	groups []revGroup
	// watermark is the last complete source revision in the set (or the
	// scan base for scan flushes).
	watermark int64
	// lastSrcKey is the last source key in the set, for the scan cursor.
	lastSrcKey string
	// oversized marks a set that alone exceeds maxOps or maxBytes: a single
	// source revision applied as one oversized Txn (the checkpoint is held
	// until it lands — it rides in the same Txn). If the target rejects it,
	// the set is irreducible: the error is permanent and the offending key
	// is surfaced.
	oversized bool
}

// batcher coalesces whole revision groups into flush sets, flushing ONLY at
// source-revision boundaries. maxOps already has the checkpoint's reserved
// op slot subtracted (MaxTxnOps - 1); maxBytes is the TxnFlushBytes
// watermark.
type batcher struct {
	maxOps   int
	maxBytes int64

	pending      []revGroup
	pendingKeys  map[string]struct{}
	pendingOps   int
	pendingBytes int64
}

func newBatcher(maxTxnOps int, txnFlushBytes int64) *batcher {
	return &batcher{
		// One op slot is always reserved for the checkpoint write.
		maxOps:   maxTxnOps - 1,
		maxBytes: txnFlushBytes,
	}
}

// add appends one whole revision group and returns any flush sets that
// became due. A group is never split: if appending it would overflow the
// pending set, the pending set is flushed first; a group that alone exceeds
// the limits becomes its own oversized flush set.
func (b *batcher) add(g revGroup) []flushSet {
	if len(g.ops) == 0 {
		return nil
	}
	var out []flushSet
	gBytes := g.bytes()

	// Flush what's pending if this group doesn't fit on top of it, or if it
	// touches a key already pending: etcd rejects duplicate keys within one
	// Txn (a catch-up watch response batches up to 1000 revisions, so one
	// key modified twice in the window would otherwise put two ops on the
	// same key into one flush set — a deterministic permanent failure).
	if len(b.pending) > 0 &&
		(b.pendingOps+len(g.ops) > b.maxOps || b.pendingBytes+gBytes > b.maxBytes ||
			b.overlapsPending(g)) {
		if fs := b.flush(); fs != nil {
			out = append(out, *fs)
		}
	}

	b.pending = append(b.pending, g)
	if b.pendingKeys == nil {
		b.pendingKeys = make(map[string]struct{}, len(g.ops))
	}
	for _, op := range g.ops {
		b.pendingKeys[op.key] = struct{}{}
	}
	b.pendingOps += len(g.ops)
	b.pendingBytes += gBytes

	// Flush immediately once the watermarks are reached — including the
	// oversized single-group case.
	if b.pendingOps >= b.maxOps || b.pendingBytes >= b.maxBytes {
		if fs := b.flush(); fs != nil {
			out = append(out, *fs)
		}
	}
	return out
}

// flush drains whatever is pending into one flush set (nil when empty).
// Called by add at watermarks and by the apply loop at the end of each
// watch response / scan page so writes are never held waiting for more
// input.
func (b *batcher) flush() *flushSet {
	if len(b.pending) == 0 {
		return nil
	}
	fs := flushSet{
		groups:    b.pending,
		watermark: b.pending[len(b.pending)-1].rev,
		oversized: b.pendingOps > b.maxOps || b.pendingBytes > b.maxBytes,
	}
	fs.ops = make([]kvOp, 0, b.pendingOps)
	for _, g := range b.pending {
		fs.ops = append(fs.ops, g.ops...)
	}
	fs.lastSrcKey = fs.ops[len(fs.ops)-1].srcKey
	b.pending = nil
	b.pendingKeys = nil
	b.pendingOps = 0
	b.pendingBytes = 0
	return &fs
}

// overlapsPending reports whether any of g's keys is already pending.
func (b *batcher) overlapsPending(g revGroup) bool {
	for _, op := range g.ops {
		if _, ok := b.pendingKeys[op.key]; ok {
			return true
		}
	}
	return false
}

// groupByRevision converts an ordered event stream (already rewritten and
// filtered) into revision groups, preserving order. Events of one revision
// are always contiguous in an etcd watch stream.
func groupByRevision(ops []kvOp, revs []int64) []revGroup {
	if len(ops) != len(revs) || len(ops) == 0 {
		return nil
	}
	var groups []revGroup
	cur := revGroup{rev: revs[0]}
	for i, op := range ops {
		if revs[i] != cur.rev {
			groups = append(groups, cur)
			cur = revGroup{rev: revs[i]}
		}
		cur.ops = append(cur.ops, op)
	}
	groups = append(groups, cur)
	return groups
}
