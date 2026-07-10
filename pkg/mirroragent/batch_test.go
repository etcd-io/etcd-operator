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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mkGroup builds a revGroup of n one-byte-key/valueLen-byte-value ops.
func mkGroup(rev int64, n, valueLen int) revGroup {
	g := revGroup{rev: rev}
	for i := range n {
		g.ops = append(g.ops, kvOp{
			key:    fmt.Sprintf("k%d-%d", rev, i),
			value:  string(make([]byte, valueLen)),
			srcKey: fmt.Sprintf("s%d-%d", rev, i),
		})
	}
	return g
}

// TestBatcherReservesCheckpointSlot pins the MaxTxnOps-1 invariant: with
// MaxTxnOps=5 the batcher flushes at exactly 4 data ops — one op slot is
// always reserved for the checkpoint write riding the same Txn.
func TestBatcherReservesCheckpointSlot(t *testing.T) {
	b := newBatcher(5, 1<<20)
	var flushed []flushSet
	for rev := int64(1); rev <= 3; rev++ {
		flushed = append(flushed, b.add(mkGroup(rev, 1, 1))...)
	}
	require.Empty(t, flushed, "3 ops must still be pending below the 4-op watermark")

	flushed = b.add(mkGroup(4, 1, 1))
	require.Len(t, flushed, 1, "the 4th op hits MaxTxnOps-1 exactly and must flush")
	assert.Len(t, flushed[0].ops, 4)
	assert.False(t, flushed[0].oversized)
	assert.EqualValues(t, 4, flushed[0].watermark, "watermark is the last complete revision")
	assert.Nil(t, b.flush(), "nothing may remain pending after the watermark flush")
}

// TestBatcherNeverSplitsRevision: a revision group that does not fit on top
// of the pending set flushes the pending set first and stays whole.
func TestBatcherNeverSplitsRevision(t *testing.T) {
	b := newBatcher(6, 1<<20) // 5 data op slots
	first := b.add(mkGroup(10, 3, 1))
	require.Empty(t, first)

	// 3 pending + 3 incoming > 5: pending flushes alone, the new group pends.
	flushed := b.add(mkGroup(11, 3, 1))
	require.Len(t, flushed, 1)
	assert.Len(t, flushed[0].ops, 3)
	assert.EqualValues(t, 10, flushed[0].watermark)
	for _, op := range flushed[0].ops {
		assert.Contains(t, op.srcKey, "s10-", "revision 11 ops must not leak into revision 10's flush")
	}

	rest := b.flush()
	require.NotNil(t, rest)
	assert.Len(t, rest.ops, 3)
	assert.EqualValues(t, 11, rest.watermark)
}

// TestBatcherByteWatermark: the TxnFlushBytes boundary triggers a flush even
// when the op count is far below MaxTxnOps — the pending set flushes alone
// (at its revision boundary) when the next revision would push it past the
// byte watermark.
func TestBatcherByteWatermark(t *testing.T) {
	b := newBatcher(100, 1000)
	require.Empty(t, b.add(mkGroup(1, 1, 400)))

	flushed := b.add(mkGroup(2, 1, 700))
	require.Len(t, flushed, 1, "revision 2 does not fit on top: revision 1 must flush first")
	assert.Len(t, flushed[0].ops, 1)
	assert.EqualValues(t, 1, flushed[0].watermark)
	assert.False(t, flushed[0].oversized)

	rest := b.flush()
	require.NotNil(t, rest)
	assert.Len(t, rest.ops, 1)
	assert.EqualValues(t, 2, rest.watermark)
}

// TestBatcherOversizedSingleRevision: one revision alone above both
// watermarks becomes its own flush set, marked oversized, never split.
func TestBatcherOversizedSingleRevision(t *testing.T) {
	b := newBatcher(4, 100) // 3 data op slots
	flushed := b.add(mkGroup(7, 9, 50))
	require.Len(t, flushed, 1, "an oversized revision must flush immediately as one set")
	fs := flushed[0]
	assert.True(t, fs.oversized)
	assert.Len(t, fs.ops, 9, "all 9 ops of the revision stay in ONE Txn")
	assert.EqualValues(t, 7, fs.watermark)
	assert.Equal(t, "s7-8", fs.lastSrcKey)
	assert.Nil(t, b.flush())
}

// TestBatcherOversizedDoesNotDragNeighbors: pending small revisions flush
// separately before an oversized revision arrives.
func TestBatcherOversizedDoesNotDragNeighbors(t *testing.T) {
	b := newBatcher(10, 1<<20)
	require.Empty(t, b.add(mkGroup(1, 2, 1)))

	flushed := b.add(mkGroup(2, 20, 1))
	require.Len(t, flushed, 2, "pending set flushes first, then the oversized revision alone")
	assert.Len(t, flushed[0].ops, 2)
	assert.False(t, flushed[0].oversized)
	assert.Len(t, flushed[1].ops, 20)
	assert.True(t, flushed[1].oversized)
}

// TestBatcherFlushesOnDuplicateKey: etcd rejects duplicate keys within one
// Txn, and an unsynced-watcher catch-up response batches up to 1000
// revisions — so a key modified twice in the window must split the flush at
// the revision boundary instead of producing a poison Txn.
func TestBatcherFlushesOnDuplicateKey(t *testing.T) {
	b := newBatcher(100, 1<<20)
	g1 := revGroup{rev: 5, ops: []kvOp{{key: "/dst/a", value: "v1"}, {key: "/dst/b", value: "v1"}}}
	require.Empty(t, b.add(g1))

	// Revision 6 touches /dst/a again: revision 5 must flush alone first.
	g2 := revGroup{rev: 6, ops: []kvOp{{key: "/dst/a", value: "v2"}}}
	flushed := b.add(g2)
	require.Len(t, flushed, 1, "a duplicate key must force a flush at the revision boundary")
	assert.EqualValues(t, 5, flushed[0].watermark)
	assert.Len(t, flushed[0].ops, 2)

	rest := b.flush()
	require.NotNil(t, rest)
	assert.Len(t, rest.ops, 1)
	assert.EqualValues(t, 6, rest.watermark)

	// Disjoint keys still coalesce.
	require.Empty(t, b.add(revGroup{rev: 7, ops: []kvOp{{key: "/dst/c"}}}))
	require.Empty(t, b.add(revGroup{rev: 8, ops: []kvOp{{key: "/dst/d"}}}))
	both := b.flush()
	require.NotNil(t, both)
	assert.Len(t, both.ops, 2, "distinct keys must keep coalescing across revisions")
}

// TestBatcherFlushSetRetainsGroups: revision boundaries survive into the
// flush set, so a target-limit rejection can be re-committed at revision
// granularity.
func TestBatcherFlushSetRetainsGroups(t *testing.T) {
	b := newBatcher(100, 1<<20)
	require.Empty(t, b.add(mkGroup(1, 2, 1)))
	require.Empty(t, b.add(mkGroup(2, 3, 1)))
	fs := b.flush()
	require.NotNil(t, fs)
	require.Len(t, fs.groups, 2)
	assert.EqualValues(t, 1, fs.groups[0].rev)
	assert.Len(t, fs.groups[0].ops, 2)
	assert.EqualValues(t, 2, fs.groups[1].rev)
	assert.Len(t, fs.groups[1].ops, 3)
}

func TestGroupByRevision(t *testing.T) {
	ops := []kvOp{
		{key: "a"}, {key: "b"}, // rev 5
		{key: "c"},             // rev 6
		{key: "d"}, {key: "e"}, // rev 9
	}
	revs := []int64{5, 5, 6, 9, 9}
	groups := groupByRevision(ops, revs)
	require.Len(t, groups, 3)
	assert.EqualValues(t, 5, groups[0].rev)
	assert.Len(t, groups[0].ops, 2)
	assert.EqualValues(t, 6, groups[1].rev)
	assert.Len(t, groups[1].ops, 1)
	assert.EqualValues(t, 9, groups[2].rev)
	assert.Len(t, groups[2].ops, 2)

	assert.Nil(t, groupByRevision(nil, nil))
	assert.Nil(t, groupByRevision(ops, revs[:2]), "length mismatch must yield nothing")
}
