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

// Package mirroragent implements the EtcdMirror replication engine: a
// continuous, one-way key-range sync from a source etcd cluster into a
// target etcd cluster. It is a pure library — no Kubernetes API types, no
// binary, no metrics endpoint; progress is exposed through [Agent.Snapshot].
//
// Engine invariants (the doc comments in api/v1alpha1/etcdmirror_types.go
// state the same contracts on the CRD side; keep both in sync):
//
//   - Genesis is an UNPINNED chunked scan with the watch already open from
//     the revision observed before the scan started; buffered events are
//     replayed over the scanned base (reflector pattern). Pages read at the
//     current revision, so mid-scan compaction cannot fail the scan.
//   - The checkpoint (source-revision watermark plus {linkUID, epoch, role})
//     lives IN THE TARGET etcd at a reserved key, written in the SAME Txn as
//     every applied batch and fenced with a mod-revision compare on EVERY
//     write path (applies, reconciliation repairs, prune deletes). The
//     reserved key is excluded by exact match from scans, counts, prune
//     passes, and the RequireEmpty check.
//   - Target Txns flush ONLY at source-revision boundaries: a source
//     revision's events are never split across Txns, whole revisions are
//     coalesced up to the MaxTxnOps/TxnFlushBytes watermarks, and one op
//     slot is always reserved for the checkpoint write. A single revision
//     larger than MaxTxnOps is applied as one oversized Txn with the
//     checkpoint held until it lands.
//   - Key rewrite is one formula, anchored, never a substring replace:
//     key' = target.prefix + destPrefix + TrimPrefix(key, source.prefix).
//   - Errors are classified (see [Classify]): compaction forces a resync
//     (with a livelock detector), NOSPACE parks the agent until an operator
//     acts, oversized requests are permanent with the offending key surfaced
//     redacted (see [RedactKey]; values never logged), throttling backs off
//     on its own curve.
//   - Memory is bounded: one in-flight scan page (byte-bounded, adaptive)
//     and a byte-bounded replay buffer for the watch opened before the scan;
//     on overflow the agent cancels the source watch and restarts the scan
//     from a fresh R0 (a bounded retry, surfaced with cause
//     WatchBufferOverflow) instead of growing.
//
// # Why mid-scan compaction cannot wedge the engine
//
//  1. At scan start one linearizable Get (WithCountOnly) returns
//     Header.Revision = R0 and the total Count in a single RPC.
//  2. The watch opens at R0+1 BEFORE any scan page is read. R0 was observed
//     this instant by a linearizable read, so it cannot already be compacted.
//  3. Every scan page is an UNPINNED Get (no WithRev). ErrCompacted is a
//     property of reads pinned below the compact revision; an unpinned read
//     is immune by construction, at every point during the scan, regardless
//     of concurrent compactions. The failure class is removed, not detected
//     and retried.
//  4. The only remaining compaction hazard is the watch stream itself going
//     quiet long enough that a re-Watch(WithRev(watermark+1)) lands below
//     the compact revision — identical in shape during InitialSync and
//     steady state, handled by one mechanism: forced resync with a
//     mandatory mark-and-sweep prune.
//
// Scan and watch are not sequential phases with a handoff race: the watch is
// live for the entire scan, and scan writes may interleave with replayed
// watch writes because both write the same idempotent final value for a
// given key — convergence to the last-write value; a duplicate Put of
// identical content is a correctness no-op, only a bounded efficiency cost.
//
// # Why the fence needs only a mod_revision compare
//
// etcd's Compare supports whole-value/mod_revision/version/create_revision
// predicates only — no field-level JSON predicates. All safety rests on one
// discipline, identical on EVERY write path (apply, reconcile repair, prune,
// cutover role-flip):
//
//	If(Compare(ModRevision(fenceKey), "=", observedModRev)).
//	Then(dataOps..., Put(fenceKey, next))
//	// on !Succeeded: re-read, recompute, retry — never blind re-Commit
//
// linkUID/epoch/role are payload for humans and the engine's state machine,
// never comparison predicates. Cutover safety falls out for free: the
// role-flip Txn bumps ModRevision, so any writer holding a pre-flip
// observedModRev fails its next compare loudly — indistinguishable from an
// ordinary concurrent-writer collision, no special role-check code needed.
//
// # Retry ownership
//
// clientv3 auto-retries Get only on codes.Unavailable; Txn/Put/Delete are
// write-at-most-once (client-retried only when no connection was ever
// established). The engine owns 100% of write-path retry/backoff. A
// fenced-Txn retry after an ambiguous timeout must re-read the fence first —
// the Txn's own success bumped the fence ModRevision.
package mirroragent
