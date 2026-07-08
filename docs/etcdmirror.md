# EtcdMirror

`EtcdMirror` continuously copies a key range from a source etcd cluster into a
target etcd cluster, one way, as a single supervised stateless pod. Progress is
checkpointed in a reserved, fenced key **in the target etcd** — written in the
same transaction as every applied batch — so the agent has no volume and can be
rescheduled freely.

It is a **byte-copy of keys and values, not a replica**. If you need a replica,
add members to the cluster; if you need a point-in-time copy with revision
fidelity, use `etcdutl snapshot restore`.

## What is and is not preserved

| Property | Preserved? | Notes |
| --- | --- | --- |
| Key names | yes | rewritten by one formula: `key' = target.prefix + destPrefix + TrimPrefix(key, source.prefix)` |
| Values | yes | byte-identical |
| Per-revision atomicity | yes | batches flush only at source-revision boundaries; a source revision is never split across target Txns |
| Revisions / mod_revision / create_revision | **no** | target-assigned. Stored fence tokens, persisted watch bookmarks, and CreateRevision-ordered elections do not survive mirroring |
| Version counters | **no** | target-assigned |
| Leases / TTLs | **no** | leases are stripped; a lease-backed source key becomes a permanent target key. Masked while the mirror runs (source expiry replicates as a delete); at cutover every in-flight leased key is immortal. `status.leaseBackedKeyCount` reports exposure; the cutover runbook includes a purge/re-lease step. Consider `sync.excludePrefixes` for lease-heavy ranges |
| Cross-key Txn atomicity | best-effort | whole revisions are coalesced; a revision larger than `maxTxnOps` is applied as one oversized Txn (provision the target's `--max-txn-ops`) |

A fidelity-preserving alternative for migrations: seed the target with
`etcdutl snapshot restore --bump-revision --mark-compacted`, then mirror only
the delta using `initialSync.startRevision`.

## Sync engine behavior

- **Genesis (InitialSync):** unpinned chunked scan (one byte-bounded page in
  flight) with the watch already open from the revision observed before the
  scan; buffered events replay over the scanned base. Mid-scan compaction on
  the source therefore cannot fail the scan.
- **Steady state:** watch with progress notifications; the checkpoint watermark
  advances even when the mirrored prefix is idle.
- **Forced resync** (watch outrun by compaction — e.g. the mirror was down or
  paused longer than the source's compaction retention): reported as
  `Phase=InitialSync` with condition `Compacted=True/ForcedResync`, bracketed by
  `ForcedResyncStarted`/`ForcedResyncCompleted` events. Every forced resync ends
  with a mandatory mark-and-sweep prune, so deletes that happened during the
  blind window do not resurrect.
- **Retention prerequisite:** source compaction retention must exceed the
  worst-case scan + throttled drain time (roughly
  `keyCount / min(scanRate, maxOpsPerSecond)`), or genesis/forced resyncs
  livelock — surfaced as `ResyncLoopDetected`, which does not self-heal.
- **Checkpoint fencing:** the reserved key carries `{linkUID, epoch, role}` and
  every write path (applies, reconciliation repairs, prune deletes) is fenced
  with a mod-revision compare, so two agents can never interleave and a
  straggler apply after cutover fails loudly. On a failed compare the engine
  re-reads the fence — a Txn that committed while its response was lost (WAN
  timeout) is recognized as this agent's own write and adopted, never
  misreported as a fence violation.
- **Destination overlap guard:** a prune pass that finds ANOTHER link's
  reserved fence key inside this link's destination prefix stops with a
  permanent prefix-conflict error instead of deleting the sibling mirror's
  fence and data.

## Monitoring / paging algebra

Page on `Available=False` sustained, **unless** `Compacted=True` and progress
fields are advancing (a forced resync healing itself). `TargetQuotaExhausted`
and `ResyncLoopDetected` page immediately — neither self-heals.

Never compute lag as `status.sourceRevision - status.lastAppliedRevision`:
revisions are cluster-global, so out-of-prefix source writes inflate the
difference, and the two fields snapshot at different instants.

The `InitialSyncCompactionRaced` event marks a genesis-scan attempt aborted
and restarted from a fresh revision. One event name, two causes named in the
message — do not conflate them:

- `WatchBufferOverflow`: the replay buffer exceeded `sync.watchBufferBytes`
  before the base scan completed. A memory-bound retry, **not** a compaction
  race — raise `watchBufferBytes` or the scan rate for high-churn sources.
- `WatchCompactedMidScan`: a watch reconnect landed below the source compact
  revision — the rare genuine race.

Repeated occurrences of either count toward `ResyncLoopDetected`.

`InvariantsHeld=True` means the verification invariants hold (lag within
threshold, per-side key counts equal, no drift, pass fresh) — it never means
"safe to cut over"; that is `CutoverReady`, which additionally requires
`spec.mode: Drain` and a reached drain target revision.

## Operations: error taxonomy and runbook

| Error | gRPC code | Class | Retry policy | Condition / Reason |
| --- | --- | --- | --- | --- |
| `ErrCompacted` on watch reopen | OutOfRange | Resync | forced resync (scan + mandatory prune); never generic retry | `Compacted=True/ForcedResync`; `forcedResyncCount`++ (`Compacted`) |
| `ErrNoSpace` | ResourceExhausted | Quota | park on slow flat timer; never hot-loop; recovers without genesis once operator compacts/defrags/disarms | `TargetQuotaExhausted=True` (pages immediately) |
| client send cap ("trying to send message larger than max") | ResourceExhausted | Permanent | never retried identically; redacted key surfaced | Failed if unavoidable |
| `ErrTooManyRequests` | ResourceExhausted | Throttle | conservative distinct curve | `TargetThrottled=True` |
| `ErrRequestTooLarge` | InvalidArgument | Permanent | one shrink attempt at revision granularity, else Failed | redacted key surfaced |
| `ErrTooManyOps` | InvalidArgument | Permanent | one shrink attempt at revision granularity; else raise target `--max-txn-ops` or lower `spec.sync.maxTxnOps` | Failed |
| Unavailable / `ErrNoLeader` | Unavailable | Transient | ReconnectBackoff curve | Source/TargetReachable=False (reason NoLeader when applicable) |
| DeadlineExceeded (requestTimeout) | DeadlineExceeded | Transient | ReconnectBackoff — the blackholed-NLB recovery path | reachability, reason RequestTimeout |
| auth token expiry | — | non-issue | clientv3 refreshes transparently | none |
| source version < 3.4 | — | Permanent | never | Failed/`UnsupportedVersion` |
| corrupt/unknown-version checkpoint | — | Permanent (fail closed) | never; operator deletes reserved key | Failed/`CheckpointInvalid` |
| linkUID / cluster-ID mismatch | — | expected transition | genesis + RequireEmpty re-arm | `CheckpointInvalidated` event |
| fence Compare loss (normal apply) | — | optimistic-concurrency loss | re-read, recompute, retry (jitter, not reconnect curve) | internal; persistent genesis-claim loss → FenceError (permanent) |
| N consecutive resyncs, no steady period | — | livelock (meta) | does not self-heal | `ResyncLoopDetected=True` (pages immediately) |

## Cutover and reversal

Two-way sync is out of scope permanently: etcd revisions are cluster-local and
there is no per-key provenance channel, so bidirectional sync is structurally
inexpressible without an application-visible format change.

Cutover (forward): quiesce source writers, set `spec.mode: Drain`, then
`kubectl wait --for=condition=CutoverReady etcdmirror/<name>`. The
`status.cutover` block records the drained revision, verification counts, and
the lease-backed key count for the purge/re-lease step. Once CutoverReady, the
fence key's role is Primary and any straggler mirror write fails its compare.

Reversal (failback) is delete-and-recreate: delete the CR, create a new one
with swapped endpoints and `initialSync.mode: OverwriteAndPrune` — the
mandatory prune pass removes keys deleted on the new primary since cutover.
Only reverse after the forward mirror reached CutoverReady.

## Prerequisites (summary)

- Source etcd >= 3.4 (hard floor, probed at connect); >= 3.4.25 / 3.5.8
  recommended — below that, watch progress notifications are unreliable.
- Target credential range-scoped via etcd RBAC to the effective destination
  prefix, **including the reserved checkpoint key**
  (default `<target.prefix + destPrefix>\x00etcdmirror-checkpoint`; an
  override must stay under the effective destination prefix — CEL-enforced).
- Range-defining fields (`source.prefix`, `target.prefix`, `sync.destPrefix`,
  `sync.excludePrefixes`, `checkpoint.key`) are immutable: the resume path
  never re-scans, so an edited range silently diverges. Change them via
  delete-and-recreate with an appropriate `initialSync.mode`.
- Target runs with auto-compaction enabled (resync churn otherwise marches the
  default 2GiB quota toward NOSPACE / `TargetQuotaExhausted`).
- Source compaction retention satisfies the formula above for your key count
  and rate limit.
- A source running `--client-cert-auth` (RKE1 default) rejects certless
  clients at the handshake: supply a client certificate; username/password
  auth alone is not viable there. When both are supplied, the token identity
  wins and must hold the range-scoped role.
- The agent Deployment runs on the namespace default ServiceAccount with
  `automountServiceAccountToken: false`; it needs zero Kubernetes API access,
  so no ServiceAccount/Role/RoleBinding is installed for it.
