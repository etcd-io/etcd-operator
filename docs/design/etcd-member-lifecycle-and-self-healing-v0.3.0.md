# Design: etcd member lifecycle management & self-healing (v0.3.0)

Related: [reconcile_loop_v0.3.0.png](reconcile_loop_v0.3.0.png),
[reconcile_cluster_v0.3.0.png](reconcile_cluster_v0.3.0.png),
[reconcile_member_v0.3.0.png](reconcile_member_v0.3.0.png)

Scope: this is not just a new API type. It's a redesign of
`EtcdClusterReconciler`'s reconcile workflow — ordinal allocation,
leadership transfer, promotion, `CORRUPT`/`NOSPACE` alarm remediation,
lost-quorum recovery, scale/upgrade sequencing, failed-member recovery, and
whole-cluster finalization — built around a new CRD, `EtcdMember`. The
**same** `EtcdClusterReconciler`
reconciles both `EtcdCluster` and `EtcdMember`; this is not a second
controller. `EtcdMember` is used both as a durable per-member state store
and as a managed child resource (owns Pod, PVC, TLS cert). Sections
4.1–4.12 describe changes to the reconcile loop itself, not just the new
type.

## 1. Summary

Introduce a new namespaced CRD, `EtcdMember`, representing exactly one etcd
member/Pod slot (ordinal) belonging to an `EtcdCluster`. `EtcdClusterReconciler`
— unchanged as the *only* reconciler in this design — now also owns
`EtcdMember`. Each member's retry/lifecycle state is durably recorded on
`EtcdMember.Status`, so a failed member can be retried across reconcile
loops (and controller restarts) and, if retries are exhausted, replaced.
Cluster-wide `CORRUPT` and `NOSPACE` alarms, and total quorum loss, each get
their own resumable remediation rather than being treated as ordinary Pod
problems. Exactly one action is ever taken at a time per cluster; every
other orchestration action (scale, upgrade, config update) waits until
every existing member is `Ready`.

This directly targets real gaps in today's implementation (§2): health-check-driven
repair is unimplemented, drift-correction only patches Pod-count-vs-member-count
symptoms, and there's nowhere today to durably remember "we've already tried
to fix this member N times."

## 2. Background: current state

Today there is a single reconciler, `EtcdClusterReconciler`
([etcdcluster_controller.go](https://github.com/etcd-io/etcd-operator/blob/59b848e9188a7776267896ed9f6f7dfb0e85956c/internal/controller/etcdcluster_controller.go)),
which runs all phases sequentially against one `EtcdCluster` object per loop:
fetch/validate → bootstrap → health-check → exception-handling →
promote-learner → update-config → scale → upgrade → status. This design
keeps that same single-reconciler shape; it doesn't introduce a second one.

Relevant facts about the current implementation:

- Pods are named `{cluster}-{ordinal}` (`memberPodName`,
  [pods.go:48](https://github.com/etcd-io/etcd-operator/blob/59b848e9188a7776267896ed9f6f7dfb0e85956c/internal/controller/pods.go#L48)), mirroring StatefulSet
  naming, but there is **no separate object per member** — ordinal bookkeeping
  is done by listing owned Pods and parsing their names.
- `nextPodOrdinal` ([pods.go:63](https://github.com/etcd-io/etcd-operator/blob/59b848e9188a7776267896ed9f6f7dfb0e85956c/internal/controller/pods.go#L63))
  currently returns the **first gap**, not max+1. That's a different policy
  than what we want (see Requirements below) and needs to change regardless
  of whether we introduce `EtcdMember`.
- `healthCheckAndFix` ([etcdcluster_controller.go:315](https://github.com/etcd-io/etcd-operator/blob/59b848e9188a7776267896ed9f6f7dfb0e85956c/internal/controller/etcdcluster_controller.go#L315))
  computes health but the "fix" part is a `TODO`, not implemented.
- `updateConfig` and `upgradeCluster` are stubs that return `ctrl.Result{}, nil`.
- `scaleCluster` already calls into `internal/etcdutils` for `AddMember` /
  `RemoveMember`, and has `gofail` fault-injection points
  (`exceptionAfterMemberAdd`, `exceptionAfterMemberDelete`) simulating a crash
  between the etcd API call and the Pod mutation — i.e. the code already
  anticipates that a single `Reconcile()` call can be interrupted mid-way and
  needs to resume correctly next loop. `reconcileExceptions` handles exactly
  that resumption today, but only for the simple "Pod count vs member count"
  case.
- `promoteLearner` ([etcdcluster_controller.go:379](https://github.com/etcd-io/etcd-operator/blob/59b848e9188a7776267896ed9f6f7dfb0e85956c/internal/controller/etcdcluster_controller.go#L379))
  already exists and already does roughly what §4.9 below needs: find the
  leader and learner from a live `MemberList`/health call, check
  `IsLearnerReady`, call `PromoteLearner`. This design mostly keeps this
  function's shape; the change is *when* it's given priority relative to
  fixing a different failed member (§4.9).
- Alarm handling (`AlarmList`/`AlarmDisarm`) and `Compact` are not wrapped in
  `internal/etcdutils` at all today; only `Defragment` exists, on the
  `Maintenance` interface, unused.
- PVCs are created and owned directly by `EtcdCluster`
  (`createPVCForMember`, [utils.go:63](https://github.com/etcd-io/etcd-operator/blob/59b848e9188a7776267896ed9f6f7dfb0e85956c/internal/controller/utils.go#L63)),
  named `etcd-data-{podName}`.
- Per-member status (`MemberStatus`: Name/ID/Version/IsHealthy/IsLearner/IsLeader,
  [etcdcluster_types.go:188](https://github.com/etcd-io/etcd-operator/blob/59b848e9188a7776267896ed9f6f7dfb0e85956c/api/v1alpha1/etcdcluster_types.go#L188)) is
  purely observational — recomputed every loop from `etcdctl member list` /
  health, with **no field that persists intent or retry-count across loops**.
- No finalizers exist anywhere in the codebase yet.

None of this is wrong for what v0.1.0/v0.2.0 needed (create, scale, basic
health observation), but it has no place to hang the state a real self-healing
loop needs: "I've restarted this Pod twice already," "this member is mid-replacement,
don't double-fix it," "I've already compacted and defragged for
this alarm, don't redo it," etc.

## 3. Requirements

1. **The controller itself never creates an ordinal gap**. A gap can
   still appear from out-of-band interference (e.g. a human manually
   deleting a non-highest-ordinal `EtcdMember`); when that happens, it
   isn't hunted down and fixed on its own, but scale-out is guaranteed to
   eventually fill it rather than allocate a new, higher ordinal.
2. **Scale-in removes the highest ordinal first.**
3. **Scale-out allocates the lowest missing ordinal, or `max(existing ordinals) + 1` if there is no gap to reuse.**
4. Support fixing failed members: the default remedy is delete-and-recreate the
   Pod with the **latest config** from `EtcdCluster.Spec` — except `Version`,
   which stays at the member's current running version rather than jumping
   to the upgrade target. Version changes are rolled out exclusively by the
   separate upgrade phase, one member at a time. **Opt-in exception:** this
   default has a deadlock: a member stuck in a repair loop never reaches
   `Ready`, so the readiness gate (requirement 9) never lets the upgrade
   phase run — even if the upgrade target is the actual fix. A human
   operator can set an `EtcdClusterSpec` flag (`AllowVersionUpgradeOnRepair`,
   off by default) to break this: while set, every repair recreate — not
   just the dedicated upgrade phase — brings the member to
   `EtcdCluster.Spec.Version` too. See §4.6.
5. If recreating doesn't help, fall back to a blunter remedy: remove the etcd member,
   clean up its data and TLS certificate, and add it back as a new member
   with a fresh Pod. Simple heuristic for "recreating didn't help": three
   failed recreate attempts.
   A `CORRUPT` alarm (requirement 13) is a faster, definitive trigger into
   this same remedy, bypassing the heuristic whenever etcd's own
   consistency checking catches the corruption itself.
6. When multiple members are broken, fix them **one at a time** — except
   `NOSPACE`/`CORRUPT` alarm remediation, a lost-quorum recovery already
   under way (requirements 12–14), and finishing the departure of a member
   that's already `Terminating` (§4.6/§4.9), which are urgent enough to
   preempt a repair already in flight on a different member, a member that's
   already leaving is never a candidate for the repair ladder anyway.
   *Starting* a lost-quorum recovery is different: it's tried only after
   `CORRUPT`/`NOSPACE`/an ordinary repair attempt have all found nothing to
   do, since it discards every other member's data and an ordinary repair
   might restore quorum on its own. See §4.8/§4.9.
7. Because fixing a member can span multiple reconcile loops, **per-member
   progress must be persisted** somewhere that survives across loops (not
   recomputed from scratch each time).
8. Each member should "own" its Pod, its data (PV/PVC), and its TLS
   certificate.
9. **Readiness gate.** As long as any existing `EtcdMember` is not `Ready`,
   `EtcdClusterReconciler` must not perform any other orchestration action
   (scale, upgrade, config update) — it only waits. This mirrors
   StatefulSet's default `OrderedReady` pod-management policy, which won't
   touch the next pod, or continue a rolling update, until the pod it's
   already touched is `Running` and `Ready`.
10. **Leadership transfer.** Before removing or recreating the Pod of a
    member that is currently the etcd leader — for scale-in, an
    upgrade/config-update rollout, or a recreate/replace (heuristic- or
    `CORRUPT`-triggered) — move leadership to another existing member
    first: the one with the lowest ordinal, excluding the member being
    acted on.
11. **Never add a second learner while one is already pending.** etcd allows at
    most one learner per cluster, so if an existing member is a
    healthy-but-not-yet-promoted learner, no *different* member is ever
    allowed to attempt `MemberAdd(learner)` — most notably, a different
    member's `Replacing` remedy (requirement 5) rejoining fresh — until the
    existing learner is promoted (once caught up) or otherwise resolved.
    This doesn't block *starting* an unrelated repair, only the specific
    moment a second learner would be added. See §4.9.
12. **`NOSPACE` alarm remediation must be resumable and idempotent, and is
    urgent enough to preempt a repair already in flight on a different
    member.** Fixing a `NOSPACE` alarm takes three ordered steps — compact,
    defragment (per member, one at a time, leader last), disarm — that can
    span multiple reconcile loops, and the controller should not redo an
    already-completed step; most importantly, it should not re-defragment a
    member that already finished (successfully or via timeout), since
    defragmentation is the most expensive and disruptive of the three. See
    §4.7.
13. **`CORRUPT` alarm remediation ranks above `NOSPACE`.** Both are
    per-member-repair-preempting remedies (requirement 6); when both are
    simultaneously applicable, `CORRUPT` goes first. The fix (remove the
    corrupted member, disarm) is two steps done within a single reconcile
    loop when uninterrupted; if interrupted, redo both on the next loop,
    unless the member was already removed, in which case just disarm. See
    §4.6/§4.7/§4.9.
14. **Total quorum loss needs a distinct, opt-in recovery path** — restarting
    the member with the most up-to-date data as a brand-new single-member
    cluster, then discarding and rejoining the rest via the *existing*
    scale in/out mechanics rather than any new membership logic. Off by
    default; should support a human operator triggering and steering it
    manually. See §4.8.
15. **Pause for manual intervention.** Expose an API (e.g.
    `EtcdClusterSpec.Paused`) letting a human operator pause
    `EtcdClusterReconciler`'s reconciliation of a cluster, so they can debug
    or repair by hand without the controller fighting them over the same
    Pods, PVCs, or etcd membership. While paused, no mutating action
    runs — including `CORRUPT`/`NOSPACE` remediation and lost-quorum
    recovery (requirements 12–14) — but the always-on, read-only
    status/health refresh keeps running so observability doesn't go stale.
    Resuming needs no special "catch up" logic: the same live-state-derived
    idempotency checks that make every other remedy in this design
    resumable also make picking back up after a pause safe, whatever the
    operator changed by hand in the meantime.
16. **Deleting an `EtcdCluster` must cleanly tear down every `EtcdMember` it
    owns before the `EtcdCluster` object itself is allowed to disappear** —
    but without paying for the graceful, one-at-a-time per-member leave
    mechanics of requirement 10 (leadership transfer, `MemberRemove`, alarm
    disarm), since those exist to protect a cluster that's still around
    after the member leaves. When every member is leaving together, there's
    no remaining cluster to protect. See §4.13.

## 4. Proposed design

### 4.1 One reconciler, two CRDs

`EtcdClusterReconciler` reconciles both `EtcdCluster` (top-level) and
`EtcdMember` (owned by it) — there is no second controller. `SetupWithManager`
adds `Owns(&EtcdMember{})` alongside the existing `Owns(&Pod{})`,
`Owns(&PersistentVolumeClaim{})`, `Owns(&Service{})`: any change to any of
these — including changes `EtcdClusterReconciler` makes to them itself —
enqueues a reconcile of the owning `EtcdCluster`. This is the same mechanism
already in place for Pods/PVCs today, just extended to one more owned type.

Because there's only one reconciler, `EtcdMember` doesn't need an API shaped
for cross-controller signaling — no asynchronous counter pair for one
controller to tell another "please act" without directly writing into a
field it doesn't own. The same function that decides a member needs fixing
can just fix it, in the same pass. §5's Alternative B covers the tradeoffs
of splitting this into two controllers instead.

What `EtcdMember` is for, then, does *not* depend on there being two
controllers:

- **A durable per-member state store.** `Status.Phase`/`RecreateCount`/etc.
  survive across reconcile loops *and* controller restarts (requirement 7)
  because they live on a Kubernetes object's status subresource, not because
  a second controller wrote them.
- **A managed child resource.** `EtcdMember` owns the member's Pod, PVC, and
  TLS certificate `Secret` (§4.10), so Kubernetes garbage-collects them
  correctly when a member is removed, and a per-member finalizer (§4.6)
  gives ordered cleanup (`MemberRemove` before PVC delete) a natural home —
  today's codebase has no finalizers anywhere, and bolting ordered cleanup
  onto a status slice field (the pre-`EtcdMember` alternative, §5.A) is more
  error-prone than an object with its own deletion lifecycle.

One consequence of the workqueue's per-key semantics worth naming explicitly:
Kubernetes' `workqueue.RateLimitingInterface` guarantees at most one
`Reconcile()` call for a given `EtcdCluster` is ever in flight at a time, no
matter how high `MaxConcurrentReconciles` is set. That's a useful property
to lean on, but it isn't a *substitute* for the explicit "one action at a
time" check in §4.9 — it only serializes reconciles of the *same* cluster;
different clusters reconcile concurrently, which is the axis this design
scales on.

### 4.2 Readiness gate: only orchestrate when everything is Ready

`EtcdClusterReconciler` must not run any of the "policy" phases — update
config, scale out/in, upgrade — while any existing `EtcdMember` is not
`Ready`. Concretely, at the top of each reconcile, after listing this
cluster's `EtcdMember`s:

```go
if !allReady(members) {
    // Per-member repair, promotion, alarm remediation, and quorum
    // recovery may still act (see §4.9); everything else waits.
    return ctrl.Result{RequeueAfter: requeueDuration}, nil
}
// ... proceed to update-config / scale / upgrade ...
```

This mirrors StatefulSet's default `OrderedReady` pod-management policy: the
controller won't launch/terminate the next pod, or continue a rolling
update, until the pod it's already touched is `Running` and `Ready`. This
gate makes that an explicit, named invariant rather than something that's
only true as an emergent property of how the reconcile phases happen to be
ordered (§4.9 is where the actual ordering is decided).

Four cases are carved out — each is *what gets a member (or the cluster) to*
a resolved state, so each has to be allowed to run even though, by
definition, it targets a not-yet-resolved situation:

- **Zero members.** "All existing members are Ready" is vacuously true when
  there are none, so bootstrap (creating `EtcdMember` ordinal 0) is
  unaffected.
- **An existing not-yet-promoted learner.** A member can be perfectly
  healthy and still not be `Ready`, because `Ready` requires promotion
  (§4.6). Promoting it is exactly what resolves that specific kind of
  not-ready.
- **An active `CORRUPT` or `NOSPACE` alarm.** Neither remedy (§4.6, §4.7)
  touches Pod or membership state for members it isn't specifically acting
  on, so there's no reason to make it wait for other members to become
  `Ready` — and no reason it should, since a write-blocked cluster likely
  can't make progress on anything else anyway.
- **Total quorum loss.** By definition most members aren't `Ready` when
  this applies; §4.8's recovery has to be exempt or it could never run.

Cheap, read-only status refresh (`updateStatus`/`updateConditions`, and the
live `MemberList`/`ClusterHealth`/`AlarmList` calls that feed it) is not
gated — it runs every loop regardless, so status doesn't go stale while
this gate is holding orchestration back. Only phases that mutate cluster or
member state are gated. This is also what makes requirement 15's
human-triggered pause safe: `Spec.Paused` (§4.9) suppresses every mutating
action, but never this refresh — don't confuse the two different senses of
"paused" in this doc.

### 4.3 `EtcdMember` API sketch

```go
// EtcdMemberSpec defines the desired state of one etcd member/ordinal.
type EtcdMemberSpec struct {
    // ClusterName is the owning EtcdCluster's name (same namespace).
    // +kubebuilder:validation:XValidation:rule="self == oldSelf",message="clusterName is immutable"
    ClusterName string `json:"clusterName"`

    // Ordinal is this member's fixed position, e.g. 0, 1, 2. The member Pod
    // and PVC are named "{clusterName}-{ordinal}" / "etcd-data-{clusterName}-{ordinal}".
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ordinal is immutable"
    Ordinal int `json:"ordinal"`

    // Version is the etcd version this member's Pod should run. Normally
    // equal to EtcdCluster.Spec.Version, but during a rolling upgrade
    // EtcdClusterReconciler bumps this one member at a time, so members can
    // transiently run different versions than the cluster's target.
    Version string `json:"version"`
}

// EtcdMemberPhase is the coarse lifecycle phase of a member.
type EtcdMemberPhase string

const (
    EtcdMemberPending      EtcdMemberPhase = "Pending"      // controller hasn't started creating this member's resources yet
    EtcdMemberProvisioning EtcdMemberPhase = "Provisioning" // resource creation under way: Pod not yet healthy, OR healthy but still an unpromoted learner
    EtcdMemberReady        EtcdMemberPhase = "Ready"        // healthy AND (bootstrap member OR promoted to voting member)
    EtcdMemberRecreating   EtcdMemberPhase = "Recreating"   // unhealthy, Pod delete+recreate in progress
    EtcdMemberReplacing    EtcdMemberPhase = "Replacing"    // recreate retries exhausted (or a CORRUPT alarm); member leaving, about to rejoin fresh
    EtcdMemberTerminating  EtcdMemberPhase = "Terminating"  // DeletionTimestamp set; leaving the cluster for good
)

// EtcdMemberStatus defines the observed state of a single etcd member.
type EtcdMemberStatus struct {
    Phase EtcdMemberPhase `json:"phase,omitempty"`

    // MemberName is this member's name, always "{clusterName}-{ordinal}" —
    // the same value is used as both the etcd member name and the Pod name.
    // Recorded for convenience/observability only, never used to make a
    // decision (the name is fully deterministic from Spec.Ordinal, and the
    // Pod-recovery ladder (§4.6) only ever needs to know whether a Pod
    // currently exists and how long it's existed, both a live check against
    // that name — no identity-tracking field is needed to tell a stale Pod
    // apart from a fresh one).
    MemberName string `json:"memberName,omitempty"`

    // MemberID is the hex etcd member ID, recorded for
    // observability/reporting only. EtcdClusterReconciler never uses this persisted value to
    // decide anything — wherever a member's live etcd identity is needed
    // (e.g. MemberRemove's target in §4.6's leave sequence), it's found by
    // a live MemberList lookup keyed on this ordinal's deterministic peer
    // URL, never by trusting this field is still current.
    MemberID string `json:"memberID,omitempty"`

    // CurrentVersion/IsHealthy/IsLearner/IsLeader are observational
    // snapshots only, refreshed from a live check every reconcile.
    // EtcdClusterReconciler's own decisions (e.g. whether promotion is
    // possible) always come from that same live check, never from trusting
    // these persisted values are still current (§4.6).
    CurrentVersion string `json:"currentVersion,omitempty"`
    IsHealthy      bool   `json:"isHealthy"`
    IsLearner      bool   `json:"isLearner,omitempty"`
    IsLeader       bool   `json:"isLeader,omitempty"`

    // RecreateCount is the number of consecutive Pod recreations performed
    // while trying to get this member healthy — whether it's a Provisioning
    // member whose Pod never came up, or a Ready member that regressed and
    // is now Recreating (§4.6's shared recovery ladder). Bumped once each
    // recreate actually happens, after the mutation rather than before —
    // the one deliberate exception to this doc's usual write-before-mutate
    // rule (§4.6). Reset to 0 both when the member reaches Ready and when
    // it escalates to Phase=Replacing, so a member rejoining fresh after a
    // replace always starts counting from zero. At RecreateCount == 3 the
    // reconciler gives up on "just recreate the Pod" and moves to
    // Phase=Replacing.
    RecreateCount int32 `json:"recreateCount,omitempty"`

    // LastDefragTime records when this member last finished a defrag
    // attempt — successful or timed out (§4.7 treats a timeout as "don't
    // retry this member again this cycle" rather than as unfinished work).
    // Used to tell whether this member still needs defragging in the
    // current NOSPACE cycle, without redoing one already attempted.
    LastDefragTime *metav1.Time `json:"lastDefragTime,omitempty"`

    Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// EtcdMember is the Schema for the etcdmembers API.
type EtcdMember struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   EtcdMemberSpec   `json:"spec,omitempty"`
    Status EtcdMemberStatus `json:"status,omitempty"`
}
```

`EtcdClusterStatus` (the existing type in `etcdcluster_types.go`) gains
fields for §4.7's and §4.8's cluster-level remediation state:

```go
type NoSpaceRemediationPhase string

const (
    RemediationCompacting    NoSpaceRemediationPhase = "Compacting"
    RemediationDefragmenting NoSpaceRemediationPhase = "Defragmenting"
)

type NoSpaceRemediationStatus struct {
    Phase NoSpaceRemediationPhase `json:"phase"`

    // CycleStarted marks when *this* remediation attempt began. Bumped to
    // "now" any time the sequence (re-)starts from Compacting — including a
    // restart after the post-Defragmenting db-size check finds the cycle
    // didn't reclaim enough — which is what invalidates old
    // LastCompactTime/LastDefragTime values and forces a real redo instead
    // of trusting stale timestamps (§4.7).
    CycleStarted metav1.Time `json:"cycleStarted"`
}

type QuorumRecoveryStatus struct {
    // Survivor is the ordinal EtcdClusterReconciler selected (or a human
    // operator specified) to become the new single-member cluster. This is
    // a decision, not something re-derivable live once other members are
    // gone — there may be nothing left to compare commit indices against —
    // and step 3's "terminate every *other* member" needs to know exactly
    // which one to spare (§4.8).
    Survivor int `json:"survivor"`
}

// Added to EtcdClusterStatus:
//   LastCompactTime    *metav1.Time              `json:"lastCompactTime,omitempty"`
//   NoSpaceRemediation *NoSpaceRemediationStatus `json:"noSpaceRemediation,omitempty"` // nil == no alarm currently being remediated
//
//   // QuorumRecovery, nil == not currently recovering, is the one
//   // exception to "recovery decisions always come from a live read":
//   // unlike the observational fields below it, EtcdClusterReconciler
//   // does decide from this field — specifically, whether a lost-quorum
//   // recovery is already under way (§4.8/§4.9) — because that's a
//   // question about decision history, not current topology; a live
//   // snapshot can't tell "mid-recovery" apart from an ordinary bootstrap
//   // or a cluster legitimately scaled down to one member. Written before
//   // the mutation it describes, same as everywhere else (§4.6), and
//   // cleared once recovery completes (§4.8 step 4). Everything *within*
//   // an already-started recovery — has `--force-new-cluster` taken
//   // effect yet, are the other members gone yet — is still checked live,
//   // the same discipline NOSPACE's own Status.Phase sub-steps use (§4.7).
//   QuorumRecovery *QuorumRecoveryStatus `json:"quorumRecovery,omitempty"`
//
//   // Observational only (§4.8) — EtcdClusterReconciler must never decide
//   // anything from these, only report them.
//   LastRecoveryTime *metav1.Time `json:"lastRecoveryTime,omitempty"`
//   RecoveryCount    int32        `json:"recoveryCount,omitempty"`
//   LastRecoveredFrom string     `json:"lastRecoveredFrom,omitempty"` // the ordinal recovered from
```

Notes:

- `EtcdMember` deliberately does **not** duplicate `ImageRegistry`,
  `EtcdOptions`, `StorageSpec`, `TLS`, `PodTemplate`, etc. from
  `EtcdClusterSpec` — `EtcdClusterReconciler` already has the parent
  `EtcdCluster` object in hand in the same reconcile call. Only truly per-member fields (`Ordinal`,
  `Version` during a rolling upgrade) live on `EtcdMemberSpec`.
- Object name = `{clusterName}-{ordinal}`, matching today's Pod name, so both
  the Pod and PVC names stay stable and human-readable and existing DNS
  (`{name}.{cluster}.{ns}.svc.cluster.local`) is unaffected.
- `EtcdClusterReconciler` adds a finalizer (e.g.
  `operator.etcd.io/member-cleanup`) at the same time it creates the
  `EtcdMember` object, and removes it only once the `Terminating` cleanup in
  §4.6 is confirmed done. This is what lets scale-in be a plain
  `client.Delete()` on the `EtcdMember` — see §4.6. The one exception is
  whole-cluster finalization (§4.13), which strips every member's finalizer
  directly instead of running §4.6's `Terminating` sequence on each — there,
  the leave sequence's own justification (protect the cluster that's still
  running after this one member is gone) doesn't hold, since every member
  is leaving at once.
- `EtcdCluster` itself also gets a finalizer (e.g.
  `operator.etcd.io/cluster-cleanup`), separate from each `EtcdMember`'s —
  see §4.13.
- Manually creating an `EtcdMember` isn't recommended, but the controller
  validates it anyway: `Ordinal >= 0` and `ClusterName`/`Ordinal`
  immutability are the CEL markers above. Rejecting a duplicate `Ordinal`
  among an `EtcdMember`'s siblings, and rejecting `Ordinal >=
  EtcdCluster.Spec.Size`, need to look at *other* objects, which CEL can't
  do — so those two are enforced by a `ValidatingWebhookConfiguration`
  (`webhook.CustomValidator`'s `ValidateCreate`) instead, requiring `list`
  on `EtcdMember` and `get` on `EtcdCluster` in its RBAC.

### 4.4 Ordinal allocation & closing gaps

```go
func nextOrdinal(existing []int) int {
    sort.Ints(existing)
    for i, o := range existing {
        if i != o {
            return i // reuse the lowest gap
        }
    }
    return len(existing) // no gap: existing is contiguous [0, len), so this is max(existing)+1
}
```

This only runs as part of an actual scale-out decision (§4.9's gated
update-config/scale/upgrade step, once every existing member is `Ready`) —
there's no separate action that hunts down and closes a gap on its own.
Existing members are never renumbered/compacted to close a gap — only a
genuinely new member ever gets assigned the reused ordinal, since peer
identity, DNS names, and the Pod/PVC/cert `Secret` names are all derived
from the ordinal.

etcd itself doesn't care whether ordinals are contiguous — only this
operator's own naming does, and that's cosmetic, not a correctness
concern. So a gap left while the member count already matches `Spec.Size`
just waits for the next scale-out to close it. Combined with the admission
webhook (§4.3, rejects a duplicate or out-of-range `Ordinal`) and the
finalizer-driven cleanup on any deletion (§4.6), a persistent gap should be
rare — the main way one still arises is a legitimate manual deletion of a
non-highest ordinal.

Scale-in always targets `max(ordinals)`, unaffected by any of this — it
only ever removes from the top, so it never creates a gap itself.

### 4.5 Leadership transfer before removing or recreating a member's Pod

Whenever `EtcdClusterReconciler` is about to delete a member's Pod — for a
health-triggered `Recreating`/`Replacing` repair, a planned config/upgrade
recreate, or a graceful `Terminating` departure (scale-in) — and that member
is currently the etcd **leader**, it must first transfer leadership away
rather than let the Pod disappear out from under the leader and force a
Raft election.

Rule: transfer to the **existing, `Ready`, voting (non-learner) member with
the lowest ordinal, excluding the member being acted on** — using the live
health picture from `etcdutils.ClusterHealth`, so leadership never moves to
a member that's itself unhealthy. This is **best-effort, not a
precondition**: `MoveLeader` must be served by the current leader itself,
which may already be unreachable when this runs as part of a
health-triggered transition. If the call errors or times out, proceed with
the removal/deletion anyway — Raft will hold a normal election once the
leader's process actually stops. The attempt is still worth making, since
it turns the common case (a *planned* rollout, or a leader that's merely
slow rather than fully down) into a zero-downtime transfer instead of an
election gap.

### 4.6 Membership lifecycle and the failure-recovery state machine (per `EtcdMember`)

**Rule: always write `Status.Phase` *before* performing the mutation it
describes, never after.** This is what makes the readiness gate correct
(§4.2/§4.9 read `Phase`) and what makes a mid-action crash safe to resume
from. `RecreateCount` is the one deliberate exception — it's bumped
*after* a recreate actually happens, not before (see the ladder below).
The trade-off: a crash in the narrow window between "Pod recreated" and
"count bumped" just costs one extra, harmless recreate on the next loop,
which is an acceptable price for not having to track a separate
per-attempt timestamp. It's bumped **every time a Pod is created for this
member, uniformly, with no special-casing for why** — the very first join,
a Pod that vanished out-of-band, a planned config/version recreate, or a
genuine health-check-driven retry all bump it the same way — and reset to
0 whenever the member reaches `Ready` or escalates to `Replacing`. A
routine, successful recreate just blips the count up and immediately back
down to 0 once the member is healthy again, so there's no need to reason
about which cases "count" and which don't.

**Opt-in: letting repair recreates also upgrade (requirement 4).** The
ladder's recreates always take `Version` from `EtcdMemberSpec.Version` —
normally untouched by repair, only bumped one member at a time by the
upgrade phase. If `EtcdClusterSpec.AllowVersionUpgradeOnRepair` is set,
`EtcdClusterReconciler` writes `EtcdMemberSpec.Version =
EtcdClusterSpec.Version` before any repair recreate (write-before-mutate,
same as `Status.Phase` above) — no other change to the ladder is needed.
This applies to every repair recreate while the flag is set, not just one
that's already exhausted ordinary retries, so it trades the upgrade
phase's deliberate one-member-at-a-time order for unblocking a
stuck-forever repair loop. Off by default, like this design's other
opt-in overrides (`Spec.Paused`, requirement 15; lost-quorum recovery,
requirement 14); the operator turns it back off once the cluster is
healthy.

**Joining the cluster.** While `Phase` is `Pending`/`Provisioning`:

1. Create the TLS certificate `Secret` if enabled and not already present,
   then the PVC if not already present — unconditionally, for every
   member, before anything etcd-side happens.
2. If this is ordinal 0 and no other member has ever joined (the bootstrap
   case), skip straight to step 4 — the first member starts as a full
   voting member, never a learner.
3. Otherwise, check (a live `MemberList` call) whether a member with this
   ordinal's deterministic peer URL already exists; if not, call
   `MemberAdd`(learner) first — same as today's `scaleCluster`.
4. Start the Pod if not already present (`--initial-cluster-state=new`
   only for the bootstrap member, `existing` otherwise), bumping
   `RecreateCount` (see above).
5. If the etcd member becomes healthy *and*, for non-bootstrap members, is
   promoted (see below), `Phase` advances to `Ready`. If it doesn't
   become healthy, the same recovery ladder used for a regressed `Ready`
   member applies here too — with one difference: `Phase` stays
   `Provisioning` throughout, only ever advancing past it to `Replacing`
   if retries are exhausted.

**`Phase: Ready` requires promotion, not just a healthy Pod — this is what
actually enforces "at most one learner at a time" (requirement 11).** etcd
itself rejects a second `MemberAdd(learner)` while one already exists; our
own protection is §4.2's readiness gate — `EtcdClusterReconciler` won't
perform any orchestration action, including creating a new `EtcdMember`,
while any existing member isn't `Ready` — and `Ready` can't be true while a
live check still reports this member as a learner. So while
healthy-but-still-a-learner, `Phase` stays `Provisioning`, and
`EtcdClusterReconciler` checks whether the learner is caught up enough to promote,
calling `MemberPromote` once it is, then writing `Phase: Ready` and
resetting `RecreateCount` once promotion succeeds. §4.9 covers when this
check is given priority over other work.

**The Pod-recovery ladder — shared by `Provisioning` and `Recreating`.**
Whenever a member's etcd process isn't healthy and `Phase` is
`Provisioning` or `Recreating`, the same sequence decides what to do,
checked in order every reconcile:

1. Is the etcd member healthy now (a live check, per `Status.IsHealthy`'s
   definition — §4.3)?
   - No → continue to step 2.
   - Yes → has its config or `Version` drifted from `EtcdCluster.Spec`/
     `EtcdMemberSpec.Version`? `Ready` requires both healthy *and*
     matching desired config — a healthy Pod running stale config isn't
     done yet. → No (matches): `Phase: Ready` (promoting first if
     needed — above), reset `RecreateCount` to 0. Done. → Yes (drifted):
     transfer leadership if it's the leader (§4.5), delete the Pod, create
     a new one with the latest config, then bump `RecreateCount` — this
     branch skips straight past steps 2–4 below, since a deliberate,
     wanted change isn't a failure signal and shouldn't wait on the
     grace-period check built for tolerating one.
2. Otherwise (unhealthy), is `RecreateCount` already `>= 3`? → give up on
   recreating: `Phase: Replacing`, reset `RecreateCount` to 0 (so a member
   that rejoins fresh afterward starts counting from zero), then follow
   `Replacing`'s leave sequence below.
3. Otherwise, does a Pod exist for this member at all? → no: create one,
   then bump `RecreateCount`.
4. A Pod exists → has it existed long enough to fairly judge it (its own
   `creationTimestamp` is older than a fixed grace period) without
   becoming healthy? → not yet: requeue and wait, no mutation, no count
   change. → yes: transfer leadership if it's the leader (§4.5), delete
   the Pod, create a new one, then bump `RecreateCount`.

`Provisioning` and `Recreating` run the exact same ladder — the only
difference is which `Phase` it's entered from, and that for `Provisioning`
step 2's escalation is the first time `Phase` changes at all. Using the
Pod's own `creationTimestamp` for step 4's grace-period check, rather than
a separately persisted timestamp, means there's nothing extra to keep in
sync: the answer is always derivable live, the same discipline used
everywhere else in this design.

**Deciding what a member's Pod currently needs, and what pushes a `Ready`
member into `Recreating`.** On every reconcile, `EtcdClusterReconciler`
computes what the *current* observed state of each member actually needs,
from a live `Get` of its Pod (by its deterministic name) — no separate
identity-tracking field is needed to tell a stale Pod apart from a fresh
one; the Pod-recovery ladder above already answers that from Pod existence
and the Pod's own age:

| Observed state                                              | Action                                  | `Phase` written first      |
|-------------------------------------------------------------|-----------------------------------------|----------------------------|
| Pod/PVC/cert don't exist yet (first time)                   | create them                             | `Pending` → `Provisioning` |
| Pod missing out-of-band (was `Ready`)                       | recreate it                             | `Provisioning`             |
| Pod present, config/version drifted from `EtcdCluster.Spec` | recreate with latest config             | `Recreating`               |
| Pod present, matches desired config/version, but unhealthy  | recreate with the *same* config/version | `Recreating`               |
| Pod present, matches desired, healthy                       | nothing to do                           | *(unchanged)*              |

Every row that creates a Pod bumps `RecreateCount`, uniformly (see the
rule above) — only the last row, which takes no action at all, doesn't.
The first row and the config-drift row are the two ways `Phase` can change
*without* the member ever failing a health check — this table's job is
just detecting that a `Ready` member needs to leave `Ready` at all (or that
a brand-new one needs to start). Once `Phase` is `Provisioning`/
`Recreating`, the ladder above takes over every subsequent reconcile —
including its own config-drift check in step 1, which is what catches a
config change that lands *while* a member is already being recreated for
an unrelated reason. A member failing health checks purely because the
*cluster* has an active `CORRUPT`/`NOSPACE` alarm is explicitly not this
table's concern — §4.7 handles both before this table is even consulted
(§4.9's priority order).

```
(created) --join (see above, RecreateCount++ on Pod create)--> Provisioning --healthy + promoted--> Ready (RecreateCount reset)
Provisioning --Pod never becomes healthy--> Provisioning (ladder retries; RecreateCount++ after each) --RecreateCount == 3--> Replacing (RecreateCount reset)
Ready --Pod missing out-of-band--> Provisioning (RecreateCount++ after recreate) --Ready--> Ready (RecreateCount reset)
Ready --config/version drifted--> Recreating (RecreateCount++ after recreate) --Ready again--> Ready (RecreateCount reset)
Ready --unhealthy, config unchanged--> Recreating (ladder retries; RecreateCount++ after each) --Ready again--> Ready (RecreateCount reset)
Recreating --RecreateCount == 3, OR a CORRUPT alarm--> Replacing (RecreateCount reset; leave — see below) --> Pending (rejoin via step 1 above)
Ready --DeletionTimestamp set--> Terminating (leave — see below) --> object removed
```

`DeletionTimestamp` gets set the same way regardless of *why* — a
controller-initiated scale-in's plain `client.Delete()` and a human
operator manually deleting an `EtcdMember` out-of-band both land here
identically; the finalizer-driven leave sequence below doesn't (and can't)
distinguish the two.

Any transition above that deletes an existing Pod (`Recreating`,
`Replacing`, `Terminating`) attempts the leadership transfer from §4.5 first.

**A `CORRUPT` alarm on a member forces it directly to `Phase: Replacing`,
skipping the `RecreateCount == 3` wait entirely** — definitive evidence
beats a heuristic, and this overrides whatever that member's `Phase`
currently is. The priority this gets over every other action — including
one already in flight on a *different* member — is covered in §4.9.

**Leaving the cluster — shared by `Terminating` and `Replacing`.** Whether a
member is leaving for good — scale-in, or a human operator deleting its
`EtcdMember` directly — or leaving to immediately rejoin fresh
(`Replacing`), it's the same steps, each checked against live state so
resuming after a crash at any point is safe:

| # | Step                                                                                                                                                                    | "Already done?" check                                                                 |
|---|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| 1 | Leadership transfer (§4.5), if currently leader                                                                                                                         | best-effort, not itself retried                                                       |
| 2 | Find this member's current etcd identity — a live `MemberList` call, matched by this ordinal's deterministic peer URL, never `Status.MemberID` — then `MemberRemove` it | does a live `MemberList` no longer have an entry for that peer URL?                   |
| 3 | If a `CORRUPT` alarm is tagged with the ID found live in step 2, `AlarmDisarm` it                                                                                       | does a live `AlarmList` no longer show it?                                            |
| 4 | Delete the Pod                                                                                                                                                          | does the Pod still exist? ("done" means fully gone, not just `deletionTimestamp` set) |
| 5 | Delete the PVC                                                                                                                                                          | does the PVC still exist?                                                             |
| 6 | Delete the TLS certificate `Secret`, if any                                                                                                                             | does it still exist?                                                                  |

Step 4 must actually complete before step 5 can, not just be listed first:
Kubernetes' `pvc-protection` finalizer blocks a PVC from being reclaimed
while any Pod still references it, and (for `Replacing`) the new Pod/PVC
reuse the exact same ordinal-derived names as the old ones — so if the old
Pod isn't gone first, rejoining would silently reuse the old (possibly
corrupted) PVC, defeating the entire point of `Replacing`.

Step 3 exists only for the `CORRUPT`-triggered case, and sits right after
removal rather than at the end: the alarm (and the cluster's read-only
mode) is caused by the corrupted member's *presence*, not by its
replacement's absence, so the cluster can go back to read-write with N-1
members as soon as it's out.

**`Terminating`** runs steps 1–6, then removes the finalizer (§4.3), which
is what finally lets the `EtcdMember` object disappear — regardless of
whether the `Delete()` that set `DeletionTimestamp` came from
`EtcdClusterReconciler`'s own scale-in (a plain `client.Delete()` on the
highest-ordinal `EtcdMember`) or from a human operator deleting one
directly. The finalizer intercepts the deletion either way, so scale-in
needs no dedicated leave-sequence code of its own, and a manual deletion
gets the exact same safe cleanup instead of leaving a half-removed member
behind.

**`Replacing`** runs the same steps 1–6, then sets `Phase: Pending` instead
of removing the object — which sends the member straight back through
"Joining the cluster" above to rejoin fresh, exactly as if it were a brand
new ordinal. This reuses the join logic outright instead of duplicating a
second "add member, create Pod" implementation inside `Replacing`.

This is why step 2 never relies on `Status.MemberID`: that field can go
stale (last successfully observed, possibly loops ago) or simply not have
been written yet if the member never got far enough to be probed, and
either way it's a workaround for not having a better way to find the
member — which a live lookup by peer URL doesn't need, since the
deterministic peer URL is derivable from `Spec.Ordinal` alone and is valid
to query the instant the `EtcdMember` object exists, no prior successful
probe required.

### 4.7 Cluster alarm remediation: `CORRUPT` and `NOSPACE`

- **`CORRUPT`** isn't a separate state machine — it's tagged with a specific
  member ID in `AlarmList`'s response, and its fix is almost entirely the
  `Replacing` remedy §4.6 already has (it's the "real corruption detection
  signal" that section's `RecreateCount == 3` heuristic was written to
  anticipate). It only matters here for how it ranks in §4.9's priority
  order. Mapping that alarm's member ID to a specific `EtcdMember` — so the
  right one gets forced into `Phase: Replacing` — is a live `MemberList`
  lookup (match the ID, read its peer URL, derive the ordinal), the same
  peer-URL-keyed live lookup used everywhere else in this design, never a
  scan of the cluster's `EtcdMember`s' persisted `Status.MemberID` values.
- **`NOSPACE`** genuinely needs its own state machine: it isn't about any
  one member's identity, and while `Defragment` is per-endpoint, `Compact`
  and `Disarm` are cluster-wide single calls.

etcd raises a cluster-wide `NOSPACE` alarm (rejecting writes) when a
member's backend database approaches its storage quota. The remedy —
compact, defragment, disarm — needs to survive across reconcile loops
without ever redoing a completed step (requirement 12).

**Detection and reset both ride on the always-on refresh** (§4.2):
`EtcdClusterReconciler` calls `AlarmList()` every reconcile before deciding
what to do:

- `NOSPACE` reported, `Status.NoSpaceRemediation` is `nil` → start a cycle:
  `Phase: Compacting`, `CycleStarted: now`.
- `Status.NoSpaceRemediation` is not `nil`, but `NOSPACE` is no longer
  reported — **regardless of current `Phase`** — reset it to `nil` and
  stop. etcd never clears `NOSPACE` on its own, only an explicit `Disarm`
  does, so seeing it gone mid-cycle means something *other* than this
  cycle's own remedy cleared it — an operator running
  `etcdctl alarm disarm` manually, most plausibly, or this cycle's own
  `Disarm` call having actually landed after all (see below); checking
  this unconditionally means an out-of-band fix is noticed next reconcile
  instead of after the machine grinds through the rest of the sequence.
- Otherwise `NOSPACE` is still active: proceed per `Phase`, subject to one
  throttle that applies before anything else in `Compacting`: if the most
  recent `LastDefragTime` across all members is less than 10 minutes old,
  skip this reconcile (requeue) instead of mutating anything — otherwise a
  cycle that keeps having to restart (see the db-size check below) could
  drive compact and defragment back-to-back every reconcile.

| Phase           | Step                                                                                                                                  | "Already done for this cycle?"                          | Next                                                                                                                                                                                      |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Compacting`    | Non-blocking `Compact` of the keyspace to the current revision                                                                        | `Status.LastCompactTime` is after `CycleStarted`        | → `Defragmenting`                                                                                                                                                                         |
| `Defragmenting` | `Defragment` one member whose `LastDefragTime` is before `CycleStarted` (or unset); defrag the **leader last** to minimize disruption | every member's `LastDefragTime` is after `CycleStarted` | once finished: if every member's db size is now under 90% of quota, `AlarmDisarm` and clear `Status.NoSpaceRemediation` — done; otherwise bump `CycleStarted` and go back to `Compacting` |

There's no separate `Disarming` phase: whether to disarm is decided
dynamically off that db-size check, not tracked as a persisted phase of
its own.

`LastDefragTime` is set whether that member's defrag succeeded *or timed
out* (§4.3) — a member that reliably times out isn't worth retrying every
loop within the same cycle.

The db-size check exists because compact and defragment sometimes don't
reclaim enough on a single pass — `Compact` only discards historical
revisions and `Defragment` only returns pages already freed, so if the
live keyspace itself is close to quota, neither shrinks it — and
disarming an alarm that's just going to refire wastes the one signal
that's supposed to mean the remedy actually worked. Restarting from
`Compacting` in that case, rather than calling `Disarm` anyway, keeps the
cycle from ever disarming before it's actually justified.

**Priority (§4.9): `CORRUPT` ranks just below a human-triggered pause, an
already-started lost-quorum recovery, and cleaning up an already-`Terminating`
member; `NOSPACE` just below `CORRUPT` — both still above everything else.**
A member failing health checks
because of either alarm isn't a Pod problem — the generic repair path
(§4.6's table) would recreate the Pod, see it fail again, and waste
`RecreateCount` strikes on a problem recreating a Pod can't solve.

### 4.8 Lost-quorum recovery

Every remedy so far assumes quorum is intact — a healthy majority is always
available to make membership changes and elect a leader. When a majority of
members are permanently lost, none of §4.6/§4.7 can make progress (there's
no leader to serve writes, `MemberRemove`/`MemberAdd` can't commit), and the
only way forward is etcd's own disaster-recovery procedure: restart the
member with the most up-to-date data as a brand-new single-member cluster,
then rebuild the rest around it.

**Off by default, and always available to a human.** Given the blast
radius (this discards the other members' data), automatic recovery must be
an explicit opt-in (`EtcdClusterSpec` flag, off by default); when it's off
— or always, as a manual override — an operator can trigger it directly and
optionally choose which member (or a snapshot) to recover from, rather than
letting the controller pick automatically.

**Mechanism**, once triggered (automatically or manually):

1. Select the member with the highest Raft commit index (or the
   operator-specified one), and write `Status.QuorumRecovery =
   {Survivor: <ordinal>}` (§4.3) *before* touching anything — this is what
   durably marks recovery as started, both for step 3 below and for §4.9's
   priority check, and is the one thing that can't be re-derived live once
   other members start disappearing.
2. Check live state: is the survivor already running as a single-member
   cluster (i.e. has the `--force-new-cluster` recreate already taken
   effect)? If not, delete and recreate its Pod with that flag set. The
   flag itself isn't persisted anywhere — it only needs to be present for
   that one startup, so the *next* time this Pod is recreated for any other
   reason, it naturally starts without it.
3. Concurrently terminate every `EtcdMember` *other than*
   `Status.QuorumRecovery.Survivor` — the existing `Terminating` flow
   (§4.6) already handles etcd-side removal and Pod/PVC/cert cleanup; no
   new membership logic needed. (These other members are no longer part of
   any cluster the survivor recognizes, so the ordinary `MemberRemove`
   idempotency check in step 2 of §4.6's leave-sequence naturally becomes a
   no-op for them — only Pod/PVC/cert cleanup and object deletion actually
   happen.)
4. Once the survivor is a healthy single-member cluster and the others are
   gone, clear `Status.QuorumRecovery` (set it back to `nil`) — this is
   what tells §4.9 recovery is no longer under way. The cluster is now
   undersized; the *existing* scale-out path (§4.4/§4.6) brings it back to
   `EtcdCluster.Spec.Size` one member at a time, exactly as it would after
   any other scale-out.

`Status.QuorumRecovery` is the one exception to the rule right below it —
see its comment in §4.3 for why "are we already recovering" can't be
re-derived live the way every other sub-question here can.

**The rest of the status fields are observational only**
(`LastRecoveryTime`, `RecoveryCount`, `LastRecoveredFrom`, §4.3) —
`EtcdClusterReconciler` must never make a decision from them, only report
them; "is `--force-new-cluster` already applied" and "are the other
members gone yet" are answered from a
live read of the environment, the same discipline as everywhere else in
this design.

**Priority (§4.9): entry and continuation rank very differently.**
*Continuing* an already-started recovery ranks just below a human-triggered
pause — above `CORRUPT`, `NOSPACE`, and everything else — since with
quorum gone, none of them can reliably commit anything anyway (all need a
functioning majority). "Already started" is `Status.QuorumRecovery !=
nil` (§4.3) — the one persisted decision this section relies on, and
deliberately so: unlike "has `--force-new-cluster` taken effect" or "are
the other members gone," *whether we're recovering at all* isn't something
a live topology snapshot can answer, since the same "only one member
exists" picture also describes an ordinary bootstrap or a cluster
legitimately scaled down to one. *Deciding to start* recovery in the
first place ranks much lower — only after `CORRUPT`, `NOSPACE`, and a
per-member repair attempt have all had a chance to run and found nothing
to do, and the cluster is *still* unhealthy, does `EtcdClusterReconciler`
begin the mechanism above (§4.9). The reasoning: an unhealthy cluster
isn't necessarily a lost-quorum situation — an ordinary member repair
might restore quorum on its own — so this data-discarding remedy is a last
resort, never a first response. A pause (requirement 15) overrides both
stages — if an operator is manually intervening, the controller's own
automated recovery must not kick in underneath them, whether starting or
continuing.

### 4.9 Picking the one action to take this loop

Requirements 6, 9, 11, 12, 13, 14, and 15 all reduce to the same question —
"what's the one thing `EtcdClusterReconciler` is allowed to do this
reconcile?" — and they compose into a single priority order, evaluated
every reconcile after the always-on, read-only refresh. Each item is
checked in order; if an item finds nothing to actually mutate this loop
(nothing new to act on, and anything already in progress is just waiting
on a grace period), evaluation **falls through to the next item** rather
than stopping — only an item that actually takes a mutating action ends
the loop.

1. **`Spec.Paused` is set (requirement 15).** Do nothing else at all this
   loop. Checked first, ahead of everything below, including an
   already-started lost-quorum recovery — a human explicitly taking manual
   control must never have the controller's own automation kick in
   underneath them.
2. **Continue an already-started lost-quorum recovery (§4.8).** "Already
   started" is `Status.QuorumRecovery != nil` (§4.3) — a persisted marker,
   the one exception to this design's usual "decide from a live read"
   rule, since no live topology snapshot can distinguish mid-recovery from
   an ordinary bootstrap or a cluster legitimately scaled down to one
   member. Exempt from the gate (§4.2) and from waiting on anything else —
   with no quorum, nothing else can make
   progress regardless.
3. **Clean up any `EtcdMember` with `DeletionTimestamp` set (§4.6's
   `Terminating`).** Continue its leave sequence. Ranked here, right below
   an already-started lost-quorum recovery and above everything else that
   follows, because a member that's already leaving — whether from
   scale-in or a human manually removing it — is a settled matter, not
   something to weigh against other work: there's no reason to ever try to
   recreate/replace it (the repair ladder never applies to a `Terminating`
   member in the first place), it only ever touches its own Pod/PVC/cert/
   membership state so it can't mechanically conflict with a repair in
   flight on a different member, and leaving it parked behind CORRUPT/
   NOSPACE/repair/scale/upgrade decisions for many loops would needlessly
   keep a departing member's Pod and (if it's a voting member) its etcd
   membership around, skewing health/quorum/leadership-transfer
   calculations that the steps below rely on.
4. **`CORRUPT` alarm detected on some member (§4.6).** Force that member
   directly to `Phase: Replacing`, regardless of what any *other* member's
   `Phase` currently is — data corruption is more urgent than letting an
   unrelated repair finish on its own schedule, and the two don't
   mechanically conflict.
5. **`NOSPACE` alarm remediation (§4.7), if a cycle is active or new.**
   Exempt from the gate; ranked below `CORRUPT` but above everything else,
   and — for the same reason `CORRUPT` does — allowed to preempt a repair
   already in flight on a different member: a write-blocked cluster likely
   can't make progress on that repair either, and compact/defragment/disarm
   don't touch that other member's Pod or membership state.
6. **If a member is already `Recreating`, continue it (§4.6).** There's
   always at most one — this step itself is the only thing that ever
   starts one (requirement 6), so there's never more than one to pick
   among; just keep running its Pod-recovery ladder. Otherwise, if the
   refresh found one or more `Ready` members newly unhealthy, pick exactly
   one to start fixing (lowest ordinal, or "not the current leader" first,
   to reduce how often §4.5's transfer is needed) — *this* is the real
   selection, made once, at the moment a member is chosen to become
   `Recreating`.
7. **Otherwise, decide whether to *start* lost-quorum recovery (§4.8).** If
   the cluster still can't serve a linearizable request after steps 3–6
   found nothing to do, and recovery isn't already under way (step 2),
   begin it — the next reconcile that finds it already running picks it up
   via step 2. Deliberately low priority: recovery discards every other
   member's data, so it's tried only once the cheaper remedies above have
   had a chance to restore quorum on their own.
8. **Otherwise, advance whatever's left not-`Ready` (`Pending`/
   `Provisioning`/`Replacing`).** An existing learner always wins this slot
   — promoting it if it's caught up, otherwise simply waiting on it — over
   any *other* not-ready member, so that a different member's `Replacing`
   rejoin can never attempt a second `MemberAdd(learner)` while one is
   already pending (requirement 11). If there's no learner, pick the
   lowest-ordinal not-ready member and continue its progress.
9. **Otherwise — everything is `Ready` and healthy** — proceed to
   update-config / scale / upgrade (§4.2 is satisfied), which is also where
   bootstrapping ordinal 0 from zero members, and closing a pending ordinal
   gap on scale-out (§4.4), both happen.

Two things about this order are easy to misread. First, per-member repair
(step 6) outranks even *deciding* to start lost-quorum recovery (step 7) —
an unhealthy cluster isn't automatically a lost-quorum situation, and an
ordinary repair might resolve it without ever resorting to a remedy that
discards data. Second, promotion doesn't need its own early slot: an unpromoted learner is
never `Ready`, so step 6 — which only ever touches `Ready` members — never
competes with it. Requirement 11 is instead enforced by step 8's
learner-first tie-break, which never lets a *different* member's
`Replacing` rejoin attempt a second `MemberAdd(learner)` while one is
already pending.

An already-started lost-quorum recovery, cleaning up a `Terminating`
member, `CORRUPT`, and `NOSPACE` (steps 2–5) still preempt a repair already
in flight on a different member, per requirements 6/12/13: recovery
preempts because with quorum gone nothing else can reliably commit anyway;
the other three each act on a specific, already-determined member (the
`Terminating` one(s), the alarm's tagged member) rather than one chosen
from among candidates the way repair (step 6) does.

Nothing from step 6 on down preempts anything, but for two different
reasons. Steps 6, 8, and 9 each just pick one thing to work on and
otherwise wait their turn, the same as a repair running on some other
member would. Step 7 (starting a lost-quorum recovery) isn't waiting its
turn — it's deliberately held to last priority as a data-discarding
remedy, tried only once steps 3–6 have found nothing else to do (§4.8).

This priority order is a deliberate design decision: pause, lost-quorum
recovery, cleaning up a `Terminating` member, and both alarm remedies are
each dedicated steps with their own fixed rank, ahead of per-member repair,
rather than being folded into it. §4.12 walks through today's reconcile
loop phase by phase in these same terms.

### 4.10 PVC/PV and certificate ownership

Move PVC create+own from `EtcdCluster` to `EtcdMember`
(`controllerutil.SetControllerReference(etcdMember, pvc, scheme)` instead of
`ec`), named `etcd-data-{clusterName}-{ordinal}` as today; do the same for
the per-member TLS certificate `Secret`. This makes each `EtcdMember`
genuinely self-contained (Pod + PVC + cert), and lets Kubernetes GC cascade
their deletion when an `EtcdMember` is deleted (scale-in) or recreated
(`Replacing`) — see §4.6's leave-sequence for the explicit ordering
`Replacing`/`Terminating` still need beyond what GC alone would give.

### 4.11 Status: `EtcdMember.Status` and the `EtcdClusterStatus.Members` roll-up

Because there's one reconciler, both `EtcdMember.Status` (per member) and
`EtcdClusterStatus.Members`
([etcdcluster_types.go:171](https://github.com/etcd-io/etcd-operator/blob/59b848e9188a7776267896ed9f6f7dfb0e85956c/api/v1alpha1/etcdcluster_types.go#L171))
are written from the **same** live snapshot in the same reconcile pass —
there's no cross-controller staleness question to design around, and the
two views can't transiently disagree the way they would if two
independently-scheduled processes computed them.

One thing worth being deliberate about regardless: only call
`Status().Update()` when the content actually changed. `EtcdMember` is an
owned resource (§4.1), so every status write re-enqueues its parent
`EtcdCluster`; writing identical values every reconcile (which would
otherwise happen, since the refresh runs unconditionally) would mean every
reconcile immediately re-triggers another one, for every cluster, all the
time. Skipping the write when nothing changed means steady-state, healthy
clusters generate no extra reconciles at all.

`kubectl get etcdmembers` remains valuable on its own merits even though
`EtcdClusterStatus.Members` already exists — it's the only place
`Phase`/`RecreateCount`/`LastDefragTime` (per-member repair progress) are
visible at all.

### 4.12 Reconcile loop, phase by phase

Putting §4.1–§4.9 together (plus §4.13's cluster-deletion check up front),
one reconcile pass does, in order:

0. **Finalize check** (§4.13): fetch the `EtcdCluster`; if its
   `DeletionTimestamp` is set, run §4.13's finalize sequence instead and
   stop — nothing below runs this loop, not even the always-on refresh.
   This is checked before validation/bootstrap/health-check, matching where
   `reconcile_loop_v0.3.0.png`'s "Finalize cluster" box sits, right after
   "Fetch Resource."
1. **Always-on refresh** (§4.2, §4.11): live `MemberList`/`ClusterHealth`/
   `AlarmList`; `EtcdMember`/`EtcdClusterStatus.Members` roll-up. Never
   gated, never skipped, regardless of anything below — as long as the
   cluster isn't being deleted (step 0).
2. **§4.9's priority order** picks at most one mutating action: pause →
   continue an already-started lost-quorum recovery → clean up any
   `Terminating` member → `CORRUPT` → `NOSPACE` → per-member repair →
   decide whether to *start* lost-quorum recovery → promote a learner or
   advance whatever's left not-`Ready` → (only once every member is
   `Ready`) update-config / scale / upgrade. Each item falls through to the
   next if it finds nothing to mutate this loop (§4.9).

| Phase                                 | What it does                                                                                                                                                                                                                                                                                                              |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Finalize cluster                      | Only runs if `EtcdCluster.DeletionTimestamp` is set; short-circuits everything else in this table for that loop (§4.13).                                                                                                                                                                                                  |
| Pause                                 | Nothing else runs this loop (§4.9 item 1, requirement 15).                                                                                                                                                                                                                                                                |
| Continue lost-quorum recovery         | §4.8: force-new-cluster the most up-to-date member, terminate the rest, let scale-out rebuild. Off by default; only reachable once `Status.QuorumRecovery != nil` (§4.9 item 2).                                                                                                                                          |
| Clean up `Terminating` members        | Continues the leave sequence for any `EtcdMember` with `DeletionTimestamp` set (§4.6). Ranked above `CORRUPT`/`NOSPACE`/per-member repair (§4.9 item 3): a member that's already leaving is never worth trying to fix, only touches its own Pod/PVC/cert/membership state, and shouldn't linger behind other remediation. |
| `CORRUPT` alarm remediation           | Forces the tagged member directly to `Phase: Replacing` (§4.6) — reuses that state machine rather than a new one.                                                                                                                                                                                                         |
| `NOSPACE` alarm remediation           | §4.7's compact → defragment (one member per loop, leader last) → disarm cycle.                                                                                                                                                                                                                                            |
| Per-member repair                     | Continues the one member already `Recreating`, if any (there's never more than one); otherwise picks exactly one newly-unhealthy `Ready` member and starts `Recreating` it. Runs the shared Pod-recovery ladder (§4.6) — the same ladder a still-`Provisioning` member whose Pod never came up is already running.        |
| Start lost-quorum recovery            | Only once `CORRUPT`/`NOSPACE`/per-member repair found nothing to do and the cluster is still unhealthy (§4.9 item 7) — a last resort, since it discards every other member's data.                                                                                                                                        |
| Promote learner / advance not-`Ready` | An existing learner always wins this slot (requirement 11); otherwise the lowest-ordinal not-`Ready` member (`Pending`/`Provisioning`/`Replacing`, including bootstrapping ordinal 0) continues its progress.                                                                                                             |
| Update config                         | Decides *which* member updates first (highest ordinal, one at a time), runs the §4.5 transfer first if needed, then recreates the Pod.                                                                                                                                                                                    |
| Scale out/in                          | Scale-out creates an `EtcdMember` at the lowest missing ordinal, or `max+1` if there's no gap to reuse (§4.4), and does the join mechanics (§4.6). Scale-in calls plain `client.Delete()` on the highest ordinal; the actual cleanup happens via the finalizer-driven `Terminating` flow.                                 |
| Upgrade                               | Bumps one `EtcdMember.Spec.Version` at a time (highest ordinal, non-leader first as a preference); the member notices the drift and recreates — a "planned" recreate, bumping `RecreateCount` like any other Pod creation but immediately reset to 0 once healthy again (§4.6).                                           |

The last three rows only run once every existing `EtcdMember` is `Ready`
(§4.2's gate); everything above them runs regardless, per §4.9's ordering.

### 4.13 Finalizing an `EtcdCluster` (cluster deletion)

Requirement 16. Conceptually a *different* teardown path from §4.6's
`Terminating`: `Terminating` is what an individual `EtcdMember` goes
through while the rest of the cluster keeps running (scale-in, or a human
deleting one member by hand), so it earns the cost of a graceful,
one-at-a-time leave — transfer leadership away first, `MemberRemove`
before anything else, disarm a `CORRUPT` alarm tied to it. None of that
needs to be paid for one member at a time when *every* member is leaving
together, since the cluster those steps exist to protect won't exist a
moment later either.

**Checked first, before anything else in the reconcile loop.** After
fetching the `EtcdCluster` and the `EtcdMember`s/Pods it owns, the very
next thing this design does is check whether the `EtcdCluster` itself has
a `DeletionTimestamp` set — before validation, cluster prerequisites
(certs/TLS/Service), the always-on member-list/health refresh (§4.2), and
§4.9's priority order. This matches `reconcile_loop_v0.3.0.png`'s "Finalize
cluster" box, placed right after "Fetch Resource" and ahead of
"Validation," "Bootstrap etcd cluster," and "Health check & Failure
Recovery":

- **No** → make sure the cluster's own finalizer (§4.3) is present, adding
  it the first time this `EtcdCluster` is seen, then continue into
  validation/prereqs/refresh/dispatch exactly as §4.2–§4.9 already
  describe. This check runs ahead of `Spec.Paused` and spec validation
  deliberately, so a paused or currently-invalid cluster can still be
  deleted.
- **Yes** → run the finalize sequence below instead, and stop — nothing
  else this design describes runs this loop. Status also doesn't get
  refreshed on the way out: once every owned member is gone and the
  cluster's own finalizer is removed, the `EtcdCluster` object may already
  be gone by the time anything would try to write to it, so writing status
  is skipped whenever deletion is under way.

**Finalize sequence** (`reconcile_cluster_v0.3.0.png`, "Finalize cluster
case"), re-checked live every reconcile so a crash at any point resumes
safely:

1. **Are there zero `EtcdMember`s left?** → Yes: remove the `EtcdCluster`'s
   own finalizer and stop — this is what finally lets the `EtcdCluster`
   object itself disappear. No: continue to step 2.
2. **Otherwise, handle every owned `EtcdMember` independently, in the same
   pass** (not a barrier that waits for every member to reach the same
   state before acting on any of them):
   - A member that hasn't started deleting yet gets deleted (best effort;
     already-gone is fine) — this is what starts it leaving.
   - A member that's already mid-deletion (from an earlier loop's delete,
     or a human deleting that one `EtcdMember` directly) has its own
     finalizer removed right away, in the same pass — it does **not** wait
     for every sibling member to also reach "already deleting" first.
   Then requeue; the next reconcile re-checks from step 1, so a cluster
   with a mix of fresh and already-deleting members converges over a
   couple of loops rather than needing every member synchronized.

   Removing a member's finalizer unblocks Kubernetes' own garbage
   collector, which then deletes that member's owned Pod, PVC, and TLS
   certificate `Secret` (§4.10) — no controller code needed for that part.
   Once those are gone, Kubernetes finishes deleting the `EtcdMember`
   object itself, which — the same owned-resource watch as everywhere else
   in this design (§4.1) — re-enqueues the `EtcdCluster`, so a later
   reconcile eventually finds zero members left and takes step 1.

## 5. Alternatives considered

**A. No new CRD — extend `EtcdClusterStatus.Members` with retry state
(`RecreateCount`, `Phase`) and keep everything on the one object.** Viable
in principle, but no natural place for a finalizer to guarantee ordered
per-member cleanup, no natural owner reference for per-member Pod/PVC/cert
garbage collection, and worse observability than `kubectl get etcdmembers`.

**B. Two separate controllers: `EtcdClusterReconciler` and
`EtcdMemberReconciler`.** Split `EtcdMember` reconciliation into its own
controller, with `EtcdClusterReconciler` limited to cluster-level
*decisions* and `EtcdMemberReconciler` owning each member's entire etcd
membership lifecycle end to end. This needs real additional machinery to
make two independently-scheduled reconcilers coordinate safely: an async
`Spec.RecreateRequested`/`Status.ObservedRecreateRequested` counter pair (a
monotonic counter, not a bool, specifically to avoid a "who resets it"
ownership conflict), a strict single-writer-per-object rule for
`EtcdMember.Status` plus a rule that the cluster controller must never read
it for its own decisions (only a live etcd query, since a field written on
a different controller's cadence is inherently a beat behind), and a
resolved question of exactly which controller calls which etcd membership
API, since splitting a multi-step sequence across two controllers needs its
own crash-safe hand-off protocol.

Splitting `EtcdMember` reconciliation into its own controller would let
each `EtcdMember` be reconciled independently and concurrently, but that
isn't a problem worth solving: an etcd cluster typically has only 3 or 5
members. And even at fleet scale — hundreds of `EtcdCluster` objects — the
concurrency problem is already covered by tuning `MaxConcurrentReconciles`
on the single existing reconciler, with no need to split it into two.

## 6. Migration / compatibility

This project has not reached v1.0.0 (see [roadmap.md](../roadmap.md); v0.3.0
is the current in-progress milestone), and there's no evidence of a released
API-compatibility guarantee yet. **Assumption**: no live-migration path is
required for already-running `EtcdCluster` objects; introducing `EtcdMember`
and changing the Pod-ownership model can ship as a breaking change gated by
the CRD version.

## 7. Open questions

1. Is the "3 failed recreates" threshold, and the Pod-recovery ladder's
   (§4.6) grace period before an unhealthy Pod counts as a failed attempt,
   a hardcoded constant for v1, or should either be user-configurable via
   `EtcdClusterSpec`?
2. What's the exact API shape for a human operator to manually trigger
   lost-quorum recovery and pick a source member/snapshot (§4.8)? The
   mechanism (force-new-cluster + terminate + scale-out) is decided; the
   trigger surface (annotation? `EtcdClusterSpec` field? separate
   sub-resource?) isn't.
3. What's the detection threshold for "quorum lost" (§4.8) — how long must
   a majority be unreachable before this is even offered as an option,
   automatically or manually? Needs to be long enough to rule out a
   transient network partition.

## 8. Implementation plan

- **M1 — API**: add `api/v1alpha1/etcdmember_types.go` (types from §4.3),
  the `NoSpaceRemediationStatus`/`LastCompactTime`/`QuorumRecoveryStatus`/
  recovery-observability additions to `EtcdClusterStatus` (§4.3),
  `EtcdClusterSpec.Paused` (requirement 15),
  `EtcdClusterSpec.AllowVersionUpgradeOnRepair` (requirement 4, §4.6), the
  two finalizer name
  constants (`operator.etcd.io/cluster-cleanup` on `EtcdCluster`,
  `operator.etcd.io/member-cleanup` on `EtcdMember`, §4.3/§4.13),
  regenerate deepcopy/CRD manifests/RBAC, sample YAML under
  `config/samples/`.
- **M2 — Refactor `EtcdClusterReconciler` onto the new design**:
  `SetupWithManager` adds `Owns(&EtcdMember{})`; the `EtcdCluster`'s own
  finalizer and the cluster-deletion check and finalize sequence (§4.13)
  landed already, checked right after fetch, ahead of
  validation/bootstrap/health-check and of the rest of this list —
  including its interim shortcut of removing each `EtcdMember`'s finalizer
  directly (the same shortcut used for an ordinary single-member
  `Terminating` cleanup) instead of the real six-step leave sequence, which
  M3 below still needs to reconcile one way or the other; bootstrap creates
  `EtcdMember` ordinal 0 and its cert/PVC/Pod directly; `nextOrdinal`/
  highest-first replace `nextPodOrdinal` (§4.4, gap-aware); the readiness
  gate (§4.2) with all four exemptions; the reconcile loop restructured
  into §4.9's dispatcher shape — the `Spec.Paused` check ahead of
  everything else, and the priority order's fall-through behavior.
  `reconcileExceptions` is retired, not ported: the Pod-count-vs-member-
  count drift it exists to catch today is a crash-recovery workaround for
  not having a durable per-member marker, and §4.6's join/leave sequences
  (M3) now catch the same case per-member, from each `EtcdMember`'s own
  live state, without needing an aggregate count comparison. Every branch
  that isn't wired up yet (per-member repair,
  `CORRUPT`/`NOSPACE` remediation, lost-quorum recovery, the status
  roll-up) is left as an inline `// TODO(#xxx): <requirement/section>`
  no-op, so the loop compiles and its control flow is reviewable on its
  own before any of that behavior lands. M3–M6 below each resolve one of
  those TODOs.
- **M3 — Per-member mechanics** (resolves M2's join/promotion/repair/leave
  TODOs): join (`MemberAdd` + bootstrap special-case + cert/PVC/Pod
  create), promotion (`MemberPromote` + `IsLearnerReady`, today's
  `promoteLearner` retargeted at `EtcdMember`), the shared Pod-recovery
  ladder used by both `Provisioning` and `Recreating` (§4.6: grace-period
  check via the Pod's own `creationTimestamp`, bounded retries,
  `RecreateCount` bumped after each recreate rather than before, reset on
  both `Ready` and `Replacing`), the `Replacing` state machine (including
  the `CORRUPT` trigger and the `Replacing`-ends-at-`Pending` reuse of join
  logic), the `etcdutils.MoveLeader` wrapper and leadership-transfer helper
  (§4.5), the shared six-step leave sequence, `Terminating` via finalizer,
  and PVC/cert ownership on `EtcdMember` (§4.10). Also decide, for
  whole-cluster finalization (§4.13, landed in M2 as an interim shortcut),
  whether to route it through this same six-step leave sequence per member
  or keep relying on Pod/PVC/cert garbage collection alone — flagged as
  unresolved, not a settled design choice.
- **M4 — Alarm remediation** (resolves M2's `CORRUPT`/`NOSPACE` TODO):
  `etcdutils.AlarmList`/`AlarmDisarm`/`Compact` wrappers; `NOSPACE`'s
  `Compacting`/`Defragmenting` state machine (§4.7) including leader-last
  defrag ordering, the pre-disarm db-size check, and cycle-restart when
  that check (or `Disarm` itself) doesn't clear the alarm; `CORRUPT`
  detection wired into M3's `Replacing` trigger.
- **M5 — Lost-quorum recovery** (resolves M2's lost-quorum TODO): the
  opt-in flag, manual-trigger surface (pending Open Question 2),
  highest-commit-index selection, `--force-new-cluster` recreate,
  concurrent termination of the rest via the existing `Terminating` flow,
  and deferring to the existing scale-out path (§4.8).
- **M6 — Status** (resolves M2's status-roll-up TODO): `EtcdMember.Status`
  and `EtcdClusterStatus.Members` written from the same live snapshot each
  reconcile (§4.11); skip the write when unchanged; conditions.
- **M7 — Docs**: update `docs/api-references`, `config/samples`.
- **M8 — Tests**: gap-aware scale-out (reuses the lowest missing ordinal,
  falls back to max+1), ordinal correctness on scale in/out, "kill a Pod 3x
  → member replaced" via `gofail`, the shared Pod-recovery ladder's
  grace-period and RecreateCount-after-mutation behavior for both
  `Provisioning` and `Recreating`, PVC/cert ownership + finalizer-driven
  cleanup on scale-in, the readiness gate, leadership transfer,
  `Replacing`'s crash-resumability, requirement 11 (an existing learner
  always wins the not-`Ready` catch-all over any other not-ready member,
  so a second `MemberAdd(learner)` never gets attempted), `NOSPACE`'s
  resumability and cycle-restart behavior, `CORRUPT` pre-empting an
  in-flight repair on a different member, cleaning up a `Terminating`
  member pre-empting `CORRUPT`/`NOSPACE`/an in-flight repair on a different
  member, lost-quorum recovery end-to-end
  (continuation outranks `CORRUPT`/`NOSPACE`/per-member repair once
  started; starting it in the first place only happens after those find
  nothing to do), and `Spec.Paused`
  (no mutating action of any kind runs while set, including an
  already-triggered lost-quorum recovery; status/health refresh keeps
  updating; resuming picks up correctly with no special handling). Also
  whole-cluster deletion (§4.13): each not-yet-deleting `EtcdMember` gets
  deleted in the same pass (not gated on its siblings); each
  already-deleting member's finalizer is removed directly rather than
  running the six-step leave sequence, and Kubernetes' garbage collector is
  what actually removes each member's owned Pod/PVC/cert; the
  `EtcdCluster`'s own finalizer isn't removed until zero members are left;
  and a crash at any point in the sequence resumes correctly on the next
  reconcile.
- **M9 (stretch)**: real corruption detection for cases etcd's own
  `CORRUPT` alarm doesn't catch; PV/PVC retention-policy handling on
  replace.
