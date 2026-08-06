# Stress e2e: three batches, and the spinup-burst budget behind them

The stress e2e suite is deliberately split into **three batches that must not be mixed**. The
split is not cosmetic — it falls out of how much CPU an etcd bring-up actually burns, of how
gofail failpoints are scoped, and of what a 2-vCPU CI runner can physically do at once. This
doc records the measured numbers and the reasoning so the batching is not re-litigated.

## TL;DR

| Tier | What | Concurrency | Why |
|------|------|-------------|-----|
| **1 — cheap-parallel** | size-1 / size-3 bring-ups | parallelize freely | each bring-up is a few CPU-seconds; bootstrap member dominates |
| **2 — heavy-throttled** | size-7 bring-ups, scale churn | low width (~1 size-7 per 2 vCPU) | one size-7 spinup peaks **~1.4–3 cores**; 4 of them saturate a **10-core** VM |
| **3 — crash-exclusive** | `TestStressCrashDuringScale` | run alone | gofail failpoints are **operator-global**; arming one panics the single operator pod for *every* cluster |

The real lever for overlapping Tier-2 work is the reconcile worker pool
(`--max-concurrent-reconciles`, default 5), **not** namespace isolation — namespaces isolate
state, they do not buy you CPU.

## Measured on (honesty box)

> **All numbers below were measured on a Docker Desktop kind cluster with 10 CPUs / 7.75 GiB
> (`00_docker_envelope.txt`), NOT a 2-vCPU CI runner.** They are **spinup-cost measurements +
> extrapolation**. The 2-vCPU starvation burst this tiering is designed around was *not*
> reproduced at the CI core count — the local VM had too much headroom. Treat the per-size-7
> core peak as a measured cost and the "how many fit on 2 vCPU" as an **extrapolation,
> confidence medium**.

Method: W concurrent size-7 `EtcdCluster`s applied simultaneously, operator at
`--max-concurrent-reconciles=5`, polled to "7 voting members healthy". Per-etcd
`usage_usec` read from cgroup `cpu.stat` at end of spinup (clusters start from zero, so it
≈ CPU-seconds to reach healthy). W6 excluded as over-escalation beyond the envelope.

| W | per-cluster CPU-s (×7) | time-to-healthy s (min/med/max) | node peak busy-cores (of 10) | peak mem | throttled |
|---|------------------------|---------------------------------|------------------------------|----------|-----------|
| 1 | 8.1 | 70 / 70 / 70 | ~1.4 (coarse) | 1.14 GiB | 0 |
| 2 | ~21.8 | 75 / 75 / 75 | — | 1.52 GiB | 0 |
| 3 | ~14.9 | 77 / 77 / 81 | — | 1.90 GiB | 0 |
| 4 | ~62.8 | 102 / 121 / 142 | **12.34** | 2.28 GiB | 0 |

(Full data + the `docker stats` CPU% caveat: `/tmp/etcd-burst-stats/SUMMARY.md`. The hi-res
busy-core sampler exists only for W4; the `docker stats` CPU% column is jittery and not used
for load-bearing claims.)

## Tier 1 — cheap-parallel (size 1–3)

A whole **size-7** bring-up in isolation costs only **8.1 CPU-seconds** total, and it is
heavily front-loaded on the bootstrap member (ec-0 = 3.3 CPU-s, ~40% of the cluster) with each
later-joined member costing less (down to 0.25 CPU-s). A size-1 is therefore roughly one
member's worth of work and a size-3 roughly three — a few CPU-seconds each, spread over a
~70s window. These never come close to saturating a runner. **Parallelize them freely**; the
limit is test-harness bookkeeping, not CPU.

## Tier 2 — heavy-throttled (size 7, churn)

This is where the budget bites. The hi-res node sampler shows **4 simultaneous size-7 spinups
peaking at 12.34 busy cores on a 10-core VM** — i.e. they oversubscribe a 10-core machine.
Dividing the overlapped peak by 4 gives **~3 cores per concurrent size-7 spinup** at the burst;
a single isolated size-7 peaked ~1.4 cores. So budget **~1.4–3 cores of instantaneous peak per
size-7 bring-up.**

Consequence for a **2-vCPU** CI runner (extrapolation, confidence medium): **only ~1 size-7
spinup fits.** A second concurrent size-7 pushes instantaneous demand well past 2 cores; the
spinups don't fail (no CPU *limit* is set, so nothing is CFS-throttled — `nr_throttled=0` in
every run) but they self-throttle on available CPU and time-to-healthy stretches. So Tier 2
runs **at low width / throttled**, and the way you safely overlap a *little* Tier-2 work is by
sizing the reconcile worker pool — **`--max-concurrent-reconciles`** — to match the cores you
have, rather than relying on namespaces to "isolate" load. Namespaces isolate Kubernetes state;
they do nothing for CPU contention.

## Tier 3 — crash-exclusive (`TestStressCrashDuringScale`)

gofail failpoints are armed over HTTP on the **single operator pod** (`enableGoFailPoint` in
`helpers_test.go`, hitting the operator's gofail port). There is one operator reconciling every
cluster, so **arming a failpoint panics that one pod for all clusters at once** — it is a global
switch, not per-cluster. Any other stress cluster sharing the operator during a crash test gets
collateral reconcile failures and corrupts the result. `TestStressCrashDuringScale` therefore
**must run alone**, with no Tier-1 or Tier-2 work in flight.

