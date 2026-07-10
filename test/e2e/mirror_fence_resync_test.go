/*
Copyright 2025.

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

package e2e

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

const (
	frNamespace      = "mirror-fr"
	frSourceName     = "fr-src"
	frTargetName     = "fr-tgt"
	frStaleAgent     = "fence-stale-agent"
	fencePrefix      = "/fence/"
	compactPrefix    = "/compact/"
	quietPrefix      = "/quiet/"
	frFastReplWait   = 30 * time.Second
	frFastReplPoll   = 250 * time.Millisecond
	frConvergeWait   = 2 * time.Minute
	frRolloutWait    = 3 * time.Minute
	frEventWait      = time.Minute
	frOrphanSentinel = compactPrefix + "orphan-1"
)

func frSrcRef() etcdPodRef { return etcdPodRef{ns: frNamespace, pod: frSourceName + "-0"} }
func frTgtRef() etcdPodRef { return etcdPodRef{ns: frNamespace, pod: frTargetName + "-0"} }

// staleFenceAgentPod hand-renders the dual-writer configuration the
// controller guards refuse to create: a second agent on the live CR's link
// (same link-uid) at a STALE epoch. RestartPolicy Never and no readiness
// probe: the expected outcome is a permanent Failed /statusz, which keeps
// /readyz unready by design.
func staleFenceAgentPod(linkUID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      frStaleAgent,
			Namespace: frNamespace,
			Labels:    map[string]string{"app": frStaleAgent},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            frStaleAgent,
				Image:           imageName,
				ImagePullPolicy: corev1.PullIfNotPresent, // kind-loaded, never pulled
				// The image ENTRYPOINT is /manager; the same image ships the agent.
				Command: []string{"/mirror-agent"},
				Args: []string{
					"--link-uid=" + linkUID,
					"--epoch=1", // stale: the live CR is at generation >= 2
					"--source-endpoints=" + etcdClientEndpointNS(frNamespace, frSourceName),
					"--target-endpoints=" + etcdClientEndpointNS(frNamespace, frTargetName),
					"--source-prefix=" + fencePrefix,
					"--target-prefix=" + fencePrefix,
					"--http-bind-address=:8080",
				},
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: mirrorAgentPort}},
			}},
		},
	}
}

// waitForFenceEpoch waits until the reserved fence key on the target carries
// at least the given epoch with role Mirror — the only deterministic proof
// that the rolled agent generation owns the link (the CR's
// observedGeneration flips before the Recreate rollout completes).
func waitForFenceEpoch(
	ctx context.Context, t *testing.T, cfg *envconf.Config, ref etcdPodRef, prefix string, epoch int64,
) {
	t.Helper()
	var last mirroragent.FenceValue
	var seen bool
	err := wait.For(func(context.Context) (bool, error) {
		f, found, ferr := findFenceValue(t, cfg, ref, prefix)
		if ferr != nil || !found {
			return false, nil //nolint:nilerr // fence not written yet; keep polling
		}
		last, seen = f, true
		return f.Epoch >= epoch && f.Role == mirroragent.RoleMirror, nil
	}, wait.WithContext(ctx), wait.WithTimeout(frConvergeWait), wait.WithInterval(time.Second))
	if err != nil {
		t.Fatalf("fence under %q never reached epoch %d (seen: %t, last: %+v): %v", prefix, epoch, seen, last, err)
	}
}

func assessFenceLiveEpoch2(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	createMirror(ctx, t, cfg, newEtcdMirror(frNamespace, "fence", frSourceName, frTargetName, fencePrefix, nil))
	waitForMirrorPhase(ctx, t, cfg, frNamespace, "fence", ecv1alpha1.EtcdMirrorPhaseSyncing, frConvergeWait)

	// Any mutable spec field bumps the generation; the controller
	// renders --epoch=<generation>, so this Recreate-rolls the agent
	// to epoch 2 — the fence view the stale agent must lose against.
	patchMirror(ctx, t, cfg, frNamespace, "fence", func(em *ecv1alpha1.EtcdMirror) {
		em.Spec.Sync.MaxOpsPerSecond = 10000
	})
	waitForFenceEpoch(ctx, t, cfg, frTgtRef(), fencePrefix, 2)
	waitForMirror(ctx, t, cfg, frNamespace, "fence", "generation-2 agent syncing", frConvergeWait,
		func(em *ecv1alpha1.EtcdMirror) bool {
			return em.Status.ObservedGeneration == 2 &&
				em.Status.Phase == ecv1alpha1.EtcdMirrorPhaseSyncing &&
				mirrorConditionIs(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionTrue)
		})

	putKeys(t, cfg, frSrcRef(), fencePrefix+"key-", "val-", 0, 20)
	waitForMirrorDataMatch(ctx, t, cfg, frSrcRef(), frTgtRef(), fencePrefix, frFastReplWait, frFastReplPoll)
	return ctx
}

func assessFenceStaleAgentFails(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	em, err := getMirror(ctx, cfg, frNamespace, "fence")
	if err != nil {
		t.Fatalf("failed to get EtcdMirror fence: %v", err)
	}
	if err := cfg.Client().Resources().Create(ctx, staleFenceAgentPod(string(em.UID))); err != nil {
		t.Fatalf("failed to create stale agent pod: %v", err)
	}
	waitForPodRunning(ctx, t, cfg, frNamespace, frStaleAgent, 2*time.Minute)

	// A stale epoch dies at startup fence validation (permanent FenceError,
	// reason FenceLost, before any txn); the binary lingers serving the
	// terminal /statusz. The apply-time mod-revision CAS — a same-epoch
	// takeover losing its compare mid-stream — is unit-covered in
	// pkg/mirroragent; e2e exercises the cheap startup variant.
	var last mirroragent.Snapshot
	if err := wait.For(func(ctx context.Context) (bool, error) {
		s, serr := getStatuszFor(ctx, cfg, frNamespace, frStaleAgent)
		if serr != nil {
			return false, nil //nolint:nilerr // proxy may 503 while the pod settles; keep polling
		}
		last = s
		return s.Phase == mirroragent.PhaseFailed && s.LastErrorReason == "FenceLost", nil
	}, wait.WithContext(ctx), wait.WithTimeout(2*time.Minute), wait.WithInterval(2*time.Second)); err != nil {
		dumpAgentDiagnosticsFor(ctx, t, cfg, frNamespace, frStaleAgent)
		t.Fatalf("stale agent never failed with FenceLost (last: %+v): %v", last, err)
	}
	if last.KeysAppliedTotal != 0 {
		t.Fatalf("stale agent applied %d keys; a fenced-out writer must write nothing", last.KeysAppliedTotal)
	}
	return ctx
}

func assessFenceLiveConverges(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	putKeys(t, cfg, frSrcRef(), fencePrefix+"key-", "val-", 20, 30)
	waitForMirrorDataMatch(ctx, t, cfg, frSrcRef(), frTgtRef(), fencePrefix, frFastReplWait, frFastReplPoll)

	// Not a point read: on an idle source Available legally dips to
	// ProgressStalled and only recovers on the 15s status poll after the
	// puts above. ForcedResyncCount is monotonic, so waiting on ==0 stays
	// a hard "never happened during the overlap" check.
	waitForMirror(ctx, t, cfg, frNamespace, "fence", "Syncing/Available with zero forced resyncs",
		30*time.Second, func(em *ecv1alpha1.EtcdMirror) bool {
			return em.Status.Phase == ecv1alpha1.EtcdMirrorPhaseSyncing &&
				em.Status.ForcedResyncCount == 0 &&
				mirrorConditionIs(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionTrue)
		})

	srcDump, serr := etcdctlJSONDump(t, cfg, frSrcRef(), fencePrefix)
	tgtDump, derr := etcdctlJSONDump(t, cfg, frTgtRef(), fencePrefix)
	if serr != nil || derr != nil {
		t.Fatalf("failed to dump %q: source %v, target %v", fencePrefix, serr, derr)
	}
	requireByteExact(t, srcDump, tgtDump)
	return ctx
}

func assessFenceCleanup(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	stale := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: frStaleAgent, Namespace: frNamespace}}
	if err := cfg.Client().Resources().Delete(ctx, stale); err != nil {
		t.Fatalf("failed to delete stale agent pod: %v", err)
	}
	deleteMirrorAndWait(ctx, t, cfg, frNamespace, "fence")

	_, found, err := findFenceValue(t, cfg, frTgtRef(), fencePrefix)
	if err != nil {
		t.Fatalf("failed to read fence range after deletion: %v", err)
	}
	if found {
		t.Fatalf("reserved checkpoint key under %q survived CR deletion", fencePrefix)
	}
	return ctx
}

func assessCompactBaseline(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	createMirror(ctx, t, cfg,
		newEtcdMirror(frNamespace, "compact", frSourceName, frTargetName, compactPrefix, nil))
	waitForMirrorSyncingAvailable(ctx, t, cfg, frNamespace, "compact", frConvergeWait)

	putKeys(t, cfg, frSrcRef(), compactPrefix+"key-", "val-", 0, 30)
	waitForMirrorDataMatch(ctx, t, cfg, frSrcRef(), frTgtRef(), compactPrefix, frFastReplWait, frFastReplPoll)

	em, err := getMirror(ctx, cfg, frNamespace, "compact")
	if err != nil {
		t.Fatalf("failed to get EtcdMirror compact: %v", err)
	}
	if em.Status.ForcedResyncCount != 0 {
		t.Fatalf("baseline forcedResyncCount is %d, want 0", em.Status.ForcedResyncCount)
	}
	return ctx
}

func assessCompactBlindWindow(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	// spec.paused scales the Deployment to zero — a deterministic
	// blind window (no agent, watch closed at the checkpoint).
	patchMirror(ctx, t, cfg, frNamespace, "compact", func(em *ecv1alpha1.EtcdMirror) {
		em.Spec.Paused = true
	})
	waitForMirrorPhase(ctx, t, cfg, frNamespace, "compact", ecv1alpha1.EtcdMirrorPhasePaused, 90*time.Second)
	waitForAgentPodsGone(ctx, t, cfg, frNamespace, "compact", 90*time.Second)

	// Churn the source past the checkpoint, then compact it away so
	// the resumed watch at watermark+1 hits CompactRevision.
	putKeys(t, cfg, frSrcRef(), compactPrefix+"blind-", "bval-", 0, 20)
	delKeys(t, cfg, frSrcRef(), compactPrefix+"key-", 0, 5)
	rev := putWithRevision(t, cfg, frSrcRef(), compactPrefix+"blind-marker", "mark")
	if _, err := execEtcdctl(t, cfg, frSrcRef(),
		"compaction", "--physical", strconv.FormatInt(rev, 10)); err != nil {
		// execEtcdctl retries: a retry after a half-committed first
		// attempt reports "required revision has been compacted",
		// which IS the success condition here.
		if !strings.Contains(err.Error(), "compacted") {
			t.Fatalf("failed to compact source at revision %d: %v", rev, err)
		}
	}

	// Orphan planted directly on the target during the blind window;
	// only the mandatory mark-and-sweep can remove it.
	if _, err := execEtcdctl(t, cfg, frTgtRef(), "put", frOrphanSentinel, "planted"); err != nil {
		t.Fatalf("failed to plant target orphan: %v", err)
	}
	return ctx
}

func assessCompactResume(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	patchMirror(ctx, t, cfg, frNamespace, "compact", func(em *ecv1alpha1.EtcdMirror) {
		em.Spec.Paused = false
	})
	// Ceiling covers pod start + compact detection + the 15s status
	// poll cadence. Compacted=True/ForcedResync is logged best-effort
	// only: on a tiny keyspace the resync window can close inside one
	// status poll.
	sawCompacted := false
	waitForMirror(ctx, t, cfg, frNamespace, "compact", "forcedResyncCount >= 1", frRolloutWait,
		func(em *ecv1alpha1.EtcdMirror) bool {
			if mirrorConditionIs(em, ecv1alpha1.EtcdMirrorConditionCompacted, metav1.ConditionTrue) {
				sawCompacted = true
			}
			return em.Status.ForcedResyncCount >= 1
		})
	t.Logf("Compacted=True observed in flight: %t (best-effort; the window can close inside one poll)",
		sawCompacted)
	// The event note embeds the trigger — the deterministic binding of this
	// resync to the compaction (an agent that resynced at every restart
	// would satisfy count>=1 alone).
	waitForMirrorEvent(ctx, t, cfg, frNamespace, "compact",
		ecv1alpha1.EtcdMirrorEventForcedResyncStarted,
		"trigger: "+string(mirroragent.ResyncReasonCompacted), frEventWait)
	return ctx
}

func assessCompactReconverges(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	// Byte-exact convergence simultaneously proves: orphan pruned,
	// blind puts applied, blind deletes not resurrected.
	waitForMirrorDataMatch(ctx, t, cfg, frSrcRef(), frTgtRef(), compactPrefix, 2*time.Minute, time.Second)

	tgtDump, err := etcdctlJSONDump(t, cfg, frTgtRef(), compactPrefix)
	if err != nil {
		t.Fatalf("failed to dump target under %q: %v", compactPrefix, err)
	}
	if _, ok := tgtDump[frOrphanSentinel]; ok {
		t.Fatalf("target orphan %q survived the mark-and-sweep", frOrphanSentinel)
	}
	if _, ok := tgtDump[compactPrefix+"key-00"]; ok {
		t.Fatalf("blind-window delete of %q resurrected on the target", compactPrefix+"key-00")
	}
	deleteMirrorAndWait(ctx, t, cfg, frNamespace, "compact")
	return ctx
}

func assessQuietReplication(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	putKeys(t, cfg, frSrcRef(), quietPrefix+"key-", "val-", 10, 15)
	waitForMirrorDataMatch(ctx, t, cfg, frSrcRef(), frTgtRef(), quietPrefix, frFastReplWait, frFastReplPoll)
	return ctx
}

func assessQuietMetrics(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	// #415 coverage in a real cluster: the manager's own /metrics
	// (plain HTTP via the e2e overlay), matched by
	// all-substrings-on-one-line, not exact label formatting.
	assertManagerMetric(ctx, t, cfg,
		"etcd_mirror_phase{", `name="quiet"`, `namespace="`+frNamespace+`"`, `phase="Syncing"`)
	assertManagerMetric(ctx, t, cfg,
		"etcd_mirror_condition{", `name="quiet"`, `namespace="`+frNamespace+`"`,
		`status="true"`, `type="Available"`)
	return ctx
}

// TestMirrorFenceAndResync exercises scenarios 1-3 of the EtcdMirror
// lifecycle suite against one shared pair of cleartext clusters, isolated by
// per-scenario prefixes and per-scenario CRs (fresh link-uid => fresh
// genesis): dual-agent fence overlap, compaction forced-resync with
// mark-and-sweep, and quiet-prefix pod-restart resume — plus the controller
// metrics assert (#415).
func TestMirrorFenceAndResync(t *testing.T) {
	feature := features.New("mirror-fence-and-resync")

	// Scenario-3 identity snapshot, shared across assessments.
	var quietPod string
	var quietRev int64
	var quietResyncs int32

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		ctx = setupMirrorNamespace(ctx, t, cfg, frNamespace)
		createEtcdClusterInNS(ctx, t, cfg, frNamespace, frSourceName)
		createEtcdClusterInNS(ctx, t, cfg, frNamespace, frTargetName)
		waitForSTSReadyInNS(ctx, t, cfg, frNamespace, frSourceName)
		waitForSTSReadyInNS(ctx, t, cfg, frNamespace, frTargetName)
		return ctx
	})

	feature.Assess("s1: live mirror converges at epoch 2", assessFenceLiveEpoch2)

	feature.Assess("s1: stale-epoch agent fails loudly with zero writes", assessFenceStaleAgentFails)

	feature.Assess("s1: live agent keeps converging byte-exact", assessFenceLiveConverges)

	feature.Assess("s1: CR deletion cleans the fence key", assessFenceCleanup)

	feature.Assess("s2: mirror converges (baseline)", assessCompactBaseline)

	feature.Assess("s2: paused blind window: churn, compact, plant orphan", assessCompactBlindWindow)

	feature.Assess("s2: resume forces resync and prunes the orphan", assessCompactResume)

	feature.Assess("s2: reconverges byte-exact", assessCompactReconverges)

	feature.Assess("s3: mirror converges",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			createMirror(ctx, t, cfg, newEtcdMirror(frNamespace, "quiet", frSourceName, frTargetName, quietPrefix, nil))
			waitForMirrorSyncingAvailable(ctx, t, cfg, frNamespace, "quiet", frConvergeWait)
			putKeys(t, cfg, frSrcRef(), quietPrefix+"key-", "val-", 0, 10)
			waitForMirrorDataMatch(ctx, t, cfg, frSrcRef(), frTgtRef(), quietPrefix, frFastReplWait, frFastReplPoll)

			em, err := getMirror(ctx, cfg, frNamespace, "quiet")
			if err != nil {
				t.Fatalf("failed to get EtcdMirror quiet: %v", err)
			}
			requireMirrorCondition(t, em, ecv1alpha1.EtcdMirrorConditionInitialSyncComplete, metav1.ConditionTrue)
			quietPod, quietRev, quietResyncs =
				em.Status.AgentPod, em.Status.LastAppliedRevision, em.Status.ForcedResyncCount
			if quietPod == "" {
				t.Fatal("status.agentPod is empty on a syncing mirror")
			}
			return ctx
		})

	feature.Assess("s3: deleted agent pod resumes same identity",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: quietPod, Namespace: frNamespace}}
			if err := cfg.Client().Resources().Delete(ctx, pod); err != nil {
				t.Fatalf("failed to delete agent pod %s: %v", quietPod, err)
			}
			em := waitForMirror(ctx, t, cfg, frNamespace, "quiet", "replacement agent pod syncing", frRolloutWait,
				func(em *ecv1alpha1.EtcdMirror) bool {
					return em.Status.AgentPod != "" && em.Status.AgentPod != quietPod &&
						em.Status.Phase == ecv1alpha1.EtcdMirrorPhaseSyncing &&
						mirrorConditionIs(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionTrue)
				})

			// Same-identity resume: same epoch (generation unchanged), no
			// forced resync, RequireEmpty never re-armed, watermark preserved
			// (resume from the fenced checkpoint, not genesis).
			if em.Status.ForcedResyncCount != quietResyncs {
				t.Fatalf("pod restart bumped forcedResyncCount: %d -> %d", quietResyncs, em.Status.ForcedResyncCount)
			}
			requireMirrorCondition(t, em, ecv1alpha1.EtcdMirrorConditionCompacted, metav1.ConditionFalse)
			requireMirrorCondition(t, em, ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation, metav1.ConditionFalse)
			requireMirrorCondition(t, em, ecv1alpha1.EtcdMirrorConditionInitialSyncComplete, metav1.ConditionTrue)
			if em.Status.LastAppliedRevision < quietRev {
				t.Fatalf("watermark regressed across the pod restart: %d < %d", em.Status.LastAppliedRevision, quietRev)
			}
			return ctx
		})

	feature.Assess("s3: replication still works", assessQuietReplication)

	feature.Assess("s3: controller metrics expose the mirror", assessQuietMetrics)

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		deleteMirrorFixture(ctx, t, cfg, frNamespace)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
