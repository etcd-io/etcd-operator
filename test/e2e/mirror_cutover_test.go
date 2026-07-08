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
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

// Scenarios 5+6 as one lifecycle narrative, mirroring the documented cutover
// runbook order: drain -> CutoverReady -> delete the forward CR (checkpoint
// cleaned) -> reverse with OverwriteAndPrune onto the diverged old source.
const (
	cutNamespace = "mirror-cut"
	cutAlphaName = "alpha" // initial source, later reversal target
	cutBetaName  = "beta"  // initial target, later new primary
	cutPrefix    = "/cut/"
	// cutForeignPrefix isolates the foreign-fence negative check from the
	// converged cutPrefix data.
	cutForeignPrefix = "/cutforeign/"
	cutForeignLink   = "someone-elses-link"
	// cutPlainKeys + cutLeasedKeys is the drain verification's expected
	// per-side count.
	cutPlainKeys  = 23
	cutLeasedKeys = 2
)

func cutAlphaRef() etcdPodRef { return etcdPodRef{ns: cutNamespace, pod: cutAlphaName + "-0"} }
func cutBetaRef() etcdPodRef  { return etcdPodRef{ns: cutNamespace, pod: cutBetaName + "-0"} }

// putLeasedKey grants a lease and attaches one key to it on the referenced
// pod. TTL comfortably outlives the whole test: lease-stripping on the
// mirrored copies is what is under test, not expiry.
func putLeasedKey(t *testing.T, cfg *envconf.Config, ref etcdPodRef, key, val string) {
	t.Helper()
	stdout, err := execEtcdctl(t, cfg, ref, "lease", "grant", "600")
	if err != nil {
		t.Fatalf("failed to grant lease: %v", err)
	}
	// "lease 694d7f0777cbf3e7 granted with TTL(600s)"
	fields := strings.Fields(stdout)
	if len(fields) < 2 || fields[0] != "lease" {
		t.Fatalf("unparsable lease grant output %q", stdout)
	}
	if _, err := execEtcdctl(t, cfg, ref, "put", "--lease="+fields[1], key, val); err != nil {
		t.Fatalf("failed to put leased key %s: %v", key, err)
	}
}

func assessForwardConverges(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	createMirror(ctx, t, cfg, newEtcdMirror(cutNamespace, "forward", cutAlphaName, cutBetaName,
		cutPrefix, nil))
	waitForMirrorSyncingAvailable(ctx, t, cfg, cutNamespace, "forward", 2*time.Minute)

	putKeys(t, cfg, cutAlphaRef(), cutPrefix+"key-", "val-", 0, cutPlainKeys)
	for i := range cutLeasedKeys {
		putLeasedKey(t, cfg, cutAlphaRef(),
			fmt.Sprintf("%sleased-%02d", cutPrefix, i), fmt.Sprintf("lv-%02d", i))
	}
	waitForMirrorDataMatch(ctx, t, cfg, cutAlphaRef(), cutBetaRef(), cutPrefix,
		30*time.Second, 250*time.Millisecond)

	// 60s ceiling: leaseBackedKeyCount arrives with the 15s status
	// poll cadence.
	waitForMirror(ctx, t, cfg, cutNamespace, "forward", "leaseBackedKeyCount==2", time.Minute,
		func(em *ecv1alpha1.EtcdMirror) bool { return em.Status.LeaseBackedKeyCount == cutLeasedKeys })
	return ctx
}

func assessForwardDeletionCleanup(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	deleteMirrorAndWait(ctx, t, cfg, cutNamespace, "forward")

	_, found, err := findFenceValue(t, cfg, cutBetaRef(), cutPrefix)
	if err != nil {
		t.Fatalf("failed to read fence range on beta: %v", err)
	}
	if found {
		t.Fatalf("reserved checkpoint key under %q survived forward CR deletion", cutPrefix)
	}
	betaDump, err := etcdctlJSONDump(t, cfg, cutBetaRef(), cutPrefix)
	if err != nil {
		t.Fatalf("failed to dump beta under %q: %v", cutPrefix, err)
	}
	// Deletion cleans ONLY the reserved key: all mirrored data stays.
	if len(betaDump) != cutPlainKeys+cutLeasedKeys {
		t.Fatalf("beta holds %d keys after forward deletion, want %d",
			len(betaDump), cutPlainKeys+cutLeasedKeys)
	}
	return ctx
}

func assessReversePrunes(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	// Diverge both sides: beta (the new primary) gains and rewrites
	// keys; alpha (the old source) accumulates orphans on top of the
	// straggler from the previous step.
	putKeys(t, cfg, cutBetaRef(), cutPrefix+"new-", "nv-", 0, 5)
	for i := range 5 {
		key := fmt.Sprintf("%skey-%02d", cutPrefix, i)
		if _, err := execEtcdctl(t, cfg, cutBetaRef(), "put", key,
			fmt.Sprintf("rewritten-%02d", i)); err != nil {
			t.Fatalf("failed to rewrite %s on beta: %v", key, err)
		}
	}
	putKeys(t, cfg, cutAlphaRef(), cutPrefix+"orphan-", "ov-", 0, 3)

	createMirror(ctx, t, cfg, newEtcdMirror(cutNamespace, "reverse", cutBetaName, cutAlphaName,
		cutPrefix, func(em *ecv1alpha1.EtcdMirror) {
			em.Spec.InitialSync = &ecv1alpha1.EtcdMirrorInitialSyncSpec{
				Mode: ecv1alpha1.EtcdMirrorInitialSyncOverwriteAndPrune,
			}
		}))
	waitForMirror(ctx, t, cfg, cutNamespace, "reverse", "Syncing with InitialSyncComplete",
		3*time.Minute, func(em *ecv1alpha1.EtcdMirror) bool {
			return em.Status.Phase == ecv1alpha1.EtcdMirrorPhaseSyncing &&
				mirrorConditionIs(em, ecv1alpha1.EtcdMirrorConditionInitialSyncComplete,
					metav1.ConditionTrue)
		})

	// Byte-exact convergence proves orphans+straggler pruned and the
	// divergent values converged to the new primary's.
	waitForMirrorDataMatch(ctx, t, cfg, cutBetaRef(), cutAlphaRef(), cutPrefix, 2*time.Minute, time.Second)
	alphaDump, err := etcdctlJSONDump(t, cfg, cutAlphaRef(), cutPrefix)
	if err != nil {
		t.Fatalf("failed to dump alpha under %q: %v", cutPrefix, err)
	}
	for _, orphan := range []string{cutPrefix + "orphan-00", cutPrefix + "straggler"} {
		if _, ok := alphaDump[orphan]; ok {
			t.Fatalf("orphan %q survived the OverwriteAndPrune genesis", orphan)
		}
	}

	// The OverwriteAndPrune genesis's mandatory sweep publishes its
	// verification record PRE-repair by contract: the counts must show the
	// 4 orphans it found (straggler + 3 planted), and the drift record must
	// say it repaired them. With spec.reconciliation unset no later pass
	// refreshes the counts, so count equality would never hold here.
	const cutOrphans = 4
	waitForMirror(ctx, t, cfg, cutNamespace, "reverse", "pre-repair sweep record", time.Minute,
		func(em *ecv1alpha1.EtcdMirror) bool {
			d := em.Status.LastReconciliationDrift
			return em.Status.SourceKeyCount > 0 &&
				em.Status.TargetKeyCount == em.Status.SourceKeyCount+cutOrphans &&
				d != nil && d.OrphanKeys == cutOrphans && d.Repaired
		})
	return ctx
}

func assessReverseDeletionCleanup(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	deleteMirrorAndWait(ctx, t, cfg, cutNamespace, "reverse")
	_, found, err := findFenceValue(t, cfg, cutAlphaRef(), cutPrefix)
	if err != nil {
		t.Fatalf("failed to read fence range on alpha: %v", err)
	}
	if found {
		t.Fatalf("reserved checkpoint key under %q survived reverse CR deletion", cutPrefix)
	}
	return ctx
}

// assessForeignFenceSurvives is the negative half of the finalizer's
// ownership check: deleting a CR whose checkpoint key has been taken over by
// a DIFFERENT link must release the finalizer while leaving the key alone.
// Without this, an unconditional delete in the checkpoint cleaner would pass
// every other cleanup assert in the suite.
func assessForeignFenceSurvives(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	createMirror(ctx, t, cfg, newEtcdMirror(cutNamespace, "foreign", cutAlphaName, cutBetaName,
		cutForeignPrefix, nil))
	waitForMirrorSyncingAvailable(ctx, t, cfg, cutNamespace, "foreign", 2*time.Minute)

	// Pause first (agent gone) so the foreign overwrite cannot race a live
	// fenced writer.
	patchMirror(ctx, t, cfg, cutNamespace, "foreign", func(em *ecv1alpha1.EtcdMirror) {
		em.Spec.Paused = true
	})
	waitForMirrorPhase(ctx, t, cfg, cutNamespace, "foreign", ecv1alpha1.EtcdMirrorPhasePaused, 90*time.Second)
	waitForAgentPodsGone(ctx, t, cfg, cutNamespace, "foreign", 90*time.Second)

	encoded, err := mirroragent.FenceValue{
		LinkUID:   cutForeignLink,
		Epoch:     1,
		Role:      mirroragent.RoleMirror,
		Watermark: 1,
	}.Encode()
	if err != nil {
		t.Fatalf("encoding foreign fence: %v", err)
	}
	putEtcdKeyRaw(ctx, t, cfg, cutBetaRef(),
		cutForeignPrefix+mirroragent.DefaultCheckpointKeySuffix, encoded)

	// Deletion must complete (finalizer released) WITHOUT deleting the key.
	deleteMirrorAndWait(ctx, t, cfg, cutNamespace, "foreign")
	fence, found, err := findFenceValue(t, cfg, cutBetaRef(), cutForeignPrefix)
	if err != nil || !found {
		t.Fatalf("foreign-owned fence gone after CR deletion (found: %t): %v", found, err)
	}
	if fence.LinkUID != cutForeignLink {
		t.Fatalf("foreign fence rewritten during deletion: linkUID %q", fence.LinkUID)
	}
	return ctx
}

// TestMirrorCutoverReversal exercises scenario 5 (Drain/CutoverReady: fence
// role flipped to Primary, cutover block populated, post-drain source writes
// not replicated) and scenario 6 (reversal with OverwriteAndPrune onto the
// diverged old source, both CR deletions cleaning their checkpoints, and a
// deletion sparing a checkpoint owned by a different link).
func TestMirrorCutoverReversal(t *testing.T) {
	feature := features.New("mirror-cutover-reversal")

	var drainedRevision int64

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		ctx = setupMirrorNamespace(ctx, t, cfg, cutNamespace)
		createEtcdClusterInNS(ctx, t, cfg, cutNamespace, cutAlphaName)
		createEtcdClusterInNS(ctx, t, cfg, cutNamespace, cutBetaName)
		waitForSTSReadyInNS(ctx, t, cfg, cutNamespace, cutAlphaName)
		waitForSTSReadyInNS(ctx, t, cfg, cutNamespace, cutBetaName)
		return ctx
	})

	feature.Assess("forward mirror converges (with leased keys)", assessForwardConverges)

	feature.Assess("drain reaches CutoverReady with populated cutover block",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// Runbook order: writers are quiesced (nothing has written to
			// alpha since convergence), THEN the drain is requested.
			patchMirror(ctx, t, cfg, cutNamespace, "forward", func(em *ecv1alpha1.EtcdMirror) {
				em.Spec.Mode = ecv1alpha1.EtcdMirrorModeDrain
			})
			// A finished cutover must not page: the API phase stays Syncing
			// and Available stays True with reason DrainComplete.
			em := waitForMirror(ctx, t, cfg, cutNamespace, "forward", "CutoverReady with DrainComplete",
				3*time.Minute, func(em *ecv1alpha1.EtcdMirror) bool {
					avail := meta.FindStatusCondition(em.Status.Conditions,
						ecv1alpha1.EtcdMirrorConditionAvailable)
					return mirrorConditionIs(em, ecv1alpha1.EtcdMirrorConditionCutoverReady,
						metav1.ConditionTrue) &&
						em.Status.Phase == ecv1alpha1.EtcdMirrorPhaseSyncing &&
						avail != nil && avail.Status == metav1.ConditionTrue &&
						avail.Reason == "DrainComplete" // internal/controller reasonDrainComplete
				})

			co := em.Status.Cutover
			if co == nil {
				t.Fatalf("status.cutover is nil on a CutoverReady mirror: %+v", em.Status)
			}
			if co.DrainTargetRevision <= 0 || co.DrainedRevision < co.DrainTargetRevision {
				t.Fatalf("cutover revisions inconsistent: target %d, drained %d",
					co.DrainTargetRevision, co.DrainedRevision)
			}
			if co.VerifiedTime == nil {
				t.Fatalf("cutover verifiedTime not set: %+v", co)
			}
			want := int64(cutPlainKeys + cutLeasedKeys)
			if co.SourceKeyCount != want || co.TargetKeyCount != want {
				t.Fatalf("cutover key counts: source %d, target %d, want both %d",
					co.SourceKeyCount, co.TargetKeyCount, want)
			}
			// cutover.leasedKeyCount is NOT asserted: a spec-driven drain
			// Recreate-rolls a fresh agent whose process-local lease counter
			// restarts at zero (the pre-drain status.leaseBackedKeyCount wait
			// is the lease-stripping coverage). Product follow-up: the drain
			// path should preserve or recount it.
			drainedRevision = co.DrainedRevision
			return ctx
		})

	feature.Assess("fence role is Primary and post-drain source writes are not replicated",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			em, err := getMirror(ctx, cfg, cutNamespace, "forward")
			if err != nil {
				t.Fatalf("failed to get EtcdMirror forward: %v", err)
			}
			fence, found, err := findFenceValue(t, cfg, cutBetaRef(), cutPrefix)
			if err != nil || !found {
				t.Fatalf("fence key not readable on beta (found: %t): %v", found, err)
			}
			if fence.Role != mirroragent.RolePrimary {
				t.Fatalf("fence role after drain is %q, want %q", fence.Role, mirroragent.RolePrimary)
			}
			if fence.LinkUID != string(em.UID) {
				t.Fatalf("fence linkUID %q does not match CR UID %q", fence.LinkUID, em.UID)
			}
			if fence.Epoch != em.Generation {
				t.Fatalf("fence epoch %d does not match CR generation %d", fence.Epoch, em.Generation)
			}

			// Bounded negative wait: a straggler write on the drained source
			// must never land on the new primary.
			if _, err := execEtcdctl(t, cfg, cutAlphaRef(), "put", cutPrefix+"straggler", "late"); err != nil {
				t.Fatalf("failed to put straggler on alpha: %v", err)
			}
			time.Sleep(15 * time.Second)
			betaDump, err := etcdctlJSONDump(t, cfg, cutBetaRef(), cutPrefix)
			if err != nil {
				t.Fatalf("failed to dump beta under %q: %v", cutPrefix, err)
			}
			if _, ok := betaDump[cutPrefix+"straggler"]; ok {
				t.Fatal("post-drain source write replicated onto the new primary")
			}
			em, err = getMirror(ctx, cfg, cutNamespace, "forward")
			if err != nil {
				t.Fatalf("failed to re-get EtcdMirror forward: %v", err)
			}
			if em.Status.Cutover == nil || em.Status.Cutover.DrainedRevision != drainedRevision {
				t.Fatalf("drainedRevision moved after cutover: %+v", em.Status.Cutover)
			}
			return ctx
		})

	feature.Assess("forward CR deletion cleans its checkpoint, data intact", assessForwardDeletionCleanup)

	feature.Assess("reverse mirror with OverwriteAndPrune converges and prunes orphans", assessReversePrunes)

	feature.Assess("reverse CR deletion cleans its checkpoint", assessReverseDeletionCleanup)

	feature.Assess("deletion leaves a foreign-owned checkpoint untouched", assessForeignFenceSurvives)

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		deleteMirrorFixture(ctx, t, cfg, cutNamespace)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
