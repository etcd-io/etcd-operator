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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// TestQuorumLossRecovery exercises the operator's automatic quorum-loss disaster
// recovery end to end:
//
//  1. Create a 3-member EtcdCluster backed by PVCs and write sentinel keys.
//  2. Break quorum by destroying two of the three members' pods AND their PVCs,
//     and HOLDING them down (the StatefulSet eagerly recreates them with empty
//     data and etcd's own membership lets an empty pod rejoin and re-form quorum,
//     so a single delete self-heals before the operator ever acts — see
//     holdMajorityDown). Keeping a majority continuously unreachable past the
//     operator's detection + grace window is what produces the textbook
//     unrecoverable-by-restart scenario the recovery path exists for.
//  3. Assert the operator detects sustained quorum loss and rebuilds a
//     single-member cluster from the surviving member with --force-new-cluster,
//     then re-adds the other members back to a healthy 3-member cluster.
//  4. Assert the irreversible --force-new-cluster residue is cleared, all members
//     share ONE cluster id (no split-brain re-fork), the possible-data-loss
//     accounting is surfaced, and the sentinel data both survived the rebuild and
//     provably replicated to a re-added member.
//
// The whole flow is bounded by explicit timeouts so a wedged recovery fails the
// test rather than hanging.
func TestQuorumLossRecovery(t *testing.T) {
	const (
		clusterName = "etcd-quorum-recovery"
		size        = 3
		sentinelKey = "quorum-recovery-canary"
		sentinelVal = "survived-the-disaster"
		// extraKeys are written alongside the sentinel so the survival check
		// exercises the keyspace (and its revision), not a single round-trip.
		extraKeys = 5
	)

	feature := features.New("quorum-loss-recovery")

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		// Build a 3-node cluster on PVCs so deleting a PVC truly destroys that
		// member's data (the precondition for an unrecoverable majority loss).
		createEtcdClusterWithPVC(ctx, t, c, clusterName, size)
		waitForSTSReadiness(t, c, clusterName, size)
		// A ready 3-replica StatefulSet on a fresh cluster already implies three
		// voting members with no learners, so we do not re-probe here; the
		// post-recovery waitForNoLearners is the load-bearing one.

		// Write the sentinel plus a handful of extra keys. Capture the resulting
		// revision so we can later assert the survivor retained the full keyspace,
		// not just that one key happens to read back.
		verifyDataOperations(t, c, clusterName, sentinelKey, sentinelVal)
		for i := 0; i < extraKeys; i++ {
			putKey(t, c, fmt.Sprintf("%s-0", clusterName),
				fmt.Sprintf("%s-extra-%d", sentinelKey, i), fmt.Sprintf("val-%d", i))
		}
		return ctx
	})

	feature.Assess("break quorum by destroying and holding down a majority of members",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Precondition: the survivor's PVC (ordinal 0) must exist — the entire
			// recovery rebuilds from it. Guard against a future STS retention-policy
			// change silently wiping it.
			assertPVCExists(ctx, t, c, fmt.Sprintf("etcd-data-%s-0", clusterName))

			// Destroy ordinals 1 and 2 (pods + PVCs) and KEEP them destroyed until
			// the operator commits to recovery. A one-shot delete is not enough: the
			// StatefulSet recreates the empty pods within seconds and etcd's persisted
			// membership lets them rejoin and re-form quorum, self-healing before the
			// operator's grace window elapses. holdMajorityDown force-deletes the
			// recreated pods+PVCs on a tight loop so a majority stays continuously
			// unreachable, which is the only state assessQuorum classifies as true,
			// sustained quorum loss. Ordinal 0 (the survivor) is never touched.
			holdMajorityDown(ctx, t, c, clusterName, []int{2, 1}, 5*time.Minute)
			t.Logf("held %s-1 and %s-2 down (pods + PVCs) until the operator committed to recovery", clusterName, clusterName)
			return ctx
		},
	)

	feature.Assess("operator recovers cluster to 3 healthy members",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Recovery: rebuild single-member cluster, then re-add 2 members via the
			// learner path. Allow a generous bound; fail (not hang) if exceeded.
			waitForRecoveryCompleted(t, c, clusterName, 12*time.Minute)
			waitForSTSReadiness(t, c, clusterName, size)
			waitForNoLearners(t, c, fmt.Sprintf("%s-0", clusterName), size, 5*time.Minute)
			return ctx
		},
	)

	feature.Assess("rebuild flag cleared and cluster did not split-brain",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// The single most dangerous failure mode is leaving --force-new-cluster
			// behind: pod-0 would re-fork the cluster on its next restart, or a pod
			// that already force-forked now diverges. Once recovery is Completed both
			// the marker annotation and the flag MUST be gone from the StatefulSet,
			// AND all three live members must report ONE shared cluster id — a re-fork
			// produces a divergent cluster id, which the spec-only check cannot catch.
			assertForceNewClusterCleared(ctx, t, c, clusterName)
			assertSingleClusterID(t, c, clusterName, size)
			return ctx
		},
	)

	feature.Assess("possible data loss is surfaced (condition + status accounting)",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Recovery via --force-new-cluster is not lossless: it must NEVER be
			// silent. After a rebuild the operator must record the data-loss
			// accounting (survivor id + the RETAINED revision) and bump the durable
			// attempt counter, and raise the DataLossPossible condition so a human can
			// audit the loss.
			assertPossibleDataLossSurfaced(ctx, t, c, clusterName)
			return ctx
		},
	)

	feature.Assess("sentinel data survived the rebuild and replicated to a re-added member",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Read from the survivor (proves the data survived the force-new-cluster
			// bootstrap) AND from a member that was destroyed and re-added. The
			// re-added member is read with a node-LOCAL, serializable read so it is
			// provably served from that member's own store — a linearizable read
			// would route to the leader (almost always the survivor) and could pass
			// even if replication to the rejoined member never happened.
			survivor := fmt.Sprintf("%s-0", clusterName)
			if got := readKey(t, c, survivor, sentinelKey); got != sentinelVal {
				t.Fatalf("sentinel missing on survivor %s after recovery: %q=%q, want %q",
					survivor, sentinelKey, got, sentinelVal)
			}
			t.Logf("sentinel intact on survivor %s: %s=%s", survivor, sentinelKey, sentinelVal)

			readded := fmt.Sprintf("%s-1", clusterName)
			if got := readKeyLocalSerializable(t, c, readded, sentinelKey); got != sentinelVal {
				t.Fatalf("sentinel did not replicate to re-added member %s (local serializable read): %q=%q, want %q",
					readded, sentinelKey, got, sentinelVal)
			}
			t.Logf("sentinel replicated to re-added member %s (local serializable read): %s=%s", readded, sentinelKey, sentinelVal)

			// The whole keyspace, not just one key, must be present on the re-added
			// member. Count keys under the sentinel prefix locally on etcd-1.
			wantKeys := extraKeys + 1 // the sentinel + the extras
			if got := countKeysLocalSerializable(t, c, readded, sentinelKey); got != wantKeys {
				t.Fatalf("re-added member %s holds %d keys under %q prefix, want %d (keyspace did not fully replicate)",
					readded, got, sentinelKey, wantKeys)
			}
			t.Logf("full keyspace (%d keys) replicated to re-added member %s", wantKeys, readded)
			return ctx
		},
	)

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, clusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// holdMajorityDown destroys the given member ordinals (pods + PVCs) and keeps them
// destroyed — force-deleting whatever the StatefulSet recreates — until the
// operator has COMMITTED to recovery (Recovery phase Rebuilding/ScalingOut/
// Completed). Holding is required because a single delete self-heals: the
// StatefulSet recreates the empty pods and etcd's persisted membership lets them
// rejoin and re-form quorum long before the operator's detection + grace window
// elapses. Once the operator scales the StatefulSet down to the single survivor
// to inject --force-new-cluster, the recreated majority pods stop coming back and
// the loop completes. Ordinal 0 (the survivor) is never touched.
func holdMajorityDown(ctx context.Context, t *testing.T, c *envconf.Config, clusterName string, ordinals []int, timeout time.Duration) {
	t.Helper()
	committed := func(ec *ecv1alpha1.EtcdCluster) bool {
		if ec.Status.Recovery == nil {
			return false
		}
		switch ec.Status.Recovery.Phase {
		case ecv1alpha1.RecoveryPhaseRebuilding,
			ecv1alpha1.RecoveryPhaseScalingOut,
			ecv1alpha1.RecoveryPhaseCompleted:
			return true
		}
		return false
	}

	err := wait.For(func(ctx context.Context) (bool, error) {
		ec := getEtcdCluster(ctx, t, c, clusterName)
		if committed(ec) {
			t.Logf("operator committed to recovery, phase=%s msg=%q; stopping the majority-down hold",
				ec.Status.Recovery.Phase, ec.Status.Recovery.Message)
			return true, nil
		}
		// Re-destroy the majority's data and pods. Best-effort: any of these may be
		// transiently absent/Terminating between recreations.
		for _, ord := range ordinals {
			deleteMemberPVCAndPod(ctx, t, c, clusterName, ord)
		}
		if rec := ec.Status.Recovery; rec != nil {
			t.Logf("holding majority down; recovery phase=%s detail=%q", rec.Phase, rec.Message)
		}
		return false, nil
	}, wait.WithTimeout(timeout), wait.WithInterval(3*time.Second))
	if err != nil {
		t.Fatalf("operator did not commit to quorum-loss recovery within %s while a majority was held down: %v", timeout, err)
	}
}

// deleteMemberPVCAndPod force-deletes a single etcd member's pod and its PVC,
// destroying that member's data. The PVC is deleted first so the StatefulSet
// cannot re-bind the old volume when it recreates the pod, and the pod is removed
// with grace period 0 so it goes away immediately rather than draining — both are
// needed for the majority to be simultaneously, durably unreachable.
func deleteMemberPVCAndPod(ctx context.Context, t *testing.T, c *envconf.Config, clusterName string, ordinal int) {
	t.Helper()
	client := c.Client()
	podName := fmt.Sprintf("%s-%d", clusterName, ordinal)
	// PVC name follows the StatefulSet volumeClaimTemplate convention: <vct>-<pod>.
	pvcName := fmt.Sprintf("etcd-data-%s-%d", clusterName, ordinal)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: namespace},
	}
	if err := client.Resources().Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		t.Logf("deleting PVC %s (best-effort): %v", pvcName, err)
	}

	zero := int64(0)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace}}
	if err := client.Resources().Delete(ctx, pod, func(o *metav1.DeleteOptions) {
		o.GracePeriodSeconds = &zero
	}); err != nil && !apierrors.IsNotFound(err) {
		t.Logf("deleting pod %s (best-effort): %v", podName, err)
	}
}

// assertPVCExists fails if the named PVC is absent. Used to assert the survivor's
// volume is intact before (and implicitly the precondition recovery relies on).
func assertPVCExists(ctx context.Context, t *testing.T, c *envconf.Config, pvcName string) {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	if err := c.Client().Resources().Get(ctx, pvcName, namespace, &pvc); err != nil {
		t.Fatalf("survivor PVC %s must exist before breaking quorum, but: %v", pvcName, err)
	}
}

// assertForceNewClusterCleared verifies the recovery left no residue on the
// StatefulSet: neither the marker annotation nor the --force-new-cluster flag may
// remain, or pod-0 would re-fork the cluster on its next restart.
func assertForceNewClusterCleared(ctx context.Context, t *testing.T, c *envconf.Config, clusterName string) {
	t.Helper()
	// Mirror the controller's constants (not importable from the e2e package).
	const (
		forceNewClusterAnnotation = "operator.etcd.io/force-new-cluster"
		forceNewClusterArg        = "--force-new-cluster"
	)
	var sts appsv1.StatefulSet
	if err := c.Client().Resources().Get(ctx, clusterName, namespace, &sts); err != nil {
		t.Fatalf("failed to get StatefulSet %s: %v", clusterName, err)
	}
	if _, ok := sts.Annotations[forceNewClusterAnnotation]; ok {
		t.Fatalf("StatefulSet %s still carries the %s annotation after recovery", clusterName, forceNewClusterAnnotation)
	}
	for _, ct := range sts.Spec.Template.Spec.Containers {
		for _, a := range ct.Args {
			if a == forceNewClusterArg {
				t.Fatalf("container %q still carries %s after recovery", ct.Name, forceNewClusterArg)
			}
		}
	}
	t.Logf("StatefulSet %s is clean: no force-new-cluster annotation or flag", clusterName)
}

// assertSingleClusterID verifies every member reports the SAME etcd cluster id.
// A --force-new-cluster re-fork produces a divergent cluster id; this is the
// cheapest strong proof that the rebuild re-formed one coherent cluster rather
// than leaving a pod running a forked raft. It also confirms the expected member
// count.
func assertSingleClusterID(t *testing.T, c *envconf.Config, clusterName string, expectedMembers int) {
	t.Helper()
	ml := getEtcdMemberListPB(t, c, fmt.Sprintf("%s-0", clusterName))
	if ml.Header == nil || ml.Header.ClusterId == 0 {
		t.Fatalf("member list response has no cluster id")
	}
	if len(ml.Members) != expectedMembers {
		t.Fatalf("member list shows %d members, want %d", len(ml.Members), expectedMembers)
	}
	// Cross-check each member's view of the cluster id from its own endpoint.
	want := ml.Header.ClusterId
	for i := 0; i < expectedMembers; i++ {
		pod := fmt.Sprintf("%s-%d", clusterName, i)
		got := getEtcdMemberListPB(t, c, pod)
		if got.Header == nil || got.Header.ClusterId != want {
			var have uint64
			if got.Header != nil {
				have = got.Header.ClusterId
			}
			t.Fatalf("member %s reports cluster id %x, want %x — the cluster split-brained during recovery",
				pod, have, want)
		}
	}
	t.Logf("all %d members share one cluster id %x — no split-brain re-fork", expectedMembers, want)
}

// assertPossibleDataLossSurfaced verifies the operator did NOT silently recover:
// after a force-new-cluster rebuild it must raise the DataLossPossible condition
// (True) and populate status.recovery.dataLoss with the survivor's identity and
// the RETAINED revision, and it must have bumped the durable attempt counter, so
// the loss is auditable.
func assertPossibleDataLossSurfaced(ctx context.Context, t *testing.T, c *envconf.Config, clusterName string) {
	t.Helper()
	// Mirror the controller's condition type (not importable from the e2e package).
	const conditionDataLossPossible = "DataLossPossible"

	ec := getEtcdCluster(ctx, t, c, clusterName)

	cond := meta.FindStatusCondition(ec.Status.Conditions, conditionDataLossPossible)
	if cond == nil {
		t.Fatalf("expected %s condition after a force-new-cluster rebuild; recovery must not be silent", conditionDataLossPossible)
	}
	if cond.Status != metav1.ConditionTrue {
		t.Fatalf("%s condition = %q, want True", conditionDataLossPossible, cond.Status)
	}

	if ec.Status.Recovery == nil {
		t.Fatalf("expected status.recovery after rebuild, got nil")
	}
	if ec.Status.Recovery.Attempts < 1 {
		t.Fatalf("expected status.recovery.attempts >= 1 after a committed recovery, got %d", ec.Status.Recovery.Attempts)
	}
	if ec.Status.Recovery.DataLoss == nil {
		t.Fatalf("expected status.recovery.dataLoss accounting after rebuild, got nil")
	}
	dl := ec.Status.Recovery.DataLoss
	if dl.SurvivorMemberID == "" {
		t.Fatalf("data-loss accounting missing survivor member id")
	}
	// The retained revision is the load-bearing accounting value. We wrote the
	// sentinel + extra keys before the disaster, so the survivor's store is well
	// past the empty-cluster revision; a zero here means the capture is broken
	// (e.g. a nil header silently zeroed it).
	if dl.SurvivorRevision <= 1 {
		t.Fatalf("data-loss accounting has SurvivorRevision=%d; expected the survivor's real (>1) revision after writes", dl.SurvivorRevision)
	}
	if dl.RecoveredTime == nil {
		t.Fatalf("data-loss accounting missing recoveredTime")
	}
	if !strings.Contains(dl.Message, "data loss") && !strings.Contains(dl.Message, "NOT retained") {
		t.Fatalf("data-loss message %q does not describe the loss", dl.Message)
	}
	t.Logf("possible data loss surfaced: member=%s revision=%d attempts=%d (%s)",
		dl.SurvivorMemberID, dl.SurvivorRevision, ec.Status.Recovery.Attempts, dl.Message)
}

// getEtcdCluster fetches the EtcdCluster CR.
func getEtcdCluster(ctx context.Context, t *testing.T, c *envconf.Config, name string) *ecv1alpha1.EtcdCluster {
	t.Helper()
	var ec ecv1alpha1.EtcdCluster
	if err := c.Client().Resources().Get(ctx, name, namespace, &ec); err != nil {
		t.Fatalf("failed to get EtcdCluster %s: %v", name, err)
	}
	return &ec
}

// waitForRecoveryCompleted blocks until the recovery state machine reports the
// Completed phase.
func waitForRecoveryCompleted(
	t *testing.T, c *envconf.Config, name string, timeout time.Duration,
) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		ec := getEtcdCluster(ctx, t, c, name)
		if ec.Status.Recovery != nil && ec.Status.Recovery.Phase == ecv1alpha1.RecoveryPhaseCompleted {
			t.Logf("recovery completed: %s", ec.Status.Recovery.Message)
			return true, nil
		}
		return false, nil
	}, wait.WithTimeout(timeout), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("quorum-loss recovery did not complete within %s: %v", timeout, err)
	}
}

// putKey writes a single key via etcdctl inside the given pod.
func putKey(t *testing.T, c *envconf.Config, podName, key, value string) {
	t.Helper()
	_, stderr, err := execInPod(t, c, podName, namespace, []string{"etcdctl", "put", key, value})
	if err != nil {
		t.Fatalf("failed to put key %q on %s: %v, stderr: %s", key, podName, err, stderr)
	}
}

// readKey reads a single key's value from the given pod via etcdctl (default
// linearizable read — may be served by the leader).
func readKey(t *testing.T, c *envconf.Config, podName, key string) string {
	t.Helper()
	stdout, stderr, err := execInPod(t, c, podName, namespace,
		[]string{"etcdctl", "get", key, "--print-value-only"})
	if err != nil {
		t.Fatalf("failed to read key %q from %s: %v, stderr: %s", key, podName, err, stderr)
	}
	return strings.TrimSpace(stdout)
}

// readKeyLocalSerializable reads a key from the pod's OWN local store: it pins the
// endpoint to the in-pod address and forces a serializable read so the answer is
// served by that member, not routed to the leader. This is what proves data
// actually replicated to a re-added member rather than being read back from the
// untouched survivor.
func readKeyLocalSerializable(t *testing.T, c *envconf.Config, podName, key string) string {
	t.Helper()
	stdout, stderr, err := execInPod(t, c, podName, namespace,
		[]string{"etcdctl", "get", key, "--print-value-only",
			"--endpoints=http://127.0.0.1:2379", "--consistency=s"})
	if err != nil {
		t.Fatalf("failed local serializable read of %q from %s: %v, stderr: %s", key, podName, err, stderr)
	}
	return strings.TrimSpace(stdout)
}

// countKeysLocalSerializable counts the keys under the given prefix from the pod's
// own local store (serializable, node-local).
func countKeysLocalSerializable(t *testing.T, c *envconf.Config, podName, prefix string) int {
	t.Helper()
	stdout, stderr, err := execInPod(t, c, podName, namespace,
		[]string{"etcdctl", "get", prefix, "--prefix", "--keys-only",
			"--endpoints=http://127.0.0.1:2379", "--consistency=s"})
	if err != nil {
		t.Fatalf("failed local serializable key count for %q from %s: %v, stderr: %s", prefix, podName, err, stderr)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}
