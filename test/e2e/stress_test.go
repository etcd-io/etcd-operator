//go:build stress

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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// The stress suite reuses the package-level testenv (one kind cluster + one
// deployed operator for the whole run, set up in TestMain). Unlike the fast e2e
// tests it drives churn imperatively rather than through the features builder,
// because the invariants (continuous quorum watch, one-learner-at-a-time
// progression) are easier to express as straight-line Go.
//
// Each test gets its own EtcdCluster name and tears it down at the end unless
// ETCD_E2E_SKIP_TEARDOWN=true (honored by the shared skipTeardown var).

// stressTeardown removes the cluster unless the operator left the teardown
// disabled for manual inspection.
func stressTeardown(ctx context.Context, t *testing.T, c *envconf.Config, name string) {
	t.Helper()
	if skipTeardown {
		t.Logf("ETCD_E2E_SKIP_TEARDOWN=true, leaving cluster %q in place", name)
		return
	}
	cleanupEtcdCluster(ctx, t, c, name)
}

// healthyTimeout scales the per-step health timeout with cluster size; a 7-member
// bootstrap is serial learner-add x6 behind the blocking reconcile, so it needs
// considerably longer than the helpers_test.go default.
func healthyTimeout(size int) time.Duration {
	if size >= 7 {
		return 12 * time.Minute
	}
	return 6 * time.Minute
}

// writeKeyset writes a deterministic set of keys via pod-0 so later assertions
// can confirm the data survived churn.
func writeKeyset(t *testing.T, c *envconf.Config, name string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		verifyDataOperations(t, c, name, fmt.Sprintf("stress-key-%d", i), fmt.Sprintf("stress-val-%d", i))
	}
}

// assertKeyset reads the keyset back from pod-0 and fails on any mismatch.
func assertKeyset(t *testing.T, c *envconf.Config, name string, n int) {
	t.Helper()
	podName := fmt.Sprintf("%s-0", name)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("stress-key-%d", i)
		want := fmt.Sprintf("stress-val-%d", i)
		stdout, stderr, err := execInPod(t, c, podName, namespace,
			[]string{"etcdctl", "get", key, "--print-value-only"})
		if err != nil {
			t.Fatalf("failed to read %s back: %v, stderr: %s", key, err, stderr)
		}
		if got := strings.TrimSpace(stdout); got != want {
			t.Errorf("keyset mismatch for %s: want %q got %q", key, want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Section 3: green tests (expected to pass on current main; upstream-PR fodder)
// ---------------------------------------------------------------------------

// TestStressBringUp brings up clusters of 1/3/7 members, recording the elapsed
// time-to-healthy per size (the efficient-spin-up baseline), and asserts data
// consistency and a round-trip keyset at each size.
func TestStressBringUp(t *testing.T) {
	ctx := context.Background()
	cfg := testEnv.EnvConf()

	for _, size := range []int{1, 3, 7} {
		size := size
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			name := fmt.Sprintf("etcd-stress-bringup-%d", size)
			defer stressTeardown(ctx, t, cfg, name)

			timeToHealthy(ctx, t, cfg, name, size, healthyTimeout(size))

			podName := fmt.Sprintf("%s-0", name)
			assertHashKVConsistent(t, cfg, podName)

			writeKeyset(t, cfg, name, 10)
			assertKeyset(t, cfg, name, 10)
			assertHashKVConsistent(t, cfg, podName)
		})
	}
}

// TestStressScaleChurn drives 1 -> 3 -> 7 -> 3 -> 1 with a quorum watcher
// running throughout. After every step it asserts steady-state membership (no
// stuck learners), hashkv consistency, and that the keyset written up-front is
// still intact.
func TestStressScaleChurn(t *testing.T) {
	ctx := context.Background()
	cfg := testEnv.EnvConf()

	name := "etcd-stress-churn"
	defer stressTeardown(ctx, t, cfg, name)
	podName := fmt.Sprintf("%s-0", name)

	// Start at 1 and seed the keyset.
	timeToHealthy(ctx, t, cfg, name, 1, healthyTimeout(1))
	writeKeyset(t, cfg, name, 10)

	stop := quorumWatcher(ctx, t, cfg, podName)
	defer stop()

	for _, size := range []int{3, 7, 3, 1} {
		t.Logf("scaling cluster %q to %d", name, size)
		scaleEtcdCluster(ctx, t, cfg, name, size)
		waitForSTSReadiness(t, cfg, name, size)
		waitForClusterHealthy(t, cfg, podName, size, healthyTimeout(size))

		// One-member-at-a-time progression: by the time we are healthy there
		// must be no learners and exactly `size` voting members.
		ml := getEtcdMemberListPB(t, cfg, podName)
		if len(ml.Members) != size {
			t.Errorf("after scaling to %d, member list has %d members", size, len(ml.Members))
		}
		for _, m := range ml.Members {
			if m.IsLearner {
				t.Errorf("after scaling to %d, found stuck learner %s (%d)", size, m.Name, m.ID)
			}
		}

		assertHashKVConsistent(t, cfg, podName)
		assertKeyset(t, cfg, name, 10)
	}
}

// TestStressSingleEditJump performs a single 1 -> 7 edit and asserts the
// operator never admits more than one learner at a time while converging.
func TestStressSingleEditJump(t *testing.T) {
	ctx := context.Background()
	cfg := testEnv.EnvConf()

	name := "etcd-stress-edit-jump"
	defer stressTeardown(ctx, t, cfg, name)
	podName := fmt.Sprintf("%s-0", name)

	timeToHealthy(ctx, t, cfg, name, 1, healthyTimeout(1))
	writeKeyset(t, cfg, name, 5)

	// Watch learner count for the duration of the convergence: the operator
	// adds members one at a time, promoting each learner before adding the
	// next, so the in-flight learner count must never exceed 1.
	wctx, cancel := context.WithCancel(ctx)
	maxLearners := startLearnerSampler(wctx, t, cfg, podName)

	scaleEtcdCluster(ctx, t, cfg, name, 7)
	waitForSTSReadiness(t, cfg, name, 7)
	waitForClusterHealthy(t, cfg, podName, 7, healthyTimeout(7))

	cancel()
	if got := maxLearners(); got > 1 {
		t.Errorf("expected at most 1 concurrent learner during 1->7 jump, observed %d", got)
	}

	assertHashKVConsistent(t, cfg, podName)
	assertKeyset(t, cfg, name, 5)
}

// startLearnerSampler polls the member list and tracks the maximum number of
// simultaneous learners seen until ctx is cancelled. The returned func blocks
// until the sampler stops and returns that maximum.
func startLearnerSampler(ctx context.Context, t *testing.T, c *envconf.Config, podName string) func() int {
	t.Helper()
	done := make(chan int, 1)
	go func() {
		max := 0
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- max
				return
			case <-ticker.C:
				ml := getEtcdMemberListPB(t, c, podName)
				learners := 0
				for _, m := range ml.Members {
					if m.IsLearner {
						learners++
					}
				}
				if learners > max {
					max = learners
				}
			}
		}
	}()
	return func() int { return <-done }
}

// TestStressCrashDuringScale arms the member-add / member-delete failpoints
// during 3 -> 7 and 7 -> 3 respectively, beyond the size-3 coverage in
// TestScaling, and asserts the operator recovers and converges.
func TestStressCrashDuringScale(t *testing.T) {
	ctx := context.Background()
	cfg := testEnv.EnvConf()

	name := "etcd-stress-crash-scale"
	defer stressTeardown(ctx, t, cfg, name)
	podName := fmt.Sprintf("%s-0", name)

	timeToHealthy(ctx, t, cfg, name, 3, healthyTimeout(3))
	writeKeyset(t, cfg, name, 5)

	// 3 -> 7 with the operator panicking right after each member add.
	withFailpoint(t, cfg, "exceptionAfterMemberAdd", "panic", func() {
		scaleEtcdCluster(ctx, t, cfg, name, 7)
		waitForSTSReadiness(t, cfg, name, 7)
		waitForClusterHealthy(t, cfg, podName, 7, healthyTimeout(7))
	})
	assertNoLearnersAt(t, cfg, podName, 7)
	assertHashKVConsistent(t, cfg, podName)

	// 7 -> 3 with the operator panicking right after each member delete.
	withFailpoint(t, cfg, "exceptionAfterMemberDelete", "panic", func() {
		scaleEtcdCluster(ctx, t, cfg, name, 3)
		waitForSTSReadiness(t, cfg, name, 3)
		waitForClusterHealthy(t, cfg, podName, 3, healthyTimeout(3))
	})
	assertNoLearnersAt(t, cfg, podName, 3)
	assertHashKVConsistent(t, cfg, podName)
	assertKeyset(t, cfg, name, 5)
}

// assertNoLearnersAt verifies exactly `size` voting members with no learners.
func assertNoLearnersAt(t *testing.T, c *envconf.Config, podName string, size int) {
	t.Helper()
	ml := getEtcdMemberListPB(t, c, podName)
	if len(ml.Members) != size {
		t.Errorf("expected %d members, got %d", size, len(ml.Members))
	}
	for _, m := range ml.Members {
		if m.IsLearner {
			t.Errorf("found stuck learner %s (%d) at size %d", m.Name, m.ID, size)
		}
	}
}

// withFailpoint enables a gofail failpoint on the operator pod, runs fn, and
// always disables the failpoint afterward (mirrors the Setup/Teardown pattern
// in TestScaling).
func withFailpoint(t *testing.T, c *envconf.Config, failpoint, term string, fn func()) {
	t.Helper()
	operator, err := getEtcdOperatorPod(t, c.Client())
	if err != nil {
		t.Fatalf("unable to get etcd-operator pod: %v", err)
	}
	if err := enableGoFailPoint(t, c, operator, failpoint, term); err != nil {
		t.Fatalf("unable to enable failpoint %s: %v", failpoint, err)
	}
	defer func() {
		// Re-fetch the pod: a panic failpoint restarts the operator, so the
		// pod object captured above may be stale.
		op, err := getEtcdOperatorPod(t, c.Client())
		if err != nil {
			t.Errorf("unable to get etcd-operator pod for failpoint cleanup: %v", err)
			return
		}
		if err := disableGoFailPoint(t, c, op, failpoint); err != nil {
			t.Errorf("unable to disable failpoint %s: %v", failpoint, err)
		}
	}()
	fn()
}

// TestStressPodRecoveryAtScale deletes a member pod at size 7 and asserts the
// member ID is stable and data is replicated to the recovered pod (extends the
// size-3 TestPodRecovery).
func TestStressPodRecoveryAtScale(t *testing.T) {
	ctx := context.Background()
	cfg := testEnv.EnvConf()

	name := "etcd-stress-recovery-7"
	defer stressTeardown(ctx, t, cfg, name)
	podName := fmt.Sprintf("%s-0", name)
	targetPodName := fmt.Sprintf("%s-4", name)

	timeToHealthy(ctx, t, cfg, name, 7, healthyTimeout(7))

	verifyPodUsesPVC(t, cfg, targetPodName, "etcd-data-"+name)

	initialMembers := getEtcdMembersName2IDMapping(t, cfg, podName)
	wantID, ok := initialMembers[targetPodName]
	if !ok {
		t.Fatalf("member %s not present before deletion: %v", targetPodName, initialMembers)
	}

	var pod corev1.Pod
	if err := cfg.Client().Resources().Get(ctx, targetPodName, namespace, &pod); err != nil {
		t.Fatalf("failed to get pod %s: %v", targetPodName, err)
	}
	deletedUID := pod.UID
	if err := cfg.Client().Resources().Delete(ctx, &pod); err != nil {
		t.Fatalf("failed to delete pod %s: %v", targetPodName, err)
	}

	// Wait for the pod to be recreated with a fresh UID and running.
	if err := wait.For(func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		if err := cfg.Client().Resources().Get(ctx, targetPodName, namespace, &p); err != nil {
			return false, nil
		}
		return p.UID != deletedUID && p.Status.Phase == corev1.PodRunning, nil
	}, wait.WithTimeout(5*time.Minute), wait.WithInterval(10*time.Second)); err != nil {
		t.Fatalf("pod %s failed to be recreated: %v", targetPodName, err)
	}

	waitForClusterHealthy(t, cfg, podName, 7, healthyTimeout(7))

	verifyPodUsesPVC(t, cfg, targetPodName, "etcd-data-"+name)

	finalMembers := getEtcdMembersName2IDMapping(t, cfg, podName)
	if len(finalMembers) != 7 {
		t.Errorf("member count changed after recovery: want 7, got %d", len(finalMembers))
	}
	if gotID, ok := finalMembers[targetPodName]; !ok {
		t.Errorf("member %s missing after recovery", targetPodName)
	} else if gotID != wantID {
		t.Errorf("member ID for %s changed across recovery: want %d got %d", targetPodName, wantID, gotID)
	}

	// Data written via the leader must be readable from the recovered pod.
	verifyDataOperations(t, cfg, name, "recovery-at-scale", "value")
	stdout, stderr, err := execInPod(t, cfg, targetPodName, namespace,
		[]string{"etcdctl", "get", "recovery-at-scale", "--print-value-only"})
	if err != nil {
		t.Fatalf("failed to read replicated data from %s: %v, stderr: %s", targetPodName, err, stderr)
	}
	if got := strings.TrimSpace(stdout); got != "value" {
		t.Errorf("replication to recovered pod %s failed: want %q got %q", targetPodName, "value", got)
	}
	assertHashKVConsistent(t, cfg, podName)
}

// ---------------------------------------------------------------------------
// Section 4: bug-proofs (t.Skip-gated; flip to passing alongside their fix PR)
// ---------------------------------------------------------------------------

// TestStressVersionUpgrade proves the silent no-op upgrade: bumping
// .spec.version must change the StatefulSet image and roll the pods while
// keeping quorum. Skipped until the version-upgrade path lands.
func TestStressVersionUpgrade(t *testing.T) {
	t.Skip("unblocks after PR17")

	ctx := context.Background()
	cfg := testEnv.EnvConf()

	name := "etcd-stress-upgrade"
	defer stressTeardown(ctx, t, cfg, name)
	podName := fmt.Sprintf("%s-0", name)

	// Create at a known patch version, then bump the patch up.
	const fromVersion = "v3.6.1"
	const toVersion = "v3.6.4"

	etcdCluster := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metaObjectMeta(name),
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:        3,
			Version:     fromVersion,
			StorageSpec: tinyStorage(ctx, t, cfg),
		},
	}
	if err := cfg.Client().Resources().Create(ctx, etcdCluster); err != nil {
		t.Fatalf("failed to create cluster at %s: %v", fromVersion, err)
	}
	waitForSTSReadiness(t, cfg, name, 3)
	waitForClusterHealthy(t, cfg, podName, 3, healthyTimeout(3))

	imageBefore := stsContainerImage(ctx, t, cfg, name)
	if !strings.HasSuffix(imageBefore, fromVersion) {
		t.Fatalf("expected initial image to end with %s, got %q", fromVersion, imageBefore)
	}

	// Bump the version and watch quorum throughout the rolling restart.
	stop := quorumWatcher(ctx, t, cfg, podName)
	var ec ecv1alpha1.EtcdCluster
	if err := cfg.Client().Resources().Get(ctx, name, namespace, &ec); err != nil {
		t.Fatalf("failed to get cluster for upgrade: %v", err)
	}
	ec.Spec.Version = toVersion
	if err := cfg.Client().Resources().Update(ctx, &ec); err != nil {
		t.Fatalf("failed to update version to %s: %v", toVersion, err)
	}

	// The STS image must actually change.
	if err := wait.For(func(ctx context.Context) (bool, error) {
		return strings.HasSuffix(stsContainerImage(ctx, t, cfg, name), toVersion), nil
	}, wait.WithTimeout(healthyTimeout(3)), wait.WithInterval(10*time.Second)); err != nil {
		t.Fatalf("STS image never updated to %s (silent no-op upgrade?): %v", toVersion, err)
	}

	waitForSTSReadiness(t, cfg, name, 3)
	waitForClusterHealthy(t, cfg, podName, 3, healthyTimeout(3))
	stop()

	assertHashKVConsistent(t, cfg, podName)
}

// TestStressEvenSizeRejected proves even sizes (2, 4) must be rejected at apply
// time once the odd-size CEL validation lands.
func TestStressEvenSizeRejected(t *testing.T) {
	t.Skip("unblocks after PR14 odd-size CEL")

	ctx := context.Background()
	cfg := testEnv.EnvConf()

	for _, size := range []int{2, 4} {
		size := size
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			name := fmt.Sprintf("etcd-stress-even-%d", size)
			defer stressTeardown(ctx, t, cfg, name)

			etcdCluster := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metaObjectMeta(name),
				Spec: ecv1alpha1.EtcdClusterSpec{
					Size:    size,
					Version: etcdVersion,
				},
			}
			err := cfg.Client().Resources().Create(ctx, etcdCluster)
			if err == nil {
				t.Fatalf("expected even size %d to be rejected at apply, but create succeeded", size)
			}
		})
	}
}

// TestStressLeaderlessScaleIn proves that removing the member that currently
// holds leadership does not cause a prolonged write-stall / election storm.
// Skipped until the leadership-transfer-before-removal fix lands.
func TestStressLeaderlessScaleIn(t *testing.T) {
	t.Skip("unblocks after PR04+PR12")

	ctx := context.Background()
	cfg := testEnv.EnvConf()

	name := "etcd-stress-leaderless"
	defer stressTeardown(ctx, t, cfg, name)
	podName := fmt.Sprintf("%s-0", name)

	timeToHealthy(ctx, t, cfg, name, 3, healthyTimeout(3))
	writeKeyset(t, cfg, name, 5)

	// Identify the leader's ordinal. Scaling in removes the highest ordinals
	// first, so to target the leader for removal we want it to be the
	// highest-ordinal pod; assert that precondition so the test is meaningful.
	_, leaderOrdinal := getEtcdLeader(t, cfg, podName)
	if leaderOrdinal != 2 {
		t.Skipf("leader is on ordinal %d, not the highest (2); rerun to exercise leader removal", leaderOrdinal)
	}

	stop := quorumWatcher(ctx, t, cfg, podName)
	scaleEtcdCluster(ctx, t, cfg, name, 1)
	waitForSTSReadiness(t, cfg, name, 1)
	waitForClusterHealthy(t, cfg, fmt.Sprintf("%s-0", name), 1, healthyTimeout(1))
	stop() // fails the test if any sustained quorum-loss window occurred

	assertKeyset(t, cfg, name, 5)
}

// --- small shared builders used by the Section 4 tests ---

func metaObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
	}
}

// tinyStorage builds the same 64Mi PVC spec createEtcdClusterWithPVC uses, for
// tests that need to set fields (e.g. Version) the helper does not expose.
func tinyStorage(ctx context.Context, t *testing.T, c *envconf.Config) *ecv1alpha1.StorageSpec {
	t.Helper()
	return &ecv1alpha1.StorageSpec{
		AccessModes:       corev1.ReadWriteOnce,
		StorageClassName:  getAvailableStorageClass(ctx, t, c),
		VolumeSizeRequest: resource.MustParse("64Mi"),
		VolumeSizeLimit:   resource.MustParse("64Mi"),
	}
}

// stsContainerImage returns the etcd container image set on the cluster's STS.
func stsContainerImage(ctx context.Context, t *testing.T, c *envconf.Config, name string) string {
	t.Helper()
	var sts appsv1.StatefulSet
	if err := c.Client().Resources().Get(ctx, name, namespace, &sts); err != nil {
		t.Fatalf("failed to get STS %s: %v", name, err)
	}
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("STS %s has no containers", name)
	}
	return sts.Spec.Template.Spec.Containers[0].Image
}
