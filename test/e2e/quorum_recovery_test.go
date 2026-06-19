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
//  1. Create a 3-member EtcdCluster backed by PVCs and write a sentinel key.
//  2. Break quorum by deleting two of the three members' pods AND their PVCs,
//     destroying their data so a majority is permanently gone — the textbook
//     unrecoverable-by-restart scenario.
//  3. Assert the operator detects sustained quorum loss (Recovering condition),
//     rebuilds a single-member cluster from the surviving member, and re-adds the
//     other members back to a healthy 3-member cluster.
//  4. Assert the sentinel data survived the rebuild.
//
// The whole flow is bounded by an explicit timeout so a wedged recovery fails the
// test rather than hanging.
func TestQuorumLossRecovery(t *testing.T) {
	const (
		clusterName = "etcd-quorum-recovery"
		size        = 3
		sentinelKey = "quorum-recovery-canary"
		sentinelVal = "survived-the-disaster"
	)

	feature := features.New("quorum-loss-recovery")

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		// Build a 3-node cluster on PVCs so deleting a PVC truly destroys that
		// member's data (the precondition for an unrecoverable majority loss).
		createEtcdClusterWithPVC(ctx, t, c, clusterName, size)
		waitForSTSReadiness(t, c, clusterName, size)
		// Confirm a healthy 3-member cluster with no learners before we break it.
		waitForNoLearners(t, c, fmt.Sprintf("%s-0", clusterName), size, 3*time.Minute)
		// Write a sentinel key we will verify survives the rebuild.
		verifyDataOperations(t, c, clusterName, sentinelKey, sentinelVal)
		return ctx
	})

	feature.Assess("break quorum by destroying a majority of members",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Delete ordinals 1 and 2 (pods + PVCs). Ordinal 0 is the survivor whose
			// data the operator rebuilds from. Deleting the PVCs makes the loss
			// permanent: the StatefulSet may recreate the pods, but they come up
			// empty and cannot rejoin the (now leaderless) cluster.
			for _, ord := range []int{2, 1} {
				deleteMemberPVCAndPod(ctx, t, c, clusterName, ord)
			}
			t.Logf("destroyed members %s-1 and %s-2 (pods + PVCs); cluster has lost quorum", clusterName, clusterName)
			return ctx
		},
	)

	feature.Assess("operator detects sustained quorum loss (Recovering condition)",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			waitForRecoveringObserved(t, c, clusterName, 5*time.Minute)
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

	feature.Assess("rebuild flag cleared from StatefulSet after recovery",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// The single most dangerous failure mode is leaving --force-new-cluster
			// permanently on the StatefulSet: pod-0 would re-fork the cluster on its
			// next restart. Once recovery is Completed, both the marker annotation
			// and the flag MUST be gone.
			assertForceNewClusterCleared(ctx, t, c, clusterName)
			return ctx
		},
	)

	feature.Assess("sentinel data survived the rebuild and replicated to a re-added member",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Read from the survivor (proves the data survived the force-new-cluster
			// bootstrap) AND from a member that was destroyed and re-added (proves the
			// rebuilt cluster actually replicated to the rejoined member, rather than
			// the test trivially reading back from the untouched survivor).
			for _, ord := range []int{0, 1} {
				pod := fmt.Sprintf("%s-%d", clusterName, ord)
				got := readKey(t, c, pod, sentinelKey)
				if got != sentinelVal {
					t.Fatalf("data not present on %s after recovery: key %q = %q, want %q",
						pod, sentinelKey, got, sentinelVal)
				}
				t.Logf("sentinel key intact on %s after recovery: %s=%s", pod, sentinelKey, got)
			}
			return ctx
		},
	)

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, clusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// deleteMemberPVCAndPod deletes a single etcd member's pod and its PVC, destroying
// that member's data. PVC is deleted first so the StatefulSet cannot re-bind the
// old volume when it recreates the pod.
func deleteMemberPVCAndPod(ctx context.Context, t *testing.T, c *envconf.Config, clusterName string, ordinal int) {
	t.Helper()
	client := c.Client()
	podName := fmt.Sprintf("%s-%d", clusterName, ordinal)
	// PVC name follows the StatefulSet volumeClaimTemplate convention: <vct>-<pod>.
	pvcName := fmt.Sprintf("etcd-data-%s-%d", clusterName, ordinal)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: namespace},
	}
	if err := client.Resources().Delete(ctx, pvc); err != nil {
		t.Logf("deleting PVC %s (best-effort): %v", pvcName, err)
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace}}
	if err := client.Resources().Delete(ctx, pod); err != nil {
		t.Logf("deleting pod %s (best-effort): %v", podName, err)
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

// getEtcdCluster fetches the EtcdCluster CR.
func getEtcdCluster(ctx context.Context, t *testing.T, c *envconf.Config, name string) *ecv1alpha1.EtcdCluster {
	t.Helper()
	var ec ecv1alpha1.EtcdCluster
	if err := c.Client().Resources().Get(ctx, name, namespace, &ec); err != nil {
		t.Fatalf("failed to get EtcdCluster %s: %v", name, err)
	}
	return &ec
}

// waitForRecoveringObserved blocks until the operator surfaces evidence that it
// has entered recovery: either the Recovering condition is present, or the
// Recovery status records a Rebuilding/ScalingOut phase. We accept either signal
// because the Recovering condition may flip back to False the instant the
// single-member rebuild completes.
func waitForRecoveringObserved(
	t *testing.T, c *envconf.Config, name string, timeout time.Duration,
) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		ec := getEtcdCluster(ctx, t, c, name)
		if ec.Status.Recovery != nil {
			switch ec.Status.Recovery.Phase {
			case ecv1alpha1.RecoveryPhaseRebuilding,
				ecv1alpha1.RecoveryPhaseScalingOut,
				ecv1alpha1.RecoveryPhaseCompleted:
				t.Logf("recovery observed, phase=%s msg=%q",
					ec.Status.Recovery.Phase, ec.Status.Recovery.Message)
				return true, nil
			}
		}
		cond := meta.FindStatusCondition(ec.Status.Conditions, "Recovering")
		if cond != nil && cond.Status == metav1.ConditionTrue {
			t.Logf("Recovering condition observed: reason=%s msg=%q", cond.Reason, cond.Message)
			return true, nil
		}
		return false, nil
	}, wait.WithTimeout(timeout), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("operator did not enter quorum-loss recovery within %s: %v", timeout, err)
	}
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
	}, wait.WithTimeout(timeout), wait.WithInterval(10*time.Second))
	if err != nil {
		t.Fatalf("quorum-loss recovery did not complete within %s: %v", timeout, err)
	}
}

// readKey reads a single key's value from the given pod via etcdctl.
func readKey(t *testing.T, c *envconf.Config, podName, key string) string {
	t.Helper()
	stdout, stderr, err := execInPod(t, c, podName, namespace,
		[]string{"etcdctl", "get", key, "--print-value-only"})
	if err != nil {
		t.Fatalf("failed to read key %q from %s: %v, stderr: %s", key, podName, err, stderr)
	}
	return strings.TrimSpace(stdout)
}
