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

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

var etcdVersion = os.Getenv("ETCD_VERSION")

// TestInvalidClusterSize test minimum cluster size behavior
func TestInvalidClusterSize(t *testing.T) {
	feature := features.New("invalid-member-cluster")
	etcdClusterName := "etcd-cluster"

	// test invalid cluster sizes
	tt := map[string]int{
		"with-zero-members":     0,
		"with-negative-members": -1,
	}

	etcdClusterSpec := &ecv1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdClusterName,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    0,
			Version: etcdVersion,
		},
	}

	for name, size := range tt {
		feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {

			// update cluster name
			etcdClusterSpec.Name = strings.Join([]string{etcdClusterName, name}, "-")
			etcdClusterSpec.Spec.Size = size

			err := c.Client().Resources().Create(ctx, etcdClusterSpec)
			if !errors.IsInvalid(err) {
				t.Fatalf("expected invalid etcdCluster %s with size %d. Got: %v", name, size, err)
			}

			return ctx
		})

		feature.Assess(fmt.Sprintf("etcdCluster %s should not be created when etcdCluster.Spec.Size is %d", name, size),
			func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {

				var etcdCluster ecv1alpha1.EtcdCluster
				err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster)
				if !errors.IsNotFound(err) {
					t.Fatalf("found unexpected etcdCluster %s with size %d. Got: %v", name, size, err)
				}

				return ctx
			},
		)
	}

	_ = testEnv.Test(t, feature.Feature())
}

func TestClusterHealthy(t *testing.T) {
	feature := features.New("etcd-operator-controller")

	feature.Assess("ensure the etcd-operator pod is running",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			client := cfg.Client()

			// get the etcd controller deployment
			var deployment appsv1.Deployment
			if err := client.Resources().Get(ctx, "etcd-operator-controller-manager", namespace, &deployment); err != nil {
				t.Fatalf("Failed to get deployment: %s", err)
			}

			// check for deployment to become available with minimum replicas
			if err := wait.For(
				conditions.New(client.Resources()).
					DeploymentConditionMatch(&deployment, appsv1.DeploymentAvailable, corev1.ConditionTrue),
				wait.WithTimeout(3*time.Minute),
				wait.WithInterval(10*time.Second),
			); err != nil {
				t.Fatal(err)
			}

			return ctx
		})

	// 'testEnv' is the env.Environment you set up in TestMain
	_ = testEnv.Test(t, feature.Feature())
}

func TestScaling(t *testing.T) {
	testCases := []struct {
		name            string
		initialSize     int
		scaleTo         int
		expectedMembers int
	}{
		{name: "ScaleInFrom3To1", initialSize: 3, scaleTo: 1, expectedMembers: 1},
		{name: "ScaleOutFrom1To3", initialSize: 1, scaleTo: 3, expectedMembers: 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			feature := features.New(tc.name)
			etcdClusterName := fmt.Sprintf("etcd-%s", strings.ToLower(tc.name))

			feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
				createEtcdClusterWithPVC(ctx, t, c, etcdClusterName, tc.initialSize)
				waitForSTSReadiness(t, c, etcdClusterName, tc.initialSize)
				return ctx
			})

			feature.Assess(
				fmt.Sprintf("scale to %d", tc.scaleTo),
				func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
					scaleEtcdCluster(ctx, t, c, etcdClusterName, tc.scaleTo)
					// Wait until StatefulSet spec/status reflect the scaled size and are ready
					waitForSTSReadiness(t, c, etcdClusterName, tc.scaleTo)
					return ctx
				},
			)

			feature.Assess("verify member list", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
				podName := fmt.Sprintf("%s-0", etcdClusterName)
				ml := getEtcdMemberListPB(t, c, podName)

				if len(ml.Members) != tc.expectedMembers {
					t.Errorf("Expected to find %d members in member list, but got %d",
						tc.expectedMembers, len(ml.Members))
				}

				// Verify controller promoted all learners to voting members
				for _, m := range ml.Members {
					if m.IsLearner {
						t.Errorf("Found unpromoted learner after scaling completed: member %s (%d)", m.Name, m.ID)
					}
				}
				return ctx
			})

			feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
				cleanupEtcdCluster(ctx, t, c, etcdClusterName)
				return ctx
			})

			_ = testEnv.Test(t, feature.Feature())
		})
	}
}

func TestPodRecovery(t *testing.T) {
	feature := features.New("multi-node-pod-recovery")
	etcdClusterName := "etcd-recovery-test"

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		createEtcdClusterWithPVC(ctx, t, c, etcdClusterName, 3)
		waitForSTSReadiness(t, c, etcdClusterName, 3)
		return ctx
	})

	feature.Assess("verify multi-node cluster recovery after pod deletion",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			var sts appsv1.StatefulSet
			if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
				t.Fatalf("Failed to get StatefulSet: %v", err)
			}

			initialReplicas := *sts.Spec.Replicas
			podName := fmt.Sprintf("%s-0", etcdClusterName)
			targetPodName := fmt.Sprintf("%s-1", etcdClusterName)

			// Verify PVC usage before test
			verifyPodUsesPVC(t, c, targetPodName, "etcd-data-"+etcdClusterName)

			// Get initial member IDs for consistency verification
			initialMembers := getEtcdMembersName2IDMapping(t, c, podName)
			initialMemberCount := len(initialMembers)

			// Delete one pod to simulate failure
			var pod corev1.Pod
			if err := c.Client().Resources().Get(ctx, targetPodName, namespace, &pod); err != nil {
				t.Fatalf("Failed to get pod: %v", err)
			}

			deletedPodUID := pod.UID
			if err := c.Client().Resources().Delete(ctx, &pod); err != nil {
				t.Fatalf("Failed to delete pod: %v", err)
			}

			// Wait for pod recreation
			if err := wait.For(func(ctx context.Context) (done bool, err error) {
				var newPod corev1.Pod
				if err := c.Client().Resources().Get(ctx, targetPodName, namespace, &newPod); err != nil {
					return false, nil
				}
				return newPod.UID != deletedPodUID && newPod.Status.Phase == corev1.PodRunning, nil
			}, wait.WithTimeout(3*time.Minute), wait.WithInterval(10*time.Second)); err != nil {
				t.Fatalf("Pod failed to be recreated: %v", err)
			}

			// Wait for full StatefulSet readiness
			waitForSTSReadiness(t, c, etcdClusterName, int(initialReplicas))

			// Verify PVC usage after recovery
			verifyPodUsesPVC(t, c, targetPodName, "etcd-data-"+etcdClusterName)

			// Verify member ID consistency
			finalMembers := getEtcdMembersName2IDMapping(t, c, podName)
			if len(finalMembers) != initialMemberCount {
				t.Errorf("Member count changed after recovery: expected %d, got %d", initialMemberCount, len(finalMembers))
			}

			// Verify the recovered pod maintains the same member ID
			if targetMemberID, exists := initialMembers[targetPodName]; exists {
				if finalMemberID, exists := finalMembers[targetPodName]; exists {
					if targetMemberID != finalMemberID {
						t.Errorf("Member ID changed for %s: expected %d, got %d", targetPodName, targetMemberID, finalMemberID)
					}
				} else {
					t.Errorf("Member %s not found after recovery", targetPodName)
				}
			}

			// Verify cluster replication works across recovered pod
			_, stderr, err := execInPod(t, c, podName, namespace, []string{"etcdctl", "put", "replication-test", "value"})
			if err != nil {
				t.Fatalf("Failed to write data after recovery: %v, stderr: %s", err, stderr)
			}

			stdout, stderr, err := execInPod(t, c, targetPodName, namespace, []string{"etcdctl", "get", "replication-test"})
			if err != nil {
				t.Fatalf("Failed to read data from recovered pod: %v, stderr: %s", err, stderr)
			}

			if !strings.Contains(stdout, "value") {
				t.Errorf("Data replication to recovered pod failed. Expected 'value', got: %s", stdout)
			}

			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, etcdClusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

func TestEtcdClusterFunctionality(t *testing.T) {
	feature := features.New("etcd-cluster-functionality")
	etcdClusterName := "etcd-functionality-test"
	testKey := "test-key"
	testValue := "test-value"

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		createEtcdClusterWithPVC(ctx, t, c, etcdClusterName, 3)
		waitForSTSReadiness(t, c, etcdClusterName, 3)
		return ctx
	})

	feature.Assess("verify cluster health", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		podName := fmt.Sprintf("%s-0", etcdClusterName)

		// Wait until all members are promoted to voting members (no learners),
		// otherwise endpoint health will fail on learners.
		waitForNoLearners(t, c, podName, 3)

		// Check health for the whole cluster rather than a single member
		command := []string{"etcdctl", "endpoint", "health", "--cluster"}
		stdout, stderr, err := execInPod(t, c, podName, namespace, command)
		if err != nil {
			t.Fatalf("Failed to check endpoint health: %v, stderr: %s", err, stderr)
		}

		// Determine expected members dynamically from member list
		ml := getEtcdMemberListPB(t, c, podName)
		expected := len(ml.Members)

		// Count healthy lines; expect all members are healthy
		healthy := 0
		for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
			if strings.Contains(line, "is healthy") {
				healthy++
			}
		}
		if healthy != expected {
			t.Errorf("Expected %d healthy endpoints, but got %d. Output: %s", expected, healthy, stdout)
		}
		return ctx
	})

	feature.Assess("test data operations", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		podName := fmt.Sprintf("%s-0", etcdClusterName)

		// Write key-value data
		command := []string{"etcdctl", "put", testKey, testValue}
		_, stderr, err := execInPod(t, c, podName, namespace, command)
		if err != nil {
			t.Fatalf("Failed to write data: %v, stderr: %s", err, stderr)
		}

		// Read key-value data
		command = []string{"etcdctl", "get", testKey}
		stdout, stderr, err := execInPod(t, c, podName, namespace, command)
		if err != nil {
			t.Fatalf("Failed to read data: %v, stderr: %s", err, stderr)
		}

		lines := strings.Split(strings.TrimSpace(stdout), "\n")
		if len(lines) < 2 || lines[0] != testKey || lines[1] != testValue {
			t.Errorf("Expected key-value pair [%s=%s], but got output: %s", testKey, testValue, stdout)
		}
		return ctx
	})

	feature.Assess("verify data consistency with hashkv",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Use --cluster to fetch hashkv from all members in one call
			podName := fmt.Sprintf("%s-0", etcdClusterName)
			responses := getClusterEndpointHashKVs(t, c, podName)
			if len(responses) != 3 {
				t.Errorf("Expected 3 hashkv responses, got %d", len(responses))
			}

			hashes := make(map[uint32]struct{})
			for _, r := range responses {
				hashes[r.Hash] = struct{}{}
			}
			if len(hashes) != 1 {
				t.Errorf("Expected identical hashkv across all members, but got %d distinct hashes", len(hashes))
			}

			// Clean up - delete the key
			command := []string{"etcdctl", "del", testKey}
			_, stderr, err := execInPod(t, c, podName, namespace, command)
			if err != nil {
				t.Fatalf("Failed to delete data: %v, stderr: %s", err, stderr)
			}
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, etcdClusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
