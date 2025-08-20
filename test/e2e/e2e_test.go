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

	ec_v1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
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

	etcdClusterSpec := &ec_v1alpha1.EtcdCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "operator.etcd.io/v1alpha1",
			Kind:       "EtcdCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdClusterName,
			Namespace: namespace,
		},
		Spec: ec_v1alpha1.EtcdClusterSpec{
			Size:    0,
			Version: etcdVersion,
		},
	}

	for name, size := range tt {
		feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {

			// update cluster name
			etcdClusterSpec.ObjectMeta.Name = strings.Join([]string{etcdClusterName, name}, "-")
			etcdClusterSpec.Spec.Size = size

			err := c.Client().Resources().Create(ctx, etcdClusterSpec)
			if !errors.IsInvalid(err) {
				t.Fatalf("expected invalid etcdCluster %s with size %d. Got: %v", name, size, err)
			}

			return ctx
		})

		feature.Assess(fmt.Sprintf("etcdCluster %s should not be created when etcdCluster.Spec.Size is %d", name, size),
			func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {

				var etcdCluster ec_v1alpha1.EtcdCluster
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
				createEtcdCluster(ctx, t, c, etcdClusterName, tc.initialSize)
				waitForStsReady(t, c, etcdClusterName, tc.initialSize)
				return ctx
			})

			feature.Assess(
				fmt.Sprintf("scale to %d", tc.scaleTo),
				func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
					scaleEtcdCluster(ctx, t, c, etcdClusterName, tc.scaleTo)

					// Verify controller updated StatefulSet replicas
					var sts appsv1.StatefulSet
					if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &sts); err != nil {
						t.Fatalf("Failed to get StatefulSet: %v", err)
					}
					if sts.Spec.Replicas == nil || *sts.Spec.Replicas != int32(tc.scaleTo) {
						t.Errorf("Controller failed to update StatefulSet replicas: expected %d, got %d", tc.scaleTo, *sts.Spec.Replicas)
					}
					return ctx
				},
			)

			feature.Assess("verify member list", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
				podName := fmt.Sprintf("%s-0", etcdClusterName)
				stdout, stderr, err := execInPod(t, c, podName, namespace, []string{"etcdctl", "member", "list"})
				if err != nil {
					t.Fatalf("Failed to exec in pod: %v, stderr: %s", err, stderr)
				}

				memberLines := strings.Split(strings.TrimSpace(stdout), "\n")
				if len(memberLines) != tc.expectedMembers {
					t.Errorf("Expected to find %d members in member list, but got %d. Output: %s",
						tc.expectedMembers, len(memberLines), stdout)
				}

				// Verify controller promoted all learners to voting members
				for _, line := range memberLines {
					if strings.Contains(line, "isLearner=true") {
						t.Errorf("Found unpromoted learner after scaling completed: %s", line)
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

// TODO: TestPodRecovery is commented out - would like maintainer feedback on expected behavior
//
// When a Pod is deleted (simulating failure), the current controller behavior is:
// 1. Detects targetReplicas (3) != memberCnt (2)
// 2. Reduces StatefulSet replicas from 3 to 2 (lines 144-163 in etcdcluster_controller.go)
// 3. This prevents Pod recovery since StatefulSet now expects only 2 replicas
//
// Question: Is this the intended behavior? Or would we expect:
// - Pod deletion to be handled by StatefulSet (which recreates the Pod)
// - Controller to only adjust replicas when EtcdCluster.Spec.Size changes
//
// Happy to open an issue for discussion or adjust the test based on design intent
//
// func TestPodRecovery(t *testing.T) {
// 	feature := features.New("pod-recovery")
// 	etcdClusterName := "etcd-recovery-test"
//
// 	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
// 		createEtcdCluster(ctx, t, c, etcdClusterName, 3)
// 		waitForStsReady(t, c, etcdClusterName, 3)
// 		return ctx
// 	})
//
// 	feature.Assess("delete pod and verify controller recovery",
//		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
// 		// Delete one pod to simulate failure
// 		podName := fmt.Sprintf("%s-1", etcdClusterName)
// 		var pod corev1.Pod
// 		if err := c.Client().Resources().Get(ctx, podName, namespace, &pod); err != nil {
// 			t.Fatalf("Failed to get pod: %v", err)
// 		}
//
// 		if err := c.Client().Resources().Delete(ctx, &pod); err != nil {
// 			t.Fatalf("Failed to delete pod: %v", err)
// 		}
//
// 		// Wait for StatefulSet to recreate the pod
// 		waitForStsReady(t, c, etcdClusterName, 3)
//
// 		// Verify etcd cluster still has 3 healthy members
// 		podName = fmt.Sprintf("%s-0", etcdClusterName)
// 		stdout, stderr, err := execInPod(t, c, podName, namespace, []string{"etcdctl", "member", "list"})
// 		if err != nil {
// 			t.Fatalf("Failed to list members after pod recovery: %v, stderr: %s", err, stderr)
// 		}
//
// 		memberLines := strings.Split(strings.TrimSpace(stdout), "\n")
// 		if len(memberLines) != 3 {
// 			t.Errorf("Expected 3 members after pod recovery, got %d. Output: %s", len(memberLines), stdout)
// 		}
//
// 		return ctx
// 	})
//
// 	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
// 		cleanupEtcdCluster(ctx, t, c, etcdClusterName)
// 		return ctx
// 	})
//
// 	_ = testEnv.Test(t, feature.Feature())
// }

func TestEtcdClusterFunctionality(t *testing.T) {
	feature := features.New("etcd-cluster-functionality")
	etcdClusterName := "etcd-functionality-test"
	testKey := "test-key"
	testValue := "test-value"

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		createEtcdCluster(ctx, t, c, etcdClusterName, 3)
		waitForStsReady(t, c, etcdClusterName, 3)
		return ctx
	})

	feature.Assess("verify cluster health", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		podName := fmt.Sprintf("%s-0", etcdClusterName)
		command := []string{"etcdctl", "endpoint", "health"}
		stdout, stderr, err := execInPod(t, c, podName, namespace, command)
		if err != nil {
			t.Fatalf("Failed to check endpoint health: %v, stderr: %s", err, stderr)
		}

		if !strings.Contains(stdout, "is healthy") {
			t.Errorf("Expected healthy endpoints, but got: %s", stdout)
		}
		return ctx
	})

	feature.Assess("verify member list", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		podName := fmt.Sprintf("%s-0", etcdClusterName)
		command := []string{"etcdctl", "member", "list"}
		stdout, stderr, err := execInPod(t, c, podName, namespace, command)
		if err != nil {
			t.Fatalf("Failed to list members: %v, stderr: %s", err, stderr)
		}

		memberLines := strings.Split(strings.TrimSpace(stdout), "\n")
		if len(memberLines) != 3 {
			t.Errorf("Expected 3 members, but got %d. Output: %s", len(memberLines), stdout)
		}

		// Verify all members are voting members (not learners)
		for _, line := range memberLines {
			if strings.Contains(line, "isLearner=true") {
				t.Errorf("Found learner member in healthy cluster: %s", line)
			}
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

	feature.Assess("verify data replication across members",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Read from a different pod to verify data replication
			podName := fmt.Sprintf("%s-1", etcdClusterName)
			command := []string{"etcdctl", "get", testKey}
			stdout, stderr, err := execInPod(t, c, podName, namespace, command)
			if err != nil {
				t.Fatalf("Failed to read data from different member: %v, stderr: %s", err, stderr)
			}

			lines := strings.Split(strings.TrimSpace(stdout), "\n")
			if len(lines) < 2 || lines[0] != testKey || lines[1] != testValue {
				t.Errorf("Data not replicated properly. Expected [%s=%s], but got: %s", testKey, testValue, stdout)
			}

			// Clean up - delete the key
			podName = fmt.Sprintf("%s-0", etcdClusterName)
			command = []string{"etcdctl", "del", testKey}
			_, stderr, err = execInPod(t, c, podName, namespace, command)
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
