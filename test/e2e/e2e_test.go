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

func TestScaleDownFrom3To1(t *testing.T) {
	feature := features.New("scale-down")
	etcdClusterName := "etcd-scale-down"

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		createEtcdCluster(ctx, t, c, etcdClusterName, 3)
		waitForStsReady(ctx, t, c, etcdClusterName, 3)
		return ctx
	})

	feature.Assess("scale down to 1", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var etcdCluster ec_v1alpha1.EtcdCluster
		if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster); err != nil {
			t.Fatalf("Failed to get EtcdCluster: %v", err)
		}

		etcdCluster.Spec.Size = 1
		if err := c.Client().Resources().Update(ctx, &etcdCluster); err != nil {
			t.Fatalf("Failed to update EtcdCluster: %v", err)
		}

		waitForStsReady(ctx, t, c, etcdClusterName, 1)
		return ctx
	})

	feature.Assess("verify member list", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		podName := fmt.Sprintf("%s-0", etcdClusterName)
		stdout, stderr, err := execInPod(t, c, podName, namespace, []string{"etcdctl", "member", "list"})
		if err != nil {
			t.Fatalf("Failed to exec in pod: %v, stderr: %s", err, stderr)
		}

		if len(strings.Split(strings.TrimSpace(stdout), "\n")) != 1 {
			t.Errorf("Expected to find 1 member in member list, but got: %s", stdout)
		}
		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var etcdCluster ec_v1alpha1.EtcdCluster
		if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster); err == nil {
			if err := c.Client().Resources().Delete(ctx, &etcdCluster); err != nil {
				t.Logf("Failed to delete EtcdCluster: %v", err)
			}
		}
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

func TestScaleUpFrom1To3(t *testing.T) {
	feature := features.New("scale-up")
	etcdClusterName := "etcd-scale-up"

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		createEtcdCluster(ctx, t, c, etcdClusterName, 1)
		waitForStsReady(ctx, t, c, etcdClusterName, 1)
		return ctx
	})

	feature.Assess("scale up to 3", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var etcdCluster ec_v1alpha1.EtcdCluster
		if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster); err != nil {
			t.Fatalf("Failed to get EtcdCluster: %v", err)
		}

		etcdCluster.Spec.Size = 3
		if err := c.Client().Resources().Update(ctx, &etcdCluster); err != nil {
			t.Fatalf("Failed to update EtcdCluster: %v", err)
		}

		waitForStsReady(ctx, t, c, etcdClusterName, 3)
		return ctx
	})

	feature.Assess("verify member list", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		podName := fmt.Sprintf("%s-0", etcdClusterName)
		stdout, stderr, err := execInPod(t, c, podName, namespace, []string{"etcdctl", "member", "list"})
		if err != nil {
			t.Fatalf("Failed to exec in pod: %v, stderr: %s", err, stderr)
		}

		if len(strings.Split(strings.TrimSpace(stdout), "\n")) != 3 {
			t.Errorf("Expected to find 3 members in member list, but got: %s", stdout)
		}
		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var etcdCluster ec_v1alpha1.EtcdCluster
		if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster); err == nil {
			if err := c.Client().Resources().Delete(ctx, &etcdCluster); err != nil {
				t.Logf("Failed to delete EtcdCluster: %v", err)
			}
		}
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

func TestPromoteReadyLearner(t *testing.T) {
	feature := features.New("promote-learner")
	etcdClusterName := "etcd-promote-learner"

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		createEtcdCluster(ctx, t, c, etcdClusterName, 2)
		waitForStsReady(ctx, t, c, etcdClusterName, 2)
		// Manually add a third member as a learner
		podName := fmt.Sprintf("%s-0", etcdClusterName)
		_, stderr, err := execInPod(t, c, podName, namespace, []string{"etcdctl", "member", "add", "etcd-promote-learner-2", "--peer-urls=http://etcd-promote-learner-2.etcd-promote-learner.etcd-operator-system.svc.cluster.local:2380", "--learner"})
		if err != nil {
			t.Fatalf("Failed to add learner: %v, stderr: %s", err, stderr)
		}
		return ctx
	})

	feature.Assess("promote learner", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var etcdCluster ec_v1alpha1.EtcdCluster
		if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster); err != nil {
			t.Fatalf("Failed to get EtcdCluster: %v", err)
		}

		etcdCluster.Spec.Size = 3
		if err := c.Client().Resources().Update(ctx, &etcdCluster); err != nil {
			t.Fatalf("Failed to update EtcdCluster: %v", err)
		}

		// Wait for the learner to be promoted.
		wait.For(func(ctx context.Context) (done bool, err error) {
			podName := fmt.Sprintf("%s-0", etcdClusterName)
			command := []string{"etcdctl", "member", "list", "-w", "table"}
			stdout, _, err := execInPod(t, c, podName, namespace, command)
			if err != nil {
				return false, nil
			}
			return !strings.Contains(stdout, "true"), nil
		}, wait.WithTimeout(2*time.Minute), wait.WithInterval(5*time.Second))

		return ctx
	})

	feature.Assess("verify member list", func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		podName := fmt.Sprintf("%s-0", etcdClusterName)
		command := []string{"etcdctl", "member", "list", "-w", "table"}
		stdout, stderr, err := execInPod(t, c, podName, namespace, command)
		if err != nil {
			t.Fatalf("Failed to exec in pod: %v, stderr: %s", err, stderr)
		}

		if strings.Contains(stdout, "true") {
			t.Errorf("Expected all members not to be learners, but found a learner. Member list:\n%s", stdout)
		}
		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		var etcdCluster ec_v1alpha1.EtcdCluster
		if err := c.Client().Resources().Get(ctx, etcdClusterName, namespace, &etcdCluster); err == nil {
			if err := c.Client().Resources().Delete(ctx, &etcdCluster); err != nil {
				t.Logf("Failed to delete EtcdCluster: %v", err)
			}
		}
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
