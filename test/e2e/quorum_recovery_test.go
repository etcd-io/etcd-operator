package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// recoverClusterFromAnnotation is how a human operator starts a lost-quorum
// recovery: its value is the ordinal of the member to recover from.
const recoverClusterFromAnnotation = "operator.etcd.io/recover-cluster-from"

func getEtcdCluster(ctx context.Context, t *testing.T, c *envconf.Config, name string) *ecv1alpha1.EtcdCluster {
	t.Helper()
	ec := &ecv1alpha1.EtcdCluster{}
	if err := c.Client().Resources().Get(ctx, name, namespace, ec); err != nil {
		t.Fatalf("Failed to get EtcdCluster %s: %v", name, err)
	}
	return ec
}

func annotateEtcdCluster(ctx context.Context, t *testing.T, c *envconf.Config, name, key, value string) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ec := getEtcdCluster(ctx, t, c, name)
		if ec.Annotations == nil {
			ec.Annotations = map[string]string{}
		}
		ec.Annotations[key] = value
		return c.Client().Resources().Update(ctx, ec)
	})
	if err != nil {
		t.Fatalf("Failed to annotate EtcdCluster %s with %s=%s: %v", name, key, value, err)
	}
}

// waitForClusterEvent waits until an event with the given reason has been
// recorded for the EtcdCluster.
func waitForClusterEvent(t *testing.T, c *envconf.Config, clusterName, reason string) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var list eventsv1.EventList
		if err := c.Client().Resources(namespace).List(ctx, &list); err != nil {
			return false, err
		}
		for _, e := range list.Items {
			if e.Regarding.Kind == "EtcdCluster" && e.Regarding.Name == clusterName && e.Reason == reason {
				return true, nil
			}
		}
		return false, nil
	}, wait.WithTimeout(2*time.Minute), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("No %s event was recorded for EtcdCluster %s: %v", reason, clusterName, err)
	}
}

// TestQuorumRecovery loses quorum by deleting two of three member Pods,
// which nothing recreates, then requests a recovery from the third member
// and checks that the cluster is rebuilt from its data.
func TestQuorumRecovery(t *testing.T) {
	feature := features.New("quorum-recovery")
	clusterName := "etcd-quorum-recovery"
	survivor := clusterName + "-1"
	key, value := "written-before-quorum-loss", "kept"

	feature.Setup(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		createEtcdClusterWithPVC(ctx, t, c, clusterName, 3)
		if err := waitForAllEtcdMemberReady(t, c, etcdClusterRef(clusterName, 3)); err != nil {
			t.Fatalf("etcd pods of cluster %s failed to reach readiness: %v", clusterName, err)
		}
		put := append(etcdctlCmd(survivor, clusterName, namespace, false), "put", key, value)
		if _, stderr, err := execInPod(t, c, survivor, namespace, put); err != nil {
			t.Fatalf("Failed to write %s through %s: %v, stderr: %s", key, survivor, err, stderr)
		}
		return ctx
	})

	feature.Assess("two of three member Pods are lost",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			for _, ordinal := range []int{0, 2} {
				name := fmt.Sprintf("%s-%d", clusterName, ordinal)
				pod := &corev1.Pod{}
				if err := c.Client().Resources().Get(ctx, name, namespace, pod); err != nil {
					t.Fatalf("Failed to get Pod %s: %v", name, err)
				}
				if err := c.Client().Resources().Delete(ctx, pod); err != nil {
					t.Fatalf("Failed to delete Pod %s: %v", name, err)
				}
				err := wait.For(func(ctx context.Context) (bool, error) {
					err := c.Client().Resources().Get(ctx, name, namespace, &corev1.Pod{})
					return apierrors.IsNotFound(err), nil
				}, wait.WithTimeout(2*time.Minute), wait.WithInterval(2*time.Second))
				if err != nil {
					t.Fatalf("Pod %s was not deleted: %v", name, err)
				}
			}
			return ctx
		})

	feature.Assess("the operator waits for a recovery request",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			// Pods aren't watched, so touch the EtcdCluster to start a
			// reconcile. While the cluster is unhealthy it requeues on its own.
			annotateEtcdCluster(ctx, t, c, clusterName, "e2e.etcd.io/poke", time.Now().Format(time.RFC3339Nano))
			waitForClusterEvent(t, c, clusterName, "ClusterUnhealthy")
			if qr := getEtcdCluster(ctx, t, c, clusterName).Status.QuorumRecovery; qr != nil {
				t.Fatalf("Status.QuorumRecovery = %+v, want nil before anyone requests a recovery", qr)
			}
			return ctx
		})

	feature.Assess("the annotation rebuilds the cluster from the survivor",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			annotateEtcdCluster(ctx, t, c, clusterName, recoverClusterFromAnnotation, "1")

			// The annotation is removed once the recovery has completed,
			// which is also when it is recorded.
			err := wait.For(func(ctx context.Context) (bool, error) {
				ec := getEtcdCluster(ctx, t, c, clusterName)
				_, requested := ec.Annotations[recoverClusterFromAnnotation]
				return ec.Status.RecoveryCount == 1 && ec.Status.QuorumRecovery == nil && !requested, nil
			}, wait.WithTimeout(5*time.Minute), wait.WithInterval(5*time.Second))
			if err != nil {
				t.Fatalf("Recovery of %s did not complete: %v", clusterName, err)
			}
			if got := getEtcdCluster(ctx, t, c, clusterName).Status.LastRecoveredFrom; got != "1" {
				t.Errorf("Status.LastRecoveredFrom = %q, want %q", got, "1")
			}
			waitForClusterEvent(t, c, clusterName, "QuorumRecoveryStarted")

			if err := waitForAllEtcdMemberReady(t, c, etcdClusterRef(clusterName, 3)); err != nil {
				t.Fatalf("cluster %s was not rebuilt to 3 members: %v", clusterName, err)
			}
			waitForNoLearners(t, c, survivor, 3, 2*time.Minute)

			for ordinal := range 3 {
				name := fmt.Sprintf("%s-%d", clusterName, ordinal)
				get := append(etcdctlCmd(name, clusterName, namespace, false), "get", key, "--print-value-only")
				stdout, stderr, err := execInPod(t, c, name, namespace, get)
				if err != nil {
					t.Fatalf("Failed to read %s through %s: %v, stderr: %s", key, name, err, stderr)
				}
				if got := strings.TrimSpace(stdout); got != value {
					t.Errorf("%s: get %s = %q, want %q", name, key, got, value)
				}
			}
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
		cleanupEtcdCluster(ctx, t, c, clusterName)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
