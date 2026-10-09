package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/controller"
)

// TestQuorumRecovery loses quorum by deleting two of three member Pods,
// which nothing recreates, then asks for a recovery through
// controller.QuorumRecoveryAnnotation and checks that the cluster is rebuilt
// from the survivor's data.
func TestQuorumRecovery(t *testing.T) {
	feature := features.New("quorum-recovery")
	clusterName := "etcd-quorum-recovery"
	survivor := clusterName + "-1"
	key, value := "written-before-quorum-loss", "kept"

	getCluster := func(ctx context.Context, t *testing.T, c *envconf.Config) *ecv1alpha1.EtcdCluster {
		t.Helper()
		ec := &ecv1alpha1.EtcdCluster{}
		if err := c.Client().Resources().Get(ctx, clusterName, namespace, ec); err != nil {
			t.Fatalf("Failed to get EtcdCluster %s: %v", clusterName, err)
		}
		return ec
	}

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

	feature.Assess("the operator waits once two of three Pods are gone",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			for _, ordinal := range []int{0, 2} {
				pod := &corev1.Pod{}
				name := fmt.Sprintf("%s-%d", clusterName, ordinal)
				if err := c.Client().Resources().Get(ctx, name, namespace, pod); err != nil {
					t.Fatalf("Failed to get Pod %s: %v", name, err)
				}
				if err := c.Client().Resources().Delete(ctx, pod); err != nil {
					t.Fatalf("Failed to delete Pod %s: %v", name, err)
				}
			}

			// Pods aren't watched, so touch the EtcdCluster to get a reconcile
			// going; once unhealthy, it requeues on its own. Wait long enough
			// for several reconciles: the operator must neither recreate the
			// Pods nor start a recovery on its own.
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				ec := getCluster(ctx, t, c)
				if ec.Annotations == nil {
					ec.Annotations = map[string]string{}
				}
				ec.Annotations["e2e.etcd.io/poke"] = time.Now().Format(time.RFC3339Nano)
				return c.Client().Resources().Update(ctx, ec)
			})
			if err != nil {
				t.Fatalf("Failed to touch EtcdCluster %s: %v", clusterName, err)
			}
			time.Sleep(30 * time.Second)

			var pods corev1.PodList
			if err := c.Client().Resources(namespace).List(ctx, &pods); err != nil {
				t.Fatalf("Failed to list Pods: %v", err)
			}
			var names []string
			for _, p := range pods.Items {
				if strings.HasPrefix(p.Name, clusterName+"-") && p.DeletionTimestamp == nil {
					names = append(names, p.Name)
				}
			}
			if !slices.Equal(names, []string{survivor}) {
				t.Fatalf("Pods of %s = %v, want only %s", clusterName, names, survivor)
			}
			if qr := getCluster(ctx, t, c).Status.QuorumRecovery; qr != nil {
				t.Fatalf("Status.QuorumRecovery = %+v, want nil before anyone asks for a recovery", qr)
			}
			return ctx
		})

	feature.Assess("the annotation rebuilds the cluster from the survivor",
		func(ctx context.Context, t *testing.T, c *envconf.Config) context.Context {
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				ec := getCluster(ctx, t, c)
				if ec.Annotations == nil {
					ec.Annotations = map[string]string{}
				}
				ec.Annotations[controller.QuorumRecoveryAnnotation] = "1"
				return c.Client().Resources().Update(ctx, ec)
			})
			if err != nil {
				t.Fatalf("Failed to annotate EtcdCluster %s: %v", clusterName, err)
			}

			err = wait.For(func(ctx context.Context) (bool, error) {
				ec := getCluster(ctx, t, c)
				return ec.Status.RecoveryCount == 1 && ec.Status.QuorumRecovery == nil, nil
			}, wait.WithTimeout(5*time.Minute), wait.WithInterval(5*time.Second))
			if err != nil {
				t.Fatalf("Recovery of %s did not finish: %v", clusterName, err)
			}
			ec := getCluster(ctx, t, c)
			if ec.Status.LastRecoveredFrom != "1" {
				t.Errorf("Status.LastRecoveredFrom = %q, want %q", ec.Status.LastRecoveredFrom, "1")
			}
			if _, ok := ec.Annotations[controller.QuorumRecoveryAnnotation]; ok {
				t.Errorf("annotation %s was not removed", controller.QuorumRecoveryAnnotation)
			}

			if err := waitForAllEtcdMemberReady(t, c, etcdClusterRef(clusterName, 3)); err != nil {
				t.Fatalf("cluster %s was not rebuilt to 3 members: %v", clusterName, err)
			}
			waitForNoLearners(t, c, survivor, 3, 2*time.Minute)

			pod := &corev1.Pod{}
			if err := c.Client().Resources().Get(ctx, survivor, namespace, pod); err != nil {
				t.Fatalf("Failed to get Pod %s: %v", survivor, err)
			}
			if slices.Contains(pod.Spec.Containers[0].Args, "--force-new-cluster") {
				t.Errorf("Pod %s still starts etcd with --force-new-cluster", survivor)
			}

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
		err := wait.For(func(ctx context.Context) (bool, error) {
			ec := &ecv1alpha1.EtcdCluster{}
			err := c.Client().Resources().Get(ctx, clusterName, namespace, ec)
			return apierrors.IsNotFound(err), nil
		}, wait.WithTimeout(3*time.Minute), wait.WithInterval(5*time.Second))
		if err != nil {
			t.Logf("EtcdCluster %s was not deleted: %v", clusterName, err)
		}
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
