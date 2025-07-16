package e2e

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
)

func createEtcdCluster(ctx context.Context, t *testing.T, c *envconf.Config, name string, size int) *ecv1alpha1.EtcdCluster {
	t.Helper()
	etcdCluster := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: etcdVersion,
		},
	}
	if err := c.Client().Resources().Create(ctx, etcdCluster); err != nil {
		t.Fatalf("Failed to create EtcdCluster: %v", err)
	}
	return etcdCluster
}

func waitForStsReady(ctx context.Context, t *testing.T, c *envconf.Config, name string, expectedReplicas int) {
	t.Helper()
	var sts appsv1.StatefulSet
	err := wait.For(func(ctx context.Context) (done bool, err error) {
		if err := c.Client().Resources().Get(ctx, name, namespace, &sts); err != nil {
			return false, err
		}
		if sts.Status.ReadyReplicas == int32(expectedReplicas) {
			return true, nil
		}
		return false, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(10*time.Second))
	if err != nil {
		t.Fatalf("StatefulSet %s failed to have %d ready replicas: %v", name, expectedReplicas, err)
	}

	// Additional wait for the StatefulSet to be fully ready
	if err := wait.For(
		conditions.New(c.Client().Resources()).ResourceScaled(&sts, func(object k8s.Object) int32 {
			return sts.Status.ReadyReplicas
		}, int32(expectedReplicas)),
		wait.WithTimeout(3*time.Minute),
		wait.WithInterval(10*time.Second),
	); err != nil {
		t.Fatalf("unable to create sts with size %d: %s", int32(expectedReplicas), err)
	}
}

func execInPod(t *testing.T, cfg *envconf.Config, podName string, namespace string, command []string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	client := cfg.Client()

	// Find the pod
	var pod corev1.Pod
	if err := client.Resources().Get(context.Background(), podName, namespace, &pod); err != nil {
		return "", "", fmt.Errorf("failed to get pod %s/%s: %w", namespace, podName, err)
	}

	// Find the container
	if len(pod.Spec.Containers) == 0 {
		return "", "", fmt.Errorf("no containers in pod %s/%s", namespace, podName)
	}
	containerName := pod.Spec.Containers[0].Name

	// Exec command
	err := client.Resources().ExecInPod(context.Background(), namespace, podName, containerName, command, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}
