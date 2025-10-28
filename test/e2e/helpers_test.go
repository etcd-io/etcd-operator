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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	test_utils "go.etcd.io/etcd-operator/test/utils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

const (
	deployMethodKustomize = "kustomize"
	deployMethodHelm      = "helm"
)

// getAvailableStorageClass returns an available StorageClass name
func getAvailableStorageClass(ctx context.Context, t *testing.T, c *envconf.Config) string {
	t.Helper()

	// First check environment variable
	if storageClass := os.Getenv("ETCD_E2E_STORAGECLASS"); storageClass != "" {
		return storageClass
	}

	// Try to find default StorageClass
	var storageClasses storagev1.StorageClassList
	if err := c.Client().Resources().List(ctx, &storageClasses); err != nil {
		t.Skip("Cannot list StorageClasses, skipping PVC test")
	}

	// Look for default StorageClass
	for _, sc := range storageClasses.Items {
		if sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" {
			return sc.Name
		}
	}

	// Fallback to common StorageClass names
	commonNames := []string{"standard", "gp2", "default"}
	for _, name := range commonNames {
		for _, sc := range storageClasses.Items {
			if sc.Name == name {
				return name
			}
		}
	}

	t.Skip("No suitable StorageClass found for PVC test")
	return ""
}

// createEtcdClusterWithPVC creates an EtcdCluster with persistent storage
func createEtcdClusterWithPVC(ctx context.Context, t *testing.T, c *envconf.Config, name string, size int) {
	t.Helper()
	storageClassName := getAvailableStorageClass(ctx, t, c)

	etcdCluster := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    size,
			Version: etcdVersion,
			StorageSpec: &ecv1alpha1.StorageSpec{
				AccessModes:       corev1.ReadWriteOnce,
				StorageClassName:  storageClassName,
				VolumeSizeRequest: resource.MustParse("64Mi"),
				VolumeSizeLimit:   resource.MustParse("64Mi"),
			},
		},
	}
	if err := c.Client().Resources().Create(ctx, etcdCluster); err != nil {
		t.Fatalf("Failed to create EtcdCluster with PVC: %v", err)
	}
}

func waitForSTSReadiness(t *testing.T, c *envconf.Config, name string, expectedReplicas int) {
	t.Helper()
	var sts appsv1.StatefulSet
	err := wait.For(func(ctx context.Context) (done bool, err error) {
		if err := c.Client().Resources().Get(ctx, name, namespace, &sts); err != nil {
			return false, err
		}

		// Ensure spec has been updated by the controller to the expected replica count
		if sts.Spec.Replicas == nil || *sts.Spec.Replicas != int32(expectedReplicas) {
			return false, nil
		}

		// Ensure status reflects latest generation
		if sts.Status.ObservedGeneration < sts.Generation {
			return false, nil
		}

		// Ensure the controller created the expected number of replicas and they are ready
		if sts.Status.Replicas != int32(expectedReplicas) {
			return false, nil
		}
		if sts.Status.ReadyReplicas != int32(expectedReplicas) {
			return false, nil
		}
		return true, nil
	}, wait.WithTimeout(5*time.Minute), wait.WithInterval(10*time.Second))
	if err != nil {
		t.Fatalf("StatefulSet %s failed to reach spec/status readiness for %d replicas: %v", name, expectedReplicas, err)
	}
}

func execInPod(
	t *testing.T, cfg *envconf.Config, podName string, namespace string, command []string,
) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	client := cfg.Client()

	// Find the pod
	var pod corev1.Pod
	if err := client.Resources().Get(t.Context(), podName, namespace, &pod); err != nil {
		return "", "", fmt.Errorf("failed to get pod %s/%s: %w", namespace, podName, err)
	}

	// Find the container
	if len(pod.Spec.Containers) == 0 {
		return "", "", fmt.Errorf("no containers in pod %s/%s", namespace, podName)
	}
	containerName := pod.Spec.Containers[0].Name

	// Exec command
	err := client.Resources().ExecInPod(t.Context(), namespace, podName, containerName, command, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func scaleEtcdCluster(ctx context.Context, t *testing.T, c *envconf.Config, name string, size int) {
	t.Helper()
	var etcdCluster ecv1alpha1.EtcdCluster
	if err := c.Client().Resources().Get(ctx, name, namespace, &etcdCluster); err != nil {
		t.Fatalf("Failed to get EtcdCluster: %v", err)
	}

	etcdCluster.Spec.Size = size
	if err := c.Client().Resources().Update(ctx, &etcdCluster); err != nil {
		t.Fatalf("Failed to update EtcdCluster: %v", err)
	}
}

func cleanupEtcdCluster(ctx context.Context, t *testing.T, c *envconf.Config, name string) {
	t.Helper()
	var etcdCluster ecv1alpha1.EtcdCluster
	if err := c.Client().Resources().Get(ctx, name, namespace, &etcdCluster); err == nil {
		if err := c.Client().Resources().Delete(ctx, &etcdCluster); err != nil {
			t.Logf("Failed to delete EtcdCluster: %v", err)
		}
	}
}

// getEtcdMembersName2IDMapping retrieves the etcd cluster member list as name->ID mapping using etcd's native types
func getEtcdMembersName2IDMapping(t *testing.T, c *envconf.Config, podName string) map[string]uint64 {
	t.Helper()
	memberList := getEtcdMemberListPB(t, c, podName)

	// Create name->ID mapping
	memberMap := make(map[string]uint64)
	for _, member := range memberList.Members {
		memberMap[member.Name] = member.ID
	}
	return memberMap
}

// getEtcdMemberListPB returns the etcdserverpb.MemberListResponse by calling etcdctl -w json.
func getEtcdMemberListPB(t *testing.T, c *envconf.Config, podName string) *etcdserverpb.MemberListResponse {
	t.Helper()
	stdout, stderr, err := execInPod(t, c, podName, namespace, []string{"etcdctl", "member", "list", "-w", "json"})
	if err != nil {
		t.Fatalf("Failed to get etcd member list: %v, stderr: %s", err, stderr)
	}
	var memberList etcdserverpb.MemberListResponse
	if err := json.Unmarshal([]byte(stdout), &memberList); err != nil {
		t.Fatalf("Failed to parse etcd member list JSON: %v", err)
	}
	return &memberList
}

// waitForNoLearners waits until the member list has the expected number of members
// and all members are voting (i.e., no learners remain).
func waitForNoLearners(t *testing.T, c *envconf.Config, podName string, expectedMembers int) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		ml := getEtcdMemberListPB(t, c, podName)
		if len(ml.Members) != expectedMembers {
			return false, nil
		}
		for _, m := range ml.Members {
			if m.IsLearner {
				return false, nil
			}
		}
		return true, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("Timeout waiting for %d voting members with no learners: %v", expectedMembers, err)
	}
}

// verifyPodUsesPVC checks that a pod is using PVC for persistent storage
func verifyPodUsesPVC(t *testing.T, c *envconf.Config, podName string, expectedPVCPrefix string) {
	t.Helper()
	var pod corev1.Pod
	if err := c.Client().Resources().Get(t.Context(), podName, namespace, &pod); err != nil {
		t.Fatalf("Failed to get pod %s: %v", podName, err)
	}

	// Check for PVC volumes
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			if strings.HasPrefix(volume.PersistentVolumeClaim.ClaimName, expectedPVCPrefix) {
				return
			}
		}
	}

	t.Errorf("Pod %s does not use expected PVC with prefix %s", podName, expectedPVCPrefix)
}

// getClusterEndpointHashKVs executes `etcdctl endpoint hashkv --cluster -w json` inside the given pod
// and returns the parsed HashKV responses from all known endpoints using etcd's native types.
func getClusterEndpointHashKVs(t *testing.T, c *envconf.Config, podName string) []etcdserverpb.HashKVResponse {
	t.Helper()
	cmd := []string{"etcdctl", "endpoint", "hashkv", "--cluster", "-w", "json"}
	stdout, stderr, err := execInPod(t, c, podName, namespace, cmd)
	if err != nil {
		t.Fatalf("Failed to get cluster endpoint hashkv from %s: %v, stderr: %s", podName, err, stderr)
	}

	// Expected JSON: array of objects like {"Endpoint":"...","HashKV":{...}}
	var entries []struct {
		Endpoint string                      `json:"Endpoint"`
		HashKV   etcdserverpb.HashKVResponse `json:"HashKV"`
	}
	if err := json.Unmarshal([]byte(stdout), &entries); err != nil {
		t.Fatalf("Failed to parse endpoint hashkv JSON: %v. Raw: %s", err, stdout)
	}
	out := make([]etcdserverpb.HashKVResponse, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.HashKV)
	}
	return out
}

func verifyDataOperations(t *testing.T, c *envconf.Config, etcdClusterName, testKey, testValue string) {
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
}

// getDeployMethod retrieves the deployment method from environment variable,
// defaults to "kustomize" if not set.
func getDeployMethod() string {
	method := os.Getenv("DEPLOY_METHOD")
	if method == "" {
		return deployMethodKustomize
	}
	return method
}

// deployOperator deploys the operator based on the deployment method.
// It supports both Kustomize and Helm deployment methods.
func deployOperator(deployMethod, imageName, namespace string) error {
	log.Printf("Deploying operator using method: %s", deployMethod)

	switch deployMethod {
	case deployMethodHelm:
		return deployWithHelm(imageName, namespace)
	case deployMethodKustomize:
		return deployWithKustomize(imageName)
	default:
		return fmt.Errorf("unknown DEPLOY_METHOD: %s (supported: %s, %s)",
			deployMethod, deployMethodKustomize, deployMethodHelm)
	}
}

// deployWithKustomize deploys the operator using Kustomize.
func deployWithKustomize(imageName string) error {
	log.Println("Deploying controller-manager resources with Kustomize...")
	cmd := exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", imageName))
	if _, err := test_utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to deploy with Kustomize: %w", err)
	}
	return nil
}

// deployWithHelm deploys the operator using Helm Chart.
func deployWithHelm(imageName, namespace string) error {
	log.Println("Generating Helm Chart...")
	cmd := exec.Command("make", "helm")
	if _, err := test_utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to generate Helm Chart: %w", err)
	}

	// Check for custom values file
	helmValues := os.Getenv("HELM_VALUES")
	if helmValues == "" {
		helmValues = "helm/values.yaml"
	} else {
		log.Printf("Using custom values file: %s", helmValues)
	}

	// Verify values file exists
	if _, err := os.Stat(helmValues); os.IsNotExist(err) {
		return fmt.Errorf("specified HELM_VALUES file does not exist: %s", helmValues)
	}

	log.Println("Installing operator with Helm...")
	args := []string{
		"install", "etcd-operator", "./helm",
		"-f", helmValues,
		"-n", namespace,
		"--wait",
		"--timeout", "5m",
		"--set", fmt.Sprintf("controllerManager.manager.image.repository=%s", getImageRepository(imageName)),
		"--set", fmt.Sprintf("controllerManager.manager.image.tag=%s", getImageTag(imageName)),
	}

	cmd = exec.Command("helm", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("helm install failed: %w\nOutput: %s", err, string(output))
	}

	log.Println("Helm Chart installed successfully")
	return nil
}

// undeployOperator undeploys the operator based on the deployment method.
func undeployOperator(deployMethod, namespace string) error {
	log.Printf("Undeploying operator using method: %s", deployMethod)

	switch deployMethod {
	case deployMethodHelm:
		return undeployWithHelm(namespace)
	case deployMethodKustomize:
		return undeployWithKustomize()
	default:
		return fmt.Errorf("unknown DEPLOY_METHOD: %s", deployMethod)
	}
}

// undeployWithKustomize undeploys the operator using Kustomize.
func undeployWithKustomize() error {
	cmd := exec.Command("make", "undeploy", "ignore-not-found=true")
	if _, err := test_utils.Run(cmd); err != nil {
		return fmt.Errorf("failed to undeploy with Kustomize: %w", err)
	}
	return nil
}

// undeployWithHelm undeploys the operator using Helm.
func undeployWithHelm(namespace string) error {
	log.Println("Uninstalling operator with Helm...")
	cmd := exec.Command("helm", "uninstall", "etcd-operator", "-n", namespace)
	if output, err := cmd.CombinedOutput(); err != nil {
		// Helm uninstall failures should only be logged as warnings during cleanup
		log.Printf("Warning: helm uninstall failed: %v\nOutput: %s", err, string(output))
	}
	return nil
}

// getImageRepository extracts the repository part from a full image name.
// Examples:
//   - "etcd-operator:v0.1" -> "etcd-operator"
//   - "ghcr.io/etcd-io/etcd-operator:v0.1" -> "ghcr.io/etcd-io/etcd-operator"
func getImageRepository(imageName string) string {
	parts := splitImageNameTag(imageName)
	return parts[0]
}

// getImageTag extracts the tag from a full image name.
// Examples:
//   - "etcd-operator:v0.1" -> "v0.1"
//   - "etcd-operator" -> "latest"
func getImageTag(imageName string) string {
	parts := splitImageNameTag(imageName)
	if len(parts) > 1 {
		return parts[1]
	}
	return "latest"
}

// splitImageNameTag splits an image name into repository and tag parts.
func splitImageNameTag(imageName string) []string {
	idx := strings.LastIndex(imageName, ":")
	if idx != -1 {
		return []string{imageName[:idx], imageName[idx+1:]}
	}
	return []string{imageName}
}
