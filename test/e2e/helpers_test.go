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
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
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

// etcdClusterRef builds a minimal EtcdCluster reference in the e2e test
// namespace, for wait helpers that only need the cluster's name and size.
func etcdClusterRef(name string, size int) *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: size},
	}
}

// waitForAllEtcdMemberReady waits until the cluster converges to the size
// recorded in ec.Spec.Size, checking every layer of readiness:
//   - EtcdMember objects: exactly that many (Terminating members still
//     running their leave sequence don't count), all Phase Ready, and no
//     ordinal at or above the expected size. Both directions are polled,
//     never hard-failed: scale-in shrinks one member at a time, so higher
//     ordinals legitimately outlive the size change until their leave
//     sequence finishes.
//   - Pods: the same number of them, all Ready.
//   - etcd membership itself, via the etcd client: that many members with no
//     learner left unpromoted.
func waitForAllEtcdMemberReady(t *testing.T, c *envconf.Config, ec *ecv1alpha1.EtcdCluster) error {
	t.Helper()
	expectedMembers := ec.Spec.Size
	// Declared outside the polling closure so a failed Pod listing can still
	// dump logs of the Pods observed on the previous pass.
	var pods corev1.PodList
	return wait.For(func(ctx context.Context) (bool, error) {
		var memberList ecv1alpha1.EtcdMemberList
		if err := c.Client().Resources().List(ctx, &memberList); err != nil {
			t.Logf("failed to list EtcdMembers for %s: %v", ec.Name, err)
			return false, nil
		}

		members := map[int]ecv1alpha1.EtcdMember{}
		for i := range memberList.Items {
			member := memberList.Items[i]
			// Terminating members are leaving for good (scale-in runs its
			// leave sequence on them); they legitimately keep higher
			// ordinals around until cleanup completes and must not fail the
			// ordinal/size checks aimed at the shrinking cluster.
			if member.Namespace == ec.Namespace && member.Spec.ClusterName == ec.Name &&
				member.DeletionTimestamp == nil {
				members[member.Spec.Ordinal] = member
			}
		}

		// Both invariants below are checked as "keep polling", not hard
		// errors: scale-in shrinks the cluster one member at a time through
		// graceful leaves, so higher ordinals legitimately exist (live, then
		// Terminating, then gone) for a while above the target size.
		for ordinal := range members {
			if ordinal >= expectedMembers {
				t.Logf("cluster %s still scaling in: ordinal %d present for size %d", ec.Name, ordinal, expectedMembers)
				return false, nil
			}
			for previousOrdinal := range ordinal {
				previous, exists := members[previousOrdinal]
				if !exists || previous.Status.Phase != ecv1alpha1.EtcdMemberReady {
					return false, nil
				}
			}
		}

		if len(members) != expectedMembers {
			return false, nil
		}
		for ordinal := range expectedMembers {
			member, exists := members[ordinal]
			if !exists || member.Status.Phase != ecv1alpha1.EtcdMemberReady {
				return false, nil
			}
		}

		if err := c.Client().Resources().List(ctx, &pods, resources.WithLabelSelector("app="+ec.Name)); err != nil {
			t.Logf("failed to list Pods for %s: %v", ec.Name, err)
			for i := range pods.Items {
				dumpPodLogs(ctx, t, c, &pods.Items[i], 500)
			}
			return false, nil
		}
		readyPods := 0
		for i := range pods.Items {
			if podReady(&pods.Items[i]) {
				readyPods++
			}
		}
		if readyPods != expectedMembers {
			t.Logf("pods ready %d/%d for cluster %s", readyPods, expectedMembers, ec.Name)
			return false, nil
		}

		// Pods are Ready, so the etcd client endpoint exists: the live
		// membership must match the expected size with no learner left.
		live, err := getEtcdMemberList(t, c, ec.Namespace, ec.Name+"-0", ec.Name, ec.Spec.TLS != nil)
		if err != nil {
			t.Logf("failed to get etcd member list for %s: %v", ec.Name, err)
			return false, nil
		}
		if len(live.Members) != expectedMembers {
			return false, nil
		}
		for _, member := range live.Members {
			if member.IsLearner {
				return false, nil
			}
		}
		return true, nil
	}, wait.WithTimeout(5*time.Minute), wait.WithInterval(2*time.Second))
}

func podReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
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

// forceCleanupEtcdResources deletes any leftover EtcdCluster objects in
// all namespaces and waits for the controller to fully drain them before
// returning. This must happen while the controller-manager is still running:
// EtcdClusterReconciler.finalizeCluster deletes each owned EtcdMember and
// clears its memberCleanupFinalizer once the EtcdCluster starts Terminating,
// allowing Kubernetes GC to remove member-owned resources. Only the running
// controller releases those finalizers; if global teardown killed it first, a
// cluster left by a test would stay Terminating and hang namespace deletion.
//
// It retries for a while rather than doing a single pass: draining an
// EtcdCluster's members takes the controller a few reconcile passes, not
// one.
func forceCleanupEtcdResources(ctx context.Context, res *resources.Resources) {
	cleanupPass := func(ctx context.Context) (bool, error) {
		var clusters ecv1alpha1.EtcdClusterList
		if err := res.List(ctx, &clusters); err == nil {
			log.Printf("Found %d etcd clusters", len(clusters.Items))
			for i := range clusters.Items {
				_ = res.Delete(ctx, &clusters.Items[i])
			}
		}

		var remainingClusters ecv1alpha1.EtcdClusterList
		var remainingMembers ecv1alpha1.EtcdMemberList
		_ = res.List(ctx, &remainingClusters)
		_ = res.List(ctx, &remainingMembers)
		return len(remainingClusters.Items) == 0 && len(remainingMembers.Items) == 0, nil
	}

	err := wait.For(cleanupPass,
		wait.WithContext(ctx), wait.WithTimeout(90*time.Second), wait.WithInterval(5*time.Second))
	if err != nil {
		log.Printf("forceCleanupEtcdResources: leftover EtcdCluster/EtcdMember objects may remain: %v", err)
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

// getEtcdMemberList returns the etcdserverpb.MemberListResponse by calling
// etcdctl -w json inside the given Pod, using the cluster's client
// certificates when tlsEnabled.
func getEtcdMemberList(
	t *testing.T,
	c *envconf.Config,
	podNamespace, podName, clusterName string,
	tlsEnabled bool,
) (*etcdserverpb.MemberListResponse, error) {
	t.Helper()
	cmd := append(etcdctlCmd(podName, clusterName, podNamespace, tlsEnabled), "member", "list", "-w", "json")
	stdout, stderr, err := execInPod(t, c, podName, podNamespace, cmd)
	if err != nil {
		return nil, fmt.Errorf("etcd member list via %s: %w, stderr: %s", podName, err, stderr)
	}
	var memberList etcdserverpb.MemberListResponse
	if err := json.Unmarshal([]byte(stdout), &memberList); err != nil {
		return nil, fmt.Errorf("parsing etcd member list JSON: %w", err)
	}
	return &memberList, nil
}

// getEtcdMemberListPB is the fatal-on-error wrapper around getEtcdMemberList
// for non-TLS assertions that run after the target Pod is already Ready.
func getEtcdMemberListPB(t *testing.T, c *envconf.Config, podName string) *etcdserverpb.MemberListResponse {
	t.Helper()
	memberList, err := getEtcdMemberList(t, c, namespace, podName, "", false)
	if err != nil {
		t.Fatalf("Failed to get etcd member list: %v", err)
	}
	return memberList
}

// waitForNoLearners waits until the member list has the expected number of members
// and all members are voting (i.e., no learners remain).
func waitForNoLearners(t *testing.T, c *envconf.Config, podName string, expectedMembers int, waitFor time.Duration) {
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
	}, wait.WithTimeout(waitFor), wait.WithInterval(5*time.Second))
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
func getClusterEndpointHashKVs(t *testing.T, c *envconf.Config, podName string) []*etcdserverpb.HashKVResponse {
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
	out := make([]*etcdserverpb.HashKVResponse, 0, len(entries))
	// Using a regular for loop, rather than range to avoid copylocks static check error.
	for i := 0; i < len(entries); i++ {
		out = append(out, &entries[i].HashKV)
	}
	return out
}

const (
	etcdServerCertMountDir = "/etc/etcd-certs/server"

	etcdServerCertFile = etcdServerCertMountDir + "/tls.crt"
	etcdServerKeyFile  = etcdServerCertMountDir + "/tls.key"
	etcdServerCAFile   = etcdServerCertMountDir + "/ca.crt"
)

func etcdctlCmd(podName, etcdClusterName, namespace string, tlsEnabled bool) []string {
	args := []string{"etcdctl"}
	if tlsEnabled {
		endpoint := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:2379",
			podName, etcdClusterName, namespace)
		args = append(args,
			"--cacert="+etcdServerCAFile,
			"--cert="+etcdServerCertFile,
			"--key="+etcdServerKeyFile,
			"--endpoints="+endpoint,
		)
	}
	return args
}

func verifyDataOperations(t *testing.T, c *envconf.Config, etcdClusterName, key, val string, tlsEnabled bool) {
	podName := fmt.Sprintf("%s-0", etcdClusterName)

	// Write key-value data
	cmd := etcdctlCmd(podName, etcdClusterName, namespace, tlsEnabled)
	command := append(cmd, "put", key, val)
	_, stderr, err := execInPod(t, c, podName, namespace, command)
	if err != nil {
		t.Fatalf("Failed to write data: %v, stderr: %s", err, stderr)
	}

	// Read key-value data
	command = append(cmd, "get", key)
	stdout, stderr, err := execInPod(t, c, podName, namespace, command)
	if err != nil {
		t.Fatalf("Failed to read data: %v, stderr: %s", err, stderr)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) < 2 || lines[0] != key || lines[1] != val {
		t.Errorf("Expected key-value pair [%s=%s], but got output: %s", key, val, stdout)
	}
}

const (
	gofailPort    = "22381"                            // from config/e2e/patch-env.yaml
	labelSelector = "control-plane=controller-manager" // from config/manager/manager.yaml,
)

// enableGoFailPoint enables the specified gofail failpoint on the specified pod via HTTP
func enableGoFailPoint(t *testing.T, cfg *envconf.Config, pod corev1.Pod, failpoint, term string) error {
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	r := client.CoreV1().RESTClient().Put()
	return httpViaProxy(t.Context(), r, pod, failpoint, term)
}

// disableGoFailPoint disables the specified gofail failpoint on the specified pod via HTTP
func disableGoFailPoint(t *testing.T, cfg *envconf.Config, pod corev1.Pod, failpoint string) error {
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	r := client.CoreV1().RESTClient().Delete()
	return httpViaProxy(t.Context(), r, pod, failpoint, "")
}

func getEtcdOperatorPod(t *testing.T, client klient.Client) (corev1.Pod, error) {
	var pods corev1.PodList
	err := client.Resources(namespace).List(t.Context(), &pods,
		resources.WithLabelSelector(labelSelector))
	if err != nil {
		return corev1.Pod{}, err
	}
	return pods.Items[0], nil
}

func httpViaProxy(ctx context.Context, r *rest.Request, pod corev1.Pod, failpoint, term string) error {
	result := r.Namespace(pod.Namespace).
		Resource("pods").
		SubResource("proxy").
		Name(fmt.Sprintf("%s:%s", pod.Name, gofailPort)).
		Suffix(failpoint).
		Body(strings.NewReader(term)).
		Do(ctx)
	return result.Error()
}

// dumpPodLogs streams the last `tailLine` lines of `pod`'s log via the apiserver and pipes them into t.Logf
func dumpPodLogs(ctx context.Context, t *testing.T, c *envconf.Config, pod *corev1.Pod, tailLine int64) {
	t.Helper()
	if pod == nil {
		return
	}
	client := kubernetes.NewForConfigOrDie(c.Client().RESTConfig())
	req := client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{TailLines: &tailLine})
	stream, streamErr := req.Stream(ctx)
	if streamErr != nil {
		t.Logf("failed to stream log for pod %s/%s: %s", pod.Namespace, pod.Name, streamErr)
		return
	}
	body, _ := io.ReadAll(stream)
	_ = stream.Close()
	t.Logf("pod %s/%s log tail:\n%s", pod.Namespace, pod.Name, string(body))
}
