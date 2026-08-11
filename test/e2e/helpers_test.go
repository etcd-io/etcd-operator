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
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	rest "k8s.io/client-go/rest"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
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

func waitForPodReadiness(t *testing.T, c *envconf.Config, name string, expectedReplicas int) error {
	t.Helper()
	label := fmt.Sprintf("app=%s", name)
	var pods corev1.PodList
	return wait.For(func(ctx context.Context) (bool, error) {
		if err := c.Client().Resources().List(ctx, &pods, resources.WithLabelSelector(label)); err != nil {
			t.Logf("failed to list pods by label %s, %s", label, err)
			for _, pod := range pods.Items {
				dumpPodLogs(context.TODO(), t, c, &pod, 500)
			}
			return false, nil
		}

		var readyCnt = 0
		var unreadyPods []string
		for _, pod := range pods.Items {
			if !podReady(&pod) {
				unreadyPods = append(unreadyPods, pod.Name)
			} else {
				readyCnt++
			}
		}
		if readyCnt != expectedReplicas {
			t.Logf("found pods(%d/%d/%d) by label(%s). unready pods: %s",
				readyCnt, len(pods.Items), expectedReplicas, label, unreadyPods)
			return false, nil
		}

		return true, nil
	}, wait.WithTimeout(5*time.Minute), wait.WithInterval(10*time.Second))
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

const (
	etcdServerCertMountDir = "/etc/etcd-certs/server"

	etcdServerCertFile = etcdServerCertMountDir + "/tls.crt"
	etcdServerKeyFile  = etcdServerCertMountDir + "/tls.key"
	etcdServerCAFile   = etcdServerCertMountDir + "/ca.crt"
)

// memberDNSSuffix mirrors internal/controller.memberDNSSuffix: the trailing
// segment appended to "<pod>.<sts>.<ns>." when building an etcd member FQDN.
// Kept in sync by hand because the e2e package cannot import the unexported
// helper. When dnsDomain is empty (the operator's default), the suffix is
// "svc"; otherwise it is "svc.<dnsDomain>".
func memberDNSSuffix(dnsDomain string) string {
	if dnsDomain == "" {
		return "svc"
	}
	return "svc." + dnsDomain
}

func etcdctlCmd(podName, etcdClusterName, namespace, dnsDomain string, tlsEnabled bool) []string {
	args := []string{"etcdctl"}
	if tlsEnabled {
		endpoint := fmt.Sprintf("https://%s.%s.%s.%s:2379",
			podName, etcdClusterName, namespace, memberDNSSuffix(dnsDomain))
		args = append(args,
			"--cacert="+etcdServerCAFile,
			"--cert="+etcdServerCertFile,
			"--key="+etcdServerKeyFile,
			"--endpoints="+endpoint,
		)
	}
	return args
}

func verifyDataOperations(t *testing.T, c *envconf.Config,
	etcdClusterName, key, val, dnsDomain string, tlsEnabled bool) {
	podName := fmt.Sprintf("%s-0", etcdClusterName)

	// Write key-value data
	cmd := etcdctlCmd(podName, etcdClusterName, namespace, dnsDomain, tlsEnabled)
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

// operatorServiceDNSDomain returns the current value of the --service-dns-domain
// flag from the etcd-operator-controller-manager Deployment. Each call performs
// a fresh API Get so the result reflects any in-flight override — for example
// a test that toggles the flag in its Setup and restores it in TearDown.
// Returns "" when the flag is absent (the operator's default), which the
// controller mirrors as the short `svc` FQDN suffix.
func operatorServiceDNSDomain(t *testing.T, ctx context.Context, cfg *envconf.Config) string {
	t.Helper()
	var deployment appsv1.Deployment
	if err := cfg.Client().Resources().Get(ctx, managerDeploymentName, namespace, &deployment); err != nil {
		t.Fatalf("failed to get %s/%s deployment: %v", namespace, managerDeploymentName, err)
	}
	for _, arg := range deployment.Spec.Template.Spec.Containers[0].Args {
		if value, ok := strings.CutPrefix(arg, "--service-dns-domain="); ok {
			return value
		}
	}
	return ""
}

// setManagerServiceDNSDomain patches the manager Deployment to set
// --service-dns-domain to dnsDomain for the rest of the test, and returns
// a closure that restores manager Deployment.
func setManagerServiceDNSDomain(t *testing.T, ctx context.Context, cfg *envconf.Config, dnsDomain string) func() {
	t.Helper()
	var deployment appsv1.Deployment
	if err := cfg.Client().Resources().Get(ctx, managerDeploymentName, namespace, &deployment); err != nil {
		t.Fatalf("failed to get %s/%s deployment: %v", namespace, managerDeploymentName, err)
	}
	container := &deployment.Spec.Template.Spec.Containers[0]
	originalArgs := append([]string(nil), container.Args...)
	container.Args = setFlagValue(originalArgs, "--service-dns-domain", dnsDomain)
	if err := cfg.Client().Resources().Update(ctx, &deployment); err != nil {
		t.Fatalf("failed to update %s/%s deployment: %v", namespace, managerDeploymentName, err)
	}
	// Args change triggers a Deployment rollout and pod restart. Wait for
	// the new pod to be Ready before the caller runs assertions against it.
	if err := wait.For(
		conditions.New(cfg.Client().Resources()).DeploymentAvailable(managerDeploymentName, namespace),
		wait.WithTimeout(3*time.Minute),
		wait.WithInterval(5*time.Second),
	); err != nil {
		t.Fatalf("%s/%s deployment did not become available after flag change: %v",
			namespace, managerDeploymentName, err)
	}
	return func() {
		var d appsv1.Deployment
		if err := cfg.Client().Resources().Get(ctx, managerDeploymentName, namespace, &d); err != nil {
			t.Errorf("failed to get %s/%s deployment on teardown: %v",
				namespace, managerDeploymentName, err)
			return
		}
		d.Spec.Template.Spec.Containers[0].Args = originalArgs
		if err := cfg.Client().Resources().Update(ctx, &d); err != nil {
			t.Errorf("failed to restore %s/%s deployment args on teardown: %v",
				namespace, managerDeploymentName, err)
		}
		// Wait for the restored pod so the next test starts against a stable
		// operator (otherwise it may race the rollout in its own Setup).
		if err := wait.For(
			conditions.New(cfg.Client().Resources()).DeploymentAvailable(managerDeploymentName, namespace),
			wait.WithTimeout(3*time.Minute),
			wait.WithInterval(5*time.Second),
		); err != nil {
			t.Errorf("%s/%s deployment did not become available after restore: %v",
				namespace, managerDeploymentName, err)
		}
	}
}

// setFlagValue returns a copy of args where the value for flag (matched as
// "--flag=value") is replaced. If value is empty the matching arg is dropped,
// and if the flag is absent it is appended when value is non-empty. The input
// slice is not mutated.
func setFlagValue(args []string, flag, value string) []string {
	prefix := flag + "="
	out := make([]string, 0, len(args))
	found := false
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			found = true
			if value != "" {
				out = append(out, prefix+value)
			}
			continue
		}
		out = append(out, a)
	}
	if !found && value != "" {
		out = append(out, prefix+value)
	}
	return out
}

const (
	gofailPort            = "22381"                            // from config/e2e/patch-env.yaml
	labelSelector         = "control-plane=controller-manager" // from config/manager/manager.yaml,
	managerDeploymentName = "etcd-operator-controller-manager"
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
	req := client.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{TailLines: &tailLine})
	stream, streamErr := req.Stream(ctx)
	if streamErr != nil {
		t.Logf("failed to stream log for pod %s/%s: %s", pod.Namespace, pod.Name, streamErr)
		return
	}
	body, _ := io.ReadAll(stream)
	_ = stream.Close()
	t.Logf("pod %s/%s log tail:\n%s", pod.Namespace, pod.Name, string(body))
}
