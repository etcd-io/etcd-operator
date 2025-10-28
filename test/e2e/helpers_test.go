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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/k8s/watcher"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/status"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
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

// StatusRecorder observes EtcdCluster status transitions and appends JSON lines to writer.
type StatusRecorder struct {
	cancel   context.CancelFunc
	watcher  *watcher.EventHandlerFuncs
	done     chan struct{}
	writer   io.Writer
	mu       sync.Mutex
	lastHash string
}

type statusRecorderHandle struct {
	recorder *StatusRecorder
	writer   io.WriteCloser
	once     sync.Once
}

func (h *statusRecorderHandle) stop(t *testing.T) {
	if h == nil {
		return
	}
	h.once.Do(func() {
		if h.recorder != nil {
			h.recorder.Stop()
		}
		if h.writer != nil {
			if err := h.writer.Close(); err != nil {
				t.Logf("failed to close status recorder writer: %v", err)
			}
		}
	})
}

// StartStatusRecorder begins watching the EtcdCluster identified by nn.
// Whenever the status (including conditions) changes, a snapshot is written as JSON.
func StartStatusRecorder(
	t *testing.T,
	cfg *envconf.Config,
	nn types.NamespacedName,
	writer io.Writer,
) (*StatusRecorder, error) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	res := cfg.Client().Resources().WithNamespace(nn.Namespace)

	w := res.Watch(
		&ecv1alpha1.EtcdClusterList{},
		resources.WithFieldSelector(fmt.Sprintf("metadata.name=%s", nn.Name)),
	)

	recorder := &StatusRecorder{
		cancel:  cancel,
		watcher: w,
		done:    make(chan struct{}),
		writer:  writer,
	}

	handle := func(event string, obj interface{}) {
		cluster, ok := obj.(*ecv1alpha1.EtcdCluster)
		if !ok || cluster == nil {
			return
		}
		recorder.recordSnapshot(t, event, cluster)
	}

	w.WithAddFunc(func(obj interface{}) { handle("ADDED", obj) }).
		WithUpdateFunc(func(obj interface{}) { handle("MODIFIED", obj) }).
		WithDeleteFunc(func(obj interface{}) { handle("DELETED", obj) })

	if err := w.Start(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to start status recorder watch: %w", err)
	}

	go func() {
		<-ctx.Done()
		close(recorder.done)
	}()

	return recorder, nil
}

// Stop stops the recorder and waits for the watcher to terminate.
func (r *StatusRecorder) Stop() {
	if r == nil {
		return
	}

	r.cancel()
	if r.watcher != nil {
		r.watcher.Stop()
	}
	<-r.done
}

func (r *StatusRecorder) recordSnapshot(t *testing.T, event string, cluster *ecv1alpha1.EtcdCluster) {
	statusCopy := ecv1alpha1.EtcdClusterStatus{}
	cluster.Status.DeepCopyInto(&statusCopy)

	statusBytes, err := json.Marshal(statusCopy)
	if err != nil {
		t.Logf("failed to marshal EtcdCluster status for recorder: %v", err)
		return
	}
	hashBytes := sha256.Sum256(statusBytes)
	hash := fmt.Sprintf("%d|%d|%x", cluster.Generation, statusCopy.ObservedGeneration, hashBytes)

	payload := struct {
		Timestamp          string                       `json:"timestamp"`
		Event              string                       `json:"event"`
		Generation         int64                        `json:"generation"`
		ObservedGeneration int64                        `json:"observedGeneration"`
		ResourceVersion    string                       `json:"resourceVersion"`
		Status             ecv1alpha1.EtcdClusterStatus `json:"status"`
	}{
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
		Event:              event,
		Generation:         cluster.Generation,
		ObservedGeneration: statusCopy.ObservedGeneration,
		ResourceVersion:    cluster.ResourceVersion,
		Status:             statusCopy,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Logf("failed to marshal recorder payload: %v", err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if hash == r.lastHash {
		return
	}
	if _, err := r.writer.Write(data); err != nil {
		t.Logf("failed to write status recorder payload: %v", err)
		return
	}
	if _, err := r.writer.Write([]byte("\n")); err != nil {
		t.Logf("failed to finalize status recorder payload: %v", err)
		return
	}
	r.lastHash = hash
}

func createStatusHistoryWriter(t *testing.T, testName, clusterName string) io.WriteCloser {
	t.Helper()

	base := resolveStatusArtifactsBase()

	dir := filepath.Join(base, sanitizeFileComponent(testName))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("failed to create status artifact directory %s: %v", dir, err)
	}

	filePath := filepath.Join(dir, fmt.Sprintf("%s-status.jsonl", sanitizeFileComponent(clusterName)))
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed to create status artifact file %s: %v", filePath, err)
	}

	t.Logf("recording EtcdCluster status to %s", filePath)
	return file
}

func sanitizeFileComponent(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unnamed"
	}
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	return b.String()
}

func resolveStatusArtifactsBase() string {
	if explicit := os.Getenv("ETCD_OPERATOR_E2E_ARTIFACTS_DIR"); explicit != "" {
		return explicit
	}

	if prowArtifacts := os.Getenv("ARTIFACTS"); prowArtifacts != "" {
		return filepath.Join(prowArtifacts, "etcdcluster-status")
	}

	return filepath.Join(".", "artifacts", "status")
}

type statusRecorderContextKey struct{}

var statusRecorderKey statusRecorderContextKey

func enableStatusRecording(
	ctx context.Context,
	t *testing.T,
	c *envconf.Config,
	testName string,
	clusterName string,
) context.Context {
	t.Helper()

	if ctx == nil {
		ctx = context.Background()
	}

	if existing, ok := ctx.Value(statusRecorderKey).(*statusRecorderHandle); ok && existing != nil {
		t.Logf("status recording already enabled for test %s", testName)
		return ctx
	}

	writer := createStatusHistoryWriter(t, testName, clusterName)
	recorder, err := StartStatusRecorder(
		t,
		c,
		types.NamespacedName{
			Name:      clusterName,
			Namespace: namespace,
		},
		writer,
	)
	if err != nil {
		if closeErr := writer.Close(); closeErr != nil {
			t.Logf("failed to close status recorder writer after start error: %v", closeErr)
		}
		t.Fatalf("failed to start status recorder: %v", err)
	}

	handle := &statusRecorderHandle{
		recorder: recorder,
		writer:   writer,
	}
	return context.WithValue(ctx, statusRecorderKey, handle)
}

func stopStatusRecording(ctx context.Context, t *testing.T) {
	t.Helper()

	if ctx == nil {
		return
	}

	if handle, ok := ctx.Value(statusRecorderKey).(*statusRecorderHandle); ok && handle != nil {
		handle.stop(t)
	}
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

func verifyDataOperations(t *testing.T, c *envconf.Config, etcdClusterName string) {
	testKey := "test-key"
	testValue := "test-value"
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

func waitForClusterHealthyStatus(
	t *testing.T,
	c *envconf.Config,
	name string,
	expectedMembers int,
) {
	t.Helper()
	var cluster ecv1alpha1.EtcdCluster
	err := wait.For(func(ctx context.Context) (bool, error) {
		if err := c.Client().Resources().Get(ctx, name, namespace, &cluster); err != nil {
			return false, err
		}

		if cluster.Status.ObservedGeneration < cluster.Generation {
			return false, nil
		}

		if cluster.Status.CurrentReplicas != int32(expectedMembers) ||
			cluster.Status.ReadyReplicas != int32(expectedMembers) ||
			cluster.Status.MemberCount != int32(expectedMembers) ||
			len(cluster.Status.Members) != expectedMembers {
			return false, nil
		}

		leaderCount := 0
		for _, member := range cluster.Status.Members {
			if !member.IsHealthy || member.IsLearner {
				return false, nil
			}
			if member.IsLeader {
				leaderCount++
			}
		}
		if leaderCount != 1 {
			return false, nil
		}

		if !conditionEquals(
			cluster.Status.Conditions,
			status.ConditionAvailable,
			metav1.ConditionTrue,
			status.ReasonClusterReady,
		) {
			return false, nil
		}
		if !conditionEquals(
			cluster.Status.Conditions,
			status.ConditionProgressing,
			metav1.ConditionFalse,
			status.ReasonReconcileSuccess,
		) {
			return false, nil
		}
		if !conditionEquals(
			cluster.Status.Conditions,
			status.ConditionDegraded,
			metav1.ConditionFalse,
			status.ReasonClusterHealthy,
		) {
			return false, nil
		}

		return true, nil
	}, wait.WithTimeout(5*time.Minute), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Fatalf("EtcdCluster %s did not reach healthy status for %d members: %v", name, expectedMembers, err)
	}
}

func conditionEquals(
	conditions []metav1.Condition,
	condType string,
	condStatus metav1.ConditionStatus,
	condReason string,
) bool {
	cond := meta.FindStatusCondition(conditions, condType)
	if cond == nil {
		return false
	}
	return cond.Status == condStatus && cond.Reason == condReason
}
