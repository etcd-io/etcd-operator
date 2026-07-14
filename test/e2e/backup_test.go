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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// Pinned MinIO/mc images. Using :latest makes the e2e's pass/fail depend on
// whatever MinIO publishes that day (flag/alias semantics have churned
// historically); a gated "optional" e2e would rot silently. Pin to known-good
// RELEASE tags so the test is reproducible.
const (
	minioImage = "quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z"
	mcImage    = "quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z"
)

// backupSeedKeyCount is the number of keys seeded into the source cluster. A
// single key is the weakest possible proof that a snapshot round-trips; seeding
// a whole keyspace lets the restore side assert the *entire* dataset survives
// (data-loss is the worst failure mode of a backup/restore feature).
const backupSeedKeyCount = 50

// TestEtcdBackupToS3 exercises the EtcdBackup snapshot+upload flow end-to-end
// against an in-cluster MinIO (S3-compatible) bucket, so it requires NO real
// cloud credentials. It is gated behind ETCD_E2E_BACKUP=true because it
// provisions extra workloads (MinIO) and is not part of the default e2e matrix.
func TestEtcdBackupToS3(t *testing.T) {
	requireBackupE2E(t)
	runFullBackupCycle(t,
		newMinIOBackend("minio-backup-e2e", "etcd-backups", "backup-e2e-creds"),
		"etcd-backup-s3", "etcd-backup-e2e", "etcd-backup-e2e-snap")
}

// TestEtcdBackupToGCS exercises the identical snapshot+upload flow against an
// in-cluster fake-gcs-server (GCS JSON-API emulator), via the new
// GCSDestinationSpec.Endpoint + the unauthenticated WithEndpoint path in
// newGCSStore. It requires NO real Google credentials and runs the SAME
// present-and-valid assertion stack as the S3 cycle, so the operator's
// second advertised provider is exercised live rather than only with fakes.
func TestEtcdBackupToGCS(t *testing.T) {
	requireBackupE2E(t)
	runFullBackupCycle(t,
		newFakeGCSBackend("fake-gcs-backup-e2e", "etcd-backups"),
		"etcd-backup-gcs", "etcd-backup-gcs-e2e", "etcd-backup-gcs-e2e-snap")
}

// requireBackupE2E skips unless the object-storage backup e2e is explicitly
// enabled; these tests provision extra in-cluster workloads (MinIO / fake-gcs)
// and are not part of the default matrix.
func requireBackupE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("ETCD_E2E_BACKUP") != "true" {
		t.Skip("set ETCD_E2E_BACKUP=true to run the object-storage backup e2e (provisions a backend)")
	}
}

// runFullBackupCycle drives one provider through the complete backup proof:
//
//  1. Stand up the object-storage backend + bucket (+ creds when authenticated).
//  2. Create a single-member EtcdCluster, wait for it ready, and seed a rich,
//     content-addressable keyspace (50 keys whose values are hash(key)) plus
//     edge values (a ~100 KiB large value, a multibyte UTF-8 value) and a
//     pre-backup provenance sentinel.
//  3. Create an EtcdBackup at the backend and wait for phase Completed.
//  4. Assert the controller status is coherent (size>0, location, completion).
//  5. Independently of the controller, stat the object directly on the backend
//     and assert its size equals status.SnapshotSizeBytes and clears the 16 KiB
//     real-snapshot floor.
//  6. Strongest, controller-independent validity proof: download the object's
//     bytes and run `etcdutl snapshot status` on them, asserting it is an
//     intact etcd backend snapshot whose totalKey equals the seeded key count.
//     A controller that set status without uploading, or uploaded a truncated
//     blob, or uploaded a snapshot missing the seeded data, fails one of (5)/(6).
func runFullBackupCycle(t *testing.T, backend backupBackend, featureName, clusterName, backupName string) {
	feature := features.New(featureName)

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.deploy(ctx, t, cfg)
		backend.bootstrapBucket(ctx, t, cfg)
		createBackupTestCluster(ctx, t, cfg, clusterName, 1)
		waitStatefulSetReady(t, cfg, clusterName, 1)
		seedRichKeyspace(t, cfg, clusterName+"-0")

		backup := &ecv1alpha1.EtcdBackup{
			ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
			Spec: ecv1alpha1.EtcdBackupSpec{
				ClusterRef:  clusterName,
				Destination: backend.destination("e2e"),
			},
		}
		if err := cfg.Client().Resources().Create(ctx, backup); err != nil {
			t.Fatalf("create EtcdBackup: %v", err)
		}
		return ctx
	})

	feature.Assess("backup reaches Completed and the object is a valid snapshot containing the seeded keyspace", func(
		ctx context.Context, t *testing.T, cfg *envconf.Config,
	) context.Context {
		done := waitBackupCompleted(t, cfg, backupName)

		// (1) The controller's self-reported status must be coherent.
		if done.Status.SnapshotSizeBytes <= 0 {
			t.Errorf("expected positive snapshot size, got %d", done.Status.SnapshotSizeBytes)
		}
		if done.Status.SnapshotLocation == "" {
			t.Errorf("expected snapshot location to be set")
		}
		if done.Status.CompletionTime == nil {
			t.Errorf("expected completion time to be set")
		}

		// (2) Independently verify the object is actually in object storage with
		//     the recorded size. A controller that set the status fields without
		//     uploading a byte would pass (1) but fail here.
		objectKey := objectKeyFromLocation(t, done.Status.SnapshotLocation)
		size := backend.statObjectSize(t, cfg, objectKey)
		if size != done.Status.SnapshotSizeBytes {
			t.Errorf("backend object size %d != status.SnapshotSizeBytes %d (key %q)",
				size, done.Status.SnapshotSizeBytes, objectKey)
		}

		// (3) Sanity floor: a real etcd backend snapshot is a bbolt DB and is
		//     never a handful of bytes.
		if size < 16*1024 {
			t.Errorf("snapshot object only %d bytes; too small to be a real etcd backend snapshot", size)
		}

		// (4) Strongest check: download the bytes and prove via `etcdutl
		//     snapshot status` that it is an intact etcd snapshot whose key count
		//     equals the seeded keyspace — controller status is never consulted.
		assertSnapshotValid(t, cfg, backend, objectKey, int64(expectedSeedKeyCount()))

		t.Logf("backup completed and verified: location=%s size=%d key=%s",
			done.Status.SnapshotLocation, done.Status.SnapshotSizeBytes, objectKey)
		return ctx
	})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		backend.cleanup(ctx, t, cfg)
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace}})
		_ = cfg.Client().Resources().Delete(ctx, &ecv1alpha1.EtcdBackup{
			ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace}})
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}

// --- locally-scoped helpers (intentionally self-contained) ------------------

func backupExecInPod(t *testing.T, cfg *envconf.Config, podName string, command []string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	var pod corev1.Pod
	if err := cfg.Client().Resources().Get(t.Context(), podName, namespace, &pod); err != nil {
		return "", "", err
	}
	containerName := pod.Spec.Containers[0].Name
	err := cfg.Client().Resources().ExecInPod(t.Context(), namespace, podName, containerName, command, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// seedKeyspace writes backupSeedKeyCount deterministic key/value pairs into the
// member via direct etcdctl exec (the etcd image ships etcdctl but no shell, so
// each put is a separate argv exec). Values are content-addressable so the
// restore side can assert exact round-trip, not mere presence.
func seedKeyspace(t *testing.T, cfg *envconf.Config, podName string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		k, v := seedKV(i)
		if _, stderr, err := backupExecInPod(t, cfg, podName,
			[]string{"etcdctl", "put", k, v}); err != nil {
			t.Fatalf("seed key %s: %v (stderr: %s)", k, err, stderr)
		}
	}
}

// seedKV is the single source of truth for the seeded keyspace so backup and
// restore agree on exactly which keys/values must exist.
func seedKV(i int) (string, string) {
	return fmt.Sprintf("restore-e2e/k-%03d", i), fmt.Sprintf("val-%03d-payload", i)
}

func createBackupTestCluster(ctx context.Context, t *testing.T, cfg *envconf.Config, name string, size int) {
	t.Helper()
	cluster := &ecv1alpha1.EtcdCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "operator.etcd.io/v1alpha1", Kind: "EtcdCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: size, Version: "v3.6.1"},
	}
	if err := cfg.Client().Resources().Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster %s: %v", name, err)
	}
}

func waitStatefulSetReady(t *testing.T, cfg *envconf.Config, name string, replicas int32) {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var sts appsv1.StatefulSet
		if err := cfg.Client().Resources().Get(ctx, name, namespace, &sts); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return sts.Status.ReadyReplicas == replicas, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(3*time.Second))
	if err != nil {
		t.Fatalf("statefulset %s not ready: %v", name, err)
	}
}

func createBackupCredsSecret(
	ctx context.Context, t *testing.T, cfg *envconf.Config, name, accessKey, secretKey string,
) {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data: map[string][]byte{
			"accessKeyID":     []byte(accessKey),
			"secretAccessKey": []byte(secretKey),
		},
	}
	if err := cfg.Client().Resources().Create(ctx, secret); err != nil {
		t.Fatalf("create creds secret: %v", err)
	}
}

func deployMinIO(ctx context.Context, t *testing.T, cfg *envconf.Config, name, accessKey, secretKey string) {
	t.Helper()
	labels := map[string]string{"app": name}
	replicas := int32(1)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "minio",
						Image: minioImage,
						Args:  []string{"server", "/data", "--address", ":9000"},
						Env: []corev1.EnvVar{
							{Name: "MINIO_ROOT_USER", Value: accessKey},
							{Name: "MINIO_ROOT_PASSWORD", Value: secretKey},
						},
						Ports: []corev1.ContainerPort{{ContainerPort: 9000}},
					}},
				},
			},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, deploy); err != nil {
		t.Fatalf("create minio deployment: %v", err)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports:    []corev1.ServicePort{{Port: 9000, TargetPort: intstr.FromInt(9000)}},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, svc); err != nil {
		t.Fatalf("create minio service: %v", err)
	}

	// Wait for Available, but fail fast (with logs) if the pod is wedged in a
	// crash/pull-back-off rather than spinning until the deadline with no clue.
	if err := wait.For(conditions.New(cfg.Client().Resources()).
		DeploymentConditionMatch(deploy, appsv1.DeploymentAvailable, corev1.ConditionTrue),
		wait.WithTimeout(2*time.Minute), wait.WithInterval(3*time.Second)); err != nil {
		dumpPodTrouble(ctx, t, cfg, labels)
		t.Fatalf("minio %q not available: %v", name, err)
	}
}

func createBucketPod(
	ctx context.Context, t *testing.T, cfg *envconf.Config, minioName, bucket, accessKey, secretKey string,
) {
	t.Helper()
	// Name the bootstrap pod per-MinIO-instance so two backup/restore features in
	// the shared namespace do not collide on a singleton "mc-mkbucket".
	podName := "mc-mkbucket-" + minioName
	script := "mc alias set m http://" + minioName + ":9000 " + accessKey + " " + secretKey +
		" && mc mb --ignore-existing m/" + bucket
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyOnFailure,
			Containers: []corev1.Container{{
				Name:    "mc",
				Image:   mcImage,
				Command: []string{"sh", "-c", script},
			}},
		},
	}
	_ = cfg.Client().Resources().Delete(ctx, pod) // tolerate a leftover from a killed run
	if err := waitPodGone(ctx, cfg, podName); err != nil {
		t.Fatalf("waiting for stale %s to clear: %v", podName, err)
	}
	if err := cfg.Client().Resources().Create(ctx, pod); err != nil {
		t.Fatalf("create bucket-create pod: %v", err)
	}
	// Succeed OR fail loudly: a Failed pod (bad alias, MinIO down) must surface
	// its logs immediately, not time out opaquely after two minutes.
	if err := waitPodSucceeded(ctx, t, cfg, podName); err != nil {
		t.Fatalf("bucket-create pod did not succeed: %v", err)
	}
}

// --- object-storage verification (mc one-shot pods) -------------------------

// objectKeyFromLocation extracts the bucket-relative object key from a snapshot
// location URI like "s3://etcd-backups/e2e/etcd-backup-e2e/...snap.db". The key
// is everything after the bucket segment.
func objectKeyFromLocation(t *testing.T, location string) string {
	t.Helper()
	rest := location
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	// rest is now "<bucket>/<key...>"; drop the first path segment (the bucket).
	if i := strings.Index(rest, "/"); i >= 0 {
		return rest[i+1:]
	}
	t.Fatalf("cannot parse object key from snapshot location %q", location)
	return ""
}

// statMinIOObjectSize runs a one-shot mc pod to `mc stat` the object and returns
// its size in bytes, failing the test if the object is absent. This is the
// independent "is it really in the bucket?" check the status fields cannot give.
func statMinIOObjectSize(
	t *testing.T, cfg *envconf.Config, minioName, bucket, key, accessKey, secretKey string,
) int64 {
	t.Helper()
	// `mc stat --json` emits a JSON line with a "size" field. Parse just that.
	script := "mc alias set m http://" + minioName + ":9000 " + accessKey + " " + secretKey +
		" >/dev/null && mc stat --json m/" + bucket + "/" + key
	out := runMCJob(t, cfg, "mc-stat-"+minioName, script)
	size, ok := jsonInt64Field(out, "size")
	if !ok {
		t.Fatalf("could not read object size for m/%s/%s; mc output: %s", bucket, key, out)
	}
	return size
}

// jsonInt64Field extracts an integer field from mc's --json output without a
// full JSON parser dependency: finds `"<field>":<number>`.
func jsonInt64Field(jsonOut, field string) (int64, bool) {
	needle := "\"" + field + "\":"
	i := strings.Index(jsonOut, needle)
	if i < 0 {
		return 0, false
	}
	rest := jsonOut[i+len(needle):]
	j := 0
	// Skip leading spaces and an optional opening quote: mc emits a bare number
	// ("size":22) while the GCS JSON API quotes it ("size":"22"). Both must
	// parse with the same extractor.
	for j < len(rest) && (rest[j] == ' ' || rest[j] == '"') {
		j++
	}
	start := j
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	if start == j {
		return 0, false
	}
	n, err := strconv.ParseInt(rest[start:j], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// runMCJob runs a one-shot mc pod with the given shell script and returns its
// stdout (the pod logs), failing the test — with the pod's logs — if it does
// not succeed.
func runMCJob(t *testing.T, cfg *envconf.Config, podBase, script string) string {
	t.Helper()
	ctx := t.Context()
	podName := podBase + "-" + strconv.FormatInt(time.Now().UnixNano()%100000, 10)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "mc",
				Image:   mcImage,
				Command: []string{"sh", "-c", script},
			}},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, pod); err != nil {
		t.Fatalf("create mc job pod %s: %v", podName, err)
	}
	defer func() { _ = cfg.Client().Resources().Delete(ctx, pod) }()
	if err := waitPodSucceeded(ctx, t, cfg, podName); err != nil {
		t.Fatalf("mc job %s did not succeed: %v", podName, err)
	}
	logs, err := podLogs(ctx, cfg, podName)
	if err != nil {
		t.Fatalf("read mc job logs %s: %v", podName, err)
	}
	return logs
}

// podLogs fetches a pod's first-container logs via the clientset (the e2e
// framework's resource client does not expose a logs subresource).
func podLogs(ctx context.Context, cfg *envconf.Config, podName string) (string, error) {
	cs := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	rc, err := cs.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, rc); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// --- pod-wait helpers that catch FAILURE, not just success -----------------

func waitPodSucceeded(ctx context.Context, t *testing.T, cfg *envconf.Config, podName string) error {
	t.Helper()
	return wait.For(func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		if err := cfg.Client().Resources().Get(ctx, podName, namespace, &p); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		switch p.Status.Phase {
		case corev1.PodSucceeded:
			return true, nil
		case corev1.PodFailed:
			logs, _ := podLogs(ctx, cfg, podName)
			return false, fmt.Errorf("pod %s entered Failed phase; logs:\n%s", podName, logs)
		}
		// Surface image-pull / crash-loop wedges instead of waiting blind.
		if reason := badContainerReason(&p); reason != "" {
			logs, _ := podLogs(ctx, cfg, podName)
			return false, fmt.Errorf("pod %s stuck: %s; logs:\n%s", podName, reason, logs)
		}
		return false, nil
	}, wait.WithTimeout(2*time.Minute), wait.WithInterval(2*time.Second))
}

// badContainerReason returns a non-empty reason if any container is wedged in a
// known-fatal waiting/terminated state (CrashLoopBackOff, ImagePullBackOff,
// ErrImagePull, etc.), so waiters fail fast with a diagnosis.
func badContainerReason(p *corev1.Pod) string {
	for _, cs := range p.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil {
			switch w.Reason {
			case "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerError",
				"CreateContainerConfigError", "InvalidImageName":
				return cs.Name + ": " + w.Reason + " (" + w.Message + ")"
			}
		}
	}
	return ""
}

func waitPodGone(ctx context.Context, cfg *envconf.Config, podName string) error {
	return wait.For(func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		err := cfg.Client().Resources().Get(ctx, podName, namespace, &p)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, nil
	}, wait.WithTimeout(1*time.Minute), wait.WithInterval(2*time.Second))
}

func dumpPodTrouble(ctx context.Context, t *testing.T, cfg *envconf.Config, labels map[string]string) {
	t.Helper()
	var pods corev1.PodList
	if err := cfg.Client().Resources().List(ctx, &pods); err != nil {
		return
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		match := true
		for k, v := range labels {
			if p.Labels[k] != v {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		if reason := badContainerReason(p); reason != "" || p.Status.Phase == corev1.PodPending {
			logs, _ := podLogs(ctx, cfg, p.Name)
			t.Logf("pod %s phase=%s reason=%q logs:\n%s", p.Name, p.Status.Phase, reason, logs)
		}
	}
}

// --- shared backup status wait + cleanup ------------------------------------

// waitBackupCompleted blocks until the named EtcdBackup reaches Completed and
// returns it, failing the test on a Failed phase or timeout. It never calls
// t.Fatalf from inside the poll closure (that is racey from a poller goroutine);
// it returns an error and fails the test from the calling goroutine instead.
func waitBackupCompleted(t *testing.T, cfg *envconf.Config, name string) ecv1alpha1.EtcdBackup {
	t.Helper()
	err := wait.For(func(ctx context.Context) (bool, error) {
		var got ecv1alpha1.EtcdBackup
		if err := cfg.Client().Resources().Get(ctx, name, namespace, &got); err != nil {
			return false, err
		}
		switch got.Status.Phase {
		case ecv1alpha1.BackupPhaseCompleted:
			return true, nil
		case ecv1alpha1.BackupPhaseFailed:
			return false, fmt.Errorf("backup %q failed: %+v", name, got.Status.Conditions)
		}
		return false, nil
	}, wait.WithTimeout(3*time.Minute), wait.WithInterval(2*time.Second))
	if err != nil {
		t.Fatalf("waiting for backup %q completion: %v", name, err)
	}
	var done ecv1alpha1.EtcdBackup
	if err := cfg.Client().Resources().Get(t.Context(), name, namespace, &done); err != nil {
		t.Fatalf("get completed backup %q: %v", name, err)
	}
	return done
}

// cleanupBackupWorkloads best-effort deletes the MinIO deployment/service, the
// bucket-bootstrap pod, the creds secret, and the named EtcdClusters so a
// re-run (or a sibling feature in the shared namespace) starts clean.
func cleanupBackupWorkloads(
	ctx context.Context, t *testing.T, cfg *envconf.Config, minioName, credsSecret string, clusters []string,
) {
	t.Helper()
	res := cfg.Client().Resources()
	_ = res.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: minioName, Namespace: namespace}})
	_ = res.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: minioName, Namespace: namespace}})
	_ = res.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mc-mkbucket-" + minioName, Namespace: namespace}})
	_ = res.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credsSecret, Namespace: namespace}})
	for _, c := range clusters {
		_ = res.Delete(ctx, &ecv1alpha1.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: c, Namespace: namespace}})
	}
}
