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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// Pinned images for the GCS emulator and the snapshot-inspection tooling. As
// with the MinIO/mc pins, fixed tags keep the gated e2e reproducible rather
// than tracking whatever the registry publishes that day.
const (
	// fakeGCSImage is the fsouza fake-gcs-server emulator. It speaks the GCS
	// JSON API, so the operator's GCS provider (pointed at it via the new
	// GCSDestinationSpec.Endpoint) exercises the real upload/list/download code
	// path with no Google credentials.
	fakeGCSImage = "fsouza/fake-gcs-server:1.52.2"
	// etcdToolsImage carries `etcdutl`, used to independently validate that an
	// uploaded object is a real, intact etcd backend snapshot (not merely a
	// blob of the right size). It is the same distroless etcd image the
	// operator deploys, so `etcdutl` is always present.
	etcdToolsImage = "gcr.io/etcd-development/etcd:v3.6.1"
	// curlImage is a tiny shell+curl image used to fetch a GCS object's bytes
	// (and metadata) directly from the emulator, independent of the operator.
	curlImage = "curlimages/curl:8.11.1"
)

// richSeedKeyCount is the number of content-addressable keys seeded into the
// source cluster on top of the edge-case keys. Each value is a pure function of
// its key (hex(sha256(key))) so any corruption of the snapshot's bbolt pages is
// a guaranteed mismatch rather than a maybe.
const richSeedKeyCount = 50

// Edge-case and provenance keys. These are seeded alongside the content-
// addressable keyspace to exercise value shapes a naive round-trip mishandles,
// and to give the (gated) restore test point-in-time provenance controls.
const (
	edgeLargeKey = "cyc/edge/large" // ~100 KiB value: forces multi-page bbolt
	edgeUTF8Key  = "cyc/edge/utf8"  // multibyte UTF-8 value
	sentinelPre  = "cyc/sentinel/before"
	sentinelPost = "cyc/sentinel/after"
	// largeValueBytes is kept well under the kernel's per-arg execve limit
	// (MAX_ARG_STRLEN, ~128 KiB) because the value is passed to `etcdctl put`
	// as an argv argument; 100 KiB still spans multiple bbolt pages and sits
	// far above the 16 KiB snapshot-size floor the presence check applies.
	largeValueBytes = 100 * 1024
)

// contentAddressableValue is the single source of truth mapping a seeded key to
// its value, shared by the seed and (gated) restore-verify paths so both agree
// on exactly what must be present.
func contentAddressableValue(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func richSeedKey(i int) string { return fmt.Sprintf("cyc/k-%04d", i) }

// expectedSeedKeyCount is the total number of distinct keys the seed writes
// (content-addressable keys + the large + utf8 edges + the pre-backup
// sentinel). The post-backup sentinel is written AFTER the snapshot, so it is
// intentionally excluded from the in-snapshot count.
func expectedSeedKeyCount() int {
	return richSeedKeyCount + 3 // large, utf8, sentinelPre
}

// seedRichKeyspace writes the full pre-backup keyspace into the member via
// per-key `etcdctl put` execs (the distroless etcd image ships etcdctl but no
// shell, so each put is its own argv exec). It returns nothing; the count that
// must appear in the snapshot is expectedSeedKeyCount().
func seedRichKeyspace(t *testing.T, cfg *envconf.Config, podName string) {
	t.Helper()
	put := func(k, v string) {
		if _, stderr, err := backupExecInPod(t, cfg, podName, []string{"etcdctl", "put", k, v}); err != nil {
			t.Fatalf("seed key %q: %v (stderr: %s)", k, err, stderr)
		}
	}
	for i := 0; i < richSeedKeyCount; i++ {
		k := richSeedKey(i)
		put(k, contentAddressableValue(k))
	}
	put(edgeLargeKey, strings.Repeat("L", largeValueBytes))
	put(edgeUTF8Key, "héllo·wörld·🔬·αβγ")
	// sentinelPre is written BEFORE the backup and must survive a restore.
	put(sentinelPre, contentAddressableValue(sentinelPre))
}

// --- provider abstraction --------------------------------------------------

// backupBackend abstracts an in-cluster object-storage backend (MinIO for S3,
// fake-gcs-server for GCS) so the full backup cycle can be driven once and run
// against both providers. Each method is independent so two providers can run
// side by side in the shared namespace without colliding.
type backupBackend interface {
	// name is the backend's Deployment/Service name (also its in-cluster DNS).
	name() string
	// deploy stands up the backend Deployment+Service and waits for Available,
	// failing fast (with logs) on a crash/pull back-off.
	deploy(ctx context.Context, t *testing.T, cfg *envconf.Config)
	// bootstrapBucket creates the destination bucket via a one-shot pod.
	bootstrapBucket(ctx context.Context, t *testing.T, cfg *envconf.Config)
	// destination builds the EtcdBackup destination targeting this backend.
	destination(prefix string) ecv1alpha1.BackupDestination
	// statObjectSize returns the stored object's size in bytes, fetched
	// directly from the backend (not via the operator), failing if absent.
	statObjectSize(t *testing.T, cfg *envconf.Config, key string) int64
	// downloadInitContainer returns an init container that fetches the object at
	// key into /snap/s.db on a shared emptyDir, for the etcdutl validity check.
	downloadInitContainer(key string) corev1.Container
	// credsSecretName is the creds Secret the destination references, or "" when
	// the backend is unauthenticated (GCS emulator).
	credsSecretName() string
	// cleanup best-effort removes the backend Deployment/Service.
	cleanup(ctx context.Context, t *testing.T, cfg *envconf.Config)
}

// --- S3 / MinIO backend -----------------------------------------------------

type minioBackend struct {
	instance  string
	bucket    string
	accessKey string
	secretKey string
	creds     string
}

func newMinIOBackend(instance, bucket, credsSecret string) *minioBackend {
	return &minioBackend{
		instance: instance, bucket: bucket,
		accessKey: "minioadmin", secretKey: "minioadmin", creds: credsSecret,
	}
}

func (m *minioBackend) name() string            { return m.instance }
func (m *minioBackend) credsSecretName() string { return m.creds }

func (m *minioBackend) deploy(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	deployMinIO(ctx, t, cfg, m.instance, m.accessKey, m.secretKey)
}

func (m *minioBackend) bootstrapBucket(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	createBucketPod(ctx, t, cfg, m.instance, m.bucket, m.accessKey, m.secretKey)
	createBackupCredsSecret(ctx, t, cfg, m.creds, m.accessKey, m.secretKey)
}

func (m *minioBackend) destination(prefix string) ecv1alpha1.BackupDestination {
	return ecv1alpha1.BackupDestination{
		Provider:  ecv1alpha1.BackupProviderS3,
		Prefix:    prefix,
		SecretRef: &corev1.LocalObjectReference{Name: m.creds},
		S3: &ecv1alpha1.S3DestinationSpec{
			Bucket:         m.bucket,
			Region:         "us-east-1",
			Endpoint:       "http://" + m.instance + "." + namespace + ".svc.cluster.local:9000",
			ForcePathStyle: true,
		},
	}
}

func (m *minioBackend) statObjectSize(t *testing.T, cfg *envconf.Config, key string) int64 {
	return statMinIOObjectSize(t, cfg, m.instance, m.bucket, key, m.accessKey, m.secretKey)
}

func (m *minioBackend) downloadInitContainer(key string) corev1.Container {
	script := "mc alias set m http://" + m.instance + ":9000 " + m.accessKey + " " + m.secretKey +
		" && mc cp m/" + m.bucket + "/" + key + " /snap/s.db"
	return corev1.Container{
		Name:         "fetch",
		Image:        mcImage,
		Command:      []string{"sh", "-c", script},
		VolumeMounts: []corev1.VolumeMount{{Name: "snap", MountPath: "/snap"}},
	}
}

func (m *minioBackend) cleanup(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	cleanupBackupWorkloads(ctx, t, cfg, m.instance, m.creds, nil)
}

// --- GCS / fake-gcs-server backend -----------------------------------------

type fakeGCSBackend struct {
	instance string
	bucket   string
}

func newFakeGCSBackend(instance, bucket string) *fakeGCSBackend {
	return &fakeGCSBackend{instance: instance, bucket: bucket}
}

func (g *fakeGCSBackend) name() string            { return g.instance }
func (g *fakeGCSBackend) credsSecretName() string { return "" } // unauthenticated emulator

// gcsBaseURL is the in-cluster URL of the emulator, used both for the operator
// endpoint and for the independent stat/download checks.
func (g *fakeGCSBackend) gcsBaseURL() string {
	return "http://" + g.instance + "." + namespace + ".svc.cluster.local:9000"
}

func (g *fakeGCSBackend) deploy(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	t.Helper()
	labels := map[string]string{"app": g.instance}
	replicas := int32(1)
	publicHost := g.instance + "." + namespace + ".svc.cluster.local:9000"
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: g.instance, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "fake-gcs",
						Image: fakeGCSImage,
						// -scheme http: the operator dials plain HTTP. -public-host
						// / -external-url are set to the in-cluster Service DNS so
						// the emulator's host/signed-URL rewrites match how the
						// operator (and the stat/download pods) reach it.
						Args: []string{
							"-scheme", "http",
							"-host", "0.0.0.0",
							"-port", "9000",
							"-public-host", publicHost,
							"-external-url", "http://" + publicHost,
						},
						Ports: []corev1.ContainerPort{{ContainerPort: 9000}},
					}},
				},
			},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, deploy); err != nil {
		t.Fatalf("create fake-gcs deployment: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: g.instance, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports:    []corev1.ServicePort{{Port: 9000, TargetPort: intstr.FromInt(9000)}},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, svc); err != nil {
		t.Fatalf("create fake-gcs service: %v", err)
	}
	if err := wait.For(conditions.New(cfg.Client().Resources()).
		DeploymentConditionMatch(deploy, appsv1.DeploymentAvailable, corev1.ConditionTrue),
		wait.WithTimeout(2*time.Minute), wait.WithInterval(3*time.Second)); err != nil {
		dumpPodTrouble(ctx, t, cfg, labels)
		t.Fatalf("fake-gcs %q not available: %v", g.instance, err)
	}
}

func (g *fakeGCSBackend) bootstrapBucket(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	t.Helper()
	// fake-gcs-server creates buckets via POST /storage/v1/b?project=… . Run it
	// in a one-shot curl pod, named per-instance so two GCS features in the
	// shared namespace do not collide.
	podName := "gcs-mkbucket-" + g.instance
	body := `{"name":"` + g.bucket + `"}`
	script := "curl -sf -X POST '" + g.gcsBaseURL() + "/storage/v1/b?project=e2e' " +
		"-H 'Content-Type: application/json' -d '" + body + "'"
	g.runCurlPod(ctx, t, cfg, podName, script)
}

func (g *fakeGCSBackend) destination(prefix string) ecv1alpha1.BackupDestination {
	return ecv1alpha1.BackupDestination{
		Provider: ecv1alpha1.BackupProviderGCS,
		Prefix:   prefix,
		// No SecretRef: the emulator is unauthenticated, exercising the new
		// WithEndpoint + WithoutAuthentication path in newGCSStore.
		GCS: &ecv1alpha1.GCSDestinationSpec{
			Bucket:   g.bucket,
			Endpoint: g.gcsBaseURL() + "/storage/v1/",
		},
	}
}

func (g *fakeGCSBackend) statObjectSize(t *testing.T, cfg *envconf.Config, key string) int64 {
	t.Helper()
	// GCS JSON API: GET /storage/v1/b/<bucket>/o/<url-encoded-key> returns
	// metadata with a "size" field (a *string* in the GCS API, unlike mc).
	enc := url.PathEscape(key)
	script := "curl -sf '" + g.gcsBaseURL() + "/storage/v1/b/" + g.bucket + "/o/" + enc + "'"
	out := g.runCurlPod(t.Context(), t, cfg, "gcs-stat-"+g.instance, script)
	size, ok := jsonInt64Field(out, "size")
	if !ok {
		t.Fatalf("could not read GCS object size for %s/%s; curl output: %s", g.bucket, key, out)
	}
	return size
}

func (g *fakeGCSBackend) downloadInitContainer(key string) corev1.Container {
	enc := url.PathEscape(key)
	url := g.gcsBaseURL() + "/storage/v1/b/" + g.bucket + "/o/" + enc + "?alt=media"
	return corev1.Container{
		Name:         "fetch",
		Image:        curlImage,
		Command:      []string{"sh", "-c", "curl -sf -o /snap/s.db '" + url + "'"},
		VolumeMounts: []corev1.VolumeMount{{Name: "snap", MountPath: "/snap"}},
	}
}

func (g *fakeGCSBackend) cleanup(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	res := cfg.Client().Resources()
	_ = res.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: g.instance, Namespace: namespace}})
	_ = res.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: g.instance, Namespace: namespace}})
	_ = res.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "gcs-mkbucket-" + g.instance, Namespace: namespace}})
}

// runCurlPod runs a one-shot curl pod with the given script and returns its
// stdout, failing — with the pod's logs — if it does not succeed.
func (g *fakeGCSBackend) runCurlPod(ctx context.Context, t *testing.T, cfg *envconf.Config, podBase, script string) string {
	t.Helper()
	podName := podBase + "-" + strconv.FormatInt(time.Now().UnixNano()%100000, 10)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "curl",
				Image:   curlImage,
				Command: []string{"sh", "-c", script},
			}},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, pod); err != nil {
		t.Fatalf("create curl pod %s: %v", podName, err)
	}
	defer func() { _ = cfg.Client().Resources().Delete(ctx, pod) }()
	if err := waitPodSucceeded(ctx, t, cfg, podName); err != nil {
		t.Fatalf("curl pod %s did not succeed: %v", podName, err)
	}
	logs, err := podLogs(ctx, cfg, podName)
	if err != nil {
		t.Fatalf("read curl pod logs %s: %v", podName, err)
	}
	return logs
}

// --- independent snapshot validity (etcdutl) --------------------------------

// etcdutlSnapshotStatus is the subset of `etcdutl snapshot status -w json`
// output the validity check asserts on.
type etcdutlSnapshotStatus struct {
	Hash     int64  `json:"hash"`
	Revision int64  `json:"revision"`
	TotalKey int64  `json:"totalKey"`
	Version  string `json:"version"`
}

// assertSnapshotValid downloads the uploaded object directly from the backend
// (via an init container) and runs `etcdutl snapshot status` on it in a main
// container, proving — entirely independently of the operator's status — that
// the object is a real, intact etcd backend snapshot AND that the seeded key
// count actually made it into the snapshot. A corrupt/truncated object makes
// etcdutl exit non-zero; a snapshot missing keys fails the count assertion.
func assertSnapshotValid(
	t *testing.T, cfg *envconf.Config, backend backupBackend, key string, wantKeys int64,
) {
	t.Helper()
	ctx := t.Context()
	podName := "snap-status-" + backend.name() + "-" + strconv.FormatInt(time.Now().UnixNano()%100000, 10)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy:  corev1.RestartPolicyNever,
			InitContainers: []corev1.Container{backend.downloadInitContainer(key)},
			Containers: []corev1.Container{{
				Name:         "etcdutl",
				Image:        etcdToolsImage,
				Command:      []string{"/usr/local/bin/etcdutl", "snapshot", "status", "/snap/s.db", "-w", "json"},
				VolumeMounts: []corev1.VolumeMount{{Name: "snap", MountPath: "/snap"}},
			}},
			Volumes: []corev1.Volume{{Name: "snap", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		},
	}
	if err := cfg.Client().Resources().Create(ctx, pod); err != nil {
		t.Fatalf("create snapshot-status pod %s: %v", podName, err)
	}
	defer func() { _ = cfg.Client().Resources().Delete(ctx, pod) }()
	if err := waitPodSucceeded(ctx, t, cfg, podName); err != nil {
		t.Fatalf("etcdutl snapshot status pod %s did not succeed (snapshot invalid/corrupt or fetch failed): %v", podName, err)
	}
	out, err := podLogs(ctx, cfg, podName)
	if err != nil {
		t.Fatalf("read etcdutl status logs %s: %v", podName, err)
	}
	status := parseEtcdutlStatus(t, out)
	if status.TotalKey != wantKeys {
		t.Errorf("snapshot totalKey=%d, want %d (the snapshot does not contain the seeded keyspace)", status.TotalKey, wantKeys)
	}
	if status.Revision <= 0 || status.Hash == 0 {
		t.Errorf("snapshot status looks degenerate: %+v", status)
	}
	t.Logf("etcdutl snapshot status OK: totalKey=%d revision=%d version=%s",
		status.TotalKey, status.Revision, status.Version)
}

// parseEtcdutlStatus extracts the status fields from etcdutl's -w json line
// without a struct-tag JSON dependency on the whole pod-log blob (the line may
// be preceded by warnings on stderr, but stdout carries only the JSON).
func parseEtcdutlStatus(t *testing.T, out string) etcdutlSnapshotStatus {
	t.Helper()
	var s etcdutlSnapshotStatus
	if v, ok := jsonInt64Field(out, "totalKey"); ok {
		s.TotalKey = v
	} else {
		t.Fatalf("etcdutl status: no totalKey in output: %s", out)
	}
	if v, ok := jsonInt64Field(out, "revision"); ok {
		s.Revision = v
	}
	if v, ok := jsonInt64Field(out, "hash"); ok {
		s.Hash = v
	}
	if i := strings.Index(out, "\"version\":\""); i >= 0 {
		rest := out[i+len("\"version\":\""):]
		if j := strings.IndexByte(rest, '"'); j >= 0 {
			s.Version = rest[:j]
		}
	}
	return s
}
