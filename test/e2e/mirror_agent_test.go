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
	"context"
	"encoding/json"
	"fmt"
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
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/pkg/features"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

const (
	mirrorNamespace  = "mirror-e2e"
	mirrorSourceName = "mirror-source"
	mirrorTargetName = "mirror-target"
	mirrorAgentName  = "mirror-agent"
	mirrorAgentPort  = 8080
	mirrorKeyPrefix  = "/mirror-e2e/"
	mirrorKeyCount   = 10
	// replicationWait bounds each replication poll loop. Replication itself
	// is sub-second; the ceiling absorbs API-server exec round-trip latency
	// on loaded CI, and the logged elapsed time carries the fast-path proof.
	replicationWait = 10 * time.Second
	replicationPoll = 250 * time.Millisecond
)

// createMirrorEtcdCluster creates a size-1 cleartext EtcdCluster in
// mirrorNamespace.
func createMirrorEtcdCluster(ctx context.Context, t *testing.T, cfg *envconf.Config, name string) {
	t.Helper()
	ec := &ecv1alpha1.EtcdCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "operator.etcd.io/v1alpha1", Kind: "EtcdCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mirrorNamespace},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: etcdVersion},
	}
	if err := cfg.Client().Resources().Create(ctx, ec); err != nil {
		t.Fatalf("failed to create EtcdCluster %s/%s: %v", mirrorNamespace, name, err)
	}
}

// waitForMirrorSTSReady waits for the cluster's StatefulSet in
// mirrorNamespace to report one ready replica.
func waitForMirrorSTSReady(ctx context.Context, t *testing.T, cfg *envconf.Config, name string) {
	t.Helper()
	client := cfg.Client()
	sts := appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mirrorNamespace}}
	// Not utils.GetKubernetesResource: its poll aborts on the first NotFound
	// instead of waiting out the timeout. ResourceMatch swallows Get errors.
	if err := wait.For(
		conditions.New(client.Resources()).ResourceMatch(&sts, func(k8s.Object) bool { return true }),
		wait.WithContext(ctx),
		wait.WithTimeout(3*time.Minute),
		wait.WithInterval(5*time.Second),
	); err != nil {
		t.Fatalf("StatefulSet %s never appeared: %v", name, err)
	}
	if err := wait.For(
		conditions.New(client.Resources()).ResourceScaled(&sts, func(k8s.Object) int32 {
			return sts.Status.ReadyReplicas
		}, 1),
		wait.WithContext(ctx),
		wait.WithTimeout(3*time.Minute),
		wait.WithInterval(5*time.Second),
	); err != nil {
		t.Fatalf("StatefulSet %s never reached 1 ready replica: %v", name, err)
	}
}

// execInPodRetried retries transient exec failures (the SPDY stream setup is
// a known flake mode under CI load). Only for idempotent commands.
func execInPodRetried(t *testing.T, cfg *envconf.Config, podName string, command []string) (string, error) {
	t.Helper()
	var stderr string
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(250 * time.Millisecond)
		}
		if _, stderr, err = execInPod(t, cfg, podName, mirrorNamespace, command); err == nil {
			return "", nil
		}
	}
	return stderr, err
}

// deleteMirrorResources best-effort deletes everything the feature creates.
// Shared by the feature Teardown and a Setup t.Cleanup: a t.Fatalf in Setup
// runtime.Goexits past the feature teardowns, and the leaked namespace would
// wedge every ETCD_E2E_SKIP_TEARDOWN rerun on AlreadyExists.
func deleteMirrorResources(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	t.Helper()
	client := cfg.Client()
	var pod corev1.Pod
	if err := client.Resources().Get(ctx, mirrorAgentName, mirrorNamespace, &pod); err == nil {
		if err := client.Resources().Delete(ctx, &pod); err != nil {
			t.Logf("failed to delete mirror-agent pod: %v", err)
		}
	}
	for _, name := range []string{mirrorSourceName, mirrorTargetName} {
		var ec ecv1alpha1.EtcdCluster
		if err := client.Resources().Get(ctx, name, mirrorNamespace, &ec); err == nil {
			if err := client.Resources().Delete(ctx, &ec); err != nil {
				t.Logf("failed to delete EtcdCluster %s: %v", name, err)
			}
		}
	}
	var ns corev1.Namespace
	if err := client.Resources().Get(ctx, mirrorNamespace, "", &ns); err == nil {
		if err := client.Resources().Delete(ctx, &ns); err != nil {
			t.Logf("failed to delete namespace %s: %v", mirrorNamespace, err)
		}
	}
}

// etcdClientEndpoint is the per-pod DNS client URL the operator itself
// advertises for member 0 of a size-1 cluster.
func etcdClientEndpoint(name string) string {
	return fmt.Sprintf("http://%s-0.%s.%s.svc.cluster.local:2379", name, name, mirrorNamespace)
}

// mirrorAgentPod renders the agent pod wired source→target, cleartext, with
// the observability listener exposed on mirrorAgentPort.
func mirrorAgentPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mirrorAgentName,
			Namespace: mirrorNamespace,
			Labels:    map[string]string{"app": mirrorAgentName},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, // a startup failure must surface, not crash-loop
			Containers: []corev1.Container{{
				Name:            mirrorAgentName,
				Image:           imageName,
				ImagePullPolicy: corev1.PullIfNotPresent, // kind-loaded, never pulled
				// The image ENTRYPOINT is /manager; the same image ships the agent.
				Command: []string{"/mirror-agent"},
				Args: []string{
					"--link-uid=mirror-e2e-link",
					"--epoch=1",
					"--source-endpoints=" + etcdClientEndpoint(mirrorSourceName),
					"--target-endpoints=" + etcdClientEndpoint(mirrorTargetName),
					"--source-prefix=" + mirrorKeyPrefix,
					"--target-prefix=" + mirrorKeyPrefix,
					"--http-bind-address=:8080",
				},
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: mirrorAgentPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/readyz", Port: intstr.FromInt32(mirrorAgentPort),
					}},
					PeriodSeconds:    2,
					FailureThreshold: 60,
				},
			}},
		},
	}
}

// getViaPodProxy GETs a path on the agent pod's HTTP port through the
// API-server pod proxy (the pod image is distroless — nothing can be exec'd).
func getViaPodProxy(ctx context.Context, cfg *envconf.Config, path string) ([]byte, error) {
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	return client.CoreV1().RESTClient().Get().
		Namespace(mirrorNamespace).
		Resource("pods").
		SubResource("proxy").
		Name(fmt.Sprintf("%s:%d", mirrorAgentName, mirrorAgentPort)).
		Suffix(path).
		Do(ctx).Raw()
}

// getStatusz decodes /statusz into mirroragent.Snapshot — the JSON tags on
// that type are the wire contract, so decoding into it cannot drift.
func getStatusz(ctx context.Context, cfg *envconf.Config) (mirroragent.Snapshot, error) {
	raw, err := getViaPodProxy(ctx, cfg, "statusz")
	if err != nil {
		return mirroragent.Snapshot{}, err
	}
	var s mirroragent.Snapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		return mirroragent.Snapshot{}, fmt.Errorf("decoding /statusz %q: %w", raw, err)
	}
	return s, nil
}

// scrapeKeysAppliedMetric returns the unlabeled
// etcd_mirror_agent_keys_applied_total sample from /metrics. Parsed by hand
// to avoid promoting prometheus/common to a direct dependency.
func scrapeKeysAppliedMetric(ctx context.Context, t *testing.T, cfg *envconf.Config) float64 {
	t.Helper()
	raw, err := getViaPodProxy(ctx, cfg, "metrics")
	if err != nil {
		t.Fatalf("failed to scrape /metrics: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "etcd_mirror_agent_keys_applied_total" {
			v, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				t.Fatalf("unparsable keys_applied_total sample %q: %v", line, err)
			}
			return v
		}
	}
	t.Fatalf("etcd_mirror_agent_keys_applied_total absent from /metrics:\n%s", raw)
	return 0
}

// dumpAgentDiagnostics logs the raw /statusz body and the agent pod logs so
// a failed poll is debuggable from CI output alone.
func dumpAgentDiagnostics(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	t.Helper()
	if raw, err := getViaPodProxy(ctx, cfg, "statusz"); err != nil {
		t.Logf("statusz unavailable: %v", err)
	} else {
		t.Logf("last /statusz: %s", raw)
	}
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	logs, err := client.CoreV1().Pods(mirrorNamespace).
		GetLogs(mirrorAgentName, &corev1.PodLogOptions{}).Do(ctx).Raw()
	if err != nil {
		t.Logf("agent pod logs unavailable: %v", err)
		return
	}
	t.Logf("mirror-agent pod logs:\n%s", logs)
}

// hasAllMirrorKeys reports whether plain etcdctl get output (alternating
// key and value lines) contains every expected key with its expected value.
func hasAllMirrorKeys(stdout string) bool {
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	got := make(map[string]string, len(lines)/2)
	for i := 0; i+1 < len(lines); i += 2 {
		got[lines[i]] = lines[i+1]
	}
	for i := range mirrorKeyCount {
		if got[fmt.Sprintf("%skey-%02d", mirrorKeyPrefix, i)] != fmt.Sprintf("val-%02d", i) {
			return false
		}
	}
	return true
}

// TestMirrorAgent smoke-tests the mirror-agent binary: two size-1 etcd
// clusters, an agent pod wired source→target, put/delete replication within
// a polled ceiling, and advancing /statusz and /metrics counters.
func TestMirrorAgent(t *testing.T) {
	feature := features.New("mirror-agent")

	var baseline mirroragent.Snapshot
	var writeTime time.Time

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		client := cfg.Client()

		// A prior failed or just-finished run can leave the namespace behind
		// (or still Terminating); purge it so reruns self-heal.
		var leftover corev1.Namespace
		if err := client.Resources().Get(ctx, mirrorNamespace, "", &leftover); err == nil {
			t.Logf("deleting leftover namespace %s from a previous run", mirrorNamespace)
			_ = client.Resources().Delete(ctx, &leftover)
			if err := wait.For(func(ctx context.Context) (bool, error) {
				var ns corev1.Namespace
				return apierrors.IsNotFound(client.Resources().Get(ctx, mirrorNamespace, "", &ns)), nil
			}, wait.WithTimeout(2*time.Minute), wait.WithInterval(2*time.Second)); err != nil {
				t.Fatalf("leftover namespace %s never finished deleting: %v", mirrorNamespace, err)
			}
		}

		ctx, err := envfuncs.CreateNamespace(mirrorNamespace)(ctx, cfg)
		if err != nil {
			t.Fatalf("failed to create namespace %s: %v", mirrorNamespace, err)
		}
		// Unlike the feature Teardown, t.Cleanup still runs after a Setup
		// t.Fatalf; deleteMirrorResources is idempotent, so double cleanup
		// on the happy path is harmless.
		t.Cleanup(func() {
			deleteMirrorResources(context.Background(), t, cfg)
		})

		createMirrorEtcdCluster(ctx, t, cfg, mirrorSourceName)
		createMirrorEtcdCluster(ctx, t, cfg, mirrorTargetName)
		waitForMirrorSTSReady(ctx, t, cfg, mirrorSourceName)
		waitForMirrorSTSReady(ctx, t, cfg, mirrorTargetName)

		// Both sides are up, so the agent never lingers in Connecting.
		pod := mirrorAgentPod()
		if err := client.Resources().Create(ctx, pod); err != nil {
			t.Fatalf("failed to create mirror-agent pod: %v", err)
		}
		// Not conditions.PodReady: that spins the full timeout on a pod that
		// already crashed (RestartPolicy=Never) and aborts on transient Get
		// errors. Fail fast on a terminal phase, swallow Get blips.
		if err := wait.For(func(ctx context.Context) (bool, error) {
			var p corev1.Pod
			if gerr := client.Resources().Get(ctx, mirrorAgentName, mirrorNamespace, &p); gerr != nil {
				return false, nil //nolint:nilerr // transient API error; keep polling
			}
			if p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
				return false, fmt.Errorf("pod is terminal (%s)", p.Status.Phase)
			}
			for _, cond := range p.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return true, nil
				}
			}
			return false, nil
		}, wait.WithTimeout(3*time.Minute), wait.WithInterval(2*time.Second)); err != nil {
			dumpAgentDiagnostics(ctx, t, cfg)
			t.Fatalf("mirror-agent pod never became ready: %v", err)
		}
		return ctx
	})

	feature.Assess("agent reaches Syncing (baseline)",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// readyz turns ready at InitialSync already, so gate on the
			// statusz phase rather than pod readiness alone.
			if err := wait.For(func(ctx context.Context) (bool, error) {
				s, err := getStatusz(ctx, cfg)
				if err != nil {
					return false, nil //nolint:nilerr // proxy may 503 while the pod settles; keep polling
				}
				baseline = s
				return s.Phase == mirroragent.PhaseSyncing, nil
			}, wait.WithTimeout(2*time.Minute), wait.WithInterval(time.Second)); err != nil {
				dumpAgentDiagnostics(ctx, t, cfg)
				t.Fatalf("agent never reached phase %s (last snapshot: %+v): %v",
					mirroragent.PhaseSyncing, baseline, err)
			}
			if baseline.SourceClusterID == baseline.TargetClusterID {
				t.Fatalf("source and target cluster IDs both %d: agent is not wired to two distinct clusters",
					baseline.SourceClusterID)
			}
			return ctx
		})

	feature.Assess("writes replicate source to target",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			sourcePod := mirrorSourceName + "-0"
			for i := range mirrorKeyCount {
				key := fmt.Sprintf("%skey-%02d", mirrorKeyPrefix, i)
				val := fmt.Sprintf("val-%02d", i)
				if stderr, err := execInPodRetried(t, cfg, sourcePod,
					[]string{"etcdctl", "put", key, val}); err != nil {
					t.Fatalf("failed to put %s on source: %v, stderr: %s", key, err, stderr)
				}
			}
			writeTime = time.Now()

			// Range over key- specifically: a bare prefix get would also
			// return the agent's reserved checkpoint key.
			targetPod := mirrorTargetName + "-0"
			if err := wait.For(func(context.Context) (bool, error) {
				stdout, _, err := execInPod(t, cfg, targetPod, mirrorNamespace,
					[]string{"etcdctl", "get", "--prefix", mirrorKeyPrefix + "key-"})
				if err != nil {
					return false, nil //nolint:nilerr // transient exec failure; keep polling
				}
				return hasAllMirrorKeys(stdout), nil
			}, wait.WithTimeout(replicationWait), wait.WithInterval(replicationPoll)); err != nil {
				dumpAgentDiagnostics(ctx, t, cfg)
				t.Fatalf("target never showed all %d keys within %s: %v", mirrorKeyCount, replicationWait, err)
			}
			t.Logf("all %d keys visible on target after %s", mirrorKeyCount, time.Since(writeTime))
			return ctx
		})

	feature.Assess("delete replicates",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			key := mirrorKeyPrefix + "key-00"
			if stderr, err := execInPodRetried(t, cfg, mirrorSourceName+"-0",
				[]string{"etcdctl", "del", key}); err != nil {
				t.Fatalf("failed to delete %s on source: %v, stderr: %s", key, err, stderr)
			}
			delTime := time.Now()

			if err := wait.For(func(context.Context) (bool, error) {
				stdout, _, err := execInPod(t, cfg, mirrorTargetName+"-0", mirrorNamespace,
					[]string{"etcdctl", "get", key, "--print-value-only"})
				if err != nil {
					return false, nil //nolint:nilerr // transient exec failure; keep polling
				}
				return strings.TrimSpace(stdout) == "", nil
			}, wait.WithTimeout(replicationWait), wait.WithInterval(replicationPoll)); err != nil {
				dumpAgentDiagnostics(ctx, t, cfg)
				t.Fatalf("delete of %s never reached the target within %s: %v", key, replicationWait, err)
			}
			t.Logf("delete visible on target after %s", time.Since(delTime))
			return ctx
		})

	feature.Assess("statusz and metrics advanced",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// Poll rather than one-shot: the snapshot counters are not
			// updated atomically with the txn our read-back observed.
			var last mirroragent.Snapshot
			if err := wait.For(func(ctx context.Context) (bool, error) {
				s, err := getStatusz(ctx, cfg)
				if err != nil {
					return false, nil //nolint:nilerr // transient proxy failure; keep polling
				}
				last = s
				return s.KeysAppliedTotal >= baseline.KeysAppliedTotal+mirrorKeyCount+1 &&
					s.Watermark > baseline.Watermark &&
					!s.LastProgressTime.IsZero() &&
					s.LastProgressTime.After(baseline.LastProgressTime) &&
					s.Phase == mirroragent.PhaseSyncing, nil
			}, wait.WithTimeout(replicationWait), wait.WithInterval(replicationPoll)); err != nil {
				dumpAgentDiagnostics(ctx, t, cfg)
				t.Fatalf("statusz never advanced past baseline\nbaseline: %+v\nlast: %+v\nerror: %v",
					baseline, last, err)
			}
			// Transient errors are normal operation (LastError only clears
			// on the next successful commit); only a permanent one is a bug.
			if last.LastErrorClass == mirroragent.ClassPermanent {
				t.Errorf("agent reports permanent error %q", last.LastError)
			}
			// Scraped after statusz and monotonic, so it can never be less.
			if v := scrapeKeysAppliedMetric(ctx, t, cfg); v < float64(last.KeysAppliedTotal) {
				t.Errorf("metrics keys_applied_total %v below statusz keysAppliedTotal %d", v, last.KeysAppliedTotal)
			}
			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		deleteMirrorResources(ctx, t, cfg)
		return ctx
	})

	_ = testEnv.Test(t, feature.Feature())
}
