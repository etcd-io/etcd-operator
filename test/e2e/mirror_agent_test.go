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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/e2e-framework/klient/wait"
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
					"--source-endpoints=" + etcdClientEndpointNS(mirrorNamespace, mirrorSourceName),
					"--target-endpoints=" + etcdClientEndpointNS(mirrorNamespace, mirrorTargetName),
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

// scrapeKeysAppliedMetric returns the unlabeled
// etcd_mirror_agent_keys_applied_total sample from /metrics. Parsed by hand
// to avoid promoting prometheus/common to a direct dependency.
func scrapeKeysAppliedMetric(ctx context.Context, t *testing.T, cfg *envconf.Config) float64 {
	t.Helper()
	// Polled, not one-shot: the API-server proxy 503s transiently on a
	// just-settled pod.
	var raw []byte
	if err := wait.For(func(ctx context.Context) (bool, error) {
		b, gerr := getViaPodProxyNS(ctx, cfg, mirrorNamespace, mirrorAgentName, mirrorAgentPort, "metrics")
		if gerr != nil {
			return false, nil //nolint:nilerr // transient proxy failure; keep polling
		}
		raw = b
		return true, nil
	}, wait.WithContext(ctx), wait.WithTimeout(30*time.Second),
		wait.WithInterval(2*time.Second)); err != nil {
		t.Fatalf("failed to scrape /metrics within 30s: %v", err)
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

		createEtcdClusterInNS(ctx, t, cfg, mirrorNamespace, mirrorSourceName)
		createEtcdClusterInNS(ctx, t, cfg, mirrorNamespace, mirrorTargetName)
		waitForSTSReadyInNS(ctx, t, cfg, mirrorNamespace, mirrorSourceName)
		waitForSTSReadyInNS(ctx, t, cfg, mirrorNamespace, mirrorTargetName)

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
			dumpAgentDiagnosticsFor(ctx, t, cfg, mirrorNamespace, mirrorAgentName)
			t.Fatalf("mirror-agent pod never became ready: %v", err)
		}
		return ctx
	})

	feature.Assess("agent reaches Syncing (baseline)",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// readyz turns ready at InitialSync already, so gate on the
			// statusz phase rather than pod readiness alone.
			if err := wait.For(func(ctx context.Context) (bool, error) {
				s, err := getStatuszFor(ctx, cfg, mirrorNamespace, mirrorAgentName)
				if err != nil {
					return false, nil //nolint:nilerr // proxy may 503 while the pod settles; keep polling
				}
				baseline = s
				return s.Phase == mirroragent.PhaseSyncing, nil
			}, wait.WithTimeout(2*time.Minute), wait.WithInterval(time.Second)); err != nil {
				dumpAgentDiagnosticsFor(ctx, t, cfg, mirrorNamespace, mirrorAgentName)
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
			sourceRef := etcdPodRef{ns: mirrorNamespace, pod: mirrorSourceName + "-0"}
			putKeys(t, cfg, sourceRef, mirrorKeyPrefix+"key-", "val-", 0, mirrorKeyCount)
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
				dumpAgentDiagnosticsFor(ctx, t, cfg, mirrorNamespace, mirrorAgentName)
				t.Fatalf("target never showed all %d keys within %s: %v", mirrorKeyCount, replicationWait, err)
			}
			t.Logf("all %d keys visible on target after %s", mirrorKeyCount, time.Since(writeTime))
			return ctx
		})

	feature.Assess("delete replicates",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			key := mirrorKeyPrefix + "key-00"
			sourceRef := etcdPodRef{ns: mirrorNamespace, pod: mirrorSourceName + "-0"}
			if _, err := execEtcdctl(t, cfg, sourceRef, "del", key); err != nil {
				t.Fatalf("failed to delete %s on source: %v", key, err)
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
				dumpAgentDiagnosticsFor(ctx, t, cfg, mirrorNamespace, mirrorAgentName)
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
				s, err := getStatuszFor(ctx, cfg, mirrorNamespace, mirrorAgentName)
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
				dumpAgentDiagnosticsFor(ctx, t, cfg, mirrorNamespace, mirrorAgentName)
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
