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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

const (
	// managerMetricsPort is where config/e2e/patch-manager-flags.yaml rebinds
	// the manager's /metrics as plain HTTP for pod-proxy scraping.
	managerMetricsPort = 8080
	managerDeployment  = "etcd-operator-controller-manager"

	// skipCheckpointCleanupAnnotation mirrors internal/controller's escape
	// hatch: leftover EtcdMirrors from a failed run may reference an etcd
	// that is already gone, and their finalizer would wedge namespace
	// deletion forever without it.
	skipCheckpointCleanupAnnotation = "operator.etcd.io/skip-checkpoint-cleanup"

	mirrorPollInterval = 2 * time.Second
	mirrorGoneWait     = 90 * time.Second
	clusterReadyWait   = 3 * time.Minute
)

// etcdPodRef names one etcd pod plus the etcdctl connection flags it needs
// (empty for operator-managed cleartext clusters; TLS flags for the
// hand-rendered TLS source).
type etcdPodRef struct {
	ns, pod   string
	extraArgs []string
}

// createEtcdClusterInNS creates a size-1 cleartext EtcdCluster.
func createEtcdClusterInNS(ctx context.Context, t *testing.T, cfg *envconf.Config, ns, name string) {
	t.Helper()
	ec := &ecv1alpha1.EtcdCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "operator.etcd.io/v1alpha1", Kind: "EtcdCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 1, Version: etcdVersion},
	}
	if err := cfg.Client().Resources().Create(ctx, ec); err != nil {
		t.Fatalf("failed to create EtcdCluster %s/%s: %v", ns, name, err)
	}
}

// waitForSTSReadyInNS waits for the named StatefulSet to report one ready
// replica.
func waitForSTSReadyInNS(ctx context.Context, t *testing.T, cfg *envconf.Config, ns, name string) {
	t.Helper()
	client := cfg.Client()
	sts := appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	// Not utils.GetKubernetesResource: its poll aborts on the first NotFound
	// instead of waiting out the timeout. ResourceMatch swallows Get errors.
	if err := wait.For(
		conditions.New(client.Resources()).ResourceMatch(&sts, func(k8s.Object) bool { return true }),
		wait.WithContext(ctx),
		wait.WithTimeout(clusterReadyWait),
		wait.WithInterval(5*time.Second),
	); err != nil {
		t.Fatalf("StatefulSet %s/%s never appeared: %v", ns, name, err)
	}
	if err := wait.For(
		conditions.New(client.Resources()).ResourceScaled(&sts, func(k8s.Object) int32 {
			return sts.Status.ReadyReplicas
		}, 1),
		wait.WithContext(ctx),
		wait.WithTimeout(clusterReadyWait),
		wait.WithInterval(5*time.Second),
	); err != nil {
		t.Fatalf("StatefulSet %s/%s never reached 1 ready replica: %v", ns, name, err)
	}
}

// etcdClientEndpointNS is the per-pod DNS client URL the operator itself
// advertises for member 0 of a size-1 cluster.
func etcdClientEndpointNS(ns, name string) string {
	return fmt.Sprintf("http://%s-0.%s.%s.svc.cluster.local:2379", name, name, ns)
}

// getViaPodProxyNS GETs a path on a pod's HTTP port through the API-server
// pod proxy (agent/manager images are distroless — nothing can be exec'd).
//
//nolint:unparam // port: agent and manager both happen to serve on 8080; the parameter is the contract
func getViaPodProxyNS(ctx context.Context, cfg *envconf.Config, ns, pod string, port int, path string) ([]byte, error) {
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	return client.CoreV1().RESTClient().Get().
		Namespace(ns).
		Resource("pods").
		SubResource("proxy").
		Name(fmt.Sprintf("%s:%d", pod, port)).
		Suffix(path).
		Do(ctx).Raw()
}

// getStatuszFor decodes a mirror-agent pod's /statusz into
// mirroragent.Snapshot — the JSON tags on that type are the wire contract,
// so decoding into it cannot drift.
func getStatuszFor(ctx context.Context, cfg *envconf.Config, ns, pod string) (mirroragent.Snapshot, error) {
	raw, err := getViaPodProxyNS(ctx, cfg, ns, pod, mirrorAgentPort, "statusz")
	if err != nil {
		return mirroragent.Snapshot{}, err
	}
	var s mirroragent.Snapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		return mirroragent.Snapshot{}, fmt.Errorf("decoding /statusz %q: %w", raw, err)
	}
	return s, nil
}

// dumpAgentDiagnosticsFor logs the raw /statusz body and the pod logs so a
// failed poll is debuggable from CI output alone.
func dumpAgentDiagnosticsFor(ctx context.Context, t *testing.T, cfg *envconf.Config, ns, pod string) {
	t.Helper()
	if raw, err := getViaPodProxyNS(ctx, cfg, ns, pod, mirrorAgentPort, "statusz"); err != nil {
		t.Logf("statusz of %s/%s unavailable: %v", ns, pod, err)
	} else {
		t.Logf("last /statusz of %s/%s: %s", ns, pod, raw)
	}
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	logs, err := client.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{}).Do(ctx).Raw()
	if err != nil {
		t.Logf("pod %s/%s logs unavailable: %v", ns, pod, err)
		return
	}
	t.Logf("pod %s/%s logs:\n%s", ns, pod, logs)
}

// execEtcdctl runs etcdctl in the referenced etcd pod, retrying transient
// exec failures (SPDY stream setup is a known flake mode under CI load).
// Only for idempotent commands. The returned error wraps stderr.
func execEtcdctl(t *testing.T, cfg *envconf.Config, ref etcdPodRef, args ...string) (string, error) {
	t.Helper()
	command := append(append([]string{"etcdctl"}, ref.extraArgs...), args...)
	var stdout, stderr string
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(250 * time.Millisecond)
		}
		if stdout, stderr, err = execInPod(t, cfg, ref.pod, ref.ns, command); err == nil {
			return stdout, nil
		}
	}
	return stdout, fmt.Errorf("etcdctl %v in %s/%s: %w (stderr: %s)", args, ref.ns, ref.pod, err, stderr)
}

// putKeys writes keyPrefix{from..to-1} = valPrefix{i} (zero-padded) via the
// referenced pod, failing the test on error.
func putKeys(t *testing.T, cfg *envconf.Config, ref etcdPodRef, keyPrefix, valPrefix string, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		key := fmt.Sprintf("%s%02d", keyPrefix, i)
		if _, err := execEtcdctl(t, cfg, ref, "put", key, fmt.Sprintf("%s%02d", valPrefix, i)); err != nil {
			t.Fatalf("failed to put %s: %v", key, err)
		}
	}
}

// delKeys deletes keyPrefix{from..to-1}, failing the test on error.
func delKeys(t *testing.T, cfg *envconf.Config, ref etcdPodRef, keyPrefix string, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		key := fmt.Sprintf("%s%02d", keyPrefix, i)
		if _, err := execEtcdctl(t, cfg, ref, "del", key); err != nil {
			t.Fatalf("failed to delete %s: %v", key, err)
		}
	}
}

// putWithRevision puts one key and returns the resulting cluster revision
// from the response header.
func putWithRevision(t *testing.T, cfg *envconf.Config, ref etcdPodRef, key, val string) int64 {
	t.Helper()
	stdout, err := execEtcdctl(t, cfg, ref, "put", "-w", "json", key, val)
	if err != nil {
		t.Fatalf("failed to put %s: %v", key, err)
	}
	var resp struct {
		Header struct {
			Revision int64 `json:"revision"`
		} `json:"header"`
	}
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil || resp.Header.Revision == 0 {
		t.Fatalf("unparsable put response %q: %v", stdout, err)
	}
	return resp.Header.Revision
}

// etcdctlJSONDump returns every key/value under prefix as a map, decoded
// from `etcdctl get --prefix -w json` (keys and values are base64 on the
// wire — plain-text output cannot round-trip the reserved \x00 key). Keys
// under prefix+"\x00" (the reserved checkpoint range) are excluded.
func etcdctlJSONDump(t *testing.T, cfg *envconf.Config, ref etcdPodRef, prefix string) (map[string]string, error) {
	t.Helper()
	kvs, err := etcdctlJSONRange(t, cfg, ref, prefix)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		if strings.HasPrefix(string(kv.Key), prefix+"\x00") {
			continue
		}
		out[string(kv.Key)] = string(kv.Value)
	}
	return out, nil
}

type etcdJSONKV struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func etcdctlJSONRange(t *testing.T, cfg *envconf.Config, ref etcdPodRef, prefix string) ([]etcdJSONKV, error) {
	t.Helper()
	stdout, err := execEtcdctl(t, cfg, ref, "get", "--prefix", prefix, "-w", "json")
	if err != nil {
		return nil, err
	}
	var resp struct {
		KVs []etcdJSONKV `json:"kvs"`
	}
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		return nil, fmt.Errorf("unparsable etcdctl get response %q: %w", stdout, err)
	}
	return resp.KVs, nil
}

// findFenceValue locates and decodes the reserved checkpoint/fence key under
// prefix on the referenced (target) etcd pod. found is false when the key is
// absent.
func findFenceValue(
	t *testing.T, cfg *envconf.Config, ref etcdPodRef, prefix string,
) (fence mirroragent.FenceValue, found bool, err error) {
	t.Helper()
	kvs, err := etcdctlJSONRange(t, cfg, ref, prefix)
	if err != nil {
		return mirroragent.FenceValue{}, false, err
	}
	fenceKey := prefix + mirroragent.DefaultCheckpointKeySuffix
	for _, kv := range kvs {
		if string(kv.Key) == fenceKey {
			f, derr := mirroragent.DecodeFenceValue(kv.Value)
			if derr != nil {
				return mirroragent.FenceValue{}, true, fmt.Errorf("decoding fence at %q: %w", fenceKey, derr)
			}
			return f, true, nil
		}
	}
	return mirroragent.FenceValue{}, false, nil
}

// logDumpDiff prints the exact key-level differences between two dumps.
func logDumpDiff(t *testing.T, src, tgt map[string]string) {
	t.Helper()
	keys := make([]string, 0, len(src)+len(tgt))
	for k := range src {
		keys = append(keys, k)
	}
	for k := range tgt {
		if _, ok := src[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		sv, sok := src[k]
		tv, tok := tgt[k]
		switch {
		case !sok:
			t.Logf("  orphan on target: %q = %q", k, tv)
		case !tok:
			t.Logf("  missing on target: %q = %q", k, sv)
		case sv != tv:
			t.Logf("  divergent: %q source %q target %q", k, sv, tv)
		}
	}
}

func dumpsEqual(src, tgt map[string]string) bool {
	if len(src) != len(tgt) {
		return false
	}
	for k, v := range src {
		if tgt[k] != v {
			return false
		}
	}
	return true
}

// requireByteExact fails the test unless the two dumps are exactly equal,
// printing the per-key diff.
func requireByteExact(t *testing.T, src, tgt map[string]string) {
	t.Helper()
	if dumpsEqual(src, tgt) {
		return
	}
	logDumpDiff(t, src, tgt)
	t.Fatalf("source (%d keys) and target (%d keys) are not byte-exact", len(src), len(tgt))
}

// waitForMirrorDataMatch polls until the target's dump under prefix is
// byte-exact with the source's, dumping the diff and diagnostics on timeout.
func waitForMirrorDataMatch(
	ctx context.Context, t *testing.T, cfg *envconf.Config,
	src, tgt etcdPodRef, prefix string, ceiling, poll time.Duration,
) {
	t.Helper()
	var lastSrc, lastTgt map[string]string
	err := wait.For(func(context.Context) (bool, error) {
		s, serr := etcdctlJSONDump(t, cfg, src, prefix)
		if serr != nil {
			return false, nil //nolint:nilerr // transient exec failure; keep polling
		}
		d, derr := etcdctlJSONDump(t, cfg, tgt, prefix)
		if derr != nil {
			return false, nil //nolint:nilerr // transient exec failure; keep polling
		}
		lastSrc, lastTgt = s, d
		return dumpsEqual(s, d), nil
	}, wait.WithContext(ctx), wait.WithTimeout(ceiling), wait.WithInterval(poll))
	if err != nil {
		logDumpDiff(t, lastSrc, lastTgt)
		t.Fatalf("target never converged byte-exact under %q within %s (source %d keys, target %d keys): %v",
			prefix, ceiling, len(lastSrc), len(lastTgt), err)
	}
}

// newEtcdMirror builds the canonical cleartext EtcdMirror: endpointList with
// the operator-advertised pod-DNS client URLs, the same prefix on both
// sides. mutate (optional) adjusts the CR before creation.
func newEtcdMirror(ns, name, srcCluster, tgtCluster, prefix string,
	mutate func(*ecv1alpha1.EtcdMirror)) *ecv1alpha1.EtcdMirror {
	em := &ecv1alpha1.EtcdMirror{
		TypeMeta:   metav1.TypeMeta{APIVersion: "operator.etcd.io/v1alpha1", Kind: "EtcdMirror"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: ecv1alpha1.EtcdMirrorSpec{
			Source: ecv1alpha1.EtcdMirrorEndpoint{
				EndpointList: []string{etcdClientEndpointNS(ns, srcCluster)},
				Prefix:       prefix,
			},
			Target: ecv1alpha1.EtcdMirrorEndpoint{
				EndpointList: []string{etcdClientEndpointNS(ns, tgtCluster)},
				Prefix:       prefix,
			},
		},
	}
	if mutate != nil {
		mutate(em)
	}
	return em
}

func createMirror(ctx context.Context, t *testing.T, cfg *envconf.Config, em *ecv1alpha1.EtcdMirror) {
	t.Helper()
	if err := cfg.Client().Resources().Create(ctx, em); err != nil {
		t.Fatalf("failed to create EtcdMirror %s/%s: %v", em.Namespace, em.Name, err)
	}
}

func getMirror(ctx context.Context, cfg *envconf.Config, ns, name string) (*ecv1alpha1.EtcdMirror, error) {
	em := &ecv1alpha1.EtcdMirror{}
	if err := cfg.Client().Resources().Get(ctx, name, ns, em); err != nil {
		return nil, err
	}
	return em, nil
}

// patchMirror get-modify-updates the CR spec with one conflict retry (the
// controller only updates status, but finalizer/metadata writes can race).
func patchMirror(ctx context.Context, t *testing.T, cfg *envconf.Config,
	ns, name string, mutate func(*ecv1alpha1.EtcdMirror)) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		em, err := getMirror(ctx, cfg, ns, name)
		if err != nil {
			t.Fatalf("failed to get EtcdMirror %s/%s: %v", ns, name, err)
		}
		mutate(em)
		err = cfg.Client().Resources().Update(ctx, em)
		if err == nil {
			return
		}
		if attempt == 0 && apierrors.IsConflict(err) {
			continue
		}
		t.Fatalf("failed to update EtcdMirror %s/%s: %v", ns, name, err)
	}
}

// waitForMirror polls the CR until pred holds, dumping full diagnostics and
// failing the test on timeout. Returns the last-read CR (which satisfied
// pred).
func waitForMirror(ctx context.Context, t *testing.T, cfg *envconf.Config,
	ns, name, desc string, ceiling time.Duration, pred func(*ecv1alpha1.EtcdMirror) bool) *ecv1alpha1.EtcdMirror {
	t.Helper()
	var last *ecv1alpha1.EtcdMirror
	err := wait.For(func(ctx context.Context) (bool, error) {
		em, gerr := getMirror(ctx, cfg, ns, name)
		if gerr != nil {
			return false, nil //nolint:nilerr // transient API error; keep polling
		}
		last = em
		return pred(em), nil
	}, wait.WithContext(ctx), wait.WithTimeout(ceiling), wait.WithInterval(mirrorPollInterval))
	if err != nil {
		dumpMirrorDiagnostics(ctx, t, cfg, ns, name)
		t.Fatalf("EtcdMirror %s/%s never reached %q within %s: %v", ns, name, desc, ceiling, err)
	}
	return last
}

func waitForMirrorPhase(ctx context.Context, t *testing.T, cfg *envconf.Config,
	ns, name string, phase ecv1alpha1.EtcdMirrorPhase, ceiling time.Duration) {
	t.Helper()
	waitForMirror(ctx, t, cfg, ns, name, "phase "+string(phase), ceiling,
		func(em *ecv1alpha1.EtcdMirror) bool { return em.Status.Phase == phase })
}

func mirrorConditionIs(em *ecv1alpha1.EtcdMirror, condType string, status metav1.ConditionStatus) bool {
	c := meta.FindStatusCondition(em.Status.Conditions, condType)
	return c != nil && c.Status == status
}

// requireMirrorCondition asserts a condition status on an already-fetched CR.
func requireMirrorCondition(t *testing.T, em *ecv1alpha1.EtcdMirror, condType string, status metav1.ConditionStatus) {
	t.Helper()
	if !mirrorConditionIs(em, condType, status) {
		t.Fatalf("EtcdMirror %s/%s condition %s is not %s (conditions: %+v)",
			em.Namespace, em.Name, condType, status, em.Status.Conditions)
	}
}

// deleteMirrorAndWait deletes the CR and waits out its finalizer (agent
// stop + checkpoint delete).
func deleteMirrorAndWait(ctx context.Context, t *testing.T, cfg *envconf.Config, ns, name string) {
	t.Helper()
	em, err := getMirror(ctx, cfg, ns, name)
	if err != nil {
		t.Fatalf("failed to get EtcdMirror %s/%s for deletion: %v", ns, name, err)
	}
	if err := cfg.Client().Resources().Delete(ctx, em); err != nil {
		t.Fatalf("failed to delete EtcdMirror %s/%s: %v", ns, name, err)
	}
	if err := wait.For(func(ctx context.Context) (bool, error) {
		_, gerr := getMirror(ctx, cfg, ns, name)
		return apierrors.IsNotFound(gerr), nil
	}, wait.WithContext(ctx), wait.WithTimeout(mirrorGoneWait), wait.WithInterval(mirrorPollInterval)); err != nil {
		dumpMirrorDiagnostics(ctx, t, cfg, ns, name)
		t.Fatalf("EtcdMirror %s/%s never finished deleting within %s: %v", ns, name, mirrorGoneWait, err)
	}
}

// waitForMirrorSyncingAvailable waits for the steady state every scenario
// converges through: phase Syncing with Available=True (watermark advancing).
func waitForMirrorSyncingAvailable(ctx context.Context, t *testing.T, cfg *envconf.Config,
	ns, name string, ceiling time.Duration) *ecv1alpha1.EtcdMirror {
	t.Helper()
	return waitForMirror(ctx, t, cfg, ns, name, "Syncing with Available=True", ceiling,
		func(em *ecv1alpha1.EtcdMirror) bool {
			return em.Status.Phase == ecv1alpha1.EtcdMirrorPhaseSyncing &&
				mirrorConditionIs(em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionTrue)
		})
}

// waitForPodRunning waits for the pod to reach Running, failing fast on a
// terminal phase (RestartPolicy=Never startup failures must surface, not
// spin the full timeout). Running, not Ready: a hand-rendered agent that is
// EXPECTED to fail keeps /readyz unready by design.
func waitForPodRunning(ctx context.Context, t *testing.T, cfg *envconf.Config, ns, name string, ceiling time.Duration) {
	t.Helper()
	client := cfg.Client()
	if err := wait.For(func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		if gerr := client.Resources().Get(ctx, name, ns, &p); gerr != nil {
			return false, nil //nolint:nilerr // transient API error; keep polling
		}
		if p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("pod is terminal (%s)", p.Status.Phase)
		}
		return p.Status.Phase == corev1.PodRunning, nil
	}, wait.WithContext(ctx), wait.WithTimeout(ceiling), wait.WithInterval(mirrorPollInterval)); err != nil {
		dumpAgentDiagnosticsFor(ctx, t, cfg, ns, name)
		t.Fatalf("pod %s/%s never reached Running within %s: %v", ns, name, ceiling, err)
	}
}

// updateSecretData replaces a Secret's data in place (one conflict retry) —
// the cert-rotation contract is an in-place update of the mounted Secret.
func updateSecretData(ctx context.Context, t *testing.T, cfg *envconf.Config,
	ns, name string, data map[string][]byte) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		var s corev1.Secret
		if err := cfg.Client().Resources().Get(ctx, name, ns, &s); err != nil {
			t.Fatalf("failed to get Secret %s/%s: %v", ns, name, err)
		}
		s.Data = data
		err := cfg.Client().Resources().Update(ctx, &s)
		if err == nil {
			return
		}
		if attempt == 0 && apierrors.IsConflict(err) {
			continue
		}
		t.Fatalf("failed to update Secret %s/%s: %v", ns, name, err)
	}
}

// setupMirrorNamespace is the shared Setup skeleton: purge a leftover
// namespace, create a fresh one, register the fixture cleanup, and fail fast
// if the manager lacks the agent-image wiring. Unlike the feature Teardown,
// t.Cleanup still runs after a Setup t.Fatalf; deleteMirrorFixture is
// idempotent, so double cleanup on the happy path is harmless.
func setupMirrorNamespace(ctx context.Context, t *testing.T, cfg *envconf.Config, ns string) context.Context {
	t.Helper()
	purgeMirrorNamespace(ctx, t, cfg, ns)
	ctx, err := envfuncs.CreateNamespace(ns)(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create namespace %s: %v", ns, err)
	}
	t.Cleanup(func() {
		deleteMirrorFixture(context.Background(), t, cfg, ns)
	})
	requireMirrorAgentImageWired(ctx, t, cfg)
	return ctx
}

// listAgentPods lists the CR's agent pods via the controller label the
// workload render stamps on them.
func listAgentPods(ctx context.Context, cfg *envconf.Config, ns, crName string) ([]corev1.Pod, error) {
	var pods corev1.PodList
	err := cfg.Client().Resources(ns).List(ctx, &pods, resources.WithLabelSelector("controller="+crName))
	if err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// waitForAgentPodsGone waits until the CR has no agent pods left (the
// deterministic "agent gone" point after a pause).
func waitForAgentPodsGone(ctx context.Context, t *testing.T, cfg *envconf.Config,
	ns, crName string, ceiling time.Duration) {
	t.Helper()
	if err := wait.For(func(ctx context.Context) (bool, error) {
		pods, perr := listAgentPods(ctx, cfg, ns, crName)
		if perr != nil {
			return false, nil //nolint:nilerr // transient API error; keep polling
		}
		return len(pods) == 0, nil
	}, wait.WithContext(ctx), wait.WithTimeout(ceiling),
		wait.WithInterval(mirrorPollInterval)); err != nil {
		dumpMirrorDiagnostics(ctx, t, cfg, ns, crName)
		t.Fatalf("agent pods of %s/%s never terminated: %v", ns, crName, err)
	}
}

// agentRestartCount sums the pod's container restart counts.
func agentRestartCount(ctx context.Context, t *testing.T, cfg *envconf.Config, ns, pod string) int32 {
	t.Helper()
	var p corev1.Pod
	if err := cfg.Client().Resources().Get(ctx, pod, ns, &p); err != nil {
		t.Fatalf("failed to get pod %s/%s: %v", ns, pod, err)
	}
	var n int32
	for _, cs := range p.Status.ContainerStatuses {
		n += cs.RestartCount
	}
	return n
}

// postViaPodProxyNS POSTs a body to a path on a pod's HTTP port through the
// API-server pod proxy.
func postViaPodProxyNS(ctx context.Context, cfg *envconf.Config,
	ns, pod string, port int, path string, body []byte) ([]byte, error) {
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	return client.CoreV1().RESTClient().Post().
		Namespace(ns).
		Resource("pods").
		SubResource("proxy").
		Name(fmt.Sprintf("%s:%d", pod, port)).
		Suffix(path).
		Body(body).
		Do(ctx).Raw()
}

// putEtcdKeyRaw writes an arbitrary key (NUL bytes and all) on the referenced
// cleartext etcd pod via the v3 gRPC gateway on the client port — the
// reserved checkpoint key contains \x00 and cannot be spelled through
// etcdctl argv.
func putEtcdKeyRaw(ctx context.Context, t *testing.T, cfg *envconf.Config, ref etcdPodRef, key, value string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"key":   base64.StdEncoding.EncodeToString([]byte(key)),
		"value": base64.StdEncoding.EncodeToString([]byte(value)),
	})
	if err != nil {
		t.Fatalf("marshaling gateway put for %q: %v", key, err)
	}
	if _, err := postViaPodProxyNS(ctx, cfg, ref.ns, ref.pod, 2379, "v3/kv/put", body); err != nil {
		t.Fatalf("gateway put of %q on %s/%s: %v", key, ref.ns, ref.pod, err)
	}
}

// dumpMirrorDiagnostics logs everything needed to debug a failed wait from
// CI output alone: CR spec+status, agent Deployment status, agent pod
// logs + raw /statusz, and the namespace's events.
func dumpMirrorDiagnostics(ctx context.Context, t *testing.T, cfg *envconf.Config, ns, name string) {
	t.Helper()
	agentPod := ""
	if em, err := getMirror(ctx, cfg, ns, name); err != nil {
		t.Logf("EtcdMirror %s/%s unavailable: %v", ns, name, err)
	} else {
		t.Logf("EtcdMirror %s/%s spec: %+v", ns, name, em.Spec)
		t.Logf("EtcdMirror %s/%s status: %+v", ns, name, em.Status)
		agentPod = em.Status.AgentPod
	}
	var dep appsv1.Deployment
	if err := cfg.Client().Resources().Get(ctx, name+"-mirror-agent", ns, &dep); err == nil {
		t.Logf("agent Deployment %s status: %+v", dep.Name, dep.Status)
	}
	if agentPod != "" {
		dumpAgentDiagnosticsFor(ctx, t, cfg, ns, agentPod)
	}
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	evs, err := client.EventsV1().Events(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Logf("events in %s unavailable: %v", ns, err)
		return
	}
	for _, e := range evs.Items {
		t.Logf("event %s %s %s/%s: %s", e.Type, e.Reason, e.Regarding.Kind, e.Regarding.Name, e.Note)
	}
}

// listMirrorEvents returns the events.k8s.io/v1 events recorded against the
// named EtcdMirror.
func listMirrorEvents(ctx context.Context, cfg *envconf.Config, ns, crName string) ([]eventsv1.Event, error) {
	client := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
	evs, err := client.EventsV1().Events(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var out []eventsv1.Event
	for _, e := range evs.Items {
		if e.Regarding.Kind == "EtcdMirror" && e.Regarding.Name == crName {
			out = append(out, e)
		}
	}
	return out, nil
}

// waitForMirrorEvent waits until an event with the given reason — and, when
// noteSubstring is non-empty, a note containing it — has been recorded
// against the CR.
func waitForMirrorEvent(ctx context.Context, t *testing.T, cfg *envconf.Config,
	ns, crName, reason, noteSubstring string, ceiling time.Duration) {
	t.Helper()
	var last []string
	err := wait.For(func(ctx context.Context) (bool, error) {
		evs, lerr := listMirrorEvents(ctx, cfg, ns, crName)
		if lerr != nil {
			return false, nil //nolint:nilerr // transient API error; keep polling
		}
		last = last[:0]
		for _, e := range evs {
			last = append(last, e.Reason+" ("+e.Note+")")
			if e.Reason == reason && strings.Contains(e.Note, noteSubstring) {
				return true, nil
			}
		}
		return false, nil
	}, wait.WithContext(ctx), wait.WithTimeout(ceiling), wait.WithInterval(5*time.Second))
	if err != nil {
		dumpMirrorDiagnostics(ctx, t, cfg, ns, crName)
		t.Fatalf("event %s with note containing %q never recorded for EtcdMirror %s/%s within %s (saw %v): %v",
			reason, noteSubstring, ns, crName, ceiling, last, err)
	}
}

// assertManagerMetric polls the manager's plain-HTTP /metrics (see
// config/e2e/patch-manager-flags.yaml) until some sample line contains every
// substring and has value 1, dumping the full scrape on timeout.
func assertManagerMetric(ctx context.Context, t *testing.T, cfg *envconf.Config, substrings ...string) {
	t.Helper()
	var lastScrape []byte
	err := wait.For(func(ctx context.Context) (bool, error) {
		pod, perr := getEtcdOperatorPod(t, cfg.Client())
		if perr != nil {
			return false, nil //nolint:nilerr // transient API error; keep polling
		}
		raw, gerr := getViaPodProxyNS(ctx, cfg, namespace, pod.Name, managerMetricsPort, "metrics")
		if gerr != nil {
			return false, nil //nolint:nilerr // transient proxy failure; keep polling
		}
		lastScrape = raw
		return metricsHaveSample(string(raw), substrings), nil
	}, wait.WithContext(ctx), wait.WithTimeout(time.Minute), wait.WithInterval(5*time.Second))
	if err != nil {
		t.Logf("last manager /metrics scrape:\n%s", lastScrape)
		t.Fatalf("no /metrics sample line with value 1 containing all of %v: %v", substrings, err)
	}
}

// metricsHaveSample reports whether any non-comment sample line contains all
// substrings and has value 1.
func metricsHaveSample(scrape string, substrings []string) bool {
	for _, line := range strings.Split(scrape, "\n") {
		if strings.HasPrefix(line, "#") || !containsAll(line, substrings) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[len(fields)-1] == "1" {
			return true
		}
	}
	return false
}

func containsAll(line string, substrings []string) bool {
	for _, s := range substrings {
		if !strings.Contains(line, s) {
			return false
		}
	}
	return true
}

// requireMirrorAgentImageWired fails fast (with a pointer at the kustomize
// wiring) when the deployed manager lacks --mirror-agent-image — otherwise
// every scenario would burn its full ceiling waiting on Pending CRs.
func requireMirrorAgentImageWired(ctx context.Context, t *testing.T, cfg *envconf.Config) {
	t.Helper()
	var dep appsv1.Deployment
	if err := cfg.Client().Resources().Get(ctx, managerDeployment, namespace, &dep); err != nil {
		t.Fatalf("failed to get manager Deployment %s/%s: %v", namespace, managerDeployment, err)
	}
	for _, c := range dep.Spec.Template.Spec.Containers {
		for _, a := range c.Args {
			if strings.HasPrefix(a, "--mirror-agent-image=") {
				return
			}
		}
	}
	t.Fatalf("manager Deployment %s/%s has no --mirror-agent-image arg; "+
		"config/e2e/patch-manager-flags.yaml is not applied (make deploy DEPLOY_MODE=e2e)", namespace, managerDeployment)
}

// purgeMirrorNamespace deletes a leftover namespace from a previous run and
// waits it out, so reruns self-heal (the ETCD_E2E_SKIP_TEARDOWN contract).
// Leftover EtcdMirrors get the skip-checkpoint-cleanup annotation first:
// their target etcd may already be gone, and the normal finalizer path would
// wedge namespace deletion forever.
func purgeMirrorNamespace(ctx context.Context, t *testing.T, cfg *envconf.Config, ns string) {
	t.Helper()
	client := cfg.Client()
	var leftover corev1.Namespace
	if err := client.Resources().Get(ctx, ns, "", &leftover); err != nil {
		return
	}
	t.Logf("deleting leftover namespace %s from a previous run", ns)
	releaseMirrorFinalizers(ctx, t, cfg, ns)
	_ = client.Resources().Delete(ctx, &leftover)
	if err := wait.For(func(ctx context.Context) (bool, error) {
		var n corev1.Namespace
		return apierrors.IsNotFound(client.Resources().Get(ctx, ns, "", &n)), nil
	}, wait.WithContext(ctx), wait.WithTimeout(2*time.Minute), wait.WithInterval(mirrorPollInterval)); err != nil {
		t.Fatalf("leftover namespace %s never finished deleting: %v", ns, err)
	}
}

// releaseMirrorFinalizers best-effort annotates every EtcdMirror in ns with
// the skip-checkpoint-cleanup escape hatch so deletion cannot wedge on an
// unreachable target.
func releaseMirrorFinalizers(ctx context.Context, t *testing.T, cfg *envconf.Config, ns string) {
	t.Helper()
	var mirrors ecv1alpha1.EtcdMirrorList
	if err := cfg.Client().Resources(ns).List(ctx, &mirrors); err != nil {
		return
	}
	for i := range mirrors.Items {
		em := &mirrors.Items[i]
		if em.Annotations == nil {
			em.Annotations = map[string]string{}
		}
		em.Annotations[skipCheckpointCleanupAnnotation] = "true"
		if err := cfg.Client().Resources().Update(ctx, em); err != nil {
			t.Logf("failed to annotate EtcdMirror %s/%s for skip-cleanup: %v", ns, em.Name, err)
		}
	}
}

// deleteMirrorFixture best-effort deletes everything a mirror e2e fixture
// creates: CRs first (so finalizers run while their target etcd still
// exists), then bare pods/StatefulSets/Services/Secrets/EtcdClusters, then
// the namespace. Idempotent; shared by feature Teardowns and Setup
// t.Cleanups (a Setup t.Fatalf runtime.Goexits past the feature teardowns).
func deleteMirrorFixture(ctx context.Context, t *testing.T, cfg *envconf.Config, ns string) {
	t.Helper()
	client := cfg.Client()

	var mirrors ecv1alpha1.EtcdMirrorList
	if err := client.Resources(ns).List(ctx, &mirrors); err == nil {
		for i := range mirrors.Items {
			if err := client.Resources().Delete(ctx, &mirrors.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				t.Logf("failed to delete EtcdMirror %s/%s: %v", ns, mirrors.Items[i].Name, err)
			}
		}
		if len(mirrors.Items) > 0 && !waitMirrorsGone(ctx, cfg, ns, 2*time.Minute) {
			// Escalate: the finalizer is wedged (target already unreachable).
			t.Logf("EtcdMirrors in %s still present after 2m; escalating to skip-checkpoint-cleanup", ns)
			releaseMirrorFinalizers(ctx, t, cfg, ns)
			_ = waitMirrorsGone(ctx, cfg, ns, time.Minute)
		}
	}

	deleteObj := func(obj k8s.Object) {
		if err := client.Resources().Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			t.Logf("failed to delete %T %s/%s: %v", obj, ns, obj.GetName(), err)
		}
	}
	var pods corev1.PodList
	if err := client.Resources(ns).List(ctx, &pods); err == nil {
		for i := range pods.Items {
			deleteObj(&pods.Items[i])
		}
	}
	var stss appsv1.StatefulSetList
	if err := client.Resources(ns).List(ctx, &stss); err == nil {
		for i := range stss.Items {
			deleteObj(&stss.Items[i])
		}
	}
	var svcs corev1.ServiceList
	if err := client.Resources(ns).List(ctx, &svcs); err == nil {
		for i := range svcs.Items {
			deleteObj(&svcs.Items[i])
		}
	}
	var secrets corev1.SecretList
	if err := client.Resources(ns).List(ctx, &secrets); err == nil {
		for i := range secrets.Items {
			deleteObj(&secrets.Items[i])
		}
	}
	var clusters ecv1alpha1.EtcdClusterList
	if err := client.Resources(ns).List(ctx, &clusters); err == nil {
		for i := range clusters.Items {
			deleteObj(&clusters.Items[i])
		}
	}

	var nsObj corev1.Namespace
	if err := client.Resources().Get(ctx, ns, "", &nsObj); err == nil {
		if err := client.Resources().Delete(ctx, &nsObj); err != nil && !apierrors.IsNotFound(err) {
			t.Logf("failed to delete namespace %s: %v", ns, err)
		}
	}
}

func waitMirrorsGone(ctx context.Context, cfg *envconf.Config, ns string, ceiling time.Duration) bool {
	err := wait.For(func(ctx context.Context) (bool, error) {
		var mirrors ecv1alpha1.EtcdMirrorList
		if lerr := cfg.Client().Resources(ns).List(ctx, &mirrors); lerr != nil {
			return false, nil //nolint:nilerr // transient API error; keep polling
		}
		return len(mirrors.Items) == 0, nil
	}, wait.WithContext(ctx), wait.WithTimeout(ceiling), wait.WithInterval(mirrorPollInterval))
	return err == nil
}
