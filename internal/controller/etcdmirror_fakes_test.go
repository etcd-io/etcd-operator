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

package controller

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

// fakeStatusClient is the AgentStatusClient seam for envtest: agent pods
// never run there, so /statusz answers come from this fixture instead.
type fakeStatusClient struct {
	mu    sync.Mutex
	snap  *mirroragent.Snapshot
	err   error
	calls []string
}

func (f *fakeStatusClient) Snapshot(_ context.Context, addr string) (*mirroragent.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, addr)
	if f.err != nil {
		return nil, f.err
	}
	snap := *f.snap
	return &snap, nil
}

func (f *fakeStatusClient) set(snap *mirroragent.Snapshot, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snap, f.err = snap, err
}

func (f *fakeStatusClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// fakeCleaner is the CheckpointCleaner seam (no reachable target etcd in
// envtest).
type fakeCleaner struct {
	mu    sync.Mutex
	err   error
	calls []CheckpointTarget
}

func (f *fakeCleaner) DeleteCheckpoint(_ context.Context, tgt CheckpointTarget) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, tgt)
	return f.err
}

func (f *fakeCleaner) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeCleaner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type recordedEvent struct {
	Type   string
	Reason string
	Note   string
}

// fakeRecorder captures events behind the events.EventRecorder interface.
type fakeRecorder struct {
	mu     sync.Mutex
	events []recordedEvent
}

func (f *fakeRecorder) Eventf(
	_ runtime.Object, _ runtime.Object, eventtype, reason, _, note string, args ...interface{},
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, recordedEvent{Type: eventtype, Reason: reason, Note: fmt.Sprintf(note, args...)})
}

func (f *fakeRecorder) countReason(reason string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.events {
		if e.Reason == reason {
			n++
		}
	}
	return n
}

func (f *fakeRecorder) lastNote(reason string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.events) - 1; i >= 0; i-- {
		if f.events[i].Reason == reason {
			return f.events[i].Note
		}
	}
	return ""
}

const testAgentImage = "example.com/etcd-operator:test"

func newTestMirrorReconciler(sc AgentStatusClient, cleaner CheckpointCleaner) (*EtcdMirrorReconciler, *fakeRecorder) {
	rec := &fakeRecorder{}
	return &EtcdMirrorReconciler{
		Client:       k8sClient,
		Scheme:       scheme.Scheme,
		Recorder:     rec,
		AgentImage:   testAgentImage,
		StatusClient: sc,
		Cleaner:      cleaner,
	}, rec
}

func createTestNamespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "etcdmirror-test-"}}
	require.NoError(t, k8sClient.Create(t.Context(), ns))
	return ns.Name
}

// mirrorSeq makes every test mirror's default endpoint hosts unique, so the
// cluster-wide guard checks never cross-talk between tests.
var mirrorSeq atomic.Int64

// newEnvtestMirror creates (via the API server, so defaults and CEL apply) a
// minimal valid EtcdMirror with unique default endpoint hosts.
func newEnvtestMirror(t *testing.T, ns string, mutate func(*ecv1alpha1.EtcdMirror)) *ecv1alpha1.EtcdMirror {
	t.Helper()
	em := &ecv1alpha1.EtcdMirror{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "mirror-", Namespace: ns},
		Spec: ecv1alpha1.EtcdMirrorSpec{
			Source: ecv1alpha1.EtcdMirrorEndpoint{Prefix: "/registry/"},
			Target: ecv1alpha1.EtcdMirrorEndpoint{Prefix: "/mirrored/"},
		},
	}
	if mutate != nil {
		mutate(em)
	}
	// Default unique endpoints unless the mutation chose its own resolution.
	n := mirrorSeq.Add(1)
	if len(em.Spec.Source.EndpointList) == 0 && em.Spec.Source.ServiceRef == nil {
		em.Spec.Source.EndpointList = []string{fmt.Sprintf("src-%d.example.com:2379", n)}
	}
	if len(em.Spec.Target.EndpointList) == 0 && em.Spec.Target.ServiceRef == nil {
		em.Spec.Target.EndpointList = []string{fmt.Sprintf("tgt-%d.example.com:2379", n)}
	}
	require.NoError(t, k8sClient.Create(t.Context(), em))
	return em
}

func mirrorRequest(em *ecv1alpha1.EtcdMirror) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: em.Namespace, Name: em.Name}}
}

func reconcileMirrorOnce(t *testing.T, r *EtcdMirrorReconciler, em *ecv1alpha1.EtcdMirror) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(t.Context(), mirrorRequest(em))
	require.NoError(t, err)
	return res
}

func refreshMirror(t *testing.T, em *ecv1alpha1.EtcdMirror) *ecv1alpha1.EtcdMirror {
	t.Helper()
	out := &ecv1alpha1.EtcdMirror{}
	require.NoError(t, k8sClient.Get(t.Context(), types.NamespacedName{Namespace: em.Namespace, Name: em.Name}, out))
	return out
}

// makeAgentPodReady creates a Running agent pod with a PodIP for em (envtest
// has no kubelet, so the test IS the kubelet).
func makeAgentPodReady(t *testing.T, em *ecv1alpha1.EtcdMirror) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentNameForEtcdMirror(em) + "-",
			Namespace:    em.Namespace,
			Labels:       etcdMirrorAgentLabels(em),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "mirror-agent", Image: testAgentImage}},
		},
	}
	require.NoError(t, k8sClient.Create(t.Context(), pod))
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "10.1.2.3"
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	require.NoError(t, k8sClient.Status().Update(t.Context(), pod))
	return pod
}

// setupActiveMirror runs a CR through finalizer-add, Deployment creation, and
// pod readiness so the next reconcile polls the (fake) agent.
func setupActiveMirror(
	t *testing.T, r *EtcdMirrorReconciler, ns string, mutate func(*ecv1alpha1.EtcdMirror),
) *ecv1alpha1.EtcdMirror {
	t.Helper()
	em := newEnvtestMirror(t, ns, mutate)
	reconcileMirrorOnce(t, r, em) // adds finalizer
	reconcileMirrorOnce(t, r, em) // creates Deployment
	makeAgentPodReady(t, em)
	return em
}

// healthySnapshot is a steady-state Syncing fixture with a fresh
// verification pass. Cluster IDs are unique per call: the runtime IDs land in
// status and the guards compare them cluster-wide, so shared fixture IDs
// would (correctly!) raise PrefixConflict between unrelated tests' mirrors.
func healthySnapshot() *mirroragent.Snapshot {
	now := time.Now()
	n := uint64(mirrorSeq.Add(1)) //nolint:gosec // test counter, never negative
	return &mirroragent.Snapshot{
		Phase:                     mirroragent.PhaseSyncing,
		SourceVersion:             "3.5.21",
		TargetVersion:             "3.6.10",
		SourceClusterID:           0xa000000000000000 + n,
		TargetClusterID:           0xb000000000000000 + n,
		Watermark:                 1200,
		SourceRevision:            1234,
		LastProgressTime:          now,
		InitialSyncKeyCount:       500,
		InitialSyncTotalKeyCount:  500,
		InitialSyncStartTime:      now.Add(-2 * time.Hour),
		InitialSyncCompletionTime: now.Add(-time.Hour),
		LeaseBackedKeyCount:       3,
		SourceKeyCount:            500,
		TargetKeyCount:            500,
		LastReconcileTime:         now.Add(-10 * time.Minute),
		LastReconcileDrift:        &mirroragent.Drift{},
	}
}

func findCond(em *ecv1alpha1.EtcdMirror, condType string) *metav1.Condition {
	for i := range em.Status.Conditions {
		if em.Status.Conditions[i].Type == condType {
			return &em.Status.Conditions[i]
		}
	}
	return nil
}

func requireCond(
	t *testing.T, em *ecv1alpha1.EtcdMirror, condType string, status metav1.ConditionStatus, reason string,
) *metav1.Condition {
	t.Helper()
	cond := findCond(em, condType)
	require.NotNil(t, cond, "condition %s not present", condType)
	require.Equal(t, status, cond.Status, "condition %s status (reason %s: %s)", condType, cond.Reason, cond.Message)
	if reason != "" {
		require.Equal(t, reason, cond.Reason, "condition %s reason", condType)
	}
	return cond
}
