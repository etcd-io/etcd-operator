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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

func getAgentDeployment(t *testing.T, em *ecv1alpha1.EtcdMirror) *appsv1.Deployment {
	t.Helper()
	dep := &appsv1.Deployment{}
	err := k8sClient.Get(t.Context(),
		types.NamespacedName{Namespace: em.Namespace, Name: deploymentNameForEtcdMirror(em)}, dep)
	require.NoError(t, err)
	return dep
}

func TestEtcdMirrorReconcile_RendersDeployment(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)
	em := newEnvtestMirror(t, ns, nil)

	// First reconcile adds the finalizer.
	reconcileMirrorOnce(t, r, em)
	em = refreshMirror(t, em)
	assert.Contains(t, em.Finalizers, checkpointCleanupFinalizer)

	// Second reconcile creates the Deployment; Pending before any pod.
	res := reconcileMirrorOnce(t, r, em)
	assert.Equal(t, statusPollInterval, res.RequeueAfter)
	em = refreshMirror(t, em)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhasePending, em.Status.Phase)

	dep := getAgentDeployment(t, em)
	require.Len(t, dep.OwnerReferences, 1)
	assert.Equal(t, em.Name, dep.OwnerReferences[0].Name)
	assert.True(t, *dep.OwnerReferences[0].Controller)
	c := dep.Spec.Template.Spec.Containers[0]
	assert.Equal(t, []string{"/mirror-agent"}, c.Command)
	assert.Equal(t, testAgentImage, c.Image)
	assert.Contains(t, c.Args, "--link-uid="+string(em.UID))
	assert.Contains(t, c.Args, "--epoch=1")
	assert.Contains(t, c.Args, "--mode=Sync")
	assert.Contains(t, c.Args, "--source-prefix=/registry/")
	assert.Contains(t, c.Args, "--source-endpoints="+em.Spec.Source.EndpointList[0])
	assert.Contains(t, c.Args, "--target-endpoints="+em.Spec.Target.EndpointList[0])

	// No pod yet: AgentPodNotReady, no statusz call.
	reconcileMirrorOnce(t, r, em)
	em = refreshMirror(t, em)
	requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonAgentPodNotReady)
	assert.Zero(t, sc.callCount())

	// Pod running with an IP: the snapshot lands in status.
	pod := makeAgentPodReady(t, em)
	res = reconcileMirrorOnce(t, r, em)
	assert.Equal(t, statusPollInterval, res.RequeueAfter)
	em = refreshMirror(t, em)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseSyncing, em.Status.Phase)
	requireCond(t, em, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionTrue, reasonWatermarkAdvancing)
	assert.Equal(t, fmt.Sprintf("%x", sc.snap.SourceClusterID), em.Status.SourceClusterID)
	assert.Equal(t, fmt.Sprintf("%x", sc.snap.TargetClusterID), em.Status.TargetClusterID)
	assert.Equal(t, int64(1200), em.Status.LastAppliedRevision)
	assert.Equal(t, pod.Name, em.Status.AgentPod)
	assert.Equal(t, em.Generation, em.Status.ObservedGeneration)
	require.Equal(t, 1, sc.callCount())
	assert.Equal(t, "10.1.2.3:8080", sc.calls[0])
}

func TestEtcdMirrorReconcile_ConditionFixtures(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	ns := createTestNamespace(t)

	t.Run("lagging sustained via the injected ledger", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.SourceRevision = snap.Watermark + lagRevisionThreshold + 100
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionReplicationLagExceeded,
			metav1.ConditionFalse, reasonWithinSustainWindow)

		// Backdate the ledger past the sustain window.
		r.mu.Lock()
		r.lagSince[got.UID] = time.Now().Add(-lagSustainDuration - time.Minute)
		r.mu.Unlock()
		reconcileMirrorOnce(t, r, em)
		got = refreshMirror(t, em)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionReplicationLagExceeded,
			metav1.ConditionTrue, reasonLagSustained)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionInvariantsHeld,
			metav1.ConditionFalse, reasonLagExceeded)
	})

	t.Run("forced resync reports InitialSync plus Compacted", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseInitialSync
		snap.Compacted = true
		snap.ForcedResyncCount = 1
		snap.LastResyncReason = mirroragent.ResyncReasonCompacted
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseInitialSync, got.Status.Phase)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionCompacted, metav1.ConditionTrue,
			ecv1alpha1.EtcdMirrorReasonForcedResync)
		assert.Equal(t, int32(1), got.Status.ForcedResyncCount)
	})

	t.Run("throttled while degraded", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseDegraded
		snap.Throttled = true
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseDegraded, got.Status.Phase)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionTargetThrottled, metav1.ConditionTrue, "")
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonBackoff)
	})

	t.Run("quota exhausted", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.QuotaExhausted = true
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionTargetQuotaExhausted, metav1.ConditionTrue, "")
	})

	t.Run("resync loop", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.ResyncLoopDetected = true
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionResyncLoopDetected, metav1.ConditionTrue, "")
	})

	t.Run("drift counts break InvariantsHeld", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.LastReconcileDrift = &mirroragent.Drift{MissingKeys: 4, OrphanKeys: 1}
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		cond := requireCond(t, got, ecv1alpha1.EtcdMirrorConditionDriftDetected, metav1.ConditionTrue, "")
		assert.Contains(t, cond.Message, "4 missing")
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionInvariantsHeld, metav1.ConditionFalse, reasonDriftDetected)
		require.NotNil(t, got.Status.LastReconciliationDrift)
		assert.Equal(t, int64(4), got.Status.LastReconciliationDrift.MissingKeys)
	})

	t.Run("drain maps Drained and populates cutover", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseDrained
		snap.CutoverReady = true
		snap.Cutover = &mirroragent.CutoverStatus{
			DrainTargetRevision: 1234,
			DrainedRevision:     1234,
			VerifiedTime:        time.Now(),
			SourceKeyCount:      500,
			TargetKeyCount:      500,
			LeasedKeyCount:      2,
		}
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, func(em *ecv1alpha1.EtcdMirror) {
			em.Spec.Mode = ecv1alpha1.EtcdMirrorModeDrain
		})
		// Drain propagates through the args (spec-driven rollout).
		dep := getAgentDeployment(t, em)
		assert.Contains(t, dep.Spec.Template.Spec.Containers[0].Args, "--mode=Drain")

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseSyncing, got.Status.Phase)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionCutoverReady, metav1.ConditionTrue, "")
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionTrue, reasonDrainComplete)
		require.NotNil(t, got.Status.Cutover)
		assert.Equal(t, int64(1234), got.Status.Cutover.DrainedRevision)
	})

	t.Run("failed with UnsupportedVersion", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseFailed
		snap.LastError = "source version 3.3.0 is below the 3.4 floor"
		snap.LastErrorReason = "UnsupportedVersion"
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		res := reconcileMirrorOnce(t, r, em)
		assert.Equal(t, slowRequeueInterval, res.RequeueAfter, "Failed mirrors poll at the slow rate")
		got := refreshMirror(t, em)
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseFailed, got.Status.Phase)
		cond := requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse,
			ecv1alpha1.EtcdMirrorReasonUnsupportedVersion)
		assert.Equal(t, snap.LastError, cond.Message)
	})

	t.Run("failed with CheckpointInvalid", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.Phase = mirroragent.PhaseFailed
		snap.LastError = "checkpoint payload corrupt"
		snap.LastErrorReason = "CheckpointInvalid"
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse,
			ecv1alpha1.EtcdMirrorReasonCheckpointInvalid)
	})

	t.Run("learner endpoint", func(t *testing.T) {
		sc := &fakeStatusClient{}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		snap := healthySnapshot()
		snap.TargetLearner = true
		sc.set(snap, nil)
		em := setupActiveMirror(t, r, ns, nil)

		reconcileMirrorOnce(t, r, em)
		got := refreshMirror(t, em)
		cond := requireCond(t, got, ecv1alpha1.EtcdMirrorConditionLearnerEndpoint, metav1.ConditionTrue, "")
		assert.Contains(t, cond.Message, "target")
	})
}

func TestEtcdMirrorReconcile_EmptyTargetViolation(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{}
	r, rec := newTestMirrorReconciler(sc, &fakeCleaner{})
	snap := healthySnapshot()
	snap.Phase = mirroragent.PhaseFailed
	snap.LastError = "destination prefix /mirrored/ holds 12 keys"
	snap.LastErrorReason = "EmptyTargetViolation"
	sc.set(snap, nil)
	ns := createTestNamespace(t)
	em := setupActiveMirror(t, r, ns, nil)

	reconcileMirrorOnce(t, r, em)
	reconcileMirrorOnce(t, r, em)
	got := refreshMirror(t, em)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseFailed, got.Status.Phase)
	cond := requireCond(t, got, ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation, metav1.ConditionTrue, "")
	assert.Contains(t, cond.Message, `etcdctl del "/mirrored/" "/mirrored0"`)
	assert.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation),
		"the Warning must fire exactly once across reconciles")
	assert.Contains(t, rec.lastNote(ecv1alpha1.EtcdMirrorConditionEmptyTargetViolation),
		`etcdctl del "/mirrored/" "/mirrored0"`)
}

func TestEtcdMirrorReconcile_NoPodYet(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)
	em := newEnvtestMirror(t, ns, nil)
	reconcileMirrorOnce(t, r, em)
	reconcileMirrorOnce(t, r, em)
	res := reconcileMirrorOnce(t, r, em)
	assert.Equal(t, statusPollInterval, res.RequeueAfter)
	got := refreshMirror(t, em)
	requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonAgentPodNotReady)
	assert.Zero(t, sc.callCount(), "statusz must never be polled without a running pod IP")
}

func TestEtcdMirrorReconcile_PodNotReady(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)
	em := newEnvtestMirror(t, ns, nil)
	reconcileMirrorOnce(t, r, em)
	reconcileMirrorOnce(t, r, em)

	// Pod exists but is not Running and has no IP.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentNameForEtcdMirror(em) + "-",
			Namespace:    em.Namespace,
			Labels:       etcdMirrorAgentLabels(em),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mirror-agent", Image: testAgentImage}}},
	}
	require.NoError(t, k8sClient.Create(t.Context(), pod))

	reconcileMirrorOnce(t, r, em)
	got := refreshMirror(t, em)
	requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonAgentPodNotReady)
	assert.Equal(t, pod.Name, got.Status.AgentPod)
	assert.Zero(t, sc.callCount())
}

func TestEtcdMirrorReconcile_StatuszTimeout(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)
	em := setupActiveMirror(t, r, ns, nil)

	// One healthy poll populates status.
	reconcileMirrorOnce(t, r, em)
	got := refreshMirror(t, em)
	require.Equal(t, ecv1alpha1.EtcdMirrorPhaseSyncing, got.Status.Phase)
	syncTime := got.Status.LastStatusSyncTime
	require.NotNil(t, syncTime)

	// Then the agent stops answering.
	sc.set(nil, errors.New("dial tcp 10.1.2.3:8080: i/o timeout"))
	res := reconcileMirrorOnce(t, r, em)
	assert.Equal(t, statusPollInterval, res.RequeueAfter)
	got = refreshMirror(t, em)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseDegraded, got.Status.Phase)
	cond := requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionUnknown,
		reasonAgentStatusUnreachable)
	assert.Contains(t, cond.Message, "i/o timeout")
	// Prior fields retained; staleness observable.
	assert.Equal(t, int64(1200), got.Status.LastAppliedRevision)
	assert.True(t, got.Status.LastStatusSyncTime.Equal(syncTime), "LastStatusSyncTime must not advance")
}

func TestEtcdMirrorReconcile_SnapshotDecodeFailure(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)
	em := setupActiveMirror(t, r, ns, nil)

	sc.set(nil, &snapshotDecodeError{err: errors.New("invalid character '<'")})
	reconcileMirrorOnce(t, r, em)
	got := refreshMirror(t, em)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhaseDegraded, got.Status.Phase)
	requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionUnknown, reasonSnapshotDecodeFailed)
}

func TestEtcdMirrorReconcile_PrefixConflict(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, rec := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)
	sharedTarget := []string{"shared-prefix-tgt.example.com:2379"}

	older := newEnvtestMirror(t, ns, func(em *ecv1alpha1.EtcdMirror) {
		em.Spec.Target.EndpointList = sharedTarget
		em.Spec.Target.Prefix = "/mirrored/"
	})
	reconcileMirrorOnce(t, r, older)
	reconcileMirrorOnce(t, r, older)

	// creationTimestamp has 1s resolution; make the second CR strictly newer.
	time.Sleep(1100 * time.Millisecond)
	newer := newEnvtestMirror(t, ns, func(em *ecv1alpha1.EtcdMirror) {
		em.Spec.Target.EndpointList = sharedTarget
		em.Spec.Target.Prefix = "/mirrored/nested/"
	})
	reconcileMirrorOnce(t, r, newer)
	res := reconcileMirrorOnce(t, r, newer)
	assert.Equal(t, slowRequeueInterval, res.RequeueAfter)

	got := refreshMirror(t, newer)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhasePending, got.Status.Phase)
	cond := requireCond(t, got, ecv1alpha1.EtcdMirrorConditionPrefixConflict, metav1.ConditionTrue, reasonConflict)
	assert.Contains(t, cond.Message, older.Name)
	dep := getAgentDeployment(t, newer)
	assert.Equal(t, int32(0), *dep.Spec.Replicas, "the loser's Deployment must be scaled to zero")
	require.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorConditionPrefixConflict))

	// Second reconcile: no extra Warning (transition-only).
	reconcileMirrorOnce(t, r, newer)
	assert.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorConditionPrefixConflict))

	// The older CR is unaffected.
	reconcileMirrorOnce(t, r, older)
	gotOlder := refreshMirror(t, older)
	requireCond(t, gotOlder, ecv1alpha1.EtcdMirrorConditionPrefixConflict, metav1.ConditionFalse, reasonNoConflict)
	assert.NotEqual(t, ecv1alpha1.EtcdMirrorPhaseFailed, gotOlder.Status.Phase)

	// Deleting the older clears the conflict on the next reconcile.
	require.NoError(t, k8sClient.Delete(t.Context(), gotOlder))
	reconcileMirrorOnce(t, r, older) // finalize path releases it
	err := k8sClient.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: older.Name}, &ecv1alpha1.EtcdMirror{})
	require.True(t, apierrors.IsNotFound(err))

	reconcileMirrorOnce(t, r, newer)
	got = refreshMirror(t, newer)
	requireCond(t, got, ecv1alpha1.EtcdMirrorConditionPrefixConflict, metav1.ConditionFalse, reasonNoConflict)
}

func TestEtcdMirrorReconcile_DirectionConflict(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)

	older := newEnvtestMirror(t, ns, nil)
	reconcileMirrorOnce(t, r, older)
	time.Sleep(1100 * time.Millisecond)
	newer := newEnvtestMirror(t, ns, nil)
	reconcileMirrorOnce(t, r, newer)

	// Seed inverted runtime cluster-ID pairs (respelled endpoints would fool
	// a spec comparison; the runtime IDs cannot be fooled).
	seed := func(em *ecv1alpha1.EtcdMirror, src, tgt string) {
		got := refreshMirror(t, em)
		got.Status.SourceClusterID = src
		got.Status.TargetClusterID = tgt
		require.NoError(t, k8sClient.Status().Update(t.Context(), got))
	}
	seed(older, "aaaa1111", "bbbb2222")
	seed(newer, "bbbb2222", "aaaa1111")

	res := reconcileMirrorOnce(t, r, newer)
	assert.Equal(t, slowRequeueInterval, res.RequeueAfter)
	gotNewer := refreshMirror(t, newer)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhasePending, gotNewer.Status.Phase)
	requireCond(t, gotNewer, ecv1alpha1.EtcdMirrorConditionDirectionConflict, metav1.ConditionTrue, reasonConflict)
	dep := getAgentDeployment(t, newer)
	assert.Equal(t, int32(0), *dep.Spec.Replicas)

	// The winner reports the mutual condition too, but keeps running.
	reconcileMirrorOnce(t, r, older)
	gotOlder := refreshMirror(t, older)
	requireCond(t, gotOlder, ecv1alpha1.EtcdMirrorConditionDirectionConflict, metav1.ConditionTrue, reasonConflict)
	assert.NotEqual(t, ecv1alpha1.EtcdMirrorPhaseFailed, gotOlder.Status.Phase)
	depOlder := getAgentDeployment(t, older)
	assert.Equal(t, int32(1), *depOlder.Spec.Replicas)
}

func TestEtcdMirrorReconcile_Paused(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)
	em := setupActiveMirror(t, r, ns, nil)
	reconcileMirrorOnce(t, r, em)
	got := refreshMirror(t, em)
	require.Equal(t, ecv1alpha1.EtcdMirrorPhaseSyncing, got.Status.Phase)

	got.Spec.Paused = true
	require.NoError(t, k8sClient.Update(t.Context(), got))
	res := reconcileMirrorOnce(t, r, em)
	assert.Equal(t, slowRequeueInterval, res.RequeueAfter)
	got = refreshMirror(t, em)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhasePaused, got.Status.Phase)
	requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonPaused)
	// Other status stays honest (retained, not zeroed).
	assert.Equal(t, int64(1200), got.Status.LastAppliedRevision)
	dep := getAgentDeployment(t, em)
	assert.Equal(t, int32(0), *dep.Spec.Replicas)

	got.Spec.Paused = false
	require.NoError(t, k8sClient.Update(t.Context(), got))
	reconcileMirrorOnce(t, r, em)
	dep = getAgentDeployment(t, em)
	assert.Equal(t, int32(1), *dep.Spec.Replicas)
}

func TestEtcdMirrorReconcile_Finalizer(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}

	t.Run("deployment first, then checkpoint delete, retries on failure", func(t *testing.T) {
		sc := &fakeStatusClient{snap: healthySnapshot()}
		cleaner := &fakeCleaner{}
		r, rec := newTestMirrorReconciler(sc, cleaner)
		ns := createTestNamespace(t)
		em := setupActiveMirror(t, r, ns, nil)

		require.NoError(t, k8sClient.Delete(t.Context(), refreshMirror(t, em)))

		// Pods still exist: the agent must stop before the key is deleted.
		res := reconcileMirrorOnce(t, r, em)
		assert.Equal(t, finalizerPodWait, res.RequeueAfter)
		assert.Zero(t, cleaner.callCount())
		dep := &appsv1.Deployment{}
		err := k8sClient.Get(t.Context(),
			types.NamespacedName{Namespace: em.Namespace, Name: deploymentNameForEtcdMirror(em)}, dep)
		if err == nil {
			assert.NotNil(t, dep.DeletionTimestamp)
		} else {
			assert.True(t, apierrors.IsNotFound(err))
		}

		// Kill the pod (envtest has no kubelet to do it).
		pods := &corev1.PodList{}
		require.NoError(t, k8sClient.List(t.Context(), pods,
			client.InNamespace(em.Namespace), client.MatchingLabels(etcdMirrorAgentLabels(em))))
		for i := range pods.Items {
			require.NoError(t, k8sClient.Delete(t.Context(), &pods.Items[i], client.GracePeriodSeconds(0)))
		}

		// Cleaner failure: finalizer retained, Warning, bounded retry.
		cleaner.setErr(errors.New("target unreachable"))
		res = reconcileMirrorOnce(t, r, em)
		assert.Positive(t, res.RequeueAfter)
		assert.Equal(t, 1, cleaner.callCount())
		assert.Equal(t, 1, rec.countReason("CheckpointCleanupFailed"))
		got := refreshMirror(t, em)
		assert.Contains(t, got.Finalizers, checkpointCleanupFinalizer)

		// Success: the checkpoint target carries the endpoints and the
		// default reserved key; the finalizer is released.
		cleaner.setErr(nil)
		reconcileMirrorOnce(t, r, em)
		require.Equal(t, 2, cleaner.callCount())
		tgt := cleaner.calls[1]
		assert.Equal(t, got.Spec.Target.EndpointList, tgt.Endpoints)
		assert.Nil(t, tgt.TLS)
		assert.Equal(t, "/mirrored/"+mirroragent.DefaultCheckpointKeySuffix, tgt.Key)
		err = k8sClient.Get(t.Context(),
			types.NamespacedName{Namespace: em.Namespace, Name: em.Name}, &ecv1alpha1.EtcdMirror{})
		assert.True(t, apierrors.IsNotFound(err))
	})

	t.Run("explicit checkpoint key is deleted verbatim", func(t *testing.T) {
		sc := &fakeStatusClient{snap: healthySnapshot()}
		cleaner := &fakeCleaner{}
		r, _ := newTestMirrorReconciler(sc, cleaner)
		ns := createTestNamespace(t)
		em := newEnvtestMirror(t, ns, func(em *ecv1alpha1.EtcdMirror) {
			em.Spec.Checkpoint = &ecv1alpha1.EtcdMirrorCheckpointSpec{Key: "/mirrored/\x00custom-cp"}
		})
		reconcileMirrorOnce(t, r, em)
		reconcileMirrorOnce(t, r, em) // Deployment exists, no pods
		require.NoError(t, k8sClient.Delete(t.Context(), refreshMirror(t, em)))
		reconcileMirrorOnce(t, r, em) // deletes Deployment; no pods -> cleaner runs
		require.Equal(t, 1, cleaner.callCount())
		assert.Equal(t, "/mirrored/\x00custom-cp", cleaner.calls[0].Key)
	})

	t.Run("skip annotation is the escape hatch", func(t *testing.T) {
		sc := &fakeStatusClient{snap: healthySnapshot()}
		cleaner := &fakeCleaner{}
		r, rec := newTestMirrorReconciler(sc, cleaner)
		ns := createTestNamespace(t)
		em := newEnvtestMirror(t, ns, nil)
		reconcileMirrorOnce(t, r, em)

		got := refreshMirror(t, em)
		got.Annotations = map[string]string{skipCheckpointCleanupAnnotation: "true"}
		require.NoError(t, k8sClient.Update(t.Context(), got))
		require.NoError(t, k8sClient.Delete(t.Context(), got))

		reconcileMirrorOnce(t, r, em)
		assert.Zero(t, cleaner.callCount())
		assert.Equal(t, 1, rec.countReason("CheckpointCleanupSkipped"))
		// The key is %q-rendered in the note, so the NUL byte appears escaped.
		assert.Contains(t, rec.lastNote("CheckpointCleanupSkipped"), "etcdmirror-checkpoint")
		err := k8sClient.Get(t.Context(),
			types.NamespacedName{Namespace: em.Namespace, Name: em.Name}, &ecv1alpha1.EtcdMirror{})
		assert.True(t, apierrors.IsNotFound(err))
	})
}

func TestEtcdMirrorReconcile_EventsEmittedOnce(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{}
	r, rec := newTestMirrorReconciler(sc, &fakeCleaner{})
	snap := healthySnapshot()
	snap.Phase = mirroragent.PhaseInitialSync
	snap.Compacted = true
	snap.ForcedResyncCount = 1
	snap.LastResyncReason = mirroragent.ResyncReasonClusterIDMismatch
	snap.ScanRestartCount = 2
	snap.LastScanRestartCause = mirroragent.ScanRestartWatchBufferOverflow
	sc.set(snap, nil)
	ns := createTestNamespace(t)
	em := setupActiveMirror(t, r, ns, nil)

	reconcileMirrorOnce(t, r, em)
	reconcileMirrorOnce(t, r, em) // identical snapshot: nothing new
	assert.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorEventForcedResyncStarted))
	assert.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorEventCheckpointInvalidated))
	assert.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorEventInitialSyncCompactionRaced))
	assert.Contains(t, rec.lastNote(ecv1alpha1.EtcdMirrorEventInitialSyncCompactionRaced),
		string(mirroragent.ScanRestartWatchBufferOverflow))

	// The resync completes: one Normal completion event.
	done := *snap
	done.Phase = mirroragent.PhaseSyncing
	done.Compacted = false
	sc.set(&done, nil)
	reconcileMirrorOnce(t, r, em)
	reconcileMirrorOnce(t, r, em)
	assert.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorEventForcedResyncCompleted))

	// A second forced resync emits again.
	again := done
	again.Phase = mirroragent.PhaseInitialSync
	again.Compacted = true
	again.ForcedResyncCount = 2
	again.LastResyncReason = mirroragent.ResyncReasonCompacted
	sc.set(&again, nil)
	reconcileMirrorOnce(t, r, em)
	assert.Equal(t, 2, rec.countReason(ecv1alpha1.EtcdMirrorEventForcedResyncStarted))
	assert.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorEventCheckpointInvalidated),
		"a Compacted-triggered resync is not a checkpoint invalidation")
}

// selfSignedCertPEM builds a throwaway certificate expiring at notAfter.
func selfSignedCertPEM(t *testing.T, notAfter time.Time) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "etcdmirror-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func TestEtcdMirrorReconcile_CertExpiryWarning(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	ns := createTestNamespace(t)

	makeTLSSecret := func(name string, notAfter time.Time) {
		certPEM, keyPEM := selfSignedCertPEM(t, notAfter)
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Data:       map[string][]byte{"tls.crt": certPEM, "tls.key": keyPEM},
		}
		require.NoError(t, k8sClient.Create(t.Context(), secret))
	}

	t.Run("expiring in 7d warns once", func(t *testing.T) {
		sc := &fakeStatusClient{snap: healthySnapshot()}
		r, rec := newTestMirrorReconciler(sc, &fakeCleaner{})
		makeTLSSecret("soon-tls", time.Now().Add(7*24*time.Hour))
		em := setupActiveMirror(t, r, ns, func(em *ecv1alpha1.EtcdMirror) {
			em.Spec.Source.TLS = &ecv1alpha1.EtcdMirrorTLS{
				SecretRef: &corev1.LocalObjectReference{Name: "soon-tls"},
			}
		})
		reconcileMirrorOnce(t, r, em)
		reconcileMirrorOnce(t, r, em) // damped: still one
		require.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorEventCertificateExpiringSoon))
		note := rec.lastNote(ecv1alpha1.EtcdMirrorEventCertificateExpiringSoon)
		assert.Contains(t, note, "source")
		assert.Contains(t, note, "tls.crt")
	})

	t.Run("expiring in 60d does not warn", func(t *testing.T) {
		sc := &fakeStatusClient{snap: healthySnapshot()}
		r, rec := newTestMirrorReconciler(sc, &fakeCleaner{})
		makeTLSSecret("later-tls", time.Now().Add(60*24*time.Hour))
		em := setupActiveMirror(t, r, ns, func(em *ecv1alpha1.EtcdMirror) {
			em.Spec.Source.TLS = &ecv1alpha1.EtcdMirrorTLS{
				SecretRef: &corev1.LocalObjectReference{Name: "later-tls"},
			}
		})
		reconcileMirrorOnce(t, r, em)
		assert.Zero(t, rec.countReason(ecv1alpha1.EtcdMirrorEventCertificateExpiringSoon))
	})
}

func TestEtcdMirrorReconcile_InsecureSkipVerifyWarning(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, rec := newTestMirrorReconciler(sc, &fakeCleaner{})
	ns := createTestNamespace(t)
	em := setupActiveMirror(t, r, ns, func(em *ecv1alpha1.EtcdMirror) {
		em.Spec.Target.TLS = &ecv1alpha1.EtcdMirrorTLS{
			InsecureSkipVerify:                true,
			InsecureSkipVerifyAcknowledgeRisk: true,
		}
	})
	reconcileMirrorOnce(t, r, em)
	reconcileMirrorOnce(t, r, em)
	require.Equal(t, 1, rec.countReason(ecv1alpha1.EtcdMirrorEventInsecureSkipVerifyEnabled))
	assert.Contains(t, rec.lastNote(ecv1alpha1.EtcdMirrorEventInsecureSkipVerifyEnabled), "target")
}

func TestEtcdMirrorReconcile_ServiceRefPortResolution(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	ns := createTestNamespace(t)

	t.Run("named port resolves to a numeric dial port", func(t *testing.T) {
		sc := &fakeStatusClient{snap: healthySnapshot()}
		r, _ := newTestMirrorReconciler(sc, &fakeCleaner{})
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "etcd-target-client", Namespace: ns},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{Name: "client", Port: 2379}},
			},
		}
		require.NoError(t, k8sClient.Create(t.Context(), svc))

		em := newEnvtestMirror(t, ns, func(em *ecv1alpha1.EtcdMirror) {
			em.Spec.Target.ServiceRef = &ecv1alpha1.EtcdMirrorServiceRef{Name: "etcd-target-client"}
		})
		reconcileMirrorOnce(t, r, em)
		reconcileMirrorOnce(t, r, em)
		dep := getAgentDeployment(t, em)
		assert.Contains(t, dep.Spec.Template.Spec.Containers[0].Args,
			"--target-endpoints=etcd-target-client."+ns+".svc.cluster.local:2379")
	})

	t.Run("missing Service parks the CR in Pending", func(t *testing.T) {
		sc := &fakeStatusClient{snap: healthySnapshot()}
		r, rec := newTestMirrorReconciler(sc, &fakeCleaner{})
		em := newEnvtestMirror(t, ns, func(em *ecv1alpha1.EtcdMirror) {
			em.Spec.Target.ServiceRef = &ecv1alpha1.EtcdMirrorServiceRef{Name: "absent-service"}
		})
		reconcileMirrorOnce(t, r, em)
		res := reconcileMirrorOnce(t, r, em)
		assert.Positive(t, res.RequeueAfter)
		got := refreshMirror(t, em)
		assert.Equal(t, ecv1alpha1.EtcdMirrorPhasePending, got.Status.Phase)
		requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonServiceNotFound)
		assert.Equal(t, 1, rec.countReason(reasonServiceNotFound))
	})
}

func TestEtcdMirrorReconcile_AgentImageNotConfigured(t *testing.T) {
	if k8sClient == nil {
		t.Skip("envtest apiserver not available")
	}
	sc := &fakeStatusClient{snap: healthySnapshot()}
	r, rec := newTestMirrorReconciler(sc, &fakeCleaner{})
	r.AgentImage = ""
	ns := createTestNamespace(t)
	em := newEnvtestMirror(t, ns, nil)
	reconcileMirrorOnce(t, r, em)
	res := reconcileMirrorOnce(t, r, em)
	assert.Positive(t, res.RequeueAfter)

	got := refreshMirror(t, em)
	assert.Equal(t, ecv1alpha1.EtcdMirrorPhasePending, got.Status.Phase)
	requireCond(t, got, ecv1alpha1.EtcdMirrorConditionAvailable, metav1.ConditionFalse, reasonAgentImageNotConfigured)
	assert.Equal(t, 1, rec.countReason(reasonAgentImageNotConfigured))

	err := k8sClient.Get(t.Context(),
		types.NamespacedName{Namespace: ns, Name: deploymentNameForEtcdMirror(em)}, &appsv1.Deployment{})
	assert.True(t, apierrors.IsNotFound(err), "no Deployment may be created without an agent image")
}
