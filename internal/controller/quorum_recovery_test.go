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

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// member builds an EpHealth entry for detection tests. leader==0 means the
// member reports no leader.
func member(id uint64, healthy, learner bool, leader uint64) etcdutils.EpHealth {
	return etcdutils.EpHealth{
		Ep:     "http://etcd-x:2379",
		Health: healthy,
		Status: &clientv3.StatusResponse{
			Header:    &etcdserverpb.ResponseHeader{MemberId: id},
			Leader:    leader,
			IsLearner: learner,
		},
	}
}

// TestAssessQuorum exercises the pure detection decision across the cases that
// distinguish true quorum loss from transient or recoverable-by-normal-means
// degradation. This is the safety-critical guard: a false positive triggers a
// disruptive rebuild, so the table leans heavily on negative cases.
func TestAssessQuorum(t *testing.T) {
	cases := []struct {
		name          string
		desiredSize   int
		health        []etcdutils.EpHealth
		memberListErr error
		wantLost      bool
	}{
		{
			name:        "3-node all healthy with leader => not lost",
			desiredSize: 3,
			health:      []etcdutils.EpHealth{member(1, true, false, 1), member(2, true, false, 1), member(3, true, false, 1)},
			wantLost:    false,
		},
		{
			name:        "3-node single member down, quorum intact => not lost",
			desiredSize: 3,
			health:      []etcdutils.EpHealth{member(1, true, false, 1), member(2, true, false, 1), member(3, false, false, 0)},
			wantLost:    false,
		},
		{
			name:        "3-node two members down, no leader => quorum LOST",
			desiredSize: 3,
			health:      []etcdutils.EpHealth{member(1, true, false, 0), member(2, false, false, 0), member(3, false, false, 0)},
			wantLost:    true,
		},
		{
			name:          "3-node total outage, member list errored => quorum LOST",
			desiredSize:   3,
			health:        nil,
			memberListErr: errors.New("context deadline exceeded"),
			wantLost:      true,
		},
		{
			name:        "3-node leaderless but majority reachable => electable, not lost",
			desiredSize: 3,
			health:      []etcdutils.EpHealth{member(1, true, false, 0), member(2, true, false, 0), member(3, false, false, 0)},
			wantLost:    false,
		},
		{
			name:        "1-node down => never auto-recover (no survivor path)",
			desiredSize: 1,
			health:      []etcdutils.EpHealth{member(1, false, false, 0)},
			wantLost:    false,
		},
		{
			name:        "1-node total outage with error => still never recover",
			desiredSize: 1, health: nil, memberListErr: errors.New("down"),
			wantLost: false,
		},
		{
			name:        "5-node three down, no leader => quorum LOST",
			desiredSize: 5,
			health: []etcdutils.EpHealth{
				member(1, true, false, 0), member(2, true, false, 0),
				member(3, false, false, 0), member(4, false, false, 0), member(5, false, false, 0),
			},
			wantLost: true,
		},
		{
			name:        "5-node two down => quorum intact, not lost",
			desiredSize: 5,
			health: []etcdutils.EpHealth{
				member(1, true, false, 1), member(2, true, false, 1), member(3, true, false, 1),
				member(4, false, false, 0), member(5, false, false, 0),
			},
			wantLost: false,
		},
		{
			name:        "stale leader still visible on one node => treat as has-quorum, not lost",
			desiredSize: 3,
			health:      []etcdutils.EpHealth{member(1, true, false, 1), member(2, false, false, 0), member(3, false, false, 0)},
			wantLost:    false,
		},
		{
			// A learner answers health probes but cannot vote, so a surviving
			// voter + healthy learner is NOT a quorum for a 3-node cluster.
			name:        "3-node survivor voter + healthy learner, no leader => quorum LOST",
			desiredSize: 3,
			health:      []etcdutils.EpHealth{member(1, true, false, 0), member(2, true, true, 0), member(3, false, false, 0)},
			wantLost:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := assessQuorum(tc.desiredSize, tc.health, tc.memberListErr)
			assert.Equal(t, tc.wantLost, got.lost, "reason: %s", got.reason)
			assert.NotEmpty(t, got.reason)
		})
	}
}

func newTestReconciler() *EtcdClusterReconciler {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	return &EtcdClusterReconciler{Client: cl, Scheme: scheme}
}

func objKey(name, namespace string) client.ObjectKey {
	return client.ObjectKey{Name: name, Namespace: namespace}
}

func threeNodeCluster() *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default"},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
	}
}

func threeReplicaSTS() *appsv1.StatefulSet {
	three := int32(3)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &three,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name: "etcd",
						Args: []string{"--name=$(POD_NAME)", "--listen-peer-urls=http://0.0.0.0:2380"},
					}},
				},
			},
		},
	}
}

// quorumLostHealth is the observation for a 3-node cluster that has lost quorum.
func quorumLostHealth() []etcdutils.EpHealth {
	return []etcdutils.EpHealth{member(1, true, false, 0), member(2, false, false, 0), member(3, false, false, 0)}
}

// survivorPod builds the ordinal-0 pod that the rebuild step requires to exist
// before it arms --force-new-cluster. Tests that drive the Rebuilding phase must
// create it so the survivor-presence gate passes.
func survivorPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd-0", Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "etcd"}}},
	}
}

// TestMaybeRecoverQuorum_GracePeriod verifies the detection state machine waits
// out the grace window and only commits to recovery once quorum loss is
// sustained — and that it cancels cleanly if quorum returns first.
func TestMaybeRecoverQuorum_GracePeriod(t *testing.T) {
	t.Run("transient loss within grace period does not recover", func(t *testing.T) {
		r := newTestReconciler()
		ec := threeNodeCluster()
		s := &reconcileState{cluster: ec, sts: threeReplicaSTS()}

		// First observation: enters Detecting, requeues, does NOT recover.
		handled, _, err := r.maybeRecoverQuorum(context.Background(), s, quorumLostHealth(), nil)
		require.NoError(t, err)
		assert.True(t, handled)
		require.NotNil(t, ec.Status.Recovery)
		assert.Equal(t, ecv1alpha1.RecoveryPhaseDetecting, ec.Status.Recovery.Phase)

		// Quorum returns (leader visible) before grace elapses => detection cancelled.
		healthy := []etcdutils.EpHealth{member(1, true, false, 1), member(2, true, false, 1), member(3, true, false, 1)}
		handled, _, err = r.maybeRecoverQuorum(context.Background(), s, healthy, nil)
		require.NoError(t, err)
		assert.False(t, handled)
		assert.Nil(t, ec.Status.Recovery, "detection should be cleared when quorum returns")
		assert.Nil(t, meta.FindStatusCondition(ec.Status.Conditions, ConditionRecovering))
	})

	t.Run("sustained loss past grace period transitions to Rebuilding", func(t *testing.T) {
		sts := threeReplicaSTS()
		r := newTestReconciler()
		ec := threeNodeCluster()
		require.NoError(t, r.Create(context.Background(), sts))
		require.NoError(t, r.Create(context.Background(), survivorPod()))
		s := &reconcileState{cluster: ec, sts: sts}

		// Enter Detecting.
		_, _, err := r.maybeRecoverQuorum(context.Background(), s, quorumLostHealth(), nil)
		require.NoError(t, err)
		require.NotNil(t, ec.Status.Recovery)

		// Backdate DetectedTime so the grace period is considered elapsed.
		past := metav1.NewTime(time.Now().Add(-2 * quorumLossGracePeriod))
		ec.Status.Recovery.DetectedTime = &past

		// Next observation still shows loss => commit to recovery (Rebuilding) and
		// the rebuild step injects --force-new-cluster + scales to 1.
		handled, _, err := r.maybeRecoverQuorum(context.Background(), s, quorumLostHealth(), nil)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, ecv1alpha1.RecoveryPhaseRebuilding, ec.Status.Recovery.Phase)

		// The StatefulSet must now be scaled to a single survivor with the flag.
		var got appsv1.StatefulSet
		require.NoError(t, r.Get(context.Background(), objKey("etcd", "default"), &got))
		require.NotNil(t, got.Spec.Replicas)
		assert.Equal(t, int32(1), *got.Spec.Replicas)
		assert.True(t, stsHasForceNewCluster(&got))
		assert.Contains(t, got.Spec.Template.Spec.Containers[0].Args, forceNewClusterArg)

		// The state ConfigMap must be realigned to a single-member "new" bootstrap
		// so pod-0's environment is consistent with --force-new-cluster.
		var cm corev1.ConfigMap
		require.NoError(t, r.Get(context.Background(), objKey("etcd-state", "default"), &cm))
		assert.Equal(t, "new", cm.Data["ETCD_INITIAL_CLUSTER_STATE"])
		assert.NotContains(t, cm.Data["ETCD_INITIAL_CLUSTER"], ",", "single-member initial cluster has no comma")

		cond := meta.FindStatusCondition(ec.Status.Conditions, ConditionRecovering)
		require.NotNil(t, cond)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
	})
}

// TestRecoveryRebuild_FlagRemovedWhenSurvivorHealthy is covered indirectly via
// patch helpers; here we unit-test the idempotent patch toggling directly.
func TestPatchStatefulSetForForceNewCluster(t *testing.T) {
	sts := threeReplicaSTS()
	r := newTestReconciler()
	require.NoError(t, r.Create(context.Background(), sts))

	// Enable twice — must be idempotent (single flag, replicas==1).
	require.NoError(t, r.patchStatefulSetForForceNewCluster(context.Background(), sts, true))
	require.NoError(t, r.patchStatefulSetForForceNewCluster(context.Background(), sts, true))

	var got appsv1.StatefulSet
	require.NoError(t, r.Get(context.Background(), objKey("etcd", "default"), &got))
	assert.Equal(t, int32(1), *got.Spec.Replicas)
	assert.True(t, stsHasForceNewCluster(&got))
	count := 0
	for _, a := range got.Spec.Template.Spec.Containers[0].Args {
		if a == forceNewClusterArg {
			count++
		}
	}
	assert.Equal(t, 1, count, "flag must appear exactly once")

	// Disable removes the flag and the marker annotation.
	require.NoError(t, r.patchStatefulSetForForceNewCluster(context.Background(), &got, false))
	var got2 appsv1.StatefulSet
	require.NoError(t, r.Get(context.Background(), objKey("etcd", "default"), &got2))
	assert.False(t, stsHasForceNewCluster(&got2))
	assert.NotContains(t, got2.Spec.Template.Spec.Containers[0].Args, forceNewClusterArg)
}

// TestRecoveryScaleOut_CompletesAtDesiredSize verifies the scale-out phase hands
// back to normal reconciliation while degraded and declares completion once the
// desired number of healthy voting members with a leader is observed.
func TestRecoveryScaleOut_CompletesAtDesiredSize(t *testing.T) {
	r := newTestReconciler()
	ec := threeNodeCluster()
	now := metav1.Now()
	ec.Status.Recovery = &ecv1alpha1.RecoveryStatus{
		Phase:        ecv1alpha1.RecoveryPhaseScalingOut,
		DetectedTime: &now,
	}
	sts := threeReplicaSTS()

	t.Run("still degraded delegates to normal reconcile, stays ScalingOut", func(t *testing.T) {
		s := &reconcileState{cluster: ec, sts: sts,
			memberListResp: memberListResp(1),                               // member list is usable this loop
			memberHealth:   []etcdutils.EpHealth{member(1, true, false, 1)}} // only survivor
		handled, _, err := r.maybeRecoverQuorum(context.Background(), s, s.memberHealth, nil)
		require.NoError(t, err)
		assert.False(t, handled, "scale-out must delegate to reconcileClusterState")
		assert.Equal(t, ecv1alpha1.RecoveryPhaseScalingOut, ec.Status.Recovery.Phase)
	})

	t.Run("unusable member list holds instead of delegating (no scale-down)", func(t *testing.T) {
		// memberListResp nil and/or a member-list error must NOT delegate to
		// reconcileClusterState, which would read memberCnt=0 and scale the STS
		// down, reversing the rebuild. The state machine keeps the loop and requeues.
		s := &reconcileState{cluster: ec, sts: sts,
			memberHealth: []etcdutils.EpHealth{member(1, true, false, 1)}}
		handled, requeue, err := r.maybeRecoverQuorum(context.Background(), s, s.memberHealth, errors.New("context deadline exceeded"))
		require.NoError(t, err)
		assert.True(t, handled, "must hold ownership when member list is unusable")
		assert.Greater(t, requeue, time.Duration(0), "should requeue to retry the observation")
		assert.Equal(t, ecv1alpha1.RecoveryPhaseScalingOut, ec.Status.Recovery.Phase)
	})

	t.Run("desired size reached marks Completed", func(t *testing.T) {
		full := []etcdutils.EpHealth{member(1, true, false, 1), member(2, true, false, 1), member(3, true, false, 1)}
		s := &reconcileState{cluster: ec, sts: sts, memberListResp: memberListResp(3), memberHealth: full}
		handled, _, err := r.maybeRecoverQuorum(context.Background(), s, full, nil)
		require.NoError(t, err)
		assert.False(t, handled)
		require.NotNil(t, ec.Status.Recovery)
		assert.Equal(t, ecv1alpha1.RecoveryPhaseCompleted, ec.Status.Recovery.Phase)
		assert.False(t, recoveryActive(ec), "completed recovery is not active")

		cond := meta.FindStatusCondition(ec.Status.Conditions, ConditionRecovering)
		require.NotNil(t, cond)
		assert.Equal(t, metav1.ConditionFalse, cond.Status)
		assert.Equal(t, "RecoveryCompleted", cond.Reason)
	})
}

// memberListResp builds a minimal MemberListResponse with n members, enough for
// recoveryScaleOut's "is the member list usable this loop" guard.
func memberListResp(n int) *clientv3.MemberListResponse {
	members := make([]*etcdserverpb.Member, 0, n)
	for i := 0; i < n; i++ {
		members = append(members, &etcdserverpb.Member{ID: uint64(i + 1)})
	}
	return &clientv3.MemberListResponse{Members: members}
}

func TestCountHealthyVoting(t *testing.T) {
	health := []etcdutils.EpHealth{
		member(1, true, false, 1),  // healthy voter + leader
		member(2, true, false, 1),  // healthy voter
		member(3, true, true, 0),   // healthy but learner => excluded
		member(4, false, false, 0), // unhealthy => excluded
	}
	healthy, leader := countHealthyVoting(health)
	assert.Equal(t, 2, healthy)
	assert.True(t, leader)
}

// TestRecoveryRebuild_RemovesFlagAndAdvances drives the dangerous Rebuilding leg
// to completion: once the single survivor reports healthy and self-leader, the
// rebuild MUST remove --force-new-cluster (so a future pod-0 restart can't re-fork
// the cluster) and advance to ScalingOut. The survivor-health probe is injected
// via the clusterHealthFn seam so no live etcd is required. A FakeRecorder proves
// the scale-out transition emits an observable event.
func TestRecoveryRebuild_RemovesFlagAndAdvances(t *testing.T) {
	r := newTestReconciler()
	rec := events.NewFakeRecorder(16)
	r.Recorder = rec

	ec := threeNodeCluster()
	sts := threeReplicaSTS()
	require.NoError(t, r.Create(context.Background(), sts))
	require.NoError(t, r.Create(context.Background(), survivorPod()))

	// Enter Rebuilding with the grace period already elapsed (sustained loss).
	_, _, err := r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: sts}, quorumLostHealth(), nil)
	require.NoError(t, err)
	require.NotNil(t, ec.Status.Recovery)
	past := metav1.NewTime(time.Now().Add(-2 * quorumLossGracePeriod))
	ec.Status.Recovery.DetectedTime = &past
	_, _, err = r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: sts}, quorumLostHealth(), nil)
	require.NoError(t, err)
	require.Equal(t, ecv1alpha1.RecoveryPhaseRebuilding, ec.Status.Recovery.Phase)

	// Re-read the patched STS (flag injected, replicas==1).
	var rebuilding appsv1.StatefulSet
	require.NoError(t, r.Get(context.Background(), objKey("etcd", "default"), &rebuilding))
	require.True(t, stsHasForceNewCluster(&rebuilding))

	// First post-injection loop: survivor not yet healthy => stay in Rebuilding,
	// flag still present.
	r.clusterHealthFn = func(eps []string) ([]etcdutils.EpHealth, error) {
		return []etcdutils.EpHealth{member(1, false, false, 0)}, nil
	}
	handled, _, err := r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: &rebuilding}, nil, nil)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, ecv1alpha1.RecoveryPhaseRebuilding, ec.Status.Recovery.Phase)

	// Next loop: survivor is healthy AND its own leader => drop the flag and
	// advance to ScalingOut.
	r.clusterHealthFn = func(eps []string) ([]etcdutils.EpHealth, error) {
		return []etcdutils.EpHealth{member(1, true, false, 1)}, nil // leader==self id 1
	}
	handled, _, err = r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: &rebuilding}, nil, nil)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, ecv1alpha1.RecoveryPhaseScalingOut, ec.Status.Recovery.Phase)

	// The flag and marker annotation MUST be gone from the live StatefulSet.
	var afterRebuild appsv1.StatefulSet
	require.NoError(t, r.Get(context.Background(), objKey("etcd", "default"), &afterRebuild))
	assert.False(t, stsHasForceNewCluster(&afterRebuild), "marker annotation must be cleared after rebuild")
	assert.NotContains(t, afterRebuild.Spec.Template.Spec.Containers[0].Args, forceNewClusterArg,
		"--force-new-cluster must be removed so pod-0 can't re-fork on restart")

	// At least one event must mention the scale-out transition.
	sawScaleOut := false
	for drain := true; drain; {
		select {
		case e := <-rec.Events:
			if strings.Contains(e, EventReasonRecoveryScalingOut) {
				sawScaleOut = true
			}
		default:
			drain = false
		}
	}
	assert.True(t, sawScaleOut, "expected a RecoveryScalingOut event when the rebuild completes")
}

func TestRecoveryActive(t *testing.T) {
	ec := threeNodeCluster()
	assert.False(t, recoveryActive(ec))

	ec.Status.Recovery = &ecv1alpha1.RecoveryStatus{Phase: ecv1alpha1.RecoveryPhaseRebuilding}
	assert.True(t, recoveryActive(ec))

	ec.Status.Recovery.Phase = ecv1alpha1.RecoveryPhaseCompleted
	assert.False(t, recoveryActive(ec))
}

// survivorHealth builds the single-survivor health observation reported once the
// force-new-cluster bootstrap is up: healthy, self-leader, at a given revision.
func survivorHealth(memberID uint64, revision int64, raftIndex uint64) []etcdutils.EpHealth {
	return []etcdutils.EpHealth{{
		Ep:     "http://etcd-0:2379",
		Health: true,
		Status: &clientv3.StatusResponse{
			Header:    &etcdserverpb.ResponseHeader{MemberId: memberID, Revision: revision},
			Leader:    memberID, // self-leader
			RaftIndex: raftIndex,
		},
	}}
}

// TestRecoveryRebuild_RecordsPossibleDataLoss is the core data-loss-accounting
// test. When a rebuild from the survivor completes, the operator MUST surface the
// possible loss on every channel: the DataLoss status accounting (with the
// survivor's id + retained revision), the DataLossPossible condition set True,
// and a Warning Event. The accounting must be captured exactly once and must NOT
// be overwritten by a later loop observing a higher (post-recovery-write) revision.
func TestRecoveryRebuild_RecordsPossibleDataLoss(t *testing.T) {
	r := newTestReconciler()
	rec := events.NewFakeRecorder(32)
	r.Recorder = rec

	ec := threeNodeCluster()
	sts := threeReplicaSTS()
	require.NoError(t, r.Create(context.Background(), sts))
	require.NoError(t, r.Create(context.Background(), survivorPod()))

	// Commit to recovery (grace already elapsed) => Rebuilding, flag injected.
	_, _, err := r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: sts}, quorumLostHealth(), nil)
	require.NoError(t, err)
	require.NotNil(t, ec.Status.Recovery)
	past := metav1.NewTime(time.Now().Add(-2 * quorumLossGracePeriod))
	ec.Status.Recovery.DetectedTime = &past
	_, _, err = r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: sts}, quorumLostHealth(), nil)
	require.NoError(t, err)
	require.Equal(t, ecv1alpha1.RecoveryPhaseRebuilding, ec.Status.Recovery.Phase)

	// Committing to a destructive rebuild bumps the durable attempt counter.
	assert.Equal(t, int32(1), ec.Status.Recovery.Attempts, "first commit to rebuild => attempt #1")

	var rebuilding appsv1.StatefulSet
	require.NoError(t, r.Get(context.Background(), objKey("etcd", "default"), &rebuilding))

	// Survivor comes up healthy and self-leader at revision 4242 / raftIndex 9000.
	r.clusterHealthFn = func(eps []string) ([]etcdutils.EpHealth, error) {
		return survivorHealth(0xabc, 4242, 9000), nil
	}
	handled, _, err := r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: &rebuilding}, nil, nil)
	require.NoError(t, err)
	assert.True(t, handled)
	require.Equal(t, ecv1alpha1.RecoveryPhaseScalingOut, ec.Status.Recovery.Phase)

	// (1) DataLoss accounting captured with survivor id + retained revision.
	dl := ec.Status.Recovery.DataLoss
	require.NotNil(t, dl, "DataLoss accounting must be recorded on rebuild completion")
	assert.Equal(t, "abc", dl.SurvivorMemberID, "member id is hex-encoded")
	assert.Equal(t, int64(4242), dl.SurvivorRevision)
	assert.Equal(t, uint64(9000), dl.RaftIndex)
	require.NotNil(t, dl.RecoveredTime)
	assert.Contains(t, dl.Message, "possible data loss")
	assert.Contains(t, dl.Message, "revision 4242")

	// (2) DataLossPossible condition set True (audit marker, left True).
	cond := meta.FindStatusCondition(ec.Status.Conditions, ConditionDataLossPossible)
	require.NotNil(t, cond, "DataLossPossible condition must be set")
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "RebuiltFromSurvivor", cond.Reason)

	// (3) A Warning Event surfaced the loss loudly.
	sawDataLoss := false
	for drain := true; drain; {
		select {
		case e := <-rec.Events:
			if strings.Contains(e, EventReasonPossibleDataLoss) && strings.Contains(e, "Warning") {
				sawDataLoss = true
			}
		default:
			drain = false
		}
	}
	assert.True(t, sawDataLoss, "expected a Warning PossibleDataLoss event on rebuild completion")

	// Idempotency: a subsequent rebuild loop observing a HIGHER revision (a write
	// landed post-recovery) must NOT clobber the originally-retained revision. We
	// re-enter Rebuilding to exercise the "DataLoss already set" guard directly.
	ec.Status.Recovery.Phase = ecv1alpha1.RecoveryPhaseRebuilding
	// Re-arm the flag annotation so recoveryRebuild takes the "survivor healthy" leg.
	require.NoError(t, r.patchStatefulSetForForceNewCluster(context.Background(), &rebuilding, true))
	r.clusterHealthFn = func(eps []string) ([]etcdutils.EpHealth, error) {
		return survivorHealth(0xabc, 5000, 9999), nil
	}
	_, _, err = r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: &rebuilding}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(4242), ec.Status.Recovery.DataLoss.SurvivorRevision,
		"data-loss revision must be captured once and not overwritten by later loops")
}

// TestRecoveryRebuild_HoldsWhenSurvivorPodMissing verifies the survivor-presence
// safety gate: the operator must NOT arm the irreversible --force-new-cluster flag
// while the survivor pod does not exist. It holds (handled, no flag) and requeues.
func TestRecoveryRebuild_HoldsWhenSurvivorPodMissing(t *testing.T) {
	r := newTestReconciler()
	ec := threeNodeCluster()
	sts := threeReplicaSTS()
	require.NoError(t, r.Create(context.Background(), sts))
	// Deliberately do NOT create the survivor pod.

	ec.Status.Recovery = &ecv1alpha1.RecoveryStatus{
		Phase:        ecv1alpha1.RecoveryPhaseRebuilding,
		DetectedTime: &metav1.Time{Time: time.Now()},
	}

	handled, requeue, err := r.maybeRecoverQuorum(context.Background(), &reconcileState{cluster: ec, sts: sts}, nil, nil)
	require.NoError(t, err)
	assert.True(t, handled, "must keep ownership while waiting for the survivor pod")
	assert.Greater(t, requeue, time.Duration(0))

	var got appsv1.StatefulSet
	require.NoError(t, r.Get(context.Background(), objKey("etcd", "default"), &got))
	assert.False(t, stsHasForceNewCluster(&got),
		"--force-new-cluster must NOT be armed until the survivor pod exists")
}
