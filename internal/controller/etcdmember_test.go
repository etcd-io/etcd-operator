// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNextOrdinal(t *testing.T) {
	tests := []struct {
		name string
		list []int
		want int
	}{
		{
			name: "Missing pod-1 (gap in the middle)",
			list: []int{0, 2},
			want: 1,
		},
		{
			name: "Missing pod-0 (gap at the start)",
			list: []int{1, 2},
			want: 0,
		},
		{
			name: "Contiguous, no gap: next is max+1",
			list: []int{0, 1},
			want: 2,
		},
		{
			name: "Out-of-bounds numbers, lowest gap still wins",
			list: []int{0, 3, 9},
			want: 1,
		},
		{
			name: "Empty cluster state",
			list: []int{},
			want: 0,
		},
		{
			name: "Input list arrives completely unsorted",
			list: []int{2, 0},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextOrdinal(tt.list)
			assert.Equal(t, tt.want, got, "nextOrdinal(%v) should return %d", tt.list, tt.want)
		})
	}
}

func TestAllReady(t *testing.T) {
	ready := ecv1alpha1.EtcdMember{Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady}}
	pending := ecv1alpha1.EtcdMember{Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberPending}}

	assert.True(t, allReady(nil), "zero members should be vacuously ready")
	assert.True(t, allReady([]ecv1alpha1.EtcdMember{ready, ready}))
	assert.False(t, allReady([]ecv1alpha1.EtcdMember{ready, pending}))
}

func TestListOwnedMembers(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster", Namespace: "default", UID: "cluster-uid"},
	}
	foreign := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "other-cluster", Namespace: ec.Namespace, UID: "foreign-uid"},
	}
	makeMember := func(cluster *ecv1alpha1.EtcdCluster, ordinal int, labelCluster string) ecv1alpha1.EtcdMember {
		return ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      etcdMemberName(labelCluster, ordinal),
				Namespace: cluster.Namespace,
				Labels:    map[string]string{clusterNameLabel: labelCluster},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       cluster.Name,
					UID:        cluster.UID,
					Controller: new(true),
				}},
			},
			Spec: ecv1alpha1.EtcdMemberSpec{ClusterName: cluster.Name, Ordinal: ordinal},
		}
	}
	member0 := makeMember(ec, 0, ec.Name)
	member2 := makeMember(ec, 2, ec.Name)
	otherMember := makeMember(foreign, 0, foreign.Name)
	// Carries this cluster's selected label but is controlled by a foreign
	// cluster: the selector lets it through, the ownership check must not.
	spoofed := makeMember(foreign, 1, ec.Name)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(ec, foreign, &member0, &member2, &otherMember, &spoofed).
		Build()

	members, err := listOwnedMembers(t.Context(), fakeClient, ec)
	require.NoError(t, err)
	require.Len(t, members, 2)
	assert.Equal(t, "my-cluster-0", members[0].Name)
	assert.Equal(t, "my-cluster-2", members[1].Name)
}

func TestPickMemberToUpgrade(t *testing.T) {
	// member builds an EtcdMember. isLeader is intentionally NOT set on
	// Status — leadership is now determined from the live healthInfo map.
	member := func(ordinal int, version string) ecv1alpha1.EtcdMember {
		return ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: etcdMemberName("etcd", ordinal)},
			Spec:       ecv1alpha1.EtcdMemberSpec{Ordinal: ordinal, Version: version},
			Status:     ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady},
		}
	}

	// epHealth builds an EpHealth entry that marks a member as leader or not
	// by setting Header.MemberId == Leader (leader) or a different value (follower).
	epHealth := func(memberID uint64, leaderID uint64) etcdutils.EpHealth {
		return etcdutils.EpHealth{
			Health: true,
			Status: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{MemberId: memberID},
				Leader: leaderID,
			},
		}
	}

	tests := []struct {
		name          string
		members       []ecv1alpha1.EtcdMember
		healthInfo    map[string]etcdutils.EpHealth
		targetVersion string
		wantName      string // "" means expect nil
	}{
		{
			name:          "all members already at target — returns nil",
			members:       []ecv1alpha1.EtcdMember{member(0, "v3.5.33"), member(1, "v3.5.33")},
			healthInfo:    nil,
			targetVersion: "v3.5.33",
			wantName:      "",
		},
		{
			name:          "single member on old version — picked",
			members:       []ecv1alpha1.EtcdMember{member(0, "v3.5.32")},
			healthInfo:    nil,
			targetVersion: "v3.5.33",
			wantName:      "etcd-0",
		},
		{
			name: "prefers highest ordinal non-leader first",
			members: []ecv1alpha1.EtcdMember{
				member(0, "v3.5.32"),
				member(1, "v3.5.32"),
				member(2, "v3.5.32"),
			},
			healthInfo:    nil, // no leader known — all treated as non-leader
			targetVersion: "v3.5.33",
			wantName:      "etcd-2",
		},
		{
			name: "skips leader from live health, picks highest non-leader",
			members: []ecv1alpha1.EtcdMember{
				member(0, "v3.5.32"),
				member(1, "v3.5.32"),
				member(2, "v3.5.32"), // leader according to live health
			},
			healthInfo: map[string]etcdutils.EpHealth{
				"etcd-0": epHealth(10, 30), // follower (id=10, leader=30)
				"etcd-1": epHealth(20, 30), // follower (id=20, leader=30)
				"etcd-2": epHealth(30, 30), // leader  (id=30, leader=30)
			},
			targetVersion: "v3.5.33",
			wantName:      "etcd-1",
		},
		{
			name: "only the leader needs upgrading — picks leader as last resort",
			members: []ecv1alpha1.EtcdMember{
				member(0, "v3.5.33"),
				member(1, "v3.5.33"),
				member(2, "v3.5.32"), // leader, still on old version
			},
			healthInfo: map[string]etcdutils.EpHealth{
				"etcd-0": epHealth(10, 30),
				"etcd-1": epHealth(20, 30),
				"etcd-2": epHealth(30, 30), // leader
			},
			targetVersion: "v3.5.33",
			wantName:      "etcd-2",
		},
		{
			name: "nil health entry Status — treated as non-leader",
			members: []ecv1alpha1.EtcdMember{
				member(0, "v3.5.32"),
				member(1, "v3.5.32"),
				member(2, "v3.5.32"),
			},
			healthInfo: map[string]etcdutils.EpHealth{
				"etcd-2": {Health: false, Status: nil}, // status unavailable
			},
			targetVersion: "v3.5.33",
			wantName:      "etcd-2", // not deferred: nil Status means leader unknown, findLeaderName returns ""
		},
		{
			name: "skips members already at target version",
			members: []ecv1alpha1.EtcdMember{
				member(0, "v3.5.32"),
				member(1, "v3.5.33"), // already upgraded
				member(2, "v3.5.33"), // already upgraded
			},
			healthInfo:    nil,
			targetVersion: "v3.5.33",
			wantName:      "etcd-0",
		},
		{
			name:          "empty member list — returns nil",
			members:       []ecv1alpha1.EtcdMember{},
			healthInfo:    nil,
			targetVersion: "v3.5.33",
			wantName:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pickMemberToUpgrade(tt.members, tt.healthInfo, tt.targetVersion)
			if tt.wantName == "" {
				assert.Nil(t, got)
			} else {
				require.NotNil(t, got)
				assert.Equal(t, tt.wantName, got.Name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Terminating cleanup (§4.6): cleanupEtcdMember / removeEtcNode /
// cleanupMemberResources
// ---------------------------------------------------------------------------

// leaveTestCluster is the fixture cluster for the leave tests: three members
// with per-member (ReadWriteOnce) storage.
func leaveTestCluster() *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "1"},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    3,
			Version: "3.5.17",
			StorageSpec: &ecv1alpha1.StorageSpec{
				AccessModes:       corev1.ReadWriteOnce,
				VolumeSizeRequest: resource.MustParse("1Gi"),
			},
		},
	}
}

func leaveTestMember(ordinal int) *ecv1alpha1.EtcdMember {
	return &ecv1alpha1.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: etcdMemberName("etcd", ordinal), Namespace: "default"},
		Spec:       ecv1alpha1.EtcdMemberSpec{ClusterName: "etcd", Ordinal: ordinal, Version: "3.5.17"},
	}
}

func leaveTestPod(ordinal int) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: memberPodName("etcd", ordinal), Namespace: "default"}}
}

func leaveTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))
	return scheme
}

func TestCleanupMemberResources(t *testing.T) {
	t.Run("Deletes the Pod and PVC together", func(t *testing.T) {
		scheme := leaveTestScheme(t)
		ctx := t.Context()
		ec := leaveTestCluster()
		member := leaveTestMember(2)
		pod := leaveTestPod(2)
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcNameForMember(pod.Name), Namespace: ec.Namespace},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, member, pod, pvc).Build()
		state := &reconcileState{cluster: ec, pods: []*corev1.Pod{pod}}

		require.NoError(t, cleanupMemberResources(ctx, fakeClient, state, member))
		assert.Error(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}),
			"Pod found in the reconcile snapshot gets deleted")
		assert.Error(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}),
			"PVC is deleted by its deterministic name; pvc-protection holds the release until the Pod is gone")
	})

	t.Run("Missing resources are not an error", func(t *testing.T) {
		scheme := leaveTestScheme(t)
		ctx := t.Context()
		ec := leaveTestCluster()
		member := leaveTestMember(2)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, member).Build()
		state := &reconcileState{cluster: ec}

		assert.NoError(t, cleanupMemberResources(ctx, fakeClient, state, member))
	})

	t.Run("Pod absent from the snapshot is left untouched; the PVC still goes", func(t *testing.T) {
		scheme := leaveTestScheme(t)
		ctx := t.Context()
		ec := leaveTestCluster()
		member := leaveTestMember(2)
		pod := leaveTestPod(2)
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcNameForMember(pod.Name), Namespace: ec.Namespace},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, member, pod, pvc).Build()
		state := &reconcileState{cluster: ec, pods: nil}

		require.NoError(t, cleanupMemberResources(ctx, fakeClient, state, member))
		assert.NoError(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}),
			"the Pod lookup is snapshot-driven: one not in s.pods is not deleted")
		assert.Error(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}),
			"the PVC lookup is live by name, independent of the Pod snapshot")
	})
}

func TestRemoveEtcNode(t *testing.T) {
	_, peerURL2 := peerEndpointForOrdinalIndex(leaveTestCluster(), 2)

	t.Run("No membership snapshot — no-op when etcd is unreachable", func(t *testing.T) {
		state := &reconcileState{cluster: leaveTestCluster(), memberListResp: nil}
		assert.NoError(t, removeEtcNode(state, leaveTestMember(2)),
			"absent snapshot means no membership to amend — the #463 degradation")
	})

	t.Run("Member already absent from the membership — no-op", func(t *testing.T) {
		state := &reconcileState{
			cluster: leaveTestCluster(),
			memberListResp: &clientv3.MemberListResponse{Members: []*etcdserverpb.Member{
				{ID: 100, Name: "etcd-0", PeerURLs: []string{"http://etcd-0:2380"}},
			}},
		}
		assert.NoError(t, removeEtcNode(state, leaveTestMember(2)),
			"already-removed (or never-registered) must not trigger a RemoveMember dial")
	})

	// The matching-and-RemoveMember branch needs a real etcd; it is covered
	// live by e2e TestScaling (scale-in removes members from the cluster's
	// own membership), with the ordinal-derived peer URL it matches on
	// asserted here against the snapshot shape.
	t.Run("Snapshot entry carries the ordinal-derived peer URL", func(t *testing.T) {
		state := &reconcileState{
			cluster: leaveTestCluster(),
			memberListResp: &clientv3.MemberListResponse{Members: []*etcdserverpb.Member{
				{ID: 222, Name: "etcd-2", PeerURLs: []string{peerURL2}},
			}},
		}
		found := false
		for _, m := range state.memberListResp.Members {
			for _, u := range m.PeerURLs {
				if u == peerURL2 {
					found = true
				}
			}
		}
		assert.True(t, found, "the leave match key is the deterministic peer URL")
	})
}

// TestCleanupEtcdMember drives the whole Terminating leave in one call: the
// membership step is skipped when no snapshot exists (etcd unreachable), the
// owned Pod and PVC are deleted, and the member's finalizer is released so the
// object disappears.
func TestCleanupEtcdMember(t *testing.T) {
	scheme := leaveTestScheme(t)
	ctx := t.Context()
	ec := leaveTestCluster()
	now := metav1.Now()
	member := leaveTestMember(2)
	member.DeletionTimestamp = &now
	member.Finalizers = []string{memberCleanupFinalizer}
	pod := leaveTestPod(2)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcNameForMember(pod.Name), Namespace: ec.Namespace},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ec, member, pod, pvc).
		Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
	state := &reconcileState{cluster: ec, pods: []*corev1.Pod{pod}}

	res, err := r.cleanupEtcdMember(ctx, state, member)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

	assert.Error(t, fakeClient.Get(ctx, types.NamespacedName{Namespace: ec.Namespace, Name: pod.Name}, &corev1.Pod{}))
	assert.Error(t, fakeClient.Get(ctx, types.NamespacedName{Namespace: ec.Namespace, Name: pvc.Name}, &corev1.PersistentVolumeClaim{}))

	// Finalizer released → the member is gone, no second pass needed.
	assert.Error(t, fakeClient.Get(ctx, types.NamespacedName{Namespace: ec.Namespace, Name: member.Name}, &ecv1alpha1.EtcdMember{}))
}

// TestMarkMemberTerminating verifies the Phase write before the leave runs,
// and that a repeated call is a no-op.
func TestMarkMemberTerminating(t *testing.T) {
	scheme := leaveTestScheme(t)
	ctx := t.Context()
	member := leaveTestMember(0)
	member.Status.Phase = ecv1alpha1.EtcdMemberReady

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&ecv1alpha1.EtcdMember{}).
		WithObjects(member).
		Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	require.NoError(t, r.markMemberTerminating(ctx, member))
	got := &ecv1alpha1.EtcdMember{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: member.Name}, got))
	assert.Equal(t, ecv1alpha1.EtcdMemberTerminating, got.Status.Phase)

	// Already Terminating: no error, no write needed.
	assert.NoError(t, r.markMemberTerminating(ctx, member))
}
