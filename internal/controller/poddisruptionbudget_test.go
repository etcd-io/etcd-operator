package controller

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func memberList(voters, learners int) *clientv3.MemberListResponse {
	members := make([]*etcdserverpb.Member, 0, voters+learners)
	for i := 0; i < voters; i++ {
		members = append(members, &etcdserverpb.Member{ID: uint64(i + 1), Name: fmt.Sprintf("voter-%d", i)})
	}
	for i := 0; i < learners; i++ {
		members = append(members, &etcdserverpb.Member{
			ID: uint64(voters + i + 1), Name: fmt.Sprintf("learner-%d", i), IsLearner: true,
		})
	}
	return &clientv3.MemberListResponse{Members: members}
}

func TestPDBMinAvailable(t *testing.T) {
	tests := []struct {
		name        string
		total       int
		voting      int
		desiredSize int
		expected    int32
	}{
		{"steady 1", 1, 1, 1, 1},
		{"steady 2", 2, 2, 2, 2},
		{"steady 3", 3, 3, 3, 2},
		{"steady 5", 5, 5, 5, 3},
		{"learner counts as evictable-only pod", 4, 3, 4, 3},
		{"single voter plus learner", 2, 1, 2, 2},
		{"scale-out pending 1->3", 1, 1, 3, 2},
		{"scale-out pending 3->5", 3, 3, 5, 3},
		{"scale-out pending with learner", 4, 3, 5, 4},
		{"scale-in pending 3->2", 3, 3, 2, 3},
		{"scale-in pending 5->4", 5, 5, 4, 4},
		{"scale-in pending 5->2", 5, 5, 2, 4},
		{"scale-in pending 4->3", 4, 4, 3, 3},
		{"scale-in pending 2->1", 2, 2, 1, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, pdbMinAvailable(tt.total, tt.voting, tt.desiredSize))
		})
	}
}

func TestVotingMemberCount(t *testing.T) {
	assert.Equal(t, 0, votingMemberCount(nil))
	assert.Equal(t, 0, votingMemberCount(&clientv3.MemberListResponse{}))
	assert.Equal(t, 3, votingMemberCount(memberList(3, 2)))
}

func TestReconcilePodDisruptionBudget(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	newEtcdCluster := func(t *testing.T, name string, size int) *ecv1alpha1.EtcdCluster {
		t.Helper()
		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       ecv1alpha1.EtcdClusterSpec{Size: size, Version: "3.5.17"},
		}
		require.NoError(t, k8sClient.Create(ctx, ec))
		return ec
	}

	getPDB := func(t *testing.T, name string) *policyv1.PodDisruptionBudget {
		t.Helper()
		pdb := &policyv1.PodDisruptionBudget{}
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: "default"}, pdb))
		return pdb
	}

	t.Run("creates PDB with single voting member", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-single-voter", 1)

		err := reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(1, 0), scheme.Scheme)
		require.NoError(t, err)

		pdb := getPDB(t, ec.Name)
		expected := intstr.FromInt32(1)
		assert.Equal(t, &expected, pdb.Spec.MinAvailable)
		assert.Nil(t, pdb.Spec.MaxUnavailable)
		assert.Equal(t, map[string]string{
			"app":        ec.Name,
			"controller": ec.Name,
		}, pdb.Spec.Selector.MatchLabels)
		require.Len(t, pdb.OwnerReferences, 1)
		assert.Equal(t, "EtcdCluster", pdb.OwnerReferences[0].Kind)
		assert.Equal(t, ec.Name, pdb.OwnerReferences[0].Name)
		require.NotNil(t, pdb.OwnerReferences[0].Controller)
		assert.True(t, *pdb.OwnerReferences[0].Controller)
	})

	t.Run("three voting members", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-three-voters", 3)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 0), scheme.Scheme))
		assert.Equal(t, intstr.FromInt32(2), *getPDB(t, ec.Name).Spec.MinAvailable)
	})

	t.Run("five voting members", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-five-voters", 5)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(5, 0), scheme.Scheme))
		assert.Equal(t, intstr.FromInt32(3), *getPDB(t, ec.Name).Spec.MinAvailable)
	})

	t.Run("learner pod raises minAvailable", func(t *testing.T) {
		// The selector cannot tell voters from learners, so the learner pod
		// must not widen the eviction budget for voters.
		ec := newEtcdCluster(t, "pdb-with-learner", 4)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 1), scheme.Scheme))
		assert.Equal(t, intstr.FromInt32(3), *getPDB(t, ec.Name).Spec.MinAvailable)
	})

	t.Run("scale-in pending sizes for post-removal membership", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-scale-in", 2)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 0), scheme.Scheme))
		assert.Equal(t, intstr.FromInt32(3), *getPDB(t, ec.Name).Spec.MinAvailable)
	})

	t.Run("scale-out pending reserves the incoming learner pod", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-scale-out", 3)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(1, 0), scheme.Scheme))
		assert.Equal(t, intstr.FromInt32(2), *getPDB(t, ec.Name).Spec.MinAvailable)
	})

	t.Run("updates PDB on membership change (scale)", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-scale", 5)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(1, 0), scheme.Scheme))
		pdb := getPDB(t, ec.Name)
		uid := pdb.UID
		assert.Equal(t, intstr.FromInt32(2), *pdb.Spec.MinAvailable)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 0), scheme.Scheme))
		pdb = getPDB(t, ec.Name)
		assert.Equal(t, uid, pdb.UID)
		assert.Equal(t, intstr.FromInt32(3), *pdb.Spec.MinAvailable)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(5, 0), scheme.Scheme))
		pdb = getPDB(t, ec.Name)
		assert.Equal(t, uid, pdb.UID)
		assert.Equal(t, intstr.FromInt32(3), *pdb.Spec.MinAvailable)
	})

	t.Run("no members observed", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-no-members", 3)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, nil, scheme.Scheme))
		err := k8sClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: "default"},
			&policyv1.PodDisruptionBudget{})
		assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "expected NotFound, got %v", err)

		// A pre-existing PDB must survive a loop that observes no members.
		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 0), scheme.Scheme))
		before := getPDB(t, ec.Name)
		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, nil, scheme.Scheme))
		after := getPDB(t, ec.Name)
		assert.Equal(t, before.UID, after.UID)
		assert.Equal(t, intstr.FromInt32(2), *after.Spec.MinAvailable)
	})

	t.Run("pre-existing user PDB is not adopted", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-foreign", 3)

		maxUnavailable := intstr.FromInt32(1)
		userPDB := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: ec.Name, Namespace: "default"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &maxUnavailable,
				Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"user": "owned"}},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, userPDB))

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 0), scheme.Scheme))

		pdb := getPDB(t, ec.Name)
		assert.Empty(t, pdb.OwnerReferences)
		assert.Nil(t, pdb.Spec.MinAvailable)
		require.NotNil(t, pdb.Spec.MaxUnavailable)
		assert.Equal(t, maxUnavailable, *pdb.Spec.MaxUnavailable)
		assert.Equal(t, map[string]string{"user": "owned"}, pdb.Spec.Selector.MatchLabels)
	})
}
