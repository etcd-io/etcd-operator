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

func TestMinAvailableForVotingCount(t *testing.T) {
	tests := []struct {
		voting   int
		expected int32
	}{
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, minAvailableForVotingCount(tt.voting), "voting=%d", tt.voting)
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

	newEtcdCluster := func(t *testing.T, name string) *ecv1alpha1.EtcdCluster {
		t.Helper()
		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
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
		ec := newEtcdCluster(t, "pdb-single-voter")

		err := reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(1, 0), scheme.Scheme)
		require.NoError(t, err)

		pdb := getPDB(t, ec.Name)
		expected := intstr.FromInt32(1)
		assert.Equal(t, &expected, pdb.Spec.MinAvailable)
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
		ec := newEtcdCluster(t, "pdb-three-voters")

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 0), scheme.Scheme))
		assert.Equal(t, intstr.FromInt32(2), *getPDB(t, ec.Name).Spec.MinAvailable)
	})

	t.Run("five voting members", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-five-voters")

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(5, 0), scheme.Scheme))
		assert.Equal(t, intstr.FromInt32(3), *getPDB(t, ec.Name).Spec.MinAvailable)
	})

	t.Run("learners excluded", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-with-learner")

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 1), scheme.Scheme))
		assert.Equal(t, intstr.FromInt32(2), *getPDB(t, ec.Name).Spec.MinAvailable)
	})

	t.Run("updates PDB on membership change (scale)", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-scale")

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(1, 0), scheme.Scheme))
		uid := getPDB(t, ec.Name).UID

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(3, 0), scheme.Scheme))
		pdb := getPDB(t, ec.Name)
		assert.Equal(t, uid, pdb.UID)
		assert.Equal(t, intstr.FromInt32(2), *pdb.Spec.MinAvailable)

		require.NoError(t, reconcilePodDisruptionBudget(ctx, logger, k8sClient, ec, memberList(5, 0), scheme.Scheme))
		pdb = getPDB(t, ec.Name)
		assert.Equal(t, uid, pdb.UID)
		assert.Equal(t, intstr.FromInt32(3), *pdb.Spec.MinAvailable)
	})

	t.Run("no members observed", func(t *testing.T) {
		ec := newEtcdCluster(t, "pdb-no-members")

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
}
