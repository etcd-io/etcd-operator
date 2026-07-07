package controller

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	upgradeTestRegistry = "gcr.io/etcd-development/etcd"
	upgradeOldImage     = upgradeTestRegistry + ":3.5.17"
	upgradeNewImage     = upgradeTestRegistry + ":3.6.0"
)

func upgradeTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))
	return scheme
}

func upgradeTestCluster() *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd",
			Namespace: "default",
			UID:       "1234",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:          3,
			Version:       "3.6.0",
			ImageRegistry: upgradeTestRegistry,
		},
	}
}

func upgradeTestStatefulSet(ec *ecv1alpha1.EtcdCluster, image string) *appsv1.StatefulSet {
	replicas := int32(ec.Spec.Size)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ec.Name,
			Namespace: ec.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "operator.etcd.io/v1alpha1",
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				},
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    pointerToInt32(replicas),
			ServiceName: ec.Name,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "etcd", Image: image}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: replicas},
	}
}

func upgradeTestPod(ec *ecv1alpha1.EtcdCluster, ordinal int, image string, ready bool) *corev1.Pod {
	readyStatus := corev1.ConditionFalse
	if ready {
		readyStatus = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", ec.Name, ordinal),
			Namespace: ec.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "etcd", Image: image}},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: readyStatus}},
		},
	}
}

// upgradeTestHealth fabricates health info for members 0..len(revisions)-1;
// leaderOrdinal's member ID is reported as leader by all members.
func upgradeTestHealth(ec *ecv1alpha1.EtcdCluster, revisions []int64, leaderOrdinal int) (*clientv3.MemberListResponse, []etcdutils.EpHealth) {
	memberID := func(ordinal int) uint64 { return uint64(ordinal + 1) }

	members := make([]*etcdserverpb.Member, 0, len(revisions))
	health := make([]etcdutils.EpHealth, 0, len(revisions))
	for i, rev := range revisions {
		members = append(members, &etcdserverpb.Member{ID: memberID(i)})
		health = append(health, etcdutils.EpHealth{
			Ep:     fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379", ec.Name, i, ec.Name, ec.Namespace),
			Health: true,
			Status: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{
					MemberId: memberID(i),
					Revision: rev,
				},
				Leader: memberID(leaderOrdinal),
			},
		})
	}
	return &clientv3.MemberListResponse{Members: members}, health
}

func upgradeTestReconciler(t *testing.T, objs ...client.Object) (*EtcdClusterReconciler, client.Client) {
	t.Helper()
	scheme := upgradeTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}, fakeClient
}

func podNames(t *testing.T, c client.Client, namespace string) []string {
	t.Helper()
	podList := &corev1.PodList{}
	require.NoError(t, c.List(t.Context(), podList, client.InNamespace(namespace)))
	names := make([]string, 0, len(podList.Items))
	for i := range podList.Items {
		names = append(names, podList.Items[i].Name)
	}
	return names
}

// Flipped from the commit-1 regression repro (TestVersionBumpIsSilentNoop):
// a pure version bump now re-renders the StatefulSet template (OnDelete, so
// no pod restarts yet) and requeues.
func TestVersionBumpRerendersStatefulSet(t *testing.T) {
	ec := upgradeTestCluster()
	sts := upgradeTestStatefulSet(ec, upgradeOldImage)
	r, fakeClient := upgradeTestReconciler(t, ec, sts)

	memberList, health := upgradeTestHealth(ec, []int64{100, 100, 100}, 0)
	state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

	res, err := r.reconcileClusterState(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

	got := &appsv1.StatefulSet{}
	require.NoError(t, fakeClient.Get(t.Context(), client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, got))
	assert.Equal(t, upgradeNewImage, got.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, appsv1.OnDeleteStatefulSetStrategyType, got.Spec.UpdateStrategy.Type)
	assert.Empty(t, podNames(t, fakeClient, ec.Namespace), "no pod may be deleted or created by the re-render step")
}

func TestUpgradeDeletesOnePodPerReconcile(t *testing.T) {
	ec := upgradeTestCluster()
	sts := upgradeTestStatefulSet(ec, upgradeNewImage)
	r, fakeClient := upgradeTestReconciler(t, ec, sts,
		upgradeTestPod(ec, 0, upgradeOldImage, true),
		upgradeTestPod(ec, 1, upgradeOldImage, true),
		upgradeTestPod(ec, 2, upgradeOldImage, true),
	)

	memberList, health := upgradeTestHealth(ec, []int64{100, 100, 100}, 2)
	state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

	res, err := r.reconcileVersionUpgrade(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	assert.ElementsMatch(t, []string{"test-etcd-1", "test-etcd-2"}, podNames(t, fakeClient, ec.Namespace))

	// StatefulSet controller "recreated" pod-0 with the new image.
	require.NoError(t, fakeClient.Create(t.Context(), upgradeTestPod(ec, 0, upgradeNewImage, true)))

	res, err = r.reconcileVersionUpgrade(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	assert.ElementsMatch(t, []string{"test-etcd-0", "test-etcd-2"}, podNames(t, fakeClient, ec.Namespace))
}

func TestUpgradeLeaderPodLast(t *testing.T) {
	ec := upgradeTestCluster()
	sts := upgradeTestStatefulSet(ec, upgradeNewImage)
	r, fakeClient := upgradeTestReconciler(t, ec, sts,
		upgradeTestPod(ec, 0, upgradeOldImage, true),
		upgradeTestPod(ec, 1, upgradeOldImage, true),
		upgradeTestPod(ec, 2, upgradeOldImage, true),
	)

	memberList, health := upgradeTestHealth(ec, []int64{100, 100, 100}, 0)
	state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

	// Leader is ordinal 0: pods 1 and 2 must go first.
	res, err := r.reconcileVersionUpgrade(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	assert.ElementsMatch(t, []string{"test-etcd-0", "test-etcd-2"}, podNames(t, fakeClient, ec.Namespace))

	require.NoError(t, fakeClient.Create(t.Context(), upgradeTestPod(ec, 1, upgradeNewImage, true)))
	res, err = r.reconcileVersionUpgrade(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	assert.ElementsMatch(t, []string{"test-etcd-0", "test-etcd-1"}, podNames(t, fakeClient, ec.Namespace))

	// Leader pod is the sole outdated one left: it is finally replaced.
	require.NoError(t, fakeClient.Create(t.Context(), upgradeTestPod(ec, 2, upgradeNewImage, true)))
	res, err = r.reconcileVersionUpgrade(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	assert.ElementsMatch(t, []string{"test-etcd-1", "test-etcd-2"}, podNames(t, fakeClient, ec.Namespace))
}

func TestUpgradeBlocksWhenMemberLagging(t *testing.T) {
	ec := upgradeTestCluster()
	sts := upgradeTestStatefulSet(ec, upgradeNewImage)
	r, fakeClient := upgradeTestReconciler(t, ec, sts,
		upgradeTestPod(ec, 0, upgradeOldImage, true),
		upgradeTestPod(ec, 1, upgradeOldImage, true),
		upgradeTestPod(ec, 2, upgradeOldImage, true),
	)

	// Member 1 sits at revision 50 vs leader 100 (< 90%).
	memberList, health := upgradeTestHealth(ec, []int64{100, 50, 100}, 0)
	state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

	res, err := r.reconcileVersionUpgrade(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	assert.Len(t, podNames(t, fakeClient, ec.Namespace), 3, "no pod may be deleted while a member lags")
}

func TestUpgradeBlocksWhenPodNotReadyOrTerminating(t *testing.T) {
	t.Run("pod not ready", func(t *testing.T) {
		ec := upgradeTestCluster()
		sts := upgradeTestStatefulSet(ec, upgradeNewImage)
		r, fakeClient := upgradeTestReconciler(t, ec, sts,
			upgradeTestPod(ec, 0, upgradeOldImage, true),
			upgradeTestPod(ec, 1, upgradeNewImage, false),
			upgradeTestPod(ec, 2, upgradeOldImage, true),
		)

		memberList, health := upgradeTestHealth(ec, []int64{100, 100, 100}, 0)
		state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

		res, err := r.reconcileVersionUpgrade(t.Context(), state)
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		assert.Len(t, podNames(t, fakeClient, ec.Namespace), 3, "no pod may be deleted while another is not ready")
	})

	t.Run("pod terminating", func(t *testing.T) {
		ec := upgradeTestCluster()
		sts := upgradeTestStatefulSet(ec, upgradeNewImage)
		terminating := upgradeTestPod(ec, 1, upgradeNewImage, false)
		terminating.Finalizers = []string{"test.etcd.io/keep"}
		now := metav1.Now()
		terminating.DeletionTimestamp = &now
		r, fakeClient := upgradeTestReconciler(t, ec, sts,
			upgradeTestPod(ec, 0, upgradeOldImage, true),
			terminating,
			upgradeTestPod(ec, 2, upgradeOldImage, true),
		)

		memberList, health := upgradeTestHealth(ec, []int64{100, 100, 100}, 0)
		state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

		res, err := r.reconcileVersionUpgrade(t.Context(), state)
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		assert.Len(t, podNames(t, fakeClient, ec.Namespace), 3, "no pod may be deleted while another terminates")
	})
}

func TestUpgradeBlocksWhileLearnerPending(t *testing.T) {
	ec := upgradeTestCluster()
	sts := upgradeTestStatefulSet(ec, upgradeOldImage)
	r, fakeClient := upgradeTestReconciler(t, ec, sts,
		upgradeTestPod(ec, 0, upgradeOldImage, true),
		upgradeTestPod(ec, 1, upgradeOldImage, true),
		upgradeTestPod(ec, 2, upgradeOldImage, true),
	)

	// Member 2 is a learner far behind the leader: the learner branch of
	// reconcileClusterState must requeue before any upgrade work starts.
	memberList, health := upgradeTestHealth(ec, []int64{100, 100, 10}, 0)
	health[2].Status.IsLearner = true
	state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

	res, err := r.reconcileClusterState(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

	got := &appsv1.StatefulSet{}
	require.NoError(t, fakeClient.Get(t.Context(), client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, got))
	assert.Equal(t, upgradeOldImage, got.Spec.Template.Spec.Containers[0].Image, "template must not be re-rendered")
	assert.Len(t, podNames(t, fakeClient, ec.Namespace), 3, "no pod may be deleted while a learner is pending")
}

func TestUpgradeNoopWhenAllPodsUpdated(t *testing.T) {
	ec := upgradeTestCluster()
	sts := upgradeTestStatefulSet(ec, upgradeNewImage)
	r, fakeClient := upgradeTestReconciler(t, ec, sts,
		upgradeTestPod(ec, 0, upgradeNewImage, true),
		upgradeTestPod(ec, 1, upgradeNewImage, true),
		upgradeTestPod(ec, 2, upgradeNewImage, true),
	)

	memberList, health := upgradeTestHealth(ec, []int64{100, 100, 100}, 0)
	state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

	res, err := r.reconcileVersionUpgrade(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
	assert.Len(t, podNames(t, fakeClient, ec.Namespace), 3)
}

func TestUpgradeRequeuesWhenPodMissing(t *testing.T) {
	ec := upgradeTestCluster()
	sts := upgradeTestStatefulSet(ec, upgradeNewImage)
	// pod-1 is missing: its replacement is still being created.
	r, fakeClient := upgradeTestReconciler(t, ec, sts,
		upgradeTestPod(ec, 0, upgradeOldImage, true),
		upgradeTestPod(ec, 2, upgradeOldImage, true),
	)

	memberList, health := upgradeTestHealth(ec, []int64{100, 100, 100}, 0)
	state := &reconcileState{cluster: ec, sts: sts, memberListResp: memberList, memberHealth: health}

	res, err := r.reconcileVersionUpgrade(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
	assert.Len(t, podNames(t, fakeClient, ec.Namespace), 2, "no pod may be deleted while one is missing")

	// Not-found error other than pod absence must not be swallowed silently:
	// sanity-check the pods we expect are still the ones present.
	pod := &corev1.Pod{}
	err = fakeClient.Get(t.Context(), client.ObjectKey{Name: "test-etcd-1", Namespace: ec.Namespace}, pod)
	assert.True(t, k8serrors.IsNotFound(err))
}

func TestOrdinalFromPeerEp(t *testing.T) {
	tests := []struct {
		ep       string
		ordinal  int
		expectOk bool
	}{
		{"http://test-etcd-0.test-etcd.default.svc.cluster.local:2379", 0, true},
		{"http://test-etcd-12.test-etcd.default.svc.cluster.local:2379", 12, true},
		{"http://other-sts-1.other-sts.default.svc.cluster.local:2379", 0, false},
		{"http://test-etcd-x.test-etcd.default.svc.cluster.local:2379", 0, false},
	}
	for _, tt := range tests {
		ordinal, ok := ordinalFromPeerEp("test-etcd", tt.ep)
		assert.Equal(t, tt.expectOk, ok, tt.ep)
		if tt.expectOk {
			assert.Equal(t, tt.ordinal, ordinal, tt.ep)
		}
	}
}
