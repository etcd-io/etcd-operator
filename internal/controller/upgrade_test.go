package controller

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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

func upgradeTestStatefulSet(ec *ecv1alpha1.EtcdCluster, image string, replicas int32) *appsv1.StatefulSet {
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

// upgradeTestHealth fabricates health info for members 0..size-1 with the
// given revisions; leaderOrdinal's member ID is reported as leader by all.
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

// regression repro: pure version bump never re-renders the STS; fixed in the next commit.
func TestVersionBumpIsSilentNoop(t *testing.T) {
	scheme := upgradeTestScheme(t)
	ec := upgradeTestCluster()
	sts := upgradeTestStatefulSet(ec, upgradeOldImage, 3)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts).Build()
	r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

	memberList, health := upgradeTestHealth(ec, []int64{100, 100, 100}, 0)
	state := &reconcileState{
		cluster:        ec,
		sts:            sts,
		memberListResp: memberList,
		memberHealth:   health,
	}

	res, err := r.reconcileClusterState(t.Context(), state)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)

	got := &appsv1.StatefulSet{}
	require.NoError(t, fakeClient.Get(t.Context(), client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, got))
	assert.Equal(t, upgradeOldImage, got.Spec.Template.Spec.Containers[0].Image,
		"version bump silently ignored: StatefulSet still runs the old image")
}
