package controller

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ---------------------------------------------------------------------------
// listOwnedPods
// ---------------------------------------------------------------------------

func TestListOwnedPods(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster", Namespace: "default", UID: "abc"},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
	}

	makePod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels:    etcdClusterLabels(ec),
			},
		}
	}
	makeMember := func(name string, ordinal int, clusterUID string) *ecv1alpha1.EtcdMember {
		return &ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ec.Namespace,
				UID:       types.UID("member-" + name),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        types.UID(clusterUID),
					Controller: new(true),
				}},
			},
			Spec: ecv1alpha1.EtcdMemberSpec{ClusterName: ec.Name, Ordinal: ordinal, Version: ec.Spec.Version},
		}
	}
	// Pod owners are always EtcdMembers; the EtcdCluster never owns Pods
	// directly.
	makeMemberOwnedPod := func(name string, member *ecv1alpha1.EtcdMember) *corev1.Pod {
		pod := makePod(name)
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: ecv1alpha1.GroupVersion.String(),
			Kind:       "EtcdMember",
			Name:       member.Name,
			UID:        member.UID,
			Controller: new(true),
		}}
		return pod
	}

	t.Run("returns pods of the given members and ignores foreign or orphan pods", func(t *testing.T) {
		ctx := t.Context()
		member0 := makeMember("my-cluster-0", 0, "abc")
		member2 := makeMember("my-cluster-2", 2, "abc")
		pod0 := makeMemberOwnedPod("my-cluster-0", member0)
		pod2 := makeMemberOwnedPod("my-cluster-2", member2)
		// Controlled by a member of a different cluster, which is absent
		// from the caller's members list (listOwnedMembers filters it out).
		foreignMember := makeMember("my-cluster-3", 3, "different-cluster-uid")
		foreign := makeMemberOwnedPod("my-cluster-3", foreignMember)
		// Carries the cluster labels but has no controller owner.
		orphan := makePod("my-cluster-5")

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(ec, pod2, pod0, foreign, orphan).Build()

		pods, err := listOwnedPods(ctx, fakeClient, ec, []ecv1alpha1.EtcdMember{*member0, *member2})
		require.NoError(t, err)
		require.Len(t, pods, 2)
		assert.Equal(t, "my-cluster-0", pods[0].Name)
		assert.Equal(t, "my-cluster-2", pods[1].Name)
	})

	t.Run("sorts pods by ordinal ascending, not by name", func(t *testing.T) {
		ctx := t.Context()
		// Lexicographic name order would misplace my-cluster-10 before
		// my-cluster-2; the result must follow the numeric ordinal.
		member0 := makeMember("my-cluster-0", 0, "abc")
		member2 := makeMember("my-cluster-2", 2, "abc")
		member10 := makeMember("my-cluster-10", 10, "abc")
		pod0 := makeMemberOwnedPod("my-cluster-0", member0)
		pod2 := makeMemberOwnedPod("my-cluster-2", member2)
		pod10 := makeMemberOwnedPod("my-cluster-10", member10)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(ec, pod10, pod2, pod0).Build()

		pods, err := listOwnedPods(ctx, fakeClient, ec, []ecv1alpha1.EtcdMember{*member0, *member2, *member10})
		require.NoError(t, err)
		require.Len(t, pods, 3)
		names := []string{pods[0].Name, pods[1].Name, pods[2].Name}
		assert.Equal(t, []string{"my-cluster-0", "my-cluster-2", "my-cluster-10"}, names)
	})

	t.Run("returns empty slice when no pods exist", func(t *testing.T) {
		ctx := t.Context()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()

		pods, err := listOwnedPods(ctx, fakeClient, ec, nil)
		require.NoError(t, err)
		assert.Empty(t, pods)
	})
}

// ---------------------------------------------------------------------------
// createMemberPod
// ---------------------------------------------------------------------------

func TestBuildMemberPodUsesLifecycleInputs(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))

	cluster := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "default", UID: "cluster-uid"},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Version:       "3.5.17",
			ImageRegistry: "registry.example/etcd",
		},
	}
	member := &ecv1alpha1.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd-1", Namespace: cluster.Namespace, UID: "member-uid"},
		Spec: ecv1alpha1.EtcdMemberSpec{
			ClusterName: cluster.Name,
			Ordinal:     1,
			Version:     "3.6.2",
		},
	}
	builder, ok := any(buildMemberPod).(func(
		*ecv1alpha1.EtcdCluster,
		*ecv1alpha1.EtcdMember,
		etcdClusterState,
		string,
		*runtime.Scheme,
	) (*corev1.Pod, error))
	require.True(t, ok, "buildMemberPod should accept the target EtcdMember and explicit topology")

	for _, state := range []etcdClusterState{etcdClusterStateNew, etcdClusterStateExisting} {
		t.Run(string(state), func(t *testing.T) {
			const initialCluster = "etcd-0=http://peer-0:2380,etcd-1=http://peer-1:2380"
			pod, err := builder(cluster, member, state, initialCluster, scheme)
			require.NoError(t, err)

			env := envVarsToMap(pod)
			assert.Equal(t, string(state), env["ETCD_INITIAL_CLUSTER_STATE"])
			assert.Equal(t, initialCluster, env["ETCD_INITIAL_CLUSTER"])
			require.Len(t, pod.Spec.Containers, 1)
			assert.Equal(t, "registry.example/etcd:3.6.2", pod.Spec.Containers[0].Image)
			require.Len(t, pod.OwnerReferences, 1)
			assert.Equal(t, member.Name, pod.OwnerReferences[0].Name)
			assert.Equal(t, "EtcdMember", pod.OwnerReferences[0].Kind)
			assert.True(t, *pod.OwnerReferences[0].Controller)
		})
	}
}

func TestCreateMemberPod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	ctx := t.Context()
	logger := log.FromContext(ctx)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-etcd", Namespace: "default", UID: "1"},
		Spec:       ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
	}

	t.Run("creates pod-0 with state=new", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		member := testMemberForCluster(ec, 0)
		_, peerURL := peerEndpointForOrdinalIndex(ec, 0)
		err := createMemberPod(
			ctx,
			logger,
			fakeClient,
			ec,
			member,
			etcdClusterStateNew,
			member.Name+"="+peerURL,
			scheme,
		)
		require.NoError(t, err)

		pod := &corev1.Pod{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "test-etcd-0", Namespace: "default"}, pod))
		assert.Equal(t, "test-etcd-0", pod.Name)

		envMap := envVarsToMap(pod)
		assert.Equal(t, string(etcdClusterStateNew), envMap["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, envMap["ETCD_INITIAL_CLUSTER"], "test-etcd-0=")
		assert.NotContains(t, envMap["ETCD_INITIAL_CLUSTER"], "test-etcd-1=")
		assert.Equal(t, etcdDataDir, envMap["ETCD_DATA_DIR"])

		require.Len(t, pod.OwnerReferences, 1)
		assert.Equal(t, member.Name, pod.OwnerReferences[0].Name)
	})

	t.Run("creates pod-2 with state=existing and full initial cluster", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		member := testMemberForCluster(ec, 2)
		clusterParts := make([]string, 0, 3)
		for ordinal := range 3 {
			name, peerURL := peerEndpointForOrdinalIndex(ec, ordinal)
			clusterParts = append(clusterParts, name+"="+peerURL)
		}
		err := createMemberPod(
			ctx,
			logger,
			fakeClient,
			ec,
			member,
			etcdClusterStateExisting,
			strings.Join(clusterParts, ","),
			scheme,
		)
		require.NoError(t, err)

		pod := &corev1.Pod{}
		require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: "test-etcd-2", Namespace: "default"}, pod))

		envMap := envVarsToMap(pod)
		assert.Equal(t, string(etcdClusterStateExisting), envMap["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, envMap["ETCD_INITIAL_CLUSTER"], "test-etcd-0=")
		assert.Contains(t, envMap["ETCD_INITIAL_CLUSTER"], "test-etcd-1=")
		assert.Contains(t, envMap["ETCD_INITIAL_CLUSTER"], "test-etcd-2=")
	})
}

func testMemberForCluster(ec *ecv1alpha1.EtcdCluster, ordinal int) *ecv1alpha1.EtcdMember {
	return &ecv1alpha1.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdMemberName(ec.Name, ordinal),
			Namespace: ec.Namespace,
			UID:       types.UID(fmt.Sprintf("member-%d", ordinal)),
		},
		Spec: ecv1alpha1.EtcdMemberSpec{
			ClusterName: ec.Name,
			Ordinal:     ordinal,
			Version:     ec.Spec.Version,
		},
	}
}

// envVarsToMap converts a container's env slice into a name→value map.
func envVarsToMap(pod *corev1.Pod) map[string]string {
	m := make(map[string]string)
	for _, e := range pod.Spec.Containers[0].Env {
		m[e.Name] = e.Value
	}
	return m
}

// ---------------------------------------------------------------------------
// waitForPodReady
// ---------------------------------------------------------------------------

func TestWaitForPodReady(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	readyPod := func(name, namespace string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}},
			},
		}
	}
	notReadyPod := func(name, namespace string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		}
	}

	tests := []struct {
		name          string
		pod           *corev1.Pod
		expectedError error
	}{
		{
			name:          "Pod is ready",
			pod:           readyPod("test-pod", "default"),
			expectedError: nil,
		},
		{
			name:          "Pod is not ready",
			pod:           notReadyPod("test-pod", "default"),
			expectedError: errors.New("pod default/test-pod did not become ready"),
		},
		{
			name:          "Pod does not exist",
			pod:           nil,
			expectedError: errors.New("not found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.pod != nil {
				builder = builder.WithObjects(tt.pod)
			}
			fakeClient := builder.Build()

			ctx := t.Context()
			logger := log.FromContext(ctx)
			err := waitForPodReady(ctx, logger, fakeClient, "test-pod", "default")
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// IsLearnerReady (delegates to etcdutils)
// ---------------------------------------------------------------------------

func TestIsLearnerReady(t *testing.T) {
	tests := []struct {
		name           string
		leaderStatus   *clientv3.StatusResponse
		learnerStatus  *clientv3.StatusResponse
		expectedResult bool
	}{
		{
			name: "Learner is ready",
			leaderStatus: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 100},
			},
			learnerStatus: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 95},
			},
			expectedResult: true,
		},
		{
			name: "Learner is not ready",
			leaderStatus: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 100},
			},
			learnerStatus: &clientv3.StatusResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 80},
			},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := etcdutils.IsLearnerReady(tt.leaderStatus, tt.learnerStatus)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// ---------------------------------------------------------------------------
// createMemberPod — pod annotations and labels
// ---------------------------------------------------------------------------

func TestCreateMemberPodWithAnnotations(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	cluster := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", UID: "1"},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    3,
			Version: "3.5.17",
		},
	}

	tests := []struct {
		name                string
		clusterName         string
		podTemplate         *ecv1alpha1.PodTemplate
		expectedAnnotations map[string]string
	}{
		{
			name:        "creates pod with custom annotations",
			clusterName: "test-etcd",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "2379",
					},
				},
			},
			expectedAnnotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "2379",
			},
		},
		{
			name:        "creates pod without annotations when PodTemplate is nil",
			clusterName: "test-etcd-no-podtemplate",
			podTemplate: nil,
		},
		{
			name:        "creates pod without annotations when annotations map is empty",
			clusterName: "test-etcd-empty-annotations",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Annotations: map[string]string{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := cluster.DeepCopy()
			ec.Name = tt.clusterName
			ec.Spec.PodTemplate = tt.podTemplate

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()

			member := testMemberForCluster(ec, 0)
			err := createMemberPod(
				ctx,
				logger,
				fakeClient,
				ec,
				member,
				etcdClusterStateNew,
				"ignored",
				scheme,
			)
			require.NoError(t, err)

			pod := &corev1.Pod{}
			require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: tt.clusterName + "-0", Namespace: "default"}, pod))

			require.Len(t, pod.OwnerReferences, 1)
			assert.Equal(t, member.Name, pod.OwnerReferences[0].Name)

			// the operator can insert more annotations, but we can guarantee that the expected KVs would be there
			for k, v := range tt.expectedAnnotations {
				value, ok := pod.Annotations[k]
				assert.True(t, ok, "the annotaion entry with key %s and value %s doesn't exist in the pod", k, v)
				assert.Equal(t, v, value, "mismatch value for key %s: want %s, got %s", k, v, value)
			}
		})
	}
}

func TestCreateMemberPodWithLabels(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name           string
		clusterName    string
		podTemplate    *ecv1alpha1.PodTemplate
		expectedLabels map[string]string
	}{
		{
			name:        "custom labels merged with default labels",
			clusterName: "test-etcd",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Labels: map[string]string{
						"environment": "production",
						"version":     "v1.0.0",
						"team":        "platform",
					},
				},
			},
			expectedLabels: map[string]string{
				"app":         "test-etcd",
				"controller":  "test-etcd",
				"environment": "production",
				"version":     "v1.0.0",
				"team":        "platform",
			},
		},
		{
			name:        "only default labels when PodTemplate is nil",
			clusterName: "test-etcd-no-podtemplate",
			podTemplate: nil,
			expectedLabels: map[string]string{
				"app":        "test-etcd-no-podtemplate",
				"controller": "test-etcd-no-podtemplate",
			},
		},
		{
			name:        "only default labels when labels map is empty",
			clusterName: "test-etcd-empty-labels",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{Labels: map[string]string{}},
			},
			expectedLabels: map[string]string{
				"app":        "test-etcd-empty-labels",
				"controller": "test-etcd-empty-labels",
			},
		},
		{
			name:        "default labels override conflicting custom labels",
			clusterName: "test-etcd-override",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Labels: map[string]string{
						"app":         "custom-app",
						"controller":  "custom-controller",
						"environment": "staging",
					},
				},
			},
			expectedLabels: map[string]string{
				"app":         "test-etcd-override",
				"controller":  "test-etcd-override",
				"environment": "staging",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: tt.clusterName, Namespace: "default", UID: "1"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					Size:        3,
					Version:     "3.5.17",
					PodTemplate: tt.podTemplate,
				},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()

			member := testMemberForCluster(ec, 0)
			err := createMemberPod(
				ctx,
				logger,
				fakeClient,
				ec,
				member,
				etcdClusterStateNew,
				"ignored",
				scheme,
			)
			require.NoError(t, err)

			pod := &corev1.Pod{}
			require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: tt.clusterName + "-0", Namespace: "default"}, pod))
			assert.Equal(t, tt.expectedLabels, pod.Labels)
			require.Len(t, pod.OwnerReferences, 1)
			assert.Equal(t, member.Name, pod.OwnerReferences[0].Name)
		})
	}
}

// ---------------------------------------------------------------------------
// createArgs
// ---------------------------------------------------------------------------

func TestCreatingArgs(t *testing.T) {
	tests := []struct {
		testName       string
		etcdOptions    []string
		clusterName    string
		tlsEnabled     bool
		expectedResult []string
	}{
		{
			testName:    "No etcdOptions provided",
			etcdOptions: nil,
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
			},
		},
		{
			testName:    "TLS enabled adds TLS args and https schemes",
			etcdOptions: nil,
			clusterName: "testCluster",
			tlsEnabled:  true,
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=https://0.0.0.0:2380",
				"--listen-client-urls=https://0.0.0.0:2379",
				"--initial-advertise-peer-urls=https://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=https://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--cert-file=" + serverCertFile,
				"--key-file=" + serverKeyFile,
				"--trusted-ca-file=" + serverTrustedCAFile,
				"--client-cert-auth=true",
				"--peer-cert-file=" + peerCertFile,
				"--peer-key-file=" + peerKeyFile,
				"--peer-trusted-ca-file=" + peerTrustedCAFile,
				"--peer-client-cert-auth=true",
			},
		},
		{
			testName: "Etcd options with = sign",
			etcdOptions: []string{
				"--max-wals=7",
				"--discovery-failbox=proxy",
			},
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--max-wals=7",
				"--discovery-failbox=proxy",
			},
		},
		{
			testName: "TLS enabled with custom etcdOptions",
			etcdOptions: []string{
				"--max-wals=7",
			},
			clusterName: "testCluster",
			tlsEnabled:  true,
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=https://0.0.0.0:2380",
				"--listen-client-urls=https://0.0.0.0:2379",
				"--initial-advertise-peer-urls=https://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=https://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--cert-file=" + serverCertFile,
				"--key-file=" + serverKeyFile,
				"--trusted-ca-file=" + serverTrustedCAFile,
				"--client-cert-auth=true",
				"--peer-cert-file=" + peerCertFile,
				"--peer-key-file=" + peerKeyFile,
				"--peer-trusted-ca-file=" + peerTrustedCAFile,
				"--peer-client-cert-auth=true",
				"--max-wals=7",
			},
		},
		{
			testName: "Etcd options with spaces",
			etcdOptions: []string{
				"--max-wals 7",
				"--discovery-failbox proxy",
			},
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--max-wals 7",
				"--discovery-failbox proxy",
			},
		},
		{
			testName: "Etcd switch options",
			etcdOptions: []string{
				"--experimental-peer-skip-client-san-verification",
			},
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=http://0.0.0.0:2380",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--experimental-peer-skip-client-san-verification",
			},
		},
		{
			testName: "Overwrite default arg",
			etcdOptions: []string{
				"--listen-peer-urls=http://0.0.0.0:3200",
				"--experimental-peer-skip-client-san-verification",
			},
			clusterName: "testCluster",
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-client-urls=http://0.0.0.0:2379",
				"--initial-advertise-peer-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=http://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--listen-peer-urls=http://0.0.0.0:3200",
				"--experimental-peer-skip-client-san-verification",
			},
		},
		{
			testName: "TLS overwrite default TLS arg",
			etcdOptions: []string{
				"--cert-file=/custom/cert-path",
			},
			clusterName: "testCluster",
			tlsEnabled:  true,
			expectedResult: []string{
				"--name=$(POD_NAME)",
				"--listen-peer-urls=https://0.0.0.0:2380",
				"--listen-client-urls=https://0.0.0.0:2379",
				"--initial-advertise-peer-urls=https://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2380",
				"--advertise-client-urls=https://$(POD_NAME).testCluster.$(POD_NAMESPACE).svc.cluster.local:2379",
				"--key-file=" + serverKeyFile,
				"--trusted-ca-file=" + serverTrustedCAFile,
				"--client-cert-auth=true",
				"--peer-cert-file=" + peerCertFile,
				"--peer-key-file=" + peerKeyFile,
				"--peer-trusted-ca-file=" + peerTrustedCAFile,
				"--peer-client-cert-auth=true",
				"--cert-file=/custom/cert-path",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			result := createArgs(tt.clusterName, tt.etcdOptions, tt.tlsEnabled)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// ---------------------------------------------------------------------------
// clientEndpointForOrdinal / clientEndpointsFromPods
// ---------------------------------------------------------------------------

func TestClientEndpointForOrdinal(t *testing.T) {
	tests := []struct {
		name       string
		ordinal    int
		tlsEnabled bool
		expected   string
	}{
		{name: "ordinal 0 no TLS", ordinal: 0, tlsEnabled: false, expected: "http://test-cluster-0.test-cluster.default.svc.cluster.local:2379"},
		{name: "ordinal 1 no TLS", ordinal: 1, tlsEnabled: false, expected: "http://test-cluster-1.test-cluster.default.svc.cluster.local:2379"},
		{name: "ordinal 2 no TLS", ordinal: 2, tlsEnabled: false, expected: "http://test-cluster-2.test-cluster.default.svc.cluster.local:2379"},
		{name: "ordinal 0 with TLS", ordinal: 0, tlsEnabled: true, expected: "https://test-cluster-0.test-cluster.default.svc.cluster.local:2379"},
		{name: "ordinal 1 with TLS", ordinal: 1, tlsEnabled: true, expected: "https://test-cluster-1.test-cluster.default.svc.cluster.local:2379"},
		{name: "ordinal 2 with TLS", ordinal: 2, tlsEnabled: true, expected: "https://test-cluster-2.test-cluster.default.svc.cluster.local:2379"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clientEndpointForOrdinal("test-cluster", "default", tt.ordinal, tt.tlsEnabled)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClientEndpointsFromPods(t *testing.T) {
	makePod := func(clusterName, namespace string, ordinal int) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", clusterName, ordinal),
				Namespace: namespace,
			},
		}
	}

	threePods := []*corev1.Pod{
		makePod("test-sts", "default", 0),
		makePod("test-sts", "default", 1),
		makePod("test-sts", "default", 2),
	}

	tests := []struct {
		name       string
		pods       []*corev1.Pod
		tlsEnabled bool
		expected   []string
	}{
		{
			name:       "3 pods no TLS",
			pods:       threePods,
			tlsEnabled: false,
			expected: []string{
				"http://test-sts-0.test-sts.default.svc.cluster.local:2379",
				"http://test-sts-1.test-sts.default.svc.cluster.local:2379",
				"http://test-sts-2.test-sts.default.svc.cluster.local:2379",
			},
		},
		{
			name:       "3 pods with TLS",
			pods:       threePods,
			tlsEnabled: true,
			expected: []string{
				"https://test-sts-0.test-sts.default.svc.cluster.local:2379",
				"https://test-sts-1.test-sts.default.svc.cluster.local:2379",
				"https://test-sts-2.test-sts.default.svc.cluster.local:2379",
			},
		},
		{
			name:       "no pods",
			pods:       nil,
			tlsEnabled: false,
			expected:   []string(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clientEndpointsFromPods("test-sts", "default", tt.pods, tt.tlsEnabled)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildMemberPodTLSVolumes(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))

	mkCluster := func(tls *ecv1alpha1.TLSCertificate, storage *ecv1alpha1.StorageSpec) *ecv1alpha1.EtcdCluster {
		return &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "tls-cluster", Namespace: "default", UID: "1"},
			Spec: ecv1alpha1.EtcdClusterSpec{
				Size:        3,
				Version:     "3.5.17",
				TLS:         tls,
				StorageSpec: storage,
			},
		}
	}

	t.Run("TLS cluster mounts both secrets readonly and adds TLS args", func(t *testing.T) {
		ec := mkCluster(&ecv1alpha1.TLSCertificate{Provider: "auto"}, nil)
		pod, err := buildMemberPod(ec, testMemberForCluster(ec, 0), etcdClusterStateNew, "ignored", scheme)
		require.NoError(t, err)

		container := pod.Spec.Containers[0]

		// VolumeMounts: server + peer, both read-only.
		var mounts []corev1.VolumeMount
		for _, m := range container.VolumeMounts {
			if m.Name == "server-secret" || m.Name == "peer-secret" {
				mounts = append(mounts, m)
			}
		}
		require.Len(t, mounts, 2, "expected server-secret and peer-secret mounts")
		for _, m := range mounts {
			assert.True(t, m.ReadOnly, "mount %s must be read-only", m.Name)
		}
		assert.Contains(t, []string{mounts[0].MountPath, mounts[1].MountPath}, serverCertMountDir)
		assert.Contains(t, []string{mounts[0].MountPath, mounts[1].MountPath}, peerCertMountDir)

		// Pod-level Volumes for both secrets.
		volNames := map[string]bool{}
		for _, v := range pod.Spec.Volumes {
			volNames[v.Name] = true
		}
		assert.True(t, volNames["server-secret"], "server-secret volume present")
		assert.True(t, volNames["peer-secret"], "peer-secret volume present")

		// Args must include the TLS flags and https schemes.
		args := strings.Join(container.Args, "\n")
		assert.Contains(t, args, "--cert-file="+serverCertFile)
		assert.Contains(t, args, "--peer-cert-file="+peerCertFile)
		assert.Contains(t, args, "--listen-client-urls=https://0.0.0.0:2379")
		assert.Contains(t, args, "--listen-peer-urls=https://0.0.0.0:2380")
	})

	t.Run("non-TLS cluster adds no secret mounts and stays http", func(t *testing.T) {
		ec := mkCluster(nil, nil)
		pod, err := buildMemberPod(ec, testMemberForCluster(ec, 0), etcdClusterStateNew, "ignored", scheme)
		require.NoError(t, err)

		for _, m := range pod.Spec.Containers[0].VolumeMounts {
			assert.NotEqual(t, "server-secret", m.Name, "non-TLS pod must not mount server-secret")
			assert.NotEqual(t, "peer-secret", m.Name, "non-TLS pod must not mount peer-secret")
		}
		for _, v := range pod.Spec.Volumes {
			assert.NotEqual(t, "server-secret", v.Name)
			assert.NotEqual(t, "peer-secret", v.Name)
		}
		args := strings.Join(pod.Spec.Containers[0].Args, "\n")
		assert.NotContains(t, args, "--cert-file=")
		assert.Contains(t, args, "--listen-client-urls=http://0.0.0.0:2379")
	})
}
