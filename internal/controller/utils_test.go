package controller

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
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
	"go.etcd.io/etcd-operator/pkg/certificate"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func pointerToBool(value bool) *bool {
	return &value
}

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

	makePod := func(name, uid string, owned bool) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels:    etcdClusterLabels(ec),
			},
		}
		if owned {
			pod.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: ecv1alpha1.GroupVersion.String(),
				Kind:       "EtcdCluster",
				Name:       ec.Name,
				UID:        types.UID(uid),
				Controller: pointerToBool(true),
			}}
		}
		return pod
	}

	t.Run("returns only owned pods sorted by ordinal", func(t *testing.T) {
		ctx := t.Context()
		pod0 := makePod("my-cluster-0", "abc", true)
		pod2 := makePod("my-cluster-2", "abc", true)
		pod1 := makePod("my-cluster-1", "abc", true)
		foreign := makePod("my-cluster-3", "different-uid", false)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(ec, pod0, pod1, pod2, foreign).Build()

		pods, err := listOwnedPods(ctx, fakeClient, ec)
		require.NoError(t, err)
		require.Len(t, pods, 3)
		assert.Equal(t, "my-cluster-0", pods[0].Name)
		assert.Equal(t, "my-cluster-1", pods[1].Name)
		assert.Equal(t, "my-cluster-2", pods[2].Name)
	})

	t.Run("returns empty slice when no pods exist", func(t *testing.T) {
		ctx := t.Context()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()

		pods, err := listOwnedPods(ctx, fakeClient, ec)
		require.NoError(t, err)
		assert.Empty(t, pods)
	})
}

// ---------------------------------------------------------------------------
// createMemberPod
// ---------------------------------------------------------------------------

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
		err := createMemberPod(ctx, logger, fakeClient, ec, 0, scheme)
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
		assert.Equal(t, ec.Name, pod.OwnerReferences[0].Name)
	})

	t.Run("creates pod-2 with state=existing and full initial cluster", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		err := createMemberPod(ctx, logger, fakeClient, ec, 2, scheme)
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
// createHeadlessServiceIfNotExist
// ---------------------------------------------------------------------------

func TestCreateHeadlessServiceIfNotExist(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-etcd", Namespace: "default"},
	}

	t.Run("creates headless service if it does not exist", func(t *testing.T) {
		err := createHeadlessServiceIfNotExist(ctx, logger, fakeClient, ec, scheme)
		assert.NoError(t, err)

		service := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: "test-etcd", Namespace: "default"}, service)
		assert.NoError(t, err)
		assert.Equal(t, "None", service.Spec.ClusterIP)
		assert.Equal(t, map[string]string{
			"app":        "test-etcd",
			"controller": "test-etcd",
		}, service.Spec.Selector)
		require.Len(t, service.OwnerReferences, 1)
		assert.Equal(t, ec.Name, service.OwnerReferences[0].Name)
	})

	t.Run("does not create service if it already exists", func(t *testing.T) {
		err := createHeadlessServiceIfNotExist(ctx, logger, fakeClient, ec, scheme)
		assert.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// clientEndpointForOrdinal / clientEndpointsFromPods
// ---------------------------------------------------------------------------

func TestClientEndpointForOrdinal(t *testing.T) {
	tests := []struct {
		ordinal  int
		expected string
	}{
		{0, "http://test-cluster-0.test-cluster.default.svc.cluster.local:2379"},
		{1, "http://test-cluster-1.test-cluster.default.svc.cluster.local:2379"},
		{2, "http://test-cluster-2.test-cluster.default.svc.cluster.local:2379"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("ordinal %d", tt.ordinal), func(t *testing.T) {
			result := clientEndpointForOrdinal("test-cluster", "default", tt.ordinal)
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

	tests := []struct {
		name     string
		pods     []*corev1.Pod
		expected []string
	}{
		{
			name: "3 pods",
			pods: []*corev1.Pod{
				makePod("test-sts", "default", 0),
				makePod("test-sts", "default", 1),
				makePod("test-sts", "default", 2),
			},
			expected: []string{
				"http://test-sts-0.test-sts.default.svc.cluster.local:2379",
				"http://test-sts-1.test-sts.default.svc.cluster.local:2379",
				"http://test-sts-2.test-sts.default.svc.cluster.local:2379",
			},
		},
		{
			name:     "no pods",
			pods:     nil,
			expected: []string(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clientEndpointsFromPods("test-sts", "default", tt.pods)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ---------------------------------------------------------------------------
// areAllMembersHealthy
// ---------------------------------------------------------------------------

func TestAreAllMembersHealthy(t *testing.T) {
	tests := []struct {
		name     string
		health   []etcdutils.EpHealth
		expected bool
	}{
		{
			name:     "empty slice — no unhealthy members",
			health:   nil,
			expected: true,
		},
		{
			name: "all healthy",
			health: []etcdutils.EpHealth{
				{Health: true},
				{Health: true},
				{Health: true},
			},
			expected: true,
		},
		{
			name: "one unhealthy",
			health: []etcdutils.EpHealth{
				{Health: true},
				{Health: false},
				{Health: true},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, areAllMembersHealthy(tt.health))
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

	tests := []struct {
		name                string
		clusterName         string
		podTemplate         *ecv1alpha1.PodTemplate
		expectedAnnotations map[string]string
		expectNil           bool
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
			expectNil: false,
		},
		{
			name:        "creates pod without annotations when PodTemplate is nil",
			clusterName: "test-etcd-no-podtemplate",
			podTemplate: nil,
			expectNil:   true,
		},
		{
			name:        "creates pod without annotations when annotations map is empty",
			clusterName: "test-etcd-empty-annotations",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Annotations: map[string]string{},
				},
			},
			expectNil: true,
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

			err := createMemberPod(ctx, logger, fakeClient, ec, 0, scheme)
			require.NoError(t, err)

			pod := &corev1.Pod{}
			require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: tt.clusterName + "-0", Namespace: "default"}, pod))

			if tt.expectNil {
				assert.Nil(t, pod.Annotations)
			} else {
				assert.Equal(t, tt.expectedAnnotations, pod.Annotations)
			}
			require.Len(t, pod.OwnerReferences, 1)
			assert.Equal(t, ec.Name, pod.OwnerReferences[0].Name)
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

			err := createMemberPod(ctx, logger, fakeClient, ec, 0, scheme)
			require.NoError(t, err)

			pod := &corev1.Pod{}
			require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: tt.clusterName + "-0", Namespace: "default"}, pod))
			assert.Equal(t, tt.expectedLabels, pod.Labels)
			require.Len(t, pod.OwnerReferences, 1)
			assert.Equal(t, ec.Name, pod.OwnerReferences[0].Name)
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
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			result := createArgs(tt.clusterName, tt.etcdOptions)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// ---------------------------------------------------------------------------
// validateEtcdUpgradePath
// ---------------------------------------------------------------------------

func TestValidateEtcdUpgradePath(t *testing.T) {
	etcdVersions := []semver.Version{
		{Major: 3, Minor: 0},
		{Major: 3, Minor: 1},
		{Major: 3, Minor: 2},
		{Major: 3, Minor: 3},
		{Major: 3, Minor: 4},
		{Major: 3, Minor: 5},
		{Major: 3, Minor: 6},
		{Major: 3, Minor: 7},
		{Major: 4, Minor: 0},
	}

	tests := []struct {
		name      string
		current   string
		target    string
		canParse  bool
		expectErr bool
	}{
		{name: "equal versions", current: "3.2.0", target: "3.2.0", canParse: true, expectErr: false},
		{name: "valid minor level upgrade", current: "3.4.0", target: "3.5.0", canParse: true, expectErr: false},
		{name: "valid patch level upgrade", current: "3.4.0", target: "3.4.1", canParse: true, expectErr: false},
		{name: "invalid current version", current: "invalid", target: "3.1.0", canParse: false, expectErr: true},
		{name: "invalid target version", current: "3.1.0", target: "invalid", canParse: false, expectErr: true},
		{name: "minor downgrade not allowed", current: "3.2.0", target: "3.1.0", canParse: true, expectErr: true},
		{name: "patch downgrade not allowed", current: "3.5.1", target: "3.5.0", canParse: true, expectErr: true},
		{name: "unknown current version", current: "3.9.0", target: "4.0.0", canParse: true, expectErr: true},
		{name: "unknown target version", current: "4.0.0", target: "4.1.0", canParse: true, expectErr: true},
		{name: "invalid upgrade skipping minor", current: "3.4.0", target: "3.6.0", canParse: true, expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canParse, err := validateEtcdUpgradePath(etcdVersions, tt.current, tt.target)
			if canParse != tt.canParse {
				t.Fatalf("expected canParse=%v, got %v", tt.canParse, canParse)
			}
			if tt.expectErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("did not expect error, got %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Certificate config helpers
// ---------------------------------------------------------------------------

func TestCreateAutoCertificateConfig(t *testing.T) {
	tests := []struct {
		name     string
		ec       *ecv1alpha1.EtcdCluster
		expected *certInterface.Config
		wantErr  bool
	}{
		{
			name: "auto config with all fields set",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "test-namespace"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					TLS: &ecv1alpha1.TLSCertificate{
						Provider: string(certificate.Auto),
						ProviderCfg: ecv1alpha1.ProviderConfig{
							AutoCfg: &ecv1alpha1.ProviderAutoConfig{
								CommonConfig: ecv1alpha1.CommonConfig{
									CommonName:       "custom.example.com",
									Organization:     []string{"Test Org"},
									ValidityDuration: "720h",
									AltNames: ecv1alpha1.AltNames{
										DNSNames: []string{"custom1.example.com", "custom2.example.com"},
									},
								},
							},
						},
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "custom.example.com",
				Organization:     []string{"Test Org"},
				ValidityDuration: 720 * time.Hour,
				AltNames: certInterface.AltNames{
					DNSNames: []string{"custom1.example.com", "custom2.example.com"},
					IPs:      make([]net.IP, 2),
				},
			},
			wantErr: false,
		},
		{
			name: "auto config with nil AutoCfg — uses defaults",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "test-namespace"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					TLS: &ecv1alpha1.TLSCertificate{
						Provider:    string(certificate.Auto),
						ProviderCfg: ecv1alpha1.ProviderConfig{AutoCfg: nil},
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "test-cluster.test-namespace.svc.cluster.local",
				Organization:     nil,
				ValidityDuration: certInterface.DefaultAutoValidity,
				AltNames: certInterface.AltNames{
					DNSNames: []string{
						"*.test-cluster.test-namespace.svc.cluster.local",
						"test-cluster.test-namespace.svc.cluster.local",
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := createAutoCertificateConfig(tt.ec)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.expected.CommonName, result.CommonName)
				assert.Equal(t, tt.expected.Organization, result.Organization)
				assert.Equal(t, tt.expected.ValidityDuration, result.ValidityDuration)
				assert.Equal(t, tt.expected.AltNames.DNSNames, result.AltNames.DNSNames)
				assert.Equal(t, tt.expected.AltNames.IPs, result.AltNames.IPs)
			}
		})
	}
}

func TestCreateCMCertificateConfig(t *testing.T) {
	tests := []struct {
		name     string
		ec       *ecv1alpha1.EtcdCluster
		expected *certInterface.Config
		wantErr  bool
	}{
		{
			name: "cert-manager config with all fields set",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "test-namespace"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					TLS: &ecv1alpha1.TLSCertificate{
						Provider: string(certificate.CertManager),
						ProviderCfg: ecv1alpha1.ProviderConfig{
							CertManagerCfg: &ecv1alpha1.ProviderCertManagerConfig{
								CommonConfig: ecv1alpha1.CommonConfig{
									CommonName:       "cm.example.com",
									Organization:     []string{"CM Org"},
									ValidityDuration: "1440h",
									AltNames: ecv1alpha1.AltNames{
										DNSNames: []string{"cm1.example.com", "cm2.example.com"},
									},
								},
								IssuerName: "test-issuer",
								IssuerKind: "ClusterIssuer",
							},
						},
					},
				},
			},
			expected: &certInterface.Config{
				CommonName:       "cm.example.com",
				Organization:     []string{"CM Org"},
				ValidityDuration: 1440 * time.Hour,
				AltNames: certInterface.AltNames{
					DNSNames: []string{"cm1.example.com", "cm2.example.com"},
					IPs:      make([]net.IP, 2),
				},
				ExtraConfig: map[string]any{
					"issuerName": "test-issuer",
					"issuerKind": "ClusterIssuer",
				},
			},
			wantErr: false,
		},
		{
			name: "cert-manager config with nil CertManagerCfg",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "test-namespace"},
				Spec: ecv1alpha1.EtcdClusterSpec{
					TLS: &ecv1alpha1.TLSCertificate{
						Provider:    string(certificate.CertManager),
						ProviderCfg: ecv1alpha1.ProviderConfig{CertManagerCfg: nil},
					},
				},
			},
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := createCMCertificateConfig(tt.ec)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.expected.CommonName, result.CommonName)
				assert.Equal(t, tt.expected.Organization, result.Organization)
				assert.Equal(t, tt.expected.ValidityDuration, result.ValidityDuration)
				assert.Equal(t, tt.expected.AltNames.DNSNames, result.AltNames.DNSNames)
				assert.Equal(t, tt.expected.AltNames.IPs, result.AltNames.IPs)
				assert.Equal(t, tt.expected.ExtraConfig, result.ExtraConfig)
			}
		})
	}
}
