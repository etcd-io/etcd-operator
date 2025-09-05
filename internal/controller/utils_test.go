package controller

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func pointerToInt32(value int32) *int32 {
	return &value
}

func TestReconcileStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = ecv1alpha1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().Build()
	logger := log.FromContext(t.Context())

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd",
			Namespace: "default",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    3,
			Version: "3.5.17",
		},
	}

	_, _ = reconcileStatefulSet(t.Context(), logger, ec, fakeClient, 3, scheme)

	sts := &appsv1.StatefulSet{}
	err := fakeClient.Get(t.Context(), client.ObjectKey{Name: "test-etcd", Namespace: "default"}, sts)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if *sts.Spec.Replicas != 3 {
		t.Fatalf("expected 3 replicas, got %d", *sts.Spec.Replicas)
	}
}

func TestWaitForStatefulSetReady(t *testing.T) {
	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name           string
		statefulSet    *appsv1.StatefulSet
		expectedResult bool
		expectedError  error
	}{
		{
			name: "StatefulSet is ready",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(3),
				},
				Status: appsv1.StatefulSetStatus{
					ReadyReplicas: 3,
				},
			},
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name: "StatefulSet is not ready",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(3),
				},
				Status: appsv1.StatefulSetStatus{
					ReadyReplicas: 2,
				},
			},
			expectedResult: false,
			expectedError:  errors.New("StatefulSet default/test-sts did not become ready: timed out waiting for the condition"),
		},
		{
			name:           "StatefulSet does not exist",
			statefulSet:    nil,
			expectedResult: false,
			expectedError:  errors.New("statefulsets.apps \"test-sts\" not found"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var clientBuilder *fake.ClientBuilder
			if tt.statefulSet != nil {
				clientBuilder = fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.statefulSet)
			} else {
				clientBuilder = fake.NewClientBuilder().WithScheme(scheme)
			}
			fakeClient := clientBuilder.Build()

			ctx := t.Context()
			logger := log.FromContext(ctx)

			err := waitForStatefulSetReady(ctx, logger, fakeClient, "test-sts", "default")
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateHeadlessServiceIfNotExist(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	// Create a fake client
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Create an EtcdCluster instance
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd",
			Namespace: "default",
		},
	}

	t.Run("creates headless service if it does not exist", func(t *testing.T) {
		err := createHeadlessServiceIfNotExist(ctx, logger, fakeClient, ec, scheme)
		assert.NoError(t, err)

		// Verify that the service was created
		service := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: "test-etcd", Namespace: "default"}, service)
		assert.NoError(t, err)
		assert.Equal(t, "None", service.Spec.ClusterIP)
		assert.Equal(t, map[string]string{
			"app":        "test-etcd",
			"controller": "test-etcd",
		}, service.Spec.Selector)
	})

	t.Run("does not create service if it already exists", func(t *testing.T) {
		// Service was already created in previous test. Call the function again to ensure no error
		err := createHeadlessServiceIfNotExist(ctx, logger, fakeClient, ec, scheme)
		assert.NoError(t, err)
	})
}

func TestClientEndpointForOrdinalIndex(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sts",
			Namespace: "default",
		},
	}

	tests := []struct {
		index          int
		expectedResult string
	}{
		{index: 0, expectedResult: "http://test-sts-0.test-sts.default.svc.cluster.local:2379"},
		{index: 1, expectedResult: "http://test-sts-1.test-sts.default.svc.cluster.local:2379"},
		{index: 2, expectedResult: "http://test-sts-2.test-sts.default.svc.cluster.local:2379"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("index %d", tt.index), func(t *testing.T) {
			result := clientEndpointForOrdinalIndex(sts, tt.index)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

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

func TestCheckStatefulSetControlledByEtcdOperator(t *testing.T) {
	tests := []struct {
		name          string
		ec            *ecv1alpha1.EtcdCluster
		sts           *appsv1.StatefulSet
		expectedError error
	}{
		{
			name: "StatefulSet controlled by EtcdCluster",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-cluster",
					Namespace: "default",
					UID:       "1234",
				},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-sts",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "ecv1alpha1/v1alpha1",
							Kind:       "EtcdCluster",
							Name:       "etcd-cluster",
							UID:        "1234",
							Controller: pointerToBool(true),
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			name: "StatefulSet not controlled by EtcdCluster",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-cluster",
					Namespace: "default",
					UID:       "1234",
				},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-sts",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "ecv1alpha1/v1alpha1",
							Kind:       "EtcdCluster",
							Name:       "other-etcd-cluster",
							UID:        "5678",
							Controller: pointerToBool(true),
						},
					},
				},
			},
			expectedError: fmt.Errorf("StatefulSet default/etcd-sts is not controlled by EtcdCluster default/etcd-cluster"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkStatefulSetControlledByEtcdOperator(tt.ec, tt.sts)

			if (err != nil) != (tt.expectedError != nil) {
				t.Errorf("expected error: %v, got: %v", tt.expectedError, err)
				return
			}

			if err != nil && err.Error() != tt.expectedError.Error() {
				t.Errorf("unexpected error: got %v, want %v", err, tt.expectedError)
			}
		})
	}
}

func pointerToBool(value bool) *bool {
	return &value
}

func TestClientEndpointsFromStatefulsets(t *testing.T) {
	tests := []struct {
		name           string
		statefulSet    *appsv1.StatefulSet
		expectedResult []string
	}{
		{
			name: "StatefulSet with 3 replicas",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(3),
				},
			},
			expectedResult: []string{
				"http://test-sts-0.test-sts.default.svc.cluster.local:2379",
				"http://test-sts-1.test-sts.default.svc.cluster.local:2379",
				"http://test-sts-2.test-sts.default.svc.cluster.local:2379",
			},
		},
		{
			name: "StatefulSet with 1 replica",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(1),
				},
			},
			expectedResult: []string{
				"http://test-sts-0.test-sts.default.svc.cluster.local:2379",
			},
		},
		{
			name: "StatefulSet with 0 replicas",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(0),
				},
			},
			expectedResult: []string(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clientEndpointsFromStatefulsets(tt.statefulSet)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestAreAllMembersHealthy(t *testing.T) {
	tests := []struct {
		name           string
		statefulSet    *appsv1.StatefulSet
		healthInfos    []etcdutils.EpHealth
		expectedResult bool
		expectedError  error
	}{
		// TODO: Add test cases for healthy members and non healthy members
		{
			name: "Error during health check",
			statefulSet: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sts",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Replicas: pointerToInt32(3),
				},
				Status: appsv1.StatefulSetStatus{
					ReadyReplicas: 3,
				},
			},
			healthInfos:    nil,
			expectedResult: false,
			expectedError:  errors.New("context deadline exceeded"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logr.Discard() // Use a no-op logger for testing

			result, err := areAllMembersHealthy(tt.statefulSet, logger)
			assert.Equal(t, tt.expectedResult, result)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestApplyEtcdClusterState(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	// Create a fake client
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Create an EtcdCluster instance
	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-etcd",
			Namespace: "default",
		},
	}

	t.Run("creates configmap if it does not exist", func(t *testing.T) {
		err := applyEtcdClusterState(ctx, ec, 3, fakeClient, scheme, logger)
		assert.NoError(t, err)

		// Verify that the configmap was created
		configMap := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: "default"}, configMap)
		assert.NoError(t, err)
		assert.Equal(t, "existing", configMap.Data["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, configMap.Data["ETCD_INITIAL_CLUSTER"], "test-etcd-0=http://test-etcd-0.test-etcd.default.svc.cluster.local:2380")
		err = fakeClient.Delete(ctx, configMap) // Delete the configmap to avoid conflicts in future tests
		assert.NoError(t, err)
	})

	t.Run("updates configmap if it already exists", func(t *testing.T) {
		// Create the configmap first
		configMap := newEtcdClusterState(ec, 3)
		err := fakeClient.Create(ctx, configMap)
		assert.NoError(t, err)

		// Call the function again to ensure it updates the configmap
		err = applyEtcdClusterState(ctx, ec, 3, fakeClient, scheme, logger)
		assert.NoError(t, err)

		// Verify that the configmap was updated
		updatedConfigMap := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: "default"}, updatedConfigMap)
		assert.NoError(t, err)
		assert.Equal(t, "existing", updatedConfigMap.Data["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, updatedConfigMap.Data["ETCD_INITIAL_CLUSTER"], "test-etcd-0=http://test-etcd-0.test-etcd.default.svc.cluster.local:2380")
	})
}

func TestCreateOrPatchStatefulSetWithPodAnnotations(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name                string
		etcdClusterName     string
		podTemplate         *ecv1alpha1.PodTemplate
		expectedAnnotations map[string]string
		expectNil           bool
	}{
		{
			name:            "creates statefulset with pod annotations",
			etcdClusterName: "test-etcd",
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
			name:                "creates statefulset without pod annotations when PodTemplate is nil",
			etcdClusterName:     "test-etcd-no-podtemplate",
			podTemplate:         nil,
			expectedAnnotations: nil,
			expectNil:           true,
		},
		{
			name:            "creates statefulset without pod annotations when annotations are empty",
			etcdClusterName: "test-etcd-empty-annotations",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Annotations: map[string]string{},
				},
			},
			expectedAnnotations: nil,
			expectNil:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fake client for each test case to avoid interference
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

			// Create an EtcdCluster instance
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.etcdClusterName,
					Namespace: "default",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{
					Size:        3,
					Version:     "3.5.17",
					PodTemplate: tt.podTemplate,
				},
			}

			err := createOrPatchStatefulSet(ctx, logger, ec, fakeClient, 3, scheme)
			assert.NoError(t, err)

			// Verify that the StatefulSet was created
			sts := &appsv1.StatefulSet{}
			err = fakeClient.Get(ctx, client.ObjectKey{Name: tt.etcdClusterName, Namespace: "default"}, sts)
			assert.NoError(t, err)

			// Check annotations
			if tt.expectNil {
				assert.Nil(t, sts.Spec.Template.Annotations)
			} else {
				assert.Equal(t, tt.expectedAnnotations, sts.Spec.Template.Annotations)
			}
		})
	}
}

func TestCreateOrPatchStatefulSetWithPodLabels(t *testing.T) {
	ctx := t.Context()
	logger := log.FromContext(ctx)

	// Create a scheme and register the necessary types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := []struct {
		name            string
		etcdClusterName string
		podTemplate     *ecv1alpha1.PodTemplate
		expectedLabels  map[string]string
	}{
		{
			name:            "creates statefulset with pod labels merged with default labels",
			etcdClusterName: "test-etcd",
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
				// Default labels that should always be present
				"app":        "test-etcd",
				"controller": "test-etcd",
				// Custom labels from PodTemplate
				"environment": "production",
				"version":     "v1.0.0",
				"team":        "platform",
			},
		},
		{
			name:            "creates statefulset with default labels when PodTemplate is nil",
			etcdClusterName: "test-etcd-no-podtemplate",
			podTemplate:     nil,
			expectedLabels: map[string]string{
				"app":        "test-etcd-no-podtemplate",
				"controller": "test-etcd-no-podtemplate",
			},
		},
		{
			name:            "creates statefulset with default labels when labels are empty",
			etcdClusterName: "test-etcd-empty-labels",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Labels: map[string]string{},
				},
			},
			expectedLabels: map[string]string{
				"app":        "test-etcd-empty-labels",
				"controller": "test-etcd-empty-labels",
			},
		},
		{
			name:            "default labels override custom labels when same key exists",
			etcdClusterName: "test-etcd-override",
			podTemplate: &ecv1alpha1.PodTemplate{
				Metadata: &ecv1alpha1.PodMetadata{
					Labels: map[string]string{
						"app":         "custom-app-name",   // Override default app label
						"controller":  "custom-controller", // Override default controller label
						"environment": "staging",
					},
				},
			},
			expectedLabels: map[string]string{
				"app":         "test-etcd-override", // Default labels are applied last, so they override custom ones
				"controller":  "test-etcd-override", // Default labels are applied last, so they override custom ones
				"environment": "staging",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fake client for each test case to avoid interference
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

			// Create an EtcdCluster instance
			ec := &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.etcdClusterName,
					Namespace: "default",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{
					Size:        3,
					Version:     "3.5.17",
					PodTemplate: tt.podTemplate,
				},
			}

			err := createOrPatchStatefulSet(ctx, logger, ec, fakeClient, 3, scheme)
			assert.NoError(t, err)

			// Verify that the StatefulSet was created with correct labels
			sts := &appsv1.StatefulSet{}
			err = fakeClient.Get(ctx, client.ObjectKey{Name: tt.etcdClusterName, Namespace: "default"}, sts)
			assert.NoError(t, err)

			// Check that pod template has the expected labels
			assert.Equal(t, tt.expectedLabels, sts.Spec.Template.Labels)
		})
	}
}

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
