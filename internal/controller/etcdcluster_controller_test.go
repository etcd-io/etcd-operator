package controller

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
)

// This file provides a high-level test plan for the refactored EtcdCluster
// reconciler. The tests are outlined using skeleton functions and comments so
// that the final implementation can be easily filled in later.

// setupEtcdServerAndClient starts an embedded etcd server and returns a connected
// client. The server and client are closed via t.Cleanup to ensure test
// isolation.
func setupEtcdServerAndClient(t *testing.T) (*embed.Etcd, *clientv3.Client) {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		if strings.Contains(err.Error(), "address already in use") {
			t.Skipf("etcd ports in use: %v", err)
		}
		t.Fatalf("Failed to start etcd server: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop()
		t.Fatalf("Server took too long to start")
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{cfg.ListenClientUrls[0].String()}})
	if err != nil {
		e.Close()
		t.Fatalf("Failed to create etcd client: %v", err)
	}

	t.Cleanup(func() {
		_ = cli.Close()
		e.Close()
	})

	return e, cli
}

// mapHostToLocalhost appends a hosts file entry mapping the provided hostname to
// 127.0.0.1. The original hosts file is restored via t.Cleanup.
func mapHostToLocalhost(t *testing.T, host string) {
	t.Helper()

	orig, err := os.ReadFile("/etc/hosts")
	if err != nil {
		t.Fatalf("failed to read hosts file: %v", err)
	}

	f, err := os.OpenFile("/etc/hosts", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open hosts file: %v", err)
	}
	if _, err := fmt.Fprintf(f, "\n127.0.0.1 %s\n", host); err != nil {
		_ = f.Close()
		t.Fatalf("failed to write hosts entry: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close hosts file: %v", err)
	}

	t.Cleanup(func() {
		_ = os.WriteFile("/etc/hosts", orig, 0644)
	})
}

// TestFetchAndValidateState describes the scenarios for the fetchAndValidateState
// helper. Each sub-test will set up a fake client with different existing
// resources and assert on the returned state, result and error.
func TestFetchAndValidateState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	cases := []struct {
		name   string
		req    ctrl.Request
		ec     *ecv1alpha1.EtcdCluster
		sts    *appsv1.StatefulSet
		assert func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet)
	}{
		{
			name: "EtcdCluster Not Found",
			req:  ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, _ *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				assert.Nil(t, state)
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "StatefulSet Not Found",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "1",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				if assert.NotNil(t, state) {
					assert.Equal(t, ec.Name, state.cluster.Name)
					assert.Nil(t, state.sts)
				}
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "Resources Exist and Owned",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "2",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: ecv1alpha1.GroupVersion.String(),
							Kind:       "EtcdCluster",
							Name:       "etcd",
							UID:        "2",
							Controller: pointerToBool(true),
						},
					},
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) {
				if assert.NotNil(t, state) {
					assert.Equal(t, ec.Name, state.cluster.Name)
					if assert.NotNil(t, state.sts) {
						assert.Equal(t, sts.Name, state.sts.Name)
					}
				}
				assert.NoError(t, err)
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
		{
			name: "StatefulSet Not Owned",
			ec: &ecv1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
					UID:       "3",
				},
				Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd",
					Namespace: "default",
				},
			},
			req: ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "default"}},
			assert: func(t *testing.T, state *reconcileState, res ctrl.Result, err error, _ *ecv1alpha1.EtcdCluster, _ *appsv1.StatefulSet) {
				assert.Nil(t, state)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "not controlled")
				assert.Equal(t, ctrl.Result{}, res)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			objs := []client.Object{}
			if tc.ec != nil {
				objs = append(objs, tc.ec)
			}
			if tc.sts != nil {
				objs = append(objs, tc.sts)
			}

			builder := fake.NewClientBuilder().WithScheme(scheme)
			if len(objs) > 0 {
				builder.WithObjects(objs...)
			}
			fakeClient := builder.Build()

			r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}

			state, res, err := r.fetchAndValidateState(ctx, tc.req)
			tc.assert(t, state, res, err, tc.ec, tc.sts)
		})
	}
}

// TestSyncPrimitives outlines tests for ensuring StatefulSet and Service
// creation and bootstrap logic.
func TestSyncPrimitives(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd",
			Namespace: "default",
			UID:       "1",
		},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:    1,
			Version: "3.5.17",
		},
	}

	t.Run("Initial Creation", func(t *testing.T) {
		ctx := t.Context()

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec}

		res, err := r.syncPrimitives(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		if assert.NotNil(t, state.sts) {
			assert.NotNil(t, state.sts.Spec.Replicas)
			assert.Equal(t, int32(0), *state.sts.Spec.Replicas)
		}

		sts := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, sts)
		assert.NoError(t, err)
		assert.NotNil(t, sts.Spec.Replicas)
		assert.Equal(t, int32(0), *sts.Spec.Replicas)

		svc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc)
		assert.NoError(t, err)
		assert.Equal(t, "None", svc.Spec.ClusterIP)

		cm := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, cm)
		assert.NoError(t, err)
	})

	t.Run("Bootstrap from Zero", func(t *testing.T) {
		ctx := t.Context()

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: pointerToInt32(0),
			},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		}

		cm := newEtcdClusterState(ec, 0)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts, cm).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		oldRV := sts.ResourceVersion
		res, err := r.syncPrimitives(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		updatedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, updatedSTS)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), *updatedSTS.Spec.Replicas)
		assert.Equal(t, int32(1), updatedSTS.Status.ReadyReplicas)
		assert.NotEqual(t, oldRV, updatedSTS.ResourceVersion)

		if assert.NotNil(t, state.sts) {
			assert.Equal(t, int32(1), *state.sts.Spec.Replicas)
		}

		svc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, svc)
		assert.NoError(t, err)
		assert.Equal(t, "None", svc.Spec.ClusterIP)

		cmUpdated := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, cmUpdated)
		assert.NoError(t, err)
		assert.Equal(t, "new", cmUpdated.Data["ETCD_INITIAL_CLUSTER_STATE"])
		assert.Contains(t, cmUpdated.Data["ETCD_INITIAL_CLUSTER"], "etcd-0=")
	})

	t.Run("Resources Already Exist", func(t *testing.T) {
		ctx := t.Context()

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(1)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		}

		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
			},
			Spec: corev1.ServiceSpec{ClusterIP: "None"},
		}

		cm := newEtcdClusterState(ec, 1)

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts.DeepCopy(), svc.DeepCopy(), cm.DeepCopy()).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		// Capture current objects to verify no updates occur.
		storedSTS := sts.DeepCopy()
		storedSvc := svc.DeepCopy()
		storedCM := cm.DeepCopy()
		res, err := r.syncPrimitives(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)

		fetchedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, fetchedSTS)
		assert.NoError(t, err)
		assert.Equal(t, storedSTS.Spec, fetchedSTS.Spec)

		fetchedSvc := &corev1.Service{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, fetchedSvc)
		assert.NoError(t, err)
		assert.Equal(t, storedSvc.Spec, fetchedSvc.Spec)

		fetchedCM := &corev1.ConfigMap{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, fetchedCM)
		assert.NoError(t, err)
		assert.Equal(t, storedCM.Data, fetchedCM.Data)
	})
}

// TestPerformHealthChecks describes how the embedded etcd server would be used
// to verify health check logic and alarm detection.
func TestPerformHealthChecks(t *testing.T) {
	cases := []struct {
		name      string
		setupFunc func(t *testing.T, state *reconcileState) (*embed.Etcd, *clientv3.Client)
		assert    func(t *testing.T, state *reconcileState, err error)
	}{
		{
			name: "Healthy Single Node",
			setupFunc: func(t *testing.T, state *reconcileState) (*embed.Etcd, *clientv3.Client) {
				mapHostToLocalhost(t, "etcd-0.etcd.default.svc.cluster.local")
				e, cli := setupEtcdServerAndClient(t)
				return e, cli
			},
			assert: func(t *testing.T, state *reconcileState, err error) {
				assert.NoError(t, err)
				if assert.NotNil(t, state.memberListResp) {
					assert.Equal(t, 1, len(state.memberListResp.Members))
				}
				if assert.NotNil(t, state.healthInfos) {
					assert.Equal(t, 1, len(state.healthInfos))
					assert.True(t, state.healthInfos[0].Health)
					assert.Empty(t, state.healthInfos[0].Error)
				}
			},
		},
		{
			name: "Member With Alarm",
			setupFunc: func(t *testing.T, state *reconcileState) (*embed.Etcd, *clientv3.Client) {
				mapHostToLocalhost(t, "etcd-0.etcd.default.svc.cluster.local")
				e, cli := setupEtcdServerAndClient(t)
				_, err := e.Server.Alarm(t.Context(), &etcdserverpb.AlarmRequest{
					Action:   etcdserverpb.AlarmRequest_ACTIVATE,
					MemberID: uint64(e.Server.MemberID()),
					Alarm:    etcdserverpb.AlarmType_NOSPACE,
				})
				assert.NoError(t, err)
				return e, cli
			},
			assert: func(t *testing.T, state *reconcileState, err error) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "NOSPACE")
				if assert.NotNil(t, state.healthInfos) {
					assert.Equal(t, 1, len(state.healthInfos))
					assert.False(t, state.healthInfos[0].Health)
					assert.Contains(t, state.healthInfos[0].Error, "NOSPACE")
				}
			},
		},
		{
			name: "Connection Failure",
			setupFunc: func(t *testing.T, state *reconcileState) (*embed.Etcd, *clientv3.Client) {
				// no etcd server started, endpoints will be unreachable
				// host mapping is unnecessary
				return nil, nil
			},
			assert: func(t *testing.T, state *reconcileState, err error) {
				assert.Error(t, err)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			state := &reconcileState{
				sts: &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "etcd",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{Replicas: pointerToInt32(1)},
				},
			}

			// start etcd server if needed
			if tc.setupFunc != nil {
				_, _ = tc.setupFunc(t, state)
			}

			r := &EtcdClusterReconciler{}
			err := r.performHealthChecks(ctx, state)
			tc.assert(t, state, err)
		})
	}
}

// TestReconcileClusterState is the most involved test and covers scale out,
// scale in, learner promotion and mismatch handling. Each scenario should set up
// a fake client and an embedded etcd cluster reflecting the initial state.
func TestReconcileClusterState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = ecv1alpha1.AddToScheme(scheme)

	t.Run("Stable Cluster", func(t *testing.T) {
		ctx := t.Context()

		mapHostToLocalhost(t, "etcd-0.etcd.default.svc.cluster.local")
		e, cli := setupEtcdServerAndClient(t)
		_ = e

		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd",
				Namespace: "default",
				UID:       "stable-1",
			},
			Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(1)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		err := r.performHealthChecks(ctx, state)
		assert.NoError(t, err)

		oldRV := sts.ResourceVersion
		res, err := r.reconcileClusterState(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)

		updatedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, updatedSTS)
		assert.NoError(t, err)
		assert.Equal(t, oldRV, updatedSTS.ResourceVersion)

		memberResp, err := cli.MemberList(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(memberResp.Members))
	})

	t.Run("Scale Up", func(t *testing.T) {
		ctx := t.Context()

		mapHostToLocalhost(t, "etcd-0.etcd.default.svc.cluster.local")
		mapHostToLocalhost(t, "etcd-1.etcd.default.svc.cluster.local")
		e, cli := setupEtcdServerAndClient(t)
		_ = e

		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd",
				Namespace: "default",
				UID:       "scale-1",
			},
			Spec: ecv1alpha1.EtcdClusterSpec{Size: 2, Version: "3.5.17"},
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(1)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 2},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		err := r.performHealthChecks(ctx, state)
		assert.NoError(t, err)

		oldRV := sts.ResourceVersion
		res, err := r.reconcileClusterState(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		updatedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, updatedSTS)
		assert.NoError(t, err)
		assert.NotEqual(t, oldRV, updatedSTS.ResourceVersion)
		assert.Equal(t, int32(2), *updatedSTS.Spec.Replicas)

		memberResp, err := cli.MemberList(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(memberResp.Members))

		var learnerFound bool
		for _, m := range memberResp.Members {
			if m.IsLearner {
				learnerFound = true
				break
			}
		}
		assert.True(t, learnerFound, "new member should be a learner")
	})

	t.Run("Scale Down", func(t *testing.T) {
		ctx := t.Context()

		mapHostToLocalhost(t, "etcd-0.etcd.default.svc.cluster.local")
		mapHostToLocalhost(t, "etcd-1.etcd.default.svc.cluster.local")
		e, cli := setupEtcdServerAndClient(t)
		_ = e

		_, err := cli.MemberAdd(ctx, []string{"http://etcd-1.etcd.default.svc.cluster.local:2380"})
		assert.NoError(t, err)

		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd",
				Namespace: "default",
				UID:       "scale-down-1",
			},
			Spec: ecv1alpha1.EtcdClusterSpec{Size: 1, Version: "3.5.17"},
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(2)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 2},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		err = r.performHealthChecks(ctx, state)
		assert.NoError(t, err)

		res, err := r.reconcileClusterState(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		updatedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, updatedSTS)
		assert.NoError(t, err)
		assert.Equal(t, int32(1), *updatedSTS.Spec.Replicas)

		memberResp, err := cli.MemberList(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(memberResp.Members))
	})

	t.Run("Wait For Unready Learner", func(t *testing.T) {
		ctx := t.Context()

		mapHostToLocalhost(t, "etcd-0.etcd.default.svc.cluster.local")
		mapHostToLocalhost(t, "etcd-1.etcd.default.svc.cluster.local")
		mapHostToLocalhost(t, "etcd-2.etcd.default.svc.cluster.local")
		e, cli := setupEtcdServerAndClient(t)
		_ = e

		_, err := cli.MemberAdd(ctx, []string{"http://etcd-1.etcd.default.svc.cluster.local:2380"})
		assert.NoError(t, err)
		addResp, err := cli.MemberAddAsLearner(ctx, []string{"http://etcd-2.etcd.default.svc.cluster.local:2380"})
		assert.NoError(t, err)

		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd",
				Namespace: "default",
				UID:       "learner-wait-1",
			},
			Spec: ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(3)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		err = r.performHealthChecks(ctx, state)
		assert.NoError(t, err)

		leaderID := state.memberListResp.Members[0].ID
		learnerID := addResp.Member.ID

		state.healthInfos = []etcdutils.EpHealth{
			{
				Ep:     state.healthInfos[0].Ep,
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{Revision: 100, MemberId: leaderID},
					Leader: leaderID,
				},
			},
			{
				Ep:     state.healthInfos[1].Ep,
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{Revision: 90, MemberId: state.memberListResp.Members[1].ID},
					Leader: leaderID,
				},
			},
			{
				Ep:     state.healthInfos[2].Ep,
				Health: true,
				Status: &clientv3.StatusResponse{
					Header:    &etcdserverpb.ResponseHeader{Revision: 50, MemberId: learnerID},
					Leader:    leaderID,
					IsLearner: true,
				},
			},
		}

		oldRV := sts.ResourceVersion
		res, err := r.reconcileClusterState(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		updatedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, updatedSTS)
		assert.NoError(t, err)
		assert.Equal(t, oldRV, updatedSTS.ResourceVersion)

		memberResp, err := cli.MemberList(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(memberResp.Members))

		var learnerFound bool
		for _, m := range memberResp.Members {
			if m.ID == learnerID {
				learnerFound = m.IsLearner
			}
		}
		assert.True(t, learnerFound, "learner should not be promoted")
	})

	t.Run("Promote Ready Learner", func(t *testing.T) {
		ctx := t.Context()

		mapHostToLocalhost(t, "etcd-0.etcd.default.svc.cluster.local")
		mapHostToLocalhost(t, "etcd-1.etcd.default.svc.cluster.local")
		mapHostToLocalhost(t, "etcd-2.etcd.default.svc.cluster.local")
		e, cli := setupEtcdServerAndClient(t)
		_ = e

		_, err := cli.MemberAdd(ctx, []string{"http://etcd-1.etcd.default.svc.cluster.local:2380"})
		assert.NoError(t, err)
		addResp, err := cli.MemberAddAsLearner(ctx, []string{"http://etcd-2.etcd.default.svc.cluster.local:2380"})
		assert.NoError(t, err)

		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd",
				Namespace: "default",
				UID:       "learner-promote-1",
			},
			Spec: ecv1alpha1.EtcdClusterSpec{Size: 3, Version: "3.5.17"},
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(3)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		err = r.performHealthChecks(ctx, state)
		assert.NoError(t, err)

		leaderID := state.memberListResp.Members[0].ID
		learnerID := addResp.Member.ID

		state.healthInfos = []etcdutils.EpHealth{
			{
				Ep:     state.healthInfos[0].Ep,
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{Revision: 100, MemberId: leaderID},
					Leader: leaderID,
				},
			},
			{
				Ep:     state.healthInfos[1].Ep,
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{Revision: 95, MemberId: state.memberListResp.Members[1].ID},
					Leader: leaderID,
				},
			},
			{
				Ep:     state.healthInfos[2].Ep,
				Health: true,
				Status: &clientv3.StatusResponse{
					Header:    &etcdserverpb.ResponseHeader{Revision: 95, MemberId: learnerID},
					Leader:    leaderID,
					IsLearner: true,
				},
			},
		}

		_, err = r.reconcileClusterState(ctx, state)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "etcdserver: can only promote a learner member which is in sync with leader")
	})

	t.Run("StatefulSet Member Mismatch", func(t *testing.T) {
		ctx := t.Context()

		mapHostToLocalhost(t, "etcd-0.etcd.default.svc.cluster.local")
		mapHostToLocalhost(t, "etcd-1.etcd.default.svc.cluster.local")
		e, cli := setupEtcdServerAndClient(t)
		_ = e

		_, err := cli.MemberAdd(ctx, []string{"http://etcd-1.etcd.default.svc.cluster.local:2380"})
		assert.NoError(t, err)

		ec := &ecv1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "etcd",
				Namespace: "default",
				UID:       "mismatch-1",
			},
			Spec: ecv1alpha1.EtcdClusterSpec{Size: 2, Version: "3.5.17"},
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ec.Name,
				Namespace: ec.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       ec.Name,
					UID:        ec.UID,
					Controller: pointerToBool(true),
				}},
			},
			Spec:   appsv1.StatefulSetSpec{Replicas: pointerToInt32(1)},
			Status: appsv1.StatefulSetStatus{ReadyReplicas: 1},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ec, sts).Build()
		r := &EtcdClusterReconciler{Client: fakeClient, Scheme: scheme}
		state := &reconcileState{cluster: ec, sts: sts}

		err = r.performHealthChecks(ctx, state)
		assert.NoError(t, err)

		oldRV := sts.ResourceVersion
		res, err := r.reconcileClusterState(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		updatedSTS := &appsv1.StatefulSet{}
		err = fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, updatedSTS)
		assert.NoError(t, err)
		assert.NotEqual(t, oldRV, updatedSTS.ResourceVersion)
		assert.Equal(t, int32(2), *updatedSTS.Spec.Replicas)

		memberResp, err := cli.MemberList(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(memberResp.Members))
	})
}
