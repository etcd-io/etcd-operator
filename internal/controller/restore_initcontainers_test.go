/*
Copyright 2024.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

func restoreTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	return scheme
}

func newEtcdClusterFixture(name string, annotations map[string]string) *ecv1alpha1.EtcdCluster {
	return &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: annotations},
		Spec: ecv1alpha1.EtcdClusterSpec{
			Size:          1,
			Version:       "v3.6.1",
			ImageRegistry: "gcr.io/etcd-development/etcd",
		},
	}
}

func buildStatefulSetForCluster(t *testing.T, ec *ecv1alpha1.EtcdCluster) *appsv1.StatefulSet {
	t.Helper()
	ctx := context.Background()
	scheme := restoreTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	require.NoError(t, createOrPatchStatefulSet(ctx, log.FromContext(ctx), ec, fakeClient, 1, scheme))
	var sts appsv1.StatefulSet
	require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, &sts))
	return &sts
}

// TestStatefulSet_NormalCluster_NoRestoreInitContainers is the regression guard
// that protects the existing cluster e2e: a cluster WITHOUT the restore-source
// annotation must yield ZERO init-containers (a byte-identical pod template), so
// injecting the restore path never triggers a fleet-wide rolling restart.
func TestStatefulSet_NormalCluster_NoRestoreInitContainers(t *testing.T) {
	ec := newEtcdClusterFixture("normal-cluster", nil)
	sts := buildStatefulSetForCluster(t, ec)
	assert.Empty(t, sts.Spec.Template.Spec.InitContainers,
		"a non-restore cluster must have zero init-containers")
	for _, v := range sts.Spec.Template.Spec.Volumes {
		assert.NotEqual(t, volumeName, v.Name, "no data volume without StorageSpec on a normal cluster")
	}
}

// TestStatefulSet_RestoreCluster_InjectsOneInitContainer proves a cluster
// carrying a valid restore-source annotation gets exactly ONE restore
// init-container (operator image, restore-localize), a shared data emptyDir, and
// the data-dir mount on the etcd container.
func TestStatefulSet_RestoreCluster_InjectsOneInitContainer(t *testing.T) {
	src := restoreSource{
		Provider: "s3", Bucket: "b", Key: "etcd-x/snap.db", Prefix: "e2e",
		Region: "us-east-1", Endpoint: "http://minio:9000", ForcePathStyle: true,
		CredsSecretName: "creds", OperatorImage: "op:img", Generation: "etcd-x/snap.db",
	}
	raw, err := encodeRestoreSource(src)
	require.NoError(t, err)
	ec := newEtcdClusterFixture("restore-cluster", map[string]string{restoreSourceAnnotation: raw})
	sts := buildStatefulSetForCluster(t, ec)

	require.Len(t, sts.Spec.Template.Spec.InitContainers, 1)
	init := sts.Spec.Template.Spec.InitContainers[0]
	assert.Equal(t, restoreInitContainerName, init.Name)
	assert.Equal(t, "op:img", init.Image)
	assert.Equal(t, []string{"/manager", "restore-localize"}, init.Command)

	var foundDataMount bool
	for _, m := range init.VolumeMounts {
		if m.Name == volumeName && m.MountPath == etcdDataDir {
			foundDataMount = true
		}
	}
	assert.True(t, foundDataMount, "restore init-container must mount the etcd data dir")

	var dataVol *corev1.Volume
	for i := range sts.Spec.Template.Spec.Volumes {
		if sts.Spec.Template.Spec.Volumes[i].Name == volumeName {
			dataVol = &sts.Spec.Template.Spec.Volumes[i]
		}
	}
	require.NotNil(t, dataVol, "a shared data volume must exist on a restore cluster")
	assert.NotNil(t, dataVol.EmptyDir)

	var etcdHasDataMount bool
	for _, m := range sts.Spec.Template.Spec.Containers[0].VolumeMounts {
		if m.Name == volumeName && m.MountPath == etcdDataDir {
			etcdHasDataMount = true
		}
	}
	assert.True(t, etcdHasDataMount, "etcd container must mount the shared restored data dir")

	envByName := map[string]string{}
	for _, e := range init.Env {
		envByName[e.Name] = e.Value
	}
	assert.Equal(t, "s3", envByName["RESTORE_PROVIDER"])
	assert.Equal(t, "b", envByName["RESTORE_BUCKET"])
	assert.Equal(t, "etcd-x/snap.db", envByName["RESTORE_KEY"])
	assert.Equal(t, etcdDataDir, envByName["RESTORE_DATA_DIR"])
	assert.Equal(t, "$(POD_NAME)", envByName["RESTORE_MEMBER_NAME"])
	assert.Contains(t, envByName["RESTORE_PEER_URL"], "restore-cluster")

	// Credentials must be wired as optional secretKeyRefs scoped to this
	// init-container, never inlined values.
	var sawCredRef bool
	for _, e := range init.Env {
		if e.Name == "RESTORE_S3_ACCESS_KEY_ID" {
			require.NotNil(t, e.ValueFrom)
			require.NotNil(t, e.ValueFrom.SecretKeyRef)
			assert.Equal(t, "creds", e.ValueFrom.SecretKeyRef.Name)
			sawCredRef = true
		}
	}
	assert.True(t, sawCredRef, "creds must reach the restore init-container via secretKeyRef")
}

// TestStatefulSet_RestoreCluster_MalformedAnnotationErrors proves a present-but-
// malformed annotation fails the StatefulSet build loudly rather than emitting a
// data-less member.
func TestStatefulSet_RestoreCluster_MalformedAnnotationErrors(t *testing.T) {
	ec := newEtcdClusterFixture("bad-restore", map[string]string{restoreSourceAnnotation: "{not json"})
	ctx := context.Background()
	scheme := restoreTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	err := createOrPatchStatefulSet(ctx, log.FromContext(ctx), ec, fakeClient, 1, scheme)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "restore-source annotation")
}
