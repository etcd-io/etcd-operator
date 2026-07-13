/*
Copyright 2026.

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
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// memberPodName returns the deterministic name for an etcd member pod.
// The naming convention mirrors StatefulSet so that headless-service DNS is identical.
func memberPodName(clusterName string, ordinal int) string {
	return fmt.Sprintf("%s-%d", clusterName, ordinal)
}

// podOrdinal extracts the numeric ordinal from a pod name of the form
// "{clusterName}-{ordinal}".  Returns -1 on parse failure.
func podOrdinal(podName, clusterName string) int {
	suffix := strings.TrimPrefix(podName, clusterName+"-")
	ordinal, err := strconv.Atoi(suffix)
	if err != nil {
		return -1
	}
	return ordinal
}

func nextPodOrdinal(currentOrdinals []int, expectedReplica int) int {
	sort.Ints(currentOrdinals)
	for i := range expectedReplica {
		if i >= len(currentOrdinals) || i != currentOrdinals[i] {
			return i
		}
	}
	return -1
}

// listOwnedPods returns all Pods that are owned (via OwnerReference) by ec,
// sorted in ascending ordinal order.
func listOwnedPods(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster) ([]*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(ec.Namespace),
		client.MatchingLabels(etcdClusterLabels(ec)),
	); err != nil {
		return nil, fmt.Errorf("failed to list pods for cluster %s: %w", ec.Name, err)
	}

	var owned []*corev1.Pod
	for i := range podList.Items {
		if metav1.IsControlledBy(&podList.Items[i], ec) {
			owned = append(owned, &podList.Items[i])
		}
	}

	sort.Slice(owned, func(i, j int) bool {
		return podOrdinal(owned[i].Name, ec.Name) < podOrdinal(owned[j].Name, ec.Name)
	})
	return owned, nil
}

// isPodReady returns true when the Pod's Ready condition is True.
func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// waitForPodReady polls until the given Pod has its Ready condition set to True,
// using an exponential back-off.  It is provided as a utility; the primary
// reconcile paths do not block on it, relying on natural requeueing instead.
func waitForPodReady(ctx context.Context, logger logr.Logger, c client.Client, podName, namespace string) error {
	logger.Info("Waiting for pod to become ready", "name", podName, "namespace", namespace)

	backoff := wait.Backoff{
		Duration: 3 * time.Second,
		Factor:   2.0,
		Steps:    5,
	}

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		pod := &corev1.Pod{}
		if err := c.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod); err != nil {
			return false, err
		}
		if isPodReady(pod) {
			logger.Info("Pod is ready", "name", podName, "namespace", namespace)
			return true, nil
		}
		logger.Info("Pod is not ready yet", "name", podName, "namespace", namespace)
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("pod %s/%s did not become ready: %w", namespace, podName, err)
	}
	return nil
}

// clientEndpointsFromPods builds the client endpoint URL for every pod in the
// slice, in the same order.  The DNS form is:
//
//	http://{podName}.{clusterName}.{namespace}.svc.cluster.local:2379
func clientEndpointsFromPods(clusterName, namespace string, pods []*corev1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	eps := make([]string, 0, len(pods))
	for _, pod := range pods {
		eps = append(eps, clientEndpointForOrdinal(clusterName, namespace, podOrdinal(pod.Name, clusterName)))
	}
	return eps
}

// clientEndpointForOrdinal returns the client endpoint URL for a member at the
// given ordinal index.
func clientEndpointForOrdinal(clusterName, namespace string, ordinal int) string {
	return fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379",
		clusterName, ordinal, clusterName, namespace)
}

// areAllMembersHealthy returns true when every entry in the supplied health
// slice reports healthy.  It uses already-fetched health data and does not make
// additional network calls.
func areAllMembersHealthy(memberHealth []etcdutils.EpHealth) bool {
	for _, h := range memberHealth {
		if !h.Health {
			return false
		}
	}
	return true
}

// healthCheck returns a MemberListResponse and per-endpoint health information
// for the etcd cluster reachable through the given pods.
func healthCheck(clusterName, namespace string, pods []*corev1.Pod, lg klog.Logger) (*clientv3.MemberListResponse, []etcdutils.EpHealth, error) {
	if len(pods) == 0 {
		return nil, nil, nil
	}

	endpoints := clientEndpointsFromPods(clusterName, namespace, pods)

	memberlistResp, err := etcdutils.MemberList(endpoints)
	if err != nil {
		return nil, nil, err
	}
	memberCnt := len(memberlistResp.Members)

	// Use the smaller of the two counts: pods that are starting up may not yet
	// appear in the member list and already-removed members may have no pod.
	cnt := min(len(pods), memberCnt)
	lg.Info("health checking", "podCount", len(pods), "len(members)", memberCnt)
	endpoints = endpoints[:cnt]

	healthInfos, err := etcdutils.ClusterHealth(endpoints)
	if err != nil {
		return memberlistResp, nil, err
	}

	var memberErrors []error
	for _, healthInfo := range healthInfos {
		if !healthInfo.Health {
			memberErrors = append(memberErrors, errors.New(healthInfo.String()))
		}
		lg.Info(healthInfo.String())
	}

	return memberlistResp, healthInfos, utilerrors.NewAggregate(memberErrors)
}

// ---------------------------------------------------------------------------
// etcd argument helpers
// ---------------------------------------------------------------------------

func defaultArgs(name string) []string {
	return []string{
		"--name=$(POD_NAME)",
		"--listen-peer-urls=http://0.0.0.0:2380",
		"--listen-client-urls=http://0.0.0.0:2379",
		fmt.Sprintf("--initial-advertise-peer-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", name),
		fmt.Sprintf("--advertise-client-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379", name),
	}
}

const (
	HashMetadataKey = "operator.etcd.io/spec-hash"
)

// buildMemberPod constructs the Pod object for a single etcd member.
func buildMemberPod(ec *ecv1alpha1.EtcdCluster, podName string, initialClusterState etcdClusterState, initialCluster string) *corev1.Pod {
	// Start with custom labels then overwrite with the mandatory defaults so
	// that the headless-service selector is always satisfied.
	labels := make(map[string]string)
	if ec.Spec.PodTemplate != nil && ec.Spec.PodTemplate.Metadata != nil {
		maps.Copy(labels, ec.Spec.PodTemplate.Metadata.Labels)
	}
	maps.Copy(labels, etcdClusterLabels(ec))

	// Apply annotations from EtcdCluster and add additional annnotations required by etcd operator
	annotations := make(map[string]string)
	if ec.Spec.PodTemplate != nil && ec.Spec.PodTemplate.Metadata != nil &&
		len(ec.Spec.PodTemplate.Metadata.Annotations) > 0 {
		maps.Copy(annotations, ec.Spec.PodTemplate.Metadata.Annotations)
	}
	annotations[HashMetadataKey] = EtcdClusterHash(ec)

	envVars := []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		},
		{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: string(initialClusterState)},
		{Name: "ETCD_INITIAL_CLUSTER", Value: initialCluster},
		{Name: "ETCD_DATA_DIR", Value: etcdDataDir},
	}

	container := corev1.Container{
		Name:    "etcd",
		Image:   fmt.Sprintf("%s:%s", ec.Spec.ImageRegistry, ec.Spec.Version),
		Command: []string{"/usr/local/bin/etcd"},
		Args:    createArgs(ec.Name, ec.Spec.EtcdOptions),
		Env:     envVars,
		Ports: []corev1.ContainerPort{
			{Name: "client", ContainerPort: 2379},
			{Name: "peer", ContainerPort: 2380},
		},
	}

	podSpec := corev1.PodSpec{
		Hostname:   podName,
		Subdomain:  ec.Name,
		Containers: []corev1.Container{container},
	}

	// Pod scheduling customisation.
	if ec.Spec.PodTemplate != nil && ec.Spec.PodTemplate.Spec != nil {
		podSpec.Affinity = ec.Spec.PodTemplate.Spec.Affinity
		podSpec.NodeSelector = ec.Spec.PodTemplate.Spec.NodeSelector
		podSpec.Tolerations = ec.Spec.PodTemplate.Spec.Tolerations
	}

	// Persistent storage volumes.
	if ec.Spec.StorageSpec != nil {
		podSpec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
			Name:      volumeName,
			MountPath: etcdDataDir,
		}}

		switch ec.Spec.StorageSpec.AccessModes {
		case corev1.ReadWriteMany:
			// All pods share a single pre-existing PVC.
			podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: ec.Spec.StorageSpec.PVCName,
					},
				},
			})
		default: // ReadWriteOnce
			podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcNameForMember(podName),
					},
				},
			})
		}
	}

	// TLS certificate volumes (mounted as secrets).
	if ec.Spec.TLS != nil {
		podSpec.Volumes = append(podSpec.Volumes,
			corev1.Volume{
				Name: "server-secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: getServerCertName(ec.Name)},
				},
			},
			corev1.Volume{
				Name: "peer-secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: getPeerCertName(ec.Name)},
				},
			},
		)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   ec.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: podSpec,
	}
}

// createMemberPod creates a single etcd member Pod (and, if needed, its PVC)
// for the given ordinal index.  It does not wait for the pod to become ready;
// the caller is responsible for requeueing until the pod is healthy.
func createMemberPod(ctx context.Context, logger logr.Logger, c client.Client, ec *ecv1alpha1.EtcdCluster, ordinal int, scheme *runtime.Scheme) error {
	podName := memberPodName(ec.Name, ordinal)

	// Ensure TLS certificates exist before the pod mounts them.
	if err := applyEtcdMemberCerts(ctx, ec, c); err != nil {
		return err
	}

	// Create per-member PVC for ReadWriteOnce storage.
	if ec.Spec.StorageSpec != nil && ec.Spec.StorageSpec.AccessModes != corev1.ReadWriteMany {
		if err := createPVCForMember(ctx, c, ec, podName, scheme); err != nil {
			return err
		}
	}

	state := etcdClusterStateExisting
	if ordinal == 0 {
		state = etcdClusterStateNew
	}

	// Build the initial-cluster value: all peers from ordinal 0 to this one.
	var clusterParts []string
	for i := range ordinal + 1 {
		name, peerURL := peerEndpointForOrdinalIndex(ec, i)
		clusterParts = append(clusterParts, fmt.Sprintf("%s=%s", name, peerURL))
	}

	pod := buildMemberPod(ec, podName, state, strings.Join(clusterParts, ","))
	if err := controllerutil.SetControllerReference(ec, pod, scheme); err != nil {
		return err
	}

	logger.Info("Creating member pod", "name", podName, "ordinal", ordinal, "state", state)
	return c.Create(ctx, pod)
}
