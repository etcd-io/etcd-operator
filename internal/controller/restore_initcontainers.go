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
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// restoreSourceAnnotation is the annotation the EtcdRestore controller stamps on
// the target EtcdCluster to mark it as a restore-target. Its value is a
// JSON-encoded restoreSource. The cluster controller reads it at StatefulSet
// build time: when ABSENT, the pod template is byte-identical to a normal
// cluster (zero restore init-containers, no extra volumes) so the existing
// cluster e2e is unaffected; when PRESENT, the controller injects ONE restore
// init-container (the operator image, `manager restore-localize`) that
// bootstraps the genesis member from the snapshot before etcd starts.
const restoreSourceAnnotation = "operator.etcd.io/restore-source"

const (
	// restoreInitContainerName is the single restore init-container injected on a
	// restore-target cluster. It downloads the snapshot AND restores it
	// in-process (the operator image links etcd's snapshot-restore library), so
	// there is no second image hop and no shell.
	restoreInitContainerName = "restore-localize"

	// restoreCredsVolumeName is the (optional) projected mount of the object-store
	// credentials Secret into the restore init-container only.
	restoreCredsVolumeName = "restore-creds"
)

// restoreSource is the JSON payload carried by restoreSourceAnnotation. It fully
// describes the snapshot object to fetch and the operator image to fetch+restore
// it with. Credential *values* are NOT carried here (that would leak them into
// the EtcdCluster object); the credentials Secret is referenced by name and its
// keys are surfaced as env vars into the restore init-container only.
type restoreSource struct {
	Provider       string `json:"provider"`
	Bucket         string `json:"bucket"`
	Prefix         string `json:"prefix,omitempty"`
	Key            string `json:"key"`
	Region         string `json:"region,omitempty"`
	Endpoint       string `json:"endpoint,omitempty"`
	ForcePathStyle bool   `json:"forcePathStyle,omitempty"`

	// CredsSecretName names the Secret holding object-store credentials, or "" for
	// ambient/unauthenticated access. Its keys are mapped to RESTORE_* env vars on
	// the restore init-container only (never the long-running etcd container).
	CredsSecretName string `json:"credsSecretName,omitempty"`

	// OperatorImage is the image the restore init-container runs (the operator
	// image, invoked as `manager restore-localize`). The cluster controller knows
	// its own image and stamps it here so the member pod need not guess it.
	OperatorImage string `json:"operatorImage"`

	// Generation ties the restore to the idempotency marker the init-container
	// writes on success: a pod restart with the same generation is a no-op
	// (never re-wipes-and-restores), while a genuinely new restore into a reused
	// (empty) cluster name carries a new generation. Derived from the snapshot key.
	Generation string `json:"generation"`
}

// encodeRestoreSource serializes a restoreSource for the annotation value.
func encodeRestoreSource(src restoreSource) (string, error) {
	b, err := json.Marshal(src)
	if err != nil {
		return "", fmt.Errorf("encode restore source: %w", err)
	}
	return string(b), nil
}

// decodeRestoreSource parses an annotation value into a restoreSource, rejecting
// anything missing the load-bearing fields so the controller never silently
// builds a half-configured restore pod that would boot an empty member.
func decodeRestoreSource(v string) (restoreSource, error) {
	var src restoreSource
	if err := json.Unmarshal([]byte(v), &src); err != nil {
		return src, fmt.Errorf("decode restore source annotation: %w", err)
	}
	if src.Provider == "" || src.Bucket == "" || src.Key == "" || src.OperatorImage == "" {
		return src, fmt.Errorf(
			"restore source annotation incomplete (provider=%q bucket=%q key=%q operatorImage=%q)",
			src.Provider, src.Bucket, src.Key, src.OperatorImage)
	}
	return src, nil
}

// restoreSourceFromCluster returns the decoded restore source and true if the
// EtcdCluster carries the restore annotation, false otherwise. A present-but-
// malformed annotation surfaces as an error so the StatefulSet build fails
// loudly rather than emitting a data-less member that boots empty.
func restoreSourceFromCluster(ec *ecv1alpha1.EtcdCluster) (restoreSource, bool, error) {
	v, ok := ec.Annotations[restoreSourceAnnotation]
	if !ok || v == "" {
		return restoreSource{}, false, nil
	}
	src, err := decodeRestoreSource(v)
	if err != nil {
		return restoreSource{}, true, err
	}
	return src, true, nil
}

// credEnv maps the object-store creds Secret's keys onto the RESTORE_* env vars
// the restore-localize subcommand reads. Each entry is Optional so a key absent
// for the OTHER provider (e.g. serviceAccountJSON on an S3 restore) does not
// block the pod. When CredsSecretName is "", no credential env is added and the
// subcommand falls back to ambient/unauthenticated access.
func (src restoreSource) credEnv() []corev1.EnvVar {
	if src.CredsSecretName == "" {
		return nil
	}
	ref := func(key string) *corev1.EnvVarSource {
		opt := true
		return &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: src.CredsSecretName},
				Key:                  key,
				Optional:             &opt,
			},
		}
	}
	return []corev1.EnvVar{
		{Name: "RESTORE_S3_ACCESS_KEY_ID", ValueFrom: ref("accessKeyID")},
		{Name: "RESTORE_S3_SECRET_ACCESS_KEY", ValueFrom: ref("secretAccessKey")},
		{Name: "RESTORE_S3_SESSION_TOKEN", ValueFrom: ref("sessionToken")},
		{Name: "RESTORE_GCS_SERVICE_ACCOUNT_JSON", ValueFrom: ref("serviceAccountJSON")},
	}
}

// applyRestoreInitContainers mutates podSpec to add the volume(s) and the single
// restore init-container that bootstraps the genesis member from a snapshot. It
// is invoked from createOrPatchStatefulSet ONLY when the cluster carries a valid
// restore-source annotation, so a normal cluster's pod template is never touched.
//
// Data-dir sharing: the restore init-container must write into the SAME
// filesystem the etcd container reads as ETCD_DATA_DIR (/var/lib/etcd). When the
// cluster has a StorageSpec, the etcd container already mounts the PVC there
// (added by the caller); this function mirrors that mount onto the init-
// container. When there is NO StorageSpec, the etcd container has no data volume
// at all (it writes to its ephemeral container FS, which an init-container
// cannot share), so this function adds an emptyDir data volume and mounts it on
// BOTH the etcd container and the restore init-container.
//
// Member/ordinal scoping and idempotency are enforced inside the init-container
// (restore-localize): only ordinal 0 restores, and a per-generation marker makes
// a pod restart a no-op so a reschedule never re-wipes-and-restores.
func applyRestoreInitContainers(
	podSpec *corev1.PodSpec, ec *ecv1alpha1.EtcdCluster, src restoreSource,
) {
	dataMount := corev1.VolumeMount{
		Name:        volumeName,
		MountPath:   etcdDataDir,
		SubPathExpr: "$(POD_NAME)",
	}

	// No StorageSpec: add a shared emptyDir data volume and mount it on the etcd
	// container so it boots from the restored dir.
	if ec.Spec.StorageSpec == nil {
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name:         volumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
		podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, dataMount)
	}

	// The restore init-container needs POD_NAME (ordinal guard + SubPathExpr) and
	// POD_NAMESPACE, the snapshot addressing env, the restore identity env, and
	// (optionally) the creds env.
	env := []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: "RESTORE_PROVIDER", Value: src.Provider},
		{Name: "RESTORE_BUCKET", Value: src.Bucket},
		{Name: "RESTORE_PREFIX", Value: src.Prefix},
		{Name: "RESTORE_KEY", Value: src.Key},
		{Name: "RESTORE_REGION", Value: src.Region},
		{Name: "RESTORE_ENDPOINT", Value: src.Endpoint},
		{Name: "RESTORE_FORCE_PATH_STYLE", Value: fmt.Sprintf("%t", src.ForcePathStyle)},
		{Name: "RESTORE_DATA_DIR", Value: etcdDataDir},
		{Name: "RESTORE_GENERATION", Value: src.Generation},
		// Member identity MUST match defaultArgs / the state ConfigMap so the
		// restored genesis member advertises the same name + peer URL the etcd
		// container boots with.
		{Name: "RESTORE_MEMBER_NAME", Value: "$(POD_NAME)"},
		{Name: "RESTORE_PEER_URL", Value: fmt.Sprintf(
			"http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", ec.Name)},
	}
	env = append(env, src.credEnv()...)

	restore := corev1.Container{
		Name:         restoreInitContainerName,
		Image:        src.OperatorImage,
		Command:      []string{"/manager", "restore-localize"},
		Env:          env,
		VolumeMounts: []corev1.VolumeMount{dataMount},
	}

	podSpec.InitContainers = append(podSpec.InitContainers, restore)
}
