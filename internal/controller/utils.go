package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func prepareOwnerReference(ec *ecv1alpha1.EtcdCluster, scheme *runtime.Scheme) ([]metav1.OwnerReference, error) {
	gvk, err := apiutil.GVKForObject(ec, scheme)
	if err != nil {
		return []metav1.OwnerReference{}, err
	}
	ref := metav1.OwnerReference{
		APIVersion:         gvk.GroupVersion().String(),
		Kind:               gvk.Kind,
		Name:               ec.GetName(),
		UID:                ec.GetUID(),
		BlockOwnerDeletion: ptr.To(true),
		Controller:         ptr.To(true),
	}

	var owners []metav1.OwnerReference
	owners = append(owners, ref)
	return owners, nil
}

func reconcileStatefulSet(ctx context.Context, logger logr.Logger, ec *ecv1alpha1.EtcdCluster, c client.Client, replicas int32, scheme *runtime.Scheme) (*appsv1.StatefulSet, error) {

	// prepare/update configmap for StatefulSet
	err := applyEtcdClusterState(ctx, ec, int(replicas), c, scheme, logger)
	if err != nil {
		return nil, err
	}

	// Create Update StatefulSet
	err = createOrPatchStatefulSet(ctx, logger, ec, c, replicas, scheme)
	if err != nil {
		return nil, err
	}

	// Wait for statefulset to be ready
	err = waitForStatefulSetReady(ctx, logger, c, ec.Name, ec.Namespace)
	if err != nil {
		return nil, err
	}

	// Return latest Stateful set. (This is to ensure that we return the latest statefulset for next operation to act on)
	return getStatefulSet(ctx, c, ec.Name, ec.Namespace)
}

func createOrPatchStatefulSet(ctx context.Context, logger logr.Logger, ec *ecv1alpha1.EtcdCluster, c client.Client, replicas int32, scheme *runtime.Scheme) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ec.Name,
			Namespace: ec.Namespace,
		},
	}

	labels := map[string]string{
		"app":        ec.Name,
		"controller": ec.Name,
	}

	// Create a new controller ref.
	owners, err := prepareOwnerReference(ec, scheme)
	if err != nil {
		return err
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:    "etcd",
				Command: []string{"/usr/local/bin/etcd"},
				Args: []string{
					"--name=$(POD_NAME)",
					"--listen-peer-urls=http://0.0.0.0:2380",   // TODO: only listen on 127.0.0.1 and host IP
					"--listen-client-urls=http://0.0.0.0:2379", // TODO: only listen on 127.0.0.1 and host IP
					fmt.Sprintf("--initial-advertise-peer-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", ec.Name),
					fmt.Sprintf("--advertise-client-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379", ec.Name),
				},
				Image: fmt.Sprintf("gcr.io/etcd-development/etcd:%s", ec.Spec.Version),
				Env: []corev1.EnvVar{
					{
						Name: "POD_NAME",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.name",
							},
						},
					},
					{
						Name: "POD_NAMESPACE",
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{
								FieldPath: "metadata.namespace",
							},
						},
					},
				},
				EnvFrom: []corev1.EnvFromSource{
					{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: configMapNameForEtcdCluster(ec),
							},
						},
					},
				},
				Ports: []corev1.ContainerPort{
					{
						Name:          "client",
						ContainerPort: 2379,
					},
					{
						Name:          "peer",
						ContainerPort: 2380,
					},
				},
			},
		},
	}

	logger.Info("Now creating/updating statefulset", "name", ec.Name, "namespace", ec.Namespace, "replicas", replicas)
	_, err = controllerutil.CreateOrPatch(ctx, c, sts, func() error {
		// Define or update the desired spec
		sts.ObjectMeta = metav1.ObjectMeta{
			Name:            ec.Name,
			Namespace:       ec.Namespace,
			OwnerReferences: owners,
		}
		sts.Spec = appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: ec.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: podSpec,
			},
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("Stateful set created/updated", "name", ec.Name, "namespace", ec.Namespace, "replicas", replicas)
	return nil
}

func waitForStatefulSetReady(ctx context.Context, logger logr.Logger, r client.Client, name, namespace string) error {
	logger.Info("Now checking the readiness of statefulset", "name", name, "namespace", namespace)
	// Define backoff parameters
	initialDuration := 3 * time.Second
	factor := 2.0
	maxSteps := 5

	duration := initialDuration

	for i := 0; i < maxSteps; i++ {
		// Fetch the StatefulSet
		sts, err := getStatefulSet(ctx, r, name, namespace)
		if err != nil {
			return err
		}

		// Check if the StatefulSet is ready
		if sts.Status.ReadyReplicas == *sts.Spec.Replicas {
			// StatefulSet is ready
			logger.Info("StatefulSet is ready", "name", name, "namespace", namespace)
			return nil
		}

		// Log the current status
		logger.Info("StatefulSet is not ready", "ReadyReplicas", strconv.Itoa(int(sts.Status.ReadyReplicas)), "DesiredReplicas", strconv.Itoa(int(*sts.Spec.Replicas)))

		// Wait for the backoff duration
		select {
		case <-ctx.Done():
			return ctx.Err() // Context canceled or timed out
		case <-time.After(duration):
			// Proceed to the next retry
		}

		// Increase the backoff duration
		duration = time.Duration(float64(duration) * factor)
	}

	return fmt.Errorf("StatefulSet %s/%s did not become ready after %d attempts", namespace, name, maxSteps)
}

func createHeadlessServiceIfNotExist(ctx context.Context, logger logr.Logger, c client.Client, ec *ecv1alpha1.EtcdCluster, scheme *runtime.Scheme) error {
	service := &corev1.Service{}
	err := c.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, service)

	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("Headless service does not exist. Creating headless service")
			owners, err1 := prepareOwnerReference(ec, scheme)
			if err1 != nil {
				return err1
			}
			labels := map[string]string{
				"app":        ec.Name,
				"controller": ec.Name,
			}
			// Create the headless service
			headlessSvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:            ec.Name,
					Namespace:       ec.Namespace,
					Labels:          labels,
					OwnerReferences: owners,
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "None", // Key for headless service
					Selector:  labels,
				},
			}
			if createErr := c.Create(ctx, headlessSvc); createErr != nil {
				return fmt.Errorf("failed to create headless service: %w", createErr)
			}
			logger.Info("Headless service created successfully")
			return nil
		}
		return fmt.Errorf("failed to get headless service: %w", err)
	}
	return nil
}

func checkStatefulSetControlledByEtcdOperator(ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet) error {
	if !metav1.IsControlledBy(sts, ec) {
		return fmt.Errorf("StatefulSet %s/%s is not controlled by EtcdCluster %s/%s", sts.Namespace, sts.Name, ec.Namespace, ec.Name)
	}
	return nil
}

func configMapNameForEtcdCluster(ec *ecv1alpha1.EtcdCluster) string {
	return fmt.Sprintf("%s-state", ec.Name)
}

func peerEndpointForOrdinalIndex(ec *ecv1alpha1.EtcdCluster, index int) (string, string) {
	name := fmt.Sprintf("%s-%d", ec.Name, index)
	return name, fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2380",
		ec.Name, index, ec.Name, ec.Namespace)
}

func newEtcdClusterState(ec *ecv1alpha1.EtcdCluster, replica int) *corev1.ConfigMap {
	// We always add members one by one, so the state is always
	// "existing" if replica > 1.
	state := "new"
	if replica > 1 {
		state = "existing"
	}

	var initialCluster []string
	for i := 0; i < replica; i++ {
		name, peerURL := peerEndpointForOrdinalIndex(ec, i)
		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", name, peerURL))
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapNameForEtcdCluster(ec),
			Namespace: ec.Namespace,
		},
		Data: map[string]string{
			"ETCD_INITIAL_CLUSTER_STATE": state,
			"ETCD_INITIAL_CLUSTER":       strings.Join(initialCluster, ","),
		},
	}
}

func applyEtcdClusterState(ctx context.Context, ec *ecv1alpha1.EtcdCluster, replica int, c client.Client, scheme *runtime.Scheme, logger logr.Logger) error {
	cm := newEtcdClusterState(ec, replica)

	// Create a new controller ref.
	owners, err := prepareOwnerReference(ec, scheme)
	if err != nil {
		return err
	}

	cm.OwnerReferences = owners

	logger.Info("Now updating configmap", "name", configMapNameForEtcdCluster(ec), "namespace", ec.Namespace)
	err = c.Get(ctx, types.NamespacedName{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, &corev1.ConfigMap{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			createErr := c.Create(ctx, cm)
			return createErr
		}
		return err
	}

	updateErr := c.Update(ctx, cm)
	return updateErr
}

func clientEndpointForOrdinalIndex(sts *appsv1.StatefulSet, index int) string {
	return fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379",
		sts.Name, index, sts.Name, sts.Namespace)
}

func getStatefulSet(ctx context.Context, c client.Client, name, namespace string) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, sts)
	if err != nil {
		return nil, err
	}
	return sts, nil
}

func clientEndpointsFromStatefulsets(sts *appsv1.StatefulSet) []string {
	var endpoints []string
	replica := int(*sts.Spec.Replicas)
	if replica > 0 {
		for i := 0; i < replica; i++ {
			endpoints = append(endpoints, clientEndpointForOrdinalIndex(sts, i))
		}
	}
	return endpoints
}

func areAllMembersHealthy(sts *appsv1.StatefulSet, logger logr.Logger) (bool, error) {
	_, health, err := healthCheck(sts, logger)
	if err != nil {
		return false, err
	}

	for _, h := range health {
		if !h.Health {
			return false, nil
		}
	}
	return true, nil
}

// healthCheck returns a memberList and an error.
// If any member (excluding not yet started or already removed member)
// is unhealthy, the error won't be nil.
func healthCheck(sts *appsv1.StatefulSet, lg klog.Logger) (*clientv3.MemberListResponse, []etcdutils.EpHealth, error) {
	replica := int(*sts.Spec.Replicas)
	if replica == 0 {
		return nil, nil, nil
	}

	endpoints := clientEndpointsFromStatefulsets(sts)

	memberlistResp, err := etcdutils.MemberList(endpoints)
	if err != nil {
		return nil, nil, err
	}
	memberCnt := len(memberlistResp.Members)

	// Usually replica should be equal to memberCnt. If it isn't, then
	// it means previous reconcile loop somehow interrupted right after
	// adding (replica < memberCnt) or removing (replica > memberCnt)
	// a member from the cluster. In that case, we shouldn't run health
	// check on the not yet started or already removed member.
	cnt := min(replica, memberCnt)

	lg.Info("health checking", "replica", replica, "len(members)", memberCnt)
	endpoints = endpoints[:cnt]

	healthInfos, err := etcdutils.ClusterHealth(endpoints)
	if err != nil {
		return memberlistResp, nil, err
	}

	for _, healthInfo := range healthInfos {
		if !healthInfo.Health {
			// TODO: also update metrics?
			return memberlistResp, healthInfos, errors.New(healthInfo.String())
		}
		lg.Info(healthInfo.String())
	}

	return memberlistResp, healthInfos, nil
}
