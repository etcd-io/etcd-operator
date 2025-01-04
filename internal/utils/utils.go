package utils

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strconv"
	"strings"
	"time"
)

const (
	SSSmallerThanDesired = "SSSmallerThanDesired"
	SSBiggerThanDesired  = "SSBiggerThanDesired"
	SSEqualToDesired     = "SSEqualToDesired"
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
func CreateOrPatchSS(ctx context.Context, logger logr.Logger, ec *ecv1alpha1.EtcdCluster, c *client.Client, replicas int32, scheme *runtime.Scheme) error {
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

	if err := validateOwner(ec, sts); err != nil {
		return err
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
					"--listen-peer-urls=http://0.0.0.0:2380",   //TODO: only listen on 127.0.0.1 and host IP
					"--listen-client-urls=http://0.0.0.0:2379", //TODO: only listen on 127.0.0.1 and host IP
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

	logger.Info("Now updating configmap", "name", configMapNameForEtcdCluster(ec), "namespace", ec.Namespace)
	err = applyEtcdClusterState(ctx, ec, int(replicas), *c, scheme)
	if err != nil {
		return err
	}

	logger.Info("Now creating/updating statefulset", "name", ec.Name, "namespace", ec.Namespace, "replicas", replicas)
	_, err = controllerutil.CreateOrPatch(ctx, *c, sts, func() error {
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
	logger.Info("Now checking if stateful set is ready", "name", ec.Name, "namespace", ec.Namespace)
	ok, err := WaitForStatefulSetReady(ctx, logger, *c, ec.Name, ec.Namespace)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("StatefulSet %s/%s is not ready", ec.Namespace, ec.Name)
	}

	//err = SetControllerReference(ctx, ec, *c, scheme)
	//if err != nil {
	//	return err
	//}

	err = CheckStatefulSetControlledByEtcdOperator(ctx, *c, ec)
	if err != nil {
		return err
	}
	//os.Exit(0)

	return nil
}

func validateOwner(owner, object metav1.Object) error {
	ownerNs := owner.GetNamespace()
	if ownerNs != "" {
		objNs := object.GetNamespace()
		if objNs == "" {
			return fmt.Errorf("cluster-scoped resource must not have a namespace-scoped owner, owner's namespace %s", ownerNs)
		}
		if ownerNs != objNs {
			return fmt.Errorf("cross-namespace owner references are disallowed, owner's namespace %s, obj's namespace %s", owner.GetNamespace(), object.GetNamespace())
		}
	}
	return nil
}

func WaitForStatefulSetReady(ctx context.Context, logger logr.Logger, r client.Client, name, namespace string) (bool, error) {
	backoff := wait.Backoff{
		Duration: 3 * time.Second, // Initial wait duration
		Factor:   2.0,             // Backoff factor
		Steps:    5,               // Number of attempts
	}

	err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		// Fetch the StatefulSet
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, sts); err != nil {
			return false, err
		}

		// Check if the StatefulSet is ready
		if sts.Status.ReadyReplicas == *sts.Spec.Replicas {
			// StatefulSet is ready
			logger.Info("StatefulSet is ready", "name", name, "namespace", namespace)
			return true, nil
		}

		logger.Info("StatefulSet is not ready: ReadyReplicas=%s, DesiredReplicas=%s\n", strconv.Itoa(int(sts.Status.ReadyReplicas)), strconv.Itoa(int(*sts.Spec.Replicas)))
		return false, nil
	})

	if err != nil {
		return false, err
	}

	return true, nil
}

func CreateHeadlessServiceIfDoesntExist(ctx context.Context, logger logr.Logger, c client.Client, ec *ecv1alpha1.EtcdCluster, scheme *runtime.Scheme) error {
	service := &corev1.Service{}
	err := c.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, service)

	if err != nil {
		if errors.IsNotFound(err) {
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
			logger.Info("Headless service does not exist. Creating headless service")
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

func CheckStatefulSetControlledByEtcdOperator(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster) error {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, client.ObjectKey{Namespace: ec.Namespace, Name: ec.Name}, sts)
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(sts, ec) {
		return fmt.Errorf("StatefulSet %s/%s is not controlled by EtcdCluster %s/%s", sts.Namespace, sts.Name, ec.Namespace, ec.Name)
	}
	return nil
}

func configMapNameForEtcdCluster(ec *ecv1alpha1.EtcdCluster) string {
	return fmt.Sprintf("%s-state", ec.Name)
}

func PeerEndpointForOrdinalIndex(ec *ecv1alpha1.EtcdCluster, index int) (string, string) {
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
		name, peerURL := PeerEndpointForOrdinalIndex(ec, i)
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

func applyEtcdClusterState(ctx context.Context, ec *ecv1alpha1.EtcdCluster, replica int, c client.Client, scheme *runtime.Scheme) error {
	cm := newEtcdClusterState(ec, replica)

	err := c.Get(ctx, types.NamespacedName{Name: configMapNameForEtcdCluster(ec), Namespace: ec.Namespace}, &corev1.ConfigMap{})
	if err != nil && errors.IsNotFound(err) {
		createErr := c.Create(ctx, cm)
		return createErr
	}

	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("cannot find ConfigMap for EtcdCluster %s: %w", ec.Name, err)
	}

	if err := validateOwner(ec, cm); err != nil {
		return err
	}
	// Create a new controller ref.
	owners, err := prepareOwnerReference(ec, scheme)
	if err != nil {
		return err
	}

	cm.OwnerReferences = owners

	updateErr := c.Update(ctx, cm)
	return updateErr
}

func IsStatefulSetDesiredSize(ctx context.Context, etcdCluster *ecv1alpha1.EtcdCluster, r client.Client) (string, error) {
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKey{Name: etcdCluster.Name, Namespace: etcdCluster.Namespace}, sts)
	if err != nil {
		return "", err
	}
	if *sts.Spec.Replicas < int32(etcdCluster.Spec.Size) {
		return SSSmallerThanDesired, nil
	}

	if *sts.Spec.Replicas > int32(etcdCluster.Spec.Size) {
		return SSBiggerThanDesired, nil
	}

	if *sts.Spec.Replicas == int32(etcdCluster.Spec.Size) {
		return SSEqualToDesired, nil
	}

	return "", nil
}
