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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.etcd.io,resources=etcdclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the EtcdCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.1/pkg/reconcile
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the etcd cluster resource
	var etcdCluster operatorv1alpha1.EtcdCluster
	if err := r.Get(ctx, req.NamespacedName, &etcdCluster); err != nil {
		log.Error(err, "unable to fetch etcd cluster")
		return ctrl.Result{}, nil
	}
	// Check if StatefulSet already exists
	var sts appsv1.StatefulSet
	err := r.Get(ctx, req.NamespacedName, &sts)
	// If it doesn't exist and Size is specified > 0 then create it with 0 replicas
	// End Reconcile loop otherwise
	if apierrors.IsNotFound(err) {
		if etcdCluster.Spec.Size > 0 {
			log.Info("creating statefulset with replica = 0")
			if err := r.CreateStatefulset(ctx, &etcdCluster); err != nil {
				log.Error(err, "failed to create StatefulSet")
			}
		}
		return ctrl.Result{}, nil
	} else if err != nil {
		log.Error(err, "failed to create StatefulSet")
		return ctrl.Result{}, err
	}
	log.Info("StatefulSet exists")

	return ctrl.Result{}, nil
}

// Create Statefulset
func (r *EtcdClusterReconciler) CreateStatefulset(ctx context.Context, etcdCluster *operatorv1alpha1.EtcdCluster) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdCluster.ObjectMeta.Name,
			Namespace: etcdCluster.ObjectMeta.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(int32(0)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": etcdCluster.ObjectMeta.Name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": etcdCluster.ObjectMeta.Name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "etcd",
							Image: "quay.io/coreos/etcd:v2.3.8",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 2379},
								{ContainerPort: 2380},
							},
						},
					},
				},
			},
		},
	}
	// Set EtcdCluster as the owner and controller
	if err := ctrl.SetControllerReference(etcdCluster, sts, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, sts)
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.EtcdCluster{}).
		Named("etcdcluster").
		Complete(r)
}
