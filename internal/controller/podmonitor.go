/*
Copyright 2025.

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
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// podMonitorGVK is the prometheus-operator PodMonitor type. We address it via
// unstructured so the operator does not take a hard dependency on the
// prometheus-operator Go module or require its CRDs to be present unless a user
// opts in to the PodMonitor feature.
var podMonitorGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "PodMonitor",
}

const defaultPodMonitorPort = "client"

// podMonitorCRDAbsent reports whether the error indicates the PodMonitor kind is
// not registered in the cluster (CRD absent), which we treat as a soft skip
// rather than a reconcile failure.
func podMonitorCRDAbsent(err error) bool {
	return err != nil && meta.IsNoMatchError(err)
}

// newPodMonitorObject returns an empty unstructured object addressing this
// cluster's PodMonitor (same namespace/name as the EtcdCluster).
func newPodMonitorObject(ec *ecv1alpha1.EtcdCluster) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(podMonitorGVK)
	u.SetNamespace(ec.Namespace)
	u.SetName(ec.Name)
	return u
}

// reconcilePodMonitor creates, updates, or deletes a PodMonitor selecting the
// cluster's etcd member pods, driven by spec.metrics.podMonitor. It never fails
// the reconcile: if the PodMonitor CRD is not installed it logs and returns. The
// PodMonitor is owned by the EtcdCluster so it is garbage-collected on deletion.
func (r *EtcdClusterReconciler) reconcilePodMonitor(ctx context.Context, ec *ecv1alpha1.EtcdCluster) {
	logger := log.FromContext(ctx)
	desired := newPodMonitorObject(ec)

	if !ec.Spec.PodMonitorEnabled() {
		// Best-effort removal of a previously created PodMonitor. Ignore a
		// missing object (already gone) and a missing CRD (never installed).
		if err := r.Delete(ctx, desired); err != nil &&
			!apierrors.IsNotFound(err) && !podMonitorCRDAbsent(err) {
			logger.Error(err, "Failed to delete PodMonitor")
		}
		return
	}

	port := defaultPodMonitorPort
	interval := ""
	var userLabels map[string]string
	if pm := ec.Spec.Metrics.PodMonitor; pm != nil {
		if pm.Port != "" {
			port = pm.Port
		}
		interval = pm.Interval
		userLabels = pm.Labels
	}

	endpoint := map[string]any{
		"port": port,
		"path": "/metrics",
	}
	if interval != "" {
		endpoint["interval"] = interval
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, desired, func() error {
		// Operator-managed labels first, then overlay any user-provided
		// labels so spec.metrics.podMonitor.labels (e.g. release selectors)
		// win on conflict while leaving the defaults intact when absent.
		labels := map[string]string{
			"app.kubernetes.io/name":       "etcd-operator",
			"app.kubernetes.io/managed-by": "etcd-operator",
			"app.kubernetes.io/instance":   ec.Name,
		}
		for k, v := range userLabels {
			labels[k] = v
		}
		desired.SetLabels(labels)
		spec := map[string]any{
			"selector": map[string]any{
				"matchLabels": map[string]any{
					"app":        ec.Name,
					"controller": ec.Name,
				},
			},
			"podMetricsEndpoints": []any{endpoint},
		}
		if err := unstructured.SetNestedMap(desired.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(ec, desired, r.Scheme)
	})
	if err != nil {
		if podMonitorCRDAbsent(err) {
			logger.Info("PodMonitor CRD not installed; skipping PodMonitor reconciliation. " +
				"Install prometheus-operator CRDs to enable spec.metrics.podMonitor.")
			return
		}
		logger.Error(err, "Failed to reconcile PodMonitor")
		return
	}
	if op != controllerutil.OperationResultNone {
		logger.Info("Reconciled PodMonitor", "operation", op)
	}
}
