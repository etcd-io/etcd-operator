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
	"fmt"

	"github.com/go-logr/logr"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func pdbNameForEtcdCluster(ec *ecv1alpha1.EtcdCluster) string {
	return ec.Name
}

func votingMemberCount(resp *clientv3.MemberListResponse) int {
	if resp == nil {
		return 0
	}
	count := 0
	for _, m := range resp.Members {
		if !m.IsLearner {
			count++
		}
	}
	return count
}

// evictableVoterCount is how many voters can be lost while keeping quorum.
func evictableVoterCount(voting int) int {
	if voting <= 0 {
		return 0
	}
	quorum := voting/2 + 1
	return voting - quorum
}

// pdbMinAvailable sizes minAvailable for a selector matching every cluster
// pod. A label selector cannot tell voters from learners (or from the pod of
// a just-removed member), so the budget must assume every permitted eviction
// lands on a voter: minAvailable = total members - evictable voters.
//
// The loop reconciles the PDB before mutating membership, so pending scale
// steps are priced in ahead of time:
//   - scale-in: RemoveMember runs before the StatefulSet shrinks, leaving the
//     removed member's pod matching the selector; size for the post-removal
//     voter set and keep the stricter value.
//   - scale-out: a learner (and its pod) is added before the PDB is next
//     updated; reserve the extra pod now.
func pdbMinAvailable(total, voting, desiredSize int) int32 {
	minAvailable := total - evictableVoterCount(voting)
	switch {
	case total > desiredSize:
		if postRemoval := total - evictableVoterCount(voting-1); postRemoval > minAvailable {
			minAvailable = postRemoval
		}
	case total < desiredSize:
		minAvailable++
	}
	return int32(minAvailable)
}

// reconcilePodDisruptionBudget keeps a PDB sized so voluntary evictions can
// never break quorum (see pdbMinAvailable). With no observed members it
// leaves any existing PDB alone: a stale-but-protective PDB during an outage
// beats deleting it, and minAvailable 0 protects nothing.
func reconcilePodDisruptionBudget(
	ctx context.Context,
	logger logr.Logger,
	c client.Client,
	ec *ecv1alpha1.EtcdCluster,
	memberListResp *clientv3.MemberListResponse,
	scheme *runtime.Scheme,
) error {
	voting := votingMemberCount(memberListResp)
	if voting == 0 {
		return nil
	}
	total := len(memberListResp.Members)

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pdbNameForEtcdCluster(ec),
			Namespace: ec.Namespace,
		},
	}

	// Never adopt a PDB this operator did not create: patching it would
	// clobber a user-managed spec, and SetControllerReference fails forever
	// when another controller owns it. Skipping keeps the permanent name
	// collision out of the requeue path.
	existing := &policyv1.PodDisruptionBudget{}
	switch err := c.Get(ctx, client.ObjectKeyFromObject(pdb), existing); {
	case err == nil:
		if !metav1.IsControlledBy(existing, ec) {
			logger.Info("Skipping PodDisruptionBudget: existing object is not controlled by this EtcdCluster",
				"name", pdb.Name, "namespace", pdb.Namespace)
			return nil
		}
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("failed to get PodDisruptionBudget %s/%s: %w", pdb.Namespace, pdb.Name, err)
	}

	minAvailable := intstr.FromInt32(pdbMinAvailable(total, voting, ec.Spec.Size))
	result, err := controllerutil.CreateOrPatch(ctx, c, pdb, func() error {
		pdb.Spec.MinAvailable = &minAvailable
		pdb.Spec.MaxUnavailable = nil // apiserver rejects specs with both fields set
		pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: etcdPodLabels(ec)}
		return controllerutil.SetControllerReference(ec, pdb, scheme)
	})
	if err != nil {
		return err
	}

	if result == controllerutil.OperationResultCreated || result == controllerutil.OperationResultUpdated {
		logger.Info("PodDisruptionBudget reconciled",
			"name", pdb.Name, "namespace", pdb.Namespace, "result", result,
			"minAvailable", minAvailable.IntValue())
	}
	return nil
}
