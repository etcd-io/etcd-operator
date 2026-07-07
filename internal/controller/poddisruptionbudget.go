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

	"github.com/go-logr/logr"
	policyv1 "k8s.io/api/policy/v1"
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

func minAvailableForVotingCount(voting int) int32 {
	return int32(voting/2 + 1)
}

// reconcilePodDisruptionBudget keeps a PDB sized to the quorum of the observed
// voting members. With no observed members it leaves any existing PDB alone:
// a stale-but-protective PDB during an outage beats deleting it, and
// minAvailable 0 protects nothing.
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

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pdbNameForEtcdCluster(ec),
			Namespace: ec.Namespace,
		},
	}

	result, err := controllerutil.CreateOrPatch(ctx, c, pdb, func() error {
		minAvailable := intstr.FromInt32(minAvailableForVotingCount(voting))
		pdb.Spec.MinAvailable = &minAvailable
		// Same labels as the StatefulSet pods, so learners are covered too:
		// evicting a learner is still allowed while minAvailable voters remain.
		pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: etcdPodLabels(ec)}
		return controllerutil.SetControllerReference(ec, pdb, scheme)
	})
	if err != nil {
		return err
	}

	if result == controllerutil.OperationResultCreated || result == controllerutil.OperationResultUpdated {
		logger.Info("PodDisruptionBudget reconciled",
			"name", pdb.Name, "namespace", pdb.Namespace, "result", result,
			"minAvailable", minAvailableForVotingCount(voting))
	}
	return nil
}
