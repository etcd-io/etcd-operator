// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// memberCleanupFinalizer is added to every EtcdMember at creation time so the
// Terminating leave-sequence (design doc §4.6, wired up in M3) can run before
// the object is actually removed, whether the deletion came from a
// controller-initiated scale-in or a human operator deleting it directly.
const memberCleanupFinalizer = "operator.etcd.io/member-cleanup"

// etcdMemberName returns the deterministic name for a member's EtcdMember
// object, matching today's Pod naming so both stay stable and human-readable.
func etcdMemberName(clusterName string, ordinal int) string {
	return fmt.Sprintf("%s-%d", clusterName, ordinal)
}

// listOwnedMembers returns all EtcdMembers owned by ec, sorted in ascending
// ordinal order.
func listOwnedMembers(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster) ([]ecv1alpha1.EtcdMember, error) {
	memberList := &ecv1alpha1.EtcdMemberList{}
	if err := c.List(ctx, memberList, client.InNamespace(ec.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list EtcdMembers for cluster %s: %w", ec.Name, err)
	}

	var owned []ecv1alpha1.EtcdMember
	for i := range memberList.Items {
		if metav1.IsControlledBy(&memberList.Items[i], ec) {
			owned = append(owned, memberList.Items[i])
		}
	}

	sort.Slice(owned, func(i, j int) bool {
		return owned[i].Spec.Ordinal < owned[j].Spec.Ordinal
	})
	return owned, nil
}

// memberOrdinals returns the ordinals of the given members.
func memberOrdinals(members []ecv1alpha1.EtcdMember) []int {
	ordinals := make([]int, 0, len(members))
	for _, m := range members {
		ordinals = append(ordinals, m.Spec.Ordinal)
	}
	return ordinals
}

// nextOrdinal picks the ordinal a newly-scaled-out member should use: the
// lowest gap in existing, or max(existing)+1 if there is no gap to reuse.
// Design doc §4.4. This only runs as part of an actual scale-out decision;
// there's no separate action that hunts down and closes a gap on its own.
func nextOrdinal(existing []int) int {
	sort.Ints(existing)
	for i, o := range existing {
		if i != o {
			return i
		}
	}
	return len(existing)
}

// allReady reports whether every member is Phase: Ready. Vacuously true for
// zero members, which is what lets bootstrap (creating ordinal 0) proceed
// through the §4.2 readiness gate with no special-case code.
func allReady(members []ecv1alpha1.EtcdMember) bool {
	for _, m := range members {
		if m.Status.Phase != ecv1alpha1.EtcdMemberReady {
			return false
		}
	}
	return true
}

// pickNotReadyMember returns the not-Ready member the dispatcher's §4.9 item
// 8 should advance next: an existing learner always wins (requirement 11, so
// a different member's Replacing rejoin never attempts a second
// MemberAdd(learner) while one is already pending); otherwise the
// lowest-ordinal not-Ready member (members is sorted by ordinal already, per
// listOwnedMembers). Members already claimed by an earlier dispatch step —
// Recreating (item 5) or Terminating/DeletionTimestamp set (item 7) — are
// not this step's concern.
func pickNotReadyMember(members []ecv1alpha1.EtcdMember) *ecv1alpha1.EtcdMember {
	for i := range members {
		if members[i].Status.IsLearner {
			return &members[i]
		}
	}
	for i := range members {
		m := &members[i]
		if m.DeletionTimestamp != nil {
			continue
		}
		switch m.Status.Phase {
		case ecv1alpha1.EtcdMemberPending, ecv1alpha1.EtcdMemberProvisioning, ecv1alpha1.EtcdMemberReplacing:
			return m
		}
	}
	return nil
}

// createEtcdMember creates the EtcdMember object for the given ordinal,
// owned by ec, with the cleanup finalizer already attached (design doc §4.3)
// and Phase: Pending. It does not create the member's Pod/PVC/cert or touch
// etcd membership — see design doc §4.6 ("Joining the cluster", M3).
func createEtcdMember(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster, ordinal int, scheme *runtime.Scheme) (*ecv1alpha1.EtcdMember, error) {
	member := &ecv1alpha1.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name:       etcdMemberName(ec.Name, ordinal),
			Namespace:  ec.Namespace,
			Finalizers: []string{memberCleanupFinalizer},
		},
		Spec: ecv1alpha1.EtcdMemberSpec{
			ClusterName: ec.Name,
			Ordinal:     ordinal,
			Version:     ec.Spec.Version,
		},
	}
	if err := controllerutil.SetControllerReference(ec, member, scheme); err != nil {
		return nil, err
	}
	if err := c.Create(ctx, member); err != nil {
		return nil, err
	}

	// Phase lives on the status subresource, so it has to be written in a
	// separate call after the object exists.
	member.Status.Phase = ecv1alpha1.EtcdMemberPending
	if err := c.Status().Update(ctx, member); err != nil {
		return nil, err
	}
	return member, nil
}
