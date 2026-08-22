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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

func TestNextOrdinal(t *testing.T) {
	tests := []struct {
		name string
		list []int
		want int
	}{
		{
			name: "Missing pod-1 (gap in the middle)",
			list: []int{0, 2},
			want: 1,
		},
		{
			name: "Missing pod-0 (gap at the start)",
			list: []int{1, 2},
			want: 0,
		},
		{
			name: "Contiguous, no gap: next is max+1",
			list: []int{0, 1},
			want: 2,
		},
		{
			name: "Out-of-bounds numbers, lowest gap still wins",
			list: []int{0, 3, 9},
			want: 1,
		},
		{
			name: "Empty cluster state",
			list: []int{},
			want: 0,
		},
		{
			name: "Input list arrives completely unsorted",
			list: []int{2, 0},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextOrdinal(tt.list)
			assert.Equal(t, tt.want, got, "nextOrdinal(%v) should return %d", tt.list, tt.want)
		})
	}
}

func TestAllReady(t *testing.T) {
	ready := ecv1alpha1.EtcdMember{Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberReady}}
	pending := ecv1alpha1.EtcdMember{Status: ecv1alpha1.EtcdMemberStatus{Phase: ecv1alpha1.EtcdMemberPending}}

	assert.True(t, allReady(nil), "zero members should be vacuously ready")
	assert.True(t, allReady([]ecv1alpha1.EtcdMember{ready, ready}))
	assert.False(t, allReady([]ecv1alpha1.EtcdMember{ready, pending}))
}

func TestListOwnedMembers(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, ecv1alpha1.AddToScheme(scheme))

	ec := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster", Namespace: "default", UID: "cluster-uid"},
	}
	foreign := &ecv1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "other-cluster", Namespace: ec.Namespace, UID: "foreign-uid"},
	}
	makeMember := func(cluster *ecv1alpha1.EtcdCluster, ordinal int, labelCluster string) ecv1alpha1.EtcdMember {
		return ecv1alpha1.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      etcdMemberName(labelCluster, ordinal),
				Namespace: cluster.Namespace,
				Labels:    map[string]string{clusterNameLabel: labelCluster},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: ecv1alpha1.GroupVersion.String(),
					Kind:       "EtcdCluster",
					Name:       cluster.Name,
					UID:        cluster.UID,
					Controller: new(true),
				}},
			},
			Spec: ecv1alpha1.EtcdMemberSpec{ClusterName: cluster.Name, Ordinal: ordinal},
		}
	}
	member0 := makeMember(ec, 0, ec.Name)
	member2 := makeMember(ec, 2, ec.Name)
	otherMember := makeMember(foreign, 0, foreign.Name)
	// Carries this cluster's selected label but is controlled by a foreign
	// cluster: the selector lets it through, the ownership check must not.
	spoofed := makeMember(foreign, 1, ec.Name)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(ec, foreign, &member0, &member2, &otherMember, &spoofed).
		Build()

	members, err := listOwnedMembers(t.Context(), fakeClient, ec)
	require.NoError(t, err)
	require.Len(t, members, 2)
	assert.Equal(t, "my-cluster-0", members[0].Name)
	assert.Equal(t, "my-cluster-2", members[1].Name)
}
