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
