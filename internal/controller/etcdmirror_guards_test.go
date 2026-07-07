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
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

func TestEffectiveDestRangeOverlap(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		want bool
	}{
		{"nested", "/mirrored/", "/mirrored/sub/", true},
		{"nested reversed", "/mirrored/sub/", "/mirrored/", true},
		{"equal", "/mirrored/", "/mirrored/", true},
		{"disjoint", "/a/", "/b/", false},
		{"shared string prefix but disjoint ranges", "/mirrored-a/", "/mirrored-b/", false},
		{"one empty is whole keyspace", "", "/anything/", true},
		{"both empty", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, prefixRangesOverlap(tc.a, tc.b))
			assert.Equal(t, tc.want, prefixRangesOverlap(tc.b, tc.a))
		})
	}
}

func guardMirror(ns string, target ecv1alpha1.EtcdMirrorEndpoint, targetClusterID string) *ecv1alpha1.EtcdMirror {
	return &ecv1alpha1.EtcdMirror{
		ObjectMeta: metav1.ObjectMeta{Name: "g", Namespace: ns},
		Spec:       ecv1alpha1.EtcdMirrorSpec{Target: target},
		Status:     ecv1alpha1.EtcdMirrorStatus{TargetClusterID: targetClusterID},
	}
}

func TestSameTargetClusterHeuristic(t *testing.T) {
	epList := func(eps ...string) ecv1alpha1.EtcdMirrorEndpoint {
		return ecv1alpha1.EtcdMirrorEndpoint{EndpointList: eps}
	}
	svcRef := func(name, ns, port string) ecv1alpha1.EtcdMirrorEndpoint {
		return ecv1alpha1.EtcdMirrorEndpoint{
			ServiceRef: &ecv1alpha1.EtcdMirrorServiceRef{Name: name, Namespace: ns, Port: port},
		}
	}

	cases := []struct {
		name string
		a, b *ecv1alpha1.EtcdMirror
		want bool
	}{
		{
			name: "matching runtime IDs decide alone",
			a:    guardMirror("ns1", epList("a.example.com:2379"), "abc123"),
			b:    guardMirror("ns2", epList("completely-respelled.example.com:2379"), "abc123"),
			want: true,
		},
		{
			name: "differing runtime IDs override an endpoint match",
			a:    guardMirror("ns1", epList("same.example.com:2379"), "abc123"),
			b:    guardMirror("ns2", epList("same.example.com:2379"), "def456"),
			want: false,
		},
		{
			name: "endpoint normalization: scheme, case, trailing slash",
			a:    guardMirror("ns1", epList("https://ETCD.Example.com:2379/"), ""),
			b:    guardMirror("ns2", epList("etcd.example.com:2379"), ""),
			want: true,
		},
		{
			name: "nonempty endpoint intersection suffices",
			a:    guardMirror("ns1", epList("a:2379", "shared:2379"), ""),
			b:    guardMirror("ns2", epList("shared:2379", "b:2379"), ""),
			want: true,
		},
		{
			name: "disjoint endpoints",
			a:    guardMirror("ns1", epList("a:2379"), ""),
			b:    guardMirror("ns2", epList("b:2379"), ""),
			want: false,
		},
		{
			name: "serviceRef namespace defaults to the CR namespace",
			a:    guardMirror("shared-ns", svcRef("etcd-client", "", "client"), ""),
			b:    guardMirror("other-ns", svcRef("etcd-client", "shared-ns", ""), ""),
			want: true,
		},
		{
			name: "same service name in different namespaces is different",
			a:    guardMirror("ns1", svcRef("etcd-client", "", ""), ""),
			b:    guardMirror("ns2", svcRef("etcd-client", "", ""), ""),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sameCluster(targetIdentity(tc.a), targetIdentity(tc.b))
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSameTargetClusterConflictLoser(t *testing.T) {
	older := &ecv1alpha1.EtcdMirror{ObjectMeta: metav1.ObjectMeta{
		UID: "b-uid", CreationTimestamp: metav1.Unix(100, 0),
	}}
	newer := &ecv1alpha1.EtcdMirror{ObjectMeta: metav1.ObjectMeta{
		UID: "a-uid", CreationTimestamp: metav1.Unix(200, 0),
	}}
	assert.True(t, isConflictLoser(newer, older))
	assert.False(t, isConflictLoser(older, newer))

	// equal timestamps: greater UID string loses
	twinA := &ecv1alpha1.EtcdMirror{ObjectMeta: metav1.ObjectMeta{
		UID: "aaa", CreationTimestamp: metav1.Unix(100, 0),
	}}
	twinB := &ecv1alpha1.EtcdMirror{ObjectMeta: metav1.ObjectMeta{
		UID: "bbb", CreationTimestamp: metav1.Unix(100, 0),
	}}
	assert.True(t, isConflictLoser(twinB, twinA))
	assert.False(t, isConflictLoser(twinA, twinB))
}
