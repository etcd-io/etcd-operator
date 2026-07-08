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
	"fmt"
	"strings"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// effectiveDestPrefix is the range mirrored keys land under on the target
// (the first two terms of the rewrite formula).
func effectiveDestPrefix(em *ecv1alpha1.EtcdMirror) string {
	return em.Spec.Target.Prefix + em.Spec.Sync.DestPrefix
}

// prefixRangesOverlap reports whether the key ranges covered by two prefixes
// intersect. Prefix ranges are nested-or-disjoint, and the empty prefix is
// the whole keyspace, so prefix containment IS the overlap computation.
func prefixRangesOverlap(a, b string) bool {
	return a == "" || b == "" || strings.HasPrefix(a, b) || strings.HasPrefix(b, a)
}

// endpointIdentity is one CR side's best-available cluster identity: the
// runtime cluster ID when the agent has probed it, and the spec-derived
// identity (serviceRef coordinates or normalized endpoint set) before then.
type endpointIdentity struct {
	// clusterID is the hex-formatted runtime cluster ID from status
	// ("" = not yet probed).
	clusterID string
	// serviceKey is "ns/name:port" when the side uses a serviceRef.
	serviceKey string
	// endpoints is the normalized endpointList (scheme stripped, host
	// case-folded, trailing slash trimmed).
	endpoints map[string]struct{}
}

func normalizeEndpoint(ep string) string {
	ep = strings.TrimPrefix(ep, "https://")
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimSuffix(ep, "/")
	return strings.ToLower(strings.TrimSpace(ep))
}

// endpointIdentityFor builds the identity of one side of em. clusterID is the
// corresponding status field (source or target).
func endpointIdentityFor(em *ecv1alpha1.EtcdMirror, ep ecv1alpha1.EtcdMirrorEndpoint, clusterID string) endpointIdentity {
	id := endpointIdentity{clusterID: clusterID}
	if ep.ServiceRef != nil {
		ns := ep.ServiceRef.Namespace
		if ns == "" {
			ns = em.Namespace
		}
		port := ep.ServiceRef.Port
		if port == "" {
			port = defaultClientPortName
		}
		id.serviceKey = ns + "/" + ep.ServiceRef.Name + ":" + port
		return id
	}
	id.endpoints = make(map[string]struct{}, len(ep.EndpointList))
	for _, e := range ep.EndpointList {
		if n := normalizeEndpoint(e); n != "" {
			id.endpoints[n] = struct{}{}
		}
	}
	return id
}

// sameCluster decides whether two endpoint identities point at the same etcd
// cluster. When both runtime cluster IDs are known, ID equality decides alone
// — it is authoritative, survives respelled endpoints, and avoids false
// positives after an NLB DNS rotation. Otherwise fall back to the spec
// identity: same serviceRef coordinates, or a nonempty normalized-endpoint
// intersection.
func sameCluster(a, b endpointIdentity) bool {
	if a.clusterID != "" && b.clusterID != "" {
		return a.clusterID == b.clusterID
	}
	if a.serviceKey != "" && a.serviceKey == b.serviceKey {
		return true
	}
	for e := range a.endpoints {
		if _, ok := b.endpoints[e]; ok {
			return true
		}
	}
	return false
}

// mirrorConflict names a guard hit: the condition to raise and a message
// naming the sibling and the colliding ranges/IDs.
type mirrorConflict struct {
	conditionType string
	sibling       *ecv1alpha1.EtcdMirror
	message       string
}

func targetIdentity(em *ecv1alpha1.EtcdMirror) endpointIdentity {
	return endpointIdentityFor(em, em.Spec.Target, em.Status.TargetClusterID)
}

func sourceIdentity(em *ecv1alpha1.EtcdMirror) endpointIdentity {
	return endpointIdentityFor(em, em.Spec.Source, em.Status.SourceClusterID)
}

// findPrefixConflict returns the first other EtcdMirror (any namespace) whose
// effective destination range overlaps em's on the same target cluster.
func findPrefixConflict(em *ecv1alpha1.EtcdMirror, all []ecv1alpha1.EtcdMirror) *mirrorConflict {
	emTarget := targetIdentity(em)
	emRange := effectiveDestPrefix(em)
	for i := range all {
		other := &all[i]
		if other.UID == em.UID {
			continue
		}
		if !sameCluster(emTarget, targetIdentity(other)) {
			continue
		}
		otherRange := effectiveDestPrefix(other)
		if !prefixRangesOverlap(emRange, otherRange) {
			continue
		}
		return &mirrorConflict{
			conditionType: ecv1alpha1.EtcdMirrorConditionPrefixConflict,
			sibling:       other,
			message: fmt.Sprintf(
				"EtcdMirror %s/%s targets the overlapping effective destination range %q (this mirror: %q) on the same target cluster",
				other.Namespace, other.Name, otherRange, emRange),
		}
	}
	return nil
}

// selfMirror reports whether em's source and target resolve to the same
// cluster (an intra-cluster prefix copy). Cluster-level inversion is
// trivially true between any two such mirrors on one cluster — all four
// identities equal — yet no two-way loop exists, so the direction guard
// excludes them. (An intra-cluster loop through overlapping prefixes needs
// prefix awareness the cluster-level check cannot express; the agent-side
// fence-ownership backstop still catches destructive overlaps.)
func selfMirror(em *ecv1alpha1.EtcdMirror) bool {
	return sameCluster(sourceIdentity(em), targetIdentity(em))
}

// findDirectionConflict returns the first other EtcdMirror forming a two-way
// loop with em: runtime check first (both bound cluster-ID pairs known and
// exact inverses — catches respelled endpoints), then the spec-identity
// pre-runtime check (source/target identities crosswise equal).
func findDirectionConflict(em *ecv1alpha1.EtcdMirror, all []ecv1alpha1.EtcdMirror) *mirrorConflict {
	if selfMirror(em) {
		return nil
	}
	emSource, emTarget := sourceIdentity(em), targetIdentity(em)
	for i := range all {
		other := &all[i]
		if other.UID == em.UID || selfMirror(other) {
			continue
		}
		runtimeInverse := em.Status.SourceClusterID != "" && em.Status.TargetClusterID != "" &&
			other.Status.SourceClusterID == em.Status.TargetClusterID &&
			other.Status.TargetClusterID == em.Status.SourceClusterID
		specInverse := sameCluster(emSource, targetIdentity(other)) &&
			sameCluster(emTarget, sourceIdentity(other))
		if !runtimeInverse && !specInverse {
			continue
		}
		return &mirrorConflict{
			conditionType: ecv1alpha1.EtcdMirrorConditionDirectionConflict,
			sibling:       other,
			message: fmt.Sprintf(
				"EtcdMirror %s/%s mirrors the opposite direction (its source/target cluster IDs %q/%q are the inverse of this mirror's %q/%q) — two CRs forming a two-way loop",
				other.Namespace, other.Name,
				other.Status.SourceClusterID, other.Status.TargetClusterID,
				em.Status.SourceClusterID, em.Status.TargetClusterID),
		}
	}
	return nil
}

// isConflictLoser picks the CR that stops on a conflict: the newer one by
// creationTimestamp, tiebroken by the greater UID string. The conflict is
// re-evaluated every reconcile, so deleting the sibling clears it; the
// agent-side PrefixConflictError (permanent) is the backstop for anything the
// spec comparison misses.
func isConflictLoser(em, other *ecv1alpha1.EtcdMirror) bool {
	if em.CreationTimestamp.Equal(&other.CreationTimestamp) {
		return string(em.UID) > string(other.UID)
	}
	return other.CreationTimestamp.Before(&em.CreationTimestamp)
}
