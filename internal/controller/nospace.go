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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

const (
	// noSpaceDefragThrottle is the minimum time between defrag passes, so a
	// cycle that keeps restarting doesn't compact and defragment every reconcile.
	noSpaceDefragThrottle = 10 * time.Minute

	// backendQuotaFlag sets the etcd backend storage quota.
	backendQuotaFlag = "--quota-backend-bytes"

	// defaultBackendQuotaBytes is etcd's default --quota-backend-bytes (2 GiB).
	defaultBackendQuotaBytes = 2 * 1024 * 1024 * 1024

	// backendQuotaUsageLimitPercent is the backend-usage threshold below which a
	// NOSPACE cycle is considered to have reclaimed enough space to disarm.
	backendQuotaUsageLimitPercent = 90
)

// handleNoSpaceAlarm implements §4.7's NOSPACE remedy — compact, defragment (one
// member at a time, leader last), disarm — as a resumable cycle driven from the alarm snapshot.
func (r *EtcdClusterReconciler) handleNoSpaceAlarm(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	// A failed AlarmList leaves Alarms empty, which reads like
	// "the alarm is gone" — skip that case, and resume the
	// cycle on a healthy reconcile.
	if s.health == nil || !s.health.Healthy {
		return ctrl.Result{}, nil
	}

	alarms := noSpaceAlarms(s.health.Alarms)
	remediation := s.cluster.Status.NoSpaceRemediation

	if len(alarms) == 0 {
		if remediation == nil {
			return ctrl.Result{}, nil
		}
		logger.Info("[NoSpace] the NOSPACE alarm is gone, ends the remediation cycle")
		return ctrl.Result{RequeueAfter: requeueDuration}, r.clearNoSpaceRemediation(ctx, s)
	}

	if remediation == nil {
		logger.Info("[NoSpace] starting a remediation cycle")
		return ctrl.Result{RequeueAfter: requeueDuration}, r.startNoSpaceRemediationCompact(ctx, s)
	}

	switch remediation.Phase {
	case ecv1alpha1.RemediationCompacting:
		return r.reconcileNoSpaceCompacting(ctx, s)
	case ecv1alpha1.RemediationDefragmenting:
		return r.reconcileNoSpaceDefragmenting(ctx, s, alarms)
	default:
		logger.Info("[NoSpace] unknown remediation phase", "phase", remediation.Phase)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}
}

// reconcileNoSpaceCompacting runs the cycle's compaction step (§4.7).
func (r *EtcdClusterReconciler) reconcileNoSpaceCompacting(ctx context.Context, s *reconcileState) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if last := mostRecentDefragTime(s.members); last != nil && time.Since(last.Time) < noSpaceDefragThrottle {
		logger.Info("[NoSpace] a member was defragmented recently; skipping this reconcile", "lastDefragTime", last.Time)
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	if lastCompact := s.cluster.Status.LastCompactTime; lastCompact != nil && lastCompact.After(s.cluster.Status.NoSpaceRemediation.CycleStarted.Time) {
		logger.Info("[NoSpace] compaction done; moving on to defragmentation")
		return ctrl.Result{RequeueAfter: requeueDuration}, r.startNoSpaceRemediationDefrag(ctx, s)
	}

	rev, err := currentRevision(s.health)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	logger.Info("[NoSpace] compacting the keyspace", "revision", rev)
	endpoints := clientEndpointsFromPods(s.cluster.Name, s.cluster.Namespace, s.dnsDomain, s.pods, clusterTLSEnabled(s.cluster))
	err = etcdutils.Compact(etcdutils.ClientConfig{Endpoints: endpoints, TLS: s.tlsConfig}, rev)
	// ErrCompacted means rev is already compacted — by a retry after a
	// crash between the RPC and the status update, or by etcd's own
	// auto-compaction. Either way this step is done.
	if err != nil && !errors.Is(err, rpctypes.ErrCompacted) {
		return ctrl.Result{}, fmt.Errorf("compacting the keyspace for NOSPACE remediation: %w", err)
	}

	now := metav1.Now()
	s.cluster.Status.LastCompactTime = &now
	return ctrl.Result{RequeueAfter: requeueDuration}, r.Status().Update(ctx, s.cluster)
}

// reconcileNoSpaceDefragmenting defragments one member per reconcile, then
// decides whether to disarm or restart the cycle (§4.7).
func (r *EtcdClusterReconciler) reconcileNoSpaceDefragmenting(ctx context.Context, s *reconcileState, alarms []*etcdserverpb.AlarmMember) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	cycleStarted := s.cluster.Status.NoSpaceRemediation.CycleStarted.Time

	// Known limitation:
	// The compaction is non-blocking and we don't check if the
	// compaction is finished here.
	// If a member has not applied the compaction entry yet, its
	// defragment reclaims nothing.
	// The db-size check then fails and the cycle restarts.

	if target := pickMemberToDefragment(s.members, findLeaderName(s.health.Members), cycleStarted); target != nil {
		endpoint := clientEndpointForOrdinal(s.cluster.Name, s.cluster.Namespace, s.dnsDomain, target.Spec.Ordinal, clusterTLSEnabled(s.cluster))
		logger.Info("[NoSpace] defragmenting member", "member", target.Name, "endpoint", endpoint)
		err := etcdutils.Defragment(etcdutils.ClientConfig{Endpoints: []string{endpoint}, TLS: s.tlsConfig}, endpoint)
		if err != nil {
			if !etcdutils.IsTimedOut(err) {
				return ctrl.Result{}, fmt.Errorf("defragmenting the EtcdMember %q for NOSPACE remediation: %w", target.Name, err)
			}
			logger.Error(err, "[NoSpace] defragmentation timed out; the attempt is recorded and not retried this cycle", "member", target.Name)
		}

		now := metav1.Now()
		if err := r.updateEtcdMemberStatus(ctx, target, func(status *ecv1alpha1.EtcdMemberStatus) {
			status.LastDefragTime = &now
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	// Every member has been defragmented this cycle.
	if dbSizesUnderQuota(s) {
		logger.Info("[NoSpace] the keyspace was reclaimed; disarming the NOSPACE alarm")
		endpoints := clientEndpointsFromPods(s.cluster.Name, s.cluster.Namespace, s.dnsDomain, s.pods, clusterTLSEnabled(s.cluster))
		if err := etcdutils.AlarmDisarm(etcdutils.ClientConfig{Endpoints: endpoints, TLS: s.tlsConfig}, alarms); err != nil {
			return ctrl.Result{}, fmt.Errorf("disarming the NOSPACE alarm: %w", err)
		}
		return ctrl.Result{RequeueAfter: requeueDuration}, r.clearNoSpaceRemediation(ctx, s)
	}

	logger.Info("[NoSpace] the keyspace is still near quota; restarting the cycle")
	return ctrl.Result{RequeueAfter: requeueDuration}, r.startNoSpaceRemediationCompact(ctx, s)
}

// startNoSpaceRemediationCompact opens a remediation cycle at the compacting phase.
func (r *EtcdClusterReconciler) startNoSpaceRemediationCompact(ctx context.Context, s *reconcileState) error {
	s.cluster.Status.NoSpaceRemediation = &ecv1alpha1.NoSpaceRemediationStatus{
		Phase:        ecv1alpha1.RemediationCompacting,
		CycleStarted: metav1.Now(),
	}
	return r.Status().Update(ctx, s.cluster)
}

// clearNoSpaceRemediation ends the remediation cycle.
func (r *EtcdClusterReconciler) clearNoSpaceRemediation(ctx context.Context, s *reconcileState) error {
	s.cluster.Status.NoSpaceRemediation = nil
	return r.Status().Update(ctx, s.cluster)
}

// startNoSpaceRemediationDefrag moves an in-progress cycle to the
// defragmentation phase.
func (r *EtcdClusterReconciler) startNoSpaceRemediationDefrag(ctx context.Context, s *reconcileState) error {
	s.cluster.Status.NoSpaceRemediation.Phase = ecv1alpha1.RemediationDefragmenting
	return r.Status().Update(ctx, s.cluster)
}

// noSpaceAlarms filters the active alarms down to the NOSPACE ones.
func noSpaceAlarms(alarms []*etcdserverpb.AlarmMember) []*etcdserverpb.AlarmMember {
	var noSpace []*etcdserverpb.AlarmMember
	for _, alarm := range alarms {
		if alarm.Alarm == etcdserverpb.AlarmType_NOSPACE {
			noSpace = append(noSpace, alarm)
		}
	}
	return noSpace
}

// currentRevision returns the latest revision any member reported in the live
// health snapshot, used as the compaction target.
func currentRevision(health *etcdutils.ClusterHealth) (int64, error) {
	if health == nil {
		return 0, errors.New("no health report available")
	}
	var rev int64
	for _, member := range health.Members {
		if member.Status != nil && member.Status.Header != nil && member.Status.Header.Revision > rev {
			rev = member.Status.Header.Revision
		}
	}
	if rev == 0 {
		return 0, errors.New("no member reported a revision")
	}
	return rev, nil
}

// mostRecentDefragTime returns the latest LastDefragTime across the members, or
// nil when no member has ever been defragmented.
func mostRecentDefragTime(members []ecv1alpha1.EtcdMember) *metav1.Time {
	var latest *metav1.Time
	for i := range members {
		t := members[i].Status.LastDefragTime
		if t == nil {
			continue
		}
		if latest == nil || t.After(latest.Time) {
			latest = t
		}
	}
	return latest
}

// pickMemberToDefragment returns the lowest-ordinal member still needing a
// defrag this cycle (LastDefragTime unset or before cycleStarted), preferring a
// non-leader and falling back to the leader only when it is the last candidate
// ("leader last", §4.7). members is sorted by ordinal. Returns nil when every
// member has already been attempted.
func pickMemberToDefragment(members []ecv1alpha1.EtcdMember, leaderName string, cycleStarted time.Time) *ecv1alpha1.EtcdMember {
	var leader *ecv1alpha1.EtcdMember
	for i := range members {
		member := &members[i]
		if t := member.Status.LastDefragTime; t != nil && t.After(cycleStarted) {
			continue // already attempted this cycle
		}
		if member.Name == leaderName {
			if leader == nil {
				leader = member
			}
			continue
		}
		return member
	}
	return leader
}

// dbSizesUnderQuota reports whether every member's backend size is below 90% of
// the configured quota. A member whose size cannot be confirmed makes the
// verdict false: the cycle only disarms once reclamation is confirmed for all.
func dbSizesUnderQuota(s *reconcileState) bool {
	limit := backendQuotaBytes(s.cluster.Spec.EtcdOptions) * backendQuotaUsageLimitPercent / 100
	for i := range s.members {
		health, err := findHealthStatusForEtcdMember(s, &s.members[i])
		if err != nil || health.Status == nil {
			return false
		}
		if health.Status.DbSize >= limit {
			return false
		}
	}
	return true
}

// backendQuotaBytes returns the cluster's etcd backend quota, from
// --quota-backend-bytes in EtcdOptions, or etcd's default when unset.
func backendQuotaBytes(etcdOptions []string) int64 {
	for _, opt := range etcdOptions {
		if getArgName(opt) != backendQuotaFlag {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(opt, backendQuotaFlag))
		value = strings.TrimPrefix(value, "=")
		if n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return defaultBackendQuotaBytes
}
