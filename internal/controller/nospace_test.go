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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/internal/etcdutils"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func noSpaceAlarm(memberID uint64) *etcdserverpb.AlarmMember {
	return &etcdserverpb.AlarmMember{MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE}
}

func corruptAlarm(memberID uint64) *etcdserverpb.AlarmMember {
	return &etcdserverpb.AlarmMember{MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT}
}

// memberStatus returns the health of member ordinal with the given revision
// and db size. Member IDs are ordinal+1, and member 0 is the leader.
func memberStatus(ordinal int, revision, dbSize int64) etcdutils.EpHealth {
	return etcdutils.EpHealth{
		Name:   memberPodName("etcd", ordinal),
		Health: true,
		Status: &clientv3.StatusResponse{
			Header: &etcdserverpb.ResponseHeader{MemberId: uint64(ordinal + 1), Revision: revision},
			Leader: 1,
			DbSize: dbSize,
		},
	}
}

// noSpaceHealth returns a healthy three-member report with an active NOSPACE
// alarm, member 0 as the leader, and dbSize for every member.
func noSpaceHealth(dbSize int64) *etcdutils.ClusterHealth {
	health := &etcdutils.ClusterHealth{
		Healthy: true,
		Members: map[string]etcdutils.EpHealth{},
		Alarms:  []*etcdserverpb.AlarmMember{noSpaceAlarm(1)},
	}
	for ordinal := range 3 {
		health.Members[memberPodName("etcd", ordinal)] = memberStatus(ordinal, 100, dbSize)
	}
	return health
}

func timeAgo(d time.Duration) *metav1.Time {
	t := metav1.NewTime(time.Now().Add(-d).Truncate(time.Second))
	return &t
}

func TestNoSpaceAlarms(t *testing.T) {
	t.Run("Keeps only the NOSPACE alarms, in order", func(t *testing.T) {
		alarms := []*etcdserverpb.AlarmMember{noSpaceAlarm(1), corruptAlarm(2), noSpaceAlarm(3)}
		assert.Equal(t, []*etcdserverpb.AlarmMember{alarms[0], alarms[2]}, noSpaceAlarms(alarms))
	})

	t.Run("Returns nothing without a NOSPACE alarm", func(t *testing.T) {
		assert.Empty(t, noSpaceAlarms(nil))
		assert.Empty(t, noSpaceAlarms([]*etcdserverpb.AlarmMember{corruptAlarm(1)}))
	})
}

func TestCurrentRevision(t *testing.T) {
	t.Run("Returns the highest revision any member reported", func(t *testing.T) {
		health := &etcdutils.ClusterHealth{Members: map[string]etcdutils.EpHealth{
			"etcd-0": memberStatus(0, 10, 0),
			"etcd-1": memberStatus(1, 42, 0),
			"etcd-2": memberStatus(2, 7, 0),
		}}
		rev, err := currentRevision(health)
		require.NoError(t, err)
		assert.Equal(t, int64(42), rev)
	})

	t.Run("Skips members without a status or header", func(t *testing.T) {
		health := &etcdutils.ClusterHealth{Members: map[string]etcdutils.EpHealth{
			"etcd-0": {Name: "etcd-0"},
			"etcd-1": {Name: "etcd-1", Status: &clientv3.StatusResponse{}},
			"etcd-2": memberStatus(2, 5, 0),
		}}
		rev, err := currentRevision(health)
		require.NoError(t, err)
		assert.Equal(t, int64(5), rev)
	})

	t.Run("Fails without a health report", func(t *testing.T) {
		_, err := currentRevision(nil)
		assert.Error(t, err)
	})

	t.Run("Fails when no member reported a revision", func(t *testing.T) {
		_, err := currentRevision(&etcdutils.ClusterHealth{Members: map[string]etcdutils.EpHealth{
			"etcd-0": {Name: "etcd-0"},
		}})
		assert.Error(t, err)
	})
}

func TestMostRecentDefragTime(t *testing.T) {
	t.Run("Returns nil when no member was defragmented", func(t *testing.T) {
		assert.Nil(t, mostRecentDefragTime(nil))
		assert.Nil(t, mostRecentDefragTime([]ecv1alpha1.EtcdMember{*leaveTestMember(0), *leaveTestMember(1)}))
	})

	t.Run("Returns the latest LastDefragTime", func(t *testing.T) {
		members := []ecv1alpha1.EtcdMember{*leaveTestMember(0), *leaveTestMember(1), *leaveTestMember(2)}
		members[0].Status.LastDefragTime = timeAgo(time.Hour)
		members[2].Status.LastDefragTime = timeAgo(time.Minute)
		assert.Equal(t, members[2].Status.LastDefragTime, mostRecentDefragTime(members))
	})
}

func TestPickMemberToDefragment(t *testing.T) {
	cycleStarted := time.Now().Add(-time.Hour)
	before := timeAgo(2 * time.Hour)
	after := timeAgo(time.Minute)

	cases := []struct {
		name       string
		defragTime []*metav1.Time // LastDefragTime of members 0 to 2
		leader     string
		want       string // name of the picked member, "" for nil
	}{
		{
			name:       "Picks the lowest ordinal non-leader",
			defragTime: []*metav1.Time{nil, nil, nil},
			leader:     "etcd-0",
			want:       "etcd-1",
		},
		{
			name:       "Skips members already defragmented this cycle",
			defragTime: []*metav1.Time{nil, after, nil},
			leader:     "etcd-0",
			want:       "etcd-2",
		},
		{
			name:       "Redoes members defragmented before the cycle started",
			defragTime: []*metav1.Time{after, before, after},
			leader:     "etcd-2",
			want:       "etcd-1",
		},
		{
			name:       "Picks the leader last",
			defragTime: []*metav1.Time{nil, after, after},
			leader:     "etcd-0",
			want:       "etcd-0",
		},
		{
			name:       "Picks in ordinal order without a known leader",
			defragTime: []*metav1.Time{nil, nil, nil},
			leader:     "",
			want:       "etcd-0",
		},
		{
			name:       "Returns nil when every member was defragmented this cycle",
			defragTime: []*metav1.Time{after, after, after},
			leader:     "etcd-1",
			want:       "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := make([]ecv1alpha1.EtcdMember, 0, len(tc.defragTime))
			for ordinal, defragTime := range tc.defragTime {
				m := leaveTestMember(ordinal)
				m.Status.LastDefragTime = defragTime
				members = append(members, *m)
			}

			got := pickMemberToDefragment(members, tc.leader, cycleStarted)
			if tc.want == "" {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.Name)
		})
	}
}

func TestDbSizesUnderQuota(t *testing.T) {
	const quota = 1000 // the limit is 90% of it, 900 bytes

	state := func(health *etcdutils.ClusterHealth) *reconcileState {
		cluster := leaveTestCluster()
		cluster.Spec.EtcdOptions = []string{"--quota-backend-bytes=1000"}
		members := []ecv1alpha1.EtcdMember{*leaveTestMember(0), *leaveTestMember(1), *leaveTestMember(2)}
		return &reconcileState{cluster: cluster, members: members, health: health}
	}

	t.Run("True when every member is below the limit", func(t *testing.T) {
		assert.True(t, dbSizesUnderQuota(state(noSpaceHealth(quota*90/100-1))))
	})

	t.Run("False when a member is at the limit", func(t *testing.T) {
		health := noSpaceHealth(100)
		health.Members["etcd-2"] = memberStatus(2, 100, quota*90/100)
		assert.False(t, dbSizesUnderQuota(state(health)))
	})

	t.Run("False when a member has no health entry", func(t *testing.T) {
		health := noSpaceHealth(100)
		delete(health.Members, "etcd-1")
		assert.False(t, dbSizesUnderQuota(state(health)))
	})

	t.Run("False when a member has no status", func(t *testing.T) {
		health := noSpaceHealth(100)
		health.Members["etcd-1"] = etcdutils.EpHealth{Name: "etcd-1"}
		assert.False(t, dbSizesUnderQuota(state(health)))
	})

	t.Run("False without a health report", func(t *testing.T) {
		assert.False(t, dbSizesUnderQuota(state(nil)))
	})
}

// The cases below cover the steps of the NOSPACE cycle that don't call etcd.
// Compact, Defragment and AlarmDisarm need a live cluster and are covered by
// test/e2e/nospace_test.go.
func TestHandleNoSpaceAlarm(t *testing.T) {
	// noSpaceFixture returns a reconciler for a three-member cluster whose
	// NOSPACE remediation status is remediation, after setup has changed the
	// fixture.
	noSpaceFixture := func(t *testing.T, remediation *ecv1alpha1.NoSpaceRemediationStatus, setup func(f *recoveryFixture)) (*EtcdClusterReconciler, *recoveryFixture) {
		t.Helper()
		f := newRecoveryFixture()
		f.cluster.Status.NoSpaceRemediation = remediation
		if setup != nil {
			setup(f)
		}
		return f.reconciler(t), f
	}
	// storedCluster reads the EtcdCluster back from the fake API server.
	storedCluster := func(t *testing.T, r *EtcdClusterReconciler, f *recoveryFixture) *ecv1alpha1.EtcdCluster {
		t.Helper()
		ec := &ecv1alpha1.EtcdCluster{}
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(f.cluster), ec))
		return ec
	}
	// assertUnchanged checks that the stored remediation status is still want.
	// It compares CycleStarted at second precision, as it is stored.
	assertUnchanged := func(t *testing.T, want, got *ecv1alpha1.NoSpaceRemediationStatus) {
		t.Helper()
		require.NotNil(t, got)
		assert.Equal(t, want.Phase, got.Phase)
		assert.Equal(t, want.CycleStarted.Unix(), got.CycleStarted.Unix())
	}
	compacting := func(cycleStarted *metav1.Time) *ecv1alpha1.NoSpaceRemediationStatus {
		return &ecv1alpha1.NoSpaceRemediationStatus{Phase: ecv1alpha1.RemediationCompacting, CycleStarted: *cycleStarted}
	}

	t.Run("Does nothing while the cluster is unhealthy", func(t *testing.T) {
		for _, health := range []*etcdutils.ClusterHealth{nil, {Healthy: false, Alarms: []*etcdserverpb.AlarmMember{noSpaceAlarm(1)}}} {
			r, f := noSpaceFixture(t, nil, nil)

			res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, health))
			require.NoError(t, err)
			assert.Equal(t, ctrl.Result{}, res)
			assert.Nil(t, storedCluster(t, r, f).Status.NoSpaceRemediation)
		}
	})

	t.Run("Does nothing without an alarm or a cycle", func(t *testing.T) {
		r, f := noSpaceFixture(t, nil, nil)
		health := noSpaceHealth(100)
		health.Alarms = []*etcdserverpb.AlarmMember{corruptAlarm(1)}

		res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, health))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)
		assert.Nil(t, storedCluster(t, r, f).Status.NoSpaceRemediation)
	})

	t.Run("Starts a cycle at compacting when the alarm appears", func(t *testing.T) {
		r, f := noSpaceFixture(t, nil, nil)
		start := time.Now().Truncate(time.Second)

		res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, noSpaceHealth(100)))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)

		remediation := storedCluster(t, r, f).Status.NoSpaceRemediation
		require.NotNil(t, remediation)
		assert.Equal(t, ecv1alpha1.RemediationCompacting, remediation.Phase)
		assert.False(t, remediation.CycleStarted.Time.Before(start), "CycleStarted must be the time the cycle started")
	})

	t.Run("Ends the cycle once the alarm is gone", func(t *testing.T) {
		r, f := noSpaceFixture(t, &ecv1alpha1.NoSpaceRemediationStatus{
			Phase:        ecv1alpha1.RemediationDefragmenting,
			CycleStarted: *timeAgo(time.Hour),
		}, nil)
		health := noSpaceHealth(100)
		health.Alarms = nil

		res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, health))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		assert.Nil(t, storedCluster(t, r, f).Status.NoSpaceRemediation)
	})

	t.Run("Leaves an unknown phase alone", func(t *testing.T) {
		unknown := &ecv1alpha1.NoSpaceRemediationStatus{Phase: "Unknown", CycleStarted: *timeAgo(time.Hour)}
		r, f := noSpaceFixture(t, unknown, nil)

		res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, noSpaceHealth(100)))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		assertUnchanged(t, unknown, storedCluster(t, r, f).Status.NoSpaceRemediation)
	})

	t.Run("Compacting waits while a member was defragmented recently", func(t *testing.T) {
		remediation := compacting(timeAgo(time.Hour))
		r, f := noSpaceFixture(t, remediation, func(f *recoveryFixture) {
			f.members[1].Status.LastDefragTime = timeAgo(time.Minute)
		})

		res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, noSpaceHealth(100)))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		ec := storedCluster(t, r, f)
		assertUnchanged(t, remediation, ec.Status.NoSpaceRemediation)
		assert.Nil(t, ec.Status.LastCompactTime)
	})

	t.Run("Compacting moves on to defragmenting once this cycle compacted", func(t *testing.T) {
		r, f := noSpaceFixture(t, compacting(timeAgo(time.Hour)), func(f *recoveryFixture) {
			f.cluster.Status.LastCompactTime = timeAgo(30 * time.Minute)
		})

		res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, noSpaceHealth(100)))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		remediation := storedCluster(t, r, f).Status.NoSpaceRemediation
		require.NotNil(t, remediation)
		assert.Equal(t, ecv1alpha1.RemediationDefragmenting, remediation.Phase)
	})

	t.Run("Compacting retries later when no member reported a revision", func(t *testing.T) {
		remediation := compacting(timeAgo(time.Hour))
		// A compaction from before this cycle doesn't count.
		lastCompact := timeAgo(2 * time.Hour)
		r, f := noSpaceFixture(t, remediation, func(f *recoveryFixture) {
			f.cluster.Status.LastCompactTime = lastCompact
		})
		health := noSpaceHealth(100)
		for name, member := range health.Members {
			member.Status.Header.Revision = 0
			health.Members[name] = member
		}

		res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, health))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		ec := storedCluster(t, r, f)
		assertUnchanged(t, remediation, ec.Status.NoSpaceRemediation)
		require.NotNil(t, ec.Status.LastCompactTime)
		assert.Equal(t, lastCompact.Unix(), ec.Status.LastCompactTime.Unix())
	})

	t.Run("Defragmenting restarts the cycle when space was not reclaimed", func(t *testing.T) {
		cycleStarted := timeAgo(time.Hour)
		r, f := noSpaceFixture(t, &ecv1alpha1.NoSpaceRemediationStatus{
			Phase:        ecv1alpha1.RemediationDefragmenting,
			CycleStarted: *cycleStarted,
		}, func(f *recoveryFixture) {
			for _, m := range f.members {
				m.Status.LastDefragTime = timeAgo(30 * time.Minute)
			}
		})
		// Every member was defragmented this cycle, but all of them are
		// still above 90% of the default quota.
		health := noSpaceHealth(defaultBackendQuotaBytes)

		res, err := r.handleNoSpaceAlarm(t.Context(), readState(t, r, f.cluster, health))
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{RequeueAfter: requeueDuration}, res)
		remediation := storedCluster(t, r, f).Status.NoSpaceRemediation
		require.NotNil(t, remediation)
		assert.Equal(t, ecv1alpha1.RemediationCompacting, remediation.Phase)
		assert.True(t, remediation.CycleStarted.After(cycleStarted.Time), "a restarted cycle must start now")
	})
}
