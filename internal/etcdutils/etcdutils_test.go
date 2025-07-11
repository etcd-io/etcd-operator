package etcdutils

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
)

func setupEtcdServer(t *testing.T) *embed.Etcd {
	// Create a temporary directory for the mock etcd server
	dir := t.TempDir()

	// Configure the mock etcd server
	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.Logger = "zap"
	cfg.LogLevel = "error"

	// Start the mock etcd server
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("Failed to start etcd server: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
		// Server is ready
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		t.Fatalf("Server took too long to start")
	}
	return e
}

func TestMemberList(t *testing.T) {
	e := setupEtcdServer(t)
	defer e.Close()

	t.Run("ReturnsMembers", func(t *testing.T) {
		member := membership.Member{
			ID: 0,
			RaftAttributes: membership.RaftAttributes{
				PeerURLs:  []string{"http://127.0.0.1:2380"},
				IsLearner: false,
			},
			Attributes: membership.Attributes{
				Name:       "test",
				ClientURLs: []string{"http://localhost:2379"},
			},
		}

		_, err := e.Server.AddMember(t.Context(), member)
		assert.NoError(t, err)

		eps := []string{"http://localhost:2379"}
		resp, err := MemberList(eps)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Greater(t, len(resp.Members), 0)
	})

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		resp, err := MemberList(eps)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestClusterHealth(t *testing.T) {
	e := setupEtcdServer(t)
	defer e.Close()

	// This is a valid, healthy endpoint for our tests
	healthyEp := e.Clients[0].Addr().String()

	testCases := []struct {
		name       string
		setupFunc  func(t *testing.T, e *embed.Etcd) // Optional setup for specific cases
		endpoints  []string
		assertions func(t *testing.T, health []EpHealth, err error)
	}{
		{
			name:      "ReturnsHealthStatusForSingleNode",
			endpoints: []string{healthyEp},
			assertions: func(t *testing.T, health []EpHealth, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, health)
				assert.Len(t, health, 1)
				assert.True(t, health[0].Health)
				assert.Equal(t, healthyEp, health[0].Ep)
				assert.Empty(t, health[0].Error)
			},
		},
		{
			name:      "HandlesInvalidEndpoint",
			endpoints: []string{"http://invalid:2379"},
			assertions: func(t *testing.T, health []EpHealth, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, health)
				assert.Len(t, health, 1)
				assert.False(t, health[0].Health)
				assert.Equal(t, "http://invalid:2379", health[0].Ep)
				assert.Contains(t, health[0].Error, "context deadline exceeded")
			},
		},
		{
			name:      "HandlesMixedHealthEndpoints",
			endpoints: []string{healthyEp, "http://invalid:2379"},
			assertions: func(t *testing.T, health []EpHealth, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, health)
				assert.Len(t, health, 2) // Should have results for both

				// Don't rely on sort order. Find each result and test it.
				var healthyResult, invalidResult EpHealth
				var foundHealthy, foundInvalid bool

				for _, h := range health {
					if h.Ep == healthyEp {
						healthyResult = h
						foundHealthy = true
					} else if h.Ep == "http://invalid:2379" {
						invalidResult = h
						foundInvalid = true
					}
				}

				assert.True(t, foundHealthy, "Did not find result for healthy endpoint")
				assert.True(t, foundInvalid, "Did not find result for invalid endpoint")

				// Check healthy endpoint's result
				assert.True(t, healthyResult.Health)
				assert.Empty(t, healthyResult.Error)

				// Check invalid endpoint's result
				assert.False(t, invalidResult.Health)
				assert.Contains(t, invalidResult.Error, "context deadline exceeded")
			},
		},
		{
			name: "DetectsMemberWithAlarms",
			setupFunc: func(t *testing.T, e *embed.Etcd) {
				// Activate a NOSPACE alarm on the server
				_, err := e.Server.Alarm(context.Background(), &etcdserverpb.AlarmRequest{
					Action:   etcdserverpb.AlarmRequest_ACTIVATE,
					MemberID: uint64(e.Server.MemberID()),
					Alarm:    etcdserverpb.AlarmType_NOSPACE,
				})
				assert.NoError(t, err)
			},
			endpoints: []string{healthyEp},
			assertions: func(t *testing.T, health []EpHealth, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, health)
				assert.Len(t, health, 1)

				// A member with a NOSPACE alarm is not considered fully healthy
				assert.False(t, health[0].Health)

				// Check that the error reported by the member contains the alarm type.
				assert.Contains(t, health[0].Error, "NOSPACE")

				// Verify that our alarm detection logic still works correctly.
				assert.NotEmpty(t, health[0].Alarms)
				assert.Contains(t, health[0].Alarms, "NOSPACE")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setupFunc != nil {
				tc.setupFunc(t, e)
				t.Cleanup(func() {
					// Disarm the NOSPACE alarm to clean up the state for the next test.
					_, _ = e.Server.Alarm(context.Background(), &etcdserverpb.AlarmRequest{
						Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
						MemberID: uint64(e.Server.MemberID()),
						Alarm:    etcdserverpb.AlarmType_NOSPACE,
					})
				})
			}

			health, err := ClusterHealth(tc.endpoints)
			tc.assertions(t, health, err)
		})
	}
}

func TestAddMember(t *testing.T) {
	e := setupEtcdServer(t)
	defer e.Close()

	t.Run("AddsNewMember", func(t *testing.T) {
		eps := []string{"http://localhost:2379"}
		peerURLs := []string{"http://127.0.0.1:2380"}
		resp, err := AddMember(eps, peerURLs, false)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Greater(t, len(resp.Members), 0)
	})

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		peerURLs := []string{"http://127.0.0.1:2380"}
		resp, err := AddMember(eps, peerURLs, false)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestPromoteLearner(t *testing.T) {
	e := setupEtcdServer(t)
	defer e.Close()

	t.Run("PromotesMember", func(t *testing.T) {
		eps := []string{"http://localhost:2379"}
		peerURLs := []string{"http://test123:2380"}
		addResp, err := AddMember(eps, peerURLs, true)
		assert.NoError(t, err)

		err = PromoteLearner(eps, addResp.Member.ID)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "etcdserver: can only promote a learner member which is in sync with leader")
	})

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		err := PromoteLearner(eps, 12345)
		assert.Error(t, err)
	})
}

func TestRemoveMember(t *testing.T) {
	e := setupEtcdServer(t)
	defer e.Close()

	// Positive case: Successfully remove a member
	t.Run("RemovesExistingMember", func(t *testing.T) {
		// 1. Add a new member to be removed
		// Note: The new member won't actually start, but it will exist in the member list.
		newMemberPeerURL := "http://new-member:2380"
		memberToAdd := membership.Member{
			RaftAttributes: membership.RaftAttributes{PeerURLs: []string{newMemberPeerURL}},
		}

		// The return value is the full list of members after addition.
		updatedMemberList, err := e.Server.AddMember(context.Background(), memberToAdd)
		assert.NoError(t, err)

		// Find the newly added member in the list to get its ID
		var newMemberID uint64
		foundNewMember := false
		for _, m := range updatedMemberList {
			// Find the member by the unique PeerURL we assigned.
			if len(m.PeerURLs) > 0 && m.PeerURLs[0] == newMemberPeerURL {
				newMemberID = uint64(m.ID)
				foundNewMember = true
				break
			}
		}
		assert.True(t, foundNewMember, "Failed to find the newly added member in the member list")

		// 2. Use our utility function to remove it using the correct ID
		eps := []string{e.Clients[0].Addr().String()}
		err = RemoveMember(eps, newMemberID)
		assert.NoError(t, err)

		// 3. Verify it's gone
		membersResp, err := MemberList(eps)
		assert.NoError(t, err)

		found := false
		for _, member := range membersResp.Members {
			if member.ID == newMemberID {
				found = true
				break
			}
		}
		assert.False(t, found, "Removed member should not be in the member list")
	})

	// Negative case
	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		err := RemoveMember(eps, 12345)
		assert.Error(t, err)
	})
}

func TestIsLearnerReady(t *testing.T) {
	leaderStatus := &clientv3.StatusResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: 100,
		},
	}
	learnerStatus := &clientv3.StatusResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: 90,
		},
	}

	assert.True(t, IsLearnerReady(leaderStatus, learnerStatus))

	learnerStatus.Header.Revision = 80
	assert.False(t, IsLearnerReady(leaderStatus, learnerStatus))
}

func TestFindLeaderStatus(t *testing.T) {
	logger := logr.Discard() // Use a no-op logger for testing

	t.Run("FindsLeader", func(t *testing.T) {
		healthInfos := []EpHealth{
			{
				Ep:     "http://localhost:2379",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{
						MemberId: 1,
					},
					Leader: 1,
				},
			},
			{
				Ep:     "http://localhost:2380",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{
						MemberId: 2,
					},
					Leader: 1,
				},
			},
		}

		leader, leaderStatus := FindLeaderStatus(healthInfos, logger)
		assert.Equal(t, uint64(1), leader)
		assert.NotNil(t, leaderStatus)
		assert.Equal(t, uint64(1), leaderStatus.Header.MemberId)
	})

	t.Run("NoLeaderFound", func(t *testing.T) {
		healthInfos := []EpHealth{
			{
				Ep:     "http://localhost:2379",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{
						MemberId: 1,
					},
					Leader: 2,
				},
			},
			{
				Ep:     "http://localhost:2380",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{
						MemberId: 2,
					},
					Leader: 3,
				},
			},
		}

		leader, leaderStatus := FindLeaderStatus(healthInfos, logger)
		assert.Equal(t, uint64(0), leader)
		assert.Nil(t, leaderStatus)
	})

	t.Run("HandlesNilStatus", func(t *testing.T) {
		logger := logr.Discard()
		healthInfos := []EpHealth{
			{Status: nil},
			{
				Ep:     "http://localhost:2380",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{MemberId: 2},
					Leader: 2,
				},
			},
		}
		// We just want to ensure it doesn't panic.
		// The result should be finding the leader from the valid entry.
		leader, leaderStatus := FindLeaderStatus(healthInfos, logger)
		assert.Equal(t, uint64(2), leader)
		assert.NotNil(t, leaderStatus)
	})
}

func TestFindLearnerStatus(t *testing.T) {
	logger := logr.Discard() // Use a no-op logger for testing

	t.Run("LearnerFound", func(t *testing.T) {
		healthInfos := []EpHealth{
			{
				Ep:     "http://localhost:2379",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{
						MemberId: 1,
					},
					IsLearner: true,
				},
			},
			{
				Ep:     "http://localhost:2380",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{
						MemberId: 2,
					},
					IsLearner: false,
				},
			},
		}

		learner, learnerStatus := FindLearnerStatus(healthInfos, logger)
		assert.Equal(t, uint64(1), learner)
		assert.NotNil(t, learnerStatus)
		assert.Equal(t, uint64(1), learnerStatus.Header.MemberId)
		assert.True(t, learnerStatus.IsLearner)
	})

	t.Run("NoLearnerFound", func(t *testing.T) {
		healthInfos := []EpHealth{
			{
				Ep:     "http://localhost:2379",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{
						MemberId: 1,
					},
					IsLearner: false,
				},
			},
			{
				Ep:     "http://localhost:2380",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header: &etcdserverpb.ResponseHeader{
						MemberId: 2,
					},
					IsLearner: false,
				},
			},
		}

		learner, learnerStatus := FindLearnerStatus(healthInfos, logger)
		assert.Equal(t, uint64(0), learner)
		assert.Nil(t, learnerStatus)
	})

	t.Run("HandlesNilStatus", func(t *testing.T) {
		logger := logr.Discard()
		healthInfos := []EpHealth{
			{Status: nil},
			{
				Ep:     "http://localhost:2380",
				Health: true,
				Status: &clientv3.StatusResponse{
					Header:    &etcdserverpb.ResponseHeader{MemberId: 2},
					IsLearner: false,
				},
			},
		}
		// We just want to ensure it doesn't panic.
		// The result should be finding no learner.
		learner, learnerStatus := FindLearnerStatus(healthInfos, logger)
		assert.Equal(t, uint64(0), learner)
		assert.Nil(t, learnerStatus)
	})
}

func TestEpHealthString(t *testing.T) {
	// Define test cases in a table
	testCases := []struct {
		name     string
		input    EpHealth
		expected string
	}{
		{
			name: "Basic healthy with no status",
			input: EpHealth{
				Ep:     "http://ep1:2379",
				Health: true,
				Took:   "50ms",
			},
			expected: "endpoint: http://ep1:2379, health: true, took: 50ms",
		},
		{
			name: "Unhealthy with error and no status",
			input: EpHealth{
				Ep:     "http://ep2:2379",
				Health: false,
				Took:   "5s",
				Error:  "connection refused",
			},
			expected: "endpoint: http://ep2:2379, health: false, took: 5s, error: connection refused",
		},
		{
			name: "Healthy with Status but Header is nil (case that caused panic)",
			input: EpHealth{
				Ep:     "http://ep3:2379",
				Health: true,
				Took:   "100ms",
				Status: &clientv3.StatusResponse{
					Version:   "3.5.0",
					DbSize:    12345,
					IsLearner: false,
					Header:    nil, // Important: Header is nil
				},
			},
			expected: "endpoint: http://ep3:2379, health: true, took: 100ms, version: 3.5.0, dbSize: 12345, isLearner: false",
		},
		{
			name: "Healthy with full Status and Header",
			input: EpHealth{
				Ep:     "http://leader:2379",
				Health: true,
				Took:   "20ms",
				Status: &clientv3.StatusResponse{
					Version:   "3.5.1",
					DbSize:    54321,
					IsLearner: false,
					Header:    &etcdserverpb.ResponseHeader{MemberId: 123, RaftTerm: 5},
				},
			},
			expected: "endpoint: http://leader:2379, health: true, took: 20ms, version: 3.5.1, dbSize: 54321, isLearner: false, memberId: 7b, raftTerm: 5",
		},
		{
			name: "Healthy with Alarms but no error",
			input: EpHealth{
				Ep:     "http://ep4:2379",
				Health: true,
				Took:   "30ms",
				Alarms: []string{"NOSPACE"},
			},
			expected: "endpoint: http://ep4:2379, health: true, took: 30ms, alarms: [NOSPACE]",
		},
		{
			name: "Healthy with multiple Alarms",
			input: EpHealth{
				Ep:     "http://ep5:2379",
				Health: true,
				Took:   "35ms",
				Alarms: []string{"NOSPACE", "CORRUPT"},
			},
			expected: "endpoint: http://ep5:2379, health: true, took: 35ms, alarms: [NOSPACE; CORRUPT]",
		},
		{
			name: "Kitchen sink - all fields populated",
			input: EpHealth{
				Ep:     "http://ep6:2379",
				Health: true,
				Took:   "40ms",
				Status: &clientv3.StatusResponse{
					Version:   "3.5.2",
					DbSize:    99999,
					IsLearner: true,
					Header:    &etcdserverpb.ResponseHeader{MemberId: 456, RaftTerm: 6},
				},
				Alarms: []string{"NOSPACE"},
				Error:  "internal warning",
			},
			expected: "endpoint: http://ep6:2379, health: true, took: 40ms, version: 3.5.2, dbSize: 99999, isLearner: true, memberId: 1c8, raftTerm: 6, alarms: [NOSPACE], error: internal warning",
		},
		{
			name: "Error string",
			input: EpHealth{
				Ep:     "http://localhost:2379",
				Health: true,
				Took:   "1s",
				Status: &clientv3.StatusResponse{
					// Version is empty string, DbSize is 0 in a default StatusResponse
					IsLearner: true,
				},
				Error: "some error",
			},
			// The new String() method adds ", version: , dbSize: 0" and a comma before the error
			expected: "endpoint: http://localhost:2379, health: true, took: 1s, version: , dbSize: 0, isLearner: true, error: some error",
		},
		{
			name:     "Empty struct",
			input:    EpHealth{},
			expected: "endpoint: , health: false, took: ",
		},
	}

	// Iterate over the test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.input.String()
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestHealthReportSwap(t *testing.T) {
	healthReports := healthReport{
		{Ep: "http://localhost:2379", Health: true},
		{Ep: "http://localhost:2380", Health: false},
	}

	// Swap the elements
	healthReports.Swap(0, 1)

	// Check if the elements are swapped
	assert.Equal(t, "http://localhost:2380", healthReports[0].Ep)
	assert.Equal(t, "http://localhost:2379", healthReports[1].Ep)
}
