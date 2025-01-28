package etcdutils

import (
	"context"
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

		_, err := e.Server.AddMember(context.Background(), member)
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

	t.Run("ReturnsHealthStatus", func(t *testing.T) {
		eps := []string{"http://localhost:2379"}
		health, err := ClusterHealth(eps)
		assert.NoError(t, err)
		assert.NotNil(t, health)
		assert.Greater(t, len(health), 0)
		assert.True(t, health[0].Health)
	})

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		health, err := ClusterHealth(eps)
		assert.NoError(t, err)
		assert.Equal(t, "http://invalid:2379", health[0].Ep)
		assert.Equal(t, false, health[0].Health)
		assert.Equal(t, "context deadline exceeded", health[0].Error)
	})
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
}

func TestEpHealthString(t *testing.T) {
	eh := EpHealth{
		Ep:     "http://localhost:2379",
		Health: true,
		Took:   "1s",
		Status: &clientv3.StatusResponse{
			IsLearner: true,
		},
		Error: "some error",
	}

	expected := "endpoint: http://localhost:2379, health: true, took: 1s, isLearner: trueerror: some error"
	assert.Equal(t, expected, eh.String())

	eh = EpHealth{
		Ep:     "http://localhost:2379",
		Health: false,
		Took:   "1s",
		Status: nil,
		Error:  "",
	}

	expected = "endpoint: http://localhost:2379, health: false, took: 1s"
	assert.Equal(t, expected, eh.String())
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
