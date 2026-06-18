package etcdutils

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
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
		resp, err := MemberList(eps, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Greater(t, len(resp.Members), 0)
	})

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		resp, err := MemberList(eps, nil)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	// The optional *tls.Config must actually be threaded onto clientv3.Config.TLS,
	// not dropped. Point the client at a raw TLS listener over an https:// endpoint:
	// a non-nil config drives a TLS handshake against the listener (which records
	// it), proving the parameter reaches the dialer.
	t.Run("NonNilTLSConfigIsApplied", func(t *testing.T) {
		ln, err := tls.Listen("tcp", "127.0.0.1:0", selfSignedServerTLS(t))
		assert.NoError(t, err)
		defer ln.Close()

		handshakeSeen := make(chan struct{}, 1)
		go func() {
			for {
				conn, aerr := ln.Accept()
				if aerr != nil {
					return
				}
				if tc, ok := conn.(*tls.Conn); ok {
					// A successful handshake means the client dialed TLS.
					if tc.Handshake() == nil {
						select {
						case handshakeSeen <- struct{}{}:
						default:
						}
					}
				}
				_ = conn.Close()
			}
		}()

		eps := []string{"https://" + ln.Addr().String()}
		// The gRPC dial is lazy, so MemberList will error (the listener is not etcd),
		// but the dial itself must perform a TLS handshake when a config is supplied.
		// Run it in the background so the test returns as soon as the handshake lands
		// rather than waiting out the client's dial-retry timeout.
		go func() {
			_, _ = MemberList(eps, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12})
		}()

		select {
		case <-handshakeSeen:
			// good: the *tls.Config drove a real TLS handshake.
		case <-time.After(10 * time.Second):
			t.Fatal("expected a TLS handshake from the threaded *tls.Config, saw none")
		}
	})
}

// selfSignedServerTLS returns a *tls.Config with a self-signed server cert for the
// raw TLS listener used to prove the client-side *tls.Config is applied.
func selfSignedServerTLS(t *testing.T) *tls.Config {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(crand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("createcert: %v", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}},
		MinVersion:   tls.VersionTLS12,
	}
}

func TestClusterHealth(t *testing.T) {
	e := setupEtcdServer(t)
	defer e.Close()

	t.Run("ReturnsHealthStatus", func(t *testing.T) {
		eps := []string{"http://localhost:2379"}
		health, err := ClusterHealth(eps, nil)
		assert.NoError(t, err)
		assert.NotNil(t, health)
		assert.Greater(t, len(health), 0)
		assert.True(t, health[0].Health)
	})

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		health, err := ClusterHealth(eps, nil)
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
		resp, err := AddMember(eps, peerURLs, false, nil)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Greater(t, len(resp.Members), 0)
	})

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		peerURLs := []string{"http://127.0.0.1:2380"}
		resp, err := AddMember(eps, peerURLs, false, nil)
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
		addResp, err := AddMember(eps, peerURLs, true, nil)
		assert.NoError(t, err)

		err = PromoteLearner(eps, addResp.Member.ID, nil)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "etcdserver: can only promote a learner member which is in sync with leader")
	})

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		err := PromoteLearner(eps, 12345, nil)
		assert.Error(t, err)
	})
}

func TestRemoveMember(t *testing.T) {
	e := setupEtcdServer(t)
	defer e.Close()

	t.Run("ReturnsErrorForInvalidEndpoint", func(t *testing.T) {
		eps := []string{"http://invalid:2379"}
		err := RemoveMember(eps, 12345, nil)
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
