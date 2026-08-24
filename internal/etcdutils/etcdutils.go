package etcdutils

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// defaultTimeout is the default timeout used for etcd client SDK calls.
const defaultTimeout = 10 * time.Second

// ClientConfig is the subset of etcd client settings consumed by the helpers in this package.
type ClientConfig struct {
	Endpoints []string
	// Names holds the Pod/etcd member name for each entry in Endpoints, in
	// the same order (both share the same value, see EpHealth.Name). Only
	// consumed by MemberHealth; optional, callers that don't need EpHealth.Name
	// populated may leave it nil.
	Names []string
	TLS   *tls.Config
}

// buildConfig produces the clientv3.Config used by every helper.
func (c ClientConfig) buildConfig() clientv3.Config {
	return clientv3.Config{
		Endpoints:            c.Endpoints,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
		TLS:                  c.TLS,
	}
}

// closeAndCancel closes etcd client and cancels context
func closeAndCancel(c *clientv3.Client, cancel context.CancelFunc) {
	if err := c.Close(); err != nil {
		log.Log.Error(err, "failed to close client")
	}
	cancel()
}

// MemberList is a linearizable call: it goes through Raft consensus, so a
// successful response confirms the cluster has quorum.
func MemberList(cfg ClientConfig) (*clientv3.MemberListResponse, error) {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer func() { closeAndCancel(c, cancel) }()

	return c.MemberList(ctx)
}

// AlarmList is, like MemberList, a linearizable call: it reports every
// active alarm (e.g. NOSPACE, CORRUPT) across the cluster.
func AlarmList(cfg ClientConfig) (*clientv3.AlarmResponse, error) {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer func() { closeAndCancel(c, cancel) }()

	return c.AlarmList(ctx)
}

// ClusterHealth aggregates the results of a single health-check pass over an
// etcd cluster: overall cluster health, per-member health, and any active
// alarms.
type ClusterHealth struct {
	// Healthy reports whether the cluster's linearizable path is up: both
	// MemberList and AlarmList round-tripped through Raft successfully.
	Healthy bool
	// Members holds the per-member health check result, keyed by member
	// name, determined via a serializable range request against each
	// member (see MemberHealth).
	Members map[string]EpHealth
	// Alarms lists any active etcd alarms (e.g. NOSPACE, CORRUPT).
	Alarms []*etcdserverpb.AlarmMember
}

type EpHealth struct {
	// Name is the Pod name, which is also the etcd member name.
	Name   string `json:"name,omitempty"`
	Ep     string `json:"endpoint"`
	Health bool   `json:"health"`
	Took   string `json:"took"`
	Status *clientv3.StatusResponse
	Error  string `json:"error,omitempty"`
}

type healthReport []EpHealth

func (r healthReport) Len() int {
	return len(r)
}

func (r healthReport) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r healthReport) Less(i, j int) bool {
	return r[i].Ep < r[j].Ep
}

func (eh EpHealth) String() string {
	var sb strings.Builder
	if len(eh.Name) > 0 {
		fmt.Fprintf(&sb, "name: %s, ", eh.Name)
	}
	fmt.Fprintf(&sb, "endpoint: %s, health: %t, took: %s", eh.Ep, eh.Health, eh.Took)
	if eh.Status != nil {
		fmt.Fprintf(&sb, ", isLearner: %t", eh.Status.IsLearner)
	}
	if len(eh.Error) > 0 {
		sb.WriteString("error: ")
		sb.WriteString(eh.Error)
	}
	return sb.String()
}

func IsLearnerReady(leaderStatus, learnerStatus *clientv3.StatusResponse) bool {
	leaderRev := leaderStatus.Header.Revision
	learnerRev := learnerStatus.Header.Revision

	learnerReadyPercent := float64(learnerRev) / float64(leaderRev)
	return learnerReadyPercent >= 0.9
}

func FindLeaderStatus(healthInfos map[string]EpHealth, logger logr.Logger) (uint64, *clientv3.StatusResponse) {
	var leader uint64
	var leaderStatus *clientv3.StatusResponse
	// Find the leader status
	for _, healthInfo := range healthInfos {
		status := healthInfo.Status
		if status == nil {
			continue
		}
		if status.Leader == status.Header.MemberId {
			leader = status.Header.MemberId
			leaderStatus = status
			break
		}
	}
	if leaderStatus != nil {
		logger.Info("Leader found", "leaderID", leader)
	}

	return leader, leaderStatus
}

func FindLearnerStatus(healthInfos map[string]EpHealth, logger logr.Logger) (uint64, *clientv3.StatusResponse) {
	var learner uint64
	var learnerStatus *clientv3.StatusResponse
	logger.Info("Now checking if there is any pending learner member that needs to be promoted")
	for _, healthInfo := range healthInfos {
		status := healthInfo.Status
		if status == nil {
			continue
		}
		if status.IsLearner {
			learner = status.Header.MemberId
			learnerStatus = status
			logger.Info("Learner member found", "memberID", learner)
			break
		}
	}
	return learner, learnerStatus
}

// MemberHealth checks each endpoint's health via a serializable range
// request — the request succeeding (or failing only with PermissionDenied,
// which still proves the member is serving) is the sole signal for
// EpHealth.Health, matching etcd's own health-check convention of not
// requiring quorum for a per-member check. Status is then fetched
// best-effort, purely to populate metadata (version, leader, learner) for
// callers; a Status failure does not affect Health.
//
// cfg.Names must have the same length as cfg.Endpoints, and every name in
// it must be non-empty.
func MemberHealth(cfg ClientConfig) ([]EpHealth, error) {
	lg, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
	if err != nil {
		return nil, err
	}

	type epConfig struct {
		cfg  *clientv3.Config
		name string
	}

	var cfgs = make([]epConfig, 0, len(cfg.Endpoints))
	for i, ep := range cfg.Endpoints {
		epCfg := ClientConfig{Endpoints: []string{ep}, TLS: cfg.TLS}
		built := epCfg.buildConfig()
		cfgs = append(cfgs, epConfig{cfg: &built, name: cfg.Names[i]})
	}

	healthCh := make(chan EpHealth, len(cfg.Endpoints))

	var wg sync.WaitGroup
	for _, ec := range cfgs {
		wg.Add(1)
		go func(cfg *clientv3.Config, name string) {
			defer wg.Done()

			ep := cfg.Endpoints[0]
			cfg.Logger = lg.Named("client")
			cli, err := clientv3.New(*cfg)
			if err != nil {
				healthCh <- EpHealth{Name: name, Ep: ep, Health: false, Error: err.Error()}
				return
			}
			defer func() {
				err := cli.Close()
				if err != nil {
					lg.Warn("failed to close etcd client", zap.String("endpoint", ep), zap.Error(err))
				}
			}()
			startTs := time.Now()
			// get a random key. As long as we can get the response
			// without an error, the endpoint is health.
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			_, err = cli.Get(ctx, "health", clientv3.WithSerializable())
			eh := EpHealth{Name: name, Ep: ep, Health: false, Took: time.Since(startTs).String()}
			if err == nil || errors.Is(err, rpctypes.ErrPermissionDenied) {
				eh.Health = true
			} else {
				eh.Error = err.Error()
			}

			if eh.Health {
				// Best-effort metadata fetch: its outcome does not affect
				// Health, which is determined solely by the range request
				// above.
				if epStatus, err := cli.Status(ctx, ep); err == nil {
					eh.Status = epStatus
				} else {
					eh.Error = fmt.Sprintf("unable to fetch status: %s", err.Error())
				}
			}
			healthCh <- eh
		}(ec.cfg, ec.name)
	}
	wg.Wait()
	close(healthCh)

	var healthList = make([]EpHealth, 0, len(healthCh))
	for h := range healthCh {
		healthList = append(healthList, h)
	}
	sort.Sort(healthReport(healthList))

	return healthList, nil
}

func AddMember(cfg ClientConfig, peerURLs []string, learner bool) (*clientv3.MemberAddResponse, error) {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer func() { closeAndCancel(c, cancel) }()

	if learner {
		return c.MemberAddAsLearner(ctx, peerURLs)
	}

	return c.MemberAdd(ctx, peerURLs)
}

func PromoteLearner(cfg ClientConfig, learnerId uint64) error {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer func() { closeAndCancel(c, cancel) }()

	_, err = c.MemberPromote(ctx, learnerId)
	return err
}

func RemoveMember(cfg ClientConfig, memberID uint64) error {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer func() { closeAndCancel(c, cancel) }()

	_, err = c.MemberRemove(ctx, memberID)
	return err
}

func MoveLeader(cfg ClientConfig, memberId uint64) error {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer func() { closeAndCancel(c, cancel) }()

	_, err = c.MoveLeader(ctx, memberId)
	return err
}
