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

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ClientConfig is the subset of etcd client settings consumed by the helpers in this package.
type ClientConfig struct {
	Endpoints []string
	TLS       *tls.Config
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

func MemberList(cfg ClientConfig) (*clientv3.MemberListResponse, error) {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() { closeAndCancel(c, cancel) }()

	return c.MemberList(ctx)
}

type EpHealth struct {
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

func FindLeaderStatus(healthInfos []EpHealth, logger logr.Logger) (uint64, *clientv3.StatusResponse) {
	var leader uint64
	var leaderStatus *clientv3.StatusResponse
	// Find the leader status
	for i := range healthInfos {
		status := healthInfos[i].Status
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

func FindLearnerStatus(healthInfos []EpHealth, logger logr.Logger) (uint64, *clientv3.StatusResponse) {
	var learner uint64
	var learnerStatus *clientv3.StatusResponse
	logger.Info("Now checking if there is any pending learner member that needs to be promoted")
	for i := range healthInfos {
		if healthInfos[i].Status.IsLearner {
			learner = healthInfos[i].Status.Header.MemberId
			learnerStatus = healthInfos[i].Status
			logger.Info("Learner member found", "memberID", learner)
			break
		}
	}
	return learner, learnerStatus
}

func ClusterHealth(cfg ClientConfig) ([]EpHealth, error) {
	lg, err := logutil.CreateDefaultZapLogger(zap.InfoLevel)
	if err != nil {
		return nil, err
	}

	var cfgs = make([]*clientv3.Config, 0, len(cfg.Endpoints))
	for _, ep := range cfg.Endpoints {
		epCfg := ClientConfig{Endpoints: []string{ep}, TLS: cfg.TLS}
		built := epCfg.buildConfig()
		cfgs = append(cfgs, &built)
	}

	healthCh := make(chan EpHealth, len(cfg.Endpoints))

	var wg sync.WaitGroup
	for _, cfg := range cfgs {
		wg.Add(1)
		go func(cfg *clientv3.Config) {
			defer wg.Done()

			ep := cfg.Endpoints[0]
			cfg.Logger = lg.Named("client")
			cli, err := clientv3.New(*cfg)
			if err != nil {
				healthCh <- EpHealth{Ep: ep, Health: false, Error: err.Error()}
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
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err = cli.Get(ctx, "health", clientv3.WithSerializable())
			eh := EpHealth{Ep: ep, Health: false, Took: time.Since(startTs).String()}
			if err == nil || errors.Is(err, rpctypes.ErrPermissionDenied) {
				eh.Health = true
			} else {
				eh.Error = err.Error()
			}

			if eh.Health {
				epStatus, err := cli.Status(ctx, ep)
				if err != nil {
					eh.Health = false
					eh.Error = fmt.Sprintf("Unable to fetch the status :%s", err.Error())
				} else {
					eh.Status = epStatus
					if len(epStatus.Errors) > 0 {
						eh.Health = false
						eh.Error = strings.Join(epStatus.Errors, ",")
					}
				}
			}
			healthCh <- eh
		}(cfg)
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() { closeAndCancel(c, cancel) }()

	_, err = c.MemberPromote(ctx, learnerId)
	return err
}

func RemoveMember(cfg ClientConfig, memberID uint64) error {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() { closeAndCancel(c, cancel) }()

	_, err = c.MemberRemove(ctx, memberID)
	return err
}

func MoveLeader(cfg ClientConfig, memberId uint64) error {
	c, err := clientv3.New(cfg.buildConfig())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() { closeAndCancel(c, cancel) }()

	_, err = c.MoveLeader(ctx, memberId)
	return err
}
