package etcdutils

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func MemberList(eps []string) (*clientv3.MemberListResponse, error) {
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		err := c.Close()
		if err != nil {
			cancel()
			return
		}
		cancel()
	}()

	return c.MemberList(ctx)
}

type EpHealth struct {
	Ep     string                   `json:"endpoint"`
	Health bool                     `json:"health"`
	Took   string                   `json:"took"`
	Status *clientv3.StatusResponse // Contains MemberID via Status.Header.MemberId
	Alarms []string                 `json:"alarms,omitempty"` // Stores string representations of alarm types specific to this member
	Error  string                   `json:"error,omitempty"`
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
	sb.WriteString(fmt.Sprintf("endpoint: %s, health: %t, took: %s", eh.Ep, eh.Health, eh.Took))
	if eh.Status != nil {
		sb.WriteString(fmt.Sprintf(", version: %s", eh.Status.Version))
		sb.WriteString(fmt.Sprintf(", dbSize: %d", eh.Status.DbSize))
		sb.WriteString(fmt.Sprintf(", isLearner: %t", eh.Status.IsLearner))
		if eh.Status.Header != nil {
			sb.WriteString(fmt.Sprintf(", memberId: %x", eh.Status.Header.MemberId))
			sb.WriteString(fmt.Sprintf(", raftTerm: %d", eh.Status.Header.RaftTerm))
		}
	}
	if len(eh.Alarms) > 0 {
		sb.WriteString(fmt.Sprintf(", alarms: [%s]", strings.Join(eh.Alarms, "; ")))
	}
	if len(eh.Error) > 0 {
		sb.WriteString(", error: ")
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

func FindLearnerStatus(healthInfos []EpHealth, logger logr.Logger) (uint64, *clientv3.StatusResponse) {
	var learner uint64
	var learnerStatus *clientv3.StatusResponse
	logger.Info("Now checking if there is any pending learner member that needs to be promoted")
	for i := range healthInfos {
		status := healthInfos[i].Status
		if status == nil {
			continue
		}
		if healthInfos[i].Status.IsLearner {
			learner = healthInfos[i].Status.Header.MemberId
			learnerStatus = healthInfos[i].Status
			logger.Info("Learner member found", "memberID", learner)
			break
		}
	}
	return learner, learnerStatus
}

func ClusterHealth(eps []string) ([]EpHealth, error) {
	if len(eps) == 0 {
		return nil, nil
	}

	// Overall context for the duration of ClusterHealth operation, can be passed down.
	// For simplicity, Background is used here, but a cancellable context from caller is better.
	// TODO: Change the method signature to support cancellable context in the future.
	ctx := context.Background()

	lg, err := logutil.CreateDefaultZapLogger(zap.InfoLevel) // Or pass logger as an argument
	if err != nil {
		// This error is about creating a logger, which is a prerequisite.
		return nil, fmt.Errorf("failed to create zap logger for ClusterHealth: %w", err)
	}
	lg = lg.Named("ClusterHealth") // Add a name to the logger for context

	// 1. Get a client for cluster-wide operations
	clusterOpClient, clientErr := getClusterOperationClient(eps, lg)
	if clientErr != nil {
		// If we can't even create a client for cluster ops, log it.
		// We might decide to proceed and try individual endpoint checks without alarm info,
		// or return an error. For now, let's log and proceed, alarms will be empty.
		lg.Error("Failed to create cluster operation client", zap.Error(clientErr), zap.Strings("endpoints", eps))
		// clusterOpClient will be nil
	}
	if clusterOpClient != nil {
		defer clusterOpClient.Close()
	}

	// 2. Fetch cluster-wide alarms ONCE
	var clusterAlarmsResponse *clientv3.AlarmResponse
	if clusterOpClient != nil {
		clusterAlarmsResponse, err = fetchClusterAlarmsOnce(ctx, clusterOpClient, lg)
		if err != nil {
			lg.Error("Failed to fetch cluster-wide alarms, proceeding without alarm information.", zap.Error(err))
			// clusterAlarmsResponse remains nil, subsequent logic handles this.
		}
	}

	// 3. Index alarms by MemberID for quick lookup
	alarmsMap := indexAlarmsByMemberID(clusterAlarmsResponse)
	if len(alarmsMap) > 0 {
		lg.Debug("Indexed cluster alarms", zap.Int("distinct_members_with_alarms", len(alarmsMap)))
	}

	// 4. Prepare per-endpoint client configurations
	var cfgs = make([]clientv3.Config, 0, len(eps)) // Store as value to ensure copies for goroutines
	for _, ep := range eps {
		cfg := clientv3.Config{
			Endpoints:            []string{ep},
			DialTimeout:          2 * time.Second,
			DialKeepAliveTime:    2 * time.Second,
			DialKeepAliveTimeout: 6 * time.Second,
			// Logger will be set per-goroutine for its client instance
		}
		cfgs = append(cfgs, cfg)
	}

	// 5. Concurrently check each endpoint's health
	healthCh := make(chan EpHealth, len(cfgs))
	var wg sync.WaitGroup

	for _, endpointCfg := range cfgs {
		wg.Add(1)
		go func(cfg clientv3.Config) { // Pass cfg by value to ensure each goroutine gets its own copy
			defer wg.Done()

			// Perform core health check for this single endpoint
			epHealthResult := checkSingleEndpointCoreHealth(ctx, cfg, lg) // lg is the base ClusterHealth logger

			// Populate alarms using the pre-fetched and indexed data
			if epHealthResult.Status != nil && epHealthResult.Status.Header != nil {
				memberID := epHealthResult.Status.Header.MemberId
				if specificAlarms, found := alarmsMap[memberID]; found {
					epHealthResult.Alarms = specificAlarms
				} else {
					epHealthResult.Alarms = nil
				}
			} else {
				// No Status or Header, so cannot determine MemberID to look up alarms.
				epHealthResult.Alarms = nil
			}
			healthCh <- epHealthResult
		}(endpointCfg)
	}

	wg.Wait()
	close(healthCh)

	// 6. Collect and sort results
	var healthList = make([]EpHealth, 0, len(healthCh))
	for h := range healthCh {
		healthList = append(healthList, h)
	}
	sort.Sort(healthReport(healthList))

	lg.Info("Cluster health check completed", zap.Int("endpoints_checked", len(eps)), zap.Int("results_collected", len(healthList)))
	return healthList, nil
}

// appendError concatenates error strings.
func appendError(existingError string, newErrorString string) string {
	if existingError == "" {
		return newErrorString
	}
	return existingError + "; " + newErrorString
}

// getClusterOperationClient tries to create a client suitable for cluster-wide operations.
func getClusterOperationClient(eps []string, lg *zap.Logger) (*clientv3.Client, error) {
	if len(eps) == 0 {
		return nil, errors.New("no endpoints provided to create cluster operation client")
	}
	// Use all provided endpoints to create a robust client for cluster operations.
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          5 * time.Second, // Reasonably short timeout for one-off ops
		DialKeepAliveTime:    10 * time.Second,
		DialKeepAliveTimeout: 5 * time.Second,
		Logger:               lg.Named("cluster-op-client"),
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster operation client with endpoints %v: %w", eps, err)
	}
	return cli, nil
}

// fetchClusterAlarmsOnce calls client.AlarmList() once.
func fetchClusterAlarmsOnce(ctx context.Context, client *clientv3.Client, lg *zap.Logger) (*clientv3.AlarmResponse, error) {
	// Use a specific timeout for this operation.
	alarmCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // 5-second timeout for AlarmList
	defer cancel()

	alarmResp, err := client.AlarmList(alarmCtx)
	if err != nil {
		// Log the error at the source of the call if needed, or let the caller decide.
		return nil, fmt.Errorf("client.AlarmList call failed: %w", err)
	}
	lg.Debug("Successfully fetched cluster alarms")
	return alarmResp, nil
}

// indexAlarmsByMemberID converts AlarmResponse to a map for easy lookup.
func indexAlarmsByMemberID(alarmResp *clientv3.AlarmResponse) map[uint64][]string {
	alarmsMap := make(map[uint64][]string)
	if alarmResp != nil && len(alarmResp.Alarms) > 0 {
		for _, alarmMember := range alarmResp.Alarms {
			alarmsMap[alarmMember.MemberID] = append(
				alarmsMap[alarmMember.MemberID],
				alarmMember.GetAlarm().String(), // Store string representation of AlarmType
			)
		}
	}
	return alarmsMap
}

// checkSingleEndpointCoreHealth performs basic health checks for a single endpoint.
// It does NOT call AlarmList.
func checkSingleEndpointCoreHealth(ctx context.Context, cfg clientv3.Config, parentLogger *zap.Logger) EpHealth {
	ep := cfg.Endpoints[0]

	// Set a specific logger for the etcd client instance.
	// Since cfg is passed by value, modifying it here is safe and only affects this local copy.
	cfg.Logger = parentLogger.Named(fmt.Sprintf("client-%s", strings.ReplaceAll(ep, "://", "_")))

	epHealth := EpHealth{Ep: ep, Health: false} // Initialize with Health: false

	cli, err := clientv3.New(cfg) // Use the modified local cfg copy directly.
	if err != nil {
		epHealth.Error = err.Error()
		// Took is not measured if client creation fails.
		return epHealth
	}
	defer cli.Close()

	startTs := time.Now()

	// Context for this endpoint's operations
	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second) // 10-second timeout for Get + Status
	defer cancel()

	// 1. Basic health check (e.g., Get a key)
	_, getErr := cli.Get(opCtx, "health", clientv3.WithSerializable())
	if getErr == nil || errors.Is(getErr, rpctypes.ErrPermissionDenied) {
		epHealth.Health = true // Initial health check passed
	} else {
		epHealth.Error = getErr.Error()
		// If basic Get fails, we might still want to attempt Status for more info,
		// but the endpoint is likely unhealthy. Health remains false.
	}

	// 2. Get detailed status, regardless of initial Get result, to gather more info
	epStatus, statusErr := cli.Status(opCtx, ep) // ep is this client's only endpoint
	if statusErr != nil {
		epHealth.Health = false // Explicitly set to false if Status fails
		epHealth.Error = appendError(epHealth.Error, fmt.Sprintf("unable to fetch status: %v", statusErr))
	} else {
		epHealth.Status = epStatus
		if len(epStatus.Errors) > 0 { // Errors reported by the etcd member itself
			epHealth.Health = false
			epHealth.Error = appendError(epHealth.Error, fmt.Sprintf("etcd member reported errors: %s", strings.Join(epStatus.Errors, ", ")))
		} else if !epHealth.Health {
			// If Get failed but Status succeeded without internal errors,
			// it's a mixed signal. For now, if Get failed, overall Health is false.
		}
	}

	epHealth.Took = time.Since(startTs).String()
	return epHealth
}

func AddMember(eps []string, peerURLs []string, learner bool) (*clientv3.MemberAddResponse, error) {
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		err := c.Close()
		if err != nil {
			cancel()
			return
		}
		cancel()
	}()

	if learner {
		return c.MemberAddAsLearner(ctx, peerURLs)
	}

	return c.MemberAdd(ctx, peerURLs)
}

func PromoteLearner(eps []string, learnerId uint64) error {
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		err := c.Close()
		if err != nil {
			cancel()
			return
		}
		cancel()
	}()

	_, err = c.MemberPromote(ctx, learnerId)
	return err
}

func RemoveMember(eps []string, memberID uint64) error {
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    2 * time.Second,
		DialKeepAliveTimeout: 6 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		err := c.Close()
		if err != nil {
			cancel()
			return
		}
		cancel()
	}()

	_, err = c.MemberRemove(ctx, memberID)
	return err
}
