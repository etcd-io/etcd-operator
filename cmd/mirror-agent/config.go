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

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

const (
	sideSource = "source"
	sideTarget = "target"
)

// stringSliceFlag is a repeatable flag.Value accumulating one entry per
// occurrence. Repeatable rather than comma-separated because etcd key
// prefixes may legally contain commas.
type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }

func (s *stringSliceFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// sideFlags is the per-side (source/target) flag surface: endpoints, TLS
// material paths, and auth credential paths. Secrets arrive only as
// mounted-file paths — never as flag values or environment variables.
type sideFlags struct {
	side string

	endpoints string

	// tls mirrors has(spec.<side>.tls) on the EtcdMirror CR: true dials TLS
	// even with no file flags (server-auth against the system trust roots).
	tls                bool
	certFile           string
	keyFile            string
	caFile             string
	caBundleFile       string
	serverName         string
	insecureSkipVerify bool

	usernameFile string
	passwordFile string
}

// agentFlags is the binary's whole flag surface. Each field maps onto the
// EtcdMirror CRD field of the same name; semantics are pinned there.
type agentFlags struct {
	linkUID string
	epoch   int64
	mode    string

	sourcePrefix    string
	targetPrefix    string
	destPrefix      string
	excludePrefixes stringSliceFlag

	initialSyncMode string
	startRevision   int64
	checkpointKey   string

	maxTxnOps        int
	txnFlushBytes    string
	pageBytes        string
	watchBufferBytes string
	pageKeyLimit     int
	maxOpsPerSecond  int
	requestTimeout   time.Duration
	dialTimeout      time.Duration

	backoffInitialDelay time.Duration
	backoffMaxDelay     time.Duration

	reconcileEnabled       bool
	reconcileInterval      time.Duration
	reconcileDeleteOrphans bool

	httpBindAddress string

	source sideFlags
	target sideFlags
}

// newAgentFlags registers every mirror-agent flag on fs and returns the
// struct they populate.
func newAgentFlags(fs *flag.FlagSet) *agentFlags {
	f := &agentFlags{}
	fs.StringVar(&f.linkUID, "link-uid", "",
		"Unique identifier of this mirror link, stamped into the fence key "+
			"(typically the EtcdMirror object's UID). Required.")
	fs.Int64Var(&f.epoch, "epoch", 0,
		"This agent generation within the link, bumped by the supervisor on each re-deploy. Must be >= 1. Required.")
	fs.StringVar(&f.mode, "mode", string(mirroragent.ModeSync),
		"Operating mode: Sync (continuous replication) or Drain (cutover drain).")
	fs.StringVar(&f.sourcePrefix, "source-prefix", "",
		"Source key prefix to mirror; empty means the whole keyspace.")
	fs.StringVar(&f.targetPrefix, "target-prefix", "",
		"Target prefix under which mirrored keys land.")
	fs.StringVar(&f.destPrefix, "dest-prefix", "",
		"Middle term of the rewrite formula key' = target-prefix + dest-prefix + TrimPrefix(key, source-prefix).")
	fs.Var(&f.excludePrefixes, "exclude-prefix",
		"Source key prefix skipped entirely (not scanned, watched, counted, or pruned). Repeatable.")
	fs.StringVar(&f.initialSyncMode, "initial-sync-mode", "",
		"Pre-existing destination-key policy at genesis: RequireEmpty (default), Overwrite, or OverwriteAndPrune.")
	fs.Int64Var(&f.startRevision, "start-revision", 0,
		"When > 0, skip the genesis scan and tail from this source revision + 1 "+
			"(requires initial-sync-mode Overwrite or OverwriteAndPrune).")
	fs.StringVar(&f.checkpointKey, "checkpoint-key", "",
		"Reserved checkpoint/fence key on the target; empty selects the engine default "+
			"(effective destination prefix + a \\x00-prefixed suffix). Only pass when spec.checkpoint.key is set.")
	fs.IntVar(&f.maxTxnOps, "max-txn-ops", 0,
		"Max operations per target Txn including the reserved checkpoint slot; 0 selects the engine default (128).")
	fs.StringVar(&f.txnFlushBytes, "txn-flush-bytes", "",
		"Byte watermark at which a batch is flushed, as a Kubernetes quantity (e.g. 1Mi); empty selects the default.")
	fs.StringVar(&f.pageBytes, "page-bytes", "",
		"Byte bound per source scan page, as a Kubernetes quantity; empty selects the default.")
	fs.StringVar(&f.watchBufferBytes, "watch-buffer-bytes", "",
		"Replay-buffer byte bound while the genesis scan runs, as a Kubernetes quantity; empty selects the default.")
	fs.IntVar(&f.pageKeyLimit, "page-key-limit", 0,
		"Key bound per source scan page; 0 selects the engine default (512).")
	fs.IntVar(&f.maxOpsPerSecond, "max-ops-per-second", 0,
		"Target write rate limit (puts+deletes/sec); 0 means unlimited.")
	fs.DurationVar(&f.requestTimeout, "request-timeout", 0,
		"Per-RPC deadline for every unary call on both sides; 0 selects the engine default (30s).")
	fs.DurationVar(&f.dialTimeout, "dial-timeout", 10*time.Second,
		"Bound on establishing the initial client connection to each side.")
	fs.DurationVar(&f.backoffInitialDelay, "backoff-initial-delay", 0,
		"Initial delay of the connection-error retry curve; 0 selects the engine default (1s).")
	fs.DurationVar(&f.backoffMaxDelay, "backoff-max-delay", 0,
		"Max delay of the connection-error retry curve; 0 selects the engine default (30s).")
	fs.BoolVar(&f.reconcileEnabled, "reconcile-enabled", false,
		"Enable the periodic full diff-and-repair pass.")
	fs.DurationVar(&f.reconcileInterval, "reconcile-interval", 0,
		"Interval between periodic reconciliation passes; 0 with --reconcile-enabled selects the default (1h).")
	fs.BoolVar(&f.reconcileDeleteOrphans, "reconcile-delete-orphans", false,
		"Allow the periodic pass to delete target keys with no source counterpart.")
	fs.StringVar(&f.httpBindAddress, "http-bind-address", ":8080",
		"Listen address for /statusz, /healthz, /readyz and /metrics.")
	registerSideFlags(fs, sideSource, &f.source)
	registerSideFlags(fs, sideTarget, &f.target)
	return f
}

// registerSideFlags registers one side's flag block; side is "source" or
// "target", matching spec.source / spec.target on the EtcdMirror CR.
func registerSideFlags(fs *flag.FlagSet, side string, out *sideFlags) {
	out.side = side
	p := func(name string) string { return side + "-" + name }
	fs.StringVar(&out.endpoints, p("endpoints"), "",
		"Comma-separated etcd client endpoints of the "+side+" cluster. Schemeless endpoints get the "+
			"scheme implied by --"+p("tls")+"; a scheme contradicting that flag is an error. Required.")
	fs.BoolVar(&out.tls, p("tls"), false,
		"Dial the "+side+" side with TLS; true with no file flags means server-auth TLS against the "+
			"system trust roots (mirrors an empty spec tls block).")
	fs.StringVar(&out.certFile, p("cert-file"), "",
		"Path to the "+side+" client certificate for mTLS. Requires --"+p("key-file")+".")
	fs.StringVar(&out.keyFile, p("key-file"), "",
		"Path to the "+side+" client key for mTLS. Requires --"+p("cert-file")+".")
	fs.StringVar(&out.caFile, p("ca-file"), "",
		"Path to the PEM trust anchors verifying "+side+" servers (the mounted secret's ca.crt).")
	fs.StringVar(&out.caBundleFile, p("ca-bundle-file"), "",
		"Path to a separate PEM CA bundle; takes precedence over --"+p("ca-file")+" (mirrors tls.caBundleRef).")
	fs.StringVar(&out.serverName, p("server-name"), "",
		"TLS ServerName (SNI) override used to verify every "+side+" endpoint.")
	fs.BoolVar(&out.insecureSkipVerify, p("insecure-skip-verify"), false,
		"Disable "+side+" server certificate verification. The supervisor only sets this when the "+
			"EtcdMirror acknowledged the risk (insecureSkipVerifyAcknowledgeRisk).")
	fs.StringVar(&out.usernameFile, p("username-file"), "",
		"Path to a file holding the "+side+" etcd RBAC username, read once at startup. "+
			"Requires --"+p("password-file")+".")
	fs.StringVar(&out.passwordFile, p("password-file"), "",
		"Path to a file holding the "+side+" etcd RBAC password, read once at startup. "+
			"Requires --"+p("username-file")+".")
}

// buildConfig translates flags into the engine Config. Only translation-level
// checks live here (quantity parsing, required flags); everything else is
// left to mirroragent.New → Config.Validate — no duplicated validation. In
// particular a negative --watch-buffer-bytes passes through and is rejected
// by Validate (lockstep: 0 = engine default, negative rejected).
func buildConfig(f *agentFlags) (mirroragent.Config, error) {
	if f.linkUID == "" {
		return mirroragent.Config{}, fmt.Errorf("--link-uid is required")
	}
	if f.epoch < 1 {
		return mirroragent.Config{}, fmt.Errorf("--epoch must be >= 1, got %d", f.epoch)
	}
	txnFlushBytes, err := parseQuantityBytes("txn-flush-bytes", f.txnFlushBytes)
	if err != nil {
		return mirroragent.Config{}, err
	}
	pageBytes, err := parseQuantityBytes("page-bytes", f.pageBytes)
	if err != nil {
		return mirroragent.Config{}, err
	}
	watchBufferBytes, err := parseQuantityBytes("watch-buffer-bytes", f.watchBufferBytes)
	if err != nil {
		return mirroragent.Config{}, err
	}
	return mirroragent.Config{
		LinkUID:                f.linkUID,
		Epoch:                  f.epoch,
		Mode:                   mirroragent.Mode(f.mode),
		SourcePrefix:           f.sourcePrefix,
		TargetPrefix:           f.targetPrefix,
		DestPrefix:             f.destPrefix,
		ExcludePrefixes:        f.excludePrefixes,
		InitialSyncMode:        mirroragent.InitialSyncMode(f.initialSyncMode),
		StartRevision:          f.startRevision,
		CheckpointKey:          f.checkpointKey,
		MaxTxnOps:              f.maxTxnOps,
		TxnFlushBytes:          txnFlushBytes,
		PageKeyLimit:           f.pageKeyLimit,
		PageBytes:              pageBytes,
		MaxOpsPerSecond:        f.maxOpsPerSecond,
		RequestTimeout:         f.requestTimeout,
		BackoffInitialDelay:    f.backoffInitialDelay,
		BackoffMaxDelay:        f.backoffMaxDelay,
		ReconcileInterval:      reconcilePeriod(f.reconcileEnabled, f.reconcileInterval),
		ReconcileDeleteOrphans: f.reconcileDeleteOrphans,
		WatchBufferBytes:       watchBufferBytes,
	}, nil
}

// parseQuantityBytes parses a Kubernetes resource.Quantity string ("16Mi",
// "1048576") into bytes. Empty means unset (0 → engine default).
func parseQuantityBytes(name, s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, fmt.Errorf("--%s: invalid quantity %q: %w", name, s, err)
	}
	return q.Value(), nil
}

// reconcilePeriod is the spec.reconciliation → Config.ReconcileInterval
// translation assigned to this rung (see DefaultReconcilePeriod in
// pkg/mirroragent): disabled → 0 regardless of interval; enabled with no
// interval → the 1h default.
func reconcilePeriod(enabled bool, interval time.Duration) time.Duration {
	switch {
	case !enabled:
		return 0
	case interval > 0:
		return interval
	default:
		return mirroragent.DefaultReconcilePeriod
	}
}

// normalizeEndpoints splits the comma-separated list (URLs cannot contain
// commas) and makes the CRD's scheme/TLS agreement true by construction:
// schemeless endpoints get the scheme implied by the tls flag, and a scheme
// contradicting the flag is an error — the binary mirror of the CEL rules on
// EtcdMirrorEndpoint.
func normalizeEndpoints(side, list string, useTLS bool) ([]string, error) {
	var out []string
	for _, ep := range strings.Split(list, ",") {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			continue
		}
		switch {
		case strings.HasPrefix(ep, "http://"):
			if useTLS {
				return nil, fmt.Errorf(
					"--%s-endpoints: http:// endpoint %q conflicts with --%s-tls", side, ep, side)
			}
		case strings.HasPrefix(ep, "https://"):
			if !useTLS {
				return nil, fmt.Errorf(
					"--%s-endpoints: https:// endpoint %q requires --%s-tls", side, ep, side)
			}
		case useTLS:
			ep = "https://" + ep
		default:
			ep = "http://" + ep
		}
		out = append(out, ep)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--%s-endpoints is required", side)
	}
	return out, nil
}

// readAuthFiles reads the etcd RBAC username/password from mounted files,
// once at startup: clientv3 fixes credentials at client construction and
// transparently re-authenticates on token expiry, so there is nothing to
// re-read. One trailing newline is stripped. PRECEDENCE (per the
// EtcdMirrorAuth contract): when both a client certificate and auth are
// supplied, etcd uses the token identity, not the certificate CN — the auth
// user must hold the range-scoped role.
func readAuthFiles(side, usernameFile, passwordFile string) (string, string, error) {
	if (usernameFile == "") != (passwordFile == "") {
		return "", "", fmt.Errorf(
			"--%s-username-file and --%s-password-file must be set together", side, side)
	}
	if usernameFile == "" {
		return "", "", nil
	}
	username, err := readSecretFile(usernameFile)
	if err != nil {
		return "", "", fmt.Errorf("--%s-username-file: %w", side, err)
	}
	password, err := readSecretFile(passwordFile)
	if err != nil {
		return "", "", fmt.Errorf("--%s-password-file: %w", side, err)
	}
	return username, password, nil
}

func readSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r"), nil
}
