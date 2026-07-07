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

// Command mirror-agent runs one pkg/mirroragent replication engine — one
// EtcdMirror link, one process. The operator's EtcdMirror controller renders
// a Deployment running this binary; it can equally be run by hand against
// two reachable etcd clusters. The binary talks only to the two etcd
// clusters: it has zero Kubernetes API dependency.
//
// # Configuration
//
// Everything arrives as flags (the repo's operator convention); secrets —
// TLS material and RBAC credentials — arrive only as mounted-file PATHS
// (--<side>-cert-file, --<side>-username-file, ...), never as flag values or
// environment variables. Flag semantics mirror the EtcdMirror CRD fields of
// the same names; quantities (--txn-flush-bytes, --page-bytes,
// --watch-buffer-bytes) accept Kubernetes resource.Quantity strings.
// Unset/zero flags select the engine defaults; the binary validates only
// what the engine cannot (required flags, quantity syntax, scheme/TLS
// agreement, file pairing) and leaves the rest to Config.Validate.
//
// TLS is built from file paths via transport.TLSInfo, so the client leaf
// pair is re-read from the mount on every handshake: certificate rotation
// needs no restart. --<side>-ca-bundle-file takes precedence over
// --<side>-ca-file, mirroring tls.caBundleRef. --<side>-insecure-skip-verify
// has no acknowledge-risk companion flag — that ceremony is CRD/manifest
// UX; the supervisor only passes the flag when the CR acknowledged the
// risk, and the binary logs a standing warning. Auth username/password are
// read once at startup (clientv3 re-auths transparently); the token
// identity wins over the certificate CN when both are supplied.
//
// # Endpoints
//
// One listener (--http-bind-address) serves:
//
//   - /statusz — JSON dump of the engine Snapshot (the controller's poll
//     surface).
//   - /healthz — process liveness, always 200.
//   - /readyz — 200 once the engine is past Connecting and not Failed;
//     Degraded (transient backoff) and Drained (terminal success) are ready.
//   - /metrics — Prometheus (etcd_mirror_agent_* plus process/Go runtime).
//
// # Lifecycle and exit codes
//
// When the engine's Run returns — drain complete or permanent failure — the
// process does NOT exit: it lingers, serving /statusz, /metrics and /readyz,
// so the controller can read the terminal state (Drained + the cutover
// block, or Failed + lastError). Crash-looping would hide that state and
// re-run genesis under the same epoch; restart/epoch-bump policy belongs to
// the supervisor. On SIGINT/SIGTERM the engine context is cancelled, Run is
// awaited, and the HTTP server drains (10s bound).
//
// Exit codes: 0 = clean signal shutdown (engine cancelled mid-run, or it had
// drained); 1 = startup failure or the engine ended in permanent failure by
// the time the signal arrived; 2 = flag-parse errors (stdlib flag). Any
// non-cancellation error escaping Run is permanent by construction —
// transient, throttle and quota conditions are retried inside the engine and
// never escape.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// httpShutdownTimeout bounds the HTTP server drain after the engine stopped.
const httpShutdownTimeout = 10 * time.Second

var setupLog = crlog.Log.WithName("mirror-agent")

func main() {
	f := newAgentFlags(flag.CommandLine)
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	crlog.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, f, nil); err != nil {
		setupLog.Error(err, "mirror-agent failed")
		os.Exit(1)
	}
}

// run wires flags → clients → engine → HTTP and blocks until ctx is
// cancelled. onListen (optional) observes the bound HTTP address, for tests
// binding port 0. A non-nil return is a startup failure or a permanent
// engine failure; cancellation mid-run and a completed drain return nil.
func run(ctx context.Context, f *agentFlags, onListen func(net.Addr)) error {
	cfg, err := buildConfig(f)
	if err != nil {
		return err
	}
	src, err := buildClient(&f.source, f.dialTimeout)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := buildClient(&f.target, f.dialTimeout)
	if err != nil {
		return err
	}
	defer func() { _ = dst.Close() }()

	agent, err := mirroragent.New(cfg, src, dst)
	if err != nil {
		return err
	}

	tracker := newCertExpiryTracker(certExpiryFiles(&f.source, &f.target))
	tracker.refresh()
	go tracker.loop(ctx)

	listener, err := net.Listen("tcp", f.httpBindAddress)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", f.httpBindAddress, err)
	}
	if onListen != nil {
		onListen(listener.Addr())
	}
	srv := &http.Server{
		Handler:           newMux(agent.Snapshot, newRegistry(agent.Snapshot, tracker.gauge)),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	go func() {
		if serr := srv.Serve(listener); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			setupLog.Error(serr, "http server failed")
		}
	}()

	setupLog.Info("starting replication engine", "linkUID", cfg.LinkUID, "epoch", cfg.Epoch,
		"mode", cfg.Mode, "httpBindAddress", listener.Addr().String())
	runDone := make(chan error, 1)
	go func() { runDone <- agent.Run(ctx) }()

	var runErr error
	select {
	case runErr = <-runDone:
		// Terminal engine state (drained or permanent failure): LINGER,
		// keep serving the observability surface until the supervisor
		// signals — see the package doc's lifecycle contract.
		if runErr == nil {
			setupLog.Info("drain completed; serving terminal status until signalled")
		} else {
			setupLog.Error(runErr, "engine failed permanently; serving terminal status until signalled")
		}
		<-ctx.Done()
	case <-ctx.Done():
		runErr = <-runDone
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutCtx)

	if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		return fmt.Errorf("engine failed permanently: %w", runErr)
	}
	return nil
}

// buildClient dials one side per its flags: normalized endpoints, TLS from
// mounted file paths (see buildTLSInfo), auth from mounted files, and the
// keepalive settings the engine mandates (NewClientConfig).
func buildClient(side *sideFlags, dialTimeout time.Duration) (*clientv3.Client, error) {
	endpoints, err := normalizeEndpoints(side.side, side.endpoints, side.tls)
	if err != nil {
		return nil, err
	}
	info, tlsEnabled, err := buildTLSInfo(side)
	if err != nil {
		return nil, err
	}
	var tlsCfg *tls.Config
	if tlsEnabled {
		if side.insecureSkipVerify {
			setupLog.Info("WARNING: TLS server certificate verification is DISABLED", "side", side.side)
		}
		tlsCfg, err = info.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("building %s TLS config: %w", side.side, err)
		}
	}
	cc := mirroragent.NewClientConfig(endpoints, tlsCfg, dialTimeout)
	cc.Username, cc.Password, err = readAuthFiles(side.side, side.usernameFile, side.passwordFile)
	if err != nil {
		return nil, err
	}
	cli, err := clientv3.New(cc)
	if err != nil {
		return nil, fmt.Errorf("creating %s client: %w", side.side, err)
	}
	return cli, nil
}
