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
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd-operator/pkg/mirroragent"
)

// parseArgs runs the real flag registration over args.
func parseArgs(t *testing.T, args ...string) *agentFlags {
	t.Helper()
	fs := flag.NewFlagSet("mirror-agent-test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	f := newAgentFlags(fs)
	require.NoError(t, fs.Parse(args))
	return f
}

func minimalArgs(extra ...string) []string {
	return append([]string{
		"--link-uid=test-link", "--epoch=1",
		"--source-endpoints=127.0.0.1:2379", "--target-endpoints=127.0.0.1:2380",
	}, extra...)
}

func TestBuildConfigDefaults(t *testing.T) {
	f := parseArgs(t, minimalArgs()...)
	cfg, err := buildConfig(f)
	require.NoError(t, err)

	require.Equal(t, "test-link", cfg.LinkUID)
	require.Equal(t, int64(1), cfg.Epoch)
	require.Equal(t, mirroragent.ModeSync, cfg.Mode)
	// Only what was given is carried; zeros select engine defaults.
	require.Empty(t, cfg.InitialSyncMode)
	require.Empty(t, cfg.CheckpointKey)
	require.Zero(t, cfg.MaxTxnOps)
	require.Zero(t, cfg.TxnFlushBytes)
	require.Zero(t, cfg.PageBytes)
	require.Zero(t, cfg.PageKeyLimit)
	require.Zero(t, cfg.WatchBufferBytes)
	require.Zero(t, cfg.RequestTimeout)
	require.Zero(t, cfg.ReconcileInterval)
	require.Equal(t, 10*time.Second, f.dialTimeout)

	// Engine defaulting+validation accepts the translated config.
	_, err = mirroragent.New(cfg, nil, nil)
	require.NoError(t, err)
}

func TestBuildConfigRequiredFlags(t *testing.T) {
	_, err := buildConfig(parseArgs(t, "--epoch=1"))
	require.ErrorContains(t, err, "--link-uid")
	_, err = buildConfig(parseArgs(t, "--link-uid=x"))
	require.ErrorContains(t, err, "--epoch")
}

func TestBuildConfigQuantities(t *testing.T) {
	f := parseArgs(t, minimalArgs(
		"--txn-flush-bytes=16Mi", "--page-bytes=1048576", "--watch-buffer-bytes=32Mi")...)
	cfg, err := buildConfig(f)
	require.NoError(t, err)
	require.Equal(t, int64(16777216), cfg.TxnFlushBytes)
	require.Equal(t, int64(1048576), cfg.PageBytes)
	require.Equal(t, int64(33554432), cfg.WatchBufferBytes)

	_, err = buildConfig(parseArgs(t, minimalArgs("--page-bytes=garbage")...))
	require.ErrorContains(t, err, "--page-bytes")

	// Lockstep with the engine: a negative watch buffer passes translation
	// (0 = default there, not here) and is rejected by Config.Validate.
	cfg, err = buildConfig(parseArgs(t, minimalArgs("--watch-buffer-bytes=-1")...))
	require.NoError(t, err)
	require.Equal(t, int64(-1), cfg.WatchBufferBytes)
	_, err = mirroragent.New(cfg, nil, nil)
	require.ErrorContains(t, err, "watchBufferBytes")
}

func TestBuildConfigReconcileTranslation(t *testing.T) {
	cases := []struct {
		name    string
		enabled bool
		interv  time.Duration
		want    time.Duration
	}{
		{"enabled default", true, 0, mirroragent.DefaultReconcilePeriod},
		{"enabled explicit", true, 45 * time.Minute, 45 * time.Minute},
		{"disabled ignores interval", false, 45 * time.Minute, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, reconcilePeriod(tc.enabled, tc.interv))
		})
	}
}

func TestBuildConfigExcludePrefixRepeatable(t *testing.T) {
	f := parseArgs(t, minimalArgs(
		"--exclude-prefix=/registry/events/", "--exclude-prefix=/a,b/")...)
	cfg, err := buildConfig(f)
	require.NoError(t, err)
	require.Equal(t, []string{"/registry/events/", "/a,b/"}, cfg.ExcludePrefixes)
}

func TestNormalizeEndpoints(t *testing.T) {
	got, err := normalizeEndpoints(sideSource, "a:2379, b:2379", true)
	require.NoError(t, err)
	require.Equal(t, []string{"https://a:2379", "https://b:2379"}, got)

	got, err = normalizeEndpoints(sideSource, "a:2379", false)
	require.NoError(t, err)
	require.Equal(t, []string{"http://a:2379"}, got)

	got, err = normalizeEndpoints(sideTarget, "https://a:2379,http://b:2379", false)
	require.Nil(t, got)
	require.ErrorContains(t, err, "requires --target-tls")

	got, err = normalizeEndpoints(sideTarget, "http://a:2379", true)
	require.Nil(t, got)
	require.ErrorContains(t, err, "conflicts with --target-tls")

	got, err = normalizeEndpoints(sideSource, "https://a:2379,https://b:2379", true)
	require.NoError(t, err)
	require.Equal(t, []string{"https://a:2379", "https://b:2379"}, got)

	_, err = normalizeEndpoints(sideSource, "", false)
	require.ErrorContains(t, err, "--source-endpoints is required")
}

func TestReadAuthFiles(t *testing.T) {
	dir := t.TempDir()
	userFile := filepath.Join(dir, "username")
	passFile := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(userFile, []byte("mirror-user\n"), 0o600))
	require.NoError(t, os.WriteFile(passFile, []byte("s3cret"), 0o600))

	user, pass, err := readAuthFiles(sideSource, userFile, passFile)
	require.NoError(t, err)
	require.Equal(t, "mirror-user", user, "one trailing newline is stripped")
	require.Equal(t, "s3cret", pass)

	user, pass, err = readAuthFiles(sideSource, "", "")
	require.NoError(t, err)
	require.Empty(t, user)
	require.Empty(t, pass)

	_, _, err = readAuthFiles(sideTarget, userFile, "")
	require.ErrorContains(t, err, "must be set together")

	_, _, err = readAuthFiles(sideSource, filepath.Join(dir, "missing"), passFile)
	require.Error(t, err)
}
