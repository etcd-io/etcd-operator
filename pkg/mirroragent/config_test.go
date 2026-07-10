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

package mirroragent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func minimalCfg() Config {
	return Config{LinkUID: "l", Epoch: 1, SourcePrefix: "/s/", TargetPrefix: "/d/"}
}

func TestConfigDefaults(t *testing.T) {
	c := minimalCfg().withDefaults()
	require.NoError(t, c.Validate())

	assert.Equal(t, ModeSync, c.Mode)
	assert.Equal(t, InitialSyncRequireEmpty, c.InitialSyncMode)
	assert.Equal(t, "/d/"+DefaultCheckpointKeySuffix, c.CheckpointKey)
	assert.Equal(t, DefaultMaxTxnOps, c.MaxTxnOps)
	assert.EqualValues(t, DefaultTxnFlushBytes, c.TxnFlushBytes)
	assert.Equal(t, DefaultPageKeyLimit, c.PageKeyLimit)
	assert.EqualValues(t, DefaultPageBytes, c.PageBytes)
	assert.Equal(t, DefaultRequestTimeout, c.RequestTimeout)
	assert.EqualValues(t, DefaultWatchBufferBytes, c.WatchBufferBytes)
	assert.Equal(t, DefaultResyncLoopThreshold, c.ResyncLoopThreshold)
	assert.Equal(t, DefaultProgressInterval, c.ProgressInterval)
	assert.Equal(t, DefaultQuotaProbeInterval, c.QuotaProbeInterval)
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "valid", mutate: func(*Config) {}},
		{name: "missing linkUID", mutate: func(c *Config) { c.LinkUID = "" },
			wantErr: "linkUID"},
		{name: "epoch below one", mutate: func(c *Config) { c.Epoch = 0 },
			wantErr: "epoch"},
		{name: "bad mode", mutate: func(c *Config) { c.Mode = "Paused" },
			wantErr: "mode"},
		{name: "bad initialSyncMode", mutate: func(c *Config) { c.InitialSyncMode = "Merge" },
			wantErr: "initialSyncMode"},
		{name: "negative startRevision", mutate: func(c *Config) { c.StartRevision = -1 },
			wantErr: "startRevision"},
		// Defense-in-depth mirror of the CRD's CEL rule: a startRevision
		// seed skips the scan, so RequireEmpty could never be satisfied
		// meaningfully.
		{name: "startRevision requires overwrite", mutate: func(c *Config) { c.StartRevision = 10 },
			wantErr: "startRevision requires initialSyncMode Overwrite"},
		{name: "startRevision with overwrite ok", mutate: func(c *Config) {
			c.StartRevision = 10
			c.InitialSyncMode = InitialSyncOverwrite
		}},
		{name: "maxTxnOps below two", mutate: func(c *Config) { c.MaxTxnOps = 1 },
			wantErr: "maxTxnOps must be >= 2"},
		{name: "negative txnFlushBytes", mutate: func(c *Config) { c.TxnFlushBytes = -1 },
			wantErr: "positive"},
		{name: "negative pageBytes", mutate: func(c *Config) { c.PageBytes = -1 },
			wantErr: "positive"},
		{name: "negative pageKeyLimit", mutate: func(c *Config) { c.PageKeyLimit = -1 },
			wantErr: "positive"},
		{name: "negative maxOpsPerSecond", mutate: func(c *Config) { c.MaxOpsPerSecond = -1 },
			wantErr: "maxOpsPerSecond"},
		{name: "negative watchBufferBytes", mutate: func(c *Config) { c.WatchBufferBytes = -1 },
			wantErr: "watchBufferBytes"},
		{name: "checkpoint key outside dest prefix", mutate: func(c *Config) { c.CheckpointKey = "/elsewhere" },
			wantErr: "checkpointKey"},
		{name: "empty exclude entry", mutate: func(c *Config) { c.ExcludePrefixes = []string{""} },
			wantErr: "excludePrefixes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := minimalCfg()
			tc.mutate(&c)
			err := c.withDefaults().Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestConfigValidateReconcileInterval: negative intervals are rejected, 0
// (disabled) and positive intervals pass, and ReconcileDeleteOrphans with a
// zero interval is accepted as the documented no-op.
func TestConfigValidateReconcileInterval(t *testing.T) {
	c := minimalCfg()
	c.ReconcileInterval = -time.Second
	err := c.withDefaults().Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconcileInterval")

	c = minimalCfg()
	assert.NoError(t, c.withDefaults().Validate(), "zero interval (disabled) must validate")

	c = minimalCfg()
	c.ReconcileInterval = time.Minute
	assert.NoError(t, c.withDefaults().Validate())

	c = minimalCfg()
	c.ReconcileDeleteOrphans = true
	assert.NoError(t, c.withDefaults().Validate(),
		"deleteOrphans with the periodic pass disabled is a documented no-op, not an error")
}

// TestNormalizePrefixes: nested/duplicate exclude entries are collapsed at
// defaulting time — count corrections assume a disjoint set, and a key
// covered by two overlapping entries must never be subtracted twice
// (previously a byte-exact converged drain could fail verification).
func TestNormalizePrefixes(t *testing.T) {
	c := minimalCfg()
	c.ExcludePrefixes = []string{"/s/tmp/cache/", "/s/tmp/", "/s/other/", "/s/tmp/"}
	c = c.withDefaults()
	require.NoError(t, c.Validate())
	assert.Equal(t, []string{"/s/other/", "/s/tmp/"}, c.ExcludePrefixes)

	c2 := minimalCfg()
	c2.ExcludePrefixes = []string{"/s/a/"}
	assert.Equal(t, []string{"/s/a/"}, c2.withDefaults().ExcludePrefixes)
	c3 := minimalCfg()
	assert.Empty(t, c3.withDefaults().ExcludePrefixes)
}

// TestCheckpointKeyConvention: the default reserved key uses the
// \x00-after-prefix convention, which no real key under the prefix can
// collide with, and lives under the effective destination prefix.
func TestCheckpointKeyConvention(t *testing.T) {
	c := Config{LinkUID: "l", Epoch: 1, SourcePrefix: "/s/", TargetPrefix: "/d/", DestPrefix: "sub/"}
	c = c.withDefaults()
	require.NoError(t, c.Validate())
	assert.Equal(t, "/d/sub/", c.EffectiveDestPrefix())
	assert.Equal(t, "/d/sub/\x00etcdmirror-checkpoint", c.CheckpointKey)
}
