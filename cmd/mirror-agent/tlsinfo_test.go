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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildTLSInfo(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		info, enabled, err := buildTLSInfo(&sideFlags{side: sideSource})
		require.NoError(t, err)
		require.False(t, enabled, "no --source-tls means cleartext (nil tls.Config)")
		require.True(t, info.Empty())
	})

	t.Run("enabled with no files uses system roots", func(t *testing.T) {
		info, enabled, err := buildTLSInfo(&sideFlags{side: sideSource, tls: true})
		require.NoError(t, err)
		require.True(t, enabled)
		require.True(t, info.Empty())
		require.Empty(t, info.TrustedCAFile)
	})

	t.Run("bundle takes precedence over ca file", func(t *testing.T) {
		info, enabled, err := buildTLSInfo(&sideFlags{
			side: sideTarget, tls: true,
			caFile: "/mnt/tls/ca.crt", caBundleFile: "/mnt/bundle/ca.crt",
		})
		require.NoError(t, err)
		require.True(t, enabled)
		require.Equal(t, "/mnt/bundle/ca.crt", info.TrustedCAFile)
	})

	t.Run("propagates server name, skip-verify and leaf paths", func(t *testing.T) {
		info, _, err := buildTLSInfo(&sideFlags{
			side: sideSource, tls: true,
			certFile: "/mnt/tls/tls.crt", keyFile: "/mnt/tls/tls.key",
			caFile: "/mnt/tls/ca.crt", serverName: "etcd.example.com",
			insecureSkipVerify: true,
		})
		require.NoError(t, err)
		require.Equal(t, "/mnt/tls/tls.crt", info.CertFile)
		require.Equal(t, "/mnt/tls/tls.key", info.KeyFile)
		require.Equal(t, "/mnt/tls/ca.crt", info.TrustedCAFile)
		require.Equal(t, "etcd.example.com", info.ServerName)
		require.True(t, info.InsecureSkipVerify)
	})

	t.Run("cert without key", func(t *testing.T) {
		_, _, err := buildTLSInfo(&sideFlags{side: sideSource, tls: true, certFile: "/mnt/tls/tls.crt"})
		require.ErrorContains(t, err, "must be set together")
	})
}
