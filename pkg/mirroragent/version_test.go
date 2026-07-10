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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClassifyVersion pins the version gates: the >=3.4.0 hard floor (below
// it the agent fails permanently) and the 3.4.25/3.5.8 progress-trust floor
// (below it progress notifications can report revisions ahead of delivered
// events, so trusting them would checkpoint the watermark past undelivered
// data — silent loss after a restart).
func TestClassifyVersion(t *testing.T) {
	cases := []struct {
		version   string
		wantErr   bool
		wantFloor bool // UnsupportedVersionError specifically
		wantTrust bool
	}{
		{version: "3.3.9", wantErr: true, wantFloor: true},
		{version: "3.0.0", wantErr: true, wantFloor: true},
		{version: "3.4.0", wantTrust: false},
		{version: "3.4.24", wantTrust: false},
		{version: "3.4.25", wantTrust: true},
		{version: "3.4.33", wantTrust: true},
		{version: "3.5.0", wantTrust: false},
		{version: "3.5.7", wantTrust: false},
		{version: "3.5.8", wantTrust: true},
		{version: "3.6.0", wantTrust: true},
		{version: "3.6.12", wantTrust: true},
		{version: "4.0.0", wantTrust: true},
		{version: "garbage", wantErr: true},
		{version: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			info, err := classifyVersion("source", tc.version)
			if tc.wantErr {
				require.Error(t, err)
				var uv *UnsupportedVersionError
				if tc.wantFloor {
					require.ErrorAs(t, err, &uv)
					assert.Equal(t, "source", uv.Side)
					assert.Equal(t, ClassPermanent, Classify(err))
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.version, info.Version)
			assert.Equal(t, tc.wantTrust, info.TrustProgressNotify,
				"progress-trust gate for %s", tc.version)
		})
	}
}
