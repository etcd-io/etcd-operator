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

func validFence() FenceValue {
	return FenceValue{
		LinkUID:         "link-1",
		Epoch:           3,
		Role:            RoleMirror,
		Watermark:       42,
		SubRevision:     7,
		Scanning:        true,
		ScanCursor:      "/src/key-0100",
		PrunePending:    true,
		SourceClusterID: 0xdeadbeefcafef00d,
		TargetClusterID: 0x0123456789abcdef,
	}
}

func TestFenceValueRoundTrip(t *testing.T) {
	in := validFence()
	raw, err := in.Encode()
	require.NoError(t, err)

	out, err := DecodeFenceValue([]byte(raw))
	require.NoError(t, err)
	in.Version = FenceVersion // Encode stamps it
	assert.Equal(t, in, out)
	assert.True(t, out.PrunePending, "PrunePending must survive the round trip")
	assert.Equal(t, uint64(0xdeadbeefcafef00d), out.SourceClusterID,
		"cluster IDs must survive the full uint64 range (string-encoded)")
}

// TestDecodeFenceValueFailsClosed pins the WIP semantics: corrupt content or
// an unknown wire version is a *CheckpointInvalidError AND classifies
// Permanent — never a resync, never a resume on a guess.
func TestDecodeFenceValueFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "garbage", raw: "not json at all {"},
		{name: "empty", raw: ""},
		{name: "future version", raw: `{"v":99,"linkUID":"l","epoch":1,"role":"Mirror",` +
			`"watermark":1,"sourceClusterID":"1","targetClusterID":"2"}`},
		{name: "version zero", raw: `{"linkUID":"l","epoch":1,"role":"Mirror",` +
			`"sourceClusterID":"1","targetClusterID":"2"}`},
		{name: "empty linkUID", raw: `{"v":1,"linkUID":"","epoch":1,"role":"Mirror",` +
			`"sourceClusterID":"1","targetClusterID":"2"}`},
		{name: "epoch below one", raw: `{"v":1,"linkUID":"l","epoch":0,"role":"Mirror",` +
			`"sourceClusterID":"1","targetClusterID":"2"}`},
		{name: "unknown role", raw: `{"v":1,"linkUID":"l","epoch":1,"role":"Standby",` +
			`"sourceClusterID":"1","targetClusterID":"2"}`},
		{name: "negative watermark", raw: `{"v":1,"linkUID":"l","epoch":1,"role":"Mirror",` +
			`"watermark":-5,"sourceClusterID":"1","targetClusterID":"2"}`},
		{name: "negative subrevision", raw: `{"v":1,"linkUID":"l","epoch":1,"role":"Mirror",` +
			`"subRevision":-1,"sourceClusterID":"1","targetClusterID":"2"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeFenceValue([]byte(tc.raw))
			var ci *CheckpointInvalidError
			require.ErrorAs(t, err, &ci)
			assert.Equal(t, ClassPermanent, Classify(err),
				"an undecodable checkpoint must fail closed (permanent), not resync")
		})
	}
}

func TestFenceValueEncodeRejectsInvalid(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*FenceValue)
	}{
		{name: "empty linkUID", mutate: func(f *FenceValue) { f.LinkUID = "" }},
		{name: "epoch below one", mutate: func(f *FenceValue) { f.Epoch = 0 }},
		{name: "bad role", mutate: func(f *FenceValue) { f.Role = "Replica" }},
		{name: "negative watermark", mutate: func(f *FenceValue) { f.Watermark = -1 }},
		{name: "negative subrevision", mutate: func(f *FenceValue) { f.SubRevision = -2 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := validFence()
			tc.mutate(&f)
			_, err := f.Encode()
			assert.Error(t, err)
		})
	}
}
