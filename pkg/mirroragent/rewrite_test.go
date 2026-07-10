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

// TestRewriteFormula pins the single anchored rewrite formula:
// key' = target.prefix + destPrefix + TrimPrefix(key, source.prefix).
func TestRewriteFormula(t *testing.T) {
	cases := []struct {
		name   string
		cfg    Config
		srcKey string
		want   string
		wantOK bool
	}{
		{name: "default destPrefix strips the source prefix",
			cfg:    Config{SourcePrefix: "/apps/", TargetPrefix: "/mirror/"},
			srcKey: "/apps/foo", want: "/mirror/foo", wantOK: true},
		{name: "non-empty destPrefix is the middle term",
			cfg:    Config{SourcePrefix: "/apps/", TargetPrefix: "/mirror/", DestPrefix: "west/"},
			srcKey: "/apps/foo", want: "/mirror/west/foo", wantOK: true},
		{name: "key equal to the source prefix maps to the effective dest prefix",
			cfg:    Config{SourcePrefix: "/apps/", TargetPrefix: "/mirror/", DestPrefix: "west/"},
			srcKey: "/apps/", want: "/mirror/west/", wantOK: true},
		{name: "anchored, never a substring replace",
			cfg:    Config{SourcePrefix: "/apps/", TargetPrefix: "/mirror/"},
			srcKey: "/apps/foo/apps/bar", want: "/mirror/foo/apps/bar", wantOK: true},
		{name: "out of scope",
			cfg:    Config{SourcePrefix: "/apps/", TargetPrefix: "/mirror/"},
			srcKey: "/other/foo", wantOK: false},
		{name: "excluded prefix",
			cfg: Config{SourcePrefix: "/apps/", TargetPrefix: "/mirror/",
				ExcludePrefixes: []string{"/apps/skip/"}},
			srcKey: "/apps/skip/foo", wantOK: false},
		{name: "empty source prefix mirrors the whole keyspace",
			cfg:    Config{TargetPrefix: "/mirror/"},
			srcKey: "/anything", want: "/mirror//anything", wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := newRewriter(tc.cfg.withDefaults())
			got, ok := rw.rewrite(tc.srcKey)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

// TestRewriteReservedKeyCollision: a source key whose image would be the
// reserved checkpoint key is never produced by any apply path.
func TestRewriteReservedKeyCollision(t *testing.T) {
	cfg := Config{SourcePrefix: "/s/", TargetPrefix: "/d/", CheckpointKey: "/d/ckpt"}.withDefaults()
	rw := newRewriter(cfg)

	_, ok := rw.rewrite("/s/ckpt")
	assert.False(t, ok, "the reserved key's preimage must be dropped")
	got, ok := rw.rewrite("/s/ckpt2")
	assert.True(t, ok, "only the exact match is reserved")
	assert.Equal(t, "/d/ckpt2", got)
}

func TestExcludedImage(t *testing.T) {
	cfg := Config{
		SourcePrefix:    "/s/",
		TargetPrefix:    "/d/",
		ExcludePrefixes: []string{"/s/skip/", "/elsewhere/"},
	}.withDefaults()
	rw := newRewriter(cfg)

	assert.True(t, rw.excludedImage("/d/skip/x"), "images of excluded ranges are never orphans")
	assert.False(t, rw.excludedImage("/d/keep/x"))
	assert.False(t, rw.excludedImage("/d/elsewhere/x"),
		"excludes outside the mirrored scope have no image")
}

func TestKeyRange(t *testing.T) {
	s, e := keyRange("/p/")
	assert.Equal(t, "/p/", s)
	assert.Equal(t, "/p0", e, "prefix range end increments the last byte")

	s, e = keyRange("")
	assert.Equal(t, "\x00", s, "empty prefix means the whole keyspace")
	assert.Equal(t, "\x00", e, "with the >=-key sentinel range end")
}

// TestScanRanges pins the range decomposition: the genesis scan reads ONLY
// the source range minus the excluded ranges — excluded data is elided
// server-side, not filtered client-side.
func TestScanRanges(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		exclude []string
		want    []scanRange
	}{
		{name: "no excludes", src: "/s/",
			want: []scanRange{{"/s/", "/s0"}}},
		{name: "one middle exclude", src: "/s/", exclude: []string{"/s/b/"},
			want: []scanRange{{"/s/", "/s/b/"}, {"/s/b0", "/s0"}}},
		{name: "two excludes stay sorted", src: "/s/", exclude: []string{"/s/b/", "/s/d/"},
			want: []scanRange{{"/s/", "/s/b/"}, {"/s/b0", "/s/d/"}, {"/s/d0", "/s0"}}},
		{name: "exclude at the start", src: "/s/", exclude: []string{"/s/"},
			want: []scanRange{}},
		{name: "exclude covering the whole source prefix", src: "/s/sub/", exclude: []string{"/s/"},
			want: []scanRange{}},
		{name: "exclude outside the source range", src: "/s/", exclude: []string{"/t/"},
			want: []scanRange{{"/s/", "/s0"}}},
		{name: "whole keyspace with one exclude", src: "", exclude: []string{"/b/"},
			want: []scanRange{{"\x00", "/b/"}, {"/b0", "\x00"}}},
		{name: "nested excludes merge", src: "/s/", exclude: []string{"/s/b/", "/s/b/c/"},
			want: []scanRange{{"/s/", "/s/b/"}, {"/s/b0", "/s0"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := newRewriter(Config{
				SourcePrefix:    tc.src,
				TargetPrefix:    "/d/",
				ExcludePrefixes: tc.exclude,
			}.withDefaults())
			got := rw.scanRanges()
			if len(tc.want) == 0 {
				assert.Empty(t, got)
				return
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestEndAfter(t *testing.T) {
	assert.True(t, endAfter(rangeEndInf, "/z/"), "the sentinel end is +inf")
	assert.True(t, endAfter("/b/", "/a/"))
	assert.False(t, endAfter("/a/", "/a/"))
	assert.False(t, endAfter("/a/", "/b/"))
}
