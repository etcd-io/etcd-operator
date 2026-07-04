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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Class
	}{
		// Engine-typed errors.
		{name: "resync compacted", err: &ResyncError{Reason: ResyncReasonCompacted}, want: ClassResync},
		{name: "resync cluster id", err: &ResyncError{Reason: ResyncReasonClusterIDMismatch}, want: ClassResync},
		{name: "wrapped resync", err: fmt.Errorf("cycle: %w", &ResyncError{Reason: ResyncReasonCompacted}),
			want: ClassResync},
		{name: "checkpoint invalid fails closed", err: &CheckpointInvalidError{Reason: "garbage"},
			want: ClassPermanent},
		{name: "fence violation", err: &FenceError{Detail: "taken over"}, want: ClassPermanent},
		{name: "too large", err: &TooLargeError{Key: "/dst/…deadbeef"}, want: ClassPermanent},
		{name: "empty target violation", err: &EmptyTargetViolationError{KeyCount: 3}, want: ClassPermanent},
		{name: "unsupported version", err: &UnsupportedVersionError{Side: "source", Version: "3.3.0"},
			want: ClassPermanent},
		{name: "drain verification", err: &DrainVerificationError{SourceKeys: 1, TargetKeys: 2},
			want: ClassPermanent},
		{name: "config error", err: &ConfigError{Detail: "no endpoints"}, want: ClassPermanent},
		{name: "prefix conflict", err: &PrefixConflictError{Key: "/dst/…deadbeef", OwnerLinkUID: "other"},
			want: ClassPermanent},

		// etcd-typed rpc errors (client-side singletons).
		{name: "no space", err: rpctypes.ErrNoSpace, want: ClassQuota},
		{name: "too many requests", err: rpctypes.ErrTooManyRequests, want: ClassThrottle},
		{name: "compacted", err: rpctypes.ErrCompacted, want: ClassResync},
		{name: "future rev", err: rpctypes.ErrFutureRev, want: ClassResync},
		{name: "too many ops", err: rpctypes.ErrTooManyOps, want: ClassPermanent},
		{name: "request too large", err: rpctypes.ErrRequestTooLarge, want: ClassPermanent},
		{name: "permission denied", err: rpctypes.ErrPermissionDenied, want: ClassPermanent},

		// etcd-typed rpc errors (gRPC wire form).
		{name: "no space wire", err: rpctypes.ErrGRPCNoSpace, want: ClassQuota},
		{name: "compacted wire", err: rpctypes.ErrGRPCCompacted, want: ClassResync},

		// The three-way codes.ResourceExhausted disambiguation: identical
		// gRPC code, three different classes — classification must never be
		// by code alone.
		{name: "resource exhausted quota",
			err:  status.Error(codes.ResourceExhausted, "etcdserver: mvcc: database space exceeded"),
			want: ClassQuota},
		{name: "resource exhausted throttle",
			err:  status.Error(codes.ResourceExhausted, "etcdserver: too many requests"),
			want: ClassThrottle},
		{name: "resource exhausted client send cap",
			err: status.Error(codes.ResourceExhausted,
				"trying to send message larger than max (3145728 vs. 2097152)"),
			want: ClassPermanent},
		{name: "resource exhausted unknown rate flavor",
			err:  status.Error(codes.ResourceExhausted, "some proxy rate limit"),
			want: ClassThrottle},

		// Context and transport errors.
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: ClassTransient},
		{name: "canceled", err: context.Canceled, want: ClassTransient},
		{name: "no leader", err: rpctypes.ErrNoLeader, want: ClassTransient},
		{name: "unavailable", err: status.Error(codes.Unavailable, "connection refused"), want: ClassTransient},
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, "etcdserver: request is too large"),
			want: ClassPermanent},
		{name: "unauthenticated", err: status.Error(codes.Unauthenticated, "invalid auth token"),
			want: ClassPermanent},
		{name: "out of range", err: status.Error(codes.OutOfRange, "required revision has been compacted"),
			want: ClassResync},

		// Unknowns default to transient: retrying an unknown error is
		// recoverable, silently dropping a permanent one is not.
		{name: "unknown error", err: errors.New("weather is bad"), want: ClassTransient},
		{name: "nil", err: nil, want: ClassTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Classify(tc.err))
		})
	}
}

func TestRedactKey(t *testing.T) {
	secret := []byte("/dst/tenants/acme/api-token-primary")
	got := RedactKey("/dst/", secret)

	assert.Equal(t, got, RedactKey("/dst/", secret), "redaction must be deterministic")
	assert.True(t, strings.HasPrefix(got, "/dst/…"), "the public prefix survives: %q", got)
	assert.Len(t, got, len("/dst/…")+8, "prefix + ellipsis + 8 hex chars")
	assert.NotContains(t, got, "tenants", "no raw key bytes beyond the prefix may surface")
	assert.NotContains(t, got, "acme")
	assert.NotEqual(t, RedactKey("/dst/", []byte("/dst/other")), got)
}

func TestTooLargeErrorNeverLeaksRawKey(t *testing.T) {
	// Mirrors the construction sites: Key is always the RedactKey form.
	e := &TooLargeError{
		Key:   RedactKey("/dst/", []byte("/dst/tenants/acme/api-token-primary")),
		Ops:   2,
		Bytes: 3 << 20,
		Cause: errors.New("etcdserver: request is too large"),
	}
	msg := e.Error()
	assert.NotContains(t, msg, "acme", "error text must not carry raw key bytes")
	assert.Contains(t, msg, "/dst/…", "error text must carry the redacted key for operators")
	assert.Equal(t, ClassPermanent, Classify(e))
}
