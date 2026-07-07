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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// Class is the engine's error taxonomy. Every failure the retry loop sees is
// classified into exactly one class, and each class has its own recovery
// policy — misclassification (e.g. labelling an oversized Txn "throttling")
// is itself a bug class this taxonomy exists to eliminate.
type Class string

const (
	// ClassTransient covers connection-class errors (unavailable, timeouts,
	// leader loss): retried with the standard exponential backoff.
	ClassTransient Class = "Transient"
	// ClassThrottle covers the target rejecting the write rate
	// (rpctypes.ErrTooManyRequests and other rate-flavored
	// ResourceExhausted): retried on a more conservative curve, distinct
	// from ClassTransient and never conflated with ClassQuota.
	ClassThrottle Class = "Throttle"
	// ClassResync covers conditions that invalidate the watch/checkpoint
	// position (source compaction outran the watch, a bound cluster identity
	// changed): the agent runs a forced resync (scan + mandatory
	// mark-and-sweep), counted by the resync-loop livelock detector.
	ClassResync Class = "Resync"
	// ClassQuota is rpctypes.ErrNoSpace from the target: permanent until an
	// operator compacts/defrags/disarms. The agent parks on a slow flat
	// probe instead of burning backoff against a full quota.
	ClassQuota Class = "Quota"
	// ClassPermanent is never retried: oversized requests (server
	// "request is too large"/"too many operations" and the client 2MiB
	// send cap), auth/permission misconfiguration, fence violations,
	// corrupt/unknown-version checkpoints, version-floor failures,
	// RequireEmpty violations.
	ClassPermanent Class = "Permanent"
)

// ResyncReason distinguishes why a forced resync was required.
type ResyncReason string

const (
	// ResyncReasonCompacted: source compaction outran the watch (restart or
	// pause longer than retention).
	ResyncReasonCompacted ResyncReason = "Compacted"
	// ResyncReasonClusterIDMismatch: a bound cluster ID (source or target)
	// no longer matches the probed cluster; genesis is forced and
	// RequireEmpty re-arms.
	ResyncReasonClusterIDMismatch ResyncReason = "ClusterIDMismatch"
)

// ResyncError forces a full resync (genesis scan + mark-and-sweep).
type ResyncError struct {
	Reason ResyncReason
	Cause  error
}

func (e *ResyncError) Error() string {
	return fmt.Sprintf("forced resync required (%s): %v", e.Reason, e.Cause)
}
func (e *ResyncError) Unwrap() error { return e.Cause }

// CheckpointInvalidError reports a corrupt or unknown-version checkpoint.
// Distinct from an absent checkpoint (plain genesis). PERMANENT: an
// undecodable fence cannot prove link ownership, epoch ordering, or role, so
// overwriting it (and running a resync's mandatory prune over data it may be
// protecting) is never safe. The operator must inspect the reserved key and
// delete it to recover.
type CheckpointInvalidError struct {
	Reason string
}

func (e *CheckpointInvalidError) Error() string {
	return "checkpoint invalid: " + e.Reason
}

// ConfigError reports a client/engine configuration defect detected at
// runtime (e.g. a client with no endpoints). Permanent.
type ConfigError struct {
	Detail string
}

func (e *ConfigError) Error() string { return "configuration error: " + e.Detail }

// FenceError is a fence violation: the reserved key's mod revision moved
// under us (another agent generation took over, or the role flipped to
// Primary at cutover). Permanent — this agent must never write again.
type FenceError struct {
	Detail string
}

func (e *FenceError) Error() string { return "fence violation: " + e.Detail }

// RedactKey returns a safe display form for a key surfaced in status,
// events, or logs: the configured destination prefix (already public in
// the spec) + "…" + the first 8 hex chars of sha256(key). Key bytes beyond
// the prefix are never surfaced; values never at all.
func RedactKey(prefix string, key []byte) string {
	sum := sha256.Sum256(key)
	return prefix + "…" + hex.EncodeToString(sum[:])[:8]
}

// TooLargeError is an oversized request: the server's Txn size or op-count
// limit, or the gRPC client send cap. Permanent; carries the offending key
// (never the value) so operators can find the poison key.
type TooLargeError struct {
	// Key is the redacted form (RedactKey) of the first key of the offending
	// batch (target-side): the destination prefix plus a hash — raw key
	// suffixes and values are deliberately never carried.
	Key   string
	Ops   int
	Bytes int64
	Cause error
}

func (e *TooLargeError) Error() string {
	return fmt.Sprintf("request too large (%d ops, %d bytes, first key %q): %v",
		e.Ops, e.Bytes, e.Key, e.Cause)
}
func (e *TooLargeError) Unwrap() error { return e.Cause }

// EmptyTargetViolationError reports a non-empty destination prefix under
// InitialSyncRequireEmpty. Permanent. The range identifies exactly what an
// operator must clear (`etcdctl del` over [RangeStart, RangeEnd)); the
// reserved checkpoint key was excluded from the count.
type EmptyTargetViolationError struct {
	RangeStart string
	RangeEnd   string
	KeyCount   int64
}

func (e *EmptyTargetViolationError) Error() string {
	return fmt.Sprintf(
		"destination prefix not empty: %d pre-existing keys in [%q, %q) and initialSyncMode is RequireEmpty",
		e.KeyCount, e.RangeStart, e.RangeEnd)
}

// PrefixConflictError reports another EtcdMirror link's reserved fence key
// found inside this link's effective destination prefix during a prune pass:
// two links target overlapping destination ranges on the same cluster.
// Deleting the sibling's fence (and its data, as "orphans") would silently
// destroy the other link, so the pass stops loudly instead. Permanent until
// the operator resolves the overlap.
type PrefixConflictError struct {
	// Key is the redacted form (RedactKey) of the foreign reserved key.
	Key string
	// OwnerLinkUID is the link that owns the foreign fence.
	OwnerLinkUID string
}

func (e *PrefixConflictError) Error() string {
	return fmt.Sprintf(
		"destination prefix conflict: reserved fence key %q under this link's destination prefix belongs to link %q",
		e.Key, e.OwnerLinkUID)
}

// DrainVerificationError reports a post-drain per-side key-count mismatch
// that one repair pass did not resolve. Permanent — cutover must not proceed
// on divergent data.
type DrainVerificationError struct {
	SourceKeys int64
	TargetKeys int64
}

func (e *DrainVerificationError) Error() string {
	return fmt.Sprintf("drain verification failed: source has %d keys, target has %d",
		e.SourceKeys, e.TargetKeys)
}

// UnsupportedVersionError reports an etcd server below the declared >=3.4
// hard floor. Permanent.
type UnsupportedVersionError struct {
	Side    string // "source" or "target"
	Version string
}

func (e *UnsupportedVersionError) Error() string {
	return fmt.Sprintf("%s etcd version %s is below the supported floor %s", e.Side, e.Version, hardVersionFloor)
}

// ReasonFor maps a typed engine error to its API-aligned condition/phase
// reason string ("" for untyped errors). The controller reads this off
// Snapshot.LastErrorReason instead of matching LastError message substrings,
// which would break on any message edit. Same errors.As ladder as Classify.
func ReasonFor(err error) string {
	if err == nil {
		return ""
	}
	var (
		cpInvalid    *CheckpointInvalidError
		fenceErr     *FenceError
		tooLarge     *TooLargeError
		emptyTarget  *EmptyTargetViolationError
		unsupportedV *UnsupportedVersionError
		drainVerify  *DrainVerificationError
		configErr    *ConfigError
		prefixErr    *PrefixConflictError
	)
	switch {
	case errors.As(err, &unsupportedV):
		return "UnsupportedVersion"
	case errors.As(err, &cpInvalid):
		return "CheckpointInvalid"
	case errors.As(err, &emptyTarget):
		return "EmptyTargetViolation"
	case errors.As(err, &prefixErr):
		return "PrefixConflict"
	case errors.As(err, &drainVerify):
		return "DrainVerificationFailed"
	case errors.As(err, &tooLarge):
		return "RequestTooLarge"
	case errors.As(err, &configErr):
		return "InvalidConfig"
	case errors.As(err, &fenceErr):
		return "FenceLost"
	}
	return ""
}

// Classify maps any error the engine encounters to its taxonomy class.
// Typed engine errors win; then etcd's typed rpc errors; then gRPC status
// codes; unknown errors default to transient (retrying an unknown error is
// recoverable, silently dropping a permanent one is not).
//
// The engine owns 100% of write-path retry per the class returned here —
// see the retry-ownership contract on [backoff]. Never classify by gRPC
// code alone: ErrNoSpace (quota), ErrTooManyRequests (throttle), and the
// client send cap (permanent) all share codes.ResourceExhausted.
func Classify(err error) Class {
	if err == nil {
		return ClassTransient
	}

	// Engine-typed errors first.
	var (
		resyncErr    *ResyncError
		cpInvalid    *CheckpointInvalidError
		fenceErr     *FenceError
		tooLarge     *TooLargeError
		emptyTarget  *EmptyTargetViolationError
		unsupportedV *UnsupportedVersionError
		drainVerify  *DrainVerificationError
		configErr    *ConfigError
		prefixErr    *PrefixConflictError
	)
	switch {
	case errors.As(err, &resyncErr):
		return ClassResync
	case errors.As(err, &cpInvalid),
		errors.As(err, &fenceErr),
		errors.As(err, &tooLarge),
		errors.As(err, &emptyTarget),
		errors.As(err, &unsupportedV),
		errors.As(err, &drainVerify),
		errors.As(err, &configErr),
		errors.As(err, &prefixErr):
		return ClassPermanent
	}

	// etcd-typed errors: normalize the raw gRPC error to its canonical
	// rpctypes singleton where one exists, so both wire and pre-converted
	// forms classify identically.
	switch rpctypes.Error(err) {
	case rpctypes.ErrNoSpace:
		return ClassQuota
	case rpctypes.ErrTooManyRequests:
		return ClassThrottle
	case rpctypes.ErrCompacted, rpctypes.ErrFutureRev:
		return ClassResync
	case rpctypes.ErrTooManyOps, rpctypes.ErrRequestTooLarge:
		return ClassPermanent
	case rpctypes.ErrPermissionDenied, rpctypes.ErrUserEmpty, rpctypes.ErrAuthFailed:
		return ClassPermanent
	}

	// Context errors: cancellation/deadline of our own contexts.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ClassTransient
	}

	// Remaining gRPC status codes.
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.ResourceExhausted:
			// The client-side send cap surfaces as ResourceExhausted
			// "trying to send message larger than max": that is an oversized
			// request, NOT throttling — mislabelling it throttling retries a
			// poison batch forever.
			if strings.Contains(s.Message(), "larger than max") {
				return ClassPermanent
			}
			return ClassThrottle
		case codes.InvalidArgument:
			return ClassPermanent
		case codes.PermissionDenied, codes.Unauthenticated:
			return ClassPermanent
		case codes.OutOfRange:
			return ClassResync
		}
	}

	return ClassTransient
}
