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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// backoffRoundsBeforeWatchCancel is how many transient/throttle backoff
// rounds a single fenced apply endures before the live source watch is
// cancelled: clientv3 buffers undelivered watch responses without bound, so
// a sustained target stall must stop the source stream (the tail re-watches
// from the checkpoint watermark once applies succeed again). Quota parks
// cancel immediately — they are expected to last minutes to hours.
const backoffRoundsBeforeWatchCancel = 3

// applyOps writes one flush set plus checkpoint f in a single fenced Txn,
// through the class-appropriate retry policy. A multi-revision set the
// target rejects for its Txn limits gets ONE shrink attempt at revision
// granularity (the checkpoint advancing with each sub-Txn); a single
// revision that alone trips the limits is irreducible and stays permanent.
func (a *Agent) applyOps(ctx context.Context, fs *flushSet, f FenceValue) error {
	a.pace(ctx, len(fs.ops))
	err := a.applyFlushSet(ctx, fs, f)
	var tle *TooLargeError
	if err == nil || !errors.As(err, &tle) || len(fs.groups) <= 1 {
		return err
	}
	// Shrink: the whole set fit the engine's own watermarks but not the
	// target's --max-txn-ops / --max-request-bytes (foreign cluster; its
	// flags are not inspectable). Re-commit revision by revision — every
	// sub-Txn still cuts at a revision boundary.
	for _, g := range fs.groups {
		sub := flushSet{
			ops:        g.ops,
			groups:     []revGroup{g},
			watermark:  g.rev,
			lastSrcKey: g.ops[len(g.ops)-1].srcKey,
		}
		fsub := f
		fsub.Watermark = g.rev
		if fsub.Scanning {
			fsub.ScanCursor = sub.lastSrcKey
		}
		if serr := a.applyFlushSet(ctx, &sub, fsub); serr != nil {
			return serr
		}
	}
	return nil
}

// applyFlushSet converts one flush set to ops and commits it fenced.
func (a *Agent) applyFlushSet(ctx context.Context, fs *flushSet, f FenceValue) error {
	ops := make([]clientv3.Op, 0, len(fs.ops))
	var nBytes int64
	for _, o := range fs.ops {
		nBytes += o.bytes()
		if o.isDelete {
			ops = append(ops, clientv3.OpDelete(o.key))
		} else {
			ops = append(ops, clientv3.OpPut(o.key, o.value))
		}
	}
	return a.applyFenced(ctx, ops, f, fs.ops[0].key, nBytes)
}

// applyFenced commits ops plus the checkpoint write under the fence compare,
// retrying per class: transient and throttle back off on their own curves,
// quota parks on the flat probe interval (never backoff — quota only heals
// when an operator acts), resync and permanent errors propagate. A fence
// claim (before this generation's first successful commit) that loses a race
// against an older generation's last write retries the takeover; post-claim,
// a moved fence is a loud permanent failure.
func (a *Agent) applyFenced(
	ctx context.Context, ops []clientv3.Op, f FenceValue, firstKey string, nBytes int64,
) error {
	prior := a.phase()
	rounds := 0
	for {
		err := a.commitFenced(ctx, ops, f)
		if err == nil {
			a.bo.noteSuccess()
			a.update(func(s *Snapshot) {
				s.Throttled = false
				s.QuotaExhausted = false
				s.LastError = ""
				s.LastErrorClass = ""
				if s.Phase == PhaseDegraded && prior != PhaseDegraded {
					s.Phase = prior
				}
			})
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errFenceLost) {
			if !a.claimed {
				// Pre-claim race: an older generation wrote between our fence
				// read and this claim. fenceViolation adopted the raced mod
				// revision, so the retry re-runs the takeover against it.
				a.recordErr(err, ClassTransient)
				if serr := sleepCtx(ctx, a.bo.next(ClassTransient)); serr != nil {
					return serr
				}
				continue
			}
			// Post-claim, older generations fail their own compares and can
			// never move the fence again: surface loudly rather than retrying
			// against a stale mod revision forever.
			err = &FenceError{Detail: "checkpoint mod revision moved under an active generation"}
		}
		class := Classify(err)
		if class == ClassPermanent && isOversized(err) {
			err = &TooLargeError{
				Key:   RedactKey(a.cfg.EffectiveDestPrefix(), []byte(firstKey)),
				Ops:   len(ops) + 1,
				Bytes: nBytes,
				Cause: err,
			}
		}
		a.recordErr(err, class)
		switch class {
		case ClassQuota:
			a.cancelSourceWatch()
			a.update(func(s *Snapshot) {
				s.QuotaExhausted = true
				s.Phase = PhaseDegraded
			})
			if serr := sleepCtx(ctx, a.cfg.QuotaProbeInterval); serr != nil {
				return serr
			}
		case ClassThrottle:
			if rounds++; rounds >= backoffRoundsBeforeWatchCancel {
				a.cancelSourceWatch()
			}
			a.update(func(s *Snapshot) {
				s.Throttled = true
				s.Phase = PhaseDegraded
			})
			if serr := sleepCtx(ctx, a.bo.next(class)); serr != nil {
				return serr
			}
		case ClassTransient:
			if rounds++; rounds >= backoffRoundsBeforeWatchCancel {
				a.cancelSourceWatch()
			}
			a.setPhase(PhaseDegraded)
			if serr := sleepCtx(ctx, a.bo.next(class)); serr != nil {
				return serr
			}
		default:
			return err
		}
	}
}

// commitFenced is one fenced Txn attempt: ops plus the checkpoint write in
// the reserved op slot, guarded by the mod-revision compare on the reserved
// key. On success the cached fence and mod revision advance together. A
// failed compare is resolved by fenceViolation, which recognizes this
// generation's own ambiguously-timed-out commit and adopts it as success.
func (a *Agent) commitFenced(ctx context.Context, ops []clientv3.Op, f FenceValue) error {
	val, err := f.Encode()
	if err != nil {
		// Encoding our own fence value can only fail on an engine bug; fail
		// closed the same way a corrupt stored checkpoint does.
		return &CheckpointInvalidError{Reason: fmt.Sprintf("encoding checkpoint: %v", err)}
	}
	cmp := clientv3.Compare(clientv3.ModRevision(a.cfg.CheckpointKey), "=", a.fenceModRev)
	all := make([]clientv3.Op, 0, len(ops)+1)
	all = append(all, ops...)
	all = append(all, clientv3.OpPut(a.cfg.CheckpointKey, val))
	tctx, cancel := context.WithTimeout(ctx, a.cfg.RequestTimeout)
	resp, err := a.dst.Txn(tctx).If(cmp).Then(all...).Commit()
	cancel()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return a.fenceViolation(ctx, f, val)
	}
	a.fence = f
	a.fenceModRev = resp.Header.Revision
	a.claimed = true
	return nil
}

// fenceViolation resolves a failed fence compare by re-reading the reserved
// key — never a blind re-Commit (doc.go's retry-ownership contract). Three
// outcomes:
//
//   - The stored value is byte-identical to the value this attempt was
//     writing: an earlier attempt of this exact Txn committed but its
//     response was lost (the classic ambiguous timeout — the Txn's own
//     checkpoint Put bumped the fence ModRevision). The data ops landed
//     exactly once; adopt the new mod revision and report success (nil).
//   - The stored fence is an OLDER generation of this link: a claim raced
//     the old generation's last write. Adopt the raced mod revision and
//     return errFenceLost so the caller retries the takeover.
//   - Anything else (another link, a newer epoch, a Primary role we did not
//     write): a genuine, permanent fence violation.
func (a *Agent) fenceViolation(ctx context.Context, f FenceValue, attempted string) error {
	resp, err := a.getRetry(ctx, a.dst, a.cfg.CheckpointKey)
	if err != nil {
		// getRetry already retried transient/throttle reads; what escapes is
		// cancellation or a permanent read failure.
		return fmt.Errorf("re-reading fence after a failed compare: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return &FenceError{Detail: "the reserved key was deleted under an active generation"}
	}
	kv := resp.Kvs[0]
	if string(kv.Value) == attempted {
		a.fence = f
		a.fenceModRev = kv.ModRevision
		a.claimed = true
		return nil
	}
	stored, derr := DecodeFenceValue(kv.Value)
	if derr != nil {
		return &FenceError{Detail: "checkpoint mod revision moved and the current fence is undecodable"}
	}
	switch {
	case stored.LinkUID != a.cfg.LinkUID:
		return &FenceError{Detail: fmt.Sprintf("fence taken over by link %q", stored.LinkUID)}
	case stored.Role == RolePrimary:
		return &FenceError{
			Detail: "fence role is Primary: cutover completed, mirror writes are forbidden",
		}
	case stored.Epoch > a.cfg.Epoch:
		return &FenceError{Detail: fmt.Sprintf(
			"newer agent epoch %d owns the link (this agent is epoch %d)", stored.Epoch, a.cfg.Epoch)}
	case stored.Epoch < a.cfg.Epoch:
		a.fenceModRev = kv.ModRevision
		return errFenceLost
	default:
		return &FenceError{Detail: "another agent with the same epoch holds the fence"}
	}
}

// isOversized reports whether err is the server's Txn size/op-count limit or
// the gRPC client send cap — permanent errors that must surface the poison
// batch, never be retried as throttling.
func isOversized(err error) bool {
	switch rpctypes.Error(err) {
	case rpctypes.ErrTooManyOps, rpctypes.ErrRequestTooLarge:
		return true
	}
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.ResourceExhausted && strings.Contains(s.Message(), "larger than max")
	}
	return false
}
