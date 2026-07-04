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
	"encoding/json"
	"fmt"
)

// FenceRole is the role stamped into the fence key. Applies are only legal
// while the role is Mirror; the drain flow flips it to Primary at cutover so
// a straggler mirror apply fails its mod-revision compare loudly.
type FenceRole string

const (
	// RoleMirror means the mirror owns the destination prefix and applies
	// are in flight.
	RoleMirror FenceRole = "Mirror"
	// RolePrimary means cutover completed: the destination prefix is now
	// authoritative and no mirror may write under it.
	RolePrimary FenceRole = "Primary"
)

// FenceVersion is the current checkpoint wire-format version. Decoding fails
// closed on any other version (distinct from an absent checkpoint).
const FenceVersion = 1

// FenceValue is the checkpoint/fence document stored at the reserved key in
// the target etcd. It is written in the same Txn as every applied batch;
// its mod revision is the compare every write path is fenced on.
//
// State encoding — the tuple (Scanning, PrunePending, Role) subsumes an
// explicit phase enum:
//
//	Scanning=true               genesis scan in flight: Watermark is the
//	                            scan's watch-start revision R0 (the replay
//	                            base), NOT a caught-up-through claim;
//	                            progress lives in ScanCursor/SubRevision.
//	                            Consumers must never read Watermark as
//	                            replication progress while Scanning is true.
//	Scanning=false, Role=Mirror steady state: Watermark is the source
//	                            revision fully applied through.
//	PrunePending=true           a forced resync's mandatory mark-and-sweep
//	                            is owed; survives crashes.
//	Role=Primary                cutover complete: no mirror may write under
//	                            the destination prefix; stragglers fail
//	                            their mod-revision compare loudly.
type FenceValue struct {
	// Version is the wire-format version (FenceVersion).
	Version int `json:"v"`
	// LinkUID identifies the mirror link that owns this fence.
	LinkUID string `json:"linkUID"`
	// Epoch is the agent generation that last wrote the checkpoint.
	Epoch int64 `json:"epoch"`
	// Role is Mirror until cutover flips it to Primary.
	Role FenceRole `json:"role"`
	// Watermark is the source revision through which the target is caught
	// up. While a genesis scan is in flight it is the scan's watch-start
	// revision (the base the buffered watch replays over), not a fully
	// applied revision — Scanning distinguishes the two.
	Watermark int64 `json:"watermark"`
	// SubRevision is the ordinal progress marker within a Watermark that is
	// not yet revision-complete: the genesis scan stamps its page ordinal
	// here. Zero on every revision-complete checkpoint.
	SubRevision int64 `json:"subRevision,omitempty"`
	// Scanning is true while a genesis scan is in flight; ScanCursor is then
	// the last source key whose page has been applied, so a restarted agent
	// resumes the scan instead of starting over.
	Scanning   bool   `json:"scanning,omitempty"`
	ScanCursor string `json:"scanCursor,omitempty"`
	// PrunePending is true from the moment a forced resync claims the fence
	// until its mandatory mark-and-sweep prune pass has completed. It makes
	// the owed sweep durable: an agent that crashes mid-forced-resync and
	// resumes the scan still runs the prune, so deletes from the blind window
	// that triggered the resync cannot silently resurrect on the target.
	PrunePending bool `json:"prunePending,omitempty"`
	// SourceClusterID / TargetClusterID bind the checkpoint to BOTH cluster
	// identities. Either changing means an endpoint now points at a
	// different cluster than the checkpoint was taken against: the
	// checkpoint is invalidated, genesis is forced, and RequireEmpty
	// re-arms. String-encoded: cluster IDs use the full uint64 range.
	SourceClusterID uint64 `json:"sourceClusterID,string"`
	TargetClusterID uint64 `json:"targetClusterID,string"`
}

// Encode serializes the fence value for storage at the reserved key. The
// wire-format version is stamped unconditionally.
func (f FenceValue) Encode() (string, error) {
	f.Version = FenceVersion
	if err := f.validate(); err != nil {
		return "", err
	}
	b, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DecodeFenceValue parses a stored checkpoint. Corrupt content or an unknown
// version returns a *CheckpointInvalidError, which classifies PERMANENT: an
// undecodable fence proves nothing about ownership, epoch ordering, or role
// (it may be a newer agent generation's format, or a corrupted post-cutover
// Primary fence), so no write — least of all a resync's prune — is provably
// safe. The operator must inspect and delete the reserved key to recover.
func DecodeFenceValue(raw []byte) (FenceValue, error) {
	var f FenceValue
	if err := json.Unmarshal(raw, &f); err != nil {
		return FenceValue{}, &CheckpointInvalidError{Reason: fmt.Sprintf("undecodable checkpoint: %v", err)}
	}
	if f.Version != FenceVersion {
		return FenceValue{}, &CheckpointInvalidError{
			Reason: fmt.Sprintf("unknown checkpoint version %d (agent supports %d)", f.Version, FenceVersion),
		}
	}
	if err := f.validate(); err != nil {
		return FenceValue{}, &CheckpointInvalidError{Reason: err.Error()}
	}
	return f, nil
}

func (f FenceValue) validate() error {
	if f.LinkUID == "" {
		return fmt.Errorf("checkpoint has empty linkUID")
	}
	if f.Epoch < 1 {
		return fmt.Errorf("checkpoint has invalid epoch %d", f.Epoch)
	}
	if f.Role != RoleMirror && f.Role != RolePrimary {
		return fmt.Errorf("checkpoint has invalid role %q", f.Role)
	}
	if f.Watermark < 0 || f.SubRevision < 0 {
		return fmt.Errorf("checkpoint has negative revision fields")
	}
	return nil
}
