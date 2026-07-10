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
	"fmt"

	"github.com/coreos/go-semver/semver"
)

// hardVersionFloor is the declared minimum etcd server version. Below it the
// agent fails permanently (UnsupportedVersion) rather than degrading in
// undefined ways.
const hardVersionFloor = "3.4.0"

// Watch progress notifications are unreliable below 3.4.25 / 3.5.8. Below
// these floors the agent does not trust progress notifications: the
// watermark only advances on applies, so idle prefixes resync from scratch
// after any restart longer than retention, and a Drain may not terminate on
// a quiet prefix.
var progressTrustFloors = map[int64]semver.Version{
	4: {Major: 3, Minor: 4, Patch: 25},
	5: {Major: 3, Minor: 5, Patch: 8},
}

// versionInfo is the outcome of the connect-time maintenance Status() probe.
type versionInfo struct {
	Version string
	// TrustProgressNotify gates the watermark machinery that drives lag,
	// idle-prefix checkpointing, and the Drain gate.
	TrustProgressNotify bool
}

// classifyVersion enforces the hard floor and derives progress trust.
// side is "source" or "target"; the hard floor is enforced on both, the
// progress-trust floor only matters for the source (the watched side).
func classifyVersion(side, version string) (versionInfo, error) {
	v, err := semver.NewVersion(version)
	if err != nil {
		return versionInfo{}, fmt.Errorf("unparseable %s etcd version %q: %w", side, version, err)
	}
	floor := semver.New(hardVersionFloor)
	if v.LessThan(*floor) {
		return versionInfo{}, &UnsupportedVersionError{Side: side, Version: version}
	}
	info := versionInfo{Version: version, TrustProgressNotify: true}
	if trustFloor, ok := progressTrustFloors[v.Minor]; ok && v.Major == 3 {
		info.TrustProgressNotify = !v.LessThan(trustFloor)
	}
	return info, nil
}
