// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import etcdversion "go.etcd.io/etcd/api/v3/version"

// testEtcdVersion is the etcd version used by unit tests that need a valid
// version but do not depend on its value. It follows the etcd API module
// version in go.mod, the same source the Makefile uses for E2E_ETCD_VERSION,
// so it moves forward with dependency bumps instead of being edited by hand.
//
// Tests that exercise version semantics (upgrade paths, downgrades, version
// skew) keep their own explicit versions.
var testEtcdVersion = etcdversion.Version
