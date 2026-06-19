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

package controller

import (
	"fmt"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
)

// validateRestoreSpec checks the cross-field invariants the CRD's OpenAPI
// schema cannot express: exactly one snapshot source, a present and consistent
// destination on the explicit-location path, and a target name. Validation runs
// before any I/O so a misconfigured EtcdRestore fails cleanly and terminally.
func validateRestoreSpec(spec *ecv1alpha1.EtcdRestoreSpec) error {
	src := spec.Source
	hasRef := src.BackupRef != nil
	hasLoc := src.Location != nil
	switch {
	case hasRef && hasLoc:
		return fmt.Errorf("spec.source must set exactly one of backupRef or location, not both")
	case !hasRef && !hasLoc:
		return fmt.Errorf("spec.source must set one of backupRef or location")
	}

	if hasRef && src.BackupRef.Name == "" {
		return fmt.Errorf("spec.source.backupRef.name must not be empty")
	}
	if hasLoc {
		if src.Location.Key == "" {
			return fmt.Errorf("spec.source.location.key must not be empty")
		}
		if err := validateDestination(src.Location.Destination); err != nil {
			return fmt.Errorf("spec.source.location.destination invalid: %w", err)
		}
	}

	if spec.Target.Name == "" {
		return fmt.Errorf("spec.target.name must not be empty")
	}
	return nil
}

// snapshotKeyFromBackup derives the object key (relative to the destination
// prefix) of a completed EtcdBackup's snapshot. It re-derives the key the
// backup controller wrote rather than parsing status.snapshotLocation, so the
// key construction stays in one place (snapshotObjectKey) and round-trips
// exactly. A backup that has not completed has no stable key and is rejected.
func snapshotKeyFromBackup(backup *ecv1alpha1.EtcdBackup) (string, error) {
	if backup.Status.SnapshotLocation == "" {
		return "", fmt.Errorf("EtcdBackup %q has no recorded snapshotLocation", backup.Name)
	}
	return snapshotObjectKey(backup), nil
}
