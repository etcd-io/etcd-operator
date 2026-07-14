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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/objectstore"
)

// resolveStoreCredentials reads object-store credentials for a destination from
// its referenced Secret, if any, and returns them for the objectstore factory.
//
// SECURITY: object-store credentials originate *only* from the destination's
// secretRef (or the operator's ambient identity when secretRef is unset). This
// function is the single seam that reads that secret, and it is written so the
// credential *values* are never logged or returned in an error:
//
//   - It emits a structured audit log line recording the namespace, provider,
//     and the secret *name* — never any secret key value.
//   - On a missing/unreadable secret it wraps only the secret name and the API
//     error (which never contains the secret's data), never the bytes it failed
//     to read.
//   - The returned objectstore.Credentials is passed straight to the provider
//     factory and is never rendered into status, conditions, or events.
//
// Tests assert this guarantee directly (see the cred-never-logged test).
func resolveStoreCredentials(
	ctx context.Context,
	c client.Client,
	logger logr.Logger,
	namespace string,
	dst ecv1alpha1.BackupDestination,
) (objectstore.Credentials, error) {
	var creds objectstore.Credentials

	if dst.SecretRef == nil {
		// Ambient-credential path (IRSA / Workload Identity). Record that no
		// secret was used so an auditor can see the operator's own identity is
		// in play, with nothing sensitive to redact.
		logger.Info("objectstore credentials: using ambient operator identity (no secretRef)",
			"namespace", namespace, "provider", string(dst.Provider))
		return creds, nil
	}

	// Audit the *reference* before reading it. Only the secret name is logged;
	// the values pulled below are never logged anywhere.
	logger.Info("objectstore credentials: loading from secretRef",
		"namespace", namespace, "provider", string(dst.Provider), "secretName", dst.SecretRef.Name)

	var secret corev1.Secret
	key := types.NamespacedName{Namespace: namespace, Name: dst.SecretRef.Name}
	if err := c.Get(ctx, key, &secret); err != nil {
		return creds, fmt.Errorf("get credentials secret %q: %w", dst.SecretRef.Name, err)
	}

	switch dst.Provider {
	case ecv1alpha1.BackupProviderS3:
		creds.AccessKeyID = string(secret.Data["accessKeyID"])
		creds.SecretAccessKey = string(secret.Data["secretAccessKey"])
		creds.SessionToken = string(secret.Data["sessionToken"])
		if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
			return creds, fmt.Errorf(
				"secret %q is missing required s3 keys accessKeyID/secretAccessKey", dst.SecretRef.Name)
		}
	case ecv1alpha1.BackupProviderGCS:
		creds.ServiceAccountJSON = secret.Data["serviceAccountJSON"]
		if len(creds.ServiceAccountJSON) == 0 {
			return creds, fmt.Errorf(
				"secret %q is missing required gcs key serviceAccountJSON", dst.SecretRef.Name)
		}
	default:
		return creds, fmt.Errorf("unsupported provider %q for secret-based credentials", dst.Provider)
	}
	return creds, nil
}

// validateDestination performs structural validation of a BackupDestination
// shared by the backup and restore paths: the provider-specific block must be
// present and consistent, and a referenced secret name must be non-empty. It is
// the controller-side complement to the API-level CRD validation, catching the
// cross-field invariants OpenAPI cannot express.
func validateDestination(dst ecv1alpha1.BackupDestination) error {
	if _, err := toObjectStoreDestination(dst); err != nil {
		return err
	}
	if dst.SecretRef != nil && dst.SecretRef.Name == "" {
		return fmt.Errorf("destination.secretRef.name must not be empty when secretRef is set")
	}
	return nil
}
