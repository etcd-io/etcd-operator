// Package status provides utilities for managing Kubernetes resource status,
// particularly focusing on Conditions and status patching.
package status

import (
	"context"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors" // For GroupResource in conflict error
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DefaultRetry is the default backoff for retrying patch operations.
// Customize this if needed, e.g., by providing a different
// retry.Backoff instance to retry.RetryOnConflict.
var DefaultRetry = retry.DefaultRetry

// PatchStatusMutate applies status changes using a mutate function.
// It fetches the latest version of the object, applies mutations via the mutate func,
// and then patches the status subresource if changes are detected.
// It uses optimistic locking and retries on conflict errors (like "the object has been modified").
//
// Parameters:
//   - ctx: The context for the operation.
//   - c: The controller-runtime client.
//   - originalObj: The original object instance fetched at the start of the reconcile loop.
//     This is used to get the ObjectKey and the Generation at the start of the reconcile.
//     It should not be modified by the reconciliation logic directly; status changes
//     should be calculated and applied via the mutateFn on a fresh copy.
//   - mutateFn: A function that takes the latest fetched object (as type T) and applies
//     the calculated status changes *directly to that object's status field*.
//     This function should return an error if the mutation itself fails for some reason,
//     which will abort the patch attempt.
func PatchStatusMutate[T client.Object](
	ctx context.Context,
	c client.Client,
	originalObj T, // Original object from reconcile start
	mutateFn func(latestFetchedObj T) error, // Mutate function now takes the latest object
) error {
	logger := log.FromContext(ctx).WithValues(
		"objectName", originalObj.GetName(),
		"objectNamespace", originalObj.GetNamespace(),
		"gvk", originalObj.GetObjectKind().GroupVersionKind().String(),
	)
	key := client.ObjectKeyFromObject(originalObj)
	startGeneration := originalObj.GetGeneration() // Generation at the start of reconcile

	return retry.RetryOnConflict(DefaultRetry, func() error {
		// Fetch the latest version of the object in each retry iteration.
		latestFetchedObj := originalObj.DeepCopyObject().(T) // Create a new instance of type T
		getErr := c.Get(ctx, key, latestFetchedObj)
		if getErr != nil {
			if apierrors.IsNotFound(getErr) {
				logger.Info("Object not found while attempting to patch status, skipping patch.")
				return nil // Object deleted, no status to patch.
			}
			logger.Error(getErr, "Failed to get latest object for status patch.")
			// Returning the error from Get will stop retries by default retry settings
			// if it's not a conflict error.
			return fmt.Errorf("failed to get latest object %v for status patch: %w", key, getErr)
		}

		// Check if the generation changed during our reconcile. This might mean the spec changed
		// and our calculated status is stale.
		if startGeneration != latestFetchedObj.GetGeneration() {
			logger.Info("Object generation changed during reconcile, status calculated based on old spec might be stale. "+
				"Aborting this patch attempt.",
				"key", key, "startGeneration", startGeneration, "currentGeneration", latestFetchedObj.GetGeneration())
			// Returning nil here means we acknowledge the generation change and decide *not* to patch stale status.
			// The main Reconcile loop will requeue soon with the new generation.
			// This avoids spamming patch retries for stale data.
			return nil
		}

		// Create a copy *before* mutation to compare against. This is the object state
		// from the API server *before* we apply our calculated changes for this attempt.
		beforeMutateStatusCopy := latestFetchedObj.DeepCopyObject().(T)

		// Apply the calculated status changes by calling the mutate function.
		// The mutate function should modify the 'latestFetchedObj's status field directly.
		if err := mutateFn(latestFetchedObj); err != nil {
			// If the mutation logic itself fails, we likely want to stop retrying the patch.
			logger.Error(err, "Mutation function failed during status patch attempt.")
			// Stop retries by returning non-conflict error
			return fmt.Errorf("mutate function failed during status patch: %w", err)
		}

		// Compare the object's status before and after mutation.
		// We use Semantic.DeepEqual on the whole objects because `Status()` subresource patch
		// still sends a patch based on the whole object typically. More accurately,
		// we should compare just the status fields if we could extract them generically.
		// However, comparing the whole object after mutation (which only touched status)
		// against its state before mutation (but after GET) is correct.
		if apiequality.Semantic.DeepEqual(beforeMutateStatusCopy, latestFetchedObj) {
			logger.V(1).Info("No status change detected after applying mutation, skipping patch.")
			return nil // No actual changes to status, no need to patch
		}

		// Patch the status subresource using the mutated 'latestFetchedObj' object.
		// client.MergeFrom(beforeMutateStatusCopy) generates a JSON merge patch from the diff between the
		// mutated object and its pre-mutation copy. We intentionally avoid StrategicMerge
		// because CRDs (like EtcdCluster) don't support it.
		logger.Info("Status change detected, attempting to patch status subresource.")
		patchErr := c.Status().Patch(ctx, latestFetchedObj, client.MergeFrom(beforeMutateStatusCopy))
		if patchErr != nil {
			// Log the patch error. RetryOnConflict will decide whether to retry based on the error type.
			// Conflict errors will be retried. Other errors might not.
			logger.Info("Failed to patch status, will retry if conflict error.", "error", patchErr.Error())
			return patchErr // Return the error to retry.RetryOnConflict
		}

		logger.Info("Successfully patched status subresource.")
		return nil // Patch successful
	})
}
