package status

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Manager helps manage conditions in a Kubernetes resource status.
type Manager struct {
	conditions *[]metav1.Condition
	generation int64 // Store the generation
}

// NewManager creates a new condition manager for the given slice of conditions and generation.
// The slice passed must be a pointer, as the manager will modify it directly.
func NewManager(conditions *[]metav1.Condition, generation int64) *Manager {
	if conditions == nil {
		// Initialize if nil to prevent panics.
		emptyConditions := make([]metav1.Condition, 0)
		conditions = &emptyConditions
	}
	return &Manager{conditions: conditions, generation: generation}
}

// set is a private helper to set or update a condition using meta.SetStatusCondition.
// meta.SetStatusCondition handles updating LastTransitionTime only when the status changes.
func (m *Manager) set(conditionType string, status metav1.ConditionStatus, reason, message string) {
	newCondition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: m.generation, // Use stored generation
		LastTransitionTime: metav1.NewTime(time.Now()),
		Reason:             reason,
		Message:            message,
	}
	meta.SetStatusCondition(m.conditions, newCondition)
}

// --- Semantic helpers ---
// These helpers make setting common conditions more readable in the reconciler.

// SetAvailable sets the Available condition status.
func (m *Manager) SetAvailable(status bool, reason, message string) {
	conditionStatus := metav1.ConditionFalse
	if status {
		conditionStatus = metav1.ConditionTrue
	}
	m.set(ConditionAvailable, conditionStatus, reason, message)
}

// SetProgressing sets the Progressing condition status.
func (m *Manager) SetProgressing(status bool, reason, message string) {
	conditionStatus := metav1.ConditionFalse
	if status {
		conditionStatus = metav1.ConditionTrue
	}
	m.set(ConditionProgressing, conditionStatus, reason, message)
}

// SetDegraded sets the Degraded condition status.
func (m *Manager) SetDegraded(status bool, reason, message string) {
	conditionStatus := metav1.ConditionFalse
	if status {
		conditionStatus = metav1.ConditionTrue
	}
	m.set(ConditionDegraded, conditionStatus, reason, message)
}

// Update returns a ConditionUpdater that allows chaining condition changes before applying them.
func (m *Manager) Update() ConditionUpdater {
	return ConditionUpdater{manager: m}
}

// ConditionUpdater batches updates to conditions so callers can configure multiple
// condition states without repeating boilerplate. Call Apply() to persist the changes.
type ConditionUpdater struct {
	manager *Manager
	updates []func()
}

// Available queues an update for the Available condition.
func (u ConditionUpdater) Available(status bool, reason, message string) ConditionUpdater {
	if u.manager == nil {
		return u
	}

	statusCopy := status
	reasonCopy := reason
	messageCopy := message

	u.updates = append(u.updates, func() {
		u.manager.SetAvailable(statusCopy, reasonCopy, messageCopy)
	})
	return u
}

// Progressing queues an update for the Progressing condition.
func (u ConditionUpdater) Progressing(status bool, reason, message string) ConditionUpdater {
	if u.manager == nil {
		return u
	}

	statusCopy := status
	reasonCopy := reason
	messageCopy := message

	u.updates = append(u.updates, func() {
		u.manager.SetProgressing(statusCopy, reasonCopy, messageCopy)
	})
	return u
}

// Degraded queues an update for the Degraded condition.
func (u ConditionUpdater) Degraded(status bool, reason, message string) ConditionUpdater {
	if u.manager == nil {
		return u
	}

	statusCopy := status
	reasonCopy := reason
	messageCopy := message

	u.updates = append(u.updates, func() {
		u.manager.SetDegraded(statusCopy, reasonCopy, messageCopy)
	})
	return u
}

// Apply executes the queued condition updates.
func (u ConditionUpdater) Apply() {
	if u.manager == nil {
		return
	}

	for _, update := range u.updates {
		update()
	}
}

// Helper to format error messages concisely for Conditions
func FormatError(prefix string, err error) string {
	if err == nil {
		return prefix
	}
	return fmt.Sprintf("%s: %v", prefix, err)
}
