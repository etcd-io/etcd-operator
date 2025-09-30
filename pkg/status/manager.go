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

// RemoveCondition removes a condition of the given type.
func (m *Manager) RemoveCondition(conditionType string) {
	meta.RemoveStatusCondition(m.conditions, conditionType)
}

// FindCondition retrieves a condition by type. Returns nil if not found.
func (m *Manager) FindCondition(conditionType string) *metav1.Condition {
	return meta.FindStatusCondition(*m.conditions, conditionType)
}

// IsConditionTrue checks if a condition of the given type is present and has Status=True.
func (m *Manager) IsConditionTrue(conditionType string) bool {
	return meta.IsStatusConditionTrue(*m.conditions, conditionType)
}

// IsConditionFalse checks if a condition of the given type is present and has Status=False.
func (m *Manager) IsConditionFalse(conditionType string) bool {
	return meta.IsStatusConditionFalse(*m.conditions, conditionType)
}

// IsConditionUnknown checks if a condition of the given type is present and has Status=Unknown, or if it's absent.
func (m *Manager) IsConditionUnknown(conditionType string) bool {
	cond := m.FindCondition(conditionType)
	if cond == nil {
		return true // Absent often implies Unknown for readiness/availability
	}
	return cond.Status == metav1.ConditionUnknown
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

// --- Optional more specific semantic helpers ---

// SetScaling sets the Scaling condition status.
// func (m *Manager) SetScaling(status bool, reason, message string) {
// 	conditionStatus := metav1.ConditionFalse
// 	if status {
// 		conditionStatus = metav1.ConditionTrue
// 		m.SetProgressing(true, reason, message) // Scaling implies Progressing
// 	}
// 	m.set(ConditionScaling, conditionStatus, reason, message)
// }

// SetPromoting sets the Promoting condition status.
// func (m *Manager) SetPromoting(status bool, reason, message string) {
// 	conditionStatus := metav1.ConditionFalse
// 	if status {
// 		conditionStatus = metav1.ConditionTrue
// 		m.SetProgressing(true, reason, message) // Promoting implies Progressing
// 	}
// 	m.set(ConditionPromoting, conditionStatus, reason, message)
// }

// SetInitializing sets the Initializing condition status.
// func (m *Manager) SetInitializing(status bool, reason, message string) {
// 	conditionStatus := metav1.ConditionFalse
// 	if status {
// 		conditionStatus = metav1.ConditionTrue
// 		m.SetProgressing(true, reason, message) // Initializing implies Progressing
// 	}
// 	m.set(ConditionInitializing, conditionStatus, reason, message)
// }

// Helper to format error messages concisely for Conditions
func FormatError(prefix string, err error) string {
	if err == nil {
		return prefix
	}
	return fmt.Sprintf("%s: %v", prefix, err)
}
