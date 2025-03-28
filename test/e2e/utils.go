package e2e

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

func getContainerByName(containers []corev1.Container, name string) (corev1.Container, error) {
	var container corev1.Container
	for c := range containers {
		if containers[c].Name == name {
			container = containers[c]
			return container, nil
		}
	}
	return container, fmt.Errorf("container named %s not found", name)
}
