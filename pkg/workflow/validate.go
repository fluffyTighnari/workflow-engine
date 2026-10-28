package workflow

import "fmt"

// ValidateDefinition checks that a workflow definition has a non-empty ID,
// a positive version, at least one step, unique step IDs, valid dependency
// references, and no dependency cycles.
func ValidateDefinition(def WorkflowDefinition) error {
	if def.ID == "" || def.Version <= 0 || len(def.Steps) == 0 {
		return fmt.Errorf(
			"ID, positive version, and at least one step are required",
		)
	}

	stepIDs := make(map[string]struct{}, len(def.Steps))

	for _, step := range def.Steps {
		if step.ID == "" || step.Type == "" {
			return fmt.Errorf("step ID and type are required")
		}

		if _, exists := stepIDs[step.ID]; exists {
			return fmt.Errorf("duplicate step ID %q", step.ID)
		}

		stepIDs[step.ID] = struct{}{}
	}

	for _, step := range def.Steps {
		for _, depID := range step.DependsOn {
			if depID == step.ID {
				return fmt.Errorf(
					"step %q cannot depend on itself",
					step.ID,
				)
			}

			if _, exists := stepIDs[depID]; !exists {
				return fmt.Errorf(
					"step %q depends on unknown step %q",
					step.ID,
					depID,
				)
			}
		}
	}

	if hasCycle(def.Steps) {
		return fmt.Errorf("dependency cycle detected")
	}

	return nil
}

// hasCycle detects cycles in the directed dependency graph using
// DFS three-color marking. An edge points from a step to each step
// it depends on.
func hasCycle(steps []Step) bool {
	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)

	status := make(map[string]int, len(steps))
	stepByID := make(map[string]Step, len(steps))

	for _, step := range steps {
		stepByID[step.ID] = step
	}

	var visit func(string) bool
	visit = func(stepID string) bool {
		switch status[stepID] {
		case visiting:
			return true
		case visited:
			return false
		}

		status[stepID] = visiting

		for _, depID := range stepByID[stepID].DependsOn {
			if visit(depID) {
				return true
			}
		}

		status[stepID] = visited
		return false
	}

	for _, step := range steps {
		if status[step.ID] == unvisited && visit(step.ID) {
			return true
		}
	}

	return false
}
