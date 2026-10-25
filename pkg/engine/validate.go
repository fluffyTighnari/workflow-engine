package engine

import (
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// validateDefinition checks that the definition has a non-empty ID,
// a positive version, at least one step, unique step IDs, valid
// dependency references, and no dependency cycles.
func validateDefinition(def workflow.WorkflowDefinition) error {
	if def.ID == "" || def.Version <= 0 || len(def.Steps) == 0 {
		return fmt.Errorf(
			"%w: ID, positive version, and at least one step are required",
			ErrInvalidDefinition,
		)
	}

	stepIDs := make(map[string]struct{}, len(def.Steps))

	for _, step := range def.Steps {
		if step.ID == "" || step.Type == "" {
			return fmt.Errorf(
				"%w: step ID and type are required",
				ErrInvalidDefinition,
			)
		}

		if _, exists := stepIDs[step.ID]; exists {
			return fmt.Errorf(
				"%w: duplicate step ID %q",
				ErrInvalidDefinition,
				step.ID,
			)
		}

		stepIDs[step.ID] = struct{}{}
	}

	for _, step := range def.Steps {
		for _, depID := range step.DependsOn {
			if depID == step.ID {
				return fmt.Errorf(
					"%w: step %q cannot depend on itself",
					ErrInvalidDefinition,
					step.ID,
				)
			}

			if _, exists := stepIDs[depID]; !exists {
				return fmt.Errorf(
					"%w: step %q depends on unknown step %q",
					ErrInvalidDefinition,
					step.ID,
					depID,
				)
			}
		}
	}

	if hasCycle(def.Steps) {
		return fmt.Errorf("%w: dependency cycle detected", ErrInvalidDefinition)
	}

	return nil
}

// hasCycle detects cycles in the directed dependency graph using
// DFS three-color marking. An edge points from a step to each step
// it depends on.
func hasCycle(steps []workflow.Step) bool {
	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)

	status := make(map[string]int, len(steps))
	stepByID := make(map[string]workflow.Step, len(steps))

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
