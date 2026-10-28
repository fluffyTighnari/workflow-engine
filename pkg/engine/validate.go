package engine

import (
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// validateDefinition checks that the definition is valid, preserving the
// engine.ErrInvalidDefinition sentinel for callers of the engine API.
func validateDefinition(def workflow.WorkflowDefinition) error {
	if err := workflow.ValidateDefinition(def); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDefinition, err)
	}
	return nil
}
