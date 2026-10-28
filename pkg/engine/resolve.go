package engine

import (
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// resolveStepInput resolves each declared input name to a concrete
// value from either the workflow input or a succeeded dependency's output.
func resolveStepInput(
	step workflow.Step,
	run *workflow.WorkflowRun,
) (map[string]any, error) {
	resolved := make(map[string]any, len(step.Inputs))

	for inputName, ref := range step.Inputs {
		var source map[string]any

		if ref.Source == "workflow_input" {
			source = run.Input
		} else {
			dep, ok := run.StepExecutions[ref.Source]
			if !ok || dep == nil {
				return nil, fmt.Errorf(
					"%w: input %q references unknown step %q",
					ErrInputUnknownSource,
					inputName,
					ref.Source,
				)
			}

			if dep.Status != workflow.StepSucceeded {
				return nil, fmt.Errorf(
					"%w: input %q references unfinished step %q",
					ErrInputUnfinishedSource,
					inputName,
					ref.Source,
				)
			}

			source = dep.Output
		}

		value, exists := source[ref.Key]
		if !exists {
			return nil, fmt.Errorf(
				"%w: input %q references missing key %q in %q",
				ErrInputMissingKey,
				inputName,
				ref.Key,
				ref.Source,
			)
		}

		resolved[inputName] = value
	}

	return resolved, nil
}
