package engine

import (
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestResolveStepInput(t *testing.T) {
	run := &workflow.WorkflowRun{
		Input: map[string]any{
			"initial": float64(7),
		},
		StepExecutions: map[string]*workflow.StepExecution{
			"A": {
				StepID: "A",
				Status: workflow.StepSucceeded,
				Output: map[string]any{
					"result": float64(12),
				},
			},
		},
	}

	step := workflow.Step{
		ID: "B",
		Inputs: map[string]workflow.InputRef{
			"left": {
				Source: "workflow_input",
				Key:    "initial",
			},
			"right": {
				Source: "A",
				Key:    "result",
			},
		},
	}

	got, err := resolveStepInput(step, run)
	if err != nil {
		t.Fatal(err)
	}

	if got["left"] != float64(7) {
		t.Errorf("left = %v, want 7", got["left"])
	}

	if got["right"] != float64(12) {
		t.Errorf("right = %v, want 12", got["right"])
	}
}
