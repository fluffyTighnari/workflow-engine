package engine

import (
	"context"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestRunWorkflowAutomaticallyExecutesDependencies(t *testing.T) {
	engine := New()

	def := workflow.WorkflowDefinition{
		ID:      "pipeline",
		Version: 1,
		Steps: []workflow.Step{
			{
				ID:     "A",
				Type:   "compute",
				Config: map[string]string{"a": "2", "b": "3"},
			},
			{
				ID:        "B",
				Type:      "double",
				DependsOn: []string{"A"},
				Inputs: map[string]workflow.InputRef{
					"value": {Source: "A", Key: "result"},
				},
			},
			{
				ID:        "C",
				Type:      "double",
				DependsOn: []string{"B"},
				Inputs: map[string]workflow.InputRef{
					"value": {Source: "B", Key: "result"},
				},
			},
		},
	}

	run, err := engine.CreateRun(context.Background(), def, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.RunWorkflow(context.Background(), def, run); err != nil {
		t.Fatalf("RunWorkflow() error = %v", err)
	}

	if run.Status != workflow.RunSucceeded {
		t.Fatalf("run status = %s, want SUCCEEDED", run.Status)
	}

	tests := []struct {
		stepID string
		want   float64
	}{
		{"A", 5},
		{"B", 10},
		{"C", 20},
	}

	for _, tt := range tests {
		execution := run.StepExecutions[tt.stepID]

		if execution.Status != workflow.StepSucceeded {
			t.Errorf(
				"step %s status = %s, want SUCCEEDED",
				tt.stepID,
				execution.Status,
			)
			continue
		}

		got, ok := execution.Output["result"].(float64)
		if !ok || got != tt.want {
			t.Errorf(
				"step %s result = %v, want %v",
				tt.stepID,
				execution.Output["result"],
				tt.want,
			)
		}
	}
}
