package engine

import (
	"context"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestExecuteStepRespectsDependencies(t *testing.T) {
	engine := New()

	def := workflow.WorkflowDefinition{
		ID:      "demo",
		Version: 1,
		Steps: []workflow.Step{
			{
				ID:     "A",
				Type:   "compute",
				Config: map[string]string{"a": "2", "b": "3"},
			},
			{
				ID:        "B",
				Type:      "compute",
				Config:    map[string]string{"a": "10", "b": "20"},
				DependsOn: []string{"A"},
			},
		},
	}

	run, err := engine.CreateRun(context.Background(), def, nil)
	if err != nil {
		t.Fatal(err)
	}

	// B must not run before A succeeds.
	if err := engine.ExecuteStep(context.Background(), def, run, "B"); err == nil {
		t.Fatal("expected dependency error")
	}

	if err := engine.ExecuteStep(context.Background(), def, run, "A"); err != nil {
		t.Fatalf("execute A: %v", err)
	}

	if run.StepExecutions["A"].Status != workflow.StepSucceeded {
		t.Fatal("A should have succeeded")
	}

	if got := run.StepExecutions["A"].Output["result"]; got != float64(5) {
		t.Fatalf("A result = %v, want 5", got)
	}

	if err := engine.ExecuteStep(context.Background(), def, run, "B"); err != nil {
		t.Fatalf("execute B: %v", err)
	}

	if got := run.StepExecutions["B"].Output["result"]; got != float64(30) {
		t.Fatalf("B result = %v, want 30", got)
	}

	if run.Status != workflow.RunSucceeded {
		t.Fatalf("run status = %s, want SUCCEEDED", run.Status)
	}
}
