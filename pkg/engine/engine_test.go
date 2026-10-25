package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestCreateRun(t *testing.T) {
	ctx := context.Background()
	def := workflow.WorkflowDefinition{
		ID:      "demo",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "A", Type: "compute"},
			{ID: "B", Type: "transform", DependsOn: []string{"A"}},
		},
	}

	input := map[string]any{"value": 10}

	run, err := New().CreateRun(ctx, def, input)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	if run.ID == "" {
		t.Fatal("run ID must not be empty")
	}
	if run.Status != workflow.RunPending {
		t.Fatalf("run status = %q, want PENDING", run.Status)
	}
	if run.WorkflowID != def.ID || run.WorkflowVersion != def.Version {
		t.Fatal("run must reference the supplied workflow version")
	}
	if len(run.StepExecutions) != 2 {
		t.Fatalf("got %d step executions, want 2", len(run.StepExecutions))
	}

	for _, stepID := range []string{"A", "B"} {
		exec, ok := run.StepExecutions[stepID]
		if !ok {
			t.Fatalf("missing execution for step %q", stepID)
		}
		if exec.Status != workflow.StepPending {
			t.Errorf("step %q status = %q, want PENDING", stepID, exec.Status)
		}
	}

	if run.CreatedAt.IsZero() || run.UpdatedAt.IsZero() {
		t.Fatal("run timestamps must be initialized")
	}
}

func TestCreateRunRejectsInvalidDefinitions(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		def  workflow.WorkflowDefinition
	}{
		{
			name: "empty ID",
			def: workflow.WorkflowDefinition{
				Version: 1,
				Steps:   []workflow.Step{{ID: "A", Type: "compute"}},
			},
		},
		{
			name: "no steps",
			def: workflow.WorkflowDefinition{
				ID:      "demo",
				Version: 1,
			},
		},
		{
			name: "duplicate step ID",
			def: workflow.WorkflowDefinition{
				ID:      "demo",
				Version: 1,
				Steps: []workflow.Step{
					{ID: "A", Type: "compute"},
					{ID: "A", Type: "transform"},
				},
			},
		},
		{
			name: "unknown dependency",
			def: workflow.WorkflowDefinition{
				ID:      "demo",
				Version: 1,
				Steps: []workflow.Step{
					{ID: "A", Type: "compute", DependsOn: []string{"MISSING"}},
				},
			},
		},
		{
			name: "dependency cycle",
			def: workflow.WorkflowDefinition{
				ID:      "demo",
				Version: 1,
				Steps: []workflow.Step{
					{ID: "A", Type: "compute", DependsOn: []string{"B"}},
					{ID: "B", Type: "compute", DependsOn: []string{"A"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New().CreateRun(ctx, tt.def, nil)
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("error = %v, want ErrInvalidDefinition", err)
			}
		})
	}
}

func TestCreateRunReturnsIndependentInstances(t *testing.T) {
	ctx := context.Background()
	def := workflow.WorkflowDefinition{
		ID:      "demo",
		Version: 1,
		Steps:   []workflow.Step{{ID: "A", Type: "compute"}},
	}

	engine := New()
	run1, err := engine.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}
	run2, err := engine.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	if run1.ID == run2.ID {
		t.Fatal("each run must have a unique ID")
	}

	run1.StepExecutions["A"].Status = workflow.StepSucceeded
	if run2.StepExecutions["A"].Status != workflow.StepPending {
		t.Fatal("different runs must not share step execution state")
	}
}
