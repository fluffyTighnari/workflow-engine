package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/store"
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

	run, err := newMemoryTestEngine(t).CreateRun(ctx, def, input)
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

	e := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := e.CreateRun(ctx, tt.def, nil)
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

	e := newMemoryTestEngine(t)
	run1, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}
	run2, err := e.CreateRun(ctx, def, nil)
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

func TestSaveDefinitionValidatesAndIsInsertOnly(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	def := pipelineDefinition()

	if err := e.SaveDefinition(ctx, def); err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if err := e.SaveDefinition(ctx, def); !errors.Is(err, store.ErrDefinitionExists) {
		t.Fatalf("duplicate save error = %v, want ErrDefinitionExists", err)
	}

	invalid := workflow.WorkflowDefinition{ID: "invalid", Version: 1}
	if err := e.SaveDefinition(ctx, invalid); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("invalid save error = %v, want ErrInvalidDefinition", err)
	}
}

func TestCreateRunReusesImmutableDefinition(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	def := pipelineDefinition()

	run1, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	run2, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatalf("second run with identical definition: %v", err)
	}
	if run1.ID == run2.ID {
		t.Fatal("runs must have distinct IDs")
	}

	// Nil and empty collections are equivalent in the persisted JSON.
	equivalent := pipelineDefinition()
	equivalent.Steps[0].DependsOn = []string{}
	if _, err := e.CreateRun(ctx, equivalent, nil); err != nil {
		t.Fatalf("equivalent definition should be reusable: %v", err)
	}
}

func TestCreateRunRejectsDifferentDefinitionForSameVersion(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	def := pipelineDefinition()

	if _, err := e.CreateRun(ctx, def, nil); err != nil {
		t.Fatal(err)
	}

	changed := pipelineDefinition()
	changed.Steps[0].Config["a"] = "99"

	if _, err := e.CreateRun(ctx, changed, nil); !errors.Is(err, store.ErrDefinitionExists) {
		t.Fatalf("error = %v, want ErrDefinitionExists", err)
	}
}
