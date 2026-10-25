package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestRunSurvivesDatabaseReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "workflow.db")

	def := workflow.WorkflowDefinition{
		ID:      "demo",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "A", Type: "compute"},
			{ID: "B", Type: "double", DependsOn: []string{"A"}},
		},
	}

	// Create and save a run.
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.SaveDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	run := &workflow.WorkflowRun{
		ID:              "test-run-1",
		WorkflowID:      "demo",
		WorkflowVersion: 1,
		Status:          workflow.RunPending,
		Input:           map[string]any{"value": 42},
		StepExecutions: map[string]*workflow.StepExecution{
			"A": {
				StepID: "A",
				Status: workflow.StepPending,
			},
			"B": {
				StepID: "B",
				Status: workflow.StepPending,
			},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := s.CreateRun(ctx, def, run); err != nil {
		t.Fatal(err)
	}

	runID := run.ID

	// Close and reopen the database.
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	loadedDef, err := s2.GetDefinition(ctx, "demo", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(loadedDef.Steps) != 2 {
		t.Fatalf("loaded %d steps, want 2", len(loadedDef.Steps))
	}

	loadedRun, err := s2.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	if loadedRun.ID != runID {
		t.Fatalf("run ID = %q, want %q", loadedRun.ID, runID)
	}

	if loadedRun.Input["value"] != float64(42) {
		t.Fatalf("input value = %v, want 42", loadedRun.Input["value"])
	}

	if len(loadedRun.StepExecutions) != 2 {
		t.Fatalf("loaded %d step executions, want 2",
			len(loadedRun.StepExecutions))
	}

	if loadedRun.StepExecutions["A"].Status != workflow.StepPending {
		t.Fatal("step A should remain pending")
	}
}
