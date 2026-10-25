package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestGetRunNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.GetRun(ctx, "nonexistent-run-id")
	if err == nil {
		t.Fatal("expected error for missing run, got nil")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestGetRunLoadsAllFields(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	def := workflow.WorkflowDefinition{
		ID:      "demo",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "A", Type: "compute", Config: map[string]string{"a": "2", "b": "3"}},
			{
				ID:        "B",
				Type:      "double",
				DependsOn: []string{"A"},
				Inputs: map[string]workflow.InputRef{
					"value": {Source: "A", Key: "result"},
				},
			},
		},
	}

	if err := s.SaveDefinition(ctx, def); err != nil {
		t.Fatalf("save definition: %v", err)
	}

	// Create a run manually (bypass engine).
	run := &workflow.WorkflowRun{
		ID:              "test-run-1",
		WorkflowID:      "demo",
		WorkflowVersion: 1,
		Status:          workflow.RunPending,
		Input:           map[string]any{"origin": "test"},
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
		t.Fatalf("create run in store: %v", err)
	}

	// Simulate partial execution: step A succeeds, step B still pending.
	execA := run.StepExecutions["A"]
	startedAt := time.Now().UTC().Add(-2 * time.Second)
	finishedAt := time.Now().UTC().Add(-1 * time.Second)
	execA.Status = workflow.StepSucceeded
	execA.Output = map[string]any{"result": float64(5)}
	execA.Input = map[string]any{}
	execA.StartedAt = &startedAt
	execA.FinishedAt = &finishedAt
	run.Status = workflow.RunRunning
	run.UpdatedAt = finishedAt

	if err := s.SaveExecutionState(ctx, run, "A"); err != nil {
		t.Fatalf("save step A state: %v", err)
	}

	loaded, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	// Run-level fields.
	if loaded.ID != run.ID {
		t.Errorf("run ID = %q, want %q", loaded.ID, run.ID)
	}
	if loaded.WorkflowID != "demo" || loaded.WorkflowVersion != 1 {
		t.Errorf("workflow ref = %s/%d, want demo/1", loaded.WorkflowID, loaded.WorkflowVersion)
	}
	if loaded.Status != workflow.RunRunning {
		t.Errorf("run status = %q, want RUNNING", loaded.Status)
	}
	if loaded.Input["origin"] != "test" {
		t.Errorf("run input = %v, want origin=test", loaded.Input)
	}

	// Step A fields.
	a := loaded.StepExecutions["A"]
	if a.Status != workflow.StepSucceeded {
		t.Errorf("A status = %q, want SUCCEEDED", a.Status)
	}
	if a.Output["result"] != float64(5) {
		t.Errorf("A output = %v, want result=5", a.Output["result"])
	}
	if a.StartedAt == nil || a.FinishedAt == nil {
		t.Fatal("A started/finished timestamps must be loaded")
	}
	if !a.StartedAt.Equal(startedAt) {
		t.Errorf("A started_at = %v, want %v", a.StartedAt, startedAt)
	}
	if a.Error != "" {
		t.Errorf("A error = %q, want empty", a.Error)
	}

	// Step B should still be pending.
	b := loaded.StepExecutions["B"]
	if b.Status != workflow.StepPending {
		t.Errorf("B status = %q, want PENDING", b.Status)
	}
	if b.Output != nil {
		t.Errorf("B output = %v, want nil", b.Output)
	}
}
