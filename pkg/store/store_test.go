package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestSaveExecutionState(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "workflow.db")

	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	def := workflow.WorkflowDefinition{
		ID:      "demo",
		Version: 1,
		Steps: []workflow.Step{
			{
				ID:   "A",
				Type: "compute",
			},
		},
	}

	if err := s.SaveDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	run := &workflow.WorkflowRun{
		ID:              "test-run-1",
		WorkflowID:      "demo",
		WorkflowVersion: 1,
		Status:          workflow.RunPending,
		StepExecutions: map[string]*workflow.StepExecution{
			"A": {
				StepID: "A",
				Status: workflow.StepPending,
			},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := s.CreateRun(ctx, def, run); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	execution := run.StepExecutions["A"]
	execution.Status = workflow.StepSucceeded
	execution.Output = map[string]any{
		"result": float64(5),
	}
	execution.StartedAt = &now
	execution.FinishedAt = &now

	run.Status = workflow.RunSucceeded
	run.UpdatedAt = now

	if err := s.SaveExecutionState(ctx, run, "A"); err != nil {
		t.Fatal(err)
	}

	// 从数据库重新读取，不能只检查内存中的对象。
	loaded, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	got := loaded.StepExecutions["A"]

	if got.Status != workflow.StepSucceeded {
		t.Fatalf("step status = %q, want %q",
			got.Status, workflow.StepSucceeded)
	}

	if got.Output["result"] != float64(5) {
		t.Fatalf("step output = %#v, want result = 5", got.Output)
	}

	if loaded.Status != workflow.RunSucceeded {
		t.Fatalf("run status = %q, want %q",
			loaded.Status, workflow.RunSucceeded)
	}
}
