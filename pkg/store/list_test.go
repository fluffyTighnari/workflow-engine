package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// seedRunForList 保存一个定义并创建一个指定状态的 run。
func seedRunForList(t *testing.T, ctx context.Context, s WorkflowStore, runID, status string) {
	t.Helper()

	def := workflow.WorkflowDefinition{
		ID:      "list-demo",
		Version: 1,
		Steps:   []workflow.Step{{ID: "A", Type: "compute"}},
	}
	if err := s.SaveDefinition(ctx, def); err != nil {
		t.Fatalf("save definition: %v", err)
	}

	run := &workflow.WorkflowRun{
		ID:              runID,
		WorkflowID:      "list-demo",
		WorkflowVersion: 1,
		Status:          workflow.RunStatus(status),
		StepExecutions: map[string]*workflow.StepExecution{
			"A": {StepID: "A", Status: workflow.StepPending},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := s.CreateRun(ctx, def, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
}

func TestListRunsByStatusSQLite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "workflow.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	seedRunForList(t, ctx, s, "run-pending", string(workflow.RunPending))
	seedRunForList(t, ctx, s, "run-running", string(workflow.RunRunning))
	seedRunForList(t, ctx, s, "run-done", string(workflow.RunSucceeded))

	ids, err := s.ListRunsByStatus(ctx, workflow.RunPending, workflow.RunRunning)
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if !got["run-pending"] || !got["run-running"] {
		t.Errorf("ids = %v, want run-pending and run-running", ids)
	}
	if got["run-done"] {
		t.Errorf("ids = %v, must not include run-done", ids)
	}
}

func TestListRunsByStatusMemory(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	defer s.Close()

	seedRunForList(t, ctx, s, "run-pending", string(workflow.RunPending))
	seedRunForList(t, ctx, s, "run-running", string(workflow.RunRunning))
	seedRunForList(t, ctx, s, "run-done", string(workflow.RunSucceeded))

	ids, err := s.ListRunsByStatus(ctx, workflow.RunPending, workflow.RunRunning)
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if !got["run-pending"] || !got["run-running"] {
		t.Errorf("ids = %v, want run-pending and run-running", ids)
	}
	if got["run-done"] {
		t.Errorf("ids = %v, must not include run-done", ids)
	}
}

func TestListRunsByStatusEmpty(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	defer s.Close()

	ids, err := s.ListRunsByStatus(ctx, workflow.RunPending)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("ids = %v, want empty", ids)
	}
}
