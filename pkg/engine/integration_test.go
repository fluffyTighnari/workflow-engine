package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/store"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func testWorkflowDefinition() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
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
		},
	}
}

// runWorkflowAgainstStore runs a complete workflow against the given store
// and verifies the expected results. This test runs against both SQLiteStore
// and MemoryStore to ensure both implementations behave identically.
func runWorkflowAgainstStore(t *testing.T, s store.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	def := testWorkflowDefinition()

	eng := NewWithStore(s)
	run, err := eng.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := eng.RunWorkflow(ctx, def, run); err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}

	if run.Status != workflow.RunSucceeded {
		t.Fatalf("run status = %s, want SUCCEEDED", run.Status)
	}

	expected := map[string]float64{"A": 5, "B": 10}
	for stepID, want := range expected {
		got := run.StepExecutions[stepID].Output["result"].(float64)
		if got != want {
			t.Errorf("step %s result = %v, want %v", stepID, got, want)
		}
	}
}

// TestMemoryStoreRunWorkflow runs the same workflow against MemoryStore.
func TestMemoryStoreRunWorkflow(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()
	runWorkflowAgainstStore(t, s)
}

// TestSQLiteStoreRunWorkflow runs the same workflow against SQLiteStore.
func TestSQLiteStoreRunWorkflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	runWorkflowAgainstStore(t, s)
}

// TestMemoryStoreGetRunNotFound verifies error on missing run.
func TestMemoryStoreGetRunNotFound(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()

	_, err := s.GetRun(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for missing run")
	}
}

// TestMemoryStoreGetDefinitionNotFound verifies error on missing definition.
func TestMemoryStoreGetDefinitionNotFound(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()

	_, err := s.GetDefinition(context.Background(), "missing", 1)
	if err == nil {
		t.Fatal("expected error for missing definition")
	}
}
