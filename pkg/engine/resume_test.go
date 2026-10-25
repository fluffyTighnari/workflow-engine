package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// TestGetRunWithoutStore panics/errors: an engine created with New()
// has no store, so GetRun must report an error.
func TestGetRunWithoutStore(t *testing.T) {
	e := New()
	_, err := e.GetRun(context.Background(), "anything")
	if err == nil {
		t.Fatal("expected error when engine has no store")
	}
}

// TestResumeRunAfterReload simulates a process restart: create a run,
// execute only the first step, then reload the run from the store with
// a fresh engine and continue execution.
func TestResumeRunAfterReload(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "workflow.db")

	engine1, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer engine1.Close()

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

	// CreateRun automatically persists definition and run.
	run, err := engine1.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine1.ExecuteStep(ctx, def, run, "A"); err != nil {
		t.Fatalf("execute A: %v", err)
	}

	if run.StepExecutions["A"].Status != workflow.StepSucceeded {
		t.Fatal("A should have succeeded")
	}
	if run.StepExecutions["B"].Status != workflow.StepPending {
		t.Fatal("B should still be pending before crash")
	}
	if run.StepExecutions["C"].Status != workflow.StepPending {
		t.Fatal("C should still be pending before crash")
	}

	// Simulate process restart: a brand new engine loads the run from the DB.
	engine2, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	loaded, err := engine2.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("reload run: %v", err)
	}

	// Verify the loaded state matches where we left off.
	if loaded.StepExecutions["A"].Status != workflow.StepSucceeded {
		t.Fatalf("loaded A status = %q, want SUCCEEDED",
			loaded.StepExecutions["A"].Status)
	}
	if loaded.StepExecutions["B"].Status != workflow.StepPending {
		t.Fatalf("loaded B status = %q, want PENDING",
			loaded.StepExecutions["B"].Status)
	}
	if loaded.StepExecutions["C"].Status != workflow.StepPending {
		t.Fatalf("loaded C status = %q, want PENDING",
			loaded.StepExecutions["C"].Status)
	}

	// Continue execution from the loaded state.
	if err := engine2.RunWorkflow(ctx, def, loaded); err != nil {
		t.Fatalf("resume RunWorkflow: %v", err)
	}

	if loaded.Status != workflow.RunSucceeded {
		t.Fatalf("run status = %s, want SUCCEEDED", loaded.Status)
	}

	want := map[string]float64{
		"A": 5,
		"B": 10,
		"C": 20,
	}
	for stepID, expected := range want {
		exec := loaded.StepExecutions[stepID]
		if exec.Status != workflow.StepSucceeded {
			t.Errorf("step %s status = %s, want SUCCEEDED",
				stepID, exec.Status)
			continue
		}
		got, ok := exec.Output["result"].(float64)
		if !ok || got != expected {
			t.Errorf("step %s result = %v, want %v",
				stepID, exec.Output["result"], expected)
		}
	}
}
