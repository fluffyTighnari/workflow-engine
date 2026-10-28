package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// pipelineDefinition is the shared fixture for most engine tests:
// A (compute 2+3=5) -> B (double 5*2=10) -> C (double 10*2=20).
func pipelineDefinition() workflow.WorkflowDefinition {
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
}

// newSQLiteTestEngine returns a SQLite-backed engine closed via t.Cleanup.
func newSQLiteTestEngine(t *testing.T) *Engine {
	t.Helper()

	path := filepath.Join(t.TempDir(), "workflow.db")
	e, err := NewSQLite(path)
	if err != nil {
		t.Fatalf("new sqlite engine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })

	return e
}

// newMemoryTestEngine returns a memory-backed engine closed via t.Cleanup.
func newMemoryTestEngine(t *testing.T) *Engine {
	t.Helper()

	e := NewMemory()
	t.Cleanup(func() { _ = e.Close() })

	return e
}

// createRunForPipeline creates a run for pipelineDefinition.
func createRunForPipeline(
	t *testing.T,
	e *Engine,
) (*workflow.WorkflowRun, workflow.WorkflowDefinition) {
	t.Helper()

	def := pipelineDefinition()
	run, err := e.CreateRun(context.Background(), def, nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	return run, def
}

// assertStepResults verifies that each step succeeded with the expected
// numeric "result" output.
func assertStepResults(
	t *testing.T,
	run *workflow.WorkflowRun,
	want map[string]float64,
) {
	t.Helper()

	for stepID, expected := range want {
		execution, ok := run.StepExecutions[stepID]
		if !ok {
			t.Errorf("missing step execution %q", stepID)
			continue
		}
		if execution.Status != workflow.StepSucceeded {
			t.Errorf("step %s status = %s, want SUCCEEDED",
				stepID, execution.Status)
			continue
		}

		got, ok := execution.Output["result"].(float64)
		if !ok || got != expected {
			t.Errorf("step %s result = %v, want %v",
				stepID, execution.Output["result"], expected)
		}
	}
}
