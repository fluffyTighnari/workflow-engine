package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/store"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestExecuteStepRespectsDependencies(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	run, def := createRunForPipeline(t, e)

	// B must not run before A succeeds.
	if err := e.ExecuteStep(ctx, def, run, "B"); err == nil {
		t.Fatal("expected dependency error")
	}

	if err := e.ExecuteStep(ctx, def, run, "A"); err != nil {
		t.Fatalf("execute A: %v", err)
	}

	if run.StepExecutions["A"].Status != workflow.StepSucceeded {
		t.Fatal("A should have succeeded")
	}

	if got := run.StepExecutions["A"].Output["result"]; got != float64(5) {
		t.Fatalf("A result = %v, want 5", got)
	}

	if err := e.ExecuteStep(ctx, def, run, "B"); err != nil {
		t.Fatalf("execute B: %v", err)
	}

	if got := run.StepExecutions["B"].Output["result"]; got != float64(10) {
		t.Fatalf("B result = %v, want 10", got)
	}

	// C is still pending, so the run is not terminal yet.
	if run.Status == workflow.RunSucceeded {
		t.Fatal("run must not succeed while C is still pending")
	}

	if err := e.ExecuteStep(ctx, def, run, "C"); err != nil {
		t.Fatalf("execute C: %v", err)
	}

	if run.Status != workflow.RunSucceeded {
		t.Fatalf("run status = %s, want SUCCEEDED", run.Status)
	}
}

func TestRunWorkflowAutomaticallyExecutesDependencies(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	run, def := createRunForPipeline(t, e)

	if err := e.RunWorkflow(ctx, def, run); err != nil {
		t.Fatalf("RunWorkflow() error = %v", err)
	}

	if run.Status != workflow.RunSucceeded {
		t.Fatalf("run status = %s, want SUCCEEDED", run.Status)
	}

	assertStepResults(t, run, map[string]float64{
		"A": 5,
		"B": 10,
		"C": 20,
	})
}

// TestRunWorkflowAgainstStore runs the same workflow against every built-in
// store so backend parity is verified through the engine.
func TestRunWorkflowAgainstStore(t *testing.T) {
	cases := []struct {
		name string
		open func(t *testing.T) store.WorkflowStore
	}{
		{
			name: "memory",
			open: func(t *testing.T) store.WorkflowStore {
				return store.NewMemoryStore()
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T) store.WorkflowStore {
				s, err := store.Open(filepath.Join(t.TempDir(), "workflow.db"))
				if err != nil {
					t.Fatalf("open sqlite store: %v", err)
				}
				return s
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := tc.open(t)
			t.Cleanup(func() { _ = s.Close() })

			e := NewWithStore(s)
			run, def := createRunForPipeline(t, e)

			if err := e.RunWorkflow(ctx, def, run); err != nil {
				t.Fatalf("RunWorkflow: %v", err)
			}
			if run.Status != workflow.RunSucceeded {
				t.Fatalf("run status = %s, want SUCCEEDED", run.Status)
			}

			assertStepResults(t, run, map[string]float64{
				"A": 5,
				"B": 10,
				"C": 20,
			})
		})
	}
}

// failingSaveStore delegates to an in-memory store but fails the Nth
// SaveExecutionState call, simulating a persistence error.
type failingSaveStore struct {
	store.WorkflowStore
	failAt int
	calls  int
}

func (s *failingSaveStore) SaveExecutionState(
	ctx context.Context,
	run *workflow.WorkflowRun,
	stepID string,
) error {
	s.calls++
	if s.calls == s.failAt {
		return errors.New("injected persist failure")
	}
	return s.WorkflowStore.SaveExecutionState(ctx, run, stepID)
}

func TestExecuteStepRollsBackFinalSuccessOnPersistFailure(t *testing.T) {
	ctx := context.Background()
	backend := store.NewMemoryStore()
	s := &failingSaveStore{WorkflowStore: backend, failAt: 2}
	e := NewWithStore(s)
	run, def := createRunForPipeline(t, e)

	err := e.ExecuteStep(ctx, def, run, "A")
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("error = %v, want ErrPersistence", err)
	}

	// The final result was never accepted by the store, so memory must
	// still describe a running step with no output.
	execution := run.StepExecutions["A"]
	if execution.Status != workflow.StepRunning {
		t.Errorf("step status = %s, want RUNNING", execution.Status)
	}
	if execution.Output != nil {
		t.Errorf("step output = %v, want nil", execution.Output)
	}
	if execution.Error != "" {
		t.Errorf("step error = %q, want empty", execution.Error)
	}
	if run.Status != workflow.RunRunning {
		t.Errorf("run status = %s, want RUNNING", run.Status)
	}

	persisted, err := backend.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("load persisted run: %v", err)
	}
	if persisted.StepExecutions["A"].Status != workflow.StepRunning {
		t.Errorf("persisted step status = %s, want RUNNING",
			persisted.StepExecutions["A"].Status)
	}
}

// errorExecutor always fails, used to exercise the failure path.
type errorExecutor struct{}

func (errorExecutor) Execute(
	context.Context,
	workflow.Step,
	map[string]any,
) (map[string]any, error) {
	return nil, errors.New("executor failed")
}

func TestExecuteStepRollsBackFinalFailureOnPersistFailure(t *testing.T) {
	ctx := context.Background()
	backend := store.NewMemoryStore()
	s := &failingSaveStore{WorkflowStore: backend, failAt: 2}
	e := NewWithStore(s)
	if err := e.RegisterExecutor("fail", errorExecutor{}); err != nil {
		t.Fatal(err)
	}

	def := workflow.WorkflowDefinition{
		ID:      "failing",
		Version: 1,
		Steps:   []workflow.Step{{ID: "A", Type: "fail"}},
	}
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = e.ExecuteStep(ctx, def, run, "A")
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("error = %v, want ErrPersistence", err)
	}

	execution := run.StepExecutions["A"]
	if execution.Status != workflow.StepRunning {
		t.Errorf("step status = %s, want RUNNING", execution.Status)
	}
	if execution.Error != "" {
		t.Errorf("step error = %q, want empty", execution.Error)
	}
	if run.Status != workflow.RunRunning {
		t.Errorf("run status = %s, want RUNNING", run.Status)
	}
}

func TestExecuteStepRollsBackResolveFailureOnPersistFailure(t *testing.T) {
	ctx := context.Background()
	backend := store.NewMemoryStore()
	s := &failingSaveStore{WorkflowStore: backend, failAt: 1}
	e := NewWithStore(s)

	def := workflow.WorkflowDefinition{
		ID:      "resolve-failure",
		Version: 1,
		Steps: []workflow.Step{
			{
				ID:   "A",
				Type: "compute",
				Inputs: map[string]workflow.InputRef{
					"value": {Source: "workflow_input", Key: "missing"},
				},
			},
		},
	}
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}
	previousUpdatedAt := run.UpdatedAt

	err = e.ExecuteStep(ctx, def, run, "A")
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("error = %v, want ErrPersistence", err)
	}

	execution := run.StepExecutions["A"]
	if execution.Status != workflow.StepPending {
		t.Errorf("step status = %s, want PENDING", execution.Status)
	}
	if execution.Error != "" {
		t.Errorf("step error = %q, want empty", execution.Error)
	}
	if execution.FinishedAt != nil {
		t.Errorf("step finished_at = %v, want nil", execution.FinishedAt)
	}
	if run.Status != workflow.RunPending {
		t.Errorf("run status = %s, want PENDING", run.Status)
	}
	if !run.UpdatedAt.Equal(previousUpdatedAt) {
		t.Errorf("run updated_at = %v, want %v", run.UpdatedAt, previousUpdatedAt)
	}
}
