package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func newRecoverTestEngine(t *testing.T) (*Engine, workflow.WorkflowDefinition) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "workflow.db")
	e, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })

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

	return e, def
}

func TestRecoverRunResetsRunningSteps(t *testing.T) {
	ctx := context.Background()
	e, def := newRecoverTestEngine(t)

	// Create run and execute A.
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.ExecuteStep(ctx, def, run, "A"); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-step-B: mark B as RUNNING with stale output
	// that was never persisted as a successful result.
	run.StepExecutions["B"].Status = workflow.StepRunning
	startedAt := time.Now().UTC().Add(-30 * time.Second)
	run.StepExecutions["B"].StartedAt = &startedAt
	run.StepExecutions["B"].Output = map[string]any{"result": float64(999)} // bogus, never persisted as success
	run.Status = workflow.RunRunning

	// Persist this crashed state directly (as if the DB had RUNNING B).
	if err := e.store.SaveExecutionState(ctx, run, "B"); err != nil {
		t.Fatal(err)
	}

	// Now recover from a fresh engine instance.
	e2, err := NewSQLite(filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	// We need to load the run from the same DB, so open it again.
	// Actually, the DB path is in the temp dir, let's use the same path.
	// Hmm, we need to pass the path. Let's refactor.
	// For simplicity, just use the same engine's store.
	e2 = NewWithStore(e.store)
	recovered, err := e2.RecoverRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("RecoverRun: %v", err)
	}

	// B should have been reset and re-executed (5 * 2 = 10, not 999).
	if recovered.Status != workflow.RunSucceeded {
		t.Fatalf("run status = %s, want SUCCEEDED", recovered.Status)
	}

	if got := recovered.StepExecutions["B"].Output["result"]; got != float64(10) {
		t.Errorf("B result = %v, want 10 (the stale 999 must be discarded)", got)
	}
	if got := recovered.StepExecutions["C"].Output["result"]; got != float64(20) {
		t.Errorf("C result = %v, want 20", got)
	}

	// B should now be SUCCEEDED, not still RUNNING.
	if recovered.StepExecutions["B"].Status != workflow.StepSucceeded {
		t.Errorf("B status = %s, want SUCCEEDED", recovered.StepExecutions["B"].Status)
	}
}

func TestRecoverRunAdvancesRunWhenAllStepsSucceeded(t *testing.T) {
	ctx := context.Background()
	e, def := newRecoverTestEngine(t)

	// Create a run and manually mark all steps SUCCEEDED in memory,
	// but leave the run itself as RUNNING (crash between step success
	// and the final run-status persistence).
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Execute A.
	if err := e.ExecuteStep(ctx, def, run, "A"); err != nil {
		t.Fatal(err)
	}

	// Manually simulate B and C having succeeded but run not yet updated.
	now := time.Now().UTC()
	run.StepExecutions["B"].Status = workflow.StepSucceeded
	run.StepExecutions["B"].Output = map[string]any{"result": float64(10)}
	run.StepExecutions["B"].StartedAt = &now
	run.StepExecutions["B"].FinishedAt = &now

	run.StepExecutions["C"].Status = workflow.StepSucceeded
	run.StepExecutions["C"].Output = map[string]any{"result": float64(20)}
	run.StepExecutions["C"].StartedAt = &now
	run.StepExecutions["C"].FinishedAt = &now

	// Leave run as RUNNING — this is the inconsistency repair must fix.
	run.Status = workflow.RunRunning
	run.UpdatedAt = now

	// Persist C as the last step; the run row still says RUNNING.
	if err := e.store.SaveExecutionState(ctx, run, "C"); err != nil {
		t.Fatal(err)
	}

	// Recover should detect all steps succeeded and mark the run SUCCEEDED.
	e2 := NewWithStore(e.store)
	recovered, err := e2.RecoverRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("RecoverRun: %v", err)
	}

	if recovered.Status != workflow.RunSucceeded {
		t.Errorf("run status = %s, want SUCCEEDED", recovered.Status)
	}
}

func TestRecoverRunReturnsTerminalRunsAsIs(t *testing.T) {
	ctx := context.Background()
	e, def := newRecoverTestEngine(t)

	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Run the whole workflow to completion.
	if err := e.RunWorkflow(ctx, def, run); err != nil {
		t.Fatal(err)
	}
	if run.Status != workflow.RunSucceeded {
		t.Fatalf("precondition: run status = %s", run.Status)
	}

	// Recover on a terminal run should be a no-op.
	e2 := NewWithStore(e.store)
	recovered, err := e2.RecoverRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("RecoverRun: %v", err)
	}

	if recovered.Status != workflow.RunSucceeded {
		t.Errorf("run status = %s, want SUCCEEDED", recovered.Status)
	}
	for stepID, exec := range recovered.StepExecutions {
		if exec.Status != workflow.StepSucceeded {
			t.Errorf("step %s status = %s, want SUCCEEDED", stepID, exec.Status)
		}
	}
}

func TestRecoverRunNoStoreErrors(t *testing.T) {
	e := New()
	_, err := e.RecoverRun(context.Background(), "anything")
	if err == nil {
		t.Fatal("expected error when engine has no store")
	}
}
