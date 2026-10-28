package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

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
	t.Cleanup(func() { _ = engine1.Close() })

	def := pipelineDefinition()

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
	t.Cleanup(func() { _ = engine2.Close() })

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

	assertStepResults(t, loaded, map[string]float64{
		"A": 5,
		"B": 10,
		"C": 20,
	})
}

func TestRecoverRunResetsRunningSteps(t *testing.T) {
	ctx := context.Background()
	e := newSQLiteTestEngine(t)
	run, def := createRunForPipeline(t, e)

	// Create run and execute A.
	if err := e.ExecuteStep(ctx, def, run, "A"); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-step-B: mark B as RUNNING with stale output
	// that was never persisted as a successful result.
	now := time.Now().UTC()
	run.StepExecutions["B"].Status = workflow.StepRunning
	run.StepExecutions["B"].StartedAt = &now
	run.StepExecutions["B"].Output = map[string]any{"result": float64(999)}
	run.Status = workflow.RunRunning
	run.UpdatedAt = now

	// Persist this crashed state directly (as if the DB had RUNNING B).
	if err := e.SaveExecutionState(ctx, run, "B"); err != nil {
		t.Fatal(err)
	}

	// Recover through a fresh engine sharing the same store.
	e2 := NewWithStore(e.store)
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
	if recovered.StepExecutions["B"].Status != workflow.StepSucceeded {
		t.Errorf("B status = %s, want SUCCEEDED", recovered.StepExecutions["B"].Status)
	}
}

func TestRecoverRunAdvancesRunWhenAllStepsSucceeded(t *testing.T) {
	ctx := context.Background()
	e := newSQLiteTestEngine(t)
	run, def := createRunForPipeline(t, e)

	// Execute A.
	if err := e.ExecuteStep(ctx, def, run, "A"); err != nil {
		t.Fatal(err)
	}

	// Manually simulate B and C having succeeded but the run not yet updated.
	now := time.Now().UTC()
	for _, stepID := range []string{"B", "C"} {
		execution := run.StepExecutions[stepID]
		execution.Status = workflow.StepSucceeded
		execution.StartedAt = &now
		execution.FinishedAt = &now
	}
	run.StepExecutions["B"].Output = map[string]any{"result": float64(10)}
	run.StepExecutions["C"].Output = map[string]any{"result": float64(20)}

	// Leave run as RUNNING — this is the inconsistency repair must fix.
	run.Status = workflow.RunRunning
	run.UpdatedAt = now

	// Persist B and C; the run row still says RUNNING.
	for _, stepID := range []string{"B", "C"} {
		if err := e.SaveExecutionState(ctx, run, stepID); err != nil {
			t.Fatalf("save step %s state: %v", stepID, err)
		}
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
	e := newSQLiteTestEngine(t)
	run, def := createRunForPipeline(t, e)

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
	for stepID, execution := range recovered.StepExecutions {
		if execution.Status != workflow.StepSucceeded {
			t.Errorf("step %s status = %s, want SUCCEEDED",
				stepID, execution.Status)
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
