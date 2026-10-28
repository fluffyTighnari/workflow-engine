package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// canceledExecutor returns context.Canceled to exercise failure-kind
// classification without treating it as a generic executor error.
type canceledExecutor struct{}

func (canceledExecutor) Execute(
	context.Context,
	workflow.Step,
	map[string]any,
) (map[string]any, error) {
	return nil, context.Canceled
}

func TestCreateRunSetsPendingReasons(t *testing.T) {
	e := newMemoryTestEngine(t)
	run, _ := createRunForPipeline(t, e)

	if run.Status != workflow.RunPending {
		t.Fatalf("run status = %s, want PENDING", run.Status)
	}
	if run.PendingReason != workflow.PendingReasonNew {
		t.Errorf("run pending reason = %q, want NEW", run.PendingReason)
	}

	want := map[string]workflow.PendingReason{
		"A": workflow.PendingReasonReady,
		"B": workflow.PendingReasonWaitingDependencies,
		"C": workflow.PendingReasonWaitingDependencies,
	}
	for stepID, reason := range want {
		if got := run.StepExecutions[stepID].PendingReason; got != reason {
			t.Errorf("step %s pending reason = %q, want %q",
				stepID, got, reason)
		}
	}
}

func TestExecuteStepRefreshesDownstreamPendingReasons(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	run, def := createRunForPipeline(t, e)

	if err := e.ExecuteStep(ctx, def, run, "A"); err != nil {
		t.Fatalf("execute A: %v", err)
	}

	if got := run.StepExecutions["B"].PendingReason; got != workflow.PendingReasonReady {
		t.Errorf("B pending reason = %q, want READY", got)
	}
	if got := run.StepExecutions["C"].PendingReason; got != workflow.PendingReasonWaitingDependencies {
		t.Errorf("C pending reason = %q, want WAITING_DEPENDENCIES", got)
	}
}

func TestExecuteStepInputFailureHasFailureKind(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)

	def := workflow.WorkflowDefinition{
		ID:      "input-failure",
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

	err = e.ExecuteStep(ctx, def, run, "A")
	if !errors.Is(err, ErrInputResolution) {
		t.Fatalf("error = %v, want ErrInputResolution", err)
	}
	if !errors.Is(err, ErrInputMissingKey) {
		t.Fatalf("error = %v, want ErrInputMissingKey", err)
	}
	if !IsTerminalFailure(err) || IsRetryable(err) {
		t.Fatalf("input failure classification: terminal=%v retryable=%v",
			IsTerminalFailure(err), IsRetryable(err))
	}

	execution := run.StepExecutions["A"]
	if execution.Status != workflow.StepFailed {
		t.Errorf("step status = %s, want FAILED", execution.Status)
	}
	if execution.Failure == nil ||
		execution.Failure.Kind != workflow.FailureKindInput {
		t.Errorf("step failure = %+v, want kind INPUT", execution.Failure)
	}
	if execution.PendingReason != workflow.PendingReasonNone {
		t.Errorf("step pending reason = %q, want empty", execution.PendingReason)
	}

	if run.Status != workflow.RunFailed {
		t.Errorf("run status = %s, want FAILED", run.Status)
	}
	if run.Failure == nil ||
		run.Failure.Kind != workflow.FailureKindInput ||
		run.Failure.StepID != "A" {
		t.Errorf("run failure = %+v, want kind INPUT for step A", run.Failure)
	}
}

func TestExecuteStepExecutorFailureHasFailureKind(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	if err := e.RegisterExecutor("fail", errorExecutor{}); err != nil {
		t.Fatal(err)
	}

	def := workflow.WorkflowDefinition{
		ID:      "executor-failure",
		Version: 1,
		Steps:   []workflow.Step{{ID: "A", Type: "fail"}},
	}
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = e.ExecuteStep(ctx, def, run, "A")
	if !errors.Is(err, ErrExecutorFailure) {
		t.Fatalf("error = %v, want ErrExecutorFailure", err)
	}
	if !IsTerminalFailure(err) {
		t.Fatal("executor failure must be terminal")
	}

	if got := run.StepExecutions["A"].Failure; got == nil ||
		got.Kind != workflow.FailureKindExecutor {
		t.Errorf("step failure = %+v, want kind EXECUTOR", got)
	}
	if run.Failure == nil ||
		run.Failure.Kind != workflow.FailureKindExecutor ||
		run.Failure.StepID != "A" {
		t.Errorf("run failure = %+v, want kind EXECUTOR for step A", run.Failure)
	}
}

func TestExecuteStepCanceledFailureHasFailureKind(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	if err := e.RegisterExecutor("cancel", canceledExecutor{}); err != nil {
		t.Fatal(err)
	}

	def := workflow.WorkflowDefinition{
		ID:      "canceled-failure",
		Version: 1,
		Steps:   []workflow.Step{{ID: "A", Type: "cancel"}},
	}
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = e.ExecuteStep(ctx, def, run, "A")
	if !errors.Is(err, ErrExecutionCanceled) {
		t.Fatalf("error = %v, want ErrExecutionCanceled", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatal("canceled error must wrap context.Canceled")
	}

	if got := run.StepExecutions["A"].Failure; got == nil ||
		got.Kind != workflow.FailureKindCanceled {
		t.Errorf("step failure = %+v, want kind CANCELED", got)
	}
}

func TestExecuteStepPreflightErrorsAreTyped(t *testing.T) {
	ctx := context.Background()

	t.Run("terminal run", func(t *testing.T) {
		e := newMemoryTestEngine(t)
		run, def := createRunForPipeline(t, e)
		run.Status = workflow.RunSucceeded

		err := e.ExecuteStep(ctx, def, run, "A")
		if !errors.Is(err, ErrRunTerminal) {
			t.Fatalf("error = %v, want ErrRunTerminal", err)
		}
	})

	t.Run("step not found", func(t *testing.T) {
		e := newMemoryTestEngine(t)
		run, def := createRunForPipeline(t, e)

		err := e.ExecuteStep(ctx, def, run, "MISSING")
		if !errors.Is(err, ErrStepNotFound) {
			t.Fatalf("error = %v, want ErrStepNotFound", err)
		}
	})

	t.Run("execution not found", func(t *testing.T) {
		e := newMemoryTestEngine(t)
		def := workflow.WorkflowDefinition{
			ID:      "missing-execution",
			Version: 1,
			Steps:   []workflow.Step{{ID: "A", Type: "compute"}},
		}
		run := &workflow.WorkflowRun{
			ID:             "run",
			Status:         workflow.RunPending,
			StepExecutions: map[string]*workflow.StepExecution{},
		}

		err := e.ExecuteStep(ctx, def, run, "A")
		if !errors.Is(err, ErrExecutionNotFound) {
			t.Fatalf("error = %v, want ErrExecutionNotFound", err)
		}
	})

	t.Run("step not pending", func(t *testing.T) {
		e := newMemoryTestEngine(t)
		run, def := createRunForPipeline(t, e)
		run.StepExecutions["A"].Status = workflow.StepRunning

		err := e.ExecuteStep(ctx, def, run, "A")
		if !errors.Is(err, ErrStepNotPending) {
			t.Fatalf("error = %v, want ErrStepNotPending", err)
		}
	})

	t.Run("dependency pending", func(t *testing.T) {
		e := newMemoryTestEngine(t)
		run, def := createRunForPipeline(t, e)

		err := e.ExecuteStep(ctx, def, run, "B")
		if !errors.Is(err, ErrDependencyPending) {
			t.Fatalf("error = %v, want ErrDependencyPending", err)
		}
		if !IsRetryable(err) {
			t.Fatal("dependency pending must be retryable")
		}
	})

	t.Run("executor missing", func(t *testing.T) {
		e := newMemoryTestEngine(t)
		def := workflow.WorkflowDefinition{
			ID:      "missing-executor",
			Version: 1,
			Steps:   []workflow.Step{{ID: "A", Type: "unknown"}},
		}
		run, err := e.CreateRun(ctx, def, nil)
		if err != nil {
			t.Fatal(err)
		}

		err = e.ExecuteStep(ctx, def, run, "A")
		if !errors.Is(err, ErrExecutorNotFound) {
			t.Fatalf("error = %v, want ErrExecutorNotFound", err)
		}
		if !IsRetryable(err) {
			t.Fatal("missing executor must be retryable")
		}
		if run.Status != workflow.RunPending {
			t.Errorf("run status = %s, want PENDING", run.Status)
		}
	})
}

func TestRunWorkflowPersistsRunSuccessWhenAllStepsSucceeded(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	run, def := createRunForPipeline(t, e)

	// Persist all step results while the run is still RUNNING, then let
	// RunWorkflow observe the all-succeeded state and finalize the run.
	run.Status = workflow.RunRunning
	for _, stepID := range []string{"A", "B", "C"} {
		execution := run.StepExecutions[stepID]
		execution.Status = workflow.StepSucceeded
		execution.Output = map[string]any{"result": float64(1)}
		if err := e.SaveExecutionState(ctx, run, stepID); err != nil {
			t.Fatalf("persist step %s: %v", stepID, err)
		}
	}

	if err := e.RunWorkflow(ctx, def, run); err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}

	persisted, err := e.store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != workflow.RunSucceeded {
		t.Errorf("persisted run status = %s, want SUCCEEDED", persisted.Status)
	}
	if persisted.PendingReason != workflow.PendingReasonNone {
		t.Errorf("persisted pending reason = %q, want empty",
			persisted.PendingReason)
	}
}

func TestRunWorkflowDeadlockIsTerminalFailure(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)

	def := workflow.WorkflowDefinition{
		ID:      "deadlock",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "A", Type: "compute", DependsOn: []string{"B"}},
			{ID: "B", Type: "compute"},
		},
	}
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	// B is failed but the run was never marked failed: A can never run.
	run.StepExecutions["B"].Status = workflow.StepFailed
	run.StepExecutions["B"].Error = "boom"
	if err := e.SaveExecutionState(ctx, run, "B"); err != nil {
		t.Fatal(err)
	}

	err = e.RunWorkflow(ctx, def, run)
	if !errors.Is(err, ErrNoProgress) {
		t.Fatalf("error = %v, want ErrNoProgress", err)
	}
	if !IsTerminalFailure(err) || IsRetryable(err) {
		t.Fatalf("deadlock classification: terminal=%v retryable=%v",
			IsTerminalFailure(err), IsRetryable(err))
	}
	if run.Status != workflow.RunFailed {
		t.Errorf("run status = %s, want FAILED", run.Status)
	}
	if run.Failure == nil ||
		run.Failure.Kind != workflow.FailureKindDeadlock {
		t.Errorf("run failure = %+v, want kind DEADLOCK", run.Failure)
	}

	persisted, err := e.store.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != workflow.RunFailed {
		t.Errorf("persisted run status = %s, want FAILED", persisted.Status)
	}
	if persisted.Failure == nil ||
		persisted.Failure.Kind != workflow.FailureKindDeadlock {
		t.Errorf("persisted run failure = %+v, want kind DEADLOCK",
			persisted.Failure)
	}
}

func TestResetRunningStepsMarksRecovered(t *testing.T) {
	run := &workflow.WorkflowRun{
		ID:             "run",
		Status:         workflow.RunRunning,
		StepExecutions: map[string]*workflow.StepExecution{},
	}
	run.StepExecutions["A"] = &workflow.StepExecution{
		StepID: "A",
		Status: workflow.StepRunning,
	}

	run.StepExecutions["A"].Status = workflow.StepRunning
	run.StepExecutions["A"].Failure = &workflow.Failure{
		Kind:    workflow.FailureKindExecutor,
		Message: "stale crash state",
	}
	run.StepExecutions["A"].Output = map[string]any{"stale": true}
	run.StepExecutions["A"].Error = "stale"

	repaired := resetRunningSteps(run)
	if len(repaired) != 1 || repaired[0] != "A" {
		t.Fatalf("repaired = %v, want [A]", repaired)
	}

	execution := run.StepExecutions["A"]
	if execution.Status != workflow.StepPending {
		t.Errorf("step status = %s, want PENDING", execution.Status)
	}
	if execution.PendingReason != workflow.PendingReasonRecovered {
		t.Errorf("pending reason = %q, want RECOVERED", execution.PendingReason)
	}
	if execution.Failure != nil {
		t.Errorf("failure = %+v, want nil", execution.Failure)
	}
	if execution.Output != nil || execution.Error != "" {
		t.Errorf("stale output/error must be cleared: output=%v error=%q",
			execution.Output, execution.Error)
	}
}
