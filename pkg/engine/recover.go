package engine

import (
	"context"
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// RecoverRun loads a run from the store, repairs any inconsistent state
// left by a crash, and resumes execution.
//
// Repair rules:
//   - Any step left in RUNNING state (crash mid-execution) is reset to
//     PENDING with its output, error, and timestamps cleared.
//   - If every step is SUCCEEDED but the run is not yet marked SUCCEEDED,
//     the run is advanced to SUCCEEDED.
//   - Terminal runs (SUCCEEDED / FAILED) are returned as-is.
//
// After repair, RecoverRun continues execution via RunWorkflow until the
// run reaches a terminal state.
func (e *Engine) RecoverRun(
	ctx context.Context,
	runID string,
) (*workflow.WorkflowRun, error) {
	if e.store == nil {
		return nil, fmt.Errorf("engine has no store configured")
	}

	// 1. Load the run from persistent storage.
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("load run: %w", err)
	}

	// 2. Already terminal — nothing to repair or resume.
	if run.Status == workflow.RunSucceeded || run.Status == workflow.RunFailed {
		return run, nil
	}

	// 3. Repair steps left in RUNNING by a crash.
	repairedSteps := resetRunningSteps(run)

	// 4. If all steps are already SUCCEEDED, the run should be too
	//    (crash may have occurred between executor success and the final
	//     persistence of the run-level SUCCEEDED status).
	if allStepsSucceeded(run) {
		run.Status = workflow.RunSucceeded
		run.UpdatedAt = nowUTC()
		// Persist the corrected run status.
		if err := e.persistRunStatus(ctx, run); err != nil {
			return nil, fmt.Errorf("persist repaired run status: %w", err)
		}
		return run, nil
	}

	// 5. Persist any step repairs before resuming.
	for _, stepID := range repairedSteps {
		if err := e.persistExecutionState(ctx, run, stepID); err != nil {
			return nil, fmt.Errorf("persist repaired step %q: %w", stepID, err)
		}
	}

	// 6. Load the workflow definition needed to continue execution.
	def, err := e.GetDefinition(ctx, run.WorkflowID, run.WorkflowVersion)
	if err != nil {
		return nil, fmt.Errorf("load workflow definition: %w", err)
	}

	// 7. Resume execution from the repaired state.
	if err := e.RunWorkflow(ctx, def, run); err != nil {
		return run, fmt.Errorf("resume run: %w", err)
	}

	return run, nil
}

// resetRunningSteps returns the IDs of steps that were in RUNNING state
// and have been reset to PENDING. It clears their output, error, and
// timestamps so they can be safely re-executed.
func resetRunningSteps(run *workflow.WorkflowRun) []string {
	var repaired []string

	for stepID, execution := range run.StepExecutions {
		if execution.Status != workflow.StepRunning {
			continue
		}

		execution.Status = workflow.StepPending
		execution.Output = nil
		execution.Error = ""
		execution.StartedAt = nil
		execution.FinishedAt = nil

		repaired = append(repaired, stepID)
	}

	return repaired
}

// persistRunStatus writes only the run-level status and updated_at
// without touching any step row. It is used when a repair only changes
// the run status (all steps already succeeded).
func (e *Engine) persistRunStatus(
	ctx context.Context,
	run *workflow.WorkflowRun,
) error {
	if e.store == nil {
		return nil
	}

	// Delegate to SaveExecutionState: it updates both the step row and
	// the run row in one transaction. We pick any step that exists in the
	// run to satisfy the API, but the step row already has the correct
	// state, so the UPDATE is a no-op on that side.
	for stepID := range run.StepExecutions {
		return e.store.SaveExecutionState(ctx, run, stepID)
	}
	return fmt.Errorf("run %q has no step executions", run.ID)
}
