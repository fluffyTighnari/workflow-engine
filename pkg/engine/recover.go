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
		return nil, fmt.Errorf("%w", ErrStoreUnavailable)
	}

	e.logger.Info("run recovery started", "run_id", runID)

	// 1. Load the run from persistent storage.
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		wrapped := wrapStoreError("load run", err)
		e.logger.Error("run recovery load failed",
			"run_id", runID, "err", wrapped)
		return nil, wrapped
	}

	// 2. Already terminal — nothing to repair or resume.
	if run.Status == workflow.RunSucceeded || run.Status == workflow.RunFailed {
		e.logger.Debug("run recovery skipped",
			"run_id", run.ID,
			"status", run.Status,
			"reason", "run_terminal",
		)
		return run, nil
	}

	// A non-terminal run must not carry failure metadata from an older,
	// inconsistent snapshot.
	run.Failure = nil

	// 3. Repair steps left in RUNNING by a crash.
	repairedSteps := resetRunningSteps(run)
	if len(repairedSteps) > 0 {
		e.logger.Warn("running steps reset",
			"run_id", run.ID,
			"step_ids", repairedSteps,
		)
	}

	// 4. If all steps are already SUCCEEDED, the run should be too
	//    (crash may have occurred between executor success and the final
	//     persistence of the run-level SUCCEEDED status).
	if allStepsSucceeded(run) {
		previousRun := snapshotRun(run)

		run.Status = workflow.RunSucceeded
		clearRunFailure(run)
		run.UpdatedAt = nowUTC()
		// Persist the corrected run status.
		if err := e.persistRunStatus(ctx, run); err != nil {
			previousRun.restore(run)
			wrapped := fmt.Errorf(
				"%w: persist repaired run status: %w",
				ErrPersistence,
				err,
			)
			e.logger.Error("repaired run status persistence failed",
				"run_id", run.ID,
				"err", wrapped,
				"state_rolled_back", true,
			)
			return nil, wrapped
		}

		e.recordRunTerminal(run)
		e.logger.Info("run recovery finalized success",
			"run_id", run.ID,
			"workflow_id", run.WorkflowID,
			"workflow_version", run.WorkflowVersion,
		)
		return run, nil
	}

	// 5. Persist any step repairs before resuming.
	for _, stepID := range repairedSteps {
		if err := e.persistExecutionState(ctx, run, stepID); err != nil {
			wrapped := fmt.Errorf(
				"%w: persist repaired step %q: %w",
				ErrPersistence,
				stepID,
				err,
			)
			e.logger.Error("repaired step persistence failed",
				"run_id", run.ID,
				"step_id", stepID,
				"err", wrapped,
			)
			return nil, wrapped
		}
	}

	// 6. Load the workflow definition needed to continue execution.
	def, err := e.GetDefinition(ctx, run.WorkflowID, run.WorkflowVersion)
	if err != nil {
		wrapped := fmt.Errorf("load workflow definition: %w", err)
		e.logger.Error("run recovery definition load failed",
			"run_id", run.ID,
			"workflow_id", run.WorkflowID,
			"workflow_version", run.WorkflowVersion,
			"err", wrapped,
		)
		return nil, wrapped
	}

	refreshPendingReasons(run, def)

	// 7. Resume execution from the repaired state.
	if err := e.RunWorkflow(ctx, def, run); err != nil {
		e.logger.Error("run recovery failed",
			"run_id", run.ID,
			"err", err,
		)
		return run, fmt.Errorf("resume run: %w", err)
	}

	e.logger.Info("run recovery finished",
		"run_id", run.ID,
		"status", run.Status,
	)
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
		execution.PendingReason = workflow.PendingReasonRecovered
		execution.Failure = nil
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
	return fmt.Errorf(
		"%w: run %q has no step executions",
		ErrPersistence,
		run.ID,
	)
}
