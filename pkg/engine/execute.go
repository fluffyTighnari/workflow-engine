package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// ExecuteStep runs a single pending step whose dependencies have all
// succeeded. It persists RUNNING before invoking the executor and
// persists the terminal state afterwards. Whenever a persistence step
// fails, the in-memory run and step state is rolled back so it never
// gets ahead of what the store accepted.
func (e *Engine) ExecuteStep(
	ctx context.Context,
	def workflow.WorkflowDefinition,
	run *workflow.WorkflowRun,
	stepID string,
) error {
	if run.Status == workflow.RunSucceeded ||
		run.Status == workflow.RunFailed {
		e.logger.Debug("step execution skipped",
			"run_id", run.ID,
			"step_id", stepID,
			"status", run.Status,
			"reason", "run_terminal",
		)
		return fmt.Errorf(
			"%w: run %q is already terminal (%s)",
			ErrRunTerminal,
			run.ID,
			run.Status,
		)
	}

	// 1. Locate the step definition.
	target, err := findStep(def, stepID)
	if err != nil {
		return err
	}

	// 2. Check the step execution record.
	execution, err := getExecution(run, stepID)
	if err != nil {
		return err
	}

	if execution.Status != workflow.StepPending {
		return fmt.Errorf(
			"%w: step %q is not pending: %s",
			ErrStepNotPending,
			stepID,
			execution.Status,
		)
	}

	// 3. Verify all dependencies succeeded.
	if depID, waiting := unsatisfiedDependency(run, target.DependsOn); waiting {
		return fmt.Errorf(
			"%w: step %q is waiting for dependency %q",
			ErrDependencyPending,
			stepID,
			depID,
		)
	}

	// 4. Look up the executor.
	executor, ok := e.registry.Get(target.Type)
	if !ok {
		e.logger.Warn("step executor not found",
			"run_id", run.ID,
			"step_id", stepID,
			"step_type", target.Type,
		)
		return fmt.Errorf(
			"%w: no executor registered for type %q",
			ErrExecutorNotFound,
			target.Type,
		)
	}

	attemptStarted := nowUTC()
	e.metrics.StepsStarted.Inc(target.Type)
	e.metrics.StepsInFlight.Inc()
	defer e.metrics.StepsInFlight.Dec()

	// 5. Resolve inputs before marking the step running.
	// If resolution fails, the executor never runs.
	stepInput, err := resolveStepInput(*target, run)
	if err != nil {
		previousStep := snapshotStep(execution)
		previousRun := snapshotRun(run)

		now := nowUTC()

		execution.FinishedAt = &now
		setStepFailure(execution, workflow.FailureKindInput, err.Error())
		setRunFailure(run, workflow.FailureKindInput, err.Error(), stepID)
		run.UpdatedAt = now

		inputErr := fmt.Errorf("%w: step %q: %w", ErrInputResolution, stepID, err)

		e.logger.Error("step input resolution failed",
			"run_id", run.ID,
			"step_id", stepID,
			"step_type", target.Type,
			"err", inputErr,
		)

		if persistErr := e.persistExecutionState(ctx, run, stepID); persistErr != nil {
			previousStep.restore(execution)
			previousRun.restore(run)

			persistErr = fmt.Errorf(
				"%w: persist failure state: %w",
				ErrPersistence,
				persistErr,
			)
			e.logger.Error("step failure state persistence failed",
				"run_id", run.ID,
				"step_id", stepID,
				"err", persistErr,
				"state_rolled_back", true,
			)
			return errors.Join(inputErr, persistErr)
		}

		e.recordStepFinished(
			target.Type,
			workflow.StepFailed,
			workflow.FailureKindInput,
			now.Sub(attemptStarted),
		)
		e.recordRunTerminal(run)
		return inputErr
	}

	// 6. Persist RUNNING before invoking the executor.
	previousStep := snapshotStep(execution)
	previousRun := snapshotRun(run)

	startedAt := nowUTC()

	execution.Status = workflow.StepRunning
	execution.Input = stepInput
	execution.StartedAt = &startedAt
	execution.FinishedAt = nil
	clearStepFailure(execution)

	run.Status = workflow.RunRunning
	clearRunFailure(run)
	run.UpdatedAt = startedAt

	if err := e.persistExecutionState(ctx, run, stepID); err != nil {
		// The database did not confirm RUNNING, so do not call the executor.
		previousStep.restore(execution)
		previousRun.restore(run)

		wrapped := fmt.Errorf("%w: persist step start: %w", ErrPersistence, err)
		e.logger.Error("step start persistence failed",
			"run_id", run.ID,
			"step_id", stepID,
			"step_type", target.Type,
			"err", wrapped,
			"state_rolled_back", true,
		)
		return wrapped
	}

	e.logger.Info("step started",
		"run_id", run.ID,
		"step_id", stepID,
		"step_type", target.Type,
	)

	// 7. Execute the step.
	output, executionErr := executor.Execute(ctx, *target, stepInput)

	finishedAt := nowUTC()
	execution.FinishedAt = &finishedAt
	run.UpdatedAt = finishedAt

	// Snapshot the state right after the executor returned, before the
	// final result is committed. If persistence fails, memory must not
	// claim a result that the store never accepted.
	postExecution := snapshotStep(execution)
	postExecutionRun := snapshotRun(run)

	if executionErr != nil {
		kind := workflow.FailureKindExecutor
		cause := ErrExecutorFailure
		if errors.Is(executionErr, context.Canceled) ||
			errors.Is(executionErr, context.DeadlineExceeded) {
			kind = workflow.FailureKindCanceled
			cause = ErrExecutionCanceled
		}

		setStepFailure(execution, kind, executionErr.Error())
		setRunFailure(run, kind, executionErr.Error(), stepID)

		execErr := fmt.Errorf("%w: step %q: %w", cause, stepID, executionErr)
		duration := finishedAt.Sub(startedAt).Milliseconds()

		if kind == workflow.FailureKindCanceled {
			e.logger.Warn("step canceled",
				"run_id", run.ID,
				"step_id", stepID,
				"step_type", target.Type,
				"duration_ms", duration,
				"err", execErr,
			)
		} else {
			e.logger.Error("step failed",
				"run_id", run.ID,
				"step_id", stepID,
				"step_type", target.Type,
				"failure_kind", kind,
				"duration_ms", duration,
				"err", execErr,
			)
		}

		if persistErr := e.persistExecutionState(ctx, run, stepID); persistErr != nil {
			postExecution.restore(execution)
			postExecutionRun.restore(run)

			persistErr = fmt.Errorf(
				"%w: persist failure state: %w",
				ErrPersistence,
				persistErr,
			)
			e.logger.Error("step failure state persistence failed",
				"run_id", run.ID,
				"step_id", stepID,
				"err", persistErr,
				"state_rolled_back", true,
			)
			return errors.Join(execErr, persistErr)
		}

		e.recordStepFinished(
			target.Type,
			workflow.StepFailed,
			kind,
			finishedAt.Sub(startedAt),
		)
		e.recordRunTerminal(run)
		return execErr
	}

	// 8. Record the successful result.
	execution.Output = output
	execution.Status = workflow.StepSucceeded
	clearStepFailure(execution)

	clearRunFailure(run)
	if allStepsSucceeded(run) {
		run.Status = workflow.RunSucceeded
	}

	// 9. Persist the final state.
	if err := e.persistExecutionState(ctx, run, stepID); err != nil {
		postExecution.restore(execution)
		postExecutionRun.restore(run)

		wrapped := fmt.Errorf("%w: persist step result: %w", ErrPersistence, err)
		e.logger.Error("step result persistence failed",
			"run_id", run.ID,
			"step_id", stepID,
			"step_type", target.Type,
			"err", wrapped,
			"state_rolled_back", true,
		)
		return wrapped
	}

	e.recordStepFinished(
		target.Type,
		workflow.StepSucceeded,
		workflow.FailureKindNone,
		finishedAt.Sub(startedAt),
	)
	e.recordRunTerminal(run)

	e.logger.Info("step succeeded",
		"run_id", run.ID,
		"step_id", stepID,
		"step_type", target.Type,
		"duration_ms", finishedAt.Sub(startedAt).Milliseconds(),
	)

	// Downstream steps may have become ready. Persist their reasoning
	// lazily through the engine; the next read or execution recomputes it.
	refreshPendingReasons(run, def)

	return nil
}

// RunWorkflow executes steps in dependency order until every step has
// succeeded or no pending step can make progress.
func (e *Engine) RunWorkflow(
	ctx context.Context,
	def workflow.WorkflowDefinition,
	run *workflow.WorkflowRun,
) error {
	if run.Status == workflow.RunSucceeded ||
		run.Status == workflow.RunFailed {
		e.logger.Debug("run execution skipped",
			"run_id", run.ID,
			"status", run.Status,
			"reason", "run_terminal",
		)
		return fmt.Errorf(
			"%w: run %q is already terminal (%s)",
			ErrRunTerminal,
			run.ID,
			run.Status,
		)
	}

	started := nowUTC()
	defer func() {
		e.metrics.RunExecution.ObserveSeconds(
			nowUTC().Sub(started).Seconds(),
			string(run.Status),
		)
	}()

	e.logger.Info("run execution started",
		"run_id", run.ID,
		"workflow_id", run.WorkflowID,
		"workflow_version", run.WorkflowVersion,
		"status", run.Status,
	)

	for {
		if err := ctx.Err(); err != nil {
			e.logger.Warn("run execution canceled",
				"run_id", run.ID,
				"err", err,
			)
			return err
		}

		refreshPendingReasons(run, def)

		if allStepsSucceeded(run) {
			previousRun := snapshotRun(run)
			alreadyTerminal := run.Status.Terminal()

			run.Status = workflow.RunSucceeded
			clearRunFailure(run)
			run.UpdatedAt = nowUTC()

			if err := e.persistRunStatus(ctx, run); err != nil {
				previousRun.restore(run)
				wrapped := fmt.Errorf(
					"%w: persist run success: %w",
					ErrPersistence,
					err,
				)
				e.logger.Error("run success persistence failed",
					"run_id", run.ID,
					"err", wrapped,
					"state_rolled_back", true,
				)
				return wrapped
			}

			if !alreadyTerminal {
				e.recordRunTerminal(run)
			}

			e.logger.Info("run succeeded",
				"run_id", run.ID,
				"workflow_id", run.WorkflowID,
				"workflow_version", run.WorkflowVersion,
				"duration_ms", nowUTC().Sub(started).Milliseconds(),
			)
			return nil
		}

		progress := false

		for _, step := range def.Steps {
			execution, ok := run.StepExecutions[step.ID]
			if !ok || execution == nil {
				e.logger.Error("step execution record missing",
					"run_id", run.ID,
					"step_id", step.ID,
				)
				return fmt.Errorf(
					"%w: step %q",
					ErrExecutionNotFound,
					step.ID,
				)
			}

			if execution.Status != workflow.StepPending {
				continue
			}

			if _, waiting := unsatisfiedDependency(run, step.DependsOn); waiting {
				continue
			}

			if err := e.ExecuteStep(ctx, def, run, step.ID); err != nil {
				return err
			}

			progress = true
		}

		if !progress {
			previousRun := snapshotRun(run)
			noProgress := fmt.Errorf(
				"%w: workflow run %q cannot progress: no runnable step",
				ErrNoProgress,
				run.ID,
			)

			setRunFailure(
				run,
				workflow.FailureKindDeadlock,
				noProgress.Error(),
				"",
			)
			run.UpdatedAt = nowUTC()

			if err := e.persistRunStatus(ctx, run); err != nil {
				previousRun.restore(run)
				persistErr := fmt.Errorf(
					"%w: persist deadlock state: %w",
					ErrPersistence,
					err,
				)
				e.logger.Error("deadlock state persistence failed",
					"run_id", run.ID,
					"err", persistErr,
					"state_rolled_back", true,
				)
				return errors.Join(noProgress, persistErr)
			}

			e.recordRunTerminal(run)
			e.logger.Error("workflow cannot make progress",
				"run_id", run.ID,
				"workflow_id", run.WorkflowID,
				"workflow_version", run.WorkflowVersion,
				"err", noProgress,
			)
			return noProgress
		}
	}
}

// findStep returns the step definition with the given ID.
func findStep(def workflow.WorkflowDefinition, stepID string) (*workflow.Step, error) {
	for i := range def.Steps {
		if def.Steps[i].ID == stepID {
			return &def.Steps[i], nil
		}
	}
	return nil, fmt.Errorf("%w: step %q not found", ErrStepNotFound, stepID)
}

// getExecution returns the step execution record for the given step ID.
func getExecution(run *workflow.WorkflowRun, stepID string) (*workflow.StepExecution, error) {
	execution, ok := run.StepExecutions[stepID]
	if !ok || execution == nil {
		return nil, fmt.Errorf(
			"%w: execution for step %q not found",
			ErrExecutionNotFound,
			stepID,
		)
	}
	return execution, nil
}

// unsatisfiedDependency returns the first dependency that has not yet
// succeeded, if any. A missing execution record counts as unsatisfied.
func unsatisfiedDependency(
	run *workflow.WorkflowRun,
	deps []string,
) (string, bool) {
	for _, depID := range deps {
		dep, ok := run.StepExecutions[depID]
		if !ok || dep == nil || dep.Status != workflow.StepSucceeded {
			return depID, true
		}
	}
	return "", false
}

// allStepsSucceeded reports whether every step in the run has succeeded.
func allStepsSucceeded(run *workflow.WorkflowRun) bool {
	for _, execution := range run.StepExecutions {
		if execution.Status != workflow.StepSucceeded {
			return false
		}
	}
	return true
}
