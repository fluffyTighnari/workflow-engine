package engine

import (
	"context"
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// ExecuteStep runs a single pending step whose dependencies have all
// succeeded. It persists RUNNING before invoking the executor and
// persists the terminal state afterwards, rolling the in-memory state
// back if the RUNNING persistence fails.
func (e *Engine) ExecuteStep(
	ctx context.Context,
	def workflow.WorkflowDefinition,
	run *workflow.WorkflowRun,
	stepID string,
) error {
	if run.Status == workflow.RunSucceeded ||
		run.Status == workflow.RunFailed {
		return fmt.Errorf("run %q is already terminal", run.ID)
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
			"step %q is not pending: %s",
			stepID,
			execution.Status,
		)
	}

	// 3. Verify all dependencies succeeded.
	if err := checkDependenciesSucceeded(run, stepID, target.DependsOn); err != nil {
		return err
	}

	// 4. Look up the executor.
	executor, ok := e.registry.Get(target.Type)
	if !ok {
		return fmt.Errorf(
			"no executor registered for type %q",
			target.Type,
		)
	}

	// 5. Resolve inputs before marking the step running.
	// If resolution fails, the executor never runs.
	stepInput, err := resolveStepInput(*target, run)
	if err != nil {
		now := nowUTC()

		execution.Status = workflow.StepFailed
		execution.Error = err.Error()
		execution.FinishedAt = &now

		run.Status = workflow.RunFailed
		run.UpdatedAt = now

		if persistErr := e.persistExecutionState(ctx, run, stepID); persistErr != nil {
			return fmt.Errorf(
				"resolve step input: %v; persist failure state: %w",
				err,
				persistErr,
			)
		}

		return fmt.Errorf("resolve input for step %q: %w", stepID, err)
	}

	// 6. Persist RUNNING before invoking the executor.
	previousRunStatus := run.Status
	previousUpdatedAt := run.UpdatedAt

	startedAt := nowUTC()

	execution.Status = workflow.StepRunning
	execution.Input = stepInput
	execution.Error = ""
	execution.StartedAt = &startedAt
	execution.FinishedAt = nil

	run.Status = workflow.RunRunning
	run.UpdatedAt = startedAt

	if err := e.persistExecutionState(ctx, run, stepID); err != nil {
		// The database did not confirm RUNNING, so do not call the executor.
		execution.Status = workflow.StepPending
		execution.Input = nil
		execution.Error = ""
		execution.StartedAt = nil
		execution.FinishedAt = nil

		run.Status = previousRunStatus
		run.UpdatedAt = previousUpdatedAt

		return fmt.Errorf("persist step start: %w", err)
	}

	// 7. Execute the step.
	output, executionErr := executor.Execute(ctx, *target, stepInput)

	finishedAt := nowUTC()
	execution.FinishedAt = &finishedAt
	run.UpdatedAt = finishedAt

	if executionErr != nil {
		execution.Status = workflow.StepFailed
		execution.Error = executionErr.Error()
		run.Status = workflow.RunFailed

		if persistErr := e.persistExecutionState(ctx, run, stepID); persistErr != nil {
			return fmt.Errorf(
				"execute step %q: %v; persist failure state: %w",
				stepID,
				executionErr,
				persistErr,
			)
		}

		return fmt.Errorf("execute step %q: %w", stepID, executionErr)
	}

	// 8. Record the successful result.
	execution.Output = output
	execution.Status = workflow.StepSucceeded
	execution.Error = ""

	if allStepsSucceeded(run) {
		run.Status = workflow.RunSucceeded
	}

	// 9. Persist the final state.
	if err := e.persistExecutionState(ctx, run, stepID); err != nil {
		return fmt.Errorf("persist step result: %w", err)
	}

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
		return fmt.Errorf("run %q is already terminal", run.ID)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if allStepsSucceeded(run) {
			run.Status = workflow.RunSucceeded
			run.UpdatedAt = nowUTC()
			return nil
		}

		progress := false

		for _, step := range def.Steps {
			execution, ok := run.StepExecutions[step.ID]
			if !ok {
				return fmt.Errorf(
					"missing execution record for step %q",
					step.ID,
				)
			}

			if execution.Status != workflow.StepPending {
				continue
			}

			if !dependenciesSucceeded(run, step.DependsOn) {
				continue
			}

			if err := e.ExecuteStep(ctx, def, run, step.ID); err != nil {
				return err
			}

			progress = true
		}

		if !progress {
			return fmt.Errorf(
				"workflow run %q cannot progress: no runnable step",
				run.ID,
			)
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
	return nil, fmt.Errorf("step %q not found", stepID)
}

// getExecution returns the step execution record for the given step ID.
func getExecution(run *workflow.WorkflowRun, stepID string) (*workflow.StepExecution, error) {
	execution, ok := run.StepExecutions[stepID]
	if !ok || execution == nil {
		return nil, fmt.Errorf("execution for step %q not found", stepID)
	}
	return execution, nil
}

// checkDependenciesSucceeded returns an error if any dependency has
// not yet succeeded.
func checkDependenciesSucceeded(run *workflow.WorkflowRun, stepID string, deps []string) error {
	for _, depID := range deps {
		dep, ok := run.StepExecutions[depID]
		if !ok || dep.Status != workflow.StepSucceeded {
			return fmt.Errorf(
				"step %q is waiting for dependency %q",
				stepID,
				depID,
			)
		}
	}
	return nil
}

// dependenciesSucceeded reports whether every listed dependency has
// succeeded.
func dependenciesSucceeded(run *workflow.WorkflowRun, deps []string) bool {
	for _, depID := range deps {
		dep, exists := run.StepExecutions[depID]
		if !exists || dep.Status != workflow.StepSucceeded {
			return false
		}
	}
	return true
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
