package engine

import (
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// stepState is a snapshot of a StepExecution used to roll back in-memory
// mutations when persisting them fails.
type stepState struct {
	status        workflow.StepStatus
	pendingReason workflow.PendingReason
	failure       *workflow.Failure
	input         map[string]any
	output        map[string]any
	err           string
	startedAt     *time.Time
	finishedAt    *time.Time
}

func snapshotStep(execution *workflow.StepExecution) stepState {
	return stepState{
		status:        execution.Status,
		pendingReason: execution.PendingReason,
		failure:       cloneFailure(execution.Failure),
		input:         execution.Input,
		output:        execution.Output,
		err:           execution.Error,
		startedAt:     execution.StartedAt,
		finishedAt:    execution.FinishedAt,
	}
}

func (s stepState) restore(execution *workflow.StepExecution) {
	execution.Status = s.status
	execution.PendingReason = s.pendingReason
	execution.Failure = s.failure
	execution.Input = s.input
	execution.Output = s.output
	execution.Error = s.err
	execution.StartedAt = s.startedAt
	execution.FinishedAt = s.finishedAt
}

// runState is the subset of run-level fields the engine mutates while
// executing a step or scheduling the next one.
type runState struct {
	status        workflow.RunStatus
	pendingReason workflow.PendingReason
	failure       *workflow.Failure
	updatedAt     time.Time
}

func snapshotRun(run *workflow.WorkflowRun) runState {
	return runState{
		status:        run.Status,
		pendingReason: run.PendingReason,
		failure:       cloneFailure(run.Failure),
		updatedAt:     run.UpdatedAt,
	}
}

func (s runState) restore(run *workflow.WorkflowRun) {
	run.Status = s.status
	run.PendingReason = s.pendingReason
	run.Failure = s.failure
	run.UpdatedAt = s.updatedAt
}

func cloneFailure(failure *workflow.Failure) *workflow.Failure {
	if failure == nil {
		return nil
	}

	copied := *failure
	return &copied
}

func setStepFailure(
	execution *workflow.StepExecution,
	kind workflow.FailureKind,
	message string,
) {
	execution.Status = workflow.StepFailed
	execution.PendingReason = workflow.PendingReasonNone
	execution.Failure = &workflow.Failure{
		Kind:    kind,
		Message: message,
	}
	execution.Error = message
}

func setRunFailure(
	run *workflow.WorkflowRun,
	kind workflow.FailureKind,
	message string,
	stepID string,
) {
	run.Status = workflow.RunFailed
	run.PendingReason = workflow.PendingReasonNone
	run.Failure = &workflow.Failure{
		Kind:    kind,
		Message: message,
		StepID:  stepID,
	}
}

func clearStepFailure(execution *workflow.StepExecution) {
	execution.PendingReason = workflow.PendingReasonNone
	execution.Failure = nil
	execution.Error = ""
}

func clearRunFailure(run *workflow.WorkflowRun) {
	run.PendingReason = workflow.PendingReasonNone
	run.Failure = nil
}

// initialPendingReason computes the reason for a step at run creation.
func initialPendingReason(step workflow.Step) workflow.PendingReason {
	if len(step.DependsOn) == 0 {
		return workflow.PendingReasonReady
	}
	return workflow.PendingReasonWaitingDependencies
}

// refreshPendingReasons recomputes PENDING reasons for steps whose
// dependencies have changed. RECOVERED and RETRY provenance is preserved
// until the step executes again.
func refreshPendingReasons(
	run *workflow.WorkflowRun,
	def workflow.WorkflowDefinition,
) {
	for _, step := range def.Steps {
		execution := run.StepExecutions[step.ID]
		if execution == nil || execution.Status != workflow.StepPending {
			continue
		}

		switch execution.PendingReason {
		case workflow.PendingReasonRecovered, workflow.PendingReasonRetry:
			continue
		}

		if _, waiting := unsatisfiedDependency(run, step.DependsOn); waiting {
			execution.PendingReason = workflow.PendingReasonWaitingDependencies
		} else {
			execution.PendingReason = workflow.PendingReasonReady
		}
	}
}
