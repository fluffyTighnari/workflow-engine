package engine

import (
	"context"
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// persistExecutionState forwards the state to the store, or is a no-op
// when no store is configured.
func (e *Engine) persistExecutionState(
	ctx context.Context,
	run *workflow.WorkflowRun,
	stepID string,
) error {
	if e.store == nil {
		return nil
	}

	return e.store.SaveExecutionState(ctx, run, stepID)
}

// SaveExecutionState persists the current run state and the given step's
// state to the store. Exposed for direct state writes (e.g. tests and
// crash simulation).
func (e *Engine) SaveExecutionState(
	ctx context.Context,
	run *workflow.WorkflowRun,
	stepID string,
) error {
	return e.persistExecutionState(ctx, run, stepID)
}

// GetRun loads a run from the configured store. It returns an
// error when the engine has no store attached.
func (e *Engine) GetRun(
	ctx context.Context,
	runID string,
) (*workflow.WorkflowRun, error) {
	if e.store == nil {
		return nil, fmt.Errorf("engine has no store configured")
	}

	return e.store.GetRun(ctx, runID)
}
