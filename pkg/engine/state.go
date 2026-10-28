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
	if err := e.persistExecutionState(ctx, run, stepID); err != nil {
		return fmt.Errorf(
			"%w: save execution state: %w",
			ErrPersistence,
			err,
		)
	}
	return nil
}

// GetRun loads a run from the configured store. It returns an
// error when the engine has no store attached.
func (e *Engine) GetRun(
	ctx context.Context,
	runID string,
) (*workflow.WorkflowRun, error) {
	if e.store == nil {
		return nil, fmt.Errorf("%w", ErrStoreUnavailable)
	}

	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return nil, wrapStoreError("load run", err)
	}

	// Pending reasons are derived from dependency state, so refresh them
	// against the versioned definition before returning. A missing
	// definition is not fatal here: the stored state is still returnable.
	if def, err := e.store.GetDefinition(
		ctx,
		run.WorkflowID,
		run.WorkflowVersion,
	); err == nil {
		refreshPendingReasons(run, def)
	}

	return run, nil
}
