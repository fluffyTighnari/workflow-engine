package engine

import (
	"context"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// WaitRunPollInterval is the polling interval used by WaitRun.
// Overridable for tests or callers that need tighter or looser polling.
var WaitRunPollInterval = 50 * time.Millisecond

// WaitRun blocks until the run reaches a terminal status (SUCCEEDED or
// FAILED) or ctx is done. It polls GetRun at WaitRunPollInterval.
//
// It is safe to call from multiple goroutines and is the recommended way
// for integrators to wait for an asynchronously submitted run.
func (e *Engine) WaitRun(
	ctx context.Context,
	runID string,
) (*workflow.WorkflowRun, error) {
	for {
		// Use a detached context for the lookup so a cancelled caller
		// ctx is not reported as a store error; cancellation is handled
		// by the select below.
		run, err := e.GetRun(context.Background(), runID)
		if err != nil {
			return nil, err
		}

		if run.Status.Terminal() {
			return run, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(WaitRunPollInterval):
		}
	}
}
