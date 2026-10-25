package engine

import (
	"context"
	"sync"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// StepExecutor executes one step. Implementations must not modify the
// supplied input map. Register an executor with the same step type
// via Engine.RegisterExecutor.
type StepExecutor interface {
	Execute(
		ctx context.Context,
		step workflow.Step,
		input map[string]any,
	) (map[string]any, error)
}

// ExecutorRegistry maps step types to executors. It is safe for
// concurrent use.
type ExecutorRegistry struct {
	mu        sync.RWMutex
	executors map[string]StepExecutor
}
