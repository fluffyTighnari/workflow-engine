package engine

import (
	"fmt"
)

// NewExecutorRegistry returns an empty registry.
func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{
		executors: make(map[string]StepExecutor),
	}
}

// Register adds an executor for the given step type.
// It returns an error if the type is empty, the executor is nil, or
// the type is already registered.
func (r *ExecutorRegistry) Register(
	stepType string,
	executor StepExecutor,
) error {
	if stepType == "" {
		return fmt.Errorf("step type cannot be empty")
	}
	if executor == nil {
		return fmt.Errorf("executor cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.executors[stepType]; exists {
		return fmt.Errorf("executor already registered for type %q", stepType)
	}

	r.executors[stepType] = executor
	return nil
}

// Get returns the executor registered for stepType, or false if none.
func (r *ExecutorRegistry) Get(stepType string) (StepExecutor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	executor, ok := r.executors[stepType]
	return executor, ok
}
