// Package engine provides the workflow execution engine.
//
// # Public API
//
// Create an engine with NewSQLite (persistent) or NewMemory (ephemeral):
//
//	eng, _ := engine.NewSQLite("workflow.db")
//	eng := engine.NewMemory()
//
// Then drive a workflow through these methods:
//
//   - CreateRun(ctx, def, input) — validate, persist, and build a PENDING run
//   - ExecuteStep(ctx, def, run, stepID) — run one ready step
//   - RunWorkflow(ctx, def, run, stepID) — run all steps until terminal
//   - GetRun(ctx, runID)     — load a run from the store
//   - RecoverRun(ctx, runID) — load, repair crash state, and resume
//   - RegisterExecutor(type, executor) — register a custom step type
//
// # Extension points
//
// Implement StepExecutor to add new step types. Implement store.WorkflowStore
// to plug in a different persistence backend.
package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/store"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// ErrInvalidDefinition is returned when a workflow definition fails
// validation.
var ErrInvalidDefinition = errors.New("invalid workflow definition")

// Engine coordinates workflow definition validation, run creation,
// and step execution with a storage backend.
type Engine struct {
	registry *ExecutorRegistry
	store    store.WorkflowStore
}

// New returns an in-memory engine with built-in executors registered.
// This is equivalent to NewMemory().
func New() *Engine {
	registry := NewExecutorRegistry()
	_ = registry.Register("compute", &ComputeExecutor{})
	_ = registry.Register("double", &DoubleExecutor{})
	return &Engine{
		registry: registry,
	}
}

// NewWithStore returns an Engine backed by the provided WorkflowStore.
func NewWithStore(s store.WorkflowStore) *Engine {
	e := New()
	e.store = s
	return e
}

// RegisterExecutor adds a custom step executor to the engine.
func (e *Engine) RegisterExecutor(
	stepType string,
	executor StepExecutor,
) error {
	return e.registry.Register(stepType, executor)
}

// CreateRun validates a workflow definition, persists it, and builds
// a new run in PENDING status with one pending StepExecution per step.
// The definition and run are automatically persisted to the store.
func (e *Engine) CreateRun(
	ctx context.Context,
	def workflow.WorkflowDefinition,
	input map[string]any,
) (*workflow.WorkflowRun, error) {
	if err := validateDefinition(def); err != nil {
		return nil, err
	}

	runID, err := newID()
	if err != nil {
		return nil, err
	}

	run := &workflow.WorkflowRun{
		ID:              runID,
		WorkflowID:      def.ID,
		WorkflowVersion: def.Version,
		Status:          workflow.RunPending,
		Input:           cloneMap(input),
		StepExecutions:  make(map[string]*workflow.StepExecution, len(def.Steps)),
		CreatedAt:       nowUTC(),
		UpdatedAt:       nowUTC(),
	}

	for _, step := range def.Steps {
		run.StepExecutions[step.ID] = &workflow.StepExecution{
			StepID: step.ID,
			Status: workflow.StepPending,
		}
	}

	// Persist definition and run if we have a store.
	if e.store != nil {
		if err := e.store.SaveDefinition(ctx, def); err != nil {
			return nil, fmt.Errorf("persist definition: %w", err)
		}
		if err := e.store.CreateRun(ctx, def, run); err != nil {
			return nil, fmt.Errorf("persist run: %w", err)
		}
	}

	return run, nil
}

// Close releases the underlying store resources.
func (e *Engine) Close() error {
	if e.store == nil {
		return nil
	}
	return e.store.Close()
}

// SaveDefinition persists a workflow definition to the store.
func (e *Engine) SaveDefinition(ctx context.Context, def workflow.WorkflowDefinition) error {
	if e.store == nil {
		return fmt.Errorf("no store configured")
	}
	return e.store.SaveDefinition(ctx, def)
}

// GetDefinition loads a workflow definition from the store.
func (e *Engine) GetDefinition(ctx context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error) {
	if e.store == nil {
		return workflow.WorkflowDefinition{}, fmt.Errorf("no store configured")
	}
	return e.store.GetDefinition(ctx, workflowID, version)
}

// ListRunsByStatus returns the IDs of runs in any of the given statuses.
// Used by startup recovery to re-enqueue unfinished runs.
func (e *Engine) ListRunsByStatus(ctx context.Context, statuses ...workflow.RunStatus) ([]string, error) {
	if e.store == nil {
		return nil, fmt.Errorf("no store configured")
	}
	return e.store.ListRunsByStatus(ctx, statuses...)
}
