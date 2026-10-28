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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/fluffyTighnari/workflow-engine/pkg/logging"
	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
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
	logger   logging.Logger
	metrics  *metrics.Metrics
}

// SetLogger replaces the engine logger. A nil logger is ignored.
func (e *Engine) SetLogger(logger logging.Logger) {
	if logger != nil {
		e.logger = logger
	}
}

// New returns an engine without a store. CreateRun and RunWorkflow still
// work in memory, but GetRun, RecoverRun, and ListRunsByStatus require a
// store. Use NewMemory for an ephemeral store or NewSQLite for persistence.
func New() *Engine {
	return newEngineWithStore(nil)
}

// NewWithStore returns an Engine backed by the provided WorkflowStore.
func NewWithStore(s store.WorkflowStore) *Engine {
	return newEngineWithStore(s)
}

// RegisterExecutor adds a custom step executor to the engine.
func (e *Engine) RegisterExecutor(
	stepType string,
	executor StepExecutor,
) error {
	err := e.registry.Register(stepType, executor)
	if err != nil {
		e.logger.Warn("executor registration failed",
			"step_type", stepType, "err", err)
		return err
	}

	e.logger.Info("executor registered", "step_type", stepType)
	return nil
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
		e.logger.Warn("run creation rejected",
			"workflow_id", def.ID,
			"workflow_version", def.Version,
			"err", err,
		)
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
		PendingReason:   workflow.PendingReasonNew,
		Input:           cloneMap(input),
		StepExecutions:  make(map[string]*workflow.StepExecution, len(def.Steps)),
		CreatedAt:       nowUTC(),
		UpdatedAt:       nowUTC(),
	}

	for _, step := range def.Steps {
		run.StepExecutions[step.ID] = &workflow.StepExecution{
			StepID:        step.ID,
			Status:        workflow.StepPending,
			PendingReason: initialPendingReason(step),
		}
	}

	// Persist definition and run if we have a store.
	if e.store != nil {
		if err := e.ensureDefinition(ctx, def); err != nil {
			e.logger.Error("run definition persistence failed",
				"workflow_id", def.ID,
				"workflow_version", def.Version,
				"err", err,
			)
			return nil, err
		}
		if err := e.store.CreateRun(ctx, def, run); err != nil {
			if errors.Is(err, store.ErrRunExists) {
				e.logger.Error("run persistence failed",
					"run_id", run.ID, "err", err)
				return nil, err
			}
			wrapped := fmt.Errorf("%w: persist run: %w", ErrPersistence, err)
			e.logger.Error("run persistence failed",
				"run_id", run.ID, "err", wrapped)
			return nil, wrapped
		}
	}

	e.metrics.RunsCreated.Inc()
	e.logger.Info("run created",
		"run_id", run.ID,
		"workflow_id", def.ID,
		"workflow_version", def.Version,
		"step_count", len(def.Steps),
	)
	return run, nil
}

// ensureDefinition makes sure the exact definition is persisted without
// ever overwriting an existing version. Definitions are identified by
// workflow ID + version and are immutable once stored.
func (e *Engine) ensureDefinition(
	ctx context.Context,
	def workflow.WorkflowDefinition,
) error {
	stored, err := e.store.GetDefinition(ctx, def.ID, def.Version)
	switch {
	case err == nil:
		if !definitionsEqual(stored, def) {
			return fmt.Errorf(
				"%w: workflow %q version %d already exists with different content",
				store.ErrDefinitionExists,
				def.ID,
				def.Version,
			)
		}
		return nil
	case errors.Is(err, store.ErrNotFound):
		if err := e.store.SaveDefinition(ctx, def); err != nil {
			if errors.Is(err, store.ErrDefinitionExists) {
				return err
			}
			return fmt.Errorf(
				"%w: persist definition: %w",
				ErrPersistence,
				err,
			)
		}
		return nil
	default:
		return wrapStoreError("load definition", err)
	}
}

// definitionsEqual compares definitions by their canonical JSON form so
// nil and empty maps/slices are treated as equivalent, matching how they
// are persisted.
func definitionsEqual(a, b workflow.WorkflowDefinition) bool {
	aJSON, aErr := json.Marshal(a)
	bJSON, bErr := json.Marshal(b)
	if aErr != nil || bErr != nil {
		return reflect.DeepEqual(a, b)
	}
	return bytes.Equal(aJSON, bJSON)
}

// Close releases the underlying store resources.
func (e *Engine) Close() error {
	if e.store == nil {
		return nil
	}
	return e.store.Close()
}

// SaveDefinition validates and persists a new workflow version. Definitions
// are insert-only; an existing ID + version returns store.ErrDefinitionExists.
func (e *Engine) SaveDefinition(ctx context.Context, def workflow.WorkflowDefinition) error {
	if err := validateDefinition(def); err != nil {
		e.logger.Warn("definition validation failed",
			"workflow_id", def.ID,
			"workflow_version", def.Version,
			"err", err,
		)
		return err
	}
	if e.store == nil {
		return fmt.Errorf("%w", ErrStoreUnavailable)
	}

	err := wrapStoreError("save definition", e.store.SaveDefinition(ctx, def))
	if err != nil {
		if errors.Is(err, store.ErrDefinitionExists) {
			e.logger.Warn("definition already exists",
				"workflow_id", def.ID,
				"workflow_version", def.Version,
			)
		} else {
			e.logger.Error("definition persistence failed",
				"workflow_id", def.ID,
				"workflow_version", def.Version,
				"err", err,
			)
		}
		return err
	}

	e.logger.Info("definition saved",
		"workflow_id", def.ID,
		"workflow_version", def.Version,
		"step_count", len(def.Steps),
	)
	return nil
}

// GetDefinition loads a workflow definition from the store.
func (e *Engine) GetDefinition(ctx context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error) {
	if e.store == nil {
		return workflow.WorkflowDefinition{}, fmt.Errorf("%w", ErrStoreUnavailable)
	}
	def, err := e.store.GetDefinition(ctx, workflowID, version)
	if err != nil {
		return workflow.WorkflowDefinition{},
			wrapStoreError("load definition", err)
	}
	return def, nil
}

// ListRunsByStatus returns the IDs of runs in any of the given statuses.
// Used by startup recovery to re-enqueue unfinished runs.
func (e *Engine) ListRunsByStatus(ctx context.Context, statuses ...workflow.RunStatus) ([]string, error) {
	if e.store == nil {
		return nil, fmt.Errorf("%w", ErrStoreUnavailable)
	}
	ids, err := e.store.ListRunsByStatus(ctx, statuses...)
	if err != nil {
		return nil, wrapStoreError("list runs", err)
	}
	return ids, nil
}
