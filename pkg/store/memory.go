package store

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// MemoryStore is an in-memory implementation of WorkflowStore for tests
// and ephemeral workflows. It does not persist across restarts.
type MemoryStore struct {
	mu          sync.RWMutex
	definitions map[string]map[int]workflow.WorkflowDefinition // workflowID → version → def
	runs        map[string]*workflow.WorkflowRun               // runID → run
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		definitions: make(map[string]map[int]workflow.WorkflowDefinition),
		runs:        make(map[string]*workflow.WorkflowRun),
	}
}

// SaveDefinition stores a workflow version in memory. Definitions are
// insert-only; saving the same ID + version again returns
// ErrDefinitionExists.
func (m *MemoryStore) SaveDefinition(
	ctx context.Context,
	def workflow.WorkflowDefinition,
) error {
	if err := workflow.ValidateDefinition(def); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDefinition, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if versions, ok := m.definitions[def.ID]; ok {
		if _, exists := versions[def.Version]; exists {
			return fmt.Errorf(
				"%w: workflow %q version %d",
				ErrDefinitionExists,
				def.ID,
				def.Version,
			)
		}
	}

	if _, ok := m.definitions[def.ID]; !ok {
		m.definitions[def.ID] = make(map[int]workflow.WorkflowDefinition)
	}
	m.definitions[def.ID][def.Version] = cloneDefinition(def)
	return nil
}

// GetDefinition returns a previously saved workflow version.
func (m *MemoryStore) GetDefinition(
	ctx context.Context,
	workflowID string,
	version int,
) (workflow.WorkflowDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	versions, ok := m.definitions[workflowID]
	if !ok {
		return workflow.WorkflowDefinition{},
			fmt.Errorf("workflow %q: %w", workflowID, ErrNotFound)
	}
	def, ok := versions[version]
	if !ok {
		return workflow.WorkflowDefinition{},
			fmt.Errorf("workflow %q version %d: %w",
				workflowID, version, ErrNotFound)
	}
	return cloneDefinition(def), nil
}

// CreateRun stores a run and its step executions in memory.
func (m *MemoryStore) CreateRun(
	ctx context.Context,
	def workflow.WorkflowDefinition,
	run *workflow.WorkflowRun,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify the definition exists.
	if _, ok := m.definitions[def.ID][def.Version]; !ok {
		return fmt.Errorf("workflow version must be saved first")
	}

	// Run creation is insert-only; never silently replace an existing run.
	if _, exists := m.runs[run.ID]; exists {
		return fmt.Errorf("%w: %q", ErrRunExists, run.ID)
	}

	// Deep-copy the run so external mutations don't affect internal state.
	runCopy := cloneRun(run)
	m.runs[run.ID] = runCopy
	return nil
}

// GetRun returns a deep copy of the run by ID.
func (m *MemoryStore) GetRun(
	ctx context.Context,
	runID string,
) (*workflow.WorkflowRun, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	run, ok := m.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q: %w", runID, ErrNotFound)
	}
	return cloneRun(run), nil
}

// ListRunsByStatus returns the IDs of runs currently in any of the given
// statuses, sorted for deterministic output.
func (m *MemoryStore) ListRunsByStatus(
	ctx context.Context,
	statuses ...workflow.RunStatus,
) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	wanted := make(map[workflow.RunStatus]bool, len(statuses))
	for _, st := range statuses {
		wanted[st] = true
	}

	var ids []string
	for id, run := range m.runs {
		if wanted[run.Status] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// SaveExecutionState updates the run's state in memory.
func (m *MemoryStore) SaveExecutionState(
	ctx context.Context,
	run *workflow.WorkflowRun,
	stepID string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify the run exists.
	stored, ok := m.runs[run.ID]
	if !ok {
		return fmt.Errorf("run %q not found", run.ID)
	}

	// Verify the step execution exists in both the caller's view and the
	// stored run, matching SQLite's row-update semantics.
	execution, ok := run.StepExecutions[stepID]
	if !ok || execution == nil {
		return fmt.Errorf("step execution %q not found", stepID)
	}
	if _, ok := stored.StepExecutions[stepID]; !ok {
		return fmt.Errorf("step execution %q not found", stepID)
	}

	// Mirror SQLiteStore.SaveExecutionState: update only the run-level
	// status fields and the target step, not the parent run object.
	stored.Status = run.Status
	stored.PendingReason = run.PendingReason
	stored.Failure = cloneFailure(run.Failure)
	stored.UpdatedAt = run.UpdatedAt
	stored.StepExecutions[stepID] = cloneExecution(execution)

	return nil
}

// invalidateRun drops a cached run so the next GetRun reloads it from the
// authoritative store. It is a no-op when the run is not cached.
func (m *MemoryStore) invalidateRun(runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.runs, runID)
}

// invalidateDefinition drops a cached workflow version so the next
// GetDefinition reloads it from the authoritative store.
func (m *MemoryStore) invalidateDefinition(workflowID string, version int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	versions, ok := m.definitions[workflowID]
	if !ok {
		return
	}
	delete(versions, version)
	if len(versions) == 0 {
		delete(m.definitions, workflowID)
	}
}

// Close is a no-op for the in-memory store.
func (m *MemoryStore) Close() error {
	return nil
}

// cloneRun creates a deep copy of a run.
func cloneRun(run *workflow.WorkflowRun) *workflow.WorkflowRun {
	if run == nil {
		return nil
	}

	copied := *run
	copied.Failure = cloneFailure(run.Failure)

	if run.Input != nil {
		copied.Input = cloneMapAny(run.Input)
	}

	if run.StepExecutions != nil {
		copied.StepExecutions = make(map[string]*workflow.StepExecution, len(run.StepExecutions))
		for stepID, exec := range run.StepExecutions {
			if exec == nil {
				continue
			}
			copied.StepExecutions[stepID] = cloneExecution(exec)
		}
	}

	return &copied
}

func cloneDefinition(def workflow.WorkflowDefinition) workflow.WorkflowDefinition {
	copied := def
	if def.Steps == nil {
		return copied
	}

	copied.Steps = make([]workflow.Step, len(def.Steps))
	for i, step := range def.Steps {
		stepCopy := step
		if step.Config != nil {
			stepCopy.Config = make(map[string]string, len(step.Config))
			for key, value := range step.Config {
				stepCopy.Config[key] = value
			}
		}
		if step.DependsOn != nil {
			stepCopy.DependsOn = append([]string(nil), step.DependsOn...)
		}
		if step.Inputs != nil {
			stepCopy.Inputs = make(map[string]workflow.InputRef, len(step.Inputs))
			for key, value := range step.Inputs {
				stepCopy.Inputs[key] = value
			}
		}
		copied.Steps[i] = stepCopy
	}

	return copied
}

func cloneExecution(execution *workflow.StepExecution) *workflow.StepExecution {
	if execution == nil {
		return nil
	}

	copied := *execution
	copied.Failure = cloneFailure(execution.Failure)
	if execution.Input != nil {
		copied.Input = cloneMapAny(execution.Input)
	}
	if execution.Output != nil {
		copied.Output = cloneMapAny(execution.Output)
	}
	if execution.StartedAt != nil {
		t := *execution.StartedAt
		copied.StartedAt = &t
	}
	if execution.FinishedAt != nil {
		t := *execution.FinishedAt
		copied.FinishedAt = &t
	}

	return &copied
}

func cloneFailure(failure *workflow.Failure) *workflow.Failure {
	if failure == nil {
		return nil
	}

	copied := *failure
	return &copied
}

func cloneMapAny(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
