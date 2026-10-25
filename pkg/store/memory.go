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

// SaveDefinition stores a workflow version in memory.
func (m *MemoryStore) SaveDefinition(
	ctx context.Context,
	def workflow.WorkflowDefinition,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.definitions[def.ID]; !ok {
		m.definitions[def.ID] = make(map[int]workflow.WorkflowDefinition)
	}
	m.definitions[def.ID][def.Version] = def
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
		return workflow.WorkflowDefinition{}, fmt.Errorf("workflow %q not found", workflowID)
	}
	def, ok := versions[version]
	if !ok {
		return workflow.WorkflowDefinition{}, fmt.Errorf("workflow %q version %d not found", workflowID, version)
	}
	return def, nil
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
		return nil, fmt.Errorf("run %q not found", runID)
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
	if _, ok := m.runs[run.ID]; !ok {
		return fmt.Errorf("run %q not found", run.ID)
	}

	// Verify the step execution exists.
	if _, ok := run.StepExecutions[stepID]; !ok {
		return fmt.Errorf("step execution %q not found", stepID)
	}

	// Replace the entire run with the updated state (deep copy).
	m.runs[run.ID] = cloneRun(run)
	return nil
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

	if run.Input != nil {
		copied.Input = cloneMapAny(run.Input)
	}

	if run.StepExecutions != nil {
		copied.StepExecutions = make(map[string]*workflow.StepExecution, len(run.StepExecutions))
		for stepID, exec := range run.StepExecutions {
			if exec == nil {
				continue
			}
			execCopy := *exec
			if exec.Input != nil {
				execCopy.Input = cloneMapAny(exec.Input)
			}
			if exec.Output != nil {
				execCopy.Output = cloneMapAny(exec.Output)
			}
			if exec.StartedAt != nil {
				t := *exec.StartedAt
				execCopy.StartedAt = &t
			}
			if exec.FinishedAt != nil {
				t := *exec.FinishedAt
				execCopy.FinishedAt = &t
			}
			copied.StepExecutions[stepID] = &execCopy
		}
	}

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
