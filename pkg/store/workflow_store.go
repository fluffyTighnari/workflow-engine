package store

import (
	"context"
	"errors"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

var (
	// ErrNotFound is the sentinel error returned by GetRun / GetDefinition
	// when the requested run or workflow version does not exist.
	// Callers can match it with errors.Is(err, store.ErrNotFound) regardless
	// of the underlying backend (SQLite, memory, or custom stores).
	ErrNotFound = errors.New("workflow store: not found")

	// ErrDefinitionExists is returned when a workflow ID + version has
	// already been persisted. Definitions are insert-only: changing a
	// definition requires a new version.
	ErrDefinitionExists = errors.New("workflow definition already exists")

	// ErrRunExists is returned when a run ID has already been persisted.
	// Run creation is insert-only in every backend.
	ErrRunExists = errors.New("workflow run already exists")

	// ErrInvalidDefinition is returned by stores that validate definitions
	// before persisting them.
	ErrInvalidDefinition = errors.New("invalid workflow definition")
)

// WorkflowStore is the unified storage interface implemented by both
// SQLiteStore and MemoryStore. It supersedes the narrower
// engine.ExecutionStateStore interface for callers that need the full
// lifecycle API (save definition, create run, close).
type WorkflowStore interface {
	// Definition
	SaveDefinition(ctx context.Context, def workflow.WorkflowDefinition) error
	GetDefinition(ctx context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error)

	// Run lifecycle
	CreateRun(ctx context.Context, def workflow.WorkflowDefinition, run *workflow.WorkflowRun) error
	GetRun(ctx context.Context, runID string) (*workflow.WorkflowRun, error)
	// ListRunsByStatus 返回处于指定状态之一的 run ID 列表。
	// 用于进程启动时扫描未完成任务（崩溃恢复）。
	ListRunsByStatus(ctx context.Context, statuses ...workflow.RunStatus) ([]string, error)

	// Step transition persistence (engine calls this)
	SaveExecutionState(ctx context.Context, run *workflow.WorkflowRun, stepID string) error

	// Lifecycle
	Close() error
}

// Compile-time checks: every built-in store implements WorkflowStore.
var (
	_ WorkflowStore = (*SQLiteStore)(nil)
	_ WorkflowStore = (*MemoryStore)(nil)
	_ WorkflowStore = (*CachedStore)(nil)
)
