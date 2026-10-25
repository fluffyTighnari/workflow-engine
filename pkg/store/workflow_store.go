package store

import (
	"context"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
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

// Compile-time checks: SQLiteStore and MemoryStore both implement WorkflowStore.
var (
	_ WorkflowStore = (*SQLiteStore)(nil)
	_ WorkflowStore = (*MemoryStore)(nil)
)
