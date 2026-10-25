package store

import (
	"context"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// CachedStore 是一个具体的、自包含的存储实现，内部组合：
//   - MemoryStore（缓存层，快速访问）
//   - SQLiteStore（持久化层，跨重启保留）
//
// 读写策略：
//   - CreateRun / SaveDefinition：先写持久化（保证不丢），再写缓存（快速访问）
//   - SaveExecutionState：先写缓存（执行循环快速更新），再写持久化（保证不丢）
//   - GetRun / GetDefinition：先查缓存，miss 再查持久化，然后回填缓存
type CachedStore struct {
	cache   *MemoryStore // 缓存层
	persist *SQLiteStore // 持久化层
}

// NewCachedStore 创建一个 CachedStore，内部同时初始化 MemoryStore 和 SQLiteStore。
// path 是 SQLite 数据库文件路径。
func NewCachedStore(path string) (*CachedStore, error) {
	s, err := Open(path)
	if err != nil {
		return nil, err
	}
	return &CachedStore{
		cache:   NewMemoryStore(),
		persist: s,
	}, nil
}

// SaveDefinition 先写持久化，再写缓存。
func (c *CachedStore) SaveDefinition(ctx context.Context, def workflow.WorkflowDefinition) error {
	if err := c.persist.SaveDefinition(ctx, def); err != nil {
		return err
	}
	return c.cache.SaveDefinition(ctx, def)
}

// GetDefinition 先查缓存，miss 再查持久化，然后回填缓存。
func (c *CachedStore) GetDefinition(ctx context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error) {
	// Try cache first.
	def, err := c.cache.GetDefinition(ctx, workflowID, version)
	if err == nil {
		return def, nil
	}

	// Cache miss: load from persist.
	def, err = c.persist.GetDefinition(ctx, workflowID, version)
	if err != nil {
		return workflow.WorkflowDefinition{}, err
	}

	// Populate cache.
	_ = c.cache.SaveDefinition(ctx, def)
	return def, nil
}

// CreateRun 先写持久化，再写缓存。
func (c *CachedStore) CreateRun(ctx context.Context, def workflow.WorkflowDefinition, run *workflow.WorkflowRun) error {
	if err := c.persist.CreateRun(ctx, def, run); err != nil {
		return err
	}
	return c.cache.CreateRun(ctx, def, run)
}

// GetRun 先查缓存，miss 再查持久化，然后回填缓存。
func (c *CachedStore) GetRun(ctx context.Context, runID string) (*workflow.WorkflowRun, error) { // Try cache first.
	run, err := c.cache.GetRun(ctx, runID)
	if err == nil {
		return run, nil
	}

	// Cache miss: load from persist.
	run, err = c.persist.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	// Populate cache: we need the definition for MemoryStore.CreateRun,
	// so load it from persist as well.
	def, err := c.GetDefinition(ctx, run.WorkflowID, run.WorkflowVersion)
	if err != nil {
		// Definition load failed but run exists — return the run anyway.
		return run, nil
	}
	_ = c.cache.CreateRun(ctx, def, run)
	return run, nil
}

// SaveExecutionState 先写缓存（执行循环快速更新），再写持久化（保证不丢）。
func (c *CachedStore) SaveExecutionState(ctx context.Context, run *workflow.WorkflowRun, stepID string) error {
	// Update cache first so subsequent reads in the same execution see the latest state.
	if err := c.cache.SaveExecutionState(ctx, run, stepID); err != nil {
		return err
	}
	// Then persist to disk for durability.
	return c.persist.SaveExecutionState(ctx, run, stepID)
}

// ListRunsByStatus 以持久化层为准（缓存可能未完整加载），
// 用于启动恢复扫描。
func (c *CachedStore) ListRunsByStatus(ctx context.Context, statuses ...workflow.RunStatus) ([]string, error) {
	return c.persist.ListRunsByStatus(ctx, statuses...)
}

// Close 关闭持久化层，缓存层是 no-op。
func (c *CachedStore) Close() error {
	_ = c.cache.Close()
	return c.persist.Close()
}
