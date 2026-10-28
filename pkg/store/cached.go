package store

import (
	"context"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// CachedStore 是一个具体的、自包含的存储实现，内部组合：
//   - MemoryStore（缓存层，快速访问）
//   - SQLiteStore（持久化层，跨重启保留）
//
// 读写策略（单实例模式）：
//   - 所有写操作只写持久化层，随后废弃本实例对应的缓存条目
//   - 缓存只在读取 miss 时从持久化层回填
//   - 不做跨实例/跨进程缓存失效，也不设置 TTL
//
// 因此 CachedStore 只支持单实例使用：同一个 SQLite 文件不能被多个
// CachedStore 或进程同时写入。共享数据库会导致其他实例读到本地旧缓存。
type CachedStore struct {
	cache   *MemoryStore // 缓存层
	persist *SQLiteStore // 持久化层
}

// NewCachedStore 创建一个 CachedStore，内部同时初始化 MemoryStore 和
// SQLiteStore。path 是 SQLite 数据库文件路径。
//
// 同一个数据库文件只应由一个 CachedStore 实例使用；当前不支持多实例
// 或跨进程共享写入。
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

// SaveDefinition 只写持久化层；成功后废弃对应的定义缓存。
func (c *CachedStore) SaveDefinition(ctx context.Context, def workflow.WorkflowDefinition) error {
	if err := c.persist.SaveDefinition(ctx, def); err != nil {
		return err
	}

	c.cache.invalidateDefinition(def.ID, def.Version)
	return nil
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

// CreateRun 只写持久化层；成功后废弃对应的 Run 缓存。
func (c *CachedStore) CreateRun(ctx context.Context, def workflow.WorkflowDefinition, run *workflow.WorkflowRun) error {
	if err := c.persist.CreateRun(ctx, def, run); err != nil {
		return err
	}

	c.cache.invalidateRun(run.ID)
	return nil
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

// SaveExecutionState 只写持久化层；成功后废弃对应的 Run 缓存。
//
// 持久化是唯一权威来源。缓存不会在写入路径上被更新，后续读取 miss 时
// 才从持久化层重新加载最新状态。
func (c *CachedStore) SaveExecutionState(ctx context.Context, run *workflow.WorkflowRun, stepID string) error {
	if err := c.persist.SaveExecutionState(ctx, run, stepID); err != nil {
		return err
	}

	c.cache.invalidateRun(run.ID)
	return nil
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
