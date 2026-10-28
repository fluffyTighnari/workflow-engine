package engine

import (
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/logging"
	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
	"github.com/fluffyTighnari/workflow-engine/pkg/store"
)

// newEngineWithStore 创建一个带内置执行器的引擎，使用指定的存储后端。
func newEngineWithStore(s store.WorkflowStore) *Engine {
	registry := NewExecutorRegistry()
	_ = registry.Register("compute", &ComputeExecutor{})
	_ = registry.Register("double", &DoubleExecutor{})
	return &Engine{
		registry: registry,
		store:    s,
		logger:   logging.Default(),
		metrics:  metrics.New(),
	}
}

// NewSQLite 返回一个带缓存层的持久化引擎。
// 内部组合 MemoryStore（缓存）和 SQLiteStore（持久化）。
// path 是 SQLite 数据库文件路径。
//
// 缓存只支持单实例：同一个数据库文件不要被多个引擎/进程同时写入。
func NewSQLite(path string) (*Engine, error) {
	cs, err := store.NewCachedStore(path)
	if err != nil {
		return nil, fmt.Errorf("open cached store: %w", err)
	}
	return newEngineWithStore(cs), nil
}

// NewMemory 返回一个纯内存引擎，无持久化层。
// 用于测试和临时工作流，进程退出后数据全部丢失。
func NewMemory() *Engine {
	return newEngineWithStore(store.NewMemoryStore())
}
