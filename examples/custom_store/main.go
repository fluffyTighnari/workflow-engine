// 自定义存储示例：实现 store.WorkflowStore 接口（JSON 文件后端），
// 并通过 engine.NewWithStore 注入引擎，全程不修改引擎源码。
//
// 演示集成方如何接入自己的持久化（例如 PostgreSQL、云存储等）：
// 只需要实现 7 个方法的 WorkflowStore 接口，再传给 NewWithStore。
//
// 运行：go run ./examples/custom_store
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/store"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// FileStore 是一个极简的 JSON 文件存储，演示如何实现 WorkflowStore。
// 生产环境可换成 PostgreSQL / Redis / 云数据库等任意后端。
type FileStore struct {
	mu   sync.Mutex
	dir  string
	runs map[string]*workflow.WorkflowRun // runID → run（含全量状态）
	byID map[string][]int                 // workflowID → versions
	vers map[string]map[int]workflow.WorkflowDefinition
}

// NewFileStore 在 dir 下创建一个文件存储。
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{
		dir:  dir,
		runs: make(map[string]*workflow.WorkflowRun),
		byID: make(map[string][]int),
		vers: make(map[string]map[int]workflow.WorkflowDefinition),
	}, nil
}

func (f *FileStore) file(id string) string {
	return filepath.Join(f.dir, id+".json")
}

// --- WorkflowStore 接口实现 ---

func (f *FileStore) SaveDefinition(ctx context.Context, def workflow.WorkflowDefinition) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.vers[def.ID] == nil {
		f.vers[def.ID] = make(map[int]workflow.WorkflowDefinition)
	}
	f.vers[def.ID][def.Version] = def

	// 持久化定义文件
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	return os.WriteFile(f.file("def_"+def.ID+"_"+fmt.Sprint(def.Version)), data, 0o644)
}

func (f *FileStore) GetDefinition(ctx context.Context, workflowID string, version int) (workflow.WorkflowDefinition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.vers[workflowID] == nil {
		return workflow.WorkflowDefinition{}, store.ErrNotFound
	}
	def, ok := f.vers[workflowID][version]
	if !ok {
		return workflow.WorkflowDefinition{}, store.ErrNotFound
	}
	return def, nil
}

func (f *FileStore) CreateRun(ctx context.Context, def workflow.WorkflowDefinition, run *workflow.WorkflowRun) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 定义必须先保存（与 SQLite 行为一致）
	if f.vers[def.ID] == nil || f.vers[def.ID][def.Version].ID == "" {
		return fmt.Errorf("workflow version must be saved first")
	}

	// 深拷贝，避免外部修改污染内部状态
	cp := cloneRun(run)
	f.runs[run.ID] = cp

	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return os.WriteFile(f.file(run.ID), data, 0o644)
}

func (f *FileStore) GetRun(ctx context.Context, runID string) (*workflow.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	run, ok := f.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q: %w", runID, store.ErrNotFound)
	}
	return cloneRun(run), nil
}

func (f *FileStore) ListRunsByStatus(ctx context.Context, statuses ...workflow.RunStatus) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	wanted := make(map[workflow.RunStatus]bool, len(statuses))
	for _, st := range statuses {
		wanted[st] = true
	}

	var ids []string
	for id, run := range f.runs {
		if wanted[run.Status] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (f *FileStore) SaveExecutionState(ctx context.Context, run *workflow.WorkflowRun, stepID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.runs[run.ID]; !ok {
		return fmt.Errorf("run %q: %w", run.ID, store.ErrNotFound)
	}
	if _, ok := run.StepExecutions[stepID]; !ok {
		return fmt.Errorf("step execution %q not found", stepID)
	}

	f.runs[run.ID] = cloneRun(run)
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}
	return os.WriteFile(f.file(run.ID), data, 0o644)
}

func (f *FileStore) Close() error { return nil }

// cloneRun 深拷贝（示例简化实现，生产环境可复用 store 的拷贝逻辑）。
func cloneRun(run *workflow.WorkflowRun) *workflow.WorkflowRun {
	if run == nil {
		return nil
	}
	cp := *run
	cp.StepExecutions = make(map[string]*workflow.StepExecution, len(run.StepExecutions))
	for k, v := range run.StepExecutions {
		if v == nil {
			continue
		}
		vc := *v
		cp.StepExecutions[k] = &vc
	}
	return &cp
}

func main() {
	ctx := context.Background()
	dir, _ := os.MkdirTemp("", "wf_custom_store_*")
	defer os.RemoveAll(dir)

	// 1. 自定义存储实现 WorkflowStore
	fs, err := NewFileStore(dir)
	if err != nil {
		panic(err)
	}

	// 2. 注入引擎（NewWithStore 接受任意 WorkflowStore 实现）
	eng := engine.NewWithStore(fs)
	defer eng.Close()

	// 3. 注册自定义 executor 并提交工作流
	if err := eng.RegisterExecutor("greet", &GreetExecutor{}); err != nil {
		panic(err)
	}

	def := workflow.WorkflowDefinition{
		ID:      "custom-demo",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "g", Type: "greet", Inputs: map[string]workflow.InputRef{
				"name": {Source: "workflow_input", Key: "name"},
			}},
		},
	}

	run, err := eng.CreateRun(ctx, def, map[string]any{"name": "world"})
	if err != nil {
		panic(err)
	}
	if err := eng.RunWorkflow(ctx, def, run); err != nil {
		panic(err)
	}

	loaded, err := eng.GetRun(ctx, run.ID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("custom store -> run=%s status=%s output=%v\n",
		loaded.ID, loaded.Status, loaded.StepExecutions["g"].Output)

	if loaded.Status != workflow.RunSucceeded {
		fmt.Println("FAIL: expected SUCCEEDED")
		os.Exit(1)
	}
	fmt.Println("OK: custom store integrated without modifying engine source")
}

// GreetExecutor 是外部实现的自定义执行器。
type GreetExecutor struct{}

func (e *GreetExecutor) Execute(ctx context.Context, step workflow.Step,
	input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	return map[string]any{"message": "hello, " + name}, nil
}
