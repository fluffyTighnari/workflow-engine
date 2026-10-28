// 异步引擎集成示例：使用 pkg/async 一键异步执行。
//
// 相比手动组装 queue + worker，async.AsyncEngine 开箱即用：
//   - SubmitRun：持久化 run 并入队（返回 run ID）
//   - WaitRun：阻塞等待执行到终态
//   - RecoverUnfinished：进程启动时扫描并恢复未完成任务
//
// 运行：go run ./examples/async
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/async"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// SleepExecutor 演示一个耗时步骤：异步场景下能观察到 run 处于
// 非终态（RUNNING/PENDING），随后被 worker 执行完成。
type SleepExecutor struct{}

func (e *SleepExecutor) Execute(ctx context.Context, step workflow.Step,
	input map[string]any) (map[string]any, error) {
	select {
	case <-time.After(300 * time.Millisecond):
		return map[string]any{"done": true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func main() {
	ctx := context.Background()
	db := filepath.Join(os.TempDir(), "wf_async_example.db")
	_ = os.Remove(db)

	// 1. 创建异步引擎（SQLite 持久化，队列容量 16）
	a, err := async.NewSQLite(db, 16)
	if err != nil {
		panic(err)
	}
	defer a.Close()

	// 2. 注册自定义 executor（通过底层 Engine）
	if err := a.Engine().RegisterExecutor("sleep", &SleepExecutor{}); err != nil {
		panic(err)
	}

	// 3. 启动崩溃恢复：扫描上次遗留的 PENDING/RUNNING 任务
	n, err := a.RecoverUnfinished(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("recovered %d unfinished run(s)\n", n)

	// 4. 提交工作流，拿到 Run ID（立即返回，worker 后台执行）
	def := workflow.WorkflowDefinition{
		ID:      "async-demo",
		Version: 1,
		Steps:   []workflow.Step{{ID: "s", Type: "sleep"}},
	}
	runID, err := a.SubmitRun(ctx, def, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("submitted run_id=%s (async)\n", runID)

	// 5. 立即查询：大概率还在执行中（RUNNING/PENDING）
	early, err := a.GetRun(ctx, runID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("immediately after submit: status=%s\n", early.Status)

	// 6. 阻塞等待终态
	run, err := a.WaitRun(ctx, runID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("after wait: status=%s output=%v\n",
		run.Status, run.StepExecutions["s"].Output)

	if run.Status != workflow.RunSucceeded {
		fmt.Println("FAIL: expected SUCCEEDED")
		os.Exit(1)
	}
	fmt.Println("OK: async engine integrated without modifying engine source")
}
