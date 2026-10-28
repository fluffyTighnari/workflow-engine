// 外部集成验证（SQLite 持久化场景）：
// 验证外部项目可用 NewSQLite 持久化 workflow + run，
// 关闭引擎后重新打开，通过 Run ID 仍能查到结果。
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

type UpperExecutor struct{}

func (e *UpperExecutor) Execute(ctx context.Context, step workflow.Step,
	input map[string]any) (map[string]any, error) {
	s, _ := input["value"].(string)
	return map[string]any{"result": upper(s)}, nil
}

func upper(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'a' && b[i] <= 'z' {
			b[i] -= 32
		}
	}
	return string(b)
}

func main() {
	ctx := context.Background()
	db := filepath.Join(os.TempDir(), "wf_integ_sqlite.db")
	_ = os.Remove(db)

	// 第一段：写入 + 执行
	eng, err := engine.NewSQLite(db)
	if err != nil {
		fmt.Println("new sqlite engine:", err)
		os.Exit(1)
	}
	if err := eng.RegisterExecutor("upper", &UpperExecutor{}); err != nil {
		fmt.Println("register:", err)
		os.Exit(1)
	}
	def := workflow.WorkflowDefinition{
		ID:      "demo2",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "u", Type: "upper", Inputs: map[string]workflow.InputRef{
				"value": {Source: "workflow_input", Key: "name"},
			}},
		},
	}
	run, err := eng.CreateRun(ctx, def, map[string]any{"name": "hello"})
	if err != nil {
		fmt.Println("create run:", err)
		os.Exit(1)
	}
	fmt.Println("run_id:", run.ID)
	if err := eng.RunWorkflow(ctx, def, run); err != nil {
		fmt.Println("run workflow:", err)
		os.Exit(1)
	}
	eng.Close() // 模拟进程退出

	// 第二段：重新打开引擎（模拟重启），按 Run ID 查询
	eng2, err := engine.NewSQLite(db)
	if err != nil {
		fmt.Println("reopen engine:", err)
		os.Exit(1)
	}
	defer eng2.Close()

	loaded, err := eng2.GetRun(ctx, run.ID)
	if err != nil {
		fmt.Println("get run after reopen:", err)
		os.Exit(1)
	}
	fmt.Println("after reopen -> status:", loaded.Status,
		"output:", loaded.StepExecutions["u"].Output)

	if loaded.Status != workflow.RunSucceeded ||
		loaded.StepExecutions["u"].Output["result"] != "HELLO" {
		fmt.Println("FAIL: SQLite persistence across reopen")
		os.Exit(1)
	}
	_ = os.Remove(db)
	fmt.Println("OK: external integration with SQLite persistence works")
}
