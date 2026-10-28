// 外部集成验证：独立 module，不修改引擎源码。
// 验证：import 引擎 → 注册自定义 Executor → 提交工作流 → 执行 → 查询结果。
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// SendEmailExecutor 是集成方自定义的执行器（不在引擎源码内）。
type SendEmailExecutor struct{}

func (e *SendEmailExecutor) Execute(ctx context.Context, step workflow.Step,
	input map[string]any) (map[string]any, error) {
	return map[string]any{"sent": true, "to": input["to"]}, nil
}

func main() {
	ctx := context.Background()

	// 1. 创建引擎（内存即可，验证集成不依赖 SQLite）
	eng := engine.NewMemory()
	defer eng.Close()

	// 2. 注册自定义 Executor
	if err := eng.RegisterExecutor("send_email", &SendEmailExecutor{}); err != nil {
		fmt.Println("register executor failed:", err)
		os.Exit(1)
	}

	// 3. 提交工作流定义，获取 Run ID
	def := workflow.WorkflowDefinition{
		ID:      "notify",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "s1", Type: "send_email", Inputs: map[string]workflow.InputRef{
				"to": {Source: "workflow_input", Key: "email"},
			}},
		},
	}

	run, err := eng.CreateRun(ctx, def, map[string]any{"email": "user@example.com"})
	if err != nil {
		fmt.Println("create run failed:", err)
		os.Exit(1)
	}
	fmt.Println("run_id:", run.ID, "status:", run.Status)

	// 4. 执行
	if err := eng.RunWorkflow(ctx, def, run); err != nil {
		fmt.Println("run workflow failed:", err)
		os.Exit(1)
	}

	// 5. 通过 Run ID 查询执行结果
	loaded, err := eng.GetRun(ctx, run.ID)
	if err != nil {
		fmt.Println("get run failed:", err)
		os.Exit(1)
	}
	fmt.Println("final status:", loaded.Status)
	for id, se := range loaded.StepExecutions {
		fmt.Printf("step %s: status=%s output=%v\n", id, se.Status, se.Output)
	}

	if loaded.Status != workflow.RunSucceeded {
		fmt.Println("FAIL: expected SUCCEEDED")
		os.Exit(1)
	}
	if loaded.StepExecutions["s1"].Output["sent"] != true {
		fmt.Println("FAIL: custom executor output not visible")
		os.Exit(1)
	}
	fmt.Println("OK: external integration works without modifying engine source")
}
