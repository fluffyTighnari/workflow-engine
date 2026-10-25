package worker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// testEngine 创建带 SQLite 持久化的引擎（含缓存层）。
func testEngine(t *testing.T) *engine.Engine {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.db")
	e, err := engine.NewSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// testDef 定义 A(compute 2+3=5) → B(double 5*2=10)。
func testDef() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID:      "pipeline",
		Version: 1,
		Steps: []workflow.Step{
			{
				ID:     "A",
				Type:   "compute",
				Config: map[string]string{"a": "2", "b": "3"},
			},
			{
				ID:        "B",
				Type:      "double",
				DependsOn: []string{"A"},
				Inputs: map[string]workflow.InputRef{
					"value": {Source: "A", Key: "result"},
				},
			},
		},
	}
}

// waitForTerminal 轮询 run 直到终态或超时。
func waitForTerminal(t *testing.T, e *engine.Engine, runID string, timeout time.Duration) *workflow.WorkflowRun {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := e.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("get run: %v", err)
		}
		if run.Status == workflow.RunSucceeded || run.Status == workflow.RunFailed {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %s not terminal within %v", runID, timeout)
	return nil
}

func TestWorkerExecutesEnqueuedRun(t *testing.T) {
	e := testEngine(t)
	q := queue.NewBoundedQueue(4)
	w := New(q, e)
	w.Start()
	defer w.Stop()
	defer q.Close()

	ctx := context.Background()
	run, err := e.CreateRun(ctx, testDef(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := q.Enqueue(ctx, queue.Job{
		ID:    run.ID,
		RunID: run.ID,
		Type:  queue.JobExecute,
	}); err != nil {
		t.Fatal(err)
	}

	done := waitForTerminal(t, e, run.ID, 3*time.Second)
	if done.Status != workflow.RunSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", done.Status)
	}
	if got := done.StepExecutions["A"].Output["result"]; got != float64(5) {
		t.Errorf("A result = %v, want 5", got)
	}
	if got := done.StepExecutions["B"].Output["result"]; got != float64(10) {
		t.Errorf("B result = %v, want 10", got)
	}
}

func TestWorkerSkipsTerminalRun(t *testing.T) {
	e := testEngine(t)
	q := queue.NewBoundedQueue(4)
	w := New(q, e)
	w.Start()
	defer w.Stop()
	defer q.Close()

	ctx := context.Background()
	run, err := e.CreateRun(ctx, testDef(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// 先直接同步跑完，让 run 进入终态。
	def := testDef()
	if err := e.RunWorkflow(ctx, def, run); err != nil {
		t.Fatal(err)
	}
	if run.Status != workflow.RunSucceeded {
		t.Fatalf("precondition: status = %s", run.Status)
	}

	// 再投递 execute job：应被跳过，状态不变。
	if err := q.Enqueue(ctx, queue.Job{
		ID:    run.ID,
		RunID: run.ID,
		Type:  queue.JobExecute,
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	reloaded, err := e.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != workflow.RunSucceeded {
		t.Errorf("status = %s, want still SUCCEEDED", reloaded.Status)
	}
	if got := reloaded.StepExecutions["A"].Output["result"]; got != float64(5) {
		t.Errorf("A result = %v, want 5 (must not re-execute)", got)
	}
}

func TestWorkerRecoversCrashedRun(t *testing.T) {
	e := testEngine(t)
	q := queue.NewBoundedQueue(4)
	w := New(q, e)
	w.Start()
	defer w.Stop()
	defer q.Close()

	ctx := context.Background()
	run, err := e.CreateRun(ctx, testDef(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// 模拟崩溃残留：A 已成功，B 停在 RUNNING，run 停在 RUNNING。
	run.StepExecutions["A"].Status = workflow.StepSucceeded
	run.StepExecutions["A"].Output = map[string]any{"result": float64(5)}
	now := time.Now().UTC()
	run.StepExecutions["A"].StartedAt = &now
	run.StepExecutions["A"].FinishedAt = &now

	run.StepExecutions["B"].Status = workflow.StepRunning
	run.StepExecutions["B"].StartedAt = &now

	run.Status = workflow.RunRunning
	run.UpdatedAt = now

	// 直接把崩溃状态写入 store（模拟 DB 中的残留）。
	if err := e.SaveExecutionState(ctx, run, "B"); err != nil {
		t.Fatal(err)
	}

	// 投递 recover job。
	if err := q.Enqueue(ctx, queue.Job{
		ID:    run.ID,
		RunID: run.ID,
		Type:  queue.JobRecover,
	}); err != nil {
		t.Fatal(err)
	}

	done := waitForTerminal(t, e, run.ID, 3*time.Second)
	if done.Status != workflow.RunSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", done.Status)
	}
	if got := done.StepExecutions["B"].Output["result"]; got != float64(10) {
		t.Errorf("B result = %v, want 10", got)
	}
}

func TestWorkerExecutesRunningRunViaRecover(t *testing.T) {
	e := testEngine(t)
	q := queue.NewBoundedQueue(4)
	w := New(q, e)
	w.Start()
	defer w.Stop()
	defer q.Close()

	ctx := context.Background()
	run, err := e.CreateRun(ctx, testDef(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// 模拟 run 处于 RUNNING 但没有已成功步骤（例如持久化 RUNNING 后崩溃）。
	now := time.Now().UTC()
	run.Status = workflow.RunRunning
	run.UpdatedAt = now
	if err := e.SaveExecutionState(ctx, run, "A"); err != nil {
		t.Fatal(err)
	}

	// execute job 遇到 RUNNING 应自动转 recover。
	if err := q.Enqueue(ctx, queue.Job{
		ID:    run.ID,
		RunID: run.ID,
		Type:  queue.JobExecute,
	}); err != nil {
		t.Fatal(err)
	}

	done := waitForTerminal(t, e, run.ID, 3*time.Second)
	if done.Status != workflow.RunSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", done.Status)
	}
}
