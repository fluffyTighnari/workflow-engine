package async

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func asyncDef() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID:      "async-demo",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "A", Type: "compute", Config: map[string]string{"a": "2", "b": "3"}},
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

func TestSubmitRunAndWaitTerminal(t *testing.T) {
	ctx := context.Background()
	a := NewMemory(16)
	defer a.Close()

	runID, err := a.SubmitRun(ctx, asyncDef(), nil)
	if err != nil {
		t.Fatalf("submit run: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}

	run, err := a.WaitRun(ctx, runID)
	if err != nil {
		t.Fatalf("wait run: %v", err)
	}
	if run.Status != workflow.RunSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", run.Status)
	}
	if got := run.StepExecutions["B"].Output["result"]; got != float64(10) {
		t.Fatalf("B result = %v, want 10", got)
	}
}

func TestRecoverUnfinishedReexecutes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recover.db")
	a, err := NewSQLite(path, 16)
	if err != nil {
		t.Fatalf("new sqlite async engine: %v", err)
	}
	defer a.Close()

	// Create a run without enqueueing it (simulating a crash before
	// the execution job was consumed).
	def := asyncDef()
	run, err := a.Engine().CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	n, err := a.RecoverUnfinished(ctx)
	if err != nil {
		t.Fatalf("recover unfinished: %v", err)
	}
	if n != 1 {
		t.Fatalf("re-enqueued %d runs, want 1", n)
	}

	loaded, err := a.WaitRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("wait run after recover: %v", err)
	}
	if loaded.Status != workflow.RunSucceeded {
		t.Fatalf("status after recover = %q, want SUCCEEDED", loaded.Status)
	}
}

func TestCloseRejectsSubmit(t *testing.T) {
	ctx := context.Background()
	a := NewMemory(16)
	a.Close()

	_, err := a.SubmitRun(ctx, asyncDef(), nil)
	if !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestWaitRunRespectsContext(t *testing.T) {
	ctx := context.Background()
	a := NewMemory(1)
	defer a.Close()

	run, err := a.Engine().CreateRun(ctx, asyncDef(), nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	cctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, err = a.WaitRun(cctx, run.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestNewMemoryWithWorkers(t *testing.T) {
	ctx := context.Background()
	a := NewMemoryWithOptions(16, WithWorkers(4))
	defer a.Close()

	if got := a.WorkerCount(); got != 4 {
		t.Fatalf("WorkerCount = %d, want 4", got)
	}

	runID, err := a.SubmitRun(ctx, asyncDef(), nil)
	if err != nil {
		t.Fatalf("submit run: %v", err)
	}

	run, err := a.WaitRun(ctx, runID)
	if err != nil {
		t.Fatalf("wait run: %v", err)
	}
	if run.Status != workflow.RunSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", run.Status)
	}
}

func TestAsyncMetricsSnapshot(t *testing.T) {
	ctx := context.Background()
	a := NewMemoryWithOptions(8, WithWorkers(2))
	defer a.Close()

	runID, err := a.SubmitRun(ctx, asyncDef(), nil)
	if err != nil {
		t.Fatalf("submit run: %v", err)
	}
	if _, err := a.WaitRun(ctx, runID); err != nil {
		t.Fatalf("wait run: %v", err)
	}

	snapshot := a.MetricsSnapshot()
	if snapshot.RunsCreated != 1 {
		t.Errorf("runs created = %d, want 1", snapshot.RunsCreated)
	}
	if snapshot.RunsFinished["status=SUCCEEDED"] != 1 {
		t.Errorf("runs finished = %v", snapshot.RunsFinished)
	}
	if snapshot.Submissions["result=accepted"] != 1 {
		t.Errorf("submissions = %v", snapshot.Submissions)
	}
	if snapshot.WorkersTotal != 2 {
		t.Errorf("workers total = %d, want 2", snapshot.WorkersTotal)
	}
	if snapshot.QueueDepth != 0 {
		t.Errorf("queue depth = %d, want 0", snapshot.QueueDepth)
	}
}
