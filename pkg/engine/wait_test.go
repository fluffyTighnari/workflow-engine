package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestWaitRunBlocksUntilTerminal(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	run, def := createRunForPipeline(t, e)

	// Execute in the background; WaitRun should observe SUCCEEDED.
	go func() {
		_ = e.RunWorkflow(ctx, def, run)
	}()

	loaded, err := e.WaitRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("wait run: %v", err)
	}
	if loaded.Status != workflow.RunSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", loaded.Status)
	}
	if got := loaded.StepExecutions["B"].Output["result"]; got != float64(10) {
		t.Fatalf("B result = %v, want 10", got)
	}
}

func TestWaitRunContextCancel(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	run, _ := createRunForPipeline(t, e)

	oldInterval := WaitRunPollInterval
	WaitRunPollInterval = 5 * time.Millisecond
	defer func() { WaitRunPollInterval = oldInterval }()

	// No one executes the run: it stays PENDING and WaitRun must
	// return ctx.Err when the context expires.
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err := e.WaitRun(cctx, run.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestWaitRunNotFound(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)

	_, err := e.WaitRun(ctx, "no-such-run")
	if err == nil {
		t.Fatal("expected error for missing run")
	}
}
