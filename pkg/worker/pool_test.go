package worker

import (
	"context"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func newPoolTestEngine(t *testing.T) *engine.Engine {
	t.Helper()
	e := engine.NewMemory()
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func poolDefinition(stepType string) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID:      "pool-test",
		Version: 1,
		Steps:   []workflow.Step{{ID: "A", Type: stepType}},
	}
}

func quietLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func newBlockingExecutor() *blockingExecutor {
	return &blockingExecutor{
		started: make(chan struct{}, 64),
		release: make(chan struct{}),
	}
}

func (e *blockingExecutor) Execute(
	context.Context,
	workflow.Step,
	map[string]any,
) (map[string]any, error) {
	e.calls.Add(1)
	e.started <- struct{}{}
	<-e.release
	return map[string]any{"result": float64(1)}, nil
}

func TestPoolRunsDifferentRunsConcurrently(t *testing.T) {
	ctx := context.Background()
	e := newPoolTestEngine(t)
	executor := newBlockingExecutor()
	if err := e.RegisterExecutor("block", executor); err != nil {
		t.Fatal(err)
	}

	def := poolDefinition("block")
	run1, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}
	run2, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	q := queue.NewBoundedQueue(8)
	pool := NewPool(q, e, 2, NewRunLocker())
	pool.SetLogger(quietLogger())
	pool.Start()
	defer pool.Stop()

	var releaseOnce sync.Once
	releaseAll := func() {
		releaseOnce.Do(func() { close(executor.release) })
	}
	defer releaseAll()

	for _, run := range []*workflow.WorkflowRun{run1, run2} {
		if err := q.Enqueue(ctx, queue.Job{
			ID:    run.ID,
			RunID: run.ID,
			Type:  queue.JobExecute,
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case <-executor.started:
		case <-time.After(3 * time.Second):
			t.Fatal("different runs did not start concurrently")
		}
	}

	releaseAll()

	waitForTerminal(t, e, run1.ID, 3*time.Second)
	waitForTerminal(t, e, run2.ID, 3*time.Second)
	if got := executor.calls.Load(); got != 2 {
		t.Fatalf("executor calls = %d, want 2", got)
	}
}

func TestPoolDoesNotRunSameRunTwice(t *testing.T) {
	ctx := context.Background()
	e := newPoolTestEngine(t)
	executor := newBlockingExecutor()
	if err := e.RegisterExecutor("block", executor); err != nil {
		t.Fatal(err)
	}

	def := poolDefinition("block")
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	q := queue.NewBoundedQueue(64)
	pool := NewPool(q, e, 4, NewRunLocker())
	pool.SetLogger(quietLogger())
	pool.Start()
	defer pool.Stop()

	var releaseOnce sync.Once
	releaseAll := func() {
		releaseOnce.Do(func() { close(executor.release) })
	}
	defer releaseAll()

	for i := 0; i < 20; i++ {
		if err := q.Enqueue(ctx, queue.Job{
			ID:    run.ID,
			RunID: run.ID,
			Type:  queue.JobExecute,
		}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-executor.started:
	case <-time.After(3 * time.Second):
		t.Fatal("run did not start")
	}

	// Give duplicate jobs time to hit the busy lock, then let the owner finish.
	time.Sleep(100 * time.Millisecond)
	releaseAll()

	waitForTerminal(t, e, run.ID, 3*time.Second)
	if got := executor.calls.Load(); got != 1 {
		t.Fatalf("executor calls = %d, want 1", got)
	}
}

func TestPoolDoesNotDuplicateExecuteAndRecover(t *testing.T) {
	ctx := context.Background()
	e := newPoolTestEngine(t)
	executor := newBlockingExecutor()
	if err := e.RegisterExecutor("block", executor); err != nil {
		t.Fatal(err)
	}

	def := poolDefinition("block")
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	q := queue.NewBoundedQueue(8)
	pool := NewPool(q, e, 2, NewRunLocker())
	pool.SetLogger(quietLogger())
	pool.Start()
	defer pool.Stop()

	var releaseOnce sync.Once
	releaseAll := func() {
		releaseOnce.Do(func() { close(executor.release) })
	}
	defer releaseAll()

	if err := q.Enqueue(ctx, queue.Job{
		ID:    run.ID,
		RunID: run.ID,
		Type:  queue.JobExecute,
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, queue.Job{
		ID:    run.ID,
		RunID: run.ID,
		Type:  queue.JobRecover,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-executor.started:
	case <-time.After(3 * time.Second):
		t.Fatal("run did not start")
	}

	time.Sleep(100 * time.Millisecond)
	releaseAll()

	waitForTerminal(t, e, run.ID, 3*time.Second)
	if got := executor.calls.Load(); got != 1 {
		t.Fatalf("executor calls = %d, want 1", got)
	}
}

type panicOnceExecutor struct {
	calls atomic.Int32
	first chan struct{}
}

func (e *panicOnceExecutor) Execute(
	context.Context,
	workflow.Step,
	map[string]any,
) (map[string]any, error) {
	if e.calls.Add(1) == 1 {
		close(e.first)
		panic("executor panic")
	}
	return map[string]any{"result": float64(1)}, nil
}

func TestPoolRecoversPanicAndReleasesLock(t *testing.T) {
	ctx := context.Background()
	e := newPoolTestEngine(t)
	executor := &panicOnceExecutor{first: make(chan struct{})}
	if err := e.RegisterExecutor("panic-once", executor); err != nil {
		t.Fatal(err)
	}

	def := poolDefinition("panic-once")
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	locker := NewRunLocker()
	q := queue.NewBoundedQueue(8)
	pool := NewPool(q, e, 2, locker)
	pool.SetLogger(quietLogger())
	pool.Start()
	defer pool.Stop()

	if err := q.Enqueue(ctx, queue.Job{
		ID:    run.ID,
		RunID: run.ID,
		Type:  queue.JobExecute,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-executor.first:
	case <-time.After(3 * time.Second):
		t.Fatal("executor did not panic")
	}

	// The worker's panic recovery must release the run lock.
	deadline := time.Now().Add(3 * time.Second)
	for {
		release, ok := locker.TryAcquire(run.ID)
		if ok {
			release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run lock was not released after panic")
		}
		time.Sleep(5 * time.Millisecond)
	}

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
	if got := executor.calls.Load(); got != 2 {
		t.Fatalf("executor calls = %d, want 2", got)
	}
}
