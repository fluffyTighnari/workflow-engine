package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
)

func TestPoolRecordsJobMetrics(t *testing.T) {
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

	m := metrics.New()
	e.SetMetrics(m)

	q := queue.NewBoundedQueue(8)
	pool := NewPool(q, e, 2, NewRunLocker())
	pool.SetLogger(quietLogger())
	pool.SetMetrics(m)
	pool.Start()
	defer pool.Stop()

	var releaseOnce sync.Once
	releaseAll := func() {
		releaseOnce.Do(func() { close(executor.release) })
	}
	defer releaseAll()

	for i := 0; i < 2; i++ {
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

	deadline := time.Now().Add(3 * time.Second)
	for {
		if m.JobsDuplicate.Snapshot()["job_type=execute"] >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("duplicate job was not recorded")
		}
		time.Sleep(5 * time.Millisecond)
	}

	releaseAll()
	waitForTerminal(t, e, run.ID, 3*time.Second)

	snapshot := m.Snapshot()
	if snapshot.JobsStarted["job_type=execute"] != 1 {
		t.Errorf("jobs started = %v", snapshot.JobsStarted)
	}
	if snapshot.JobsFinished["job_type=execute,status=succeeded"] != 1 {
		t.Errorf("jobs finished = %v", snapshot.JobsFinished)
	}
	if snapshot.JobsDuplicate["job_type=execute"] < 1 {
		t.Errorf("jobs duplicate = %v", snapshot.JobsDuplicate)
	}
	if snapshot.JobDuration["job_type=execute"].Count != 1 {
		t.Errorf("job duration = %v", snapshot.JobDuration)
	}
	if snapshot.RunQueueWait.Count != 1 {
		t.Errorf("run queue wait = %+v", snapshot.RunQueueWait)
	}
	if snapshot.WorkersTotal != 2 {
		t.Errorf("workers total = %d, want 2", snapshot.WorkersTotal)
	}
	if snapshot.WorkersBusy != 0 {
		t.Errorf("workers busy = %d, want 0", snapshot.WorkersBusy)
	}
}
