// Package async provides an out-of-the-box asynchronous workflow engine.
//
// It assembles engine.Engine, a bounded in-process queue, and a worker
// pool so integrators only need:
//
//	a, _ := async.NewSQLite("app.db", 100)     // or async.NewMemory(100)
//	a, _ := async.NewSQLiteWithOptions("app.db", 100, async.WithWorkers(4))
//	a.Engine().RegisterExecutor("my_type", myExecutor)
//	runID, _ := a.SubmitRun(ctx, def, input)
//	run, _ := a.WaitRun(ctx, runID)
//
// Semantics follow the engine's design decisions:
//   - SubmitRun persists the definition and the run (PENDING), then enqueues
//     the execution job; a full queue blocks (backpressure, no lost jobs).
//   - WorkerCount workers consume jobs concurrently. Different runs may run
//     in parallel; the pool's RunLocker guarantees one run is never processed
//     by two workers at the same time.
//   - Duplicate delivery is safe: terminal runs are skipped and duplicate
//     jobs for an in-flight run are dropped.
//   - RecoverUnfinished scans PENDING/RUNNING runs and re-enqueues recover
//     jobs; call it at process startup for crash recovery.
//
// The current implementation is process-local and does not coordinate
// workers in different processes.
package async

import (
	"context"
	"fmt"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/logging"
	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
	"github.com/fluffyTighnari/workflow-engine/pkg/worker"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// Option configures an AsyncEngine.
type Option func(*options)

type options struct {
	workers int
	logger  logging.Logger
	metrics *metrics.Metrics
}

// WithLogger sets the logger used by the async engine and worker pool.
func WithLogger(logger logging.Logger) Option {
	return func(o *options) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// WithMetrics sets the metrics registry shared by the engine and pool.
func WithMetrics(m *metrics.Metrics) Option {
	return func(o *options) {
		if m != nil {
			o.metrics = m
		}
	}
}

// WithWorkers sets the number of workers. Values below 1 are ignored.
func WithWorkers(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.workers = n
		}
	}
}

func resolveOptions(opts ...Option) options {
	o := options{
		workers: 1,
		logger:  logging.Default(),
		metrics: metrics.New(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.workers < 1 {
		o.workers = 1
	}
	if o.logger == nil {
		o.logger = logging.Default()
	}
	if o.metrics == nil {
		o.metrics = metrics.New()
	}
	return o
}

// AsyncEngine is a ready-to-use asynchronous workflow engine:
// engine + bounded queue + worker pool, assembled and managed together.
type AsyncEngine struct {
	eng     *engine.Engine
	q       queue.Queue
	pool    *worker.Pool
	logger  logging.Logger
	metrics *metrics.Metrics
}

// NewSQLite creates an async engine backed by a CachedStore over SQLite
// with a single worker.
// queueCapacity is the in-process queue capacity; a full queue blocks
// SubmitRun (backpressure).
func NewSQLite(path string, queueCapacity int) (*AsyncEngine, error) {
	return NewSQLiteWithOptions(path, queueCapacity)
}

// NewSQLiteWithOptions is NewSQLite with configurable worker count.
func NewSQLiteWithOptions(
	path string,
	queueCapacity int,
	opts ...Option,
) (*AsyncEngine, error) {
	eng, err := engine.NewSQLite(path)
	if err != nil {
		return nil, err
	}
	return assemble(eng, queueCapacity, opts...), nil
}

// NewMemory creates an async engine with an in-memory store and a single
// worker. Data is lost when the process exits.
func NewMemory(queueCapacity int) *AsyncEngine {
	return NewMemoryWithOptions(queueCapacity)
}

// NewMemoryWithOptions is NewMemory with configurable worker count.
func NewMemoryWithOptions(
	queueCapacity int,
	opts ...Option,
) *AsyncEngine {
	return assemble(engine.NewMemory(), queueCapacity, opts...)
}

func assemble(
	eng *engine.Engine,
	queueCapacity int,
	opts ...Option,
) *AsyncEngine {
	cfg := resolveOptions(opts...)

	eng.SetMetrics(cfg.metrics)

	q := queue.NewBoundedQueue(queueCapacity)
	pool := worker.NewPool(q, eng, cfg.workers, worker.NewRunLocker())
	pool.SetStructuredLogger(cfg.logger)
	pool.SetMetrics(cfg.metrics)
	pool.Start()

	return &AsyncEngine{
		eng:     eng,
		q:       q,
		pool:    pool,
		logger:  cfg.logger,
		metrics: cfg.metrics,
	}
}

// Engine returns the underlying engine, used to register custom executors.
func (a *AsyncEngine) Engine() *engine.Engine { return a.eng }

// Queue returns the underlying queue, for callers that need to enqueue
// jobs directly.
func (a *AsyncEngine) Queue() queue.Queue { return a.q }

// WorkerCount returns the number of workers in the pool.
func (a *AsyncEngine) WorkerCount() int { return a.pool.Size() }

// Metrics returns the shared metrics registry.
func (a *AsyncEngine) Metrics() *metrics.Metrics { return a.metrics }

// MetricsSnapshot updates the queue gauge and returns a snapshot.
func (a *AsyncEngine) MetricsSnapshot() metrics.Snapshot {
	a.metrics.QueueDepth.Set(int64(a.q.Len()))
	return a.metrics.Snapshot()
}

// SubmitRun validates and persists a workflow definition and a new
// PENDING run, then enqueues the execution job. It returns the run ID.
// A full queue blocks until capacity frees up or ctx is done.
func (a *AsyncEngine) SubmitRun(
	ctx context.Context,
	def workflow.WorkflowDefinition,
	input map[string]any,
) (string, error) {
	run, err := a.eng.CreateRun(ctx, def, input)
	if err != nil {
		a.metrics.Submissions.Inc("rejected")
		a.logger.Error("run submission failed",
			"workflow_id", def.ID,
			"workflow_version", def.Version,
			"err", err,
		)
		return "", err
	}

	job := queue.Job{
		ID:        run.ID,
		RunID:     run.ID,
		Type:      queue.JobExecute,
		CreatedAt: time.Now().UTC(),
	}
	if err := a.q.Enqueue(ctx, job); err != nil {
		a.metrics.Submissions.Inc("enqueue_failed")
		wrapped := fmt.Errorf("enqueue run %s: %w", run.ID, err)
		a.logger.Error("run enqueue failed",
			"run_id", run.ID,
			"err", wrapped,
		)
		return "", wrapped
	}

	a.metrics.Submissions.Inc("accepted")
	a.logger.Info("run submitted",
		"run_id", run.ID,
		"workflow_id", def.ID,
		"workflow_version", def.Version,
	)
	return run.ID, nil
}

// GetRun loads the run (definition, status, step executions) by ID.
func (a *AsyncEngine) GetRun(
	ctx context.Context,
	runID string,
) (*workflow.WorkflowRun, error) {
	return a.eng.GetRun(ctx, runID)
}

// WaitRun blocks until the run reaches a terminal status or ctx is done.
func (a *AsyncEngine) WaitRun(
	ctx context.Context,
	runID string,
) (*workflow.WorkflowRun, error) {
	return a.eng.WaitRun(ctx, runID)
}

// RecoverUnfinished scans PENDING/RUNNING runs and enqueues a recover
// job for each, so the worker pool repairs and resumes them. Call it at
// process startup after creating the engine. It returns the number of
// runs re-enqueued.
func (a *AsyncEngine) RecoverUnfinished(ctx context.Context) (int, error) {
	ids, err := a.eng.ListRunsByStatus(ctx,
		workflow.RunPending, workflow.RunRunning)
	if err != nil {
		a.logger.Error("recovery scan failed", "err", err)
		return 0, err
	}

	enqueued := 0
	for _, id := range ids {
		job := queue.Job{
			ID:        id,
			RunID:     id,
			Type:      queue.JobRecover,
			CreatedAt: time.Now().UTC(),
		}
		if err := a.q.Enqueue(ctx, job); err != nil {
			wrapped := fmt.Errorf("enqueue recover job %s: %w", id, err)
			a.logger.Error("recovery job enqueue failed",
				"run_id", id,
				"err", wrapped,
			)
			return enqueued, wrapped
		}
		enqueued++
	}

	a.logger.Info("recovery scan finished",
		"unfinished", len(ids),
		"enqueued", enqueued,
	)
	return enqueued, nil
}

// Close gracefully shuts down: stops the worker pool (waiting for
// in-flight jobs), closes the queue, then closes the engine/store.
func (a *AsyncEngine) Close() error {
	a.logger.Info("async engine closing")
	a.pool.Stop()
	a.q.Close()
	err := a.eng.Close()
	if err != nil {
		a.logger.Error("async engine close failed", "err", err)
	}
	return err
}
