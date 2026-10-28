package worker

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/logging"
	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// Worker 是单个 goroutine 的消费者：
// 从队列取出 Job，按类型交给 handler 处理，处理完再取下一个。
//
// 语义：
//   - 同一个 Worker 串行处理 Job；
//   - 多个 Worker 可以并行处理不同 Run；
//   - 所有 Worker 通过 RunLocker 保证同一个 Run 同时只被一个 Worker 处理；
//   - 消费前一律从 store 重新加载 run，保证幂等（重复投递安全）；
//   - Stop 会取消阻塞的 Dequeue，并等待当前 Job 处理完毕。
type Worker struct {
	queue   queue.Queue
	engine  *engine.Engine
	locker  RunLocker
	logger  logging.Logger
	metrics *metrics.Metrics
	id      int

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New 创建一个带独立 RunLocker 的 worker。
func New(q queue.Queue, e *engine.Engine) *Worker {
	return NewWithLocker(q, e, NewRunLocker())
}

// NewWithLocker 创建一个共享 RunLocker 的 worker。多个 Worker 必须共享
// 同一个 RunLocker 才能保证同一个 Run 不会被并发执行。
func NewWithLocker(q queue.Queue, e *engine.Engine, locker RunLocker) *Worker {
	if locker == nil {
		locker = NewRunLocker()
	}
	return &Worker{
		queue:   q,
		engine:  e,
		locker:  locker,
		logger:  logging.Default(),
		metrics: metrics.New(),
		id:      1,
	}
}

// SetLogger 覆盖默认日志器，保留标准库 *log.Logger 兼容性。
func (w *Worker) SetLogger(l *log.Logger) {
	if l != nil {
		w.logger = logging.FromStandard(l)
	}
}

// SetStructuredLogger replaces the worker logger with a leveled logger.
func (w *Worker) SetStructuredLogger(logger logging.Logger) {
	if logger != nil {
		w.logger = logger
	}
}

// SetMetrics replaces the worker metrics registry. A nil value is ignored.
func (w *Worker) SetMetrics(m *metrics.Metrics) {
	if m != nil {
		w.metrics = m
	}
}

// Metrics returns the worker metrics registry.
func (w *Worker) Metrics() *metrics.Metrics {
	return w.metrics
}

// SetID sets the worker identifier used in logs. It is intended for
// WorkerPool and should be called before Start.
func (w *Worker) SetID(id int) {
	w.id = id
}

// Start 启动消费循环。
func (w *Worker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	w.wg.Add(1)
	go w.run(ctx)
}

// Stop 优雅停止：取消阻塞的 Dequeue，等待当前 Job 处理完成。
func (w *Worker) Stop() {
	w.signalStop()
	w.wait()
}

func (w *Worker) signalStop() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *Worker) wait() {
	w.wg.Wait()
}

// run 是消费循环。
func (w *Worker) run(ctx context.Context) {
	defer w.wg.Done()

	for {
		job, err := w.queue.Dequeue(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, queue.ErrClosed) {
				return
			}
			w.logger.Error("dequeue error", "worker_id", w.id, "err", err)
			continue
		}

		w.handle(job)
	}
}

// handle 按 Job 类型分发处理，并保证同一个 Run 不会被并发处理。
func (w *Worker) handle(job queue.Job) {
	defer func() {
		if recovered := recover(); recovered != nil {
			w.metrics.JobsPanic.Inc()
			w.logger.Error("job panic recovered",
				"worker_id", w.id,
				"run_id", job.RunID,
				"job_type", job.Type,
				"err", recovered,
			)
		}
	}()

	jobType := string(job.Type)
	release, ok := w.locker.TryAcquire(job.RunID)
	if !ok {
		w.metrics.JobsDuplicate.Inc(jobType)
		w.logger.Warn("duplicate job skipped",
			"worker_id", w.id,
			"run_id", job.RunID,
			"job_type", job.Type,
			"reason", "run_in_progress",
		)
		return
	}
	defer release()

	w.logger.Info("job started",
		"worker_id", w.id,
		"run_id", job.RunID,
		"job_type", job.Type,
	)

	started := time.Now()
	jobStatus := "failed"

	w.metrics.JobsStarted.Inc(jobType)
	w.metrics.WorkersBusy.Inc()
	defer func() {
		w.metrics.WorkersBusy.Dec()
		w.metrics.JobsFinished.Inc(jobType, jobStatus)
		w.metrics.JobDuration.ObserveSeconds(
			time.Since(started).Seconds(),
			jobType,
		)
	}()

	ctx := context.Background()
	switch job.Type {
	case queue.JobRecover:
		jobStatus = w.recoverRun(ctx, job)
	default: // JobExecute
		jobStatus = w.executeRun(ctx, job)
	}
}

// executeRun 正常执行一个 run。幂等规则：
//   - 从 store 重载 run，以持久化状态为准；
//   - 已终态（SUCCEEDED/FAILED）直接跳过，重复投递安全；
//   - 仍为 RUNNING（上次执行未正常收尾）则走 RecoverRun 修复；
//   - 否则加载定义后 RunWorkflow。
//
// RunWorkflow 返回的错误分为可重试错误、终态失败和非法状态错误；
// 只有终态失败会由 engine 持久化为 FAILED。
func (w *Worker) executeRun(ctx context.Context, job queue.Job) string {
	started := time.Now()
	run, err := w.engine.GetRun(ctx, job.RunID)
	if err != nil {
		w.logger.Error("job load run failed",
			"worker_id", w.id,
			"run_id", job.RunID,
			"err", err,
		)
		return "failed"
	}

	if run.Status.Terminal() {
		w.logger.Debug("job skipped",
			"worker_id", w.id,
			"run_id", job.RunID,
			"status", run.Status,
			"reason", "run_terminal",
		)
		return "skipped"
	}

	if job.Type == queue.JobExecute &&
		run.Status == workflow.RunPending &&
		!run.CreatedAt.IsZero() {
		w.metrics.RunQueueWait.ObserveSeconds(
			time.Since(run.CreatedAt).Seconds(),
		)
	}

	if run.Status == workflow.RunRunning {
		w.logger.Info("run is RUNNING, switching to recovery",
			"worker_id", w.id,
			"run_id", job.RunID,
		)
		return w.recoverRun(ctx, job)
	}

	def, err := w.engine.GetDefinition(ctx, run.WorkflowID, run.WorkflowVersion)
	if err != nil {
		w.logger.Error("job load definition failed",
			"worker_id", w.id,
			"run_id", job.RunID,
			"workflow_id", run.WorkflowID,
			"workflow_version", run.WorkflowVersion,
			"err", err,
		)
		return "failed"
	}

	if err := w.engine.RunWorkflow(ctx, def, run); err != nil {
		// Terminal failures are already persisted as FAILED by the engine.
		// Retryable errors leave the run in PENDING/RUNNING for a later
		// recovery attempt, so the log makes the distinction explicit.
		switch {
		case engine.IsTerminalFailure(err):
			w.logger.Error("run execution terminal failure",
				"worker_id", w.id,
				"run_id", job.RunID,
				"duration_ms", time.Since(started).Milliseconds(),
				"err", err,
			)
			return "failed"
		case engine.IsRetryable(err):
			w.logger.Warn("run execution retryable error",
				"worker_id", w.id,
				"run_id", job.RunID,
				"duration_ms", time.Since(started).Milliseconds(),
				"err", err,
			)
			return "retryable"
		default:
			w.logger.Error("run execution engine error",
				"worker_id", w.id,
				"run_id", job.RunID,
				"duration_ms", time.Since(started).Milliseconds(),
				"err", err,
			)
			return "failed"
		}
	}

	w.logger.Info("run execution finished",
		"worker_id", w.id,
		"run_id", job.RunID,
		"status", run.Status,
		"duration_ms", time.Since(started).Milliseconds(),
	)
	return "succeeded"
}

// recoverRun 使用 RecoverRun 修复崩溃残留（RUNNING step → PENDING），
// 并继续执行直到终态。
func (w *Worker) recoverRun(ctx context.Context, job queue.Job) string {
	started := time.Now()
	run, err := w.engine.RecoverRun(ctx, job.RunID)
	if err != nil {
		w.logger.Error("run recovery failed",
			"worker_id", w.id,
			"run_id", job.RunID,
			"duration_ms", time.Since(started).Milliseconds(),
			"err", err,
		)
		return "failed"
	}

	w.logger.Info("run recovery finished",
		"worker_id", w.id,
		"run_id", job.RunID,
		"status", run.Status,
		"duration_ms", time.Since(started).Milliseconds(),
	)
	return "succeeded"
}
