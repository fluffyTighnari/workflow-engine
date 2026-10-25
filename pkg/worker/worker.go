package worker

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// Worker 是单 goroutine 的串行消费者：
// 从队列取出 Job，按类型交给 handler 处理，处理完再取下一个。
//
// 语义：
//   - 同一时刻只处理一个 Job，天然串行；
//   - 消费前一律从 store 重新加载 run，保证幂等（重复投递安全）；
//   - Stop 会取消阻塞的 Dequeue，并等待当前 Job 处理完毕。
type Worker struct {
	queue  queue.Queue
	engine *engine.Engine
	logger *log.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New 创建一个串行 worker。
func New(q queue.Queue, e *engine.Engine) *Worker {
	return &Worker{
		queue:  q,
		engine: e,
		logger: log.Default(),
	}
}

// SetLogger 覆盖默认日志器。
func (w *Worker) SetLogger(l *log.Logger) {
	if l != nil {
		w.logger = l
	}
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
	if w.cancel != nil {
		w.cancel()
	}
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
			w.logger.Printf("worker: dequeue error: %v", err)
			continue
		}

		w.handle(job)
	}
}

// handle 按 Job 类型分发处理。
func (w *Worker) handle(job queue.Job) {
	ctx := context.Background()
	switch job.Type {
	case queue.JobRecover:
		w.recoverRun(ctx, job)
	default: // JobExecute
		w.executeRun(ctx, job)
	}
}

// executeRun 正常执行一个 run。幂等规则：
//   - 从 store 重载 run，以持久化状态为准；
//   - 已终态（SUCCEEDED/FAILED）直接跳过，重复投递安全；
//   - 仍为 RUNNING（上次执行未正常收尾）则走 RecoverRun 修复；
//   - 否则加载定义后 RunWorkflow。
func (w *Worker) executeRun(ctx context.Context, job queue.Job) {
	run, err := w.engine.GetRun(ctx, job.RunID)
	if err != nil {
		w.logger.Printf("worker: execute %s: load run: %v", job.RunID, err)
		return
	}

	if run.Status == workflow.RunSucceeded ||
		run.Status == workflow.RunFailed {
		w.logger.Printf("worker: execute %s: already terminal (%s), skip",
			job.RunID, run.Status)
		return
	}

	if run.Status == workflow.RunRunning {
		w.logger.Printf("worker: execute %s: found RUNNING, recovering", job.RunID)
		w.recoverRun(ctx, job)
		return
	}

	def, err := w.engine.GetDefinition(ctx, run.WorkflowID, run.WorkflowVersion)
	if err != nil {
		w.logger.Printf("worker: execute %s: load definition: %v", job.RunID, err)
		return
	}

	if err := w.engine.RunWorkflow(ctx, def, run); err != nil {
		// engine 内部已把 run 持久化为 FAILED，这里只记录。
		w.logger.Printf("worker: execute %s: failed: %v", job.RunID, err)
		return
	}

	w.logger.Printf("worker: execute %s: finished, status=%s", job.RunID, run.Status)
}

// recoverRun 使用 RecoverRun 修复崩溃残留（RUNNING step → PENDING），
// 并继续执行直到终态。
func (w *Worker) recoverRun(ctx context.Context, job queue.Job) {
	run, err := w.engine.RecoverRun(ctx, job.RunID)
	if err != nil {
		w.logger.Printf("worker: recover %s: %v", job.RunID, err)
		return
	}

	w.logger.Printf("worker: recover %s: finished, status=%s", job.RunID, run.Status)
}
