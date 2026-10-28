package worker

import (
	"log"
	"sync"

	"github.com/fluffyTighnari/workflow-engine/pkg/engine"
	"github.com/fluffyTighnari/workflow-engine/pkg/logging"
	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
	"github.com/fluffyTighnari/workflow-engine/pkg/queue"
)

// Pool manages a fixed set of Workers that share one queue and one
// RunLocker. Different runs can be processed concurrently; the same run
// cannot be processed by two workers at the same time.
type Pool struct {
	workers []*Worker
	logger  logging.Logger
	metrics *metrics.Metrics

	mu      sync.Mutex
	started bool
}

// NewPool creates a worker pool. workerCount is clamped to at least 1.
// A nil locker creates a new process-local RunLocker.
func NewPool(
	q queue.Queue,
	e *engine.Engine,
	workerCount int,
	locker RunLocker,
) *Pool {
	if workerCount < 1 {
		workerCount = 1
	}
	if locker == nil {
		locker = NewRunLocker()
	}

	m := metrics.New()
	workers := make([]*Worker, workerCount)
	for i := range workers {
		w := NewWithLocker(q, e, locker)
		w.SetID(i + 1)
		w.SetMetrics(m)
		workers[i] = w
	}
	m.WorkersTotal.Set(int64(workerCount))

	return &Pool{
		workers: workers,
		logger:  logging.Default(),
		metrics: m,
	}
}

// Size returns the number of workers in the pool.
func (p *Pool) Size() int {
	return len(p.workers)
}

// SetLogger sets the logger for every worker in the pool, preserving
// standard-library *log.Logger compatibility. It should be called before
// Start.
func (p *Pool) SetLogger(logger *log.Logger) {
	if logger == nil {
		return
	}
	p.SetStructuredLogger(logging.FromStandard(logger))
}

// SetStructuredLogger sets the leveled logger for the pool and every
// worker. It should be called before Start.
func (p *Pool) SetStructuredLogger(logger logging.Logger) {
	if logger == nil {
		return
	}
	p.logger = logger
	for _, w := range p.workers {
		w.SetStructuredLogger(logger)
	}
}

// SetMetrics sets the metrics registry for the pool and every worker.
// It should be called before Start.
func (p *Pool) SetMetrics(m *metrics.Metrics) {
	if m == nil {
		return
	}
	p.metrics = m
	m.WorkersTotal.Set(int64(len(p.workers)))
	for _, w := range p.workers {
		w.SetMetrics(m)
	}
}

// Metrics returns the pool metrics registry.
func (p *Pool) Metrics() *metrics.Metrics {
	return p.metrics
}

// Start starts every worker. Calling Start more than once is a no-op.
func (p *Pool) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return
	}
	p.started = true

	for _, w := range p.workers {
		w.Start()
	}

	p.logger.Info("worker pool started", "worker_count", len(p.workers))
}

// Stop cancels every worker's Dequeue loop first, then waits for all
// in-flight jobs to finish.
func (p *Pool) Stop() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	workers := p.workers
	logger := p.logger
	p.mu.Unlock()

	logger.Info("worker pool stopping", "worker_count", len(workers))
	for _, w := range workers {
		w.signalStop()
	}
	for _, w := range workers {
		w.wait()
	}
	logger.Info("worker pool stopped", "worker_count", len(workers))
}
