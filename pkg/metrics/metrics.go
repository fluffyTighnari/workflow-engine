// Package metrics provides a minimal, dependency-free metrics registry.
//
// It intentionally avoids Prometheus and high-cardinality labels. Values
// are kept in-process and can be exposed as JSON by the server.
package metrics

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing value.
type Counter struct {
	value atomic.Uint64
}

// Inc adds one.
func (c *Counter) Inc() {
	c.value.Add(1)
}

// Add adds n.
func (c *Counter) Add(n uint64) {
	c.value.Add(n)
}

// Value returns the current value.
func (c *Counter) Value() uint64 {
	return c.value.Load()
}

// Gauge is a value that can go up and down.
type Gauge struct {
	value atomic.Int64
}

// Set replaces the current value.
func (g *Gauge) Set(value int64) {
	g.value.Store(value)
}

// Add adds delta.
func (g *Gauge) Add(delta int64) {
	g.value.Add(delta)
}

// Inc adds one.
func (g *Gauge) Inc() {
	g.value.Add(1)
}

// Dec subtracts one.
func (g *Gauge) Dec() {
	g.value.Add(-1)
}

// Value returns the current value.
func (g *Gauge) Value() int64 {
	return g.value.Load()
}

// HistogramSnapshot is a point-in-time histogram value.
type HistogramSnapshot struct {
	Count   uint64            `json:"count"`
	Sum     float64           `json:"sum"`
	Buckets map[string]uint64 `json:"buckets"`
}

// Histogram stores count, sum, and cumulative fixed-bucket counts.
type Histogram struct {
	mu     sync.Mutex
	bounds []float64
	counts []uint64
	count  uint64
	sum    float64
}

// NewHistogram creates a histogram with the given upper bounds.
func NewHistogram(bounds []float64) *Histogram {
	return &Histogram{
		bounds: append([]float64(nil), bounds...),
		counts: make([]uint64, len(bounds)),
	}
}

// ObserveSeconds records a duration in seconds.
func (h *Histogram) ObserveSeconds(seconds float64) {
	if seconds < 0 {
		seconds = 0
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.count++
	h.sum += seconds
	for i, bound := range h.bounds {
		if seconds <= bound {
			h.counts[i]++
		}
	}
}

// Snapshot returns a copy of the current histogram state.
func (h *Histogram) Snapshot() HistogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	buckets := make(map[string]uint64, len(h.bounds))
	for i, bound := range h.bounds {
		buckets[formatBucket(bound)] = h.counts[i]
	}

	return HistogramSnapshot{
		Count:   h.count,
		Sum:     h.sum,
		Buckets: buckets,
	}
}

// CounterVec is a counter split by a small, fixed label set.
type CounterVec struct {
	labelNames []string

	mu     sync.RWMutex
	values map[string]*Counter
}

// NewCounterVec creates a labeled counter.
func NewCounterVec(labelNames ...string) *CounterVec {
	return &CounterVec{
		labelNames: append([]string(nil), labelNames...),
		values:     make(map[string]*Counter),
	}
}

// Inc adds one to the series identified by labelValues.
func (v *CounterVec) Inc(labelValues ...string) {
	v.Add(1, labelValues...)
}

// Add adds n to the series identified by labelValues.
func (v *CounterVec) Add(n uint64, labelValues ...string) {
	v.counter(labelValues...).Add(n)
}

// Snapshot returns all current label values.
func (v *CounterVec) Snapshot() map[string]uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()

	out := make(map[string]uint64, len(v.values))
	for key, counter := range v.values {
		out[key] = counter.Value()
	}
	return out
}

func (v *CounterVec) counter(labelValues ...string) *Counter {
	key := labelKey(v.labelNames, labelValues)

	v.mu.RLock()
	counter := v.values[key]
	v.mu.RUnlock()
	if counter != nil {
		return counter
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if counter = v.values[key]; counter != nil {
		return counter
	}
	counter = &Counter{}
	v.values[key] = counter
	return counter
}

// HistogramVec is a histogram split by a small, fixed label set.
type HistogramVec struct {
	labelNames []string
	bounds     []float64

	mu     sync.RWMutex
	values map[string]*Histogram
}

// NewHistogramVec creates a labeled histogram.
func NewHistogramVec(bounds []float64, labelNames ...string) *HistogramVec {
	return &HistogramVec{
		labelNames: append([]string(nil), labelNames...),
		bounds:     append([]float64(nil), bounds...),
		values:     make(map[string]*Histogram),
	}
}

// ObserveSeconds records a duration for the given label values.
func (v *HistogramVec) ObserveSeconds(seconds float64, labelValues ...string) {
	v.histogram(labelValues...).ObserveSeconds(seconds)
}

// Snapshot returns all current label values.
func (v *HistogramVec) Snapshot() map[string]HistogramSnapshot {
	v.mu.RLock()
	defer v.mu.RUnlock()

	out := make(map[string]HistogramSnapshot, len(v.values))
	for key, histogram := range v.values {
		out[key] = histogram.Snapshot()
	}
	return out
}

func (v *HistogramVec) histogram(labelValues ...string) *Histogram {
	key := labelKey(v.labelNames, labelValues)

	v.mu.RLock()
	histogram := v.values[key]
	v.mu.RUnlock()
	if histogram != nil {
		return histogram
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if histogram = v.values[key]; histogram != nil {
		return histogram
	}
	histogram = NewHistogram(v.bounds)
	v.values[key] = histogram
	return histogram
}

// Metrics is the process-wide set of workflow metrics. Engine, worker
// pool, async facade, and server share one instance.
type Metrics struct {
	RunsCreated   Counter
	RunsFinished  *CounterVec
	RunFailures   *CounterVec
	RunDuration   *HistogramVec
	RunQueueWait  *Histogram
	RunExecution  *HistogramVec
	StepsStarted  *CounterVec
	StepsFinished *CounterVec
	StepFailures  *CounterVec
	StepDuration  *HistogramVec
	StepsInFlight Gauge
	JobsStarted   *CounterVec
	JobsFinished  *CounterVec
	JobsDuplicate *CounterVec
	JobsPanic     Counter
	JobDuration   *HistogramVec
	WorkersTotal  Gauge
	WorkersBusy   Gauge
	QueueDepth    Gauge
	Submissions   *CounterVec
	HTTPRequests  *CounterVec
	HTTPDuration  *HistogramVec
	HTTPInFlight  Gauge
}

// New returns an empty metrics registry.
func New() *Metrics {
	bounds := durationBuckets()

	return &Metrics{
		RunsFinished:  NewCounterVec("status"),
		RunFailures:   NewCounterVec("failure_kind"),
		RunDuration:   NewHistogramVec(bounds, "status"),
		RunQueueWait:  NewHistogram(bounds),
		RunExecution:  NewHistogramVec(bounds, "status"),
		StepsStarted:  NewCounterVec("step_type"),
		StepsFinished: NewCounterVec("step_type", "status"),
		StepFailures:  NewCounterVec("step_type", "failure_kind"),
		StepDuration:  NewHistogramVec(bounds, "step_type", "status"),
		JobsStarted:   NewCounterVec("job_type"),
		JobsFinished:  NewCounterVec("job_type", "status"),
		JobsDuplicate: NewCounterVec("job_type"),
		JobDuration:   NewHistogramVec(bounds, "job_type"),
		Submissions:   NewCounterVec("result"),
		HTTPRequests:  NewCounterVec("method", "route", "status"),
		HTTPDuration:  NewHistogramVec(bounds, "method", "route"),
	}
}

// Snapshot is a JSON-friendly view of all metrics.
type Snapshot struct {
	RunsCreated   uint64                       `json:"workflow_runs_created_total"`
	RunsFinished  map[string]uint64            `json:"workflow_runs_finished_total"`
	RunFailures   map[string]uint64            `json:"workflow_run_failures_total"`
	RunDuration   map[string]HistogramSnapshot `json:"workflow_run_duration_seconds"`
	RunQueueWait  HistogramSnapshot            `json:"workflow_run_queue_wait_seconds"`
	RunExecution  map[string]HistogramSnapshot `json:"workflow_run_execution_duration_seconds"`
	StepsStarted  map[string]uint64            `json:"workflow_steps_started_total"`
	StepsFinished map[string]uint64            `json:"workflow_steps_finished_total"`
	StepFailures  map[string]uint64            `json:"workflow_step_failures_total"`
	StepDuration  map[string]HistogramSnapshot `json:"workflow_step_duration_seconds"`
	StepsInFlight int64                        `json:"workflow_steps_in_flight"`
	JobsStarted   map[string]uint64            `json:"workflow_jobs_started_total"`
	JobsFinished  map[string]uint64            `json:"workflow_jobs_finished_total"`
	JobsDuplicate map[string]uint64            `json:"workflow_jobs_duplicate_total"`
	JobsPanic     uint64                       `json:"workflow_jobs_panic_total"`
	JobDuration   map[string]HistogramSnapshot `json:"workflow_job_duration_seconds"`
	WorkersTotal  int64                        `json:"workflow_workers_total"`
	WorkersBusy   int64                        `json:"workflow_workers_busy"`
	QueueDepth    int64                        `json:"workflow_queue_depth"`
	Submissions   map[string]uint64            `json:"workflow_submissions_total"`
	HTTPRequests  map[string]uint64            `json:"workflow_http_requests_total"`
	HTTPDuration  map[string]HistogramSnapshot `json:"workflow_http_request_duration_seconds"`
	HTTPInFlight  int64                        `json:"workflow_http_in_flight"`
}

// Snapshot returns a consistent-enough point-in-time view.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		RunsCreated:   m.RunsCreated.Value(),
		RunsFinished:  m.RunsFinished.Snapshot(),
		RunFailures:   m.RunFailures.Snapshot(),
		RunDuration:   m.RunDuration.Snapshot(),
		RunQueueWait:  m.RunQueueWait.Snapshot(),
		RunExecution:  m.RunExecution.Snapshot(),
		StepsStarted:  m.StepsStarted.Snapshot(),
		StepsFinished: m.StepsFinished.Snapshot(),
		StepFailures:  m.StepFailures.Snapshot(),
		StepDuration:  m.StepDuration.Snapshot(),
		StepsInFlight: m.StepsInFlight.Value(),
		JobsStarted:   m.JobsStarted.Snapshot(),
		JobsFinished:  m.JobsFinished.Snapshot(),
		JobsDuplicate: m.JobsDuplicate.Snapshot(),
		JobsPanic:     m.JobsPanic.Value(),
		JobDuration:   m.JobDuration.Snapshot(),
		WorkersTotal:  m.WorkersTotal.Value(),
		WorkersBusy:   m.WorkersBusy.Value(),
		QueueDepth:    m.QueueDepth.Value(),
		Submissions:   m.Submissions.Snapshot(),
		HTTPRequests:  m.HTTPRequests.Snapshot(),
		HTTPDuration:  m.HTTPDuration.Snapshot(),
		HTTPInFlight:  m.HTTPInFlight.Value(),
	}
}

func durationBuckets() []float64 {
	return []float64{
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		1, 2.5, 5, 10, 30, 60, 120, 300,
	}
}

func labelKey(names []string, values []string) string {
	if len(names) == 0 {
		return ""
	}

	parts := make([]string, 0, len(names))
	for i, name := range names {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		parts = append(parts, name+"="+value)
	}
	return strings.Join(parts, ",")
}

func formatBucket(bound float64) string {
	return strconv.FormatFloat(bound, 'g', -1, 64)
}
