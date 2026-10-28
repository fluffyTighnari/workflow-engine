package engine

import (
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// SetMetrics replaces the engine metrics registry. A nil value is ignored.
func (e *Engine) SetMetrics(m *metrics.Metrics) {
	if m != nil {
		e.metrics = m
	}
}

// Metrics returns the engine metrics registry.
func (e *Engine) Metrics() *metrics.Metrics {
	return e.metrics
}

// recordRunTerminal records metrics only after a terminal state has been
// accepted by the store.
func (e *Engine) recordRunTerminal(run *workflow.WorkflowRun) {
	if e.metrics == nil || !run.Status.Terminal() {
		return
	}

	status := string(run.Status)
	e.metrics.RunsFinished.Inc(status)

	if !run.CreatedAt.IsZero() {
		e.metrics.RunDuration.ObserveSeconds(
			nowUTC().Sub(run.CreatedAt).Seconds(),
			status,
		)
	}

	if run.Status == workflow.RunFailed {
		kind := ""
		if run.Failure != nil {
			kind = string(run.Failure.Kind)
		}
		e.metrics.RunFailures.Inc(kind)
	}
}

func (e *Engine) recordStepFinished(
	stepType string,
	status workflow.StepStatus,
	failureKind workflow.FailureKind,
	duration time.Duration,
) {
	if e.metrics == nil {
		return
	}

	statusLabel := string(status)
	e.metrics.StepsFinished.Inc(stepType, statusLabel)
	e.metrics.StepDuration.ObserveSeconds(
		duration.Seconds(),
		stepType,
		statusLabel,
	)

	if status == workflow.StepFailed {
		e.metrics.StepFailures.Inc(stepType, string(failureKind))
	}
}
