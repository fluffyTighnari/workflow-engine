package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/metrics"
	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestEngineRecordsRunAndStepMetrics(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	m := metrics.New()
	e.SetMetrics(m)

	run, def := createRunForPipeline(t, e)
	if err := e.RunWorkflow(ctx, def, run); err != nil {
		t.Fatalf("run workflow: %v", err)
	}

	if got := m.RunsCreated.Value(); got != 1 {
		t.Fatalf("runs created = %d, want 1", got)
	}

	snapshot := m.Snapshot()
	if snapshot.RunsFinished["status=SUCCEEDED"] != 1 {
		t.Errorf("runs finished = %v", snapshot.RunsFinished)
	}
	if snapshot.RunDuration["status=SUCCEEDED"].Count != 1 {
		t.Errorf("run duration = %v", snapshot.RunDuration)
	}
	if snapshot.RunExecution["status=SUCCEEDED"].Count != 1 {
		t.Errorf("run execution = %v", snapshot.RunExecution)
	}

	if snapshot.StepsStarted["step_type=compute"] != 1 {
		t.Errorf("compute starts = %v", snapshot.StepsStarted)
	}
	if snapshot.StepsStarted["step_type=double"] != 2 {
		t.Errorf("double starts = %v", snapshot.StepsStarted)
	}
	if snapshot.StepsFinished["step_type=compute,status=SUCCEEDED"] != 1 {
		t.Errorf("compute finishes = %v", snapshot.StepsFinished)
	}
	if snapshot.StepsFinished["step_type=double,status=SUCCEEDED"] != 2 {
		t.Errorf("double finishes = %v", snapshot.StepsFinished)
	}
	if snapshot.StepsInFlight != 0 {
		t.Errorf("steps in flight = %d, want 0", snapshot.StepsInFlight)
	}
}

func TestEngineRecordsFailureMetrics(t *testing.T) {
	ctx := context.Background()
	e := newMemoryTestEngine(t)
	m := metrics.New()
	e.SetMetrics(m)

	def := workflow.WorkflowDefinition{
		ID:      "metric-failure",
		Version: 1,
		Steps: []workflow.Step{
			{
				ID:   "A",
				Type: "compute",
				Inputs: map[string]workflow.InputRef{
					"value": {Source: "workflow_input", Key: "missing"},
				},
			},
		},
	}
	run, err := e.CreateRun(ctx, def, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = e.ExecuteStep(ctx, def, run, "A")
	if !errors.Is(err, ErrInputResolution) {
		t.Fatalf("error = %v, want ErrInputResolution", err)
	}

	snapshot := m.Snapshot()
	if snapshot.RunsFinished["status=FAILED"] != 1 {
		t.Errorf("runs finished = %v", snapshot.RunsFinished)
	}
	if snapshot.RunFailures["failure_kind=INPUT"] != 1 {
		t.Errorf("run failures = %v", snapshot.RunFailures)
	}
	if snapshot.RunDuration["status=FAILED"].Count != 1 {
		t.Errorf("run duration = %v", snapshot.RunDuration)
	}
	if snapshot.StepsFinished["step_type=compute,status=FAILED"] != 1 {
		t.Errorf("step finishes = %v", snapshot.StepsFinished)
	}
	if snapshot.StepFailures["step_type=compute,failure_kind=INPUT"] != 1 {
		t.Errorf("step failures = %v", snapshot.StepFailures)
	}
	if snapshot.StepsInFlight != 0 {
		t.Errorf("steps in flight = %d, want 0", snapshot.StepsInFlight)
	}
}
