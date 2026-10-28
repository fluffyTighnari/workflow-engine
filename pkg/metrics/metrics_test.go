package metrics

import (
	"math"
	"testing"
)

func TestCounterGauge(t *testing.T) {
	var counter Counter
	counter.Inc()
	counter.Add(2)
	if got := counter.Value(); got != 3 {
		t.Fatalf("counter = %d, want 3", got)
	}

	var gauge Gauge
	gauge.Set(10)
	gauge.Inc()
	gauge.Dec()
	gauge.Add(-3)
	if got := gauge.Value(); got != 7 {
		t.Fatalf("gauge = %d, want 7", got)
	}
}

func TestHistogramCumulativeBuckets(t *testing.T) {
	h := NewHistogram([]float64{0.1, 0.5, 1})

	h.ObserveSeconds(0.05)
	h.ObserveSeconds(0.75)

	snapshot := h.Snapshot()
	if snapshot.Count != 2 {
		t.Fatalf("count = %d, want 2", snapshot.Count)
	}
	if math.Abs(snapshot.Sum-0.8) > 1e-9 {
		t.Fatalf("sum = %v, want 0.8", snapshot.Sum)
	}

	want := map[string]uint64{
		"0.1": 1,
		"0.5": 1,
		"1":   2,
	}
	for bucket, expected := range want {
		if got := snapshot.Buckets[bucket]; got != expected {
			t.Errorf("bucket %s = %d, want %d", bucket, got, expected)
		}
	}
}

func TestCounterVecAndHistogramVec(t *testing.T) {
	counters := NewCounterVec("step_type", "status")
	counters.Inc("compute", "SUCCEEDED")
	counters.Inc("compute", "SUCCEEDED")
	counters.Inc("double", "FAILED")

	got := counters.Snapshot()
	if got["step_type=compute,status=SUCCEEDED"] != 2 {
		t.Errorf("compute snapshot = %v", got)
	}
	if got["step_type=double,status=FAILED"] != 1 {
		t.Errorf("double snapshot = %v", got)
	}

	histograms := NewHistogramVec([]float64{1}, "job_type")
	histograms.ObserveSeconds(0.5, "execute")
	hist := histograms.Snapshot()["job_type=execute"]
	if hist.Count != 1 {
		t.Fatalf("histogram count = %d, want 1", hist.Count)
	}
	if hist.Buckets["1"] != 1 {
		t.Fatalf("histogram bucket = %v", hist.Buckets)
	}
}

func TestMetricsSnapshot(t *testing.T) {
	m := New()
	m.RunsCreated.Inc()
	m.RunsFinished.Inc("SUCCEEDED")
	m.RunFailures.Inc("INPUT")
	m.StepsStarted.Inc("compute")
	m.StepsFinished.Inc("compute", "SUCCEEDED")
	m.JobsStarted.Inc("execute")
	m.WorkersTotal.Set(4)
	m.WorkersBusy.Set(2)
	m.QueueDepth.Set(3)

	snapshot := m.Snapshot()
	if snapshot.RunsCreated != 1 {
		t.Errorf("runs created = %d, want 1", snapshot.RunsCreated)
	}
	if snapshot.RunsFinished["status=SUCCEEDED"] != 1 {
		t.Errorf("runs finished = %v", snapshot.RunsFinished)
	}
	if snapshot.RunFailures["failure_kind=INPUT"] != 1 {
		t.Errorf("run failures = %v", snapshot.RunFailures)
	}
	if snapshot.StepsStarted["step_type=compute"] != 1 {
		t.Errorf("steps started = %v", snapshot.StepsStarted)
	}
	if snapshot.WorkersTotal != 4 || snapshot.WorkersBusy != 2 {
		t.Errorf("workers total/busy = %d/%d, want 4/2",
			snapshot.WorkersTotal, snapshot.WorkersBusy)
	}
	if snapshot.QueueDepth != 3 {
		t.Errorf("queue depth = %d, want 3", snapshot.QueueDepth)
	}
}
