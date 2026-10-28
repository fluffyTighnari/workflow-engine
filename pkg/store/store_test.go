package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// newSQLiteTestStore returns a SQLite store that is closed via t.Cleanup.
func newSQLiteTestStore(t *testing.T) *SQLiteStore {
	t.Helper()

	path := filepath.Join(t.TempDir(), "workflow.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// forEachStore runs fn against every built-in WorkflowStore implementation.
func forEachStore(t *testing.T, fn func(t *testing.T, s WorkflowStore)) {
	t.Helper()

	t.Run("memory", func(t *testing.T) {
		s := NewMemoryStore()
		t.Cleanup(func() { _ = s.Close() })
		fn(t, s)
	})

	t.Run("sqlite", func(t *testing.T) {
		fn(t, newSQLiteTestStore(t))
	})
}

// testDefinition is the shared fixture for store tests.
func testDefinition() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID:      "demo",
		Version: 1,
		Steps: []workflow.Step{
			{ID: "A", Type: "compute"},
			{
				ID:        "B",
				Type:      "double",
				DependsOn: []string{"A"},
				Inputs: map[string]workflow.InputRef{
					"value": {Source: "A", Key: "result"},
				},
			},
		},
	}
}

// seedRun saves testDefinition and creates a two-step run in the given state.
func seedRun(
	t *testing.T,
	ctx context.Context,
	s WorkflowStore,
	runID string,
	status workflow.RunStatus,
	input map[string]any,
) *workflow.WorkflowRun {
	t.Helper()

	def := testDefinition()
	if err := s.SaveDefinition(ctx, def); err != nil &&
		!errors.Is(err, ErrDefinitionExists) {
		t.Fatalf("save definition: %v", err)
	}

	now := time.Now().UTC()
	run := &workflow.WorkflowRun{
		ID:              runID,
		WorkflowID:      def.ID,
		WorkflowVersion: def.Version,
		Status:          status,
		Input:           input,
		StepExecutions: map[string]*workflow.StepExecution{
			"A": {StepID: "A", Status: workflow.StepPending},
			"B": {StepID: "B", Status: workflow.StepPending},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.CreateRun(ctx, def, run); err != nil {
		t.Fatalf("create run: %v", err)
	}

	return run
}

func TestSaveExecutionStateRoundTrip(t *testing.T) {
	forEachStore(t, func(t *testing.T, s WorkflowStore) {
		ctx := context.Background()
		startedAt := time.Now().UTC().Add(-2 * time.Second)
		finishedAt := time.Now().UTC().Add(-1 * time.Second)

		def := testDefinition()
		run := seedRun(t, ctx, s, "run-roundtrip", workflow.RunRunning,
			map[string]any{"origin": "test"})

		// Simulate partial execution: A succeeds, B stays pending.
		execA := run.StepExecutions["A"]
		execA.Status = workflow.StepSucceeded
		execA.Input = map[string]any{"left": float64(2)}
		execA.Output = map[string]any{"result": float64(5)}
		execA.StartedAt = &startedAt
		execA.FinishedAt = &finishedAt
		run.UpdatedAt = finishedAt

		if err := s.SaveExecutionState(ctx, run, "A"); err != nil {
			t.Fatalf("save step A state: %v", err)
		}

		// Reload from the store rather than checking the in-memory object.
		loaded, err := s.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatalf("get run: %v", err)
		}

		// Run-level fields.
		if loaded.ID != run.ID {
			t.Errorf("run ID = %q, want %q", loaded.ID, run.ID)
		}
		if loaded.WorkflowID != def.ID || loaded.WorkflowVersion != def.Version {
			t.Errorf("workflow ref = %s/%d, want %s/%d",
				loaded.WorkflowID, loaded.WorkflowVersion, def.ID, def.Version)
		}
		if loaded.Status != workflow.RunRunning {
			t.Errorf("run status = %q, want RUNNING", loaded.Status)
		}
		if loaded.Input["origin"] != "test" {
			t.Errorf("run input = %v, want origin=test", loaded.Input)
		}

		// Step A fields.
		a := loaded.StepExecutions["A"]
		if a.Status != workflow.StepSucceeded {
			t.Errorf("A status = %q, want SUCCEEDED", a.Status)
		}
		if a.Input["left"] != float64(2) {
			t.Errorf("A input = %v, want left=2", a.Input)
		}
		if a.Output["result"] != float64(5) {
			t.Errorf("A output = %v, want result=5", a.Output["result"])
		}
		if a.StartedAt == nil || a.FinishedAt == nil {
			t.Fatal("A started/finished timestamps must be loaded")
		}
		if !a.StartedAt.Equal(startedAt) {
			t.Errorf("A started_at = %v, want %v", a.StartedAt, startedAt)
		}
		if a.Error != "" {
			t.Errorf("A error = %q, want empty", a.Error)
		}

		// Step B should be untouched.
		b := loaded.StepExecutions["B"]
		if b.Status != workflow.StepPending {
			t.Errorf("B status = %q, want PENDING", b.Status)
		}
		if b.Output != nil {
			t.Errorf("B output = %v, want nil", b.Output)
		}
	})
}

func TestGetRunNotFound(t *testing.T) {
	forEachStore(t, func(t *testing.T, s WorkflowStore) {
		_, err := s.GetRun(context.Background(), "nonexistent-run-id")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestGetDefinitionNotFound(t *testing.T) {
	forEachStore(t, func(t *testing.T, s WorkflowStore) {
		ctx := context.Background()

		_, err := s.GetDefinition(ctx, "no-such-workflow", 1)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("unknown workflow: expected ErrNotFound, got %v", err)
		}

		def := testDefinition()
		if err := s.SaveDefinition(ctx, def); err != nil {
			t.Fatalf("save definition: %v", err)
		}

		_, err = s.GetDefinition(ctx, def.ID, def.Version+1)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("unknown version: expected ErrNotFound, got %v", err)
		}
	})
}

func TestListRunsByStatus(t *testing.T) {
	forEachStore(t, func(t *testing.T, s WorkflowStore) {
		ctx := context.Background()

		seedRun(t, ctx, s, "run-pending", workflow.RunPending, nil)
		seedRun(t, ctx, s, "run-running", workflow.RunRunning, nil)
		seedRun(t, ctx, s, "run-done", workflow.RunSucceeded, nil)

		ids, err := s.ListRunsByStatus(ctx,
			workflow.RunPending, workflow.RunRunning)
		if err != nil {
			t.Fatal(err)
		}

		got := make(map[string]bool, len(ids))
		for _, id := range ids {
			got[id] = true
		}
		if !got["run-pending"] || !got["run-running"] {
			t.Errorf("ids = %v, want run-pending and run-running", ids)
		}
		if got["run-done"] {
			t.Errorf("ids = %v, must not include run-done", ids)
		}
	})
}

func TestListRunsByStatusEmpty(t *testing.T) {
	forEachStore(t, func(t *testing.T, s WorkflowStore) {
		ids, err := s.ListRunsByStatus(
			context.Background(), workflow.RunPending)
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != 0 {
			t.Fatalf("ids = %v, want empty", ids)
		}
	})
}

func newCachedTestStore(t *testing.T) *CachedStore {
	t.Helper()

	s, err := NewCachedStore(filepath.Join(t.TempDir(), "workflow.db"))
	if err != nil {
		t.Fatalf("open cached store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func TestCachedStoreSaveExecutionStateInvalidatesCache(t *testing.T) {
	ctx := context.Background()
	s := newCachedTestStore(t)
	run := seedRun(t, ctx, s, "run-cached", workflow.RunRunning, nil)

	execA := run.StepExecutions["A"]
	execA.Status = workflow.StepSucceeded
	execA.Output = map[string]any{"result": float64(5)}

	if err := s.SaveExecutionState(ctx, run, "A"); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// The cache entry must be discarded after the authoritative write so
	// the next read reloads the fresh state from persistence.
	if _, err := s.cache.GetRun(ctx, run.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cache still holds run after save, err = %v", err)
	}

	loaded, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if loaded.StepExecutions["A"].Status != workflow.StepSucceeded {
		t.Errorf("A status = %s, want SUCCEEDED",
			loaded.StepExecutions["A"].Status)
	}
}

func TestCachedStoreSaveExecutionStateLeavesCacheOnPersistFailure(t *testing.T) {
	ctx := context.Background()
	s := newCachedTestStore(t)
	run := seedRun(t, ctx, s, "run-cached-fail", workflow.RunRunning, nil)

	// Warm the cache so we can verify it is left untouched by a failed write.
	if _, err := s.GetRun(ctx, run.ID); err != nil {
		t.Fatalf("warm cache: %v", err)
	}

	// Break the persistent layer so SaveExecutionState fails.
	if err := s.persist.Close(); err != nil {
		t.Fatalf("close persistence: %v", err)
	}

	execA := run.StepExecutions["A"]
	execA.Status = workflow.StepSucceeded
	execA.Output = map[string]any{"result": float64(5)}

	if err := s.SaveExecutionState(ctx, run, "A"); err == nil {
		t.Fatal("expected persist error")
	}

	cached, err := s.cache.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("cached run must survive a failed persist: %v", err)
	}
	if cached.StepExecutions["A"].Status != workflow.StepPending {
		t.Errorf("cached A status = %s, want PENDING (unchanged)",
			cached.StepExecutions["A"].Status)
	}
}

func TestSaveExecutionStatePersistsFailureAndPendingReason(t *testing.T) {
	forEachStore(t, func(t *testing.T, s WorkflowStore) {
		ctx := context.Background()
		run := seedRun(t, ctx, s, "run-failure", workflow.RunRunning, nil)

		execA := run.StepExecutions["A"]
		execA.Status = workflow.StepFailed
		execA.PendingReason = workflow.PendingReasonNone
		execA.Error = "missing input"
		execA.Failure = &workflow.Failure{
			Kind:    workflow.FailureKindInput,
			Message: "missing input",
		}

		execB := run.StepExecutions["B"]
		execB.PendingReason = workflow.PendingReasonRecovered

		run.Status = workflow.RunFailed
		run.PendingReason = workflow.PendingReasonNone
		run.Failure = &workflow.Failure{
			Kind:    workflow.FailureKindInput,
			Message: "missing input",
			StepID:  "A",
		}
		run.UpdatedAt = time.Now().UTC()

		if err := s.SaveExecutionState(ctx, run, "A"); err != nil {
			t.Fatalf("save state A: %v", err)
		}
		if err := s.SaveExecutionState(ctx, run, "B"); err != nil {
			t.Fatalf("save state B: %v", err)
		}

		loaded, err := s.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatalf("get run: %v", err)
		}

		if loaded.Status != workflow.RunFailed {
			t.Errorf("run status = %s, want FAILED", loaded.Status)
		}
		if loaded.PendingReason != workflow.PendingReasonNone {
			t.Errorf("run pending reason = %q, want empty",
				loaded.PendingReason)
		}
		if loaded.Failure == nil ||
			loaded.Failure.Kind != workflow.FailureKindInput ||
			loaded.Failure.StepID != "A" {
			t.Errorf("run failure = %+v, want INPUT for step A",
				loaded.Failure)
		}

		a := loaded.StepExecutions["A"]
		if a.Failure == nil ||
			a.Failure.Kind != workflow.FailureKindInput {
			t.Errorf("A failure = %+v, want INPUT", a.Failure)
		}
		if a.Error != "missing input" {
			t.Errorf("A error = %q, want missing input", a.Error)
		}

		b := loaded.StepExecutions["B"]
		if b.PendingReason != workflow.PendingReasonRecovered {
			t.Errorf("B pending reason = %q, want RECOVERED",
				b.PendingReason)
		}
	})
}

func TestSaveDefinitionIsInsertOnly(t *testing.T) {
	forEachStore(t, func(t *testing.T, s WorkflowStore) {
		ctx := context.Background()
		def := testDefinition()

		if err := s.SaveDefinition(ctx, def); err != nil {
			t.Fatalf("first save: %v", err)
		}

		if err := s.SaveDefinition(ctx, def); !errors.Is(err, ErrDefinitionExists) {
			t.Fatalf("second save error = %v, want ErrDefinitionExists", err)
		}

		changed := testDefinition()
		changed.Steps[0].Type = "different"
		if err := s.SaveDefinition(ctx, changed); !errors.Is(err, ErrDefinitionExists) {
			t.Fatalf("changed save error = %v, want ErrDefinitionExists", err)
		}

		loaded, err := s.GetDefinition(ctx, def.ID, def.Version)
		if err != nil {
			t.Fatalf("get definition: %v", err)
		}
		if !reflect.DeepEqual(loaded, def) {
			t.Fatalf("stored definition was overwritten: got %+v want %+v",
				loaded, def)
		}
	})
}

func TestMemoryStoreDeepCopiesDefinitions(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStore()

	def := testDefinition()
	def.Steps[0].Config = map[string]string{"a": "1"}

	if err := m.SaveDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	first, err := m.GetDefinition(ctx, def.ID, def.Version)
	if err != nil {
		t.Fatal(err)
	}

	first.Steps[0].ID = "MUTATED"
	first.Steps[0].Config["a"] = "MUTATED"
	first.Steps[1].Inputs["value"] = workflow.InputRef{
		Source: "MUTATED",
		Key:    "MUTATED",
	}

	second, err := m.GetDefinition(ctx, def.ID, def.Version)
	if err != nil {
		t.Fatal(err)
	}
	if second.Steps[0].ID != "A" {
		t.Errorf("step ID = %q, want A", second.Steps[0].ID)
	}
	if second.Steps[0].Config["a"] != "1" {
		t.Errorf("config a = %q, want 1", second.Steps[0].Config["a"])
	}
	if second.Steps[1].Inputs["value"].Source != "A" {
		t.Errorf("input source = %q, want A",
			second.Steps[1].Inputs["value"].Source)
	}
}

func TestMemoryStoreRejectsDuplicateRun(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStore()
	run := seedRun(t, ctx, m, "duplicate-run", workflow.RunPending, nil)

	err := m.CreateRun(ctx, testDefinition(), run)
	if !errors.Is(err, ErrRunExists) {
		t.Fatalf("error = %v, want ErrRunExists", err)
	}

	loaded, err := m.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != workflow.RunPending {
		t.Errorf("stored run was replaced: status = %s", loaded.Status)
	}
}

func TestMemoryStoreSaveExecutionStateOnlyUpdatesTarget(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStore()
	run := seedRun(t, ctx, m, "partial-update", workflow.RunPending, nil)

	// The caller's copy claims both steps succeeded, but only B is being
	// persisted. Stored A must remain PENDING, matching SQLite semantics.
	run.StepExecutions["A"].Status = workflow.StepSucceeded
	run.StepExecutions["A"].Output = map[string]any{"result": float64(1)}
	run.StepExecutions["B"].Status = workflow.StepSucceeded
	run.StepExecutions["B"].Output = map[string]any{"result": float64(2)}
	run.Status = workflow.RunRunning

	if err := m.SaveExecutionState(ctx, run, "B"); err != nil {
		t.Fatal(err)
	}

	loaded, err := m.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.StepExecutions["A"].Status != workflow.StepPending {
		t.Errorf("A status = %s, want PENDING",
			loaded.StepExecutions["A"].Status)
	}
	if loaded.StepExecutions["B"].Status != workflow.StepSucceeded {
		t.Errorf("B status = %s, want SUCCEEDED",
			loaded.StepExecutions["B"].Status)
	}
}

func TestCachedStoreWritesPersistOnlyAndFillOnMiss(t *testing.T) {
	ctx := context.Background()
	s := newCachedTestStore(t)
	def := testDefinition()

	if err := s.SaveDefinition(ctx, def); err != nil {
		t.Fatalf("save definition: %v", err)
	}
	if _, err := s.cache.GetDefinition(ctx, def.ID, def.Version); !errors.Is(err, ErrNotFound) {
		t.Fatalf("definition cache must be empty after write, err = %v", err)
	}

	loadedDef, err := s.GetDefinition(ctx, def.ID, def.Version)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loadedDef, def) {
		t.Fatalf("loaded definition mismatch")
	}
	if _, err := s.cache.GetDefinition(ctx, def.ID, def.Version); err != nil {
		t.Fatalf("definition cache must be filled on miss: %v", err)
	}

	run := seedRun(t, ctx, s, "run-cache-policy", workflow.RunRunning, nil)
	if _, err := s.cache.GetRun(ctx, run.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("run cache must be empty after CreateRun, err = %v", err)
	}

	if _, err := s.GetRun(ctx, run.ID); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if _, err := s.cache.GetRun(ctx, run.ID); err != nil {
		t.Fatalf("run cache must be filled on miss: %v", err)
	}

	execA := run.StepExecutions["A"]
	execA.Status = workflow.StepSucceeded
	if err := s.SaveExecutionState(ctx, run, "A"); err != nil {
		t.Fatalf("save state: %v", err)
	}
	if _, err := s.cache.GetRun(ctx, run.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("run cache must be invalidated after write, err = %v", err)
	}
}

func TestCachedStoreCreateRunWithColdCache(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "workflow.db")
	def := testDefinition()

	seed, err := NewCachedStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.SaveDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	// Fresh instance with an empty cache must be able to create a run.
	c, err := NewCachedStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	run := seedRun(t, ctx, c, "run-cold-cache", workflow.RunPending, nil)
	if _, err := c.GetRun(ctx, run.ID); err != nil {
		t.Fatalf("get run with cold cache: %v", err)
	}
}

func TestSaveDefinitionRejectsInvalidDefinition(t *testing.T) {
	forEachStore(t, func(t *testing.T, s WorkflowStore) {
		ctx := context.Background()
		invalid := workflow.WorkflowDefinition{
			ID:      "invalid",
			Version: 1,
		}

		err := s.SaveDefinition(ctx, invalid)
		if !errors.Is(err, ErrInvalidDefinition) {
			t.Fatalf("error = %v, want ErrInvalidDefinition", err)
		}
	})
}
