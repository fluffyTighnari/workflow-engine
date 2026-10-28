package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

func TestRunSurvivesDatabaseReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "workflow.db")
	def := testDefinition()

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	run := seedRun(t, ctx, s, "run-reopen", workflow.RunPending,
		map[string]any{"value": 42})
	runID := run.ID

	// Close and reopen the database.
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	loadedDef, err := s2.GetDefinition(ctx, def.ID, def.Version)
	if err != nil {
		t.Fatal(err)
	}
	if len(loadedDef.Steps) != 2 {
		t.Fatalf("loaded %d steps, want 2", len(loadedDef.Steps))
	}

	loadedRun, err := s2.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	if loadedRun.ID != runID {
		t.Fatalf("run ID = %q, want %q", loadedRun.ID, runID)
	}

	if loadedRun.Input["value"] != float64(42) {
		t.Fatalf("input value = %v, want 42", loadedRun.Input["value"])
	}

	if len(loadedRun.StepExecutions) != 2 {
		t.Fatalf("loaded %d step executions, want 2",
			len(loadedRun.StepExecutions))
	}

	if loadedRun.StepExecutions["A"].Status != workflow.StepPending {
		t.Fatal("step A should remain pending")
	}
}

func TestOpenMigratesExistingSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "workflow.db")

	// Create the pre-extension schema by hand.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	oldSchema := []string{
		`CREATE TABLE workflow_versions (
			workflow_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			definition_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (workflow_id, version)
		)`,
		`CREATE TABLE workflow_runs (
			id TEXT PRIMARY KEY,
			workflow_id TEXT NOT NULL,
			workflow_version INTEGER NOT NULL,
			status TEXT NOT NULL,
			input_json TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE step_executions (
			run_id TEXT NOT NULL,
			step_id TEXT NOT NULL,
			status TEXT NOT NULL,
			input_json TEXT,
			output_json TEXT,
			error TEXT NOT NULL DEFAULT '',
			started_at TEXT,
			finished_at TEXT,
			PRIMARY KEY (run_id, step_id)
		)`,
	}
	for _, statement := range oldSchema {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create old schema: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// The new columns must be usable after migration.
	run := seedRun(t, ctx, s, "run-migrated", workflow.RunPending, nil)
	run.StepExecutions["A"].Status = workflow.StepFailed
	run.StepExecutions["A"].Failure = &workflow.Failure{
		Kind:    workflow.FailureKindInput,
		Message: "migrated",
	}
	run.StepExecutions["A"].PendingReason = workflow.PendingReasonNone
	run.Status = workflow.RunFailed
	run.Failure = &workflow.Failure{
		Kind:    workflow.FailureKindInput,
		Message: "migrated",
		StepID:  "A",
	}

	if err := s.SaveExecutionState(ctx, run, "A"); err != nil {
		t.Fatalf("save after migration: %v", err)
	}

	loaded, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Failure == nil ||
		loaded.Failure.Kind != workflow.FailureKindInput {
		t.Errorf("run failure = %+v, want INPUT", loaded.Failure)
	}
	if loaded.StepExecutions["A"].Failure == nil ||
		loaded.StepExecutions["A"].Failure.Kind != workflow.FailureKindInput {
		t.Errorf("step failure = %+v, want INPUT",
			loaded.StepExecutions["A"].Failure)
	}
}
