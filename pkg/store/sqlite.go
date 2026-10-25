package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"

	_ "modernc.org/sqlite"
)

// SQLiteStore persists workflow definitions, runs, and step executions
// in a single SQLite database file. It implements WorkflowStore.
type SQLiteStore struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path and runs migrations.
// It uses a single connection because SQLite serializes writes anyway.
func Open(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)

	s := &SQLiteStore{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}

	return s, nil
}

// Close releases the underlying database connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	statements := []string{
		`PRAGMA foreign_keys = ON`,

		`CREATE TABLE IF NOT EXISTS workflow_versions (
			workflow_id TEXT NOT NULL,
			version INTEGER NOT NULL CHECK(version > 0),
			definition_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (workflow_id, version)
		)`,

		`CREATE TABLE IF NOT EXISTS workflow_runs (
			id TEXT PRIMARY KEY,
			workflow_id TEXT NOT NULL,
			workflow_version INTEGER NOT NULL,
			status TEXT NOT NULL,
			input_json TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			FOREIGN KEY (workflow_id, workflow_version)
				REFERENCES workflow_versions(workflow_id, version)
		)`,

		`CREATE TABLE IF NOT EXISTS step_executions (
			run_id TEXT NOT NULL,
			step_id TEXT NOT NULL,
			status TEXT NOT NULL,
			input_json TEXT,
			output_json TEXT,
			error TEXT NOT NULL DEFAULT '',
			started_at TEXT,
			finished_at TEXT,
			PRIMARY KEY (run_id, step_id),
			FOREIGN KEY (run_id)
				REFERENCES workflow_runs(id)
				ON DELETE CASCADE
		)`,

		`CREATE INDEX IF NOT EXISTS idx_runs_status
			ON workflow_runs(status, created_at)`,
	}

	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate database: %w", err)
		}
	}

	return nil
}

// --- Definition ---

// SaveDefinition persists a workflow version. Saving the same
// workflow ID + version again overwrites the previous definition
// (idempotent upsert).
func (s *SQLiteStore) SaveDefinition(
	ctx context.Context,
	def workflow.WorkflowDefinition,
) error {
	data, err := marshalJSON(def)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO workflow_versions
			(workflow_id, version, definition_json, created_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (workflow_id, version) DO UPDATE
			SET definition_json = excluded.definition_json
	`,
		def.ID,
		def.Version,
		data,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("save workflow definition: %w", err)
	}

	return nil
}

// GetDefinition loads a previously saved workflow version.
func (s *SQLiteStore) GetDefinition(
	ctx context.Context,
	workflowID string,
	version int,
) (workflow.WorkflowDefinition, error) {
	var data string

	err := s.db.QueryRowContext(ctx, `
		SELECT definition_json
		FROM workflow_versions
		WHERE workflow_id = ? AND version = ?
	`, workflowID, version).Scan(&data)
	if err != nil {
		return workflow.WorkflowDefinition{}, err
	}

	var def workflow.WorkflowDefinition
	if err := json.Unmarshal([]byte(data), &def); err != nil {
		return workflow.WorkflowDefinition{}, fmt.Errorf(
			"decode workflow definition: %w",
			err,
		)
	}

	return def, nil
}

// --- Run lifecycle ---

// CreateRun inserts a new run and its step execution rows in one transaction.
// The referenced workflow version must already be saved.
func (s *SQLiteStore) CreateRun(
	ctx context.Context,
	def workflow.WorkflowDefinition,
	run *workflow.WorkflowRun,
) error {
	inputJSON, err := marshalJSON(run.Input)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create-run transaction: %w", err)
	}
	defer tx.Rollback()

	var exists int
	err = tx.QueryRowContext(ctx, `
		SELECT 1 FROM workflow_versions
		WHERE workflow_id = ? AND version = ?
	`, def.ID, def.Version).Scan(&exists)
	if err != nil {
		return fmt.Errorf("workflow version must be saved first: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO workflow_runs
			(id, workflow_id, workflow_version, status,
			 input_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		run.ID,
		run.WorkflowID,
		run.WorkflowVersion,
		run.Status,
		inputJSON,
		run.CreatedAt.UTC().Format(time.RFC3339Nano),
		run.UpdatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("insert workflow run: %w", err)
	}

	for _, execution := range run.StepExecutions {
		inputValue, err := nullableJSON(execution.Input)
		if err != nil {
			return err
		}

		outputValue, err := nullableJSON(execution.Output)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO step_executions
				(run_id, step_id, status, input_json,
				 output_json, error, started_at, finished_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`,
			run.ID,
			execution.StepID,
			execution.Status,
			inputValue,
			outputValue,
			execution.Error,
			nullableTime(execution.StartedAt),
			nullableTime(execution.FinishedAt),
		)
		if err != nil {
			return fmt.Errorf("insert step execution %q: %w",
				execution.StepID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create-run transaction: %w", err)
	}

	return nil
}

// GetRun loads a run and all of its step executions from the database.
func (s *SQLiteStore) GetRun(
	ctx context.Context,
	runID string,
) (*workflow.WorkflowRun, error) {
	var run workflow.WorkflowRun
	var inputJSON sql.NullString
	var createdAt, updatedAt string

	err := s.db.QueryRowContext(ctx, `
		SELECT id, workflow_id, workflow_version, status,
		       input_json, created_at, updated_at
		FROM workflow_runs
		WHERE id = ?
	`, runID).Scan(
		&run.ID,
		&run.WorkflowID,
		&run.WorkflowVersion,
		&run.Status,
		&inputJSON,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		return nil, err
	}

	if inputJSON.Valid {
		if err := json.Unmarshal([]byte(inputJSON.String), &run.Input); err != nil {
			return nil, err
		}
	}

	run.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return nil, err
	}

	run.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return nil, err
	}

	run.StepExecutions = make(map[string]*workflow.StepExecution)

	rows, err := s.db.QueryContext(ctx, `
		SELECT step_id, status, input_json, output_json,
		       error, started_at, finished_at
		FROM step_executions
		WHERE run_id = ?
		ORDER BY step_id
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var execution workflow.StepExecution
		var stepInputJSON, outputJSON sql.NullString
		var startedAt, finishedAt sql.NullString

		err := rows.Scan(
			&execution.StepID,
			&execution.Status,
			&stepInputJSON,
			&outputJSON,
			&execution.Error,
			&startedAt,
			&finishedAt,
		)
		if err != nil {
			return nil, err
		}

		if stepInputJSON.Valid {
			if err := json.Unmarshal(
				[]byte(stepInputJSON.String), &execution.Input,
			); err != nil {
				return nil, err
			}
		}

		if outputJSON.Valid {
			if err := json.Unmarshal(
				[]byte(outputJSON.String), &execution.Output,
			); err != nil {
				return nil, err
			}
		}

		if startedAt.Valid {
			t, err := time.Parse(time.RFC3339Nano, startedAt.String)
			if err != nil {
				return nil, err
			}
			execution.StartedAt = &t
		}

		if finishedAt.Valid {
			t, err := time.Parse(time.RFC3339Nano, finishedAt.String)
			if err != nil {
				return nil, err
			}
			execution.FinishedAt = &t
		}

		run.StepExecutions[execution.StepID] = &execution
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &run, nil
}

// ListRunsByStatus returns the IDs of runs currently in any of the given
// statuses. Used by startup recovery to re-enqueue unfinished runs.
func (s *SQLiteStore) ListRunsByStatus(
	ctx context.Context,
	statuses ...workflow.RunStatus,
) ([]string, error) {
	if len(statuses) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(statuses))
	args := make([]any, len(statuses))
	for i, st := range statuses {
		placeholders[i] = "?"
		args[i] = string(st)
	}

	query := "SELECT id FROM workflow_runs WHERE status IN (" +
		strings.Join(placeholders, ",") + ") ORDER BY created_at"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs by status: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

// --- Step transition ---

// SaveExecutionState persists the given step's state and the run's
// overall status in a single transaction.
func (s *SQLiteStore) SaveExecutionState(
	ctx context.Context,
	run *workflow.WorkflowRun,
	stepID string,
) error {
	if run == nil {
		return fmt.Errorf("run must not be nil")
	}

	execution, ok := run.StepExecutions[stepID]
	if !ok || execution == nil {
		return fmt.Errorf("step execution %q not found", stepID)
	}

	inputJSON, err := nullableJSON(execution.Input)
	if err != nil {
		return fmt.Errorf("marshal step input: %w", err)
	}

	outputJSON, err := nullableJSON(execution.Output)
	if err != nil {
		return fmt.Errorf("marshal step output: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Update the step row.
	result, err := tx.ExecContext(ctx, `
		UPDATE step_executions
		SET status = ?,
		    input_json = ?,
		    output_json = ?,
		    error = ?,
		    started_at = ?,
		    finished_at = ?
		WHERE run_id = ? AND step_id = ?
	`,
		execution.Status,
		inputJSON,
		outputJSON,
		execution.Error,
		nullableTime(execution.StartedAt),
		nullableTime(execution.FinishedAt),
		run.ID,
		stepID,
	)
	if err != nil {
		return fmt.Errorf("update step execution: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check step update: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf(
			"expected to update one step execution, updated %d",
			affected,
		)
	}

	// Update the run row.
	result, err = tx.ExecContext(ctx, `
		UPDATE workflow_runs
		SET status = ?,
		    updated_at = ?
		WHERE id = ?
	`,
		run.Status,
		run.UpdatedAt.Format(time.RFC3339Nano),
		run.ID,
	)
	if err != nil {
		return fmt.Errorf("update workflow run: %w", err)
	}

	affected, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check workflow run update: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf(
			"expected to update one workflow run, updated %d",
			affected,
		)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit execution state: %w", err)
	}

	return nil
}

// --- Internal helpers ---

func marshalJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal JSON: %w", err)
	}
	return string(data), nil
}

func nullableJSON(value map[string]any) (any, error) {
	if value == nil {
		return nil, nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal JSON: %w", err)
	}

	return string(data), nil
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}

	return value.UTC().Format(time.RFC3339Nano)
}
