package engine

import (
	"errors"
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/store"
)

// Sentinel errors classify every engine failure so callers can distinguish
// retryable infrastructure problems from terminal workflow failures.
//
// Errors returned by ExecuteStep, RunWorkflow, and RecoverRun wrap one or
// more of these sentinels (or the underlying error for store/executor
// calls) with fmt.Errorf("%w", ...). Use errors.Is to inspect them.
var (
	// Invalid state / caller errors. These never mutate the run.
	ErrRunTerminal       = errors.New("workflow run is already terminal")
	ErrStepNotFound      = errors.New("workflow step not found")
	ErrExecutionNotFound = errors.New("step execution record not found")
	ErrStepNotPending    = errors.New("step is not pending")
	ErrDependencyPending = errors.New("step dependency is not satisfied")

	// Configuration / infrastructure errors. These leave the run
	// retryable: the executor may be registered later, or the store may
	// recover.
	ErrExecutorNotFound = errors.New("step executor is not registered")
	ErrPersistence      = errors.New("workflow persistence failed")
	ErrStoreUnavailable = errors.New("workflow store is not configured")

	// Terminal execution failures. These are persisted as FAILED with a
	// FailureKind.
	ErrInputResolution   = errors.New("step input resolution failed")
	ErrExecutorFailure   = errors.New("step executor failed")
	ErrExecutionCanceled = errors.New("step execution canceled")
	ErrNoProgress        = errors.New("workflow cannot make progress")

	// Input-resolution sub-causes. ExecuteStep wraps these with
	// ErrInputResolution, so callers can inspect either the broad category
	// or the exact cause.
	ErrInputUnknownSource    = errors.New("step input references unknown source")
	ErrInputUnfinishedSource = errors.New("step input references unfinished source")
	ErrInputMissingKey       = errors.New("step input references missing key")
)

// IsRetryable reports whether err describes a condition that may succeed
// on a later attempt without changing the workflow definition or input.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrPersistence) ||
		errors.Is(err, ErrExecutorNotFound) ||
		errors.Is(err, ErrDependencyPending) ||
		errors.Is(err, ErrStoreUnavailable)
}

// IsTerminalFailure reports whether err is a workflow/executor failure
// that is persisted as FAILED rather than a transient engine error.
func IsTerminalFailure(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrInputResolution) ||
		errors.Is(err, ErrExecutorFailure) ||
		errors.Is(err, ErrExecutionCanceled) ||
		errors.Is(err, ErrNoProgress)
}

// IsInvalidState reports whether err means the caller asked the engine to
// perform an operation that is not valid for the current run/step state.
func IsInvalidState(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrRunTerminal) ||
		errors.Is(err, ErrStepNotFound) ||
		errors.Is(err, ErrExecutionNotFound) ||
		errors.Is(err, ErrStepNotPending)
}

// wrapStoreError classifies non-not-found store failures as persistence
// errors while preserving the original error (including store.ErrNotFound).
func wrapStoreError(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, store.ErrNotFound) ||
		errors.Is(err, store.ErrDefinitionExists) ||
		errors.Is(err, store.ErrRunExists) ||
		errors.Is(err, store.ErrInvalidDefinition) {
		return err
	}

	return fmt.Errorf("%w: %s: %w", ErrPersistence, op, err)
}
