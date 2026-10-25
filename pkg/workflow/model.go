package workflow

import "time"

// WorkflowDefinition describes a reusable workflow template.
type WorkflowDefinition struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
	Steps   []Step `json:"steps"`
}

type InputRef struct {
	// Source is "workflow_input" or a dependency step ID.
	Source string `json:"source"`

	// Key identifies the value inside the source map.
	Key string `json:"key"`
}

// Step describes one step in a workflow definition.
type Step struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Config    map[string]string `json:"config,omitempty"`
	DependsOn []string          `json:"depends_on,omitempty"`

	// Inputs maps executor input names to their data sources.
	Inputs map[string]InputRef `json:"inputs,omitempty"`
}

// RunStatus represents the lifecycle of a workflow run.
type RunStatus string

const (
	RunPending   RunStatus = "PENDING"
	RunRunning   RunStatus = "RUNNING"
	RunSucceeded RunStatus = "SUCCEEDED"
	RunFailed    RunStatus = "FAILED"
)

// StepStatus represents the lifecycle of a step execution.
type StepStatus string

const (
	StepPending   StepStatus = "PENDING"
	StepRunning   StepStatus = "RUNNING"
	StepSucceeded StepStatus = "SUCCEEDED"
	StepFailed    StepStatus = "FAILED"
)

// WorkflowRun represents one execution of a workflow definition.
type WorkflowRun struct {
	ID              string                    `json:"id"`
	WorkflowID      string                    `json:"workflow_id"`
	WorkflowVersion int                       `json:"workflow_version"`
	Status          RunStatus                 `json:"status"`
	Input           map[string]any            `json:"input,omitempty"`
	StepExecutions  map[string]*StepExecution `json:"step_executions"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

// StepExecution represents one step within a specific workflow run.
type StepExecution struct {
	StepID     string         `json:"step_id"`
	Status     StepStatus     `json:"status"`
	Input      map[string]any `json:"input,omitempty"`
	Output     map[string]any `json:"output,omitempty"`
	Error      string         `json:"error,omitempty"`
	StartedAt  *time.Time     `json:"started_at,omitempty"`
	FinishedAt *time.Time     `json:"finished_at,omitempty"`
}
