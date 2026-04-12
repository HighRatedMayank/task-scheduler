package internal

import (
	"encoding/json"
	"time"
)

// ============================================================
// Workflow Status Constants
// ============================================================

const (
	WorkflowStatusRunning   = "RUNNING"
	WorkflowStatusCompleted = "COMPLETED"
	WorkflowStatusFailed    = "FAILED"
)

// ============================================================
// Step / Job Status Constants (mirrors the job_status enum in DB)
// ============================================================

const (
	StatusPending    = "PENDING"
	StatusScheduled  = "SCHEDULED"
	StatusQueued     = "QUEUED"
	StatusRunning    = "RUNNING"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"
	StatusRetryWait  = "RETRY_WAIT"
	StatusDeadLetter = "DEAD_LETTER"
)

// ============================================================
// Workflow: A collection of steps forming a DAG
// ============================================================

type Workflow struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Payload        json.RawMessage `json:"payload"`
	Status         string    `json:"status"`
	TotalSteps     int       `json:"total_steps"`
	CompletedSteps int       `json:"completed_steps"`
	FailedSteps    int       `json:"failed_steps"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// ============================================================
// WorkflowStep: A single node in the DAG
// ============================================================

type WorkflowStep struct {
	ID          string          `json:"id"`
	WorkflowID  string          `json:"workflow_id"`
	StepID      string          `json:"step_id"`
	Type        string          `json:"type"`
	Config      json.RawMessage `json:"config"`
	DependsOn   []string        `json:"depends_on"`
	Status      string          `json:"status"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	Attempt     int             `json:"attempt"`
	MaxRetries  int             `json:"max_retries"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

// ============================================================
// Job: Standalone single-step job (backward compatible)
// ============================================================

type Job struct {
	ID         string          `json:"id"`
	UserID     string          `json:"user_id,omitempty"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Status     string          `json:"status"`
	Attempt    int             `json:"attempt"`
	MaxRetries int             `json:"max_retries"`
	Error      string          `json:"error,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// ============================================================
// ExecutionRecord: Audit log entry for each execution attempt
// ============================================================

type ExecutionRecord struct {
	ID           string          `json:"id"`
	JobID        *string         `json:"job_id,omitempty"`
	WorkflowID   *string         `json:"workflow_id,omitempty"`
	StepID       string          `json:"step_id,omitempty"`
	ExecutorType string          `json:"executor_type"`
	Status       string          `json:"status"`
	Attempt      int             `json:"attempt"`
	DurationMs   int64           `json:"duration_ms"`
	Error        string          `json:"error,omitempty"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

// ============================================================
// TaskMessage: Message format passed through Redis queues
// ============================================================

type TaskMessage struct {
	// Source identification (one of these will be set)
	WorkflowID string `json:"workflow_id,omitempty"`
	StepID     string `json:"step_id,omitempty"`
	JobID      string `json:"job_id,omitempty"`

	// Execution details
	Type    string          `json:"type"`
	Config  json.RawMessage `json:"config"`
	Payload json.RawMessage `json:"payload"`

	// Retry tracking
	Attempt    int `json:"attempt"`
	MaxRetries int `json:"max_retries"`
}

// ============================================================
// API Request/Response Types
// ============================================================

// WorkflowSubmission is the API request body for creating a workflow.
type WorkflowSubmission struct {
	Name    string          `json:"name" binding:"required"`
	Payload json.RawMessage `json:"payload"`
	Steps   []StepInput     `json:"steps" binding:"required,min=1"`
}

// StepInput represents a single step in a workflow submission.
type StepInput struct {
	ID        string          `json:"id" binding:"required"`
	Type      string          `json:"type" binding:"required"`
	Config    json.RawMessage `json:"config"`
	DependsOn []string        `json:"depends_on"`
}

// JobSubmission is the API request body for creating a standalone job.
type JobSubmission struct {
	UserID       string          `json:"user_id"`
	Type         string          `json:"type" binding:"required"`
	Payload      json.RawMessage `json:"payload" binding:"required"`
	MaxRetries   int             `json:"max_retries"`
	DelayMinutes int             `json:"delay_minutes"`
}

// ExecutionResult is what executors return after processing a task.
type ExecutionResult struct {
	Output   json.RawMessage `json:"output,omitempty"`
	Duration time.Duration   `json:"-"`
}
