package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ============================================================
// DAG Validation — Ensures workflows have no cycles
// ============================================================

// ValidateDAG checks that the step dependencies form a valid DAG (no cycles)
// and that all referenced dependencies actually exist.
func ValidateDAG(steps []StepInput) error {
	// Build lookup map
	stepIDs := make(map[string]bool)
	for _, s := range steps {
		if stepIDs[s.ID] {
			return fmt.Errorf("duplicate step ID: %s", s.ID)
		}
		stepIDs[s.ID] = true
	}

	// Verify all dependencies reference existing steps
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if !stepIDs[dep] {
				return fmt.Errorf("step '%s' depends on unknown step '%s'", s.ID, dep)
			}
			if dep == s.ID {
				return fmt.Errorf("step '%s' cannot depend on itself", s.ID)
			}
		}
	}

	// Cycle detection using DFS with coloring
	// WHITE (0) = unvisited, GRAY (1) = in current path, BLACK (2) = fully processed
	const (
		white = 0
		gray  = 1
		black = 2
	)

	// Build adjacency list (dependency → dependents)
	adj := make(map[string][]string)
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			adj[dep] = append(adj[dep], s.ID)
		}
	}

	color := make(map[string]int)
	for _, s := range steps {
		color[s.ID] = white
	}

	var hasCycle func(node string) bool
	hasCycle = func(node string) bool {
		color[node] = gray
		for _, neighbor := range adj[node] {
			if color[neighbor] == gray {
				return true // Back edge = cycle
			}
			if color[neighbor] == white && hasCycle(neighbor) {
				return true
			}
		}
		color[node] = black
		return false
	}

	for _, s := range steps {
		if color[s.ID] == white {
			if hasCycle(s.ID) {
				return fmt.Errorf("circular dependency detected in workflow steps")
			}
		}
	}

	return nil
}

// ============================================================
// Find Ready Steps — DAG resolution at runtime
// ============================================================

// FindReadySteps returns all steps that are PENDING and whose dependencies
// have ALL been completed. These are the steps ready to be enqueued.
func FindReadySteps(steps []WorkflowStep) []WorkflowStep {
	// Build a set of completed step IDs
	completed := make(map[string]bool)
	for _, s := range steps {
		if s.Status == StatusCompleted {
			completed[s.StepID] = true
		}
	}

	var ready []WorkflowStep
	for _, s := range steps {
		if s.Status != StatusPending {
			continue
		}

		// Check if ALL dependencies are completed
		allDepsCompleted := true
		for _, dep := range s.DependsOn {
			if !completed[dep] {
				allDepsCompleted = false
				break
			}
		}

		if allDepsCompleted {
			ready = append(ready, s)
		}
	}

	return ready
}

// ============================================================
// Advance Workflow — The core orchestration loop
// ============================================================

// AdvanceWorkflow is the main DAG orchestration function. It is called after
// every step completion or failure to determine what should happen next:
//   - Enqueue newly-ready steps (fan-out)
//   - Mark the workflow COMPLETED if all steps are done
//   - Mark the workflow FAILED if a critical step has permanently failed
func AdvanceWorkflow(ctx context.Context, db *pgxpool.Pool, rdb *redis.Client, workflowID string) error {
	logger := slog.Default().With("component", "workflow_engine", "workflow_id", workflowID)

	// 1. Load all steps for this workflow
	steps, err := loadWorkflowSteps(ctx, db, workflowID)
	if err != nil {
		return fmt.Errorf("failed to load workflow steps: %w", err)
	}

	// 2. Count statuses
	var totalSteps, completedCount, failedCount, deadLetterCount int
	totalSteps = len(steps)
	for _, s := range steps {
		switch s.Status {
		case StatusCompleted:
			completedCount++
		case StatusFailed, StatusDeadLetter:
			if s.Status == StatusDeadLetter {
				deadLetterCount++
			}
			failedCount++
		}
	}

	// 3. Update workflow counters
	_, err = db.Exec(ctx,
		`UPDATE workflows SET completed_steps = $1, failed_steps = $2, updated_at = NOW() WHERE id = $3`,
		completedCount, deadLetterCount, workflowID,
	)
	if err != nil {
		logger.Error("failed to update workflow counters", "error", err)
	}

	// 4. Check if workflow is fully complete
	if completedCount == totalSteps {
		logger.Info("workflow completed — all steps finished",
			"total_steps", totalSteps,
		)
		_, err = db.Exec(ctx,
			`UPDATE workflows SET status = $1, updated_at = NOW() WHERE id = $2`,
			WorkflowStatusCompleted, workflowID,
		)
		return err
	}

	// 5. Check if workflow has failed (a dead-lettered step blocks downstream)
	if deadLetterCount > 0 {
		// Check if the dead-lettered step blocks any pending steps
		blocked := hasBlockedSteps(steps)
		if blocked {
			logger.Error("workflow failed — dead-lettered step blocks downstream steps",
				"dead_letter_count", deadLetterCount,
			)
			_, err = db.Exec(ctx,
				`UPDATE workflows SET status = $1, updated_at = NOW() WHERE id = $2`,
				WorkflowStatusFailed, workflowID,
			)
			return err
		}
	}

	// 6. Find and enqueue newly-ready steps (the fan-out moment)
	readySteps := FindReadySteps(steps)

	if len(readySteps) > 0 {
		logger.Info("enqueuing ready steps",
			"count", len(readySteps),
			"steps", stepIDs(readySteps),
		)
	}

	// Load workflow payload to pass to each step
	var workflowPayload json.RawMessage
	err = db.QueryRow(ctx, `SELECT payload FROM workflows WHERE id = $1`, workflowID).Scan(&workflowPayload)
	if err != nil {
		logger.Error("failed to load workflow payload", "error", err)
		workflowPayload = json.RawMessage("{}")
	}

	for _, step := range readySteps {
		// Mark step as QUEUED
		_, err := db.Exec(ctx,
			`UPDATE workflow_steps SET status = $1 WHERE id = $2`,
			StatusQueued, step.ID,
		)
		if err != nil {
			logger.Error("failed to mark step as queued", "step_id", step.StepID, "error", err)
			continue
		}

		// Build task message
		msg := TaskMessage{
			WorkflowID: workflowID,
			StepID:     step.StepID,
			Type:       step.Type,
			Config:     step.Config,
			Payload:    workflowPayload,
			Attempt:    0,
			MaxRetries: step.MaxRetries,
		}

		data, err := json.Marshal(msg)
		if err != nil {
			logger.Error("failed to marshal task message", "step_id", step.StepID, "error", err)
			continue
		}

		// Push to typed queue
		queue := QueueName(step.Type)
		if err := rdb.LPush(ctx, queue, string(data)).Err(); err != nil {
			logger.Error("failed to enqueue step", "step_id", step.StepID, "queue", queue, "error", err)
			continue
		}

		logger.Info("step enqueued",
			"step_id", step.StepID,
			"type", step.Type,
			"queue", queue,
		)
	}

	return nil
}

// ============================================================
// Helper Functions
// ============================================================

// loadWorkflowSteps loads all steps for a workflow from the database.
func loadWorkflowSteps(ctx context.Context, db *pgxpool.Pool, workflowID string) ([]WorkflowStep, error) {
	rows, err := db.Query(ctx, `
		SELECT id, workflow_id, step_id, type, config, depends_on, status, 
		       result, error, attempt, max_retries, started_at, completed_at, created_at
		FROM workflow_steps
		WHERE workflow_id = $1
		ORDER BY created_at
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []WorkflowStep
	for rows.Next() {
		var s WorkflowStep
		err := rows.Scan(
			&s.ID, &s.WorkflowID, &s.StepID, &s.Type, &s.Config,
			&s.DependsOn, &s.Status, &s.Result, &s.Error,
			&s.Attempt, &s.MaxRetries, &s.StartedAt, &s.CompletedAt, &s.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow step: %w", err)
		}
		steps = append(steps, s)
	}

	return steps, rows.Err()
}

// hasBlockedSteps checks if any PENDING steps depend on a DEAD_LETTER step.
func hasBlockedSteps(steps []WorkflowStep) bool {
	// Collect dead-lettered step IDs
	deadSteps := make(map[string]bool)
	for _, s := range steps {
		if s.Status == StatusDeadLetter {
			deadSteps[s.StepID] = true
		}
	}

	// Check if any pending step depends on a dead step
	for _, s := range steps {
		if s.Status == StatusPending {
			for _, dep := range s.DependsOn {
				if deadSteps[dep] {
					return true
				}
			}
		}
	}
	return false
}

// stepIDs extracts step IDs from a slice for logging.
func stepIDs(steps []WorkflowStep) []string {
	ids := make([]string, len(steps))
	for i, s := range steps {
		ids[i] = s.StepID
	}
	return ids
}
