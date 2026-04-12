package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ============================================================
// Scheduler — Polls database and dispatches tasks to Redis queues
// ============================================================

// Scheduler handles time-based task dispatch using segment-locked polling.
type Scheduler struct {
	db     *pgxpool.Pool
	rdb    *redis.Client
	config *Config
	logger *slog.Logger
}

// NewScheduler creates a new Scheduler.
func NewScheduler(db *pgxpool.Pool, rdb *redis.Client, config *Config) *Scheduler {
	return &Scheduler{
		db:     db,
		rdb:    rdb,
		config: config,
		logger: slog.Default().With("component", "scheduler"),
	}
}

// Start begins the scheduler loop, ticking at the configured interval.
func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.config.SchedulerTickSeconds) * time.Second)
	defer ticker.Stop()

	s.logger.Info("scheduler started",
		"tick_interval", fmt.Sprintf("%ds", s.config.SchedulerTickSeconds),
		"segments", 10,
	)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler shutting down")
			return
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

// poll runs one polling cycle — checks all segments for the current minute.
func (s *Scheduler) poll(ctx context.Context) {
	currentMinute := time.Now().Unix() / 60

	s.logger.Debug("polling cycle started", "execution_minute", currentMinute)

	totalDispatched := 0

	for segment := 1; segment <= 10; segment++ {
		// Try to acquire distributed lock for this segment+minute combo
		lockKey := fmt.Sprintf("lock:segment:%d:minute:%d", segment, currentMinute)
		acquired := s.rdb.SetNX(ctx, lockKey, "locked", 50*time.Second).Val()

		if !acquired {
			continue // Another scheduler instance claimed this segment
		}

		count := s.pollSegment(ctx, currentMinute, segment)
		totalDispatched += count
	}

	if totalDispatched > 0 {
		s.logger.Info("polling cycle complete",
			"execution_minute", currentMinute,
			"tasks_dispatched", totalDispatched,
		)
	}
}

// pollSegment queries and dispatches tasks for a specific segment.
func (s *Scheduler) pollSegment(ctx context.Context, executionMinute int64, segment int) int {
	// Query for scheduled tasks in this segment
	rows, err := s.db.Query(ctx, `
		SELECT id, job_id, workflow_step_id
		FROM task_schedule
		WHERE execution_time_minute = $1 AND segment = $2 AND status = 'SCHEDULED'
	`, executionMinute, segment)
	if err != nil {
		s.logger.Error("query error", "segment", segment, "error", err)
		return 0
	}
	defer rows.Close()

	dispatched := 0

	for rows.Next() {
		var scheduleID string
		var jobID, workflowStepID *string

		if err := rows.Scan(&scheduleID, &jobID, &workflowStepID); err != nil {
			s.logger.Error("scan error", "error", err)
			continue
		}

		var dispatched_ok bool

		if jobID != nil {
			dispatched_ok = s.dispatchJob(ctx, *jobID)
		} else if workflowStepID != nil {
			dispatched_ok = s.dispatchWorkflowStep(ctx, *workflowStepID)
		}

		if dispatched_ok {
			// Mark schedule entry as QUEUED
			s.db.Exec(ctx, `UPDATE task_schedule SET status = 'QUEUED' WHERE id = $1`, scheduleID)
			dispatched++
		}
	}

	if dispatched > 0 {
		s.logger.Info("segment dispatched",
			"segment", segment,
			"tasks", dispatched,
		)
	}

	return dispatched
}

// dispatchJob pushes a standalone job to its typed Redis queue.
func (s *Scheduler) dispatchJob(ctx context.Context, jobID string) bool {
	var job Job
	err := s.db.QueryRow(ctx, `
		SELECT id, type, payload, max_retries FROM jobs WHERE id = $1
	`, jobID).Scan(&job.ID, &job.Type, &job.Payload, &job.MaxRetries)
	if err != nil {
		s.logger.Error("failed to fetch job", "job_id", jobID, "error", err)
		return false
	}

	msg := TaskMessage{
		JobID:      job.ID,
		Type:       job.Type,
		Config:     job.Payload, // For standalone jobs, payload acts as config
		Payload:    job.Payload,
		Attempt:    0,
		MaxRetries: job.MaxRetries,
	}

	data, _ := json.Marshal(msg)
	queue := QueueName(job.Type)

	if err := s.rdb.LPush(ctx, queue, string(data)).Err(); err != nil {
		s.logger.Error("failed to enqueue job", "job_id", jobID, "queue", queue, "error", err)
		return false
	}

	// Update job status to QUEUED
	s.db.Exec(ctx, `UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2`, StatusQueued, jobID)

	s.logger.Info("job dispatched",
		"job_id", jobID,
		"type", job.Type,
		"queue", queue,
	)
	return true
}

// dispatchWorkflowStep pushes a workflow step to its typed Redis queue.
func (s *Scheduler) dispatchWorkflowStep(ctx context.Context, stepUUID string) bool {
	var step WorkflowStep
	var workflowPayload json.RawMessage

	err := s.db.QueryRow(ctx, `
		SELECT ws.id, ws.workflow_id, ws.step_id, ws.type, ws.config, ws.max_retries, w.payload
		FROM workflow_steps ws
		JOIN workflows w ON w.id = ws.workflow_id
		WHERE ws.id = $1
	`, stepUUID).Scan(&step.ID, &step.WorkflowID, &step.StepID, &step.Type, &step.Config, &step.MaxRetries, &workflowPayload)
	if err != nil {
		s.logger.Error("failed to fetch workflow step", "step_uuid", stepUUID, "error", err)
		return false
	}

	msg := TaskMessage{
		WorkflowID: step.WorkflowID,
		StepID:     step.StepID,
		Type:       step.Type,
		Config:     step.Config,
		Payload:    workflowPayload,
		Attempt:    0,
		MaxRetries: step.MaxRetries,
	}

	data, _ := json.Marshal(msg)
	queue := QueueName(step.Type)

	if err := s.rdb.LPush(ctx, queue, string(data)).Err(); err != nil {
		s.logger.Error("failed to enqueue step", "step_id", step.StepID, "queue", queue, "error", err)
		return false
	}

	// Update step status to QUEUED
	s.db.Exec(ctx, `UPDATE workflow_steps SET status = $1 WHERE id = $2`, StatusQueued, step.ID)

	s.logger.Info("workflow step dispatched",
		"workflow_id", step.WorkflowID,
		"step_id", step.StepID,
		"type", step.Type,
		"queue", queue,
	)
	return true
}
