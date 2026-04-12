package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ============================================================
// TaskExecutor Interface — All executors implement this
// ============================================================

// TaskExecutor is the interface that all executor implementations must satisfy.
type TaskExecutor interface {
	// Type returns the executor type string (e.g., "email", "webhook", "pdf").
	Type() string

	// Execute processes a task and returns a result or error.
	Execute(ctx context.Context, msg TaskMessage) (*ExecutionResult, error)
}

// ============================================================
// Executor Registry — Maps task types to executor implementations
// ============================================================

// ExecutorRegistry holds all registered executor implementations.
type ExecutorRegistry struct {
	mu        sync.RWMutex
	executors map[string]TaskExecutor
}

// NewExecutorRegistry creates a new empty registry.
func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{
		executors: make(map[string]TaskExecutor),
	}
}

// Register adds an executor to the registry.
func (r *ExecutorRegistry) Register(e TaskExecutor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executors[e.Type()] = e
	slog.Info("registered executor", "type", e.Type())
}

// Get retrieves an executor by type.
func (r *ExecutorRegistry) Get(taskType string) (TaskExecutor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.executors[taskType]
	return e, ok
}

// Types returns all registered executor type names.
func (r *ExecutorRegistry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.executors))
	for t := range r.executors {
		types = append(types, t)
	}
	return types
}

// ============================================================
// Worker Pool — Manages concurrent workers per executor type
// ============================================================

// WorkerPool manages a pool of workers that consume tasks from Redis queues.
type WorkerPool struct {
	registry *ExecutorRegistry
	db       *pgxpool.Pool
	rdb      *redis.Client
	config   *Config
	logger   *slog.Logger
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(registry *ExecutorRegistry, db *pgxpool.Pool, rdb *redis.Client, config *Config) *WorkerPool {
	return &WorkerPool{
		registry: registry,
		db:       db,
		rdb:      rdb,
		config:   config,
		logger:   slog.Default().With("component", "worker_pool"),
	}
}

// Start launches worker goroutines for each registered executor type.
func (wp *WorkerPool) Start(ctx context.Context) {
	for _, execType := range wp.registry.Types() {
		concurrency, ok := wp.config.WorkerConcurrency[execType]
		if !ok {
			concurrency = 1
		}

		for i := 0; i < concurrency; i++ {
			workerID := fmt.Sprintf("%s-worker-%d", execType, i)
			go wp.runWorker(ctx, execType, workerID)
		}

		wp.logger.Info("started workers",
			"executor_type", execType,
			"concurrency", concurrency,
		)
	}

	// Also start a DLQ monitor goroutine
	go wp.monitorDLQ(ctx)
}

// QueueName returns the Redis queue name for an executor type.
func QueueName(execType string) string {
	return fmt.Sprintf("queue:%s", execType)
}

// runWorker is the main loop for a single worker goroutine.
func (wp *WorkerPool) runWorker(ctx context.Context, execType string, workerID string) {
	logger := wp.logger.With("worker_id", workerID, "executor_type", execType)
	queue := QueueName(execType)

	logger.Info("worker started, listening on queue", "queue", queue)

	for {
		select {
		case <-ctx.Done():
			logger.Info("worker shutting down")
			return
		default:
		}

		// BRPOP blocks until a task is available (5s timeout to check ctx cancellation)
		result, err := wp.rdb.BRPop(ctx, 5*time.Second, queue).Result()
		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue // timeout or shutdown
			}
			logger.Error("error reading from queue", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// result[0] = queue name, result[1] = task message JSON
		var msg TaskMessage
		if err := json.Unmarshal([]byte(result[1]), &msg); err != nil {
			logger.Error("failed to unmarshal task message", "error", err, "raw", result[1])
			continue
		}

		wp.executeTask(ctx, logger, msg)
	}
}

// executeTask handles a single task with retry logic and audit logging.
func (wp *WorkerPool) executeTask(ctx context.Context, logger *slog.Logger, msg TaskMessage) {
	executor, ok := wp.registry.Get(msg.Type)
	if !ok {
		logger.Error("no executor registered for task type", "type", msg.Type)
		return
	}

	taskLogger := logger.With(
		"task_type", msg.Type,
		"workflow_id", msg.WorkflowID,
		"step_id", msg.StepID,
		"job_id", msg.JobID,
		"attempt", msg.Attempt+1,
	)

	taskLogger.Info("executing task")

	// Mark step/job as RUNNING in the database
	wp.updateTaskStatus(ctx, msg, StatusRunning, "", nil)

	// Execute
	start := time.Now()
	result, err := executor.Execute(ctx, msg)
	duration := time.Since(start)

	if err != nil {
		wp.handleTaskFailure(ctx, taskLogger, msg, err, duration)
		return
	}

	wp.handleTaskSuccess(ctx, taskLogger, msg, result, duration)
}

// handleTaskSuccess processes a successful task execution.
func (wp *WorkerPool) handleTaskSuccess(ctx context.Context, logger *slog.Logger, msg TaskMessage, result *ExecutionResult, duration time.Duration) {
	logger.Info("task completed successfully", "duration_ms", duration.Milliseconds())

	// Update status to COMPLETED
	wp.updateTaskStatus(ctx, msg, StatusCompleted, "", result)

	// Log to execution history
	wp.logExecution(ctx, msg, StatusCompleted, msg.Attempt+1, duration, "")

	// If this is a workflow step, advance the workflow
	if msg.WorkflowID != "" {
		if err := AdvanceWorkflow(ctx, wp.db, wp.rdb, msg.WorkflowID); err != nil {
			logger.Error("failed to advance workflow", "error", err)
		}
	}
}

// handleTaskFailure processes a failed task execution with retry logic.
func (wp *WorkerPool) handleTaskFailure(ctx context.Context, logger *slog.Logger, msg TaskMessage, execErr error, duration time.Duration) {
	msg.Attempt++
	errMsg := execErr.Error()

	logger.Warn("task execution failed",
		"error", errMsg,
		"attempt", msg.Attempt,
		"max_retries", msg.MaxRetries,
		"duration_ms", duration.Milliseconds(),
	)

	// Log this attempt to execution history
	wp.logExecution(ctx, msg, StatusFailed, msg.Attempt, duration, errMsg)

	if msg.Attempt < msg.MaxRetries {
		// Retry with exponential backoff
		delay := wp.retryDelay(msg.Attempt)
		logger.Info("scheduling retry", "delay", delay, "next_attempt", msg.Attempt+1)

		wp.updateTaskStatus(ctx, msg, StatusRetryWait, errMsg, nil)

		// Re-enqueue after delay
		go func() {
			time.Sleep(delay)
			wp.enqueueTask(ctx, msg)
		}()
	} else {
		// Max retries exhausted — send to Dead Letter Queue
		logger.Error("max retries exhausted, sending to DLQ",
			"total_attempts", msg.Attempt,
		)
		wp.updateTaskStatus(ctx, msg, StatusDeadLetter, errMsg, nil)
		wp.sendToDLQ(ctx, msg, errMsg)

		// If workflow step, check if we should fail the workflow
		if msg.WorkflowID != "" {
			if err := AdvanceWorkflow(ctx, wp.db, wp.rdb, msg.WorkflowID); err != nil {
				logger.Error("failed to advance workflow after DLQ", "error", err)
			}
		}
	}
}

// retryDelay calculates exponential backoff delay.
func (wp *WorkerPool) retryDelay(attempt int) time.Duration {
	base := wp.config.RetryBaseDelay
	return base * time.Duration(1<<uint(attempt-1)) // 1s → 2s → 4s → 8s
}

// enqueueTask pushes a task message back to its typed queue.
func (wp *WorkerPool) enqueueTask(ctx context.Context, msg TaskMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		wp.logger.Error("failed to marshal task for re-enqueue", "error", err)
		return
	}
	queue := QueueName(msg.Type)
	wp.rdb.LPush(ctx, queue, string(data))
}

// sendToDLQ pushes a failed task to the dead letter queue.
func (wp *WorkerPool) sendToDLQ(ctx context.Context, msg TaskMessage, errMsg string) {
	dlqEntry := map[string]interface{}{
		"task":       msg,
		"error":      errMsg,
		"failed_at":  time.Now().UTC().Format(time.RFC3339),
		"attempts":   msg.Attempt,
	}

	data, err := json.Marshal(dlqEntry)
	if err != nil {
		wp.logger.Error("failed to marshal DLQ entry", "error", err)
		return
	}

	wp.rdb.LPush(ctx, "queue:dead_letter", string(data))
	wp.logger.Info("task sent to dead letter queue",
		"workflow_id", msg.WorkflowID,
		"step_id", msg.StepID,
		"job_id", msg.JobID,
	)
}

// monitorDLQ periodically logs the depth of the dead letter queue.
func (wp *WorkerPool) monitorDLQ(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depth, err := wp.rdb.LLen(ctx, "queue:dead_letter").Result()
			if err != nil {
				continue
			}
			if depth > 0 {
				wp.logger.Warn("dead letter queue has entries", "depth", depth)
			}
		}
	}
}

// updateTaskStatus updates the status of a workflow step or standalone job in the database.
func (wp *WorkerPool) updateTaskStatus(ctx context.Context, msg TaskMessage, status string, errMsg string, result *ExecutionResult) {
	now := time.Now()

	if msg.WorkflowID != "" && msg.StepID != "" {
		// Update workflow step
		var resultJSON []byte
		if result != nil && result.Output != nil {
			resultJSON = result.Output
		}

		query := `
			UPDATE workflow_steps 
			SET status = $1, error = $2, result = $3, attempt = $4,
				started_at = COALESCE(started_at, $5),
				completed_at = CASE WHEN $1 IN ('COMPLETED', 'FAILED', 'DEAD_LETTER') THEN $5 ELSE completed_at END
			WHERE workflow_id = $6 AND step_id = $7`

		_, err := wp.db.Exec(ctx, query, status, nullIfEmpty(errMsg), resultJSON, msg.Attempt, now, msg.WorkflowID, msg.StepID)
		if err != nil {
			wp.logger.Error("failed to update workflow step status", "error", err)
		}
	} else if msg.JobID != "" {
		// Update standalone job
		query := `
			UPDATE jobs 
			SET status = $1, error = $2, attempt = $3, updated_at = $4
			WHERE id = $5`

		_, err := wp.db.Exec(ctx, query, status, nullIfEmpty(errMsg), msg.Attempt, now, msg.JobID)
		if err != nil {
			wp.logger.Error("failed to update job status", "error", err)
		}
	}
}

// logExecution writes an entry to the execution_history table.
func (wp *WorkerPool) logExecution(ctx context.Context, msg TaskMessage, status string, attempt int, duration time.Duration, errMsg string) {
	query := `
		INSERT INTO execution_history (job_id, workflow_id, step_id, executor_type, status, attempt, duration_ms, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := wp.db.Exec(ctx, query,
		nullIfEmpty(msg.JobID),
		nullIfEmpty(msg.WorkflowID),
		msg.StepID,
		msg.Type,
		status,
		attempt,
		duration.Milliseconds(),
		nullIfEmpty(errMsg),
	)
	if err != nil {
		wp.logger.Error("failed to log execution", "error", err)
	}
}

// nullIfEmpty returns nil if the string is empty, otherwise the string pointer.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
