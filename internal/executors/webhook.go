package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"task-scheduler/internal"
)

// ============================================================
// Webhook Executor — Simulates HTTP webhook calls
// ============================================================

// WebhookExecutor handles webhook/HTTP POST tasks.
type WebhookExecutor struct {
	logger *slog.Logger
}

// NewWebhookExecutor creates a new WebhookExecutor.
func NewWebhookExecutor() *WebhookExecutor {
	return &WebhookExecutor{
		logger: slog.Default().With("executor", "webhook"),
	}
}

// Type returns the executor type identifier.
func (w *WebhookExecutor) Type() string {
	return "webhook"
}

// webhookConfig is the expected configuration for webhook tasks.
type webhookConfig struct {
	URL      string            `json:"url"`
	Method   string            `json:"method"`
	Headers  map[string]string `json:"headers"`
	Provider string            `json:"provider"`
}

// Execute simulates making an HTTP webhook call.
func (w *WebhookExecutor) Execute(ctx context.Context, msg internal.TaskMessage) (*internal.ExecutionResult, error) {
	var cfg webhookConfig
	if err := json.Unmarshal(msg.Config, &cfg); err != nil {
		return nil, fmt.Errorf("invalid webhook config: %w", err)
	}

	method := cfg.Method
	if method == "" {
		method = "POST"
	}

	w.logger.Info("executing webhook",
		"url", cfg.URL,
		"method", method,
		"provider", cfg.Provider,
		"step_id", msg.StepID,
	)

	// Simulate network latency (100-500ms)
	latency := time.Duration(100+rand.Intn(400)) * time.Millisecond
	time.Sleep(latency)

	// Simulate ~10% failure rate
	if rand.Float64() < 0.10 {
		statusCode := []int{500, 502, 503, 429}[rand.Intn(4)]
		return nil, fmt.Errorf("webhook returned HTTP %d from %s", statusCode, cfg.URL)
	}

	// Simulate successful response
	responseID := fmt.Sprintf("resp_%d", time.Now().UnixNano())
	statusCode := 200

	w.logger.Info("webhook completed",
		"url", cfg.URL,
		"status_code", statusCode,
		"response_id", responseID,
		"latency_ms", latency.Milliseconds(),
	)

	output, _ := json.Marshal(map[string]interface{}{
		"status_code": statusCode,
		"response_id": responseID,
		"url":         cfg.URL,
		"provider":    cfg.Provider,
		"latency_ms":  latency.Milliseconds(),
		"called_at":   time.Now().UTC().Format(time.RFC3339),
	})

	return &internal.ExecutionResult{
		Output:   output,
		Duration: latency,
	}, nil
}
