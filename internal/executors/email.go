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
// Email Executor — Simulates sending emails
// ============================================================

// EmailExecutor handles email sending tasks.
type EmailExecutor struct {
	logger *slog.Logger
}

// NewEmailExecutor creates a new EmailExecutor.
func NewEmailExecutor() *EmailExecutor {
	return &EmailExecutor{
		logger: slog.Default().With("executor", "email"),
	}
}

// Type returns the executor type identifier.
func (e *EmailExecutor) Type() string {
	return "email"
}

// emailConfig is the expected configuration for email tasks.
type emailConfig struct {
	To       string `json:"to"`
	Subject  string `json:"subject"`
	Template string `json:"template"`
	Body     string `json:"body"`
}

// Execute simulates sending an email with realistic logging.
func (e *EmailExecutor) Execute(ctx context.Context, msg internal.TaskMessage) (*internal.ExecutionResult, error) {
	var cfg emailConfig
	if err := json.Unmarshal(msg.Config, &cfg); err != nil {
		return nil, fmt.Errorf("invalid email config: %w", err)
	}

	// Derive recipient from config or payload
	recipient := cfg.To
	if recipient == "" {
		var payload map[string]interface{}
		if err := json.Unmarshal(msg.Payload, &payload); err == nil {
			if email, ok := payload["customer_email"].(string); ok {
				recipient = email
			}
		}
	}

	e.logger.Info("sending email",
		"to", recipient,
		"subject", cfg.Subject,
		"template", cfg.Template,
	)

	// Simulate email sending time (200-800ms)
	processingTime := time.Duration(200+rand.Intn(600)) * time.Millisecond
	time.Sleep(processingTime)

	// Simulate ~10% failure rate for testing retry/DLQ
	if rand.Float64() < 0.10 {
		return nil, fmt.Errorf("SMTP connection refused: timeout after %v", processingTime)
	}

	messageID := fmt.Sprintf("msg_%d_%s", time.Now().UnixNano(), randString(8))

	e.logger.Info("email sent successfully",
		"to", recipient,
		"message_id", messageID,
		"duration_ms", processingTime.Milliseconds(),
	)

	output, _ := json.Marshal(map[string]interface{}{
		"message_id": messageID,
		"recipient":  recipient,
		"template":   cfg.Template,
		"sent_at":    time.Now().UTC().Format(time.RFC3339),
	})

	return &internal.ExecutionResult{
		Output:   output,
		Duration: processingTime,
	}, nil
}

// randString generates a random alphanumeric string.
func randString(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}
