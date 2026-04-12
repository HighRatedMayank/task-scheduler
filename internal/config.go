package internal

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	// Database
	DatabaseURL string
	RedisAddr   string

	// API Server
	APIPort string

	// Scheduler
	SchedulerTickSeconds int

	// Worker pool: concurrency per executor type
	WorkerConcurrency map[string]int

	// Retry
	MaxRetries     int
	RetryBaseDelay time.Duration
}

// LoadConfig reads configuration from environment variables with sensible defaults.
func LoadConfig() *Config {
	cfg := &Config{
		DatabaseURL:          getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/task_scheduler?sslmode=disable"),
		RedisAddr:            getEnv("REDIS_ADDR", "localhost:6379"),
		APIPort:              getEnv("API_PORT", "8080"),
		SchedulerTickSeconds: getEnvInt("SCHEDULER_TICK_SECONDS", 10),
		MaxRetries:           getEnvInt("MAX_RETRIES", 3),
		RetryBaseDelay:       time.Duration(getEnvInt("RETRY_BASE_DELAY_MS", 1000)) * time.Millisecond,
	}

	// Worker concurrency per executor type
	cfg.WorkerConcurrency = map[string]int{
		"email":   getEnvInt("WORKER_EMAIL_CONCURRENCY", 3),
		"webhook": getEnvInt("WORKER_WEBHOOK_CONCURRENCY", 5),
		"pdf":     getEnvInt("WORKER_PDF_CONCURRENCY", 2),
	}

	return cfg
}

// getEnv reads an env var or returns a default value.
func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

// getEnvInt reads an env var as int or returns a default.
func getEnvInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}
