package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

var DB *pgxpool.Pool

// JobInput is what the client sends us
type JobInput struct {
	UserID        string `json:"user_id" binding:"required"`
	Type          string `json:"type" binding:"required"`
	Payload       any    `json:"payload" binding:"required"`
	DelayMinutes  int    `json:"delay_minutes"` // e.g., run in 5 minutes
	IsRecurring   bool   `json:"is_recurring"`
	MaxRetryCount int    `json:"max_retry_count"`
}

func ConnectDatabase() {
	// REPLACE 'YOUR_PASSWORD' WITH YOUR ACTUAL PASSWORD
	databaseUrl := "postgres://postgres:Thedoctor%4078@localhost:5432/task_scheduler?sslmode=disable"
	var err error
	DB, err = pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("🚀 Connected to Postgres using pgxpool!")
}

func createJob(c *gin.Context) {
	var input JobInput
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format"})
		return
	}

	// 1. Defaults (Run in 1 minute, retry 3 times)
	if input.MaxRetryCount == 0 {
		input.MaxRetryCount = 3
	}
	if input.DelayMinutes == 0 {
		input.DelayMinutes = 1
	}

	ctx := context.Background()

	// 2. The Core Logic from the Article
	// Calculate the execution time in UNIX minutes
	runAt := time.Now().Add(time.Duration(input.DelayMinutes) * time.Minute)
	executionMinute := runAt.Unix() / 60 

	// Assign a random segment (1 to 10) to distribute load across workers
	segment := rand.Intn(10) + 1 

	// 3. Start a Database Transaction
	tx, err := DB.Begin(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start transaction"})
		return
	}
	// Defer rollback: if the function exits before tx.Commit(), it rolls back safely
	defer tx.Rollback(ctx) 

	// 4. Insert into 'jobs' table
	var jobID string
	jobSQL := `
		INSERT INTO jobs (user_id, type, payload, is_recurring, max_retry_count) 
		VALUES ($1, $2, $3, $4, $5) 
		RETURNING job_id`

	err = tx.QueryRow(ctx, jobSQL, input.UserID, input.Type, input.Payload, input.IsRecurring, input.MaxRetryCount).Scan(&jobID)
	if err != nil {
		fmt.Println("Error inserting job:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save job"})
		return
	}

	// 5. Insert into 'task_schedule' table
	scheduleSQL := `
		INSERT INTO task_schedule (job_id, execution_time_minute, segment) 
		VALUES ($1, $2, $3)`
	
	_, err = tx.Exec(ctx, scheduleSQL, jobID, executionMinute, segment)
	if err != nil {
		fmt.Println("Error inserting schedule:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to schedule task"})
		return
	}

	// 6. Commit Transaction (Permanently save to DB)
	if err = tx.Commit(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to commit"})
		return
	}

	// 7. Return Success Response
	c.JSON(http.StatusCreated, gin.H{
		"message":          "Job scheduled successfully",
		"job_id":           jobID,
		"execution_minute": executionMinute,
		"assigned_segment": segment,
	})
}

func main() {
	ConnectDatabase()
	defer DB.Close()

	r := gin.Default()
	r.POST("/jobs", createJob)

	fmt.Println("Job Service running on port 8080...")
	r.Run(":8080")
}