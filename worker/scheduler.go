package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var dbPool *pgxpool.Pool
var redisClient *redis.Client

func initConnections() {
	// 1. Connect Postgres
	dbUrl := "postgres://postgres:Thedoctor%4078@localhost:5432/task_scheduler?sslmode=disable"
	var err error
	dbPool, err = pgxpool.New(context.Background(), dbUrl)
	if err != nil {
		log.Fatal("DB Connect Error:", err)
	}

	// 2. Connect Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatal("Redis Connect Error:", err)
	}

	fmt.Println("✅ Scheduler connected to Postgres & Redis!")
}

// pollDatabase runs every minute
func pollDatabase() {
	ctx := context.Background()
	currentMinute := time.Now().Unix() / 60

	fmt.Printf("\n⏰ Waking up for Minute: %d\n", currentMinute)

	// Loop through all 10 possible segments
	for segment := 1; segment <= 10; segment++ {
		
		// 1. Try to acquire a Distributed Lock for this segment for this specific minute
		lockKey := fmt.Sprintf("lock:segment:%d:minute:%d", segment, currentMinute)
		
		// SETNX: Only succeeds if the key doesn't exist. Lock expires in 50 seconds.
		acquired := redisClient.SetNX(ctx, lockKey, "locked", 50*time.Second).Val()

		if !acquired {
			// Another worker already locked this segment. Skip it!
			continue 
		}

		fmt.Printf("🔒 Claimed Segment %d. Polling DB...\n", segment)

		// 2. Query Postgres for tasks in this segment
		query := `
			SELECT id, job_id 
			FROM task_schedule 
			WHERE execution_time_minute = $1 AND segment = $2 AND status = 'SCHEDULED'`

		rows, err := dbPool.Query(ctx, query, currentMinute, segment)
		if err != nil {
			fmt.Println("Query error:", err)
			continue
		}

		// 3. Push found tasks to Redis Message Queue
		taskCount := 0
		for rows.Next() {
			var scheduleID, jobID string
			rows.Scan(&scheduleID, &jobID)

			// Push to a Redis List (our message queue)
			redisClient.LPush(ctx, "task_queue", jobID)
			
			// Mark as queued in Postgres so we don't pick it up again
			dbPool.Exec(ctx, "UPDATE task_schedule SET status = 'QUEUED' WHERE id = $1", scheduleID)
			
			taskCount++
		}
		rows.Close()

		if taskCount > 0 {
			fmt.Printf("   🚀 Pushed %d tasks from Segment %d to the queue!\n", taskCount, segment)
		}
	}
}

func main() {
	initConnections()
	defer dbPool.Close()

	// This is the Go version of setInterval
	// We tick exactly at the start of every minute (or close to it)
	ticker := time.NewTicker(10 * time.Second) // Ticking every 10s for testing (change to 60s for prod)
	defer ticker.Stop()

	fmt.Println("⏳ Dispatcher is running and waiting for tasks...")

	for {
		<-ticker.C // Wait for the next tick
		pollDatabase()
	}
}