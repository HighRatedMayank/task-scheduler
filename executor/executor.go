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

	fmt.Println("👷 Executor Worker connected to Postgres & Redis!")
}

func processTasks() {
	ctx := context.Background()

	for {
		// 1. Block and wait for a task in the Redis queue
		// BRPOP is brilliant: it pauses the code here FOREVER until a task appears. Zero CPU wasted.
		result, err := redisClient.BRPop(ctx, 0, "task_queue").Result()
		if err != nil {
			fmt.Println("Error reading from Redis:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// result[0] is the queue name, result[1] is the actual value (our job_id)
		jobID := result[1]
		fmt.Printf("\n📥 Grabbed Job ID %s from queue! Fetching details...\n", jobID)

		// 2. Fetch the heavy payload from the Postgres 'jobs' table
		var jobType string
		var payload []byte // JSONB comes back as a byte slice in Go

		err = dbPool.QueryRow(ctx, "SELECT type, payload FROM jobs WHERE job_id = $1", jobID).Scan(&jobType, &payload)
		if err != nil {
			fmt.Println("   ❌ Error fetching job blueprint:", err)
			continue
		}

		// 3. "Execute" the Job
		fmt.Printf("   ⚙️ Executing Task Type: [%s]\n", jobType)
		fmt.Printf("   📦 Payload: %s\n", string(payload))
		
		// Simulate hard work (like generating a PDF or sending an email)
		time.Sleep(2 * time.Second) 
		
		fmt.Println("   ✅ Task Executed Successfully!")

		// 4. Write the final grade to the Audit Log
		logSQL := `
			INSERT INTO task_execution_history (job_id, status) 
			VALUES ($1, 'COMPLETED')`
		
		_, err = dbPool.Exec(ctx, logSQL, jobID)
		if err != nil {
			fmt.Println("   ❌ Failed to save execution history:", err)
		} else {
			fmt.Println("   📝 Saved 'COMPLETED' to execution history.")
		}
	}
}

func main() {
	initConnections()
	defer dbPool.Close()

	fmt.Println("👀 Executor is staring at the Redis queue...")
	processTasks()
}