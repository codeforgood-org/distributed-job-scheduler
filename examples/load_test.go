package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	schedulerURL   = flag.String("url", "http://localhost:8001", "Scheduler URL")
	numTasks       = flag.Int("tasks", 1000, "Number of tasks to submit")
	concurrency    = flag.Int("concurrency", 10, "Concurrent workers")
	taskType       = flag.String("type", "batch", "Task type")
	priority       = flag.Int("priority", 5, "Task priority")
	taskDuration   = flag.Int("duration", 2, "Task duration in seconds")
)

type LoadTestTask struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Priority   int                    `json:"priority"`
	Payload    map[string]interface{} `json:"payload"`
	Timeout    int                    `json:"timeout"`
	MaxRetries int                    `json:"max_retries"`
}

type LoadTestResult struct {
	TotalTasks      int
	SubmittedTasks  int32
	FailedTasks     int32
	SuccessRate     float64
	TotalDuration   time.Duration
	TasksPerSecond  float64
	AvgLatency      time.Duration
}

func main() {
	flag.Parse()

	fmt.Printf("🚀 Load Testing Distributed Scheduler\n")
	fmt.Printf("   URL: %s\n", *schedulerURL)
	fmt.Printf("   Tasks: %d\n", *numTasks)
	fmt.Printf("   Concurrency: %d\n", *concurrency)
	fmt.Printf("   Task Type: %s\n", *taskType)
	fmt.Printf("   Priority: %d\n", *priority)
	fmt.Println()

	result := runLoadTest()

	fmt.Println("\n📊 Load Test Results:")
	fmt.Printf("   Total Tasks: %d\n", result.TotalTasks)
	fmt.Printf("   Submitted: %d\n", result.SubmittedTasks)
	fmt.Printf("   Failed: %d\n", result.FailedTasks)
	fmt.Printf("   Success Rate: %.2f%%\n", result.SuccessRate)
	fmt.Printf("   Total Duration: %v\n", result.TotalDuration)
	fmt.Printf("   Tasks/Second: %.2f\n", result.TasksPerSecond)
	fmt.Printf("   Avg Latency: %v\n", result.AvgLatency)
}

func runLoadTest() LoadTestResult {
	var submitted, failed int32
	var totalLatency int64

	startTime := time.Now()

	// Create task channel
	taskCh := make(chan int, *numTasks)
	for i := 0; i < *numTasks; i++ {
		taskCh <- i
	}
	close(taskCh)

	// Create worker pool
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for taskNum := range taskCh {
				task := LoadTestTask{
					Name:     fmt.Sprintf("load-test-task-%d", taskNum),
					Type:     *taskType,
					Priority: *priority,
					Payload: map[string]interface{}{
						"task_number": taskNum,
						"duration":    *taskDuration,
						"timestamp":   time.Now().Unix(),
					},
					Timeout:    300,
					MaxRetries: 1,
				}

				taskStart := time.Now()
				if err := submitTask(*schedulerURL, task); err != nil {
					atomic.AddInt32(&failed, 1)
					log.Printf("Worker %d: Failed to submit task %d: %v\n", workerID, taskNum, err)
				} else {
					atomic.AddInt32(&submitted, 1)
					latency := time.Since(taskStart)
					atomic.AddInt64(&totalLatency, int64(latency))

					if taskNum%100 == 0 {
						fmt.Printf("✓ Submitted %d tasks...\n", taskNum)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	successRate := float64(submitted) / float64(*numTasks) * 100
	tasksPerSecond := float64(submitted) / duration.Seconds()
	avgLatency := time.Duration(0)
	if submitted > 0 {
		avgLatency = time.Duration(totalLatency / int64(submitted))
	}

	return LoadTestResult{
		TotalTasks:     *numTasks,
		SubmittedTasks: submitted,
		FailedTasks:    failed,
		SuccessRate:    successRate,
		TotalDuration:  duration,
		TasksPerSecond: tasksPerSecond,
		AvgLatency:     avgLatency,
	}
}

func submitTask(baseURL string, task LoadTestTask) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/api/v1/tasks", baseURL),
		"application/json",
		bytes.NewBuffer(data),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}

	return nil
}
