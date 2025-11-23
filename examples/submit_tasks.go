package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// TaskRequest represents a task submission request
type TaskRequest struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Priority   int                    `json:"priority"`
	Payload    map[string]interface{} `json:"payload"`
	Timeout    int                    `json:"timeout"`
	MaxRetries int                    `json:"max_retries"`
}

func main() {
	schedulerURL := "http://localhost:8001"

	// Submit various types of tasks
	tasks := []TaskRequest{
		{
			Name:     "batch-processing-job",
			Type:     "batch",
			Priority: 5,
			Payload: map[string]interface{}{
				"input_file":  "data/input.csv",
				"output_file": "data/output.csv",
				"operation":   "aggregate",
				"duration":    3,
			},
			Timeout:    300,
			MaxRetries: 3,
		},
		{
			Name:     "etl-pipeline",
			Type:     "etl",
			Priority: 7,
			Payload: map[string]interface{}{
				"source":      "postgres://db1",
				"destination": "s3://bucket/data",
				"table":       "users",
			},
			Timeout:    600,
			MaxRetries: 2,
		},
		{
			Name:     "ml-training",
			Type:     "ml",
			Priority: 9,
			Payload: map[string]interface{}{
				"model":    "random_forest",
				"dataset":  "training_data.parquet",
				"epochs":   100,
				"duration": 5,
			},
			Timeout:    1800,
			MaxRetries: 1,
		},
		{
			Name:     "daily-report",
			Type:     "report",
			Priority: 3,
			Payload: map[string]interface{}{
				"report_type": "daily_summary",
				"format":      "pdf",
				"recipients":  []string{"admin@example.com"},
			},
			Timeout:    120,
			MaxRetries: 3,
		},
		{
			Name:     "data-stream-processing",
			Type:     "stream",
			Priority: 6,
			Payload: map[string]interface{}{
				"stream_id": "events-stream",
				"window":    "5m",
				"duration":  2,
			},
			Timeout:    180,
			MaxRetries: 2,
		},
	}

	fmt.Println("🚀 Submitting tasks to the distributed scheduler...")
	fmt.Println()

	for i, task := range tasks {
		taskID, err := submitTask(schedulerURL, task)
		if err != nil {
			log.Printf("❌ Failed to submit task %d: %v\n", i+1, err)
			continue
		}

		fmt.Printf("✅ Task %d submitted successfully\n", i+1)
		fmt.Printf("   ID: %s\n", taskID)
		fmt.Printf("   Name: %s\n", task.Name)
		fmt.Printf("   Type: %s\n", task.Type)
		fmt.Printf("   Priority: %d\n", task.Priority)
		fmt.Println()

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("📊 Fetching task statistics...")
	time.Sleep(2 * time.Second)

	stats, err := getStats(schedulerURL)
	if err != nil {
		log.Printf("Failed to get stats: %v\n", err)
	} else {
		fmt.Printf("\nScheduler Statistics:\n")
		fmt.Printf("  Total Tasks: %v\n", stats["total"])
		fmt.Printf("  Success Rate: %.1f%%\n", stats["success_rate"])
		fmt.Printf("  By Status: %v\n", stats["by_status"])
		fmt.Println()
	}

	fmt.Println("👷 Fetching worker information...")
	workers, err := getWorkers(schedulerURL)
	if err != nil {
		log.Printf("Failed to get workers: %v\n", err)
	} else {
		fmt.Printf("\nActive Workers: %d\n", len(workers))
		for i, worker := range workers {
			fmt.Printf("  Worker %d:\n", i+1)
			fmt.Printf("    ID: %s\n", worker["id"])
			fmt.Printf("    Status: %s\n", worker["status"])
			fmt.Printf("    Tasks: %v/%v\n", worker["current_tasks"], worker["max_concurrency"])
			fmt.Printf("    Health: %.1f\n", worker["health_score"])
		}
	}

	fmt.Println("\n✨ Example completed! Visit http://localhost:8001 for the dashboard")
}

func submitTask(baseURL string, task TaskRequest) (string, error) {
	data, err := json.Marshal(task)
	if err != nil {
		return "", err
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/api/v1/tasks", baseURL),
		"application/json",
		bytes.NewBuffer(data),
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("server returned %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	taskID, ok := result["task_id"].(string)
	if !ok {
		return "", fmt.Errorf("invalid response format")
	}

	return taskID, nil
}

func getStats(baseURL string) (map[string]interface{}, error) {
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/stats", baseURL))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var stats map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, err
	}

	return stats, nil
}

func getWorkers(baseURL string) ([]map[string]interface{}, error) {
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/workers", baseURL))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	workers, ok := result["workers"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid workers format")
	}

	var workerList []map[string]interface{}
	for _, w := range workers {
		if worker, ok := w.(map[string]interface{}); ok {
			workerList = append(workerList, worker)
		}
	}

	return workerList, nil
}
