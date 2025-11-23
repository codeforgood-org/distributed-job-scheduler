package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	grpcclient "github.com/codeforgood-org/distributed-job-scheduler/pkg/grpc"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/worker"
	"go.uber.org/zap"
)

var (
	workerID       = flag.String("worker-id", "", "Unique worker ID")
	schedulerAddr  = flag.String("scheduler", "localhost:7001", "Scheduler gRPC address")
	maxConcurrency = flag.Int("max-concurrency", 0, "Max concurrent tasks (0 = CPU count)")
	capabilities   = flag.String("capabilities", "batch,stream,report,etl,ml", "Comma-separated task types")
	logLevel       = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	logFormat      = flag.String("log-format", "json", "Log format (json, console)")
)

func main() {
	flag.Parse()

	// Initialize logger
	if err := logger.InitLogger(*logLevel, *logFormat); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Generate worker ID if not provided
	if *workerID == "" {
		hostname, _ := os.Hostname()
		*workerID = fmt.Sprintf("worker-%s-%d", hostname, os.Getpid())
	}

	logger.Info("Starting Worker Node",
		zap.String("worker_id", *workerID),
		zap.String("scheduler", *schedulerAddr),
		zap.String("version", "1.0.0"))

	// Set max concurrency
	if *maxConcurrency == 0 {
		*maxConcurrency = runtime.NumCPU()
	}

	// Parse capabilities
	caps := parseCapabilities(*capabilities)

	// Create worker configuration
	config := &worker.Config{
		WorkerID:          *workerID,
		MaxConcurrency:    *maxConcurrency,
		HeartbeatInterval: 10 * time.Second,
		Capabilities:      caps,
		Metadata: map[string]string{
			"version": "1.0.0",
			"os":      runtime.GOOS,
			"arch":    runtime.GOARCH,
		},
	}

	// Create custom executor with handlers for different task types
	executor := worker.NewCustomExecutor()

	// Register batch task handler
	executor.RegisterHandler(models.TaskTypeBatch, func(ctx context.Context, task *models.Task) (map[string]interface{}, error) {
		logger.Info("Processing batch task", zap.String("task_id", task.ID))

		// Simulate batch processing
		duration := 3 * time.Second
		if d, ok := task.Payload["duration"].(float64); ok {
			duration = time.Duration(d) * time.Second
		}

		select {
		case <-time.After(duration):
			return map[string]interface{}{
				"status":      "completed",
				"type":        "batch",
				"records":     1000,
				"duration_ms": duration.Milliseconds(),
			}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	// Register ETL task handler
	executor.RegisterHandler(models.TaskTypeETL, func(ctx context.Context, task *models.Task) (map[string]interface{}, error) {
		logger.Info("Processing ETL task", zap.String("task_id", task.ID))

		// Simulate ETL processing
		select {
		case <-time.After(2 * time.Second):
			return map[string]interface{}{
				"status":        "completed",
				"type":          "etl",
				"rows_extracted": 5000,
				"rows_transformed": 4950,
				"rows_loaded":   4950,
			}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	// Register ML task handler
	executor.RegisterHandler(models.TaskTypeML, func(ctx context.Context, task *models.Task) (map[string]interface{}, error) {
		logger.Info("Processing ML task", zap.String("task_id", task.ID))

		// Simulate ML processing
		select {
		case <-time.After(5 * time.Second):
			return map[string]interface{}{
				"status":   "completed",
				"type":     "ml",
				"model":    "random_forest",
				"accuracy": 0.95,
				"samples":  10000,
			}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	// Register report task handler
	executor.RegisterHandler(models.TaskTypeReport, func(ctx context.Context, task *models.Task) (map[string]interface{}, error) {
		logger.Info("Processing report task", zap.String("task_id", task.ID))

		select {
		case <-time.After(1 * time.Second):
			return map[string]interface{}{
				"status": "completed",
				"type":   "report",
				"format": "pdf",
				"pages":  25,
			}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	// Use default executor for other types
	defaultExecutor := &worker.DefaultExecutor{}
	for _, cap := range caps {
		if cap != models.TaskTypeBatch && cap != models.TaskTypeETL &&
			cap != models.TaskTypeML && cap != models.TaskTypeReport {
			executor.RegisterHandler(cap, defaultExecutor.Execute)
		}
	}

	// Create worker
	w := worker.NewWorker(config, *schedulerAddr, executor)

	// Start worker
	if err := w.Start(); err != nil {
		logger.Fatal("Failed to start worker", zap.Error(err))
	}
	defer w.Stop()

	// Connect to scheduler
	schedulerClient, err := grpcclient.NewSchedulerClient(*schedulerAddr)
	if err != nil {
		logger.Fatal("Failed to connect to scheduler", zap.Error(err))
	}
	defer schedulerClient.Close()

	// Register with scheduler
	workerModel := models.NewWorker(*workerID, *schedulerAddr, caps, *maxConcurrency)
	if err := schedulerClient.RegisterWorker(workerModel); err != nil {
		logger.Fatal("Failed to register with scheduler", zap.Error(err))
	}

	// Start heartbeat goroutine
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			health := w.GetHealth()
			if err := schedulerClient.Heartbeat(
				*workerID,
				health["active_tasks"].(int),
				health["cpu_usage"].(float64),
				health["memory_usage"].(float64),
			); err != nil {
				logger.Error("Heartbeat failed", zap.Error(err))
			}
		}
	}()

	logger.Info("Worker started successfully",
		zap.String("worker_id", *workerID),
		zap.Int("max_concurrency", *maxConcurrency))

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down gracefully...")

	// Deregister from scheduler
	if err := schedulerClient.DeregisterWorker(*workerID, "shutdown"); err != nil {
		logger.Error("Failed to deregister", zap.Error(err))
	}
}

func parseCapabilities(caps string) []models.TaskType {
	// Default capabilities
	return []models.TaskType{
		models.TaskTypeBatch,
		models.TaskTypeStream,
		models.TaskTypeReport,
		models.TaskTypeETL,
		models.TaskTypeML,
		models.TaskTypeCustom,
	}
}
