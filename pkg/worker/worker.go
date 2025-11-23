package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/metrics"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Worker executes tasks assigned by the scheduler
type Worker struct {
	id             string
	config         *Config
	schedulerAddr  string

	// Task execution
	executor       TaskExecutor
	activeTasks    map[string]*TaskExecution
	mu             sync.RWMutex

	// State
	ctx            context.Context
	cancel         context.CancelFunc
	running        bool

	// Metrics
	totalTasks     int64
	successTasks   int64
	failedTasks    int64
}

// Config holds worker configuration
type Config struct {
	WorkerID       string
	MaxConcurrency int
	HeartbeatInterval time.Duration
	Capabilities   []models.TaskType
	Metadata       map[string]string
}

// TaskExecutor defines the interface for task execution
type TaskExecutor interface {
	Execute(ctx context.Context, task *models.Task) (map[string]interface{}, error)
}

// TaskExecution tracks a running task
type TaskExecution struct {
	Task      *models.Task
	StartTime time.Time
	Cancel    context.CancelFunc
	Done      chan struct{}
}

// NewWorker creates a new worker instance
func NewWorker(config *Config, schedulerAddr string, executor TaskExecutor) *Worker {
	if config.WorkerID == "" {
		config.WorkerID = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}

	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = runtime.NumCPU()
	}

	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 10 * time.Second
	}

	if len(config.Capabilities) == 0 {
		config.Capabilities = []models.TaskType{
			models.TaskTypeBatch,
			models.TaskTypeStream,
			models.TaskTypeReport,
			models.TaskTypeETL,
			models.TaskTypeML,
			models.TaskTypeCustom,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		id:            config.WorkerID,
		config:        config,
		schedulerAddr: schedulerAddr,
		executor:      executor,
		activeTasks:   make(map[string]*TaskExecution),
		ctx:           ctx,
		cancel:        cancel,
		running:       false,
	}
}

// Start starts the worker
func (w *Worker) Start() error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("worker already running")
	}
	w.running = true
	w.mu.Unlock()

	logger.Info("Starting worker",
		zap.String("worker_id", w.id),
		zap.String("scheduler", w.schedulerAddr))

	// Register with scheduler
	if err := w.register(); err != nil {
		return fmt.Errorf("failed to register with scheduler: %w", err)
	}

	// Start heartbeat
	go w.sendHeartbeats()

	logger.Info("Worker started successfully", zap.String("worker_id", w.id))
	return nil
}

// Stop stops the worker
func (w *Worker) Stop() error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	w.mu.Unlock()

	logger.Info("Stopping worker", zap.String("worker_id", w.id))

	// Cancel all active tasks
	w.mu.RLock()
	for _, execution := range w.activeTasks {
		execution.Cancel()
	}
	w.mu.RUnlock()

	// Deregister from scheduler
	if err := w.deregister(); err != nil {
		logger.Error("Failed to deregister", zap.Error(err))
	}

	w.cancel()

	logger.Info("Worker stopped", zap.String("worker_id", w.id))
	return nil
}

// ExecuteTask executes a task
func (w *Worker) ExecuteTask(task *models.Task) error {
	w.mu.Lock()
	if len(w.activeTasks) >= w.config.MaxConcurrency {
		w.mu.Unlock()
		return fmt.Errorf("worker at max capacity")
	}
	w.mu.Unlock()

	// Check if worker can execute this task type
	canExecute := false
	for _, cap := range w.config.Capabilities {
		if cap == task.Type || cap == models.TaskTypeCustom {
			canExecute = true
			break
		}
	}

	if !canExecute {
		return fmt.Errorf("worker cannot execute task type: %s", task.Type)
	}

	// Create execution context
	ctx, cancel := context.WithTimeout(w.ctx, task.Timeout)
	execution := &TaskExecution{
		Task:      task,
		StartTime: time.Now(),
		Cancel:    cancel,
		Done:      make(chan struct{}),
	}

	w.mu.Lock()
	w.activeTasks[task.ID] = execution
	w.mu.Unlock()

	// Execute task in goroutine
	go w.runTask(ctx, execution)

	return nil
}

// runTask executes the task
func (w *Worker) runTask(ctx context.Context, execution *TaskExecution) {
	defer func() {
		close(execution.Done)
		w.mu.Lock()
		delete(w.activeTasks, execution.Task.ID)
		w.mu.Unlock()
	}()

	task := execution.Task
	logger.Info("Executing task",
		zap.String("task_id", task.ID),
		zap.String("task_name", task.Name),
		zap.String("type", string(task.Type)))

	// Update task status
	task.Status = models.TaskStatusRunning
	now := time.Now()
	task.StartedAt = &now

	// Execute task
	result, err := w.executor.Execute(ctx, task)

	duration := time.Since(execution.StartTime)

	if err != nil {
		// Task failed
		logger.Error("Task execution failed",
			zap.String("task_id", task.ID),
			zap.Error(err),
			zap.Duration("duration", duration))

		w.reportTaskFailure(task.ID, err.Error(), duration)
		w.failedTasks++
	} else {
		// Task succeeded
		logger.Info("Task completed successfully",
			zap.String("task_id", task.ID),
			zap.Duration("duration", duration))

		w.reportTaskSuccess(task.ID, result, duration)
		w.successTasks++
	}

	w.totalTasks++
}

// CancelTask cancels a running task
func (w *Worker) CancelTask(taskID string) error {
	w.mu.RLock()
	execution, exists := w.activeTasks[taskID]
	w.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	execution.Cancel()
	logger.Info("Task cancelled", zap.String("task_id", taskID))

	return nil
}

// GetHealth returns worker health metrics
func (w *Worker) GetHealth() map[string]interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var cpuUsage, memUsage float64
	// In production, you would get actual CPU/memory metrics
	cpuUsage = 0.0
	memUsage = 0.0

	return map[string]interface{}{
		"worker_id":      w.id,
		"active_tasks":   len(w.activeTasks),
		"total_tasks":    w.totalTasks,
		"success_tasks":  w.successTasks,
		"failed_tasks":   w.failedTasks,
		"cpu_usage":      cpuUsage,
		"memory_usage":   memUsage,
		"max_concurrency": w.config.MaxConcurrency,
	}
}

// register registers the worker with the scheduler
func (w *Worker) register() error {
	// Implementation would use gRPC to register with scheduler
	logger.Info("Registering with scheduler",
		zap.String("worker_id", w.id),
		zap.String("scheduler", w.schedulerAddr))

	// For now, just log
	return nil
}

// deregister deregisters the worker from the scheduler
func (w *Worker) deregister() error {
	// Implementation would use gRPC to deregister from scheduler
	logger.Info("Deregistering from scheduler", zap.String("worker_id", w.id))

	return nil
}

// sendHeartbeats sends periodic heartbeats to the scheduler
func (w *Worker) sendHeartbeats() {
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

// sendHeartbeat sends a single heartbeat
func (w *Worker) sendHeartbeat() {
	w.mu.RLock()
	activeTasks := len(w.activeTasks)
	w.mu.RUnlock()

	// Implementation would send heartbeat via gRPC
	logger.Debug("Sending heartbeat",
		zap.String("worker_id", w.id),
		zap.Int("active_tasks", activeTasks))
}

// reportTaskSuccess reports task success to scheduler
func (w *Worker) reportTaskSuccess(taskID string, result map[string]interface{}, duration time.Duration) {
	// Implementation would report via gRPC
	logger.Debug("Reporting task success",
		zap.String("task_id", taskID),
		zap.Duration("duration", duration))
}

// reportTaskFailure reports task failure to scheduler
func (w *Worker) reportTaskFailure(taskID string, errorMsg string, duration time.Duration) {
	// Implementation would report via gRPC
	logger.Debug("Reporting task failure",
		zap.String("task_id", taskID),
		zap.String("error", errorMsg))
}

// DefaultExecutor is a simple task executor implementation
type DefaultExecutor struct{}

// Execute executes a task
func (de *DefaultExecutor) Execute(ctx context.Context, task *models.Task) (map[string]interface{}, error) {
	logger.Info("Executing task with default executor",
		zap.String("task_id", task.ID),
		zap.String("type", string(task.Type)))

	// Simulate work based on task type
	workDuration := 2 * time.Second
	if timeout, ok := task.Payload["duration"]; ok {
		if d, ok := timeout.(float64); ok {
			workDuration = time.Duration(d) * time.Second
		}
	}

	select {
	case <-time.After(workDuration):
		// Task completed
		result := map[string]interface{}{
			"status":    "success",
			"processed": true,
			"timestamp": time.Now().Unix(),
		}

		// Copy some payload data to result
		if task.Payload != nil {
			result["input"] = task.Payload
		}

		return result, nil

	case <-ctx.Done():
		// Task cancelled or timeout
		return nil, ctx.Err()
	}
}

// CustomExecutor allows users to define custom task execution logic
type CustomExecutor struct {
	handlers map[models.TaskType]func(context.Context, *models.Task) (map[string]interface{}, error)
}

// NewCustomExecutor creates a custom executor
func NewCustomExecutor() *CustomExecutor {
	return &CustomExecutor{
		handlers: make(map[models.TaskType]func(context.Context, *models.Task) (map[string]interface{}, error)),
	}
}

// RegisterHandler registers a handler for a task type
func (ce *CustomExecutor) RegisterHandler(taskType models.TaskType, handler func(context.Context, *models.Task) (map[string]interface{}, error)) {
	ce.handlers[taskType] = handler
}

// Execute executes a task using registered handlers
func (ce *CustomExecutor) Execute(ctx context.Context, task *models.Task) (map[string]interface{}, error) {
	handler, exists := ce.handlers[task.Type]
	if !exists {
		return nil, fmt.Errorf("no handler registered for task type: %s", task.Type)
	}

	return handler(ctx, task)
}
