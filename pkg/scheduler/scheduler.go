package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/consensus"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/metrics"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/queue"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/storage"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// Scheduler is the core scheduling engine
type Scheduler struct {
	nodeID  string
	raft    *consensus.RaftNode
	storage *storage.Storage
	queue   *queue.PriorityQueue
	workers *models.WorkerPool
	cron    *cron.Cron

	// Configuration
	config *Config

	// State
	mu              sync.RWMutex
	running         bool
	ctx             context.Context
	cancel          context.CancelFunc
	taskAssignments map[string]string // taskID -> workerID
	cronJobs        map[string]cron.EntryID

	// Channels
	taskCh   chan *models.Task
	workerCh chan *models.Worker
}

// Config holds scheduler configuration
type Config struct {
	TaskTimeout        time.Duration
	WorkerTimeout      time.Duration
	MaxRetries         int
	ScheduleInterval   time.Duration
	HealthCheckInterval time.Duration
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		TaskTimeout:        5 * time.Minute,
		WorkerTimeout:      30 * time.Second,
		MaxRetries:         3,
		ScheduleInterval:   1 * time.Second,
		HealthCheckInterval: 10 * time.Second,
	}
}

// NewScheduler creates a new scheduler instance
func NewScheduler(nodeID string, raftNode *consensus.RaftNode, store *storage.Storage, config *Config) *Scheduler {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		nodeID:          nodeID,
		raft:            raftNode,
		storage:         store,
		queue:           queue.NewPriorityQueue(),
		workers:         models.NewWorkerPool(),
		cron:            cron.New(cron.WithSeconds()),
		config:          config,
		ctx:             ctx,
		cancel:          cancel,
		taskAssignments: make(map[string]string),
		cronJobs:        make(map[string]cron.EntryID),
		taskCh:          make(chan *models.Task, 1000),
		workerCh:        make(chan *models.Worker, 100),
	}
}

// Start starts the scheduler
func (s *Scheduler) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("scheduler already running")
	}
	s.running = true
	s.mu.Unlock()

	logger.Info("Starting scheduler", zap.String("node_id", s.nodeID))

	// Load existing tasks from storage
	if err := s.loadTasks(); err != nil {
		return fmt.Errorf("failed to load tasks: %w", err)
	}

	// Load existing workers
	if err := s.loadWorkers(); err != nil {
		return fmt.Errorf("failed to load workers: %w", err)
	}

	// Start cron scheduler
	s.cron.Start()

	// Start background goroutines
	go s.scheduleTasks()
	go s.monitorWorkers()
	go s.retryFailedTasks()
	go s.processTaskChannel()

	logger.Info("Scheduler started successfully")
	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	logger.Info("Stopping scheduler", zap.String("node_id", s.nodeID))

	s.cancel()
	s.cron.Stop()

	logger.Info("Scheduler stopped")
	return nil
}

// SubmitTask submits a new task to the scheduler
func (s *Scheduler) SubmitTask(task *models.Task) error {
	// Validate task
	if task.Name == "" {
		return fmt.Errorf("task name is required")
	}

	// Set defaults
	if task.ID == "" {
		task = models.NewTask(task.Name, task.Type, task.Priority, task.Payload)
	}

	task.Status = models.TaskStatusPending
	task.NodeID = s.nodeID
	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now

	// Save to storage
	if err := s.storage.SaveTask(task); err != nil {
		return fmt.Errorf("failed to save task: %w", err)
	}

	// If it's a scheduled task, add to cron
	if task.Schedule != "" {
		if err := s.addCronTask(task); err != nil {
			logger.Error("Failed to add cron task", zap.Error(err), zap.String("task_id", task.ID))
		}
	} else {
		// Add to queue
		s.queue.Push(task)
		metrics.UpdateQueueDepth("pending", s.queue.Len())
	}

	logger.Info("Task submitted", zap.String("task_id", task.ID), zap.String("name", task.Name))
	metrics.TasksTotal.WithLabelValues(string(task.Status), string(task.Type), task.Namespace).Inc()

	return nil
}

// GetTask retrieves a task by ID
func (s *Scheduler) GetTask(taskID string) (*models.Task, error) {
	return s.storage.GetTask(taskID)
}

// ListTasks lists tasks with filtering
func (s *Scheduler) ListTasks(filter *models.TaskFilter) ([]*models.Task, error) {
	return s.storage.ListTasks(filter)
}

// CancelTask cancels a running task
func (s *Scheduler) CancelTask(taskID string) error {
	task, err := s.storage.GetTask(taskID)
	if err != nil {
		return err
	}

	if task.Status == models.TaskStatusCompleted || task.Status == models.TaskStatusCancelled {
		return fmt.Errorf("task already finished")
	}

	task.Status = models.TaskStatusCancelled
	task.UpdatedAt = time.Now()

	// Remove from queue if pending
	s.queue.Remove(taskID)

	// Save updated task
	if err := s.storage.SaveTask(task); err != nil {
		return err
	}

	logger.Info("Task cancelled", zap.String("task_id", taskID))
	metrics.TasksTotal.WithLabelValues(string(task.Status), string(task.Type), task.Namespace).Inc()

	return nil
}

// RegisterWorker registers a new worker
func (s *Scheduler) RegisterWorker(worker *models.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker.Status = models.WorkerStatusIdle
	worker.RegisteredAt = time.Now()
	worker.UpdatedAt = time.Now()
	worker.LastHeartbeat = time.Now()

	s.workers.AddWorker(worker)

	if err := s.storage.SaveWorker(worker); err != nil {
		return fmt.Errorf("failed to save worker: %w", err)
	}

	logger.Info("Worker registered", zap.String("worker_id", worker.ID))
	metrics.RecordWorkerRegistration()

	return nil
}

// UpdateWorkerHeartbeat updates worker heartbeat
func (s *Scheduler) UpdateWorkerHeartbeat(workerID string, currentTasks int, cpuUsage, memUsage float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, exists := s.workers.GetWorker(workerID)
	if !exists {
		return fmt.Errorf("worker not found: %s", workerID)
	}

	worker.UpdateHeartbeat()
	worker.CurrentTasks = currentTasks
	worker.CPUUsage = cpuUsage
	worker.MemoryUsage = memUsage
	worker.CalculateHealthScore()

	if err := s.storage.SaveWorker(worker); err != nil {
		return err
	}

	metrics.UpdateWorkerHealth(workerID, worker.HealthScore)

	return nil
}

// DeregisterWorker removes a worker
func (s *Scheduler) DeregisterWorker(workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.workers.RemoveWorker(workerID)
	if err := s.storage.DeleteWorker(workerID); err != nil {
		return err
	}

	logger.Info("Worker deregistered", zap.String("worker_id", workerID))
	metrics.RecordWorkerDeregistration()

	return nil
}

// TaskComplete marks a task as completed
func (s *Scheduler) TaskComplete(taskID, workerID string, result map[string]interface{}, duration time.Duration) error {
	task, err := s.storage.GetTask(taskID)
	if err != nil {
		return err
	}

	task.Status = models.TaskStatusCompleted
	task.Result = result
	now := time.Now()
	task.CompletedAt = &now
	task.UpdatedAt = now

	if err := s.storage.SaveTask(task); err != nil {
		return err
	}

	// Update worker stats
	s.mu.Lock()
	if worker, exists := s.workers.GetWorker(workerID); exists {
		worker.TotalTasks++
		worker.SuccessfulTasks++
		worker.CurrentTasks--
		worker.Status = models.WorkerStatusIdle
		worker.CalculateHealthScore()

		metrics.WorkerTasksProcessed.WithLabelValues(workerID, "completed").Inc()
	}
	delete(s.taskAssignments, taskID)
	s.mu.Unlock()

	logger.Info("Task completed", zap.String("task_id", taskID), zap.Duration("duration", duration))
	metrics.RecordTaskComplete(string(task.Type), "completed", task.Namespace, duration.Seconds())

	return nil
}

// TaskFailed marks a task as failed
func (s *Scheduler) TaskFailed(taskID, workerID, errorMsg string) error {
	task, err := s.storage.GetTask(taskID)
	if err != nil {
		return err
	}

	task.Error = errorMsg
	task.RetryCount++
	task.UpdatedAt = time.Now()

	// Check if should retry
	if task.ShouldRetry() {
		task.Status = models.TaskStatusRetrying
		// Schedule retry
		go func() {
			delay := task.NextRetryDelay()
			logger.Info("Scheduling task retry",
				zap.String("task_id", taskID),
				zap.Int("retry_count", task.RetryCount),
				zap.Duration("delay", delay))

			time.Sleep(delay)
			task.Status = models.TaskStatusPending
			s.queue.Push(task)
			metrics.TaskRetries.WithLabelValues(string(task.Type)).Inc()
		}()
	} else {
		// Move to DLQ
		task.Status = models.TaskStatusDLQ
		logger.Error("Task moved to DLQ", zap.String("task_id", taskID), zap.String("error", errorMsg))
		metrics.TaskDLQ.Inc()
	}

	if err := s.storage.SaveTask(task); err != nil {
		return err
	}

	// Update worker stats
	s.mu.Lock()
	if worker, exists := s.workers.GetWorker(workerID); exists {
		worker.TotalTasks++
		worker.FailedTasks++
		worker.CurrentTasks--
		worker.Status = models.WorkerStatusIdle
		worker.CalculateHealthScore()

		metrics.WorkerTasksProcessed.WithLabelValues(workerID, "failed").Inc()
	}
	delete(s.taskAssignments, taskID)
	s.mu.Unlock()

	logger.Error("Task failed", zap.String("task_id", taskID), zap.String("error", errorMsg))
	metrics.RecordTaskComplete(string(task.Type), "failed", task.Namespace, 0)

	return nil
}

// scheduleTasks main scheduling loop
func (s *Scheduler) scheduleTasks() {
	ticker := time.NewTicker(s.config.ScheduleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Only schedule if leader
			if !s.raft.IsLeader() {
				continue
			}

			s.scheduleNextTask()
		}
	}
}

// scheduleNextTask assigns the next available task to a worker
func (s *Scheduler) scheduleNextTask() {
	task := s.queue.Peek()
	if task == nil {
		return
	}

	// Find available worker
	availableWorkers := s.workers.GetAvailableWorkers(task.Type)
	if len(availableWorkers) == 0 {
		return
	}

	// Select best worker (highest health score)
	var bestWorker *models.Worker
	bestScore := 0.0
	for _, worker := range availableWorkers {
		if worker.HealthScore > bestScore {
			bestScore = worker.HealthScore
			bestWorker = worker
		}
	}

	if bestWorker == nil {
		return
	}

	// Pop task from queue
	task = s.queue.Pop()
	if task == nil {
		return
	}

	// Assign task to worker
	s.mu.Lock()
	task.Status = models.TaskStatusScheduled
	task.WorkerID = bestWorker.ID
	now := time.Now()
	task.ScheduledAt = &now
	task.UpdatedAt = now
	s.taskAssignments[task.ID] = bestWorker.ID

	bestWorker.CurrentTasks++
	bestWorker.Status = models.WorkerStatusBusy
	s.mu.Unlock()

	// Save updated task
	if err := s.storage.SaveTask(task); err != nil {
		logger.Error("Failed to save task", zap.Error(err))
		s.queue.Push(task) // Re-queue
		return
	}

	logger.Info("Task assigned",
		zap.String("task_id", task.ID),
		zap.String("worker_id", bestWorker.ID))

	metrics.TasksTotal.WithLabelValues(string(task.Status), string(task.Type), task.Namespace).Inc()
	metrics.UpdateQueueDepth("pending", s.queue.Len())

	// Send task to worker (implementation would use gRPC)
	s.taskCh <- task
}

// monitorWorkers monitors worker health
func (s *Scheduler) monitorWorkers() {
	ticker := time.NewTicker(s.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkWorkerHealth()
		}
	}
}

// checkWorkerHealth checks health of all workers
func (s *Scheduler) checkWorkerHealth() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, worker := range s.workers.Workers {
		if !worker.IsHealthy(s.config.WorkerTimeout) {
			logger.Warn("Worker unhealthy", zap.String("worker_id", worker.ID))
			worker.Status = models.WorkerStatusOffline

			// Reschedule tasks assigned to this worker
			for taskID, workerID := range s.taskAssignments {
				if workerID == worker.ID {
					if task, err := s.storage.GetTask(taskID); err == nil {
						task.Status = models.TaskStatusPending
						task.WorkerID = ""
						s.queue.Push(task)
						delete(s.taskAssignments, taskID)
					}
				}
			}
		}
	}
}

// retryFailedTasks retries tasks that should be retried
func (s *Scheduler) retryFailedTasks() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Find tasks in retrying state
			tasks, _ := s.storage.GetTasksByStatus(models.TaskStatusRetrying)
			for _, task := range tasks {
				if time.Since(task.UpdatedAt) > task.NextRetryDelay() {
					task.Status = models.TaskStatusPending
					s.queue.Push(task)
					s.storage.SaveTask(task)
				}
			}
		}
	}
}

// processTaskChannel processes tasks from channel
func (s *Scheduler) processTaskChannel() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case task := <-s.taskCh:
			// Implementation would send task via gRPC to worker
			logger.Debug("Processing task from channel", zap.String("task_id", task.ID))
		}
	}
}

// addCronTask adds a cron scheduled task
func (s *Scheduler) addCronTask(task *models.Task) error {
	entryID, err := s.cron.AddFunc(task.Schedule, func() {
		// Create new instance of the task
		newTask := task.Clone()
		newTask.ID = "" // New ID will be generated
		if err := s.SubmitTask(newTask); err != nil {
			logger.Error("Failed to submit scheduled task", zap.Error(err))
		}
	})

	if err != nil {
		return fmt.Errorf("failed to add cron task: %w", err)
	}

	s.cronJobs[task.ID] = entryID
	logger.Info("Cron task added", zap.String("task_id", task.ID), zap.String("schedule", task.Schedule))

	return nil
}

// loadTasks loads pending tasks from storage
func (s *Scheduler) loadTasks() error {
	tasks, err := s.storage.GetTasksByStatus(models.TaskStatusPending)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		s.queue.Push(task)
	}

	logger.Info("Loaded pending tasks", zap.Int("count", len(tasks)))
	return nil
}

// loadWorkers loads workers from storage
func (s *Scheduler) loadWorkers() error {
	workers, err := s.storage.ListWorkers()
	if err != nil {
		return err
	}

	for _, worker := range workers {
		s.workers.AddWorker(worker)
	}

	logger.Info("Loaded workers", zap.Int("count", len(workers)))
	return nil
}

// GetStats returns scheduler statistics
func (s *Scheduler) GetStats() (*models.TaskStats, error) {
	return s.storage.GetStats()
}

// GetWorkers returns all workers
func (s *Scheduler) GetWorkers() []*models.Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]*models.Worker, 0, s.workers.Size)
	for _, worker := range s.workers.Workers {
		workers = append(workers, worker)
	}
	return workers
}

// IsLeader checks if this scheduler is the leader
func (s *Scheduler) IsLeader() bool {
	return s.raft.IsLeader()
}
