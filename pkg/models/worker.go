package models

import (
	"time"
)

// WorkerStatus represents the current state of a worker
type WorkerStatus string

const (
	WorkerStatusIdle       WorkerStatus = "idle"
	WorkerStatusBusy       WorkerStatus = "busy"
	WorkerStatusOffline    WorkerStatus = "offline"
	WorkerStatusRegistering WorkerStatus = "registering"
)

// Worker represents a task execution node
type Worker struct {
	ID              string                 `json:"id"`
	Address         string                 `json:"address"` // gRPC address
	Status          WorkerStatus           `json:"status"`
	Capabilities    []TaskType             `json:"capabilities"`
	MaxConcurrency  int                    `json:"max_concurrency"`
	CurrentTasks    int                    `json:"current_tasks"`

	// Health
	LastHeartbeat   time.Time              `json:"last_heartbeat"`
	HealthScore     float64                `json:"health_score"` // 0-100

	// Performance
	TotalTasks      int64                  `json:"total_tasks"`
	SuccessfulTasks int64                  `json:"successful_tasks"`
	FailedTasks     int64                  `json:"failed_tasks"`
	AvgTaskDuration time.Duration          `json:"avg_task_duration"`

	// Resources
	CPUUsage        float64                `json:"cpu_usage"`
	MemoryUsage     float64                `json:"memory_usage"`

	// Metadata
	Version         string                 `json:"version"`
	Tags            map[string]string      `json:"tags,omitempty"`
	Metadata        map[string]string      `json:"metadata,omitempty"`

	// Timestamps
	RegisteredAt    time.Time              `json:"registered_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

// NewWorker creates a new worker instance
func NewWorker(id, address string, capabilities []TaskType, maxConcurrency int) *Worker {
	now := time.Now()
	return &Worker{
		ID:             id,
		Address:        address,
		Status:         WorkerStatusRegistering,
		Capabilities:   capabilities,
		MaxConcurrency: maxConcurrency,
		CurrentTasks:   0,
		LastHeartbeat:  now,
		HealthScore:    100.0,
		TotalTasks:     0,
		SuccessfulTasks: 0,
		FailedTasks:    0,
		CPUUsage:       0.0,
		MemoryUsage:    0.0,
		Tags:           make(map[string]string),
		Metadata:       make(map[string]string),
		RegisteredAt:   now,
		UpdatedAt:      now,
	}
}

// IsHealthy checks if worker is healthy based on heartbeat
func (w *Worker) IsHealthy(timeout time.Duration) bool {
	return time.Since(w.LastHeartbeat) < timeout
}

// CanAcceptTask checks if worker can accept a new task
func (w *Worker) CanAcceptTask(taskType TaskType) bool {
	if w.Status == WorkerStatusOffline {
		return false
	}

	if w.CurrentTasks >= w.MaxConcurrency {
		return false
	}

	// Check if worker supports this task type
	for _, cap := range w.Capabilities {
		if cap == taskType || cap == TaskTypeCustom {
			return true
		}
	}

	return false
}

// UpdateHeartbeat updates the last heartbeat timestamp
func (w *Worker) UpdateHeartbeat() {
	w.LastHeartbeat = time.Now()
	w.UpdatedAt = time.Now()
}

// CalculateHealthScore calculates worker health based on various metrics
func (w *Worker) CalculateHealthScore() float64 {
	score := 100.0

	// Deduct for high failure rate
	if w.TotalTasks > 0 {
		failureRate := float64(w.FailedTasks) / float64(w.TotalTasks)
		score -= failureRate * 30.0
	}

	// Deduct for high resource usage
	if w.CPUUsage > 90.0 {
		score -= 20.0
	} else if w.CPUUsage > 70.0 {
		score -= 10.0
	}

	if w.MemoryUsage > 90.0 {
		score -= 20.0
	} else if w.MemoryUsage > 70.0 {
		score -= 10.0
	}

	// Deduct for high load
	loadFactor := float64(w.CurrentTasks) / float64(w.MaxConcurrency)
	if loadFactor > 0.9 {
		score -= 10.0
	}

	if score < 0 {
		score = 0
	}

	w.HealthScore = score
	return score
}

// GetSuccessRate returns the success rate as percentage
func (w *Worker) GetSuccessRate() float64 {
	if w.TotalTasks == 0 {
		return 100.0
	}
	return (float64(w.SuccessfulTasks) / float64(w.TotalTasks)) * 100.0
}

// WorkerPool represents a collection of workers
type WorkerPool struct {
	Workers map[string]*Worker `json:"workers"`
	Size    int                `json:"size"`
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool() *WorkerPool {
	return &WorkerPool{
		Workers: make(map[string]*Worker),
		Size:    0,
	}
}

// AddWorker adds a worker to the pool
func (wp *WorkerPool) AddWorker(worker *Worker) {
	wp.Workers[worker.ID] = worker
	wp.Size = len(wp.Workers)
}

// RemoveWorker removes a worker from the pool
func (wp *WorkerPool) RemoveWorker(workerID string) {
	delete(wp.Workers, workerID)
	wp.Size = len(wp.Workers)
}

// GetWorker retrieves a worker by ID
func (wp *WorkerPool) GetWorker(workerID string) (*Worker, bool) {
	worker, exists := wp.Workers[workerID]
	return worker, exists
}

// GetAvailableWorkers returns workers that can accept tasks
func (wp *WorkerPool) GetAvailableWorkers(taskType TaskType) []*Worker {
	var available []*Worker
	for _, worker := range wp.Workers {
		if worker.CanAcceptTask(taskType) {
			available = append(available, worker)
		}
	}
	return available
}

// GetHealthyWorkers returns workers that are healthy
func (wp *WorkerPool) GetHealthyWorkers(timeout time.Duration) []*Worker {
	var healthy []*Worker
	for _, worker := range wp.Workers {
		if worker.IsHealthy(timeout) {
			healthy = append(healthy, worker)
		}
	}
	return healthy
}
