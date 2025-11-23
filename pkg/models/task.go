package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// TaskStatus represents the current state of a task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusScheduled TaskStatus = "scheduled"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
	TaskStatusRetrying  TaskStatus = "retrying"
	TaskStatusDLQ       TaskStatus = "dead_letter_queue"
)

// TaskType defines the category of task
type TaskType string

const (
	TaskTypeBatch   TaskType = "batch"
	TaskTypeStream  TaskType = "stream"
	TaskTypeReport  TaskType = "report"
	TaskTypeETL     TaskType = "etl"
	TaskTypeML      TaskType = "ml"
	TaskTypeCustom  TaskType = "custom"
)

// Task represents a unit of work to be scheduled and executed
type Task struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Type          TaskType               `json:"type"`
	Priority      int                    `json:"priority"` // 1-10, higher = more priority
	Status        TaskStatus             `json:"status"`
	Payload       map[string]interface{} `json:"payload"`
	Result        map[string]interface{} `json:"result,omitempty"`
	Error         string                 `json:"error,omitempty"`

	// Scheduling
	Schedule      string                 `json:"schedule,omitempty"` // Cron expression
	ScheduledAt   *time.Time             `json:"scheduled_at,omitempty"`
	StartedAt     *time.Time             `json:"started_at,omitempty"`
	CompletedAt   *time.Time             `json:"completed_at,omitempty"`

	// Retry logic
	MaxRetries    int                    `json:"max_retries"`
	RetryCount    int                    `json:"retry_count"`
	RetryDelay    time.Duration          `json:"retry_delay"` // Initial delay

	// Timeout and constraints
	Timeout       time.Duration          `json:"timeout"`
	Deadline      *time.Time             `json:"deadline,omitempty"`

	// Dependencies
	DependsOn     []string               `json:"depends_on,omitempty"`

	// Execution
	WorkerID      string                 `json:"worker_id,omitempty"`
	NodeID        string                 `json:"node_id,omitempty"` // Scheduler node

	// Metadata
	Namespace     string                 `json:"namespace"`
	Tags          map[string]string      `json:"tags,omitempty"`
	Metadata      map[string]string      `json:"metadata,omitempty"`

	// Timestamps
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`

	// Rate limiting
	RateLimitKey  string                 `json:"rate_limit_key,omitempty"`
}

// NewTask creates a new task with defaults
func NewTask(name string, taskType TaskType, priority int, payload map[string]interface{}) *Task {
	now := time.Now()
	return &Task{
		ID:          uuid.New().String(),
		Name:        name,
		Type:        taskType,
		Priority:    priority,
		Status:      TaskStatusPending,
		Payload:     payload,
		MaxRetries:  3,
		RetryCount:  0,
		RetryDelay:  time.Second * 5,
		Timeout:     time.Minute * 5,
		Namespace:   "default",
		Tags:        make(map[string]string),
		Metadata:    make(map[string]string),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// ToJSON serializes task to JSON
func (t *Task) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}

// FromJSON deserializes task from JSON
func FromJSON(data []byte) (*Task, error) {
	var task Task
	err := json.Unmarshal(data, &task)
	return &task, err
}

// IsExpired checks if task has exceeded its deadline
func (t *Task) IsExpired() bool {
	if t.Deadline == nil {
		return false
	}
	return time.Now().After(*t.Deadline)
}

// ShouldRetry determines if task should be retried
func (t *Task) ShouldRetry() bool {
	return t.RetryCount < t.MaxRetries
}

// NextRetryDelay calculates exponential backoff delay
func (t *Task) NextRetryDelay() time.Duration {
	// Exponential backoff: initial_delay * 2^retry_count
	backoff := t.RetryDelay * (1 << uint(t.RetryCount))
	maxDelay := time.Minute * 10
	if backoff > maxDelay {
		return maxDelay
	}
	return backoff
}

// CanRun checks if task dependencies are satisfied
func (t *Task) CanRun(completedTasks map[string]bool) bool {
	if len(t.DependsOn) == 0 {
		return true
	}

	for _, depID := range t.DependsOn {
		if !completedTasks[depID] {
			return false
		}
	}
	return true
}

// Clone creates a deep copy of the task
func (t *Task) Clone() *Task {
	clone := *t

	// Deep copy maps
	if t.Payload != nil {
		clone.Payload = make(map[string]interface{})
		for k, v := range t.Payload {
			clone.Payload[k] = v
		}
	}

	if t.Result != nil {
		clone.Result = make(map[string]interface{})
		for k, v := range t.Result {
			clone.Result[k] = v
		}
	}

	if t.Tags != nil {
		clone.Tags = make(map[string]string)
		for k, v := range t.Tags {
			clone.Tags[k] = v
		}
	}

	if t.Metadata != nil {
		clone.Metadata = make(map[string]string)
		for k, v := range t.Metadata {
			clone.Metadata[k] = v
		}
	}

	if t.DependsOn != nil {
		clone.DependsOn = make([]string, len(t.DependsOn))
		copy(clone.DependsOn, t.DependsOn)
	}

	return &clone
}

// TaskFilter represents filtering criteria for tasks
type TaskFilter struct {
	Status    []TaskStatus
	Type      []TaskType
	Priority  *int
	Namespace string
	Tags      map[string]string
	Limit     int
	Offset    int
}

// TaskStats represents aggregated task statistics
type TaskStats struct {
	Total       int64                  `json:"total"`
	ByStatus    map[TaskStatus]int64   `json:"by_status"`
	ByType      map[TaskType]int64     `json:"by_type"`
	ByPriority  map[int]int64          `json:"by_priority"`
	AvgDuration time.Duration          `json:"avg_duration"`
	SuccessRate float64                `json:"success_rate"`
}
