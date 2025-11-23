package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTask(t *testing.T) {
	task := NewTask("test-task", TaskTypeBatch, 5, map[string]interface{}{
		"key": "value",
	})

	assert.NotEmpty(t, task.ID)
	assert.Equal(t, "test-task", task.Name)
	assert.Equal(t, TaskTypeBatch, task.Type)
	assert.Equal(t, 5, task.Priority)
	assert.Equal(t, TaskStatusPending, task.Status)
	assert.Equal(t, 3, task.MaxRetries)
	assert.Equal(t, 0, task.RetryCount)
	assert.NotNil(t, task.Payload)
}

func TestTask_ShouldRetry(t *testing.T) {
	task := NewTask("test", TaskTypeBatch, 5, nil)
	task.MaxRetries = 3

	// Should retry
	task.RetryCount = 0
	assert.True(t, task.ShouldRetry())

	task.RetryCount = 2
	assert.True(t, task.ShouldRetry())

	// Should not retry
	task.RetryCount = 3
	assert.False(t, task.ShouldRetry())

	task.RetryCount = 5
	assert.False(t, task.ShouldRetry())
}

func TestTask_NextRetryDelay(t *testing.T) {
	task := NewTask("test", TaskTypeBatch, 5, nil)
	task.RetryDelay = 5 * time.Second

	// First retry
	task.RetryCount = 0
	delay := task.NextRetryDelay()
	assert.Equal(t, 5*time.Second, delay)

	// Second retry (exponential backoff)
	task.RetryCount = 1
	delay = task.NextRetryDelay()
	assert.Equal(t, 10*time.Second, delay)

	// Third retry
	task.RetryCount = 2
	delay = task.NextRetryDelay()
	assert.Equal(t, 20*time.Second, delay)

	// Should cap at max delay
	task.RetryCount = 10
	delay = task.NextRetryDelay()
	assert.Equal(t, 10*time.Minute, delay)
}

func TestTask_IsExpired(t *testing.T) {
	task := NewTask("test", TaskTypeBatch, 5, nil)

	// No deadline set
	assert.False(t, task.IsExpired())

	// Future deadline
	future := time.Now().Add(1 * time.Hour)
	task.Deadline = &future
	assert.False(t, task.IsExpired())

	// Past deadline
	past := time.Now().Add(-1 * time.Hour)
	task.Deadline = &past
	assert.True(t, task.IsExpired())
}

func TestTask_CanRun(t *testing.T) {
	task := NewTask("test", TaskTypeBatch, 5, nil)

	// No dependencies
	completedTasks := make(map[string]bool)
	assert.True(t, task.CanRun(completedTasks))

	// With dependencies - all completed
	task.DependsOn = []string{"task1", "task2"}
	completedTasks["task1"] = true
	completedTasks["task2"] = true
	assert.True(t, task.CanRun(completedTasks))

	// With dependencies - some incomplete
	completedTasks["task1"] = true
	delete(completedTasks, "task2")
	assert.False(t, task.CanRun(completedTasks))

	// With dependencies - none completed
	completedTasks = make(map[string]bool)
	assert.False(t, task.CanRun(completedTasks))
}

func TestTask_Clone(t *testing.T) {
	original := NewTask("test", TaskTypeBatch, 5, map[string]interface{}{
		"key": "value",
	})
	original.Tags = map[string]string{"env": "prod"}
	original.Metadata = map[string]string{"version": "1.0"}
	original.DependsOn = []string{"task1"}

	clone := original.Clone()

	// Verify clone has same values
	assert.Equal(t, original.Name, clone.Name)
	assert.Equal(t, original.Priority, clone.Priority)

	// Modify clone
	clone.Name = "modified"
	clone.Payload["new"] = "value"
	clone.Tags["new"] = "tag"

	// Original should be unchanged
	assert.Equal(t, "test", original.Name)
	assert.Nil(t, original.Payload["new"])
	assert.Empty(t, original.Tags["new"])
}

func TestTask_ToJSON_FromJSON(t *testing.T) {
	original := NewTask("test", TaskTypeBatch, 5, map[string]interface{}{
		"key": "value",
	})

	// Serialize
	data, err := original.ToJSON()
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// Deserialize
	deserialized, err := FromJSON(data)
	assert.NoError(t, err)
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.Name, deserialized.Name)
	assert.Equal(t, original.Priority, deserialized.Priority)
}

func TestWorker_CanAcceptTask(t *testing.T) {
	worker := NewWorker("worker1", "localhost:8000", []TaskType{TaskTypeBatch, TaskTypeETL}, 4)

	// Can accept supported task type
	worker.Status = WorkerStatusIdle
	worker.CurrentTasks = 2
	assert.True(t, worker.CanAcceptTask(TaskTypeBatch))

	// Cannot accept unsupported task type
	assert.False(t, worker.CanAcceptTask(TaskTypeML))

	// Cannot accept if at max capacity
	worker.CurrentTasks = 4
	assert.False(t, worker.CanAcceptTask(TaskTypeBatch))

	// Cannot accept if offline
	worker.Status = WorkerStatusOffline
	worker.CurrentTasks = 0
	assert.False(t, worker.CanAcceptTask(TaskTypeBatch))
}

func TestWorker_IsHealthy(t *testing.T) {
	worker := NewWorker("worker1", "localhost:8000", []TaskType{TaskTypeBatch}, 4)

	timeout := 30 * time.Second

	// Just registered - should be healthy
	assert.True(t, worker.IsHealthy(timeout))

	// Old heartbeat - should be unhealthy
	worker.LastHeartbeat = time.Now().Add(-1 * time.Minute)
	assert.False(t, worker.IsHealthy(timeout))

	// Update heartbeat - should be healthy again
	worker.UpdateHeartbeat()
	assert.True(t, worker.IsHealthy(timeout))
}

func TestWorker_CalculateHealthScore(t *testing.T) {
	worker := NewWorker("worker1", "localhost:8000", []TaskType{TaskTypeBatch}, 4)

	// Perfect health
	score := worker.CalculateHealthScore()
	assert.Equal(t, 100.0, score)

	// High CPU usage
	worker.CPUUsage = 95.0
	score = worker.CalculateHealthScore()
	assert.Less(t, score, 100.0)

	// High failure rate
	worker.CPUUsage = 0
	worker.TotalTasks = 100
	worker.FailedTasks = 50
	score = worker.CalculateHealthScore()
	assert.Less(t, score, 85.0)
}

func TestWorker_GetSuccessRate(t *testing.T) {
	worker := NewWorker("worker1", "localhost:8000", []TaskType{TaskTypeBatch}, 4)

	// No tasks yet
	assert.Equal(t, 100.0, worker.GetSuccessRate())

	// 80% success rate
	worker.TotalTasks = 100
	worker.SuccessfulTasks = 80
	assert.Equal(t, 80.0, worker.GetSuccessRate())

	// 100% success
	worker.SuccessfulTasks = 100
	assert.Equal(t, 100.0, worker.GetSuccessRate())
}
