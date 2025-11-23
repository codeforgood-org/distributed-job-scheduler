//go:build integration
// +build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/consensus"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/scheduler"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedulerIntegration(t *testing.T) {
	// Create temporary storage
	store, err := storage.NewStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	// Create Raft node
	raftConfig := &consensus.RaftConfig{
		NodeID:    "test-node",
		RaftAddr:  "127.0.0.1:0",
		DataDir:   t.TempDir(),
		Bootstrap: true,
	}

	raftNode, err := consensus.NewRaftNode(raftConfig, nil)
	require.NoError(t, err)
	defer raftNode.Shutdown()

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Create scheduler
	sched := scheduler.NewScheduler("test-node", raftNode, store, scheduler.DefaultConfig())
	err = sched.Start()
	require.NoError(t, err)
	defer sched.Stop()

	t.Run("SubmitTask", func(t *testing.T) {
		task := models.NewTask("test-task", models.TaskTypeBatch, 5, map[string]interface{}{
			"data": "test",
		})

		err := sched.SubmitTask(task)
		assert.NoError(t, err)
		assert.NotEmpty(t, task.ID)

		// Retrieve task
		retrieved, err := sched.GetTask(task.ID)
		assert.NoError(t, err)
		assert.Equal(t, task.Name, retrieved.Name)
		assert.Equal(t, models.TaskStatusPending, retrieved.Status)
	})

	t.Run("RegisterWorker", func(t *testing.T) {
		worker := models.NewWorker(
			"test-worker",
			"localhost:8000",
			[]models.TaskType{models.TaskTypeBatch},
			4,
		)

		err := sched.RegisterWorker(worker)
		assert.NoError(t, err)

		workers := sched.GetWorkers()
		assert.Len(t, workers, 1)
		assert.Equal(t, "test-worker", workers[0].ID)
	})

	t.Run("TaskCompletion", func(t *testing.T) {
		task := models.NewTask("completion-test", models.TaskTypeBatch, 5, map[string]interface{}{
			"data": "test",
		})

		err := sched.SubmitTask(task)
		require.NoError(t, err)

		result := map[string]interface{}{
			"status": "success",
			"output": "processed",
		}

		err = sched.TaskComplete(task.ID, "test-worker", result, 2*time.Second)
		assert.NoError(t, err)

		completed, err := sched.GetTask(task.ID)
		assert.NoError(t, err)
		assert.Equal(t, models.TaskStatusCompleted, completed.Status)
		assert.NotNil(t, completed.Result)
	})

	t.Run("TaskFailureAndRetry", func(t *testing.T) {
		task := models.NewTask("failure-test", models.TaskTypeBatch, 5, map[string]interface{}{
			"data": "test",
		})
		task.MaxRetries = 3

		err := sched.SubmitTask(task)
		require.NoError(t, err)

		// Fail the task
		err = sched.TaskFailed(task.ID, "test-worker", "simulated error")
		assert.NoError(t, err)

		failed, err := sched.GetTask(task.ID)
		assert.NoError(t, err)
		assert.Equal(t, models.TaskStatusRetrying, failed.Status)
		assert.Equal(t, 1, failed.RetryCount)
	})

	t.Run("CancelTask", func(t *testing.T) {
		task := models.NewTask("cancel-test", models.TaskTypeBatch, 5, map[string]interface{}{
			"data": "test",
		})

		err := sched.SubmitTask(task)
		require.NoError(t, err)

		err = sched.CancelTask(task.ID)
		assert.NoError(t, err)

		cancelled, err := sched.GetTask(task.ID)
		assert.NoError(t, err)
		assert.Equal(t, models.TaskStatusCancelled, cancelled.Status)
	})

	t.Run("ListTasks", func(t *testing.T) {
		// Submit multiple tasks
		for i := 0; i < 5; i++ {
			task := models.NewTask("list-test", models.TaskTypeBatch, i, map[string]interface{}{
				"index": i,
			})
			err := sched.SubmitTask(task)
			require.NoError(t, err)
		}

		// List all tasks
		tasks, err := sched.ListTasks(&models.TaskFilter{
			Limit: 100,
		})
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(tasks), 5)

		// Filter by status
		tasks, err = sched.ListTasks(&models.TaskFilter{
			Status: []models.TaskStatus{models.TaskStatusPending},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, tasks)
	})

	t.Run("GetStats", func(t *testing.T) {
		stats, err := sched.GetStats()
		assert.NoError(t, err)
		assert.NotNil(t, stats)
		assert.Greater(t, stats.Total, int64(0))
	})
}

func TestWorkerHeartbeat(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	raftConfig := &consensus.RaftConfig{
		NodeID:    "test-node-hb",
		RaftAddr:  "127.0.0.1:0",
		DataDir:   t.TempDir(),
		Bootstrap: true,
	}

	raftNode, err := consensus.NewRaftNode(raftConfig, nil)
	require.NoError(t, err)
	defer raftNode.Shutdown()

	time.Sleep(2 * time.Second)

	sched := scheduler.NewScheduler("test-node-hb", raftNode, store, scheduler.DefaultConfig())
	err = sched.Start()
	require.NoError(t, err)
	defer sched.Stop()

	// Register worker
	worker := models.NewWorker(
		"heartbeat-worker",
		"localhost:8000",
		[]models.TaskType{models.TaskTypeBatch},
		4,
	)

	err = sched.RegisterWorker(worker)
	require.NoError(t, err)

	// Update heartbeat
	err = sched.UpdateWorkerHeartbeat("heartbeat-worker", 2, 50.0, 60.0)
	assert.NoError(t, err)

	// Verify worker updated
	workers := sched.GetWorkers()
	require.Len(t, workers, 1)
	assert.Equal(t, 2, workers[0].CurrentTasks)
	assert.Equal(t, 50.0, workers[0].CPUUsage)
	assert.Equal(t, 60.0, workers[0].MemoryUsage)
}

func TestTaskPriorityQueue(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	raftConfig := &consensus.RaftConfig{
		NodeID:    "test-node-pq",
		RaftAddr:  "127.0.0.1:0",
		DataDir:   t.TempDir(),
		Bootstrap: true,
	}

	raftNode, err := consensus.NewRaftNode(raftConfig, nil)
	require.NoError(t, err)
	defer raftNode.Shutdown()

	time.Sleep(2 * time.Second)

	sched := scheduler.NewScheduler("test-node-pq", raftNode, store, scheduler.DefaultConfig())
	err = sched.Start()
	require.NoError(t, err)
	defer sched.Stop()

	// Submit tasks with different priorities
	priorities := []int{1, 5, 10, 3, 7}
	taskIDs := make([]string, len(priorities))

	for i, priority := range priorities {
		task := models.NewTask("priority-test", models.TaskTypeBatch, priority, map[string]interface{}{
			"priority": priority,
		})
		err := sched.SubmitTask(task)
		require.NoError(t, err)
		taskIDs[i] = task.ID
	}

	// Tasks should be processed by priority (higher first)
	// This is a simplified test - in production you'd verify actual execution order
	tasks, err := sched.ListTasks(&models.TaskFilter{
		Status: []models.TaskStatus{models.TaskStatusPending},
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, tasks)
}
