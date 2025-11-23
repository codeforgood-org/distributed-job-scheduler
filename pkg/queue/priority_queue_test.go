package queue

import (
	"testing"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestPriorityQueue_PushPop(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := models.NewTask("task1", models.TaskTypeBatch, 5, nil)
	task2 := models.NewTask("task2", models.TaskTypeBatch, 10, nil)
	task3 := models.NewTask("task3", models.TaskTypeBatch, 3, nil)

	pq.Push(task1)
	pq.Push(task2)
	pq.Push(task3)

	assert.Equal(t, 3, pq.Len())

	// Should pop highest priority first (task2 with priority 10)
	popped := pq.Pop()
	assert.NotNil(t, popped)
	assert.Equal(t, task2.ID, popped.ID)
	assert.Equal(t, 10, popped.Priority)

	// Next should be task1 (priority 5)
	popped = pq.Pop()
	assert.Equal(t, task1.ID, popped.ID)

	// Last should be task3 (priority 3)
	popped = pq.Pop()
	assert.Equal(t, task3.ID, popped.ID)

	// Queue should be empty
	assert.True(t, pq.IsEmpty())
}

func TestPriorityQueue_Peek(t *testing.T) {
	pq := NewPriorityQueue()

	task := models.NewTask("task", models.TaskTypeBatch, 5, nil)
	pq.Push(task)

	// Peek should not remove the task
	peeked := pq.Peek()
	assert.NotNil(t, peeked)
	assert.Equal(t, task.ID, peeked.ID)
	assert.Equal(t, 1, pq.Len())
}

func TestPriorityQueue_Remove(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := models.NewTask("task1", models.TaskTypeBatch, 5, nil)
	task2 := models.NewTask("task2", models.TaskTypeBatch, 10, nil)

	pq.Push(task1)
	pq.Push(task2)

	// Remove task1
	removed := pq.Remove(task1.ID)
	assert.True(t, removed)
	assert.Equal(t, 1, pq.Len())

	// Try to remove again
	removed = pq.Remove(task1.ID)
	assert.False(t, removed)

	// Only task2 should remain
	popped := pq.Pop()
	assert.Equal(t, task2.ID, popped.ID)
}

func TestPriorityQueue_Update(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := models.NewTask("task1", models.TaskTypeBatch, 5, nil)
	task2 := models.NewTask("task2", models.TaskTypeBatch, 10, nil)

	pq.Push(task1)
	pq.Push(task2)

	// Update task1's priority to 15 (higher than task2)
	updated := pq.Update(task1.ID, 15)
	assert.True(t, updated)

	// task1 should now be popped first
	popped := pq.Pop()
	assert.Equal(t, task1.ID, popped.ID)
	assert.Equal(t, 15, popped.Priority)
}

func TestPriorityQueue_FIFO_SamePriority(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := models.NewTask("task1", models.TaskTypeBatch, 5, nil)
	time.Sleep(10 * time.Millisecond)
	task2 := models.NewTask("task2", models.TaskTypeBatch, 5, nil)
	time.Sleep(10 * time.Millisecond)
	task3 := models.NewTask("task3", models.TaskTypeBatch, 5, nil)

	pq.Push(task1)
	pq.Push(task2)
	pq.Push(task3)

	// With same priority, should follow FIFO
	popped1 := pq.Pop()
	popped2 := pq.Pop()
	popped3 := pq.Pop()

	assert.Equal(t, task1.ID, popped1.ID)
	assert.Equal(t, task2.ID, popped2.ID)
	assert.Equal(t, task3.ID, popped3.ID)
}

func TestPriorityQueue_FilterByStatus(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := models.NewTask("task1", models.TaskTypeBatch, 5, nil)
	task1.Status = models.TaskStatusPending

	task2 := models.NewTask("task2", models.TaskTypeBatch, 5, nil)
	task2.Status = models.TaskStatusRunning

	pq.Push(task1)
	pq.Push(task2)

	pending := pq.FilterByStatus(models.TaskStatusPending)
	assert.Len(t, pending, 1)
	assert.Equal(t, task1.ID, pending[0].ID)

	running := pq.FilterByStatus(models.TaskStatusRunning)
	assert.Len(t, running, 1)
	assert.Equal(t, task2.ID, running[0].ID)
}

func TestPriorityQueue_Clear(t *testing.T) {
	pq := NewPriorityQueue()

	for i := 0; i < 10; i++ {
		task := models.NewTask("task", models.TaskTypeBatch, i, nil)
		pq.Push(task)
	}

	assert.Equal(t, 10, pq.Len())

	pq.Clear()

	assert.Equal(t, 0, pq.Len())
	assert.True(t, pq.IsEmpty())
}

func BenchmarkPriorityQueue_Push(b *testing.B) {
	pq := NewPriorityQueue()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := models.NewTask("task", models.TaskTypeBatch, i%10, nil)
		pq.Push(task)
	}
}

func BenchmarkPriorityQueue_Pop(b *testing.B) {
	pq := NewPriorityQueue()

	// Pre-populate queue
	for i := 0; i < 10000; i++ {
		task := models.NewTask("task", models.TaskTypeBatch, i%10, nil)
		pq.Push(task)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if pq.Len() == 0 {
			break
		}
		pq.Pop()
	}
}
