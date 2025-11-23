package queue

import (
	"container/heap"
	"sync"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
)

// PriorityQueue implements a thread-safe priority queue for tasks
type PriorityQueue struct {
	mu       sync.RWMutex
	items    *taskHeap
	taskMap  map[string]*taskItem // For O(1) lookup and updates
	notEmpty chan struct{}
}

// taskItem wraps a task with its priority and index
type taskItem struct {
	task     *models.Task
	priority int
	index    int
	addedAt  time.Time
}

// taskHeap implements heap.Interface
type taskHeap []*taskItem

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	// Higher priority comes first
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	// For same priority, FIFO (earlier addedAt comes first)
	return h[i].addedAt.Before(h[j].addedAt)
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*taskItem)
	item.index = n
	*h = append(*h, item)
}

func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// NewPriorityQueue creates a new priority queue
func NewPriorityQueue() *PriorityQueue {
	items := &taskHeap{}
	heap.Init(items)

	return &PriorityQueue{
		items:    items,
		taskMap:  make(map[string]*taskItem),
		notEmpty: make(chan struct{}, 1),
	}
}

// Push adds a task to the queue
func (pq *PriorityQueue) Push(task *models.Task) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Check if task already exists
	if _, exists := pq.taskMap[task.ID]; exists {
		return
	}

	item := &taskItem{
		task:     task,
		priority: task.Priority,
		addedAt:  time.Now(),
	}

	heap.Push(pq.items, item)
	pq.taskMap[task.ID] = item

	// Signal that queue is not empty
	select {
	case pq.notEmpty <- struct{}{}:
	default:
	}
}

// Pop removes and returns the highest priority task
func (pq *PriorityQueue) Pop() *models.Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.items.Len() == 0 {
		return nil
	}

	item := heap.Pop(pq.items).(*taskItem)
	delete(pq.taskMap, item.task.ID)
	return item.task
}

// PopWithWait waits for a task to be available and returns it
func (pq *PriorityQueue) PopWithWait(timeout time.Duration) *models.Task {
	// Try to pop immediately
	if task := pq.Pop(); task != nil {
		return task
	}

	// Wait for notification or timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-pq.notEmpty:
		return pq.Pop()
	case <-timer.C:
		return nil
	}
}

// Peek returns the highest priority task without removing it
func (pq *PriorityQueue) Peek() *models.Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	if pq.items.Len() == 0 {
		return nil
	}

	return (*pq.items)[0].task
}

// Remove removes a specific task from the queue
func (pq *PriorityQueue) Remove(taskID string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item, exists := pq.taskMap[taskID]
	if !exists {
		return false
	}

	heap.Remove(pq.items, item.index)
	delete(pq.taskMap, taskID)
	return true
}

// Update updates the priority of a task in the queue
func (pq *PriorityQueue) Update(taskID string, newPriority int) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item, exists := pq.taskMap[taskID]
	if !exists {
		return false
	}

	item.priority = newPriority
	item.task.Priority = newPriority
	heap.Fix(pq.items, item.index)
	return true
}

// Get retrieves a task without removing it
func (pq *PriorityQueue) Get(taskID string) (*models.Task, bool) {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	item, exists := pq.taskMap[taskID]
	if !exists {
		return nil, false
	}
	return item.task, true
}

// Len returns the number of tasks in the queue
func (pq *PriorityQueue) Len() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return pq.items.Len()
}

// IsEmpty checks if the queue is empty
func (pq *PriorityQueue) IsEmpty() bool {
	return pq.Len() == 0
}

// Clear removes all tasks from the queue
func (pq *PriorityQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.items = &taskHeap{}
	heap.Init(pq.items)
	pq.taskMap = make(map[string]*taskItem)
}

// ToSlice returns all tasks as a slice (ordered by priority)
func (pq *PriorityQueue) ToSlice() []*models.Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	tasks := make([]*models.Task, 0, pq.items.Len())
	for _, item := range *pq.items {
		tasks = append(tasks, item.task)
	}
	return tasks
}

// FilterByStatus returns tasks matching the given status
func (pq *PriorityQueue) FilterByStatus(status models.TaskStatus) []*models.Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	var tasks []*models.Task
	for _, item := range *pq.items {
		if item.task.Status == status {
			tasks = append(tasks, item.task)
		}
	}
	return tasks
}

// FilterByType returns tasks matching the given type
func (pq *PriorityQueue) FilterByType(taskType models.TaskType) []*models.Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	var tasks []*models.Task
	for _, item := range *pq.items {
		if item.task.Type == taskType {
			tasks = append(tasks, item.task)
		}
	}
	return tasks
}
