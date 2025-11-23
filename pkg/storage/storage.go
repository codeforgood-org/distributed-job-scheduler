package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/dgraph-io/badger/v3"
)

// Storage provides persistent storage for tasks and state
type Storage struct {
	db *badger.DB
}

const (
	taskPrefix   = "task:"
	workerPrefix = "worker:"
	statePrefix  = "state:"
	indexPrefix  = "index:"
)

// NewStorage creates a new storage instance
func NewStorage(dataDir string) (*Storage, error) {
	opts := badger.DefaultOptions(dataDir).
		WithLogger(nil). // Disable badger logs
		WithNumVersionsToKeep(1)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	return &Storage{db: db}, nil
}

// Close closes the storage
func (s *Storage) Close() error {
	return s.db.Close()
}

// SaveTask persists a task
func (s *Storage) SaveTask(task *models.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		key := []byte(taskPrefix + task.ID)

		// Save task data
		if err := txn.Set(key, data); err != nil {
			return err
		}

		// Create status index
		statusKey := []byte(fmt.Sprintf("%sstatus:%s:%s", indexPrefix, task.Status, task.ID))
		if err := txn.Set(statusKey, []byte(task.ID)); err != nil {
			return err
		}

		// Create priority index
		priorityKey := []byte(fmt.Sprintf("%spriority:%d:%s", indexPrefix, task.Priority, task.ID))
		if err := txn.Set(priorityKey, []byte(task.ID)); err != nil {
			return err
		}

		return nil
	})
}

// GetTask retrieves a task by ID
func (s *Storage) GetTask(taskID string) (*models.Task, error) {
	var task *models.Task

	err := s.db.View(func(txn *badger.Txn) error {
		key := []byte(taskPrefix + taskID)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &task)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return task, err
}

// DeleteTask removes a task
func (s *Storage) DeleteTask(taskID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Get task first to clean up indexes
		key := []byte(taskPrefix + taskID)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		var task models.Task
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &task)
		})
		if err != nil {
			return err
		}

		// Delete indexes
		statusKey := []byte(fmt.Sprintf("%sstatus:%s:%s", indexPrefix, task.Status, task.ID))
		_ = txn.Delete(statusKey)

		priorityKey := []byte(fmt.Sprintf("%spriority:%d:%s", indexPrefix, task.Priority, task.ID))
		_ = txn.Delete(priorityKey)

		// Delete task
		return txn.Delete(key)
	})
}

// ListTasks retrieves tasks with filtering
func (s *Storage) ListTasks(filter *models.TaskFilter) ([]*models.Task, error) {
	var tasks []*models.Task
	offset := 0
	if filter != nil && filter.Offset > 0 {
		offset = filter.Offset
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(taskPrefix)
		count := 0
		skipped := 0

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var task models.Task
				if err := json.Unmarshal(val, &task); err != nil {
					return err
				}

				// Apply filters
				if filter != nil {
					if !matchesFilter(&task, filter) {
						return nil
					}
				}

				// Handle offset
				if skipped < offset {
					skipped++
					return nil
				}

				tasks = append(tasks, &task)
				count++

				// Handle limit
				if filter != nil && filter.Limit > 0 && count >= filter.Limit {
					return nil
				}

				return nil
			})

			if err != nil {
				return err
			}

			if filter != nil && filter.Limit > 0 && count >= filter.Limit {
				break
			}
		}

		return nil
	})

	return tasks, err
}

// GetTasksByStatus retrieves tasks by status
func (s *Storage) GetTasksByStatus(status models.TaskStatus) ([]*models.Task, error) {
	filter := &models.TaskFilter{
		Status: []models.TaskStatus{status},
	}
	return s.ListTasks(filter)
}

// SaveWorker persists a worker
func (s *Storage) SaveWorker(worker *models.Worker) error {
	data, err := json.Marshal(worker)
	if err != nil {
		return fmt.Errorf("failed to marshal worker: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		key := []byte(workerPrefix + worker.ID)
		return txn.Set(key, data)
	})
}

// GetWorker retrieves a worker by ID
func (s *Storage) GetWorker(workerID string) (*models.Worker, error) {
	var worker *models.Worker

	err := s.db.View(func(txn *badger.Txn) error {
		key := []byte(workerPrefix + workerID)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &worker)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("worker not found: %s", workerID)
	}

	return worker, err
}

// ListWorkers retrieves all workers
func (s *Storage) ListWorkers() ([]*models.Worker, error) {
	var workers []*models.Worker

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(workerPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var worker models.Worker
				if err := json.Unmarshal(val, &worker); err != nil {
					return err
				}
				workers = append(workers, &worker)
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	return workers, err
}

// DeleteWorker removes a worker
func (s *Storage) DeleteWorker(workerID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		key := []byte(workerPrefix + workerID)
		return txn.Delete(key)
	})
}

// SaveState persists arbitrary state data
func (s *Storage) SaveState(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		k := []byte(statePrefix + key)
		return txn.Set(k, data)
	})
}

// GetState retrieves state data
func (s *Storage) GetState(key string, value interface{}) error {
	return s.db.View(func(txn *badger.Txn) error {
		k := []byte(statePrefix + key)
		item, err := txn.Get(k)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, value)
		})
	})
}

// GetStats computes task statistics
func (s *Storage) GetStats() (*models.TaskStats, error) {
	stats := &models.TaskStats{
		ByStatus:   make(map[models.TaskStatus]int64),
		ByType:     make(map[models.TaskType]int64),
		ByPriority: make(map[int]int64),
	}

	var totalDuration time.Duration
	var completedCount int64
	var successCount int64

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(taskPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var task models.Task
				if err := json.Unmarshal(val, &task); err != nil {
					return err
				}

				stats.Total++
				stats.ByStatus[task.Status]++
				stats.ByType[task.Type]++
				stats.ByPriority[task.Priority]++

				if task.Status == models.TaskStatusCompleted {
					successCount++
					if task.StartedAt != nil && task.CompletedAt != nil {
						duration := task.CompletedAt.Sub(*task.StartedAt)
						totalDuration += duration
						completedCount++
					}
				}

				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if completedCount > 0 {
		stats.AvgDuration = totalDuration / time.Duration(completedCount)
	}

	if stats.Total > 0 {
		stats.SuccessRate = (float64(successCount) / float64(stats.Total)) * 100.0
	}

	return stats, nil
}

// RunGC runs garbage collection on the database
func (s *Storage) RunGC() error {
	return s.db.RunValueLogGC(0.5)
}

// matchesFilter checks if task matches filter criteria
func matchesFilter(task *models.Task, filter *models.TaskFilter) bool {
	// Check status
	if len(filter.Status) > 0 {
		found := false
		for _, status := range filter.Status {
			if task.Status == status {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check type
	if len(filter.Type) > 0 {
		found := false
		for _, taskType := range filter.Type {
			if task.Type == taskType {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check priority
	if filter.Priority != nil && task.Priority != *filter.Priority {
		return false
	}

	// Check namespace
	if filter.Namespace != "" && task.Namespace != filter.Namespace {
		return false
	}

	// Check tags
	if len(filter.Tags) > 0 {
		for key, value := range filter.Tags {
			if taskValue, exists := task.Tags[key]; !exists || taskValue != value {
				return false
			}
		}
	}

	return true
}

// Backup creates a backup of the database
func (s *Storage) Backup(path string) error {
	f := &backupWriter{path: path}
	_, err := s.db.Backup(f, 0)
	return err
}

type backupWriter struct {
	path string
}

func (bw *backupWriter) Write(p []byte) (n int, err error) {
	// Implementation would write to file
	return len(p), nil
}
