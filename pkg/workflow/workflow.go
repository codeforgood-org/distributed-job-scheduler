package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"go.uber.org/zap"
)

// WorkflowStatus represents the status of a workflow
type WorkflowStatus string

const (
	WorkflowStatusPending   WorkflowStatus = "pending"
	WorkflowStatusRunning   WorkflowStatus = "running"
	WorkflowStatusCompleted WorkflowStatus = "completed"
	WorkflowStatusFailed    WorkflowStatus = "failed"
	WorkflowStatusCancelled WorkflowStatus = "cancelled"
)

// TaskNode represents a task in the workflow DAG
type TaskNode struct {
	ID           string                 `json:"id"`
	Name         string                 `json:"name"`
	Type         models.TaskType        `json:"type"`
	Priority     int                    `json:"priority"`
	Payload      map[string]interface{} `json:"payload"`
	DependsOn    []string               `json:"depends_on"`
	Timeout      time.Duration          `json:"timeout"`
	MaxRetries   int                    `json:"max_retries"`

	// Runtime info
	TaskID       string                 `json:"task_id,omitempty"`
	Status       models.TaskStatus      `json:"status"`
	Result       map[string]interface{} `json:"result,omitempty"`
	Error        string                 `json:"error,omitempty"`
	StartedAt    *time.Time             `json:"started_at,omitempty"`
	CompletedAt  *time.Time             `json:"completed_at,omitempty"`
}

// Workflow represents a DAG of tasks
type Workflow struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Namespace   string                 `json:"namespace"`

	// DAG structure
	Tasks       map[string]*TaskNode   `json:"tasks"`

	// Workflow configuration
	MaxParallel int                    `json:"max_parallel"` // Max parallel tasks
	Timeout     time.Duration          `json:"timeout"`

	// State
	Status      WorkflowStatus         `json:"status"`
	Progress    float64                `json:"progress"` // 0-100

	// Metadata
	Tags        map[string]string      `json:"tags,omitempty"`
	Metadata    map[string]string      `json:"metadata,omitempty"`

	// Timestamps
	CreatedAt   time.Time              `json:"created_at"`
	StartedAt   *time.Time             `json:"started_at,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`

	// Execution context
	Variables   map[string]interface{} `json:"variables,omitempty"`
}

// WorkflowEngine executes workflows
type WorkflowEngine struct {
	mu         sync.RWMutex
	workflows  map[string]*Workflow
	scheduler  WorkflowScheduler
}

// WorkflowScheduler interface for submitting tasks
type WorkflowScheduler interface {
	SubmitTask(task *models.Task) error
	GetTask(taskID string) (*models.Task, error)
}

// NewWorkflowEngine creates a new workflow engine
func NewWorkflowEngine(scheduler WorkflowScheduler) *WorkflowEngine {
	return &WorkflowEngine{
		workflows: make(map[string]*Workflow),
		scheduler: scheduler,
	}
}

// CreateWorkflow creates a new workflow
func (we *WorkflowEngine) CreateWorkflow(workflow *Workflow) error {
	// Validate DAG
	if err := we.validateDAG(workflow); err != nil {
		return fmt.Errorf("invalid workflow DAG: %w", err)
	}

	workflow.Status = WorkflowStatusPending
	workflow.CreatedAt = time.Now()
	workflow.Progress = 0

	we.mu.Lock()
	we.workflows[workflow.ID] = workflow
	we.mu.Unlock()

	logger.Info("Workflow created",
		zap.String("workflow_id", workflow.ID),
		zap.String("name", workflow.Name),
		zap.Int("tasks", len(workflow.Tasks)))

	return nil
}

// validateDAG validates that the workflow forms a valid DAG (no cycles)
func (we *WorkflowEngine) validateDAG(workflow *Workflow) error {
	// Check for cycles using DFS
	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)

	var hasCycle func(nodeID string) bool
	hasCycle = func(nodeID string) bool {
		visited[nodeID] = true
		recursionStack[nodeID] = true

		node, exists := workflow.Tasks[nodeID]
		if !exists {
			return false
		}

		for _, depID := range node.DependsOn {
			if !visited[depID] {
				if hasCycle(depID) {
					return true
				}
			} else if recursionStack[depID] {
				return true
			}
		}

		recursionStack[nodeID] = false
		return false
	}

	for taskID := range workflow.Tasks {
		if !visited[taskID] {
			if hasCycle(taskID) {
				return errors.New("workflow contains cycles")
			}
		}
	}

	// Validate all dependencies exist
	for taskID, task := range workflow.Tasks {
		for _, depID := range task.DependsOn {
			if _, exists := workflow.Tasks[depID]; !exists {
				return fmt.Errorf("task %s depends on non-existent task %s", taskID, depID)
			}
		}
	}

	return nil
}

// StartWorkflow starts executing a workflow
func (we *WorkflowEngine) StartWorkflow(workflowID string) error {
	we.mu.Lock()
	workflow, exists := we.workflows[workflowID]
	if !exists {
		we.mu.Unlock()
		return fmt.Errorf("workflow not found: %s", workflowID)
	}

	if workflow.Status != WorkflowStatusPending {
		we.mu.Unlock()
		return fmt.Errorf("workflow is not in pending state")
	}

	workflow.Status = WorkflowStatusRunning
	now := time.Now()
	workflow.StartedAt = &now
	we.mu.Unlock()

	// Start execution in goroutine
	go we.executeWorkflow(workflowID)

	logger.Info("Workflow started", zap.String("workflow_id", workflowID))
	return nil
}

// executeWorkflow executes the workflow
func (we *WorkflowEngine) executeWorkflow(workflowID string) {
	we.mu.RLock()
	workflow := we.workflows[workflowID]
	we.mu.RUnlock()

	logger.Info("Executing workflow", zap.String("workflow_id", workflowID))

	// Track completed tasks
	completed := make(map[string]bool)
	running := make(map[string]bool)

	for {
		// Check if all tasks are done
		if len(completed) == len(workflow.Tasks) {
			we.completeWorkflow(workflowID, true, "")
			return
		}

		// Check for timeout
		if workflow.Timeout > 0 && workflow.StartedAt != nil {
			if time.Since(*workflow.StartedAt) > workflow.Timeout {
				we.completeWorkflow(workflowID, false, "workflow timeout")
				return
			}
		}

		// Find ready tasks
		readyTasks := we.findReadyTasks(workflow, completed, running)

		// Submit ready tasks
		for _, task := range readyTasks {
			if workflow.MaxParallel > 0 && len(running) >= workflow.MaxParallel {
				break
			}

			if err := we.submitTask(workflow, task); err != nil {
				logger.Error("Failed to submit task",
					zap.String("workflow_id", workflowID),
					zap.String("task_id", task.ID),
					zap.Error(err))
				we.completeWorkflow(workflowID, false, err.Error())
				return
			}

			running[task.ID] = true
		}

		// Check status of running tasks
		for taskNodeID := range running {
			taskNode := workflow.Tasks[taskNodeID]
			if taskNode.TaskID == "" {
				continue
			}

			task, err := we.scheduler.GetTask(taskNode.TaskID)
			if err != nil {
				continue
			}

			switch task.Status {
			case models.TaskStatusCompleted:
				taskNode.Status = task.Status
				taskNode.Result = task.Result
				taskNode.CompletedAt = task.CompletedAt
				completed[taskNodeID] = true
				delete(running, taskNodeID)

				logger.Info("Workflow task completed",
					zap.String("workflow_id", workflowID),
					zap.String("task_node_id", taskNodeID))

			case models.TaskStatusFailed, models.TaskStatusDLQ:
				taskNode.Status = task.Status
				taskNode.Error = task.Error
				we.completeWorkflow(workflowID, false, fmt.Sprintf("task %s failed: %s", taskNodeID, task.Error))
				return

			case models.TaskStatusCancelled:
				we.completeWorkflow(workflowID, false, "task cancelled")
				return
			}
		}

		// Update progress
		we.updateProgress(workflowID, float64(len(completed))/float64(len(workflow.Tasks))*100)

		// Sleep before next iteration
		time.Sleep(1 * time.Second)
	}
}

// findReadyTasks finds tasks that are ready to execute
func (we *WorkflowEngine) findReadyTasks(workflow *Workflow, completed, running map[string]bool) []*TaskNode {
	var ready []*TaskNode

	for taskID, task := range workflow.Tasks {
		// Skip if already completed or running
		if completed[taskID] || running[taskID] {
			continue
		}

		// Check if all dependencies are completed
		allDepsCompleted := true
		for _, depID := range task.DependsOn {
			if !completed[depID] {
				allDepsCompleted = false
				break
			}
		}

		if allDepsCompleted {
			ready = append(ready, task)
		}
	}

	return ready
}

// submitTask submits a task from workflow
func (we *WorkflowEngine) submitTask(workflow *Workflow, taskNode *TaskNode) error {
	task := models.NewTask(taskNode.Name, taskNode.Type, taskNode.Priority, taskNode.Payload)
	task.Namespace = workflow.Namespace
	task.Timeout = taskNode.Timeout
	task.MaxRetries = taskNode.MaxRetries

	// Inject workflow variables into payload
	if workflow.Variables != nil {
		for k, v := range workflow.Variables {
			task.Payload[k] = v
		}
	}

	// Add workflow metadata
	task.Metadata["workflow_id"] = workflow.ID
	task.Metadata["workflow_task_id"] = taskNode.ID

	if err := we.scheduler.SubmitTask(task); err != nil {
		return err
	}

	taskNode.TaskID = task.ID
	taskNode.Status = models.TaskStatusPending
	now := time.Now()
	taskNode.StartedAt = &now

	return nil
}

// completeWorkflow marks workflow as completed
func (we *WorkflowEngine) completeWorkflow(workflowID string, success bool, errorMsg string) {
	we.mu.Lock()
	defer we.mu.Unlock()

	workflow := we.workflows[workflowID]
	now := time.Now()
	workflow.CompletedAt = &now

	if success {
		workflow.Status = WorkflowStatusCompleted
		workflow.Progress = 100
	} else {
		workflow.Status = WorkflowStatusFailed
	}

	logger.Info("Workflow completed",
		zap.String("workflow_id", workflowID),
		zap.String("status", string(workflow.Status)),
		zap.String("error", errorMsg))
}

// updateProgress updates workflow progress
func (we *WorkflowEngine) updateProgress(workflowID string, progress float64) {
	we.mu.Lock()
	defer we.mu.Unlock()

	if workflow, exists := we.workflows[workflowID]; exists {
		workflow.Progress = progress
	}
}

// GetWorkflow retrieves a workflow
func (we *WorkflowEngine) GetWorkflow(workflowID string) (*Workflow, error) {
	we.mu.RLock()
	defer we.mu.RUnlock()

	workflow, exists := we.workflows[workflowID]
	if !exists {
		return nil, fmt.Errorf("workflow not found")
	}

	return workflow, nil
}

// CancelWorkflow cancels a running workflow
func (we *WorkflowEngine) CancelWorkflow(workflowID string) error {
	we.mu.Lock()
	defer we.mu.Unlock()

	workflow, exists := we.workflows[workflowID]
	if !exists {
		return fmt.Errorf("workflow not found")
	}

	workflow.Status = WorkflowStatusCancelled
	now := time.Now()
	workflow.CompletedAt = &now

	logger.Info("Workflow cancelled", zap.String("workflow_id", workflowID))
	return nil
}

// ToJSON converts workflow to JSON for visualization
func (w *Workflow) ToJSON() ([]byte, error) {
	return json.Marshal(w)
}

// GetDAGVisualization returns a DOT format visualization
func (w *Workflow) GetDAGVisualization() string {
	dot := "digraph Workflow {\n"
	dot += "  rankdir=LR;\n"
	dot += "  node [shape=box];\n\n"

	// Add nodes
	for id, task := range w.Tasks {
		color := "white"
		switch task.Status {
		case models.TaskStatusCompleted:
			color = "lightgreen"
		case models.TaskStatusRunning:
			color = "lightblue"
		case models.TaskStatusFailed:
			color = "lightcoral"
		}

		dot += fmt.Sprintf("  \"%s\" [label=\"%s\nPriority: %d\", style=filled, fillcolor=%s];\n",
			id, task.Name, task.Priority, color)
	}

	dot += "\n"

	// Add edges
	for id, task := range w.Tasks {
		for _, depID := range task.DependsOn {
			dot += fmt.Sprintf("  \"%s\" -> \"%s\";\n", depID, id)
		}
	}

	dot += "}\n"
	return dot
}
