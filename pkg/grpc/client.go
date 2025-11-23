package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// WorkerClient is a gRPC client for worker operations
type WorkerClient struct {
	conn   *grpc.ClientConn
	client proto.WorkerServiceClient
	addr   string
}

// NewWorkerClient creates a new worker gRPC client
func NewWorkerClient(addr string) (*WorkerClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &WorkerClient{
		conn:   conn,
		client: proto.NewWorkerServiceClient(conn),
		addr:   addr,
	}, nil
}

// Close closes the client connection
func (wc *WorkerClient) Close() error {
	return wc.conn.Close()
}

// AssignTask assigns a task to the worker
func (wc *WorkerClient) AssignTask(task *models.Task) error {
	payload, err := json.Marshal(task.Payload)
	if err != nil {
		return err
	}

	metadata := make(map[string]string)
	if task.Metadata != nil {
		metadata = task.Metadata
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := wc.client.AssignTask(ctx, &proto.AssignTaskRequest{
		TaskId:         task.ID,
		Name:           task.Name,
		Type:           string(task.Type),
		Payload:        payload,
		TimeoutSeconds: int32(task.Timeout.Seconds()),
		Metadata:       metadata,
	})

	if err != nil {
		return err
	}

	if !resp.Accepted {
		return fmt.Errorf("task not accepted: %s", resp.Message)
	}

	logger.Info("Task assigned to worker",
		zap.String("task_id", task.ID),
		zap.String("worker_addr", wc.addr))

	return nil
}

// CancelTask cancels a task on the worker
func (wc *WorkerClient) CancelTask(taskID, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := wc.client.CancelTask(ctx, &proto.CancelTaskRequest{
		TaskId: taskID,
		Reason: reason,
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to cancel task: %s", resp.Message)
	}

	return nil
}

// HealthCheck checks worker health
func (wc *WorkerClient) HealthCheck(schedulerID string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := wc.client.HealthCheck(ctx, &proto.HealthCheckRequest{
		SchedulerId: schedulerID,
	})

	if err != nil {
		return false, err
	}

	return resp.Healthy, nil
}

// SchedulerClient is a gRPC client for scheduler operations
type SchedulerClient struct {
	conn   *grpc.ClientConn
	client proto.SchedulerServiceClient
	addr   string
}

// NewSchedulerClient creates a new scheduler gRPC client
func NewSchedulerClient(addr string) (*SchedulerClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &SchedulerClient{
		conn:   conn,
		client: proto.NewSchedulerServiceClient(conn),
		addr:   addr,
	}, nil
}

// Close closes the client connection
func (sc *SchedulerClient) Close() error {
	return sc.conn.Close()
}

// RegisterWorker registers a worker with the scheduler
func (sc *SchedulerClient) RegisterWorker(worker *models.Worker) error {
	capabilities := make([]string, len(worker.Capabilities))
	for i, cap := range worker.Capabilities {
		capabilities[i] = string(cap)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := sc.client.RegisterWorker(ctx, &proto.RegisterWorkerRequest{
		WorkerId:       worker.ID,
		Address:        worker.Address,
		Capabilities:   capabilities,
		MaxConcurrency: int32(worker.MaxConcurrency),
		Metadata:       worker.Metadata,
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	logger.Info("Worker registered with scheduler",
		zap.String("worker_id", worker.ID),
		zap.String("scheduler_id", resp.SchedulerId))

	return nil
}

// Heartbeat sends a heartbeat to the scheduler
func (sc *SchedulerClient) Heartbeat(workerID string, currentTasks int, cpuUsage, memUsage float64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := sc.client.Heartbeat(ctx, &proto.HeartbeatRequest{
		WorkerId:     workerID,
		CurrentTasks: int32(currentTasks),
		CpuUsage:     cpuUsage,
		MemoryUsage:  memUsage,
	})

	return err
}

// ReportTaskComplete reports task completion
func (sc *SchedulerClient) ReportTaskComplete(taskID, workerID string, result map[string]interface{}, duration time.Duration) error {
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = sc.client.ReportTaskComplete(ctx, &proto.TaskCompleteRequest{
		TaskId:     taskID,
		WorkerId:   workerID,
		Result:     resultBytes,
		DurationMs: duration.Milliseconds(),
	})

	return err
}

// ReportTaskFailure reports task failure
func (sc *SchedulerClient) ReportTaskFailure(taskID, workerID, errorMsg string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := sc.client.ReportTaskFailure(ctx, &proto.TaskFailureRequest{
		TaskId:   taskID,
		WorkerId: workerID,
		Error:    errorMsg,
		Retry:    true,
	})

	return err
}

// DeregisterWorker deregisters a worker
func (sc *SchedulerClient) DeregisterWorker(workerID, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := sc.client.DeregisterWorker(ctx, &proto.DeregisterWorkerRequest{
		WorkerId: workerID,
		Reason:   reason,
	})

	return err
}
