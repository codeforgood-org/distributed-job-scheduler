package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/proto"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/scheduler"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// SchedulerServer implements the gRPC scheduler service
type SchedulerServer struct {
	proto.UnimplementedSchedulerServiceServer
	scheduler *scheduler.Scheduler
	addr      string
	server    *grpc.Server
}

// NewSchedulerServer creates a new gRPC scheduler server
func NewSchedulerServer(sched *scheduler.Scheduler, addr string) *SchedulerServer {
	return &SchedulerServer{
		scheduler: sched,
		addr:      addr,
	}
}

// Start starts the gRPC server
func (s *SchedulerServer) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.server = grpc.NewServer()
	proto.RegisterSchedulerServiceServer(s.server, s)

	logger.Info("Starting gRPC server", zap.String("addr", s.addr))

	return s.server.Serve(lis)
}

// Stop stops the gRPC server
func (s *SchedulerServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// RegisterWorker handles worker registration
func (s *SchedulerServer) RegisterWorker(ctx context.Context, req *proto.RegisterWorkerRequest) (*proto.RegisterWorkerResponse, error) {
	logger.Info("Worker registration request",
		zap.String("worker_id", req.WorkerId),
		zap.String("address", req.Address))

	capabilities := make([]models.TaskType, len(req.Capabilities))
	for i, cap := range req.Capabilities {
		capabilities[i] = models.TaskType(cap)
	}

	worker := models.NewWorker(
		req.WorkerId,
		req.Address,
		capabilities,
		int(req.MaxConcurrency),
	)

	if req.Metadata != nil {
		worker.Metadata = req.Metadata
	}

	if err := s.scheduler.RegisterWorker(worker); err != nil {
		return &proto.RegisterWorkerResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	return &proto.RegisterWorkerResponse{
		Success:     true,
		Message:     "Worker registered successfully",
		SchedulerId: s.scheduler.nodeID,
	}, nil
}

// Heartbeat handles worker heartbeats
func (s *SchedulerServer) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	err := s.scheduler.UpdateWorkerHeartbeat(
		req.WorkerId,
		int(req.CurrentTasks),
		req.CpuUsage,
		req.MemoryUsage,
	)

	if err != nil {
		return &proto.HeartbeatResponse{
			Success: false,
		}, err
	}

	return &proto.HeartbeatResponse{
		Success: true,
	}, nil
}

// ReportTaskComplete handles task completion reports
func (s *SchedulerServer) ReportTaskComplete(ctx context.Context, req *proto.TaskCompleteRequest) (*proto.TaskCompleteResponse, error) {
	var result map[string]interface{}
	if err := json.Unmarshal(req.Result, &result); err != nil {
		result = make(map[string]interface{})
	}

	duration := time.Duration(req.DurationMs) * time.Millisecond

	err := s.scheduler.TaskComplete(req.TaskId, req.WorkerId, result, duration)
	if err != nil {
		return &proto.TaskCompleteResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	return &proto.TaskCompleteResponse{
		Success: true,
		Message: "Task completion recorded",
	}, nil
}

// ReportTaskFailure handles task failure reports
func (s *SchedulerServer) ReportTaskFailure(ctx context.Context, req *proto.TaskFailureRequest) (*proto.TaskFailureResponse, error) {
	err := s.scheduler.TaskFailed(req.TaskId, req.WorkerId, req.Error)
	if err != nil {
		return &proto.TaskFailureResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	// Get task to determine retry info
	task, _ := s.scheduler.GetTask(req.TaskId)
	shouldRetry := task != nil && task.ShouldRetry()
	retryDelay := 0
	if shouldRetry && task != nil {
		retryDelay = int(task.NextRetryDelay().Seconds())
	}

	return &proto.TaskFailureResponse{
		Success:           true,
		Message:           "Task failure recorded",
		ShouldRetry:       shouldRetry,
		RetryDelaySeconds: int32(retryDelay),
	}, nil
}

// DeregisterWorker handles worker deregistration
func (s *SchedulerServer) DeregisterWorker(ctx context.Context, req *proto.DeregisterWorkerRequest) (*proto.DeregisterWorkerResponse, error) {
	logger.Info("Worker deregistration request",
		zap.String("worker_id", req.WorkerId),
		zap.String("reason", req.Reason))

	err := s.scheduler.DeregisterWorker(req.WorkerId)
	if err != nil {
		return &proto.DeregisterWorkerResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	return &proto.DeregisterWorkerResponse{
		Success: true,
		Message: "Worker deregistered successfully",
	}, nil
}
