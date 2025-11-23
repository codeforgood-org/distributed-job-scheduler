package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/api"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/consensus"
	grpcserver "github.com/codeforgood-org/distributed-job-scheduler/pkg/grpc"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/scheduler"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/storage"
	"go.uber.org/zap"
)

var (
	nodeID    = flag.String("node-id", "", "Unique node ID")
	httpAddr  = flag.String("http-addr", ":8001", "HTTP server address")
	raftAddr  = flag.String("raft-addr", ":9001", "Raft consensus address")
	grpcAddr  = flag.String("grpc-addr", ":7001", "gRPC server address")
	dataDir   = flag.String("data-dir", "./data", "Data directory for storage")
	bootstrap = flag.Bool("bootstrap", false, "Bootstrap new cluster")
	joinAddr  = flag.String("join", "", "Address of existing cluster node to join")
	logLevel  = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	logFormat = flag.String("log-format", "json", "Log format (json, console)")
)

func main() {
	flag.Parse()

	// Initialize logger
	if err := logger.InitLogger(*logLevel, *logFormat); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Distributed Job Scheduler",
		zap.String("node_id", *nodeID),
		zap.String("http_addr", *httpAddr),
		zap.String("raft_addr", *raftAddr),
		zap.String("grpc_addr", *grpcAddr),
		zap.String("version", "1.0.0"))

	// Validate flags
	if *nodeID == "" {
		logger.Fatal("node-id is required")
	}

	// Create data directory
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		logger.Fatal("Failed to create data directory", zap.Error(err))
	}

	// Initialize storage
	storagePath := fmt.Sprintf("%s/%s", *dataDir, *nodeID)
	store, err := storage.NewStorage(storagePath)
	if err != nil {
		logger.Fatal("Failed to initialize storage", zap.Error(err))
	}
	defer store.Close()

	// Initialize Raft consensus
	raftConfig := &consensus.RaftConfig{
		NodeID:    *nodeID,
		RaftAddr:  *raftAddr,
		DataDir:   fmt.Sprintf("%s/%s/raft", *dataDir, *nodeID),
		Bootstrap: *bootstrap,
		JoinAddr:  *joinAddr,
	}

	raftNode, err := consensus.NewRaftNode(raftConfig, nil)
	if err != nil {
		logger.Fatal("Failed to initialize Raft", zap.Error(err))
	}
	defer raftNode.Shutdown()

	// Join existing cluster if specified
	if *joinAddr != "" && !*bootstrap {
		if err := raftNode.Join(*nodeID, *raftAddr); err != nil {
			logger.Fatal("Failed to join cluster", zap.Error(err))
		}
	}

	// Initialize scheduler
	sched := scheduler.NewScheduler(*nodeID, raftNode, store, scheduler.DefaultConfig())
	if err := sched.Start(); err != nil {
		logger.Fatal("Failed to start scheduler", zap.Error(err))
	}
	defer sched.Stop()

	// Start gRPC server
	grpcServer := grpcserver.NewSchedulerServer(sched, *grpcAddr)
	go func() {
		if err := grpcServer.Start(); err != nil {
			logger.Fatal("gRPC server failed", zap.Error(err))
		}
	}()
	defer grpcServer.Stop()

	// Start HTTP API server
	apiServer := api.NewServer(sched, *httpAddr)
	go func() {
		if err := apiServer.Start(); err != nil {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	logger.Info("Scheduler node started successfully",
		zap.String("node_id", *nodeID),
		zap.String("http", *httpAddr),
		zap.String("grpc", *grpcAddr),
		zap.String("raft", *raftAddr))

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down gracefully...")
}
