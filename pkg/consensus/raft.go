package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/metrics"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"
)

// RaftNode represents a Raft consensus node
type RaftNode struct {
	raft      *raft.Raft
	fsm       *FSM
	config    *RaftConfig
	transport *raft.NetworkTransport
}

// RaftConfig holds Raft configuration
type RaftConfig struct {
	NodeID      string
	RaftAddr    string
	DataDir     string
	Bootstrap   bool
	JoinAddr    string
}

// FSM implements the Raft finite state machine
type FSM struct {
	applyFunc func([]byte) interface{}
}

// CommandType represents different command types for the FSM
type CommandType string

const (
	CommandTaskUpdate   CommandType = "task_update"
	CommandWorkerUpdate CommandType = "worker_update"
	CommandStateUpdate  CommandType = "state_update"
)

// Command represents a command to be applied to the FSM
type Command struct {
	Type    CommandType `json:"type"`
	Payload []byte      `json:"payload"`
}

// NewRaftNode creates a new Raft node
func NewRaftNode(config *RaftConfig, applyFunc func([]byte) interface{}) (*RaftNode, error) {
	// Create data directory
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Setup Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.SnapshotThreshold = 1024
	raftConfig.HeartbeatTimeout = 1000 * time.Millisecond
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond

	// Create FSM
	fsm := &FSM{
		applyFunc: applyFunc,
	}

	// Setup Raft transport
	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft address: %w", err)
	}

	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(config.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create log store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	// Create stable store
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	node := &RaftNode{
		raft:      r,
		fsm:       fsm,
		config:    config,
		transport: transport,
	}

	// Bootstrap cluster if needed
	if config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(config.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		}
		f := r.BootstrapCluster(configuration)
		if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
		}
		logger.Info("Bootstrapped new Raft cluster", zap.String("node_id", config.NodeID))
	}

	// Monitor leadership changes
	go node.monitorLeadership()

	return node, nil
}

// Join joins an existing Raft cluster
func (rn *RaftNode) Join(nodeID, addr string) error {
	logger.Info("Joining Raft cluster", zap.String("node_id", nodeID), zap.String("addr", addr))

	configFuture := rn.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	// Check if node already exists
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// Node already member of cluster
			if srv.ID == raft.ServerID(nodeID) && srv.Address == raft.ServerAddress(addr) {
				logger.Info("Node already member of cluster")
				return nil
			}

			// Remove existing node with same ID or address
			future := rn.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("failed to remove existing node: %w", err)
			}
		}
	}

	// Add node to cluster
	f := rn.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := f.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %w", err)
	}

	logger.Info("Successfully joined cluster")
	return nil
}

// Apply applies a command to the FSM
func (rn *RaftNode) Apply(cmd *Command) error {
	if !rn.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	future := rn.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}

	return nil
}

// IsLeader checks if this node is the leader
func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

// GetLeader returns the current leader address
func (rn *RaftNode) GetLeader() string {
	addr, _ := rn.raft.LeaderWithID()
	return string(addr)
}

// GetState returns the current Raft state
func (rn *RaftNode) GetState() raft.RaftState {
	return rn.raft.State()
}

// Shutdown shuts down the Raft node
func (rn *RaftNode) Shutdown() error {
	future := rn.raft.Shutdown()
	return future.Error()
}

// Stats returns Raft statistics
func (rn *RaftNode) Stats() map[string]string {
	return rn.raft.Stats()
}

// monitorLeadership monitors leadership changes
func (rn *RaftNode) monitorLeadership() {
	for {
		select {
		case isLeader := <-rn.raft.LeaderCh():
			if isLeader {
				logger.Info("Became leader", zap.String("node_id", rn.config.NodeID))
				metrics.RecordLeaderElection()
				metrics.SetLeaderStatus(true)
			} else {
				logger.Info("Lost leadership", zap.String("node_id", rn.config.NodeID))
				metrics.SetLeaderStatus(false)
			}
		}
	}
}

// FSM implementation

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(l *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		logger.Error("Failed to unmarshal command", zap.Error(err))
		return err
	}

	if f.applyFunc != nil {
		return f.applyFunc(l.Data)
	}

	return nil
}

// Snapshot returns a snapshot of the FSM
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	// Implementation would restore state from snapshot
	return nil
}

// FSMSnapshot implements raft.FSMSnapshot
type FSMSnapshot struct{}

// Persist writes the snapshot to the SnapshotSink
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	// Implementation would write snapshot data
	return nil
}

// Release is called when the snapshot is no longer needed
func (f *FSMSnapshot) Release() {}

// TaskCommand creates a task update command
func TaskCommand(task *models.Task) (*Command, error) {
	payload, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}

	return &Command{
		Type:    CommandTaskUpdate,
		Payload: payload,
	}, nil
}

// WorkerCommand creates a worker update command
func WorkerCommand(worker *models.Worker) (*Command, error) {
	payload, err := json.Marshal(worker)
	if err != nil {
		return nil, err
	}

	return &Command{
		Type:    CommandWorkerUpdate,
		Payload: payload,
	}, nil
}
