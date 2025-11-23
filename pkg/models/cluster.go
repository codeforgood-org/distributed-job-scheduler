package models

import (
	"time"
)

// NodeRole represents the role of a scheduler node
type NodeRole string

const (
	NodeRoleLeader    NodeRole = "leader"
	NodeRoleFollower  NodeRole = "follower"
	NodeRoleCandidate NodeRole = "candidate"
)

// ClusterNode represents a scheduler node in the cluster
type ClusterNode struct {
	ID            string    `json:"id"`
	Role          NodeRole  `json:"role"`
	HTTPAddress   string    `json:"http_address"`
	RaftAddress   string    `json:"raft_address"`
	GRPCAddress   string    `json:"grpc_address"`
	IsLeader      bool      `json:"is_leader"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	JoinedAt      time.Time `json:"joined_at"`
	Term          uint64    `json:"term"` // Raft term
}

// ClusterState represents the current state of the cluster
type ClusterState struct {
	LeaderID      string                  `json:"leader_id"`
	Nodes         map[string]*ClusterNode `json:"nodes"`
	TotalNodes    int                     `json:"total_nodes"`
	HealthyNodes  int                     `json:"healthy_nodes"`
	Term          uint64                  `json:"term"`
	LastElection  time.Time               `json:"last_election"`
	ClusterHealth string                  `json:"cluster_health"` // "healthy", "degraded", "unhealthy"
}

// NewClusterState creates a new cluster state
func NewClusterState() *ClusterState {
	return &ClusterState{
		Nodes:         make(map[string]*ClusterNode),
		ClusterHealth: "healthy",
	}
}

// AddNode adds a node to the cluster state
func (cs *ClusterState) AddNode(node *ClusterNode) {
	cs.Nodes[node.ID] = node
	cs.TotalNodes = len(cs.Nodes)
	cs.updateHealth()
}

// RemoveNode removes a node from the cluster state
func (cs *ClusterState) RemoveNode(nodeID string) {
	delete(cs.Nodes, nodeID)
	cs.TotalNodes = len(cs.Nodes)
	cs.updateHealth()
}

// SetLeader sets the current leader
func (cs *ClusterState) SetLeader(nodeID string, term uint64) {
	cs.LeaderID = nodeID
	cs.Term = term
	cs.LastElection = time.Now()

	// Update node roles
	for id, node := range cs.Nodes {
		if id == nodeID {
			node.IsLeader = true
			node.Role = NodeRoleLeader
		} else {
			node.IsLeader = false
			node.Role = NodeRoleFollower
		}
	}
}

// GetLeader returns the current leader node
func (cs *ClusterState) GetLeader() (*ClusterNode, bool) {
	if cs.LeaderID == "" {
		return nil, false
	}
	leader, exists := cs.Nodes[cs.LeaderID]
	return leader, exists
}

// updateHealth calculates cluster health
func (cs *ClusterState) updateHealth() {
	healthy := 0
	for _, node := range cs.Nodes {
		if time.Since(node.LastHeartbeat) < 30*time.Second {
			healthy++
		}
	}
	cs.HealthyNodes = healthy

	healthRatio := float64(healthy) / float64(cs.TotalNodes)
	if healthRatio >= 0.75 {
		cs.ClusterHealth = "healthy"
	} else if healthRatio >= 0.5 {
		cs.ClusterHealth = "degraded"
	} else {
		cs.ClusterHealth = "unhealthy"
	}
}
