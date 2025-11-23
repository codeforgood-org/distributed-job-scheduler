package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Task metrics
	TasksTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_tasks_total",
			Help: "Total number of tasks by status",
		},
		[]string{"status", "type", "namespace"},
	)

	TaskDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scheduler_task_duration_seconds",
			Help:    "Task execution duration in seconds",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		},
		[]string{"type", "status"},
	)

	TaskQueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_queue_depth",
			Help: "Current task queue depth",
		},
		[]string{"status"},
	)

	// Worker metrics
	WorkersActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "scheduler_workers_active",
			Help: "Number of active workers",
		},
	)

	WorkerTasksProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_tasks_processed_total",
			Help: "Total tasks processed by worker",
		},
		[]string{"worker_id", "status"},
	)

	WorkerHealthScore = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "worker_health_score",
			Help: "Worker health score (0-100)",
		},
		[]string{"worker_id"},
	)

	// Cluster metrics
	LeaderElections = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "scheduler_leader_elections_total",
			Help: "Total number of leader elections",
		},
	)

	ClusterNodes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scheduler_cluster_nodes",
			Help: "Number of cluster nodes by status",
		},
		[]string{"status"},
	)

	IsLeader = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "scheduler_is_leader",
			Help: "Whether this node is the leader (1=leader, 0=follower)",
		},
	)

	// Raft metrics
	RaftTerm = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "raft_term",
			Help: "Current Raft term",
		},
	)

	RaftCommitIndex = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "raft_commit_index",
			Help: "Raft commit index",
		},
	)

	// API metrics
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help: "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// gRPC metrics
	GRPCRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total gRPC requests",
		},
		[]string{"method", "status"},
	)

	GRPCRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "gRPC request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// Retry metrics
	TaskRetries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scheduler_task_retries_total",
			Help: "Total task retries",
		},
		[]string{"type"},
	)

	TaskDLQ = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "scheduler_task_dlq_total",
			Help: "Total tasks moved to dead letter queue",
		},
	)
)

// RecordTaskStart records the start of a task
func RecordTaskStart(taskType, namespace string) {
	TasksTotal.WithLabelValues("running", taskType, namespace).Inc()
	TaskQueueDepth.WithLabelValues("running").Inc()
}

// RecordTaskComplete records task completion
func RecordTaskComplete(taskType, status, namespace string, duration float64) {
	TasksTotal.WithLabelValues(status, taskType, namespace).Inc()
	TaskDuration.WithLabelValues(taskType, status).Observe(duration)
	TaskQueueDepth.WithLabelValues("running").Dec()
}

// RecordWorkerRegistration records worker registration
func RecordWorkerRegistration() {
	WorkersActive.Inc()
}

// RecordWorkerDeregistration records worker deregistration
func RecordWorkerDeregistration() {
	WorkersActive.Dec()
}

// UpdateWorkerHealth updates worker health score
func UpdateWorkerHealth(workerID string, score float64) {
	WorkerHealthScore.WithLabelValues(workerID).Set(score)
}

// RecordLeaderElection records a leader election event
func RecordLeaderElection() {
	LeaderElections.Inc()
}

// SetLeaderStatus sets whether this node is the leader
func SetLeaderStatus(isLeader bool) {
	if isLeader {
		IsLeader.Set(1)
	} else {
		IsLeader.Set(0)
	}
}

// UpdateQueueDepth updates the queue depth metric
func UpdateQueueDepth(status string, depth int) {
	TaskQueueDepth.WithLabelValues(status).Set(float64(depth))
}
