# Distributed Job Scheduler

A production-ready distributed task scheduler built in Go, featuring Raft-based leader election, persistent task queues, worker pools, and comprehensive monitoring.

## 🌟 Features

### Core Capabilities
- **Leader Election**: Raft consensus algorithm ensures only one scheduler distributes tasks
- **Distributed Task Queue**: Persistent, fault-tolerant task storage with priority support
- **Worker Pool Management**: Dynamic worker registration, health monitoring, and load balancing
- **Task Retry Logic**: Exponential backoff with configurable retry policies
- **Cron Scheduling**: Support for recurring tasks with cron expressions
- **Dead Letter Queue**: Failed tasks moved to DLQ for analysis

### Production Ready
- **High Availability**: Multi-node cluster with automatic failover
- **Horizontal Scalability**: Add schedulers and workers dynamically
- **Persistent Storage**: BadgerDB for local state, Raft log for consensus
- **Monitoring**: Prometheus metrics, health endpoints, structured logging
- **Graceful Shutdown**: Clean resource cleanup and task handoff
- **RESTful API**: Complete API for task management
- **Web Dashboard**: Real-time monitoring UI

### Task Features
- Priority-based scheduling (1-10)
- Task dependencies (DAG support)
- Timeout enforcement
- Task cancellation
- Rate limiting per task type
- Multi-tenancy with namespaces
- Custom task metadata and tags

## 📋 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                   │
└────────────────┬────────────────────────────────────────┘
                 │ HTTP REST API
                 ▼
┌─────────────────────────────────────────────────────────┐
│              Scheduler Cluster (Raft)                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │Scheduler1│  │Scheduler2│  │Scheduler3│              │
│  │ (Leader) │  │(Follower)│  │(Follower)│              │
│  └────┬─────┘  └──────────┘  └──────────┘              │
│       │ Raft Consensus + Task Distribution              │
└───────┼─────────────────────────────────────────────────┘
        │ gRPC
        ▼
┌─────────────────────────────────────────────────────────┐
│                   Worker Pool                            │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐        │
│  │Worker 1│  │Worker 2│  │Worker 3│  │Worker N│        │
│  └────────┘  └────────┘  └────────┘  └────────┘        │
└─────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│              Monitoring & Storage                        │
│  [Prometheus] [BadgerDB] [Raft Logs]                    │
└─────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Go 1.21+
- Docker & Docker Compose (optional)

### Run Locally

```bash
# Start a 3-node scheduler cluster
go run cmd/scheduler/main.go --node-id=node1 --http-addr=:8001 --raft-addr=:9001 --grpc-addr=:7001

go run cmd/scheduler/main.go --node-id=node2 --http-addr=:8002 --raft-addr=:9002 --grpc-addr=:7002 --join=localhost:9001

go run cmd/scheduler/main.go --node-id=node3 --http-addr=:8003 --raft-addr=:9003 --grpc-addr=:7003 --join=localhost:9001

# Start workers
go run cmd/worker/main.go --worker-id=worker1 --scheduler=localhost:7001
go run cmd/worker/main.go --worker-id=worker2 --scheduler=localhost:7001
```

### Run with Docker Compose

```bash
docker-compose up -d
```

This starts:
- 3 scheduler nodes (ports 8001-8003)
- 5 worker nodes
- Prometheus (port 9090)
- Web Dashboard (port 3000)

## 📝 Usage Examples

### Submit a Task

```bash
curl -X POST http://localhost:8001/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "data-processing",
    "type": "batch",
    "priority": 5,
    "payload": {
      "input_file": "data.csv",
      "operation": "aggregate"
    },
    "timeout": 300,
    "max_retries": 3
  }'
```

### Create Recurring Task

```bash
curl -X POST http://localhost:8001/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "daily-report",
    "type": "report",
    "schedule": "0 0 * * *",
    "payload": {"report_type": "daily"}
  }'
```

### Check Task Status

```bash
curl http://localhost:8001/api/v1/tasks/{task-id}
```

### List Tasks

```bash
curl http://localhost:8001/api/v1/tasks?status=pending&priority=5
```

## 🔧 Configuration

Configuration via YAML file or environment variables:

```yaml
# config/scheduler.yaml
node:
  id: "node1"
  data_dir: "./data"

http:
  addr: ":8001"

grpc:
  addr: ":7001"

raft:
  addr: ":9001"
  bootstrap: true
  join_addr: ""

scheduler:
  task_timeout: 300s
  worker_timeout: 30s
  max_retries: 3

storage:
  backend: "badger"
  path: "./data/tasks"

logging:
  level: "info"
  format: "json"
```

## 📊 Monitoring

### Prometheus Metrics

Available at `http://localhost:8001/metrics`:

- `scheduler_tasks_total{status}` - Total tasks by status
- `scheduler_tasks_duration_seconds` - Task execution duration
- `scheduler_workers_active` - Active worker count
- `scheduler_leader_elections_total` - Leader election count
- `scheduler_queue_depth` - Current queue depth
- `worker_tasks_processed_total` - Tasks processed by worker

### Health Checks

```bash
# Scheduler health
curl http://localhost:8001/health

# Worker health
curl http://localhost:8101/health
```

### Web Dashboard

Access at `http://localhost:3000`:
- Real-time task statistics
- Worker pool status
- Queue visualization
- Cluster health

## 🏗️ Project Structure

```
.
├── cmd/
│   ├── scheduler/          # Scheduler node binary
│   └── worker/             # Worker node binary
├── pkg/
│   ├── api/                # REST API handlers
│   ├── consensus/          # Raft consensus implementation
│   ├── scheduler/          # Core scheduling logic
│   ├── worker/             # Task execution engine
│   ├── storage/            # Persistence layer
│   ├── models/             # Data models
│   ├── queue/              # Priority queue implementation
│   ├── proto/              # gRPC/protobuf definitions
│   ├── metrics/            # Prometheus metrics
│   └── logger/             # Structured logging
├── web/                    # Dashboard UI (React)
├── config/                 # Configuration files
├── deployments/
│   ├── docker/             # Dockerfiles
│   ├── kubernetes/         # K8s manifests
│   └── docker-compose.yml
├── tests/                  # Integration tests
├── examples/               # Usage examples
└── scripts/                # Utility scripts
```

## 🧪 Testing

```bash
# Unit tests
go test ./...

# Integration tests
go test -tags=integration ./tests/...

# Load testing
go run examples/load_test.go --tasks=10000 --workers=50
```

## 🚢 Deployment

### Kubernetes

```bash
kubectl apply -f deployments/kubernetes/
```

This deploys:
- StatefulSet for scheduler cluster (3 replicas)
- Deployment for workers (auto-scaling)
- Services and ingress
- ConfigMaps and secrets

### Production Checklist

- [ ] Configure persistent volumes for scheduler data
- [ ] Set up monitoring alerts in Prometheus
- [ ] Enable TLS for gRPC and HTTP
- [ ] Configure resource limits
- [ ] Set up log aggregation
- [ ] Enable authentication/authorization
- [ ] Configure backup strategy for Raft state
- [ ] Set up distributed tracing (optional)

## 🎓 Learning Concepts

This project teaches:

1. **Leader Election**: Raft consensus ensures one leader coordinates work
2. **Distributed Consensus**: How nodes agree on cluster state
3. **Task Distribution**: Load balancing strategies and worker selection
4. **Fault Tolerance**: Handling node failures and network partitions
5. **Persistent State**: Maintaining consistency across restarts
6. **Monitoring**: Observability in distributed systems
7. **Graceful Degradation**: Circuit breakers and retry logic

## 🤝 Contributing

Contributions welcome! Please read CONTRIBUTING.md first.

## 📄 License

MIT License - see LICENSE file

## 🔗 Resources

- [Raft Consensus Algorithm](https://raft.github.io/)
- [Distributed Systems Patterns](https://martinfowler.com/articles/patterns-of-distributed-systems/)
- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
