# 🚀 Quick Start Guide

Get up and running with the Distributed Job Scheduler in 5 minutes!

## Option 1: Docker Compose (Recommended)

The fastest way to try out the scheduler:

```bash
# Clone the repository
git clone https://github.com/codeforgood-org/distributed-job-scheduler.git
cd distributed-job-scheduler

# Start the cluster
docker-compose up -d

# Wait for services to start (30 seconds)
sleep 30

# Check status
docker-compose ps
```

This starts:
- 3 scheduler nodes (with automatic leader election)
- 5 worker nodes
- Prometheus for metrics

### Access the Services

- **Dashboard**: http://localhost:8001
- **API**: http://localhost:8001/api/v1
- **Prometheus**: http://localhost:9090

### Submit Your First Task

```bash
curl -X POST http://localhost:8001/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-first-task",
    "type": "batch",
    "priority": 5,
    "payload": {
      "message": "Hello from distributed scheduler!",
      "duration": 2
    }
  }'
```

### Check Task Status

```bash
# Get all tasks
curl http://localhost:8001/api/v1/tasks

# Get specific task (use task_id from previous response)
curl http://localhost:8001/api/v1/tasks/{task-id}

# View statistics
curl http://localhost:8001/api/v1/stats
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f scheduler1
docker-compose logs -f worker1
```

### Stop the Cluster

```bash
docker-compose down
```

## Option 2: Local Development

Run scheduler and worker locally:

### Prerequisites

- Go 1.21+
- Make (optional)

### Build from Source

```bash
# Download dependencies
go mod download

# Build binaries
make build
# or
go build -o bin/scheduler ./cmd/scheduler
go build -o bin/worker ./cmd/worker
```

### Start Scheduler

```bash
# Terminal 1: Start first scheduler node (bootstrap)
./bin/scheduler \
  --node-id=scheduler1 \
  --http-addr=:8001 \
  --raft-addr=:9001 \
  --grpc-addr=:7001 \
  --bootstrap=true \
  --log-format=console
```

### Start Worker

```bash
# Terminal 2: Start worker
./bin/worker \
  --worker-id=worker1 \
  --scheduler=localhost:7001 \
  --max-concurrency=4 \
  --log-format=console
```

### Run Example Scripts

```bash
# Submit sample tasks
go run examples/submit_tasks.go

# Run load test
go run examples/load_test.go \
  --url=http://localhost:8001 \
  --tasks=1000 \
  --concurrency=10
```

## Option 3: Kubernetes

Deploy to Kubernetes cluster:

```bash
# Apply manifests
kubectl apply -f deployments/kubernetes/

# Check pods
kubectl get pods

# Port forward to access dashboard
kubectl port-forward svc/scheduler-lb 8001:80

# Access dashboard
open http://localhost:8001
```

## Common Tasks

### Submit Different Task Types

**Batch Processing**
```bash
curl -X POST http://localhost:8001/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "batch-processing",
    "type": "batch",
    "priority": 7,
    "payload": {
      "input_file": "data.csv",
      "operation": "aggregate"
    },
    "timeout": 300
  }'
```

**ETL Pipeline**
```bash
curl -X POST http://localhost:8001/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "etl-pipeline",
    "type": "etl",
    "priority": 8,
    "payload": {
      "source": "postgres://db",
      "destination": "s3://bucket"
    }
  }'
```

**Scheduled Task (Cron)**
```bash
curl -X POST http://localhost:8001/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "daily-report",
    "type": "report",
    "schedule": "0 0 * * *",
    "payload": {
      "report_type": "daily"
    }
  }'
```

### List Tasks with Filters

```bash
# By status
curl "http://localhost:8001/api/v1/tasks?status=running"

# By type
curl "http://localhost:8001/api/v1/tasks?type=batch"

# By priority
curl "http://localhost:8001/api/v1/tasks?priority=10"

# With pagination
curl "http://localhost:8001/api/v1/tasks?limit=20&offset=0"
```

### Monitor Workers

```bash
# List all workers
curl http://localhost:8001/api/v1/workers

# View specific worker
curl http://localhost:8001/api/v1/workers/worker1
```

### View Metrics

```bash
# Prometheus metrics
curl http://localhost:8001/metrics

# Scheduler stats
curl http://localhost:8001/api/v1/stats
```

## Testing Leader Election

Stop the current leader to see automatic failover:

```bash
# Find current leader
curl http://localhost:8001/api/v1/cluster

# Stop leader node
docker-compose stop scheduler1

# Wait 2-3 seconds for election

# Check new leader (should be scheduler2 or scheduler3)
curl http://localhost:8002/api/v1/cluster
```

## Scaling Workers

```bash
# Docker Compose
docker-compose up -d --scale worker=10

# Kubernetes
kubectl scale deployment worker --replicas=10
```

## Troubleshooting

### Scheduler not starting

```bash
# Check logs
docker-compose logs scheduler1

# Verify ports are not in use
lsof -i :8001
lsof -i :9001
```

### Worker not connecting

```bash
# Check worker logs
docker-compose logs worker1

# Verify scheduler is running
curl http://localhost:8001/health

# Check network connectivity
docker-compose exec worker1 ping scheduler1
```

### Tasks stuck in pending

```bash
# Check if workers are registered
curl http://localhost:8001/api/v1/workers

# Check scheduler is leader
curl http://localhost:8001/api/v1/cluster

# View worker logs for errors
docker-compose logs worker1
```

## Next Steps

1. **Read the full documentation** - [README.md](README.md)
2. **Try the examples** - [examples/](examples/)
3. **Explore the API** - See API documentation below
4. **Customize task executors** - Modify worker handlers
5. **Deploy to production** - See deployment guides

## API Reference

### Create Task
- **POST** `/api/v1/tasks`
- Body: `{name, type, priority, payload, ...}`

### Get Task
- **GET** `/api/v1/tasks/:id`

### List Tasks
- **GET** `/api/v1/tasks?status=&type=&priority=`

### Cancel Task
- **DELETE** `/api/v1/tasks/:id`

### List Workers
- **GET** `/api/v1/workers`

### Get Statistics
- **GET** `/api/v1/stats`

### Health Check
- **GET** `/health`

### Cluster Info
- **GET** `/api/v1/cluster`

## Resources

- **Documentation**: [README.md](README.md)
- **Contributing**: [CONTRIBUTING.md](CONTRIBUTING.md)
- **Examples**: [examples/](examples/)
- **Issues**: https://github.com/codeforgood-org/distributed-job-scheduler/issues

Happy scheduling! 🎉
