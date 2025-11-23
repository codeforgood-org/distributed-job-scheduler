# Distributed Job Scheduler Python SDK

Official Python client library for interacting with the Distributed Job Scheduler.

## Installation

```bash
pip install distributed-scheduler-client
```

## Quick Start

```python
from scheduler_client import SchedulerClient

# Create client
client = SchedulerClient("http://localhost:8001", api_key="your-api-key")

# Create a task
task = client.create_task(
    name="data-processing",
    task_type="batch",
    priority=7,
    payload={
        "input_file": "data.csv",
        "operation": "aggregate"
    },
    timeout=300
)

print(f"Task created: {task.id}")

# Wait for completion
completed_task = client.wait_for_task(task.id, timeout=600)

if completed_task.status == "completed":
    print(f"Result: {completed_task.result}")
else:
    print(f"Task failed: {completed_task.error}")
```

## Examples

### Create a Simple Task

```python
from scheduler_client import SchedulerClient, TaskType

client = SchedulerClient("http://localhost:8001")

task = client.create_task(
    name="hello-world",
    task_type=TaskType.BATCH.value,
    priority=5,
    payload={"message": "Hello from Python SDK!"}
)
```

### Create a Scheduled Task

```python
# Run daily at midnight
task = client.create_task(
    name="daily-report",
    task_type="report",
    schedule="0 0 * * *",
    payload={"report_type": "daily"}
)
```

### Create Tasks with Dependencies

```python
# Create parent task
task1 = client.create_task(name="extract-data", task_type="etl")

# Create dependent task
task2 = client.create_task(
    name="transform-data",
    task_type="etl",
    depends_on=[task1.id]
)
```

### List Tasks

```python
# Get all pending tasks
pending_tasks = client.list_tasks(status="pending")

# Get high-priority batch tasks
batch_tasks = client.list_tasks(task_type="batch", priority=10)

# Pagination
tasks_page1 = client.list_tasks(limit=20, offset=0)
tasks_page2 = client.list_tasks(limit=20, offset=20)
```

### Monitor Workers

```python
# List all workers
workers = client.list_workers()

for worker in workers:
    print(f"{worker.id}: {worker.status}, Health: {worker.health_score}")

# Get specific worker
worker = client.get_worker("worker1")
print(f"Current tasks: {worker.current_tasks}/{worker.max_concurrency}")
```

### Get Statistics

```python
stats = client.get_stats()

print(f"Total tasks: {stats.total}")
print(f"Success rate: {stats.success_rate}%")
print(f"By status: {stats.by_status}")
```

### Error Handling

```python
from scheduler_client import SchedulerClient, APIError, SchedulerError

client = SchedulerClient("http://localhost:8001")

try:
    task = client.create_task("my-task", "batch")
except APIError as e:
    print(f"API error ({e.status_code}): {e.message}")
except SchedulerError as e:
    print(f"Scheduler error: {str(e)}")
```

### Context Manager

```python
with SchedulerClient("http://localhost:8001") as client:
    task = client.create_task("my-task", "batch")
    result = client.wait_for_task(task.id)
# Client automatically closed
```

### Async Operations

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

async def submit_tasks(client, count):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=10) as executor:
        tasks = []
        for i in range(count):
            task_future = loop.run_in_executor(
                executor,
                client.create_task,
                f"task-{i}",
                "batch"
            )
            tasks.append(task_future)

        return await asyncio.gather(*tasks)

client = SchedulerClient("http://localhost:8001")
tasks = asyncio.run(submit_tasks(client, 100))
print(f"Created {len(tasks)} tasks")
```

## API Reference

### SchedulerClient

#### `__init__(base_url, api_key=None, jwt_token=None, timeout=30)`
Initialize the client.

#### `create_task(name, task_type, priority, payload, **kwargs)`
Create a new task.

#### `get_task(task_id)`
Get task by ID.

#### `list_tasks(status=None, task_type=None, priority=None, **kwargs)`
List tasks with filters.

#### `cancel_task(task_id)`
Cancel a task.

#### `wait_for_task(task_id, timeout=300, poll_interval=2)`
Wait for task completion.

#### `list_workers()`
List all workers.

#### `get_stats()`
Get scheduler statistics.

#### `health_check()`
Check scheduler health.

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black scheduler_client.py

# Type checking
mypy scheduler_client.py
```

## License

MIT License
