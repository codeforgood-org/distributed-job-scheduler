# Distributed Job Scheduler TypeScript SDK

Official TypeScript/JavaScript client library for the Distributed Job Scheduler.

## Installation

```bash
npm install distributed-scheduler-client
# or
yarn add distributed-scheduler-client
# or
pnpm add distributed-scheduler-client
```

## Quick Start

### TypeScript

```typescript
import { SchedulerClient } from 'distributed-scheduler-client';

const client = new SchedulerClient('http://localhost:8001', {
  apiKey: 'your-api-key',
});

// Create a task
const task = await client.createTask({
  name: 'data-processing',
  type: 'batch',
  priority: 7,
  payload: {
    input_file: 'data.csv',
    operation: 'aggregate',
  },
  timeout: 300,
});

console.log(`Task created: ${task.id}`);

// Wait for completion
const completed = await client.waitForTask(task.id, 600);

if (completed.status === 'completed') {
  console.log('Result:', completed.result);
} else {
  console.log('Task failed:', completed.error);
}
```

### JavaScript (CommonJS)

```javascript
const { SchedulerClient } = require('distributed-scheduler-client');

const client = new SchedulerClient('http://localhost:8001');

client.createTask({
  name: 'hello-world',
  type: 'batch',
  payload: { message: 'Hello!' },
})
.then(task => console.log('Created:', task.id))
.catch(err => console.error('Error:', err));
```

## Examples

### Create Tasks

```typescript
// Simple task
const task = await client.createTask({
  name: 'simple-task',
  type: 'batch',
  priority: 5,
  payload: { data: 'value' },
});

// Scheduled task (daily at midnight)
const scheduledTask = await client.createTask({
  name: 'daily-report',
  type: 'report',
  schedule: '0 0 * * *',
  payload: { report_type: 'daily' },
});

// Task with dependencies
const parentTask = await client.createTask({
  name: 'extract-data',
  type: 'etl',
});

const childTask = await client.createTask({
  name: 'transform-data',
  type: 'etl',
  depends_on: [parentTask.id],
});
```

### List and Filter Tasks

```typescript
// Get all pending tasks
const pending = await client.listTasks({ status: 'pending' });

// Get high-priority batch tasks
const batch = await client.listTasks({
  type: 'batch',
  priority: 10,
});

// Pagination
const page1 = await client.listTasks({ limit: 20, offset: 0 });
const page2 = await client.listTasks({ limit: 20, offset: 20 });
```

### Monitor Workers

```typescript
// List all workers
const workers = await client.listWorkers();

workers.forEach(worker => {
  console.log(`${worker.id}: ${worker.status}, Health: ${worker.health_score}`);
});

// Get specific worker
const worker = await client.getWorker('worker1');
console.log(`Tasks: ${worker.current_tasks}/${worker.max_concurrency}`);
```

### Error Handling

```typescript
import { SchedulerClient, APIError, SchedulerError } from 'distributed-scheduler-client';

try {
  const task = await client.createTask({
    name: 'my-task',
    type: 'batch',
  });
} catch (error) {
  if (error instanceof APIError) {
    console.error(`API error (${error.statusCode}):`, error.message);
  } else if (error instanceof SchedulerError) {
    console.error('Scheduler error:', error.message);
  } else {
    console.error('Unknown error:', error);
  }
}
```

### Async/Await Patterns

```typescript
// Wait for multiple tasks
const tasks = await Promise.all([
  client.createTask({ name: 'task1', type: 'batch' }),
  client.createTask({ name: 'task2', type: 'batch' }),
  client.createTask({ name: 'task3', type: 'batch' }),
]);

// Wait for all to complete
const results = await Promise.all(
  tasks.map(task => client.waitForTask(task.id))
);

console.log('All tasks completed:', results);
```

### React Hook Example

```typescript
import { useEffect, useState } from 'react';
import { SchedulerClient, Task } from 'distributed-scheduler-client';

function useTasks() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const client = new SchedulerClient('http://localhost:8001');

    async function fetchTasks() {
      try {
        const data = await client.listTasks({ limit: 50 });
        setTasks(data);
      } catch (error) {
        console.error('Failed to fetch tasks:', error);
      } finally {
        setLoading(false);
      }
    }

    fetchTasks();
    const interval = setInterval(fetchTasks, 5000); // Poll every 5s

    return () => clearInterval(interval);
  }, []);

  return { tasks, loading };
}
```

### Node.js Script Example

```typescript
#!/usr/bin/env node

import { createClient } from 'distributed-scheduler-client';

async function main() {
  const client = createClient('http://localhost:8001');

  // Submit 100 tasks
  console.log('Submitting tasks...');

  const promises = [];
  for (let i = 0; i < 100; i++) {
    promises.push(
      client.createTask({
        name: `task-${i}`,
        type: 'batch',
        priority: Math.floor(Math.random() * 10) + 1,
        payload: { index: i },
      })
    );
  }

  const tasks = await Promise.all(promises);
  console.log(`Created ${tasks.length} tasks`);

  // Get stats
  const stats = await client.getStats();
  console.log('Statistics:', stats);
}

main().catch(console.error);
```

## API Reference

### SchedulerClient

#### Constructor

```typescript
new SchedulerClient(baseURL: string, options?: ClientOptions)
```

Options:
- `apiKey?: string` - API key for authentication
- `jwtToken?: string` - JWT token for authentication
- `timeout?: number` - Request timeout in milliseconds (default: 30000)
- `headers?: Record<string, string>` - Additional headers

#### Methods

##### `createTask(request: CreateTaskRequest): Promise<Task>`
Create a new task.

##### `getTask(taskId: string): Promise<Task>`
Get task by ID.

##### `listTasks(options?: ListTasksOptions): Promise<Task[]>`
List tasks with optional filters.

##### `cancelTask(taskId: string): Promise<void>`
Cancel a task.

##### `waitForTask(taskId: string, timeout?: number, pollInterval?: number): Promise<Task>`
Wait for task to complete.

##### `listWorkers(): Promise<Worker[]>`
List all workers.

##### `getStats(): Promise<TaskStats>`
Get scheduler statistics.

##### `healthCheck(): Promise<boolean>`
Check scheduler health.

## TypeScript Support

This library is written in TypeScript and includes full type definitions. All interfaces and types are exported:

```typescript
import type {
  Task,
  Worker,
  TaskStats,
  CreateTaskRequest,
  ListTasksOptions,
} from 'distributed-scheduler-client';
```

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test

# Lint
npm run lint

# Format
npm run format
```

## License

MIT License
