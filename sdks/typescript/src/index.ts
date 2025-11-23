/**
 * Distributed Job Scheduler TypeScript SDK
 *
 * A comprehensive TypeScript/JavaScript client for the Distributed Job Scheduler API.
 *
 * @example
 * ```typescript
 * import { SchedulerClient } from 'distributed-scheduler-client';
 *
 * const client = new SchedulerClient('http://localhost:8001', {
 *   apiKey: 'your-api-key'
 * });
 *
 * const task = await client.createTask({
 *   name: 'my-task',
 *   type: 'batch',
 *   priority: 5,
 *   payload: { data: 'value' }
 * });
 * ```
 */

export enum TaskStatus {
  PENDING = 'pending',
  SCHEDULED = 'scheduled',
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
  RETRYING = 'retrying',
  DLQ = 'dead_letter_queue',
}

export enum TaskType {
  BATCH = 'batch',
  STREAM = 'stream',
  REPORT = 'report',
  ETL = 'etl',
  ML = 'ml',
  CUSTOM = 'custom',
}

export interface Task {
  id: string;
  name: string;
  type: string;
  status: TaskStatus;
  priority: number;
  payload: Record<string, any>;
  result?: Record<string, any>;
  error?: string;
  max_retries: number;
  retry_count: number;
  timeout: number;
  namespace: string;
  tags?: Record<string, string>;
  created_at?: string;
  updated_at?: string;
  started_at?: string;
  completed_at?: string;
}

export interface CreateTaskRequest {
  name: string;
  type?: string;
  priority?: number;
  payload?: Record<string, any>;
  timeout?: number;
  max_retries?: number;
  namespace?: string;
  schedule?: string;
  depends_on?: string[];
  tags?: Record<string, string>;
}

export interface Worker {
  id: string;
  status: string;
  address: string;
  capabilities: string[];
  max_concurrency: number;
  current_tasks: number;
  health_score: number;
  total_tasks: number;
  successful_tasks: number;
  failed_tasks: number;
  last_heartbeat: string;
}

export interface TaskStats {
  total: number;
  success_rate: number;
  by_status: Record<string, number>;
  by_type: Record<string, number>;
  avg_duration: number;
}

export interface ListTasksOptions {
  status?: string;
  type?: string;
  priority?: number;
  namespace?: string;
  limit?: number;
  offset?: number;
}

export interface ClusterInfo {
  node_id: string;
  is_leader: boolean;
  state: string;
}

export interface ClientOptions {
  apiKey?: string;
  jwtToken?: string;
  timeout?: number;
  headers?: Record<string, string>;
}

export class SchedulerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SchedulerError';
  }
}

export class APIError extends SchedulerError {
  constructor(
    public statusCode: number,
    message: string
  ) {
    super(`API Error (${statusCode}): ${message}`);
    this.name = 'APIError';
  }
}

export class SchedulerClient {
  private baseURL: string;
  private options: ClientOptions;
  private defaultHeaders: Record<string, string>;

  constructor(baseURL: string, options: ClientOptions = {}) {
    this.baseURL = baseURL.replace(/\/$/, '');
    this.options = options;

    this.defaultHeaders = {
      'Content-Type': 'application/json',
      ...options.headers,
    };

    if (options.apiKey) {
      this.defaultHeaders['Authorization'] = `ApiKey ${options.apiKey}`;
    } else if (options.jwtToken) {
      this.defaultHeaders['Authorization'] = `Bearer ${options.jwtToken}`;
    }
  }

  private async request<T>(
    method: string,
    path: string,
    body?: any,
    params?: Record<string, string | number>
  ): Promise<T> {
    const url = new URL(path, this.baseURL);

    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        url.searchParams.append(key, String(value));
      });
    }

    const response = await fetch(url.toString(), {
      method,
      headers: this.defaultHeaders,
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(this.options.timeout || 30000),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ error: response.statusText }));
      throw new APIError(response.status, errorData.error || 'Unknown error');
    }

    if (response.status === 204) {
      return {} as T;
    }

    return response.json();
  }

  // Task methods

  /**
   * Create a new task
   */
  async createTask(request: CreateTaskRequest): Promise<Task> {
    const payload = {
      name: request.name,
      type: request.type || 'batch',
      priority: request.priority ?? 5,
      payload: request.payload || {},
      timeout: request.timeout || 300,
      max_retries: request.max_retries ?? 3,
      namespace: request.namespace || 'default',
      schedule: request.schedule,
      depends_on: request.depends_on,
      tags: request.tags,
    };

    const response = await this.request<{ task_id: string }>(
      'POST',
      '/api/v1/tasks',
      payload
    );

    return this.getTask(response.task_id);
  }

  /**
   * Get task by ID
   */
  async getTask(taskId: string): Promise<Task> {
    return this.request<Task>('GET', `/api/v1/tasks/${taskId}`);
  }

  /**
   * List tasks with optional filters
   */
  async listTasks(options: ListTasksOptions = {}): Promise<Task[]> {
    const params: Record<string, string | number> = {
      limit: options.limit || 100,
      offset: options.offset || 0,
    };

    if (options.status) params.status = options.status;
    if (options.type) params.type = options.type;
    if (options.priority !== undefined) params.priority = options.priority;
    if (options.namespace) params.namespace = options.namespace;

    const response = await this.request<{ tasks: Task[] }>(
      'GET',
      '/api/v1/tasks',
      undefined,
      params
    );

    return response.tasks || [];
  }

  /**
   * Cancel a task
   */
  async cancelTask(taskId: string): Promise<void> {
    await this.request('DELETE', `/api/v1/tasks/${taskId}`);
  }

  /**
   * Wait for task to complete
   */
  async waitForTask(
    taskId: string,
    timeout: number = 300,
    pollInterval: number = 2
  ): Promise<Task> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeout * 1000) {
      const task = await this.getTask(taskId);

      if (
        [
          TaskStatus.COMPLETED,
          TaskStatus.FAILED,
          TaskStatus.CANCELLED,
          TaskStatus.DLQ,
        ].includes(task.status as TaskStatus)
      ) {
        return task;
      }

      await new Promise((resolve) => setTimeout(resolve, pollInterval * 1000));
    }

    throw new Error(`Task ${taskId} did not complete within ${timeout} seconds`);
  }

  // Worker methods

  /**
   * List all workers
   */
  async listWorkers(): Promise<Worker[]> {
    const response = await this.request<{ workers: Worker[] }>('GET', '/api/v1/workers');
    return response.workers || [];
  }

  /**
   * Get worker by ID
   */
  async getWorker(workerId: string): Promise<Worker> {
    return this.request<Worker>('GET', `/api/v1/workers/${workerId}`);
  }

  // Stats and monitoring

  /**
   * Get scheduler statistics
   */
  async getStats(): Promise<TaskStats> {
    return this.request<TaskStats>('GET', '/api/v1/stats');
  }

  /**
   * Get cluster information
   */
  async getClusterInfo(): Promise<ClusterInfo> {
    return this.request<ClusterInfo>('GET', '/api/v1/cluster');
  }

  /**
   * Check if scheduler is healthy
   */
  async healthCheck(): Promise<boolean> {
    try {
      await this.request('GET', '/health');
      return true;
    } catch {
      return false;
    }
  }
}

/**
 * Create a scheduler client with default settings
 */
export function createClient(
  baseURL: string = 'http://localhost:8001',
  options?: ClientOptions
): SchedulerClient {
  return new SchedulerClient(baseURL, options);
}

// Export everything
export default SchedulerClient;
