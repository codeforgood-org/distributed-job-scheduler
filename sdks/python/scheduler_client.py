"""
Distributed Job Scheduler Python SDK

A comprehensive Python client for interacting with the Distributed Job Scheduler API.

Example:
    >>> from scheduler_client import SchedulerClient
    >>> client = SchedulerClient("http://localhost:8001", api_key="your-api-key")
    >>> task = client.create_task("my-task", "batch", priority=5, payload={"data": "value"})
    >>> print(task.id)
"""

import requests
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum


class TaskStatus(Enum):
    """Task status enumeration"""
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    DLQ = "dead_letter_queue"


class TaskType(Enum):
    """Task type enumeration"""
    BATCH = "batch"
    STREAM = "stream"
    REPORT = "report"
    ETL = "etl"
    ML = "ml"
    CUSTOM = "custom"


@dataclass
class Task:
    """Represents a task in the scheduler"""
    id: str
    name: str
    type: str
    status: str
    priority: int
    payload: Dict[str, Any]
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    max_retries: int = 3
    retry_count: int = 0
    timeout: int = 300
    namespace: str = "default"
    tags: Optional[Dict[str, str]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict) -> 'Task':
        """Create Task from dictionary"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class Worker:
    """Represents a worker node"""
    id: str
    status: str
    address: str
    capabilities: List[str]
    max_concurrency: int
    current_tasks: int
    health_score: float
    total_tasks: int
    successful_tasks: int
    failed_tasks: int

    @classmethod
    def from_dict(cls, data: Dict) -> 'Worker':
        """Create Worker from dictionary"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class TaskStats:
    """Represents scheduler statistics"""
    total: int
    success_rate: float
    by_status: Dict[str, int]
    by_type: Dict[str, int]
    avg_duration: int

    @classmethod
    def from_dict(cls, data: Dict) -> 'TaskStats':
        """Create TaskStats from dictionary"""
        return cls(
            total=data.get('total', 0),
            success_rate=data.get('success_rate', 0.0),
            by_status=data.get('by_status', {}),
            by_type=data.get('by_type', {}),
            avg_duration=data.get('avg_duration', 0)
        )


class SchedulerError(Exception):
    """Base exception for scheduler errors"""
    pass


class APIError(SchedulerError):
    """API request failed"""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"API Error ({status_code}): {message}")


class SchedulerClient:
    """
    Client for interacting with Distributed Job Scheduler API

    Args:
        base_url: Base URL of the scheduler API
        api_key: Optional API key for authentication
        jwt_token: Optional JWT token for authentication
        timeout: Request timeout in seconds (default: 30)
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        jwt_token: Optional[str] = None,
        timeout: int = 30
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.jwt_token = jwt_token
        self.timeout = timeout
        self.session = requests.Session()

        # Set authentication headers
        if api_key:
            self.session.headers.update({'Authorization': f'ApiKey {api_key}'})
        elif jwt_token:
            self.session.headers.update({'Authorization': f'Bearer {jwt_token}'})

    def _request(
        self,
        method: str,
        path: str,
        json: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict:
        """Make HTTP request to API"""
        url = f"{self.base_url}{path}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                json=json,
                params=params,
                timeout=self.timeout
            )

            if response.status_code >= 400:
                error_msg = response.json().get('error', response.text)
                raise APIError(response.status_code, error_msg)

            return response.json() if response.content else {}

        except requests.RequestException as e:
            raise SchedulerError(f"Request failed: {str(e)}")

    # Task methods

    def create_task(
        self,
        name: str,
        task_type: str = "batch",
        priority: int = 5,
        payload: Optional[Dict] = None,
        timeout: int = 300,
        max_retries: int = 3,
        namespace: str = "default",
        schedule: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Task:
        """
        Create a new task

        Args:
            name: Task name
            task_type: Type of task (batch, etl, ml, etc.)
            priority: Priority level (1-10, higher is more priority)
            payload: Task payload data
            timeout: Task timeout in seconds
            max_retries: Maximum number of retries
            namespace: Task namespace for multi-tenancy
            schedule: Optional cron schedule for recurring tasks
            depends_on: List of task IDs this task depends on
            tags: Optional tags for task categorization

        Returns:
            Created Task object
        """
        data = {
            "name": name,
            "type": task_type,
            "priority": priority,
            "payload": payload or {},
            "timeout": timeout,
            "max_retries": max_retries,
            "namespace": namespace
        }

        if schedule:
            data["schedule"] = schedule
        if depends_on:
            data["depends_on"] = depends_on
        if tags:
            data["tags"] = tags

        response = self._request("POST", "/api/v1/tasks", json=data)

        # Fetch the created task
        return self.get_task(response["task_id"])

    def get_task(self, task_id: str) -> Task:
        """
        Get task by ID

        Args:
            task_id: Task ID

        Returns:
            Task object
        """
        data = self._request("GET", f"/api/v1/tasks/{task_id}")
        return Task.from_dict(data)

    def list_tasks(
        self,
        status: Optional[str] = None,
        task_type: Optional[str] = None,
        priority: Optional[int] = None,
        namespace: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Task]:
        """
        List tasks with optional filters

        Args:
            status: Filter by status
            task_type: Filter by type
            priority: Filter by priority
            namespace: Filter by namespace
            limit: Maximum number of tasks to return
            offset: Offset for pagination

        Returns:
            List of Task objects
        """
        params = {"limit": limit, "offset": offset}

        if status:
            params["status"] = status
        if task_type:
            params["type"] = task_type
        if priority is not None:
            params["priority"] = priority
        if namespace:
            params["namespace"] = namespace

        data = self._request("GET", "/api/v1/tasks", params=params)
        return [Task.from_dict(t) for t in data.get("tasks", [])]

    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a task

        Args:
            task_id: Task ID

        Returns:
            True if successful
        """
        self._request("DELETE", f"/api/v1/tasks/{task_id}")
        return True

    def wait_for_task(
        self,
        task_id: str,
        timeout: int = 300,
        poll_interval: int = 2
    ) -> Task:
        """
        Wait for task to complete

        Args:
            task_id: Task ID
            timeout: Maximum time to wait in seconds
            poll_interval: Polling interval in seconds

        Returns:
            Completed Task object

        Raises:
            TimeoutError: If task doesn't complete within timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            task = self.get_task(task_id)

            if task.status in [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value,
                             TaskStatus.CANCELLED.value, TaskStatus.DLQ.value]:
                return task

            time.sleep(poll_interval)

        raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")

    # Worker methods

    def list_workers(self) -> List[Worker]:
        """
        List all workers

        Returns:
            List of Worker objects
        """
        data = self._request("GET", "/api/v1/workers")
        return [Worker.from_dict(w) for w in data.get("workers", [])]

    def get_worker(self, worker_id: str) -> Worker:
        """
        Get worker by ID

        Args:
            worker_id: Worker ID

        Returns:
            Worker object
        """
        data = self._request("GET", f"/api/v1/workers/{worker_id}")
        return Worker.from_dict(data)

    # Stats and monitoring

    def get_stats(self) -> TaskStats:
        """
        Get scheduler statistics

        Returns:
            TaskStats object
        """
        data = self._request("GET", "/api/v1/stats")
        return TaskStats.from_dict(data)

    def get_cluster_info(self) -> Dict:
        """
        Get cluster information

        Returns:
            Cluster info dictionary
        """
        return self._request("GET", "/api/v1/cluster")

    def health_check(self) -> bool:
        """
        Check if scheduler is healthy

        Returns:
            True if healthy
        """
        try:
            self._request("GET", "/health")
            return True
        except:
            return False

    # Context manager support

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()


# Convenience functions

def create_client(
    base_url: str = "http://localhost:8001",
    **kwargs
) -> SchedulerClient:
    """Create a scheduler client with default settings"""
    return SchedulerClient(base_url, **kwargs)
