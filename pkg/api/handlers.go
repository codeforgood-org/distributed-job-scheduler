package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/metrics"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/scheduler"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Server provides HTTP API for the scheduler
type Server struct {
	scheduler *scheduler.Scheduler
	router    *gin.Engine
	addr      string
}

// NewServer creates a new API server
func NewServer(sched *scheduler.Scheduler, addr string) *Server {
	// Set Gin to release mode
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(loggingMiddleware())
	router.Use(metricsMiddleware())
	router.Use(corsMiddleware())

	server := &Server{
		scheduler: sched,
		router:    router,
		addr:      addr,
	}

	server.setupRoutes()
	return server
}

// setupRoutes configures API routes
func (s *Server) setupRoutes() {
	// Health check
	s.router.GET("/health", s.healthCheck)
	s.router.GET("/ready", s.readyCheck)

	// Metrics
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// API v1
	v1 := s.router.Group("/api/v1")
	{
		// Task endpoints
		tasks := v1.Group("/tasks")
		{
			tasks.POST("", s.createTask)
			tasks.GET("", s.listTasks)
			tasks.GET("/:id", s.getTask)
			tasks.DELETE("/:id", s.cancelTask)
		}

		// Worker endpoints
		workers := v1.Group("/workers")
		{
			workers.GET("", s.listWorkers)
			workers.GET("/:id", s.getWorker)
		}

		// Stats endpoints
		v1.GET("/stats", s.getStats)
		v1.GET("/cluster", s.getClusterInfo)
	}

	// Serve dashboard
	s.router.GET("/", s.serveDashboard)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	logger.Info("Starting API server", zap.String("addr", s.addr))
	return s.router.Run(s.addr)
}

// Health check handlers

func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "distributed-job-scheduler",
		"version": "1.0.0",
		"time":    time.Now().Unix(),
	})
}

func (s *Server) readyCheck(c *gin.Context) {
	// Check if scheduler is ready
	if !s.scheduler.IsLeader() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "not ready",
			"message": "not the leader",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ready",
		"leader":  true,
	})
}

// Task handlers

func (s *Server) createTask(c *gin.Context) {
	var req struct {
		Name         string                 `json:"name" binding:"required"`
		Type         models.TaskType        `json:"type"`
		Priority     int                    `json:"priority"`
		Payload      map[string]interface{} `json:"payload"`
		Schedule     string                 `json:"schedule"`
		Timeout      int                    `json:"timeout"` // seconds
		MaxRetries   int                    `json:"max_retries"`
		DependsOn    []string               `json:"depends_on"`
		Namespace    string                 `json:"namespace"`
		Tags         map[string]string      `json:"tags"`
		Metadata     map[string]string      `json:"metadata"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Create task
	task := models.NewTask(req.Name, req.Type, req.Priority, req.Payload)

	if req.Schedule != "" {
		task.Schedule = req.Schedule
	}
	if req.Timeout > 0 {
		task.Timeout = time.Duration(req.Timeout) * time.Second
	}
	if req.MaxRetries > 0 {
		task.MaxRetries = req.MaxRetries
	}
	if req.Namespace != "" {
		task.Namespace = req.Namespace
	}
	if len(req.DependsOn) > 0 {
		task.DependsOn = req.DependsOn
	}
	if len(req.Tags) > 0 {
		task.Tags = req.Tags
	}
	if len(req.Metadata) > 0 {
		task.Metadata = req.Metadata
	}

	// Submit task
	if err := s.scheduler.SubmitTask(task); err != nil {
		logger.Error("Failed to submit task", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"task_id": task.ID,
		"status":  task.Status,
		"message": "Task submitted successfully",
	})
}

func (s *Server) getTask(c *gin.Context) {
	taskID := c.Param("id")

	task, err := s.scheduler.GetTask(taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	c.JSON(http.StatusOK, task)
}

func (s *Server) listTasks(c *gin.Context) {
	// Parse query parameters
	filter := &models.TaskFilter{
		Limit:  100, // Default limit
		Offset: 0,
	}

	if status := c.Query("status"); status != "" {
		filter.Status = []models.TaskStatus{models.TaskStatus(status)}
	}

	if taskType := c.Query("type"); taskType != "" {
		filter.Type = []models.TaskType{models.TaskType(taskType)}
	}

	if priority := c.Query("priority"); priority != "" {
		if p, err := strconv.Atoi(priority); err == nil {
			filter.Priority = &p
		}
	}

	if namespace := c.Query("namespace"); namespace != "" {
		filter.Namespace = namespace
	}

	if limit := c.Query("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			filter.Limit = l
		}
	}

	if offset := c.Query("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil {
			filter.Offset = o
		}
	}

	tasks, err := s.scheduler.ListTasks(filter)
	if err != nil {
		logger.Error("Failed to list tasks", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"tasks": tasks,
		"count": len(tasks),
	})
}

func (s *Server) cancelTask(c *gin.Context) {
	taskID := c.Param("id")

	if err := s.scheduler.CancelTask(taskID); err != nil {
		logger.Error("Failed to cancel task", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Task cancelled successfully",
	})
}

// Worker handlers

func (s *Server) listWorkers(c *gin.Context) {
	workers := s.scheduler.GetWorkers()

	c.JSON(http.StatusOK, gin.H{
		"workers": workers,
		"count":   len(workers),
	})
}

func (s *Server) getWorker(c *gin.Context) {
	workerID := c.Param("id")

	workers := s.scheduler.GetWorkers()
	for _, worker := range workers {
		if worker.ID == workerID {
			c.JSON(http.StatusOK, worker)
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "Worker not found"})
}

// Stats handlers

func (s *Server) getStats(c *gin.Context) {
	stats, err := s.scheduler.GetStats()
	if err != nil {
		logger.Error("Failed to get stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, stats)
}

func (s *Server) getClusterInfo(c *gin.Context) {
	isLeader := s.scheduler.IsLeader()

	c.JSON(http.StatusOK, gin.H{
		"node_id":   s.scheduler.nodeID,
		"is_leader": isLeader,
		"state":     "running",
	})
}

func (s *Server) serveDashboard(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(dashboardHTML))
}

// Middleware

func loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		duration := time.Since(start)
		status := c.Writer.Status()

		logger.Info("HTTP request",
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", status),
			zap.Duration("duration", duration),
		)
	}
}

func metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())

		metrics.HTTPRequestsTotal.WithLabelValues(method, path, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method, path).Observe(duration)
	}
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// Simple embedded dashboard HTML
const dashboardHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Distributed Job Scheduler - Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px; margin-bottom: 30px; }
        h1 { font-size: 32px; margin-bottom: 10px; }
        .subtitle { opacity: 0.9; font-size: 14px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card { background: #1e293b; border-radius: 10px; padding: 20px; border: 1px solid #334155; }
        .card-title { font-size: 14px; color: #94a3b8; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1px; }
        .card-value { font-size: 36px; font-weight: bold; color: #fff; }
        .card-subtitle { font-size: 12px; color: #64748b; margin-top: 5px; }
        table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 10px; overflow: hidden; }
        th { background: #334155; padding: 15px; text-align: left; font-weight: 600; color: #e2e8f0; }
        td { padding: 15px; border-top: 1px solid #334155; }
        .badge { display: inline-block; padding: 4px 12px; border-radius: 12px; font-size: 12px; font-weight: 600; }
        .badge-pending { background: #fef3c7; color: #92400e; }
        .badge-running { background: #dbeafe; color: #1e40af; }
        .badge-completed { background: #d1fae5; color: #065f46; }
        .badge-failed { background: #fee2e2; color: #991b1b; }
        .refresh-btn { background: #667eea; color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; font-size: 14px; margin-bottom: 20px; }
        .refresh-btn:hover { background: #5568d3; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🚀 Distributed Job Scheduler</h1>
            <p class="subtitle">Production-ready task distribution with leader election and fault tolerance</p>
        </header>

        <button class="refresh-btn" onclick="loadData()">🔄 Refresh Data</button>

        <div class="grid">
            <div class="card">
                <div class="card-title">Total Tasks</div>
                <div class="card-value" id="total-tasks">-</div>
                <div class="card-subtitle">All time</div>
            </div>
            <div class="card">
                <div class="card-title">Success Rate</div>
                <div class="card-value" id="success-rate">-</div>
                <div class="card-subtitle">Task completion rate</div>
            </div>
            <div class="card">
                <div class="card-title">Active Workers</div>
                <div class="card-value" id="active-workers">-</div>
                <div class="card-subtitle">Online and ready</div>
            </div>
            <div class="card">
                <div class="card-title">Avg Duration</div>
                <div class="card-value" id="avg-duration">-</div>
                <div class="card-subtitle">Task execution time</div>
            </div>
        </div>

        <div class="card" style="margin-bottom: 20px;">
            <h2 style="margin-bottom: 20px;">Recent Tasks</h2>
            <table id="tasks-table">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Name</th>
                        <th>Type</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Created</th>
                    </tr>
                </thead>
                <tbody></tbody>
            </table>
        </div>

        <div class="card">
            <h2 style="margin-bottom: 20px;">Workers</h2>
            <table id="workers-table">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Status</th>
                        <th>Tasks</th>
                        <th>Success Rate</th>
                        <th>Health Score</th>
                    </tr>
                </thead>
                <tbody></tbody>
            </table>
        </div>
    </div>

    <script>
        async function loadData() {
            try {
                const [stats, tasks, workers] = await Promise.all([
                    fetch('/api/v1/stats').then(r => r.json()),
                    fetch('/api/v1/tasks?limit=10').then(r => r.json()),
                    fetch('/api/v1/workers').then(r => r.json())
                ]);

                document.getElementById('total-tasks').textContent = stats.total || 0;
                document.getElementById('success-rate').textContent = (stats.success_rate || 0).toFixed(1) + '%';
                document.getElementById('active-workers').textContent = workers.count || 0;
                document.getElementById('avg-duration').textContent = formatDuration(stats.avg_duration);

                const tasksBody = document.querySelector('#tasks-table tbody');
                tasksBody.innerHTML = (tasks.tasks || []).map(task => `
                    <tr>
                        <td>${task.id.substring(0, 8)}...</td>
                        <td>${task.name}</td>
                        <td>${task.type}</td>
                        <td>${task.priority}</td>
                        <td><span class="badge badge-${task.status}">${task.status}</span></td>
                        <td>${new Date(task.created_at).toLocaleString()}</td>
                    </tr>
                `).join('');

                const workersBody = document.querySelector('#workers-table tbody');
                workersBody.innerHTML = (workers.workers || []).map(worker => `
                    <tr>
                        <td>${worker.id}</td>
                        <td><span class="badge badge-${worker.status === 'idle' ? 'completed' : 'running'}">${worker.status}</span></td>
                        <td>${worker.current_tasks}/${worker.max_concurrency}</td>
                        <td>${((worker.successful_tasks / worker.total_tasks) * 100 || 0).toFixed(1)}%</td>
                        <td>${worker.health_score.toFixed(1)}</td>
                    </tr>
                `).join('');
            } catch (error) {
                console.error('Failed to load data:', error);
            }
        }

        function formatDuration(ns) {
            if (!ns) return '0s';
            const seconds = ns / 1000000000;
            if (seconds < 60) return seconds.toFixed(1) + 's';
            return (seconds / 60).toFixed(1) + 'm';
        }

        loadData();
        setInterval(loadData, 5000);
    </script>
</body>
</html>
`
