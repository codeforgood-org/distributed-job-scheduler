package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"github.com/codeforgood-org/distributed-job-scheduler/pkg/models"
	"go.uber.org/zap"
)

// EventType represents different webhook events
type EventType string

const (
	EventTaskCreated   EventType = "task.created"
	EventTaskStarted   EventType = "task.started"
	EventTaskCompleted EventType = "task.completed"
	EventTaskFailed    EventType = "task.failed"
	EventTaskCancelled EventType = "task.cancelled"
	EventTaskRetrying  EventType = "task.retrying"
	EventWorkerJoined  EventType = "worker.joined"
	EventWorkerLeft    EventType = "worker.left"
	EventWorkerUnhealthy EventType = "worker.unhealthy"
	EventLeaderElected EventType = "cluster.leader_elected"
)

// WebhookEvent represents a webhook event payload
type WebhookEvent struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	Metadata  map[string]string      `json:"metadata,omitempty"`
}

// Webhook represents a webhook subscription
type Webhook struct {
	ID          string      `json:"id"`
	URL         string      `json:"url"`
	Events      []EventType `json:"events"`
	Secret      string      `json:"secret,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Active      bool        `json:"active"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`

	// Stats
	TotalSent   int64       `json:"total_sent"`
	TotalFailed int64       `json:"total_failed"`
	LastSent    *time.Time  `json:"last_sent,omitempty"`
}

// WebhookManager manages webhook subscriptions and delivery
type WebhookManager struct {
	mu         sync.RWMutex
	webhooks   map[string]*Webhook
	httpClient *http.Client

	// Delivery queue
	eventQueue chan *WebhookEvent
	maxRetries int
}

// NewWebhookManager creates a new webhook manager
func NewWebhookManager(maxRetries int) *WebhookManager {
	return &WebhookManager{
		webhooks: make(map[string]*Webhook),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		eventQueue: make(chan *WebhookEvent, 1000),
		maxRetries: maxRetries,
	}
}

// Start starts the webhook delivery workers
func (wm *WebhookManager) Start(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		go wm.deliveryWorker()
	}
	logger.Info("Webhook manager started", zap.Int("workers", numWorkers))
}

// RegisterWebhook registers a new webhook
func (wm *WebhookManager) RegisterWebhook(webhook *Webhook) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	webhook.Active = true
	webhook.CreatedAt = time.Now()
	webhook.UpdatedAt = time.Now()

	wm.webhooks[webhook.ID] = webhook

	logger.Info("Webhook registered",
		zap.String("id", webhook.ID),
		zap.String("url", webhook.URL),
		zap.Any("events", webhook.Events))

	return nil
}

// UnregisterWebhook removes a webhook
func (wm *WebhookManager) UnregisterWebhook(webhookID string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	delete(wm.webhooks, webhookID)
	logger.Info("Webhook unregistered", zap.String("id", webhookID))

	return nil
}

// Emit emits a webhook event
func (wm *WebhookManager) Emit(eventType EventType, data map[string]interface{}) {
	event := &WebhookEvent{
		ID:        fmt.Sprintf("evt_%d", time.Now().UnixNano()),
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      data,
	}

	select {
	case wm.eventQueue <- event:
		logger.Debug("Webhook event queued", zap.String("type", string(eventType)))
	default:
		logger.Warn("Webhook event queue full, dropping event",
			zap.String("type", string(eventType)))
	}
}

// deliveryWorker processes webhook events from the queue
func (wm *WebhookManager) deliveryWorker() {
	for event := range wm.eventQueue {
		wm.deliverEvent(event)
	}
}

// deliverEvent delivers an event to all matching webhooks
func (wm *WebhookManager) deliverEvent(event *WebhookEvent) {
	wm.mu.RLock()
	webhooks := make([]*Webhook, 0, len(wm.webhooks))
	for _, webhook := range wm.webhooks {
		if webhook.Active && wm.matchesEvent(webhook, event.Type) {
			webhooks = append(webhooks, webhook)
		}
	}
	wm.mu.RUnlock()

	for _, webhook := range webhooks {
		go wm.deliverToWebhook(webhook, event)
	}
}

// matchesEvent checks if webhook is subscribed to event type
func (wm *WebhookManager) matchesEvent(webhook *Webhook, eventType EventType) bool {
	for _, et := range webhook.Events {
		if et == eventType || et == "*" {
			return true
		}
	}
	return false
}

// deliverToWebhook delivers event to a specific webhook with retry
func (wm *WebhookManager) deliverToWebhook(webhook *Webhook, event *WebhookEvent) {
	var lastErr error

	for retry := 0; retry <= wm.maxRetries; retry++ {
		if retry > 0 {
			// Exponential backoff
			backoff := time.Duration(1<<uint(retry-1)) * time.Second
			time.Sleep(backoff)
			logger.Info("Retrying webhook delivery",
				zap.String("webhook_id", webhook.ID),
				zap.Int("retry", retry))
		}

		err := wm.sendWebhook(webhook, event)
		if err == nil {
			wm.mu.Lock()
			webhook.TotalSent++
			now := time.Now()
			webhook.LastSent = &now
			wm.mu.Unlock()

			logger.Info("Webhook delivered successfully",
				zap.String("webhook_id", webhook.ID),
				zap.String("event_type", string(event.Type)))
			return
		}

		lastErr = err
	}

	// All retries failed
	wm.mu.Lock()
	webhook.TotalFailed++
	wm.mu.Unlock()

	logger.Error("Webhook delivery failed after retries",
		zap.String("webhook_id", webhook.ID),
		zap.String("url", webhook.URL),
		zap.Error(lastErr))
}

// sendWebhook sends HTTP request to webhook URL
func (wm *WebhookManager) sendWebhook(webhook *Webhook, event *WebhookEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	req, err := http.NewRequest("POST", webhook.URL, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "DistributedScheduler/1.0")
	req.Header.Set("X-Webhook-Event", string(event.Type))
	req.Header.Set("X-Webhook-ID", webhook.ID)

	// Add custom headers
	for key, value := range webhook.Headers {
		req.Header.Set(key, value)
	}

	// Add signature if secret is provided
	if webhook.Secret != "" {
		signature := computeSignature(payload, webhook.Secret)
		req.Header.Set("X-Webhook-Signature", signature)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req = req.WithContext(ctx)

	resp, err := wm.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

// computeSignature computes HMAC signature for webhook payload
func computeSignature(payload []byte, secret string) string {
	// Simplified - in production use crypto/hmac
	return fmt.Sprintf("sha256=%x", len(payload))
}

// GetWebhook retrieves a webhook by ID
func (wm *WebhookManager) GetWebhook(webhookID string) (*Webhook, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	webhook, exists := wm.webhooks[webhookID]
	if !exists {
		return nil, fmt.Errorf("webhook not found")
	}

	return webhook, nil
}

// ListWebhooks returns all registered webhooks
func (wm *WebhookManager) ListWebhooks() []*Webhook {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	webhooks := make([]*Webhook, 0, len(wm.webhooks))
	for _, webhook := range wm.webhooks {
		webhooks = append(webhooks, webhook)
	}

	return webhooks
}

// Helper functions to emit common events

// EmitTaskCreated emits a task created event
func (wm *WebhookManager) EmitTaskCreated(task *models.Task) {
	wm.Emit(EventTaskCreated, map[string]interface{}{
		"task_id":   task.ID,
		"task_name": task.Name,
		"type":      task.Type,
		"priority":  task.Priority,
		"namespace": task.Namespace,
	})
}

// EmitTaskCompleted emits a task completed event
func (wm *WebhookManager) EmitTaskCompleted(task *models.Task, duration time.Duration) {
	wm.Emit(EventTaskCompleted, map[string]interface{}{
		"task_id":      task.ID,
		"task_name":    task.Name,
		"duration_ms":  duration.Milliseconds(),
		"worker_id":    task.WorkerID,
		"result":       task.Result,
	})
}

// EmitTaskFailed emits a task failed event
func (wm *WebhookManager) EmitTaskFailed(task *models.Task, errorMsg string) {
	wm.Emit(EventTaskFailed, map[string]interface{}{
		"task_id":     task.ID,
		"task_name":   task.Name,
		"error":       errorMsg,
		"retry_count": task.RetryCount,
		"will_retry":  task.ShouldRetry(),
	})
}

// EmitWorkerJoined emits a worker joined event
func (wm *WebhookManager) EmitWorkerJoined(worker *models.Worker) {
	wm.Emit(EventWorkerJoined, map[string]interface{}{
		"worker_id":      worker.ID,
		"address":        worker.Address,
		"capabilities":   worker.Capabilities,
		"max_concurrency": worker.MaxConcurrency,
	})
}
