package resilience

import (
	"errors"
	"sync"
	"time"

	"github.com/codeforgood-org/distributed-job-scheduler/pkg/logger"
	"go.uber.org/zap"
)

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrTooManyRequests = errors.New("too many requests")
)

// State represents circuit breaker state
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

// String returns string representation of state
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	mu sync.RWMutex

	name  string
	state State

	// Configuration
	maxRequests       uint32        // Max requests in half-open state
	interval          time.Duration // Rolling window for closed state
	timeout           time.Duration // Time before transitioning from open to half-open
	failureThreshold  uint32        // Failures before opening
	successThreshold  uint32        // Successes in half-open before closing

	// Counters
	counts            Counts
	stateChangedAt    time.Time
}

// Counts holds success/failure counters
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

// Config holds circuit breaker configuration
type Config struct {
	Name             string
	MaxRequests      uint32
	Interval         time.Duration
	Timeout          time.Duration
	FailureThreshold uint32
	SuccessThreshold uint32
}

// DefaultConfig returns default configuration
func DefaultConfig(name string) *Config {
	return &Config{
		Name:             name,
		MaxRequests:      1,
		Interval:         time.Minute,
		Timeout:          time.Minute,
		FailureThreshold: 5,
		SuccessThreshold: 2,
	}
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *Config) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:             config.Name,
		state:            StateClosed,
		maxRequests:      config.MaxRequests,
		interval:         config.Interval,
		timeout:          config.Timeout,
		failureThreshold: config.FailureThreshold,
		successThreshold: config.SuccessThreshold,
		stateChangedAt:   time.Now(),
	}

	return cb
}

// Execute runs the given function if circuit breaker allows it
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if err := cb.beforeRequest(); err != nil {
		return err
	}

	err := fn()

	cb.afterRequest(err == nil)

	return err
}

// beforeRequest checks if request is allowed
func (cb *CircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state := cb.currentState(now)

	if state == StateOpen {
		return ErrCircuitOpen
	}

	if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		return ErrTooManyRequests
	}

	cb.counts.Requests++
	return nil
}

// afterRequest records the result
func (cb *CircuitBreaker) afterRequest(success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state := cb.currentState(now)

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

// currentState returns current state
func (cb *CircuitBreaker) currentState(now time.Time) State {
	switch cb.state {
	case StateClosed:
		// Check if interval has passed
		if cb.interval > 0 && now.Sub(cb.stateChangedAt) > cb.interval {
			cb.resetCounts()
		}
	case StateOpen:
		// Check if timeout has passed
		if now.Sub(cb.stateChangedAt) > cb.timeout {
			cb.setState(StateHalfOpen, now)
		}
	}

	return cb.state
}

// onSuccess handles successful request
func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	cb.counts.TotalSuccesses++
	cb.counts.ConsecutiveSuccesses++
	cb.counts.ConsecutiveFailures = 0

	if state == StateHalfOpen {
		// Check if we should close the circuit
		if cb.counts.ConsecutiveSuccesses >= cb.successThreshold {
			cb.setState(StateClosed, now)
		}
	}
}

// onFailure handles failed request
func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	cb.counts.TotalFailures++
	cb.counts.ConsecutiveFailures++
	cb.counts.ConsecutiveSuccesses = 0

	if state == StateClosed {
		// Check if we should open the circuit
		if cb.counts.ConsecutiveFailures >= cb.failureThreshold {
			cb.setState(StateOpen, now)
		}
	} else if state == StateHalfOpen {
		// Any failure in half-open state opens the circuit
		cb.setState(StateOpen, now)
	}
}

// setState changes circuit breaker state
func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state
	cb.stateChangedAt = now
	cb.resetCounts()

	logger.Info("Circuit breaker state changed",
		zap.String("name", cb.name),
		zap.String("from", prev.String()),
		zap.String("to", state.String()))
}

// resetCounts resets all counters
func (cb *CircuitBreaker) resetCounts() {
	cb.counts = Counts{}
}

// GetState returns current state
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetCounts returns current counts
func (cb *CircuitBreaker) GetCounts() Counts {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.counts
}

// Reset resets circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = StateClosed
	cb.stateChangedAt = time.Now()
	cb.resetCounts()

	logger.Info("Circuit breaker reset", zap.String("name", cb.name))
}

// CircuitBreakerRegistry manages multiple circuit breakers
type CircuitBreakerRegistry struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
}

// NewCircuitBreakerRegistry creates a new registry
func NewCircuitBreakerRegistry() *CircuitBreakerRegistry {
	return &CircuitBreakerRegistry{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetOrCreate gets or creates a circuit breaker
func (r *CircuitBreakerRegistry) GetOrCreate(name string, config *Config) *CircuitBreaker {
	r.mu.RLock()
	cb, exists := r.breakers[name]
	r.mu.RUnlock()

	if exists {
		return cb
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check
	cb, exists = r.breakers[name]
	if exists {
		return cb
	}

	if config == nil {
		config = DefaultConfig(name)
	}

	cb = NewCircuitBreaker(config)
	r.breakers[name] = cb

	return cb
}

// Get retrieves a circuit breaker by name
func (r *CircuitBreakerRegistry) Get(name string) (*CircuitBreaker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cb, exists := r.breakers[name]
	return cb, exists
}

// List returns all circuit breakers
func (r *CircuitBreakerRegistry) List() map[string]*CircuitBreaker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]*CircuitBreaker)
	for name, cb := range r.breakers {
		result[name] = cb
	}

	return result
}
