package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	defaultDatabaseWriteQueueSize         = 1024
	defaultDatabaseWriteMaxRetries        = 3
	defaultDatabaseHealthFailureThreshold = 5
	defaultDatabaseWriteRetryBackoff      = 100 * time.Millisecond
)

var errPersistenceQueueClosed = errors.New("persistence queue closed")

type persistenceJob struct {
	operation string
	fn        func(context.Context) error
}

type persistenceQueue struct {
	jobs         chan persistenceJob
	writeTimeout time.Duration
	maxRetries   int
	retryBackoff time.Duration
	logger       *slog.Logger
	ctx          context.Context
	cancel       context.CancelFunc

	mu                  sync.Mutex
	closed              bool
	writeFailures       uint64
	droppedWrites       uint64
	consecutiveFailures uint64
	lastError           string

	done chan struct{}
}

type persistenceQueueStats struct {
	QueueDepth          int    `json:"queue_depth"`
	QueueCapacity       int    `json:"queue_capacity"`
	WriteFailures       uint64 `json:"write_failures"`
	DroppedWrites       uint64 `json:"dropped_writes"`
	ConsecutiveFailures uint64 `json:"consecutive_failures"`
	LastWriteError      string `json:"last_write_error,omitempty"`
}

func newPersistenceQueue(size int, timeout time.Duration, maxRetries int, logger *slog.Logger) *persistenceQueue {
	if size <= 0 {
		size = defaultDatabaseWriteQueueSize
	}
	if maxRetries <= 0 {
		maxRetries = defaultDatabaseWriteMaxRetries
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &persistenceQueue{
		jobs:         make(chan persistenceJob, size),
		writeTimeout: databaseWriteTimeout(timeout),
		maxRetries:   maxRetries,
		retryBackoff: defaultDatabaseWriteRetryBackoff,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
	}
	go q.run()
	return q
}

func (q *persistenceQueue) Enqueue(operation string, fn func(context.Context) error) bool {
	if q == nil || fn == nil {
		return false
	}

	q.mu.Lock()
	if q.closed {
		q.recordDropLocked(operation, errPersistenceQueueClosed)
		q.mu.Unlock()
		return false
	}

	select {
	case q.jobs <- persistenceJob{operation: operation, fn: fn}:
		q.mu.Unlock()
		return true
	default:
		q.recordDropLocked(operation, errors.New("persistence queue full"))
		q.mu.Unlock()
		if q.logger != nil {
			q.logger.Warn("persistent storage write dropped", "operation", operation, "reason", "queue_full")
		}
		return false
	}
}

func (q *persistenceQueue) Close() {
	if q == nil {
		return
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		<-q.done
		return
	}
	q.closed = true
	q.cancel()
	q.mu.Unlock()
	<-q.done
}

func (q *persistenceQueue) Stats() persistenceQueueStats {
	if q == nil {
		return persistenceQueueStats{}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return persistenceQueueStats{
		QueueDepth:          len(q.jobs),
		QueueCapacity:       cap(q.jobs),
		WriteFailures:       q.writeFailures,
		DroppedWrites:       q.droppedWrites,
		ConsecutiveFailures: q.consecutiveFailures,
		LastWriteError:      q.lastError,
	}
}

func (q *persistenceQueue) run() {
	defer close(q.done)
	for {
		select {
		case <-q.ctx.Done():
			return
		case job := <-q.jobs:
			q.runJob(job)
		}
	}
}

func (q *persistenceQueue) runJob(job persistenceJob) {
	for attempt := 1; attempt <= q.maxRetries; attempt++ {
		if q.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(q.ctx, q.writeTimeout)
		err := job.fn(ctx)
		cancel()
		if err == nil {
			q.recordSuccess()
			return
		}

		q.recordFailure(job.operation, attempt, err)
		if q.logger != nil {
			q.logger.Warn("persistent storage write failed", "operation", job.operation, "attempt", attempt, "max_attempts", q.maxRetries, "error", err)
		}
		if attempt < q.maxRetries {
			select {
			case <-q.ctx.Done():
				return
			case <-time.After(q.retryBackoff):
			}
		}
	}
}

func (q *persistenceQueue) recordSuccess() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.consecutiveFailures = 0
}

func (q *persistenceQueue) recordFailure(operation string, _ int, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.writeFailures++
	q.consecutiveFailures++
	q.lastError = persistenceQueueError(operation, err)
}

func (q *persistenceQueue) recordDropLocked(operation string, err error) {
	q.droppedWrites++
	q.lastError = persistenceQueueError(operation, err)
}

func persistenceQueueError(operation string, err error) string {
	if operation == "" {
		return err.Error()
	}
	return fmt.Sprintf("%s: %v", operation, err)
}
