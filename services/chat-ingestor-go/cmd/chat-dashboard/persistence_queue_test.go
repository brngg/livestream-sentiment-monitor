package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func TestPersistenceQueueAcceptsWriteAndRetriesFailures(t *testing.T) {
	queue := newTestPersistenceQueue(t, 4, 3)

	var attempts atomic.Int32
	done := make(chan struct{})
	if !queue.Enqueue("save_chat_bucket", func(context.Context) error {
		attempt := attempts.Add(1)
		if attempt < 3 {
			return errors.New("temporary write failure")
		}
		close(done)
		return nil
	}) {
		t.Fatal("queue did not accept write")
	}

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for queued write retry")
	}

	stats := waitForPersistenceStats(t, queue, func(stats persistenceQueueStats) bool {
		return stats.ConsecutiveFailures == 0 && stats.WriteFailures == 2
	})
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if stats.WriteFailures != 2 {
		t.Fatalf("write failures = %d, want 2", stats.WriteFailures)
	}
	if stats.DroppedWrites != 0 {
		t.Fatalf("dropped writes = %d, want 0", stats.DroppedWrites)
	}
	if stats.ConsecutiveFailures != 0 {
		t.Fatalf("consecutive failures = %d, want reset after success", stats.ConsecutiveFailures)
	}
	if !strings.Contains(stats.LastWriteError, "save_chat_bucket") || !strings.Contains(stats.LastWriteError, "temporary write failure") {
		t.Fatalf("last error did not include operation and error: %q", stats.LastWriteError)
	}
}

func TestPersistenceQueueDropsWhenBoundedQueueIsFull(t *testing.T) {
	queue := newPersistenceQueue(1, time.Second, 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	queue.retryBackoff = time.Millisecond

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAndClose := func() {
		releaseOnce.Do(func() {
			close(release)
		})
		queue.Close()
	}
	t.Cleanup(releaseAndClose)

	if !queue.Enqueue("blocking_write", func(context.Context) error {
		close(started)
		<-release
		return nil
	}) {
		t.Fatal("queue did not accept blocking write")
	}
	select {
	case <-started:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for blocking write to start")
	}

	if !queue.Enqueue("queued_write", func(context.Context) error { return nil }) {
		t.Fatal("queue did not accept write while one slot was available")
	}
	if queue.Enqueue("dropped_write", func(context.Context) error { return nil }) {
		t.Fatal("queue accepted write after bounded queue was full")
	}

	stats := queue.Stats()
	if stats.QueueDepth != 1 {
		t.Fatalf("queue depth = %d, want 1", stats.QueueDepth)
	}
	if stats.DroppedWrites != 1 {
		t.Fatalf("dropped writes = %d, want 1", stats.DroppedWrites)
	}
	if !strings.Contains(stats.LastWriteError, "dropped_write") || !strings.Contains(stats.LastWriteError, "queue full") {
		t.Fatalf("last error did not report dropped write: %q", stats.LastWriteError)
	}
}

func TestHealthDegradesAfterRepeatedPersistenceFailures(t *testing.T) {
	queue := newTestPersistenceQueue(t, 4, 3)
	server := &server{
		cfg: appConfig{
			DatabaseWriteEnabled:           true,
			DatabaseURL:                    "postgres://example",
			DatabaseWriteTimeout:           time.Second,
			DatabaseHealthFailureThreshold: 3,
		},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:       storage.NewNoopStore(),
		persistence: queue,
		humanLabels: map[string]string{},
	}

	if !queue.Enqueue("save_chat_bucket", func(context.Context) error {
		return errors.New("database unavailable")
	}) {
		t.Fatal("queue did not accept failing write")
	}
	waitForPersistenceStats(t, queue, func(stats persistenceQueueStats) bool {
		return stats.ConsecutiveFailures >= 3
	})

	recorder := httptest.NewRecorder()
	server.handleHealth(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("health status code = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	var body struct {
		Status      string                `json:"status"`
		Storage     string                `json:"storage"`
		Persistence persistenceQueueStats `json:"persistence"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if body.Status != "degraded" {
		t.Fatalf("health status = %q, want degraded", body.Status)
	}
	if body.Persistence.WriteFailures != 3 || body.Persistence.ConsecutiveFailures != 3 {
		t.Fatalf("unexpected persistence stats: %#v", body.Persistence)
	}
	if !strings.Contains(body.Persistence.LastWriteError, "database unavailable") {
		t.Fatalf("last error = %q, want database failure", body.Persistence.LastWriteError)
	}
}

func TestInitStorageReturnsNoopWhenPostgresUnavailable(t *testing.T) {
	cfg := appConfig{
		DatabaseWriteEnabled: true,
		DatabaseURL:          "://not-a-valid-postgres-url",
	}

	store := initStorage(context.Background(), &cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer store.Close()

	if _, ok := store.(storage.NoopStore); !ok {
		t.Fatalf("store type = %T, want NoopStore fallback", store)
	}
}

func TestInitStorageUsesReplayFixtureForOfflineHistory(t *testing.T) {
	start := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	fixturePath := filepath.Join(t.TempDir(), "sessions.json")
	data, err := json.Marshal([]storage.SessionReplay{
		{
			Session: storage.SessionHistory{
				SessionID: "fixture-session",
				ChannelID: "fixture-channel",
				Status:    "completed",
				StartedAt: start,
			},
			LabelCount: 1,
			WindowLabels: []storage.SignalWindowLabel{
				{
					SessionID:   "fixture-session",
					WindowStart: start,
					WindowEnd:   start.Add(5 * time.Second),
					EventLabel:  "hype_spike",
					Correctness: "correct",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if err := os.WriteFile(fixturePath, data, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	store := initStorage(context.Background(), &appConfig{ReplayFixturePath: fixturePath}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer store.Close()

	sessions, err := store.ListSessions(context.Background(), 10)
	if err != nil {
		t.Fatalf("list fixture sessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].SessionID != "fixture-session" {
		t.Fatalf("unexpected fixture sessions: %#v", sessions)
	}
	replay, err := store.GetSessionReplay(context.Background(), "fixture-session", 500)
	if err != nil {
		t.Fatalf("get fixture replay: %v", err)
	}
	if replay.LabelCount != 1 || replay.Session.LabelCount != 1 {
		t.Fatalf("fixture labels were not normalized: %#v", replay)
	}
}

func TestPersistenceQueueCloseCancelsBacklog(t *testing.T) {
	queue := newPersistenceQueue(8, time.Second, 3, slog.New(slog.NewTextHandler(io.Discard, nil)))
	queue.retryBackoff = time.Millisecond

	started := make(chan struct{})
	released := make(chan struct{})
	if !queue.Enqueue("blocking_write", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(released)
		return ctx.Err()
	}) {
		t.Fatal("queue did not accept blocking write")
	}
	for i := 0; i < 5; i++ {
		if !queue.Enqueue("queued_write", func(context.Context) error {
			t.Fatal("queued backlog should not run after close")
			return nil
		}) {
			t.Fatal("queue did not accept queued backlog")
		}
	}
	select {
	case <-started:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for blocking write")
	}

	done := make(chan struct{})
	go func() {
		queue.Close()
		close(done)
	}()

	select {
	case <-released:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("close did not cancel active write context")
	}
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("close drained queued backlog instead of stopping")
	}
}

func TestTranscriptBackpressureEventPersistsMetric(t *testing.T) {
	store := &metricCaptureStore{}
	server := &server{
		cfg:         appConfig{DatabaseWriteTimeout: time.Second},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:       store,
		persistence: newTestPersistenceQueue(t, 4, 1),
	}

	server.recordTranscriptStreamMetric(map[string]any{
		"status":          "backpressure",
		"asr_latency_ms":  1250,
		"asr_interval_ms": 1000,
	}, "session", "channel")

	metric := waitForMetric(t, store, "asr.backpressure_count")
	if metric.SessionID != "session" || metric.Value != 1 || metric.Unit != "events" {
		t.Fatalf("unexpected ASR backpressure metric: %#v", metric)
	}
	if server.metrics.asrBackpressure.Load() != 1 {
		t.Fatalf("asr backpressure counter = %d, want 1", server.metrics.asrBackpressure.Load())
	}
	if metric.Meta["channel_id"] != "channel" || metric.Meta["asr_latency_ms"] == nil {
		t.Fatalf("metric meta missing context: %#v", metric.Meta)
	}
}

func TestHealthReportsAnalyzerUnavailable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "model unavailable", http.StatusServiceUnavailable)
	}))
	defer upstream.Close()

	server := &server{
		cfg:   appConfig{NLPAnalyzerURL: upstream.URL},
		store: storage.NewNoopStore(),
	}
	recorder := httptest.NewRecorder()
	server.handleHealth(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("health status code = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	var body struct {
		Status string         `json:"status"`
		NLP    nlpHealthState `json:"nlp"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if body.Status != "degraded" || body.NLP.Status != "unavailable" {
		t.Fatalf("unexpected health body: %#v", body)
	}
}

func TestHealthDoesNotPersistSystemMetrics(t *testing.T) {
	store := &metricCaptureStore{}
	server := &server{
		cfg: appConfig{
			DatabaseWriteEnabled: true,
			DatabaseURL:          "postgres://example",
			NLPAnalyzerURL:       "",
		},
		store: store,
	}
	server.metrics.modelFallbacks.Store(2)
	server.metrics.modelSlow.Store(1)
	server.metrics.asrBackpressure.Store(3)
	server.metrics.reactionWindows.Store(4)

	recorder := httptest.NewRecorder()
	server.handleHealth(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("health status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	var body struct {
		Status  string         `json:"status"`
		Storage string         `json:"storage"`
		Metrics map[string]int `json:"metrics"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if body.Status != "ok" || body.Storage != "enabled" {
		t.Fatalf("unexpected health body: %#v", body)
	}
	if body.Metrics["model_fallback_count"] != 2 || body.Metrics["model_slow_count"] != 1 ||
		body.Metrics["asr_backpressure_count"] != 3 || body.Metrics["reaction_window_count"] != 4 {
		t.Fatalf("unexpected health metrics: %#v", body.Metrics)
	}
	if server.persistence != nil {
		t.Fatal("health initialized persistence queue; health checks must be read-only")
	}
	if count := store.metricCount(); count != 0 {
		t.Fatalf("health persisted %d metrics, want 0", count)
	}
}

func TestSystemMetricsSnapshotPersistsStorageAndPipelineCounters(t *testing.T) {
	store := &metricCaptureStore{}
	queue := newTestPersistenceQueue(t, 8, 1)
	queue.recordFailure("upsert_chat_bucket", 1, errors.New("temporary outage"))
	queue.mu.Lock()
	queue.recordDropLocked("queued_write", errors.New("persistence queue full"))
	queue.mu.Unlock()
	server := &server{
		cfg:         appConfig{DatabaseWriteTimeout: time.Second},
		store:       store,
		persistence: queue,
	}
	server.state.Session = "session"
	server.state.Channel = "channel"
	server.metrics.modelFallbacks.Store(2)
	server.metrics.modelSlow.Store(1)
	server.metrics.asrBackpressure.Store(3)
	server.metrics.reactionWindows.Store(4)

	server.persistSystemMetricsSnapshot("test")

	waitForMetric(t, store, "reaction_window.count")
	names := store.metricNames()
	for _, name := range []string{
		"storage.write_failures",
		"storage.dropped_writes",
		"storage.queue_depth",
		"model.fallback_count",
		"model.slow_count",
		"asr.backpressure_count",
		"reaction_window.count",
	} {
		if !names[name] {
			t.Fatalf("metric %q was not persisted; names=%v", name, names)
		}
	}
}

type metricCaptureStore struct {
	storage.NoopStore
	mu      sync.Mutex
	metrics []storage.SystemMetric
}

func (s *metricCaptureStore) SaveMetric(_ context.Context, metric storage.SystemMetric) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = append(s.metrics, metric)
	return nil
}

func (s *metricCaptureStore) metricNames() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := map[string]bool{}
	for _, metric := range s.metrics {
		names[metric.Name] = true
	}
	return names
}

func (s *metricCaptureStore) metricCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.metrics)
}

func waitForMetric(t *testing.T, store *metricCaptureStore, name string) storage.SystemMetric {
	t.Helper()
	deadline := time.After(250 * time.Millisecond)
	for {
		store.mu.Lock()
		for _, metric := range store.metrics {
			if metric.Name == name {
				store.mu.Unlock()
				return metric
			}
		}
		count := len(store.metrics)
		store.mu.Unlock()
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for metric %q, captured %d metrics", name, count)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func newTestPersistenceQueue(t *testing.T, size, maxRetries int) *persistenceQueue {
	t.Helper()
	queue := newPersistenceQueue(size, time.Second, maxRetries, slog.New(slog.NewTextHandler(io.Discard, nil)))
	queue.retryBackoff = time.Millisecond
	t.Cleanup(queue.Close)
	return queue
}

func waitForPersistenceStats(t *testing.T, queue *persistenceQueue, match func(persistenceQueueStats) bool) persistenceQueueStats {
	t.Helper()
	deadline := time.After(250 * time.Millisecond)
	for {
		stats := queue.Stats()
		if match(stats) {
			return stats
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for persistence stats, last stats: %#v", stats)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
