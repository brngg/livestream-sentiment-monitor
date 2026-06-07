package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/analysis"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/events"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func TestTranscriptOnlyAnalysisResult(t *testing.T) {
	app := testServer(nil, "")
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	bucket := analysis.TranscriptBucket{
		Type:                 "transcript_bucket",
		SessionID:            "session-transcript",
		ChannelID:            "channel",
		BucketStart:          start,
		BucketEnd:            start.Add(30 * time.Second),
		Text:                 "",
		TranscriptConfidence: 0.1,
	}

	response := postEvent(t, app, bucket)
	if response.SessionID != "session-transcript" {
		t.Fatalf("expected session id, got %#v", response)
	}
	if response.ChatBucketCount != 0 || response.TranscriptBucketCount != 1 {
		t.Fatalf("expected transcript-only state, got %#v", response)
	}
	if response.AlignmentCount != 0 || response.SignalWindowCount != 0 {
		t.Fatalf("expected no alignment windows without chat, got %#v", response)
	}
	if response.InsightSummary.TranscriptBucketCount != 1 || response.InsightSummary.PrimaryInsightKind != analysis.InsightTranscriptGap {
		t.Fatalf("expected transcript gap insight summary, got %#v", response.InsightSummary)
	}

	req := httptest.NewRequest(http.MethodGet, "/sessions/session-transcript/analysis", nil)
	rr := httptest.NewRecorder()
	app.routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected analysis lookup status 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var lookup sessionAnalysisResponse
	if err := json.NewDecoder(rr.Body).Decode(&lookup); err != nil {
		t.Fatal(err)
	}
	if lookup.TranscriptBucketCount != 1 || lookup.InsightCount == 0 {
		t.Fatalf("expected stored transcript analysis, got %#v", lookup)
	}
}

func TestChatTranscriptAlignmentPersistsAndEmitsSSE(t *testing.T) {
	store := &fakeAlignmentStore{}
	app := testServer(store, "postgres://test")
	httpServer := httptest.NewServer(app.routes())
	defer httpServer.Close()

	eventResponse, closeEvents := openEvents(t, httpServer.URL+"/events?session_id=session-align")
	defer closeEvents()

	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	postEventURL(t, httpServer.URL+"/events", chat.ChatBucket{
		Type:                "chat_bucket",
		SessionID:           "session-align",
		ChannelID:           "channel",
		BucketStart:         start,
		BucketEnd:           start.Add(30 * time.Second),
		MessageCount:        25,
		UniqueChatters:      20,
		ChatSentiment:       0.7,
		SentimentConfidence: 0.8,
		PositiveRatio:       0.75,
		NeutralRatio:        0.15,
		NegativeRatio:       0.10,
	})

	score := -0.2
	result := postEventURL(t, httpServer.URL+"/events", map[string]any{
		"event_type": "transcript_bucket",
		"payload": analysis.TranscriptBucket{
			SessionID:            "session-align",
			ChannelID:            "channel",
			BucketStart:          start,
			BucketEnd:            start.Add(30 * time.Second),
			Text:                 "the streamer is calm while chat gets very excited about the reveal",
			TranscriptConfidence: 0.9,
			SentimentScore:       &score,
		},
	})

	if result.AlignmentCount != 1 {
		t.Fatalf("expected one alignment, got %#v", result)
	}
	if result.Alignments[0].Relationship != "diverged" {
		t.Fatalf("expected divergent chat/transcript alignment, got %#v", result.Alignments[0])
	}
	if result.SignalWindowCount != 1 || len(result.SignalEvents) == 0 {
		t.Fatalf("expected aligned signal window and events, got %#v", result)
	}

	store.mu.Lock()
	persisted := append([]storage.AlignmentBucket(nil), store.alignments...)
	store.mu.Unlock()
	if len(persisted) != 1 {
		t.Fatalf("expected one persisted alignment, got %#v", persisted)
	}
	if persisted[0].SessionID != "session-align" || persisted[0].Relationship != "diverged" {
		t.Fatalf("unexpected persisted alignment: %#v", persisted[0])
	}

	event := readAnalysisEvent(t, eventResponse, func(event analysisSSEEvent) bool {
		return event.SessionID == "session-align" && event.Result.AlignmentCount == 1
	})
	if event.Type != "analysis_result" || event.SessionID != "session-align" || event.Result.AlignmentCount != 1 {
		t.Fatalf("expected aligned analysis_result SSE event, got %#v", event)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	app := testServer(nil, "")
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	postEvent(t, app, chat.ChatBucket{
		Type:          "chat_bucket",
		SessionID:     "session-metrics",
		ChannelID:     "channel",
		BucketStart:   start,
		BucketEnd:     start.Add(30 * time.Second),
		MessageCount:  4,
		ChatSentiment: 0.1,
	})

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthRR := httptest.NewRecorder()
	app.routes().ServeHTTP(healthRR, healthReq)
	if healthRR.Code != http.StatusOK {
		t.Fatalf("expected health status 200, got %d: %s", healthRR.Code, healthRR.Body.String())
	}
	var health map[string]any
	if err := json.NewDecoder(healthRR.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if health["status"] != "ok" || health["storage"] != "disabled" {
		t.Fatalf("unexpected health payload: %#v", health)
	}
	if health["sessions"].(float64) != 1 {
		t.Fatalf("expected one session in health payload, got %#v", health)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRR := httptest.NewRecorder()
	app.routes().ServeHTTP(metricsRR, metricsReq)
	if metricsRR.Code != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d", metricsRR.Code)
	}
	metrics := metricsRR.Body.String()
	for _, expected := range []string{
		"analysis_service_events_received_total 1",
		"analysis_service_analysis_runs_total 1",
		"analysis_service_sessions 1",
		"analysis_service_persistence_enabled 0",
	} {
		if !strings.Contains(metrics, expected) {
			t.Fatalf("metrics missing %q in:\n%s", expected, metrics)
		}
	}
}

func TestPublishesAnalysisResultEvents(t *testing.T) {
	publisher := &fakeResultPublisher{}
	app := testServer(nil, "")
	app.resultPublisher = publisher

	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	postEvent(t, app, chat.ChatBucket{
		Type:                "chat_bucket",
		SessionID:           "session-publish",
		ChannelID:           "channel",
		BucketStart:         start,
		BucketEnd:           start.Add(30 * time.Second),
		MessageCount:        10,
		ChatSentiment:       0.5,
		SentimentConfidence: 0.7,
	})

	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if len(publisher.messages) != 1 {
		t.Fatalf("published analysis results = %d, want 1", len(publisher.messages))
	}
	msg := publisher.messages[0]
	if msg.Topic != events.TopicAnalysisResults {
		t.Fatalf("topic = %q, want %q", msg.Topic, events.TopicAnalysisResults)
	}
	if msg.Headers["event_type"] != events.EventTypeAnalysisResult {
		t.Fatalf("event_type header = %q, want %q", msg.Headers["event_type"], events.EventTypeAnalysisResult)
	}
	if app.metrics.publishedResults.Load() != 1 {
		t.Fatalf("published result metric = %d, want 1", app.metrics.publishedResults.Load())
	}
}

func testServer(store alignmentStore, databaseURL string) *server {
	return newServer(appConfig{
		AlignmentWindow:      30 * time.Second,
		StorageTimeout:       time.Second,
		MaxBucketsPerSession: 20,
		DatabaseURL:          databaseURL,
	}, store, storageStatus(databaseURL), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func storageStatus(databaseURL string) string {
	if strings.TrimSpace(databaseURL) == "" {
		return "disabled"
	}
	return "enabled"
}

func postEvent(t *testing.T, app *server, value any) sessionAnalysisResponse {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	app.routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected POST /events status 202, got %d: %s", rr.Code, rr.Body.String())
	}
	var response sessionAnalysisResponse
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response
}

func postEventURL(t *testing.T, url string, value any) sessionAnalysisResponse {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected POST /events status 202, got %d: %s", resp.StatusCode, string(raw))
	}
	var response sessionAnalysisResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response
}

func openEvents(t *testing.T, url string) (*http.Response, func()) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("expected event stream status 200, got %d", resp.StatusCode)
	}
	return resp, func() { resp.Body.Close() }
}

func readAnalysisEvent(t *testing.T, resp *http.Response, accept func(analysisSSEEvent) bool) analysisSSEEvent {
	t.Helper()
	scanner := bufio.NewScanner(resp.Body)
	deadline := time.After(2 * time.Second)
	lines := make(chan string, 16)
	go func() {
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for analysis_result SSE event")
		case line, ok := <-lines:
			if !ok {
				if err := scanner.Err(); err != nil {
					t.Fatal(err)
				}
				t.Fatal("event stream closed before analysis_result")
			}
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var event analysisSSEEvent
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
				t.Fatal(err)
			}
			if event.Type == "analysis_result" && (accept == nil || accept(event)) {
				return event
			}
		}
	}
}

type fakeAlignmentStore struct {
	mu         sync.Mutex
	alignments []storage.AlignmentBucket
}

func (s *fakeAlignmentStore) SaveAlignment(ctx context.Context, item storage.AlignmentBucket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alignments = append(s.alignments, item)
	return nil
}

type fakeResultPublisher struct {
	mu       sync.Mutex
	messages []events.PublishMessage
}

func (p *fakeResultPublisher) Publish(_ context.Context, msg events.PublishMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, msg)
	return nil
}
