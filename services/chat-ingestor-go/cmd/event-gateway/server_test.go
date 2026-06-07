package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/events"
)

type fakePublisher struct {
	err      error
	messages []events.PublishMessage
}

func (p *fakePublisher) Publish(_ context.Context, msg events.PublishMessage) error {
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, msg)
	return nil
}

func TestHandleEventsPublishesRoutedEnvelope(t *testing.T) {
	publisher := &fakePublisher{}
	server := newGatewayServer(publisher, nil, time.Second)
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(validGatewayEvent(t, events.EventTypeChatBucket)))
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(publisher.messages))
	}
	msg := publisher.messages[0]
	if msg.Topic != events.TopicChatBucketsV1 {
		t.Fatalf("topic = %q, want %q", msg.Topic, events.TopicChatBucketsV1)
	}
	wantKey := "chat_bucket:session-1:streamer:2026-05-11T12:00:00Z:2026-05-11T12:00:30Z"
	if msg.Key != wantKey {
		t.Fatalf("key = %q, want %q", msg.Key, wantKey)
	}
	if msg.Headers["event_type"] != events.EventTypeChatBucket {
		t.Fatalf("event_type header = %q", msg.Headers["event_type"])
	}
}

func TestHandleEventsRejectsInvalidEnvelopeAndCountsValidationFailure(t *testing.T) {
	publisher := &fakePublisher{}
	server := newGatewayServer(publisher, nil, time.Second)
	body := validGatewayEvent(t, events.EventTypeChatBucket)
	body = bytes.Replace(body, []byte(`"schema_version":"v1"`), []byte(`"schema_version":"v0"`), 1)
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("published messages = %d, want 0", len(publisher.messages))
	}
	if got := server.metrics.validationFailures.Load(); got != 1 {
		t.Fatalf("validation failures = %d, want 1", got)
	}
}

func TestMetricsExposePublishFailureCounter(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("kafka unavailable")}
	server := newGatewayServer(publisher, nil, time.Second)
	handler := server.routes()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(validGatewayEvent(t, events.EventTypeChatBucket)))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	handler.ServeHTTP(metricsRec, metricsReq)

	if metricsRec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", metricsRec.Code)
	}
	if !strings.Contains(metricsRec.Body.String(), "event_gateway_publish_failures_total 1") {
		t.Fatalf("metrics body missing publish failure counter: %s", metricsRec.Body.String())
	}
}

func validGatewayEvent(t *testing.T, eventType string) []byte {
	t.Helper()
	payload := map[string]any{
		"type":          eventType,
		"session_id":    "session-1",
		"channel_id":    "streamer",
		"bucket_start":  "2026-05-11T12:00:00Z",
		"bucket_end":    "2026-05-11T12:00:30Z",
		"message_count": 12,
	}
	if eventType == events.EventTypeAnalysisResult {
		payload["type"] = "analysis_bucket"
	}
	body := map[string]any{
		"schema_version":       events.EnvelopeSchemaVersion,
		"event_id":             "event-1",
		"event_type":           eventType,
		"event_schema_version": events.EventSchemaVersionV1,
		"source":               "gateway-test",
		"session_id":           "session-1",
		"channel_id":           "streamer",
		"occurred_at":          "2026-05-11T12:00:31Z",
		"payload":              payload,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return raw
}
