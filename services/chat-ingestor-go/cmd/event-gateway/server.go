package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/events"
)

const maxEventBodyBytes = 1 << 20

type gatewayServer struct {
	publisher      events.Publisher
	logger         *slog.Logger
	publishTimeout time.Duration
	metrics        gatewayMetrics
}

type gatewayMetrics struct {
	received           atomic.Uint64
	accepted           atomic.Uint64
	validationFailures atomic.Uint64
	publishFailures    atomic.Uint64
}

func newGatewayServer(publisher events.Publisher, logger *slog.Logger, publishTimeout time.Duration) *gatewayServer {
	if logger == nil {
		logger = slog.Default()
	}
	if publishTimeout <= 0 {
		publishTimeout = 5 * time.Second
	}
	return &gatewayServer{
		publisher:      publisher,
		logger:         logger,
		publishTimeout: publishTimeout,
	}
}

func (s *gatewayServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

func (s *gatewayServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.metrics.received.Add(1)

	defer r.Body.Close()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxEventBodyBytes))
	if err != nil {
		s.metrics.validationFailures.Add(1)
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error": "event body too large",
		})
		return
	}
	body = bytes.TrimSpace(body)
	envelope, err := events.DecodeEnvelope(body)
	if err != nil {
		s.metrics.validationFailures.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": err.Error(),
		})
		return
	}
	if err := envelope.Validate(); err != nil {
		s.metrics.validationFailures.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": err.Error(),
		})
		return
	}
	msg, err := envelope.PublishMessage(body)
	if err != nil {
		s.metrics.validationFailures.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": err.Error(),
		})
		return
	}

	publishCtx, cancel := context.WithTimeout(r.Context(), s.publishTimeout)
	defer cancel()
	if err := s.publisher.Publish(publishCtx, msg); err != nil {
		s.metrics.publishFailures.Add(1)
		s.logger.Warn("publish event", "event_id", envelope.EventID, "event_type", envelope.EventType, "topic", msg.Topic, "error", err)
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":      "publish failed",
			"event_id":   envelope.EventID,
			"event_type": envelope.EventType,
			"topic":      msg.Topic,
		})
		return
	}

	s.metrics.accepted.Add(1)
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":     "accepted",
		"event_id":   envelope.EventID,
		"event_type": envelope.EventType,
		"topic":      msg.Topic,
		"key":        msg.Key,
	})
}

func (s *gatewayServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "event-gateway",
		"time":    time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *gatewayServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w, "# TYPE event_gateway_events_received_total counter\n")
	_, _ = fmt.Fprintf(w, "event_gateway_events_received_total %d\n", s.metrics.received.Load())
	_, _ = fmt.Fprintf(w, "# TYPE event_gateway_events_accepted_total counter\n")
	_, _ = fmt.Fprintf(w, "event_gateway_events_accepted_total %d\n", s.metrics.accepted.Load())
	_, _ = fmt.Fprintf(w, "# TYPE event_gateway_validation_failures_total counter\n")
	_, _ = fmt.Fprintf(w, "event_gateway_validation_failures_total %d\n", s.metrics.validationFailures.Load())
	_, _ = fmt.Fprintf(w, "# TYPE event_gateway_publish_failures_total counter\n")
	_, _ = fmt.Fprintf(w, "event_gateway_publish_failures_total %d\n", s.metrics.publishFailures.Load())
}

func writeJSON(w http.ResponseWriter, status int, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		slog.Default().Warn("write response", "error", err)
	}
}
