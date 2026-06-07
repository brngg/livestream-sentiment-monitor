package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/events"
)

const eventPublishTimeout = 3 * time.Second

func (s *server) publishChatBucketEvent(bucket any) {
	s.publishBucketEvent(events.EventTypeChatBucket, "chat-dashboard", bucket)
}

func (s *server) publishTranscriptBucketEvent(bucket any) {
	s.publishBucketEvent(events.EventTypeTranscriptBucket, "chat-dashboard-transcript-proxy", bucket)
}

func (s *server) publishBucketEvent(eventType, source string, payload any) {
	if !s.cfg.EventBusEnabled || strings.TrimSpace(s.cfg.EventGatewayURL) == "" {
		return
	}
	envelope, err := events.NewEnvelope(eventType, source, payload)
	if err != nil {
		s.recordEventPublishFailure(eventType, "", err)
		return
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		s.recordEventPublishFailure(eventType, envelope.EventID, err)
		return
	}
	go s.postEventEnvelope(eventType, envelope.EventID, raw)
}

func (s *server) postEventEnvelope(eventType, eventID string, body []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), eventPublishTimeout)
	defer cancel()
	endpoint := strings.TrimRight(s.cfg.EventGatewayURL, "/") + "/events"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		s.recordEventPublishFailure(eventType, eventID, err)
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		s.recordEventPublishFailure(eventType, eventID, err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		s.recordEventPublishFailure(eventType, eventID, fmt.Errorf("event gateway returned %s", response.Status))
	}
}

func (s *server) recordEventPublishFailure(eventType, eventID string, err error) {
	count := s.metrics.eventPublishFailures.Add(1)
	if s.logger != nil {
		s.logger.Warn("publish durable event", "event_type", eventType, "event_id", eventID, "total_failures", count, "error", err)
	}
}
