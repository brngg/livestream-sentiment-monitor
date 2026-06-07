package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEnvelopeValidationAndRouting(t *testing.T) {
	envelope := validEnvelope(t, EventTypeTranscriptBucket, map[string]any{
		"type":         "transcript_bucket",
		"session_id":   "session-1",
		"channel_id":   "streamer",
		"bucket_start": "2026-05-11T12:00:00Z",
		"bucket_end":   "2026-05-11T12:00:30Z",
		"text":         "that fight went badly",
	})

	if err := envelope.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	topic, err := envelope.Topic()
	if err != nil {
		t.Fatalf("Topic() error = %v", err)
	}
	if topic != TopicTranscriptBucketsV1 {
		t.Fatalf("topic = %q, want %q", topic, TopicTranscriptBucketsV1)
	}
}

func TestEnvelopeValidationRejectsUnknownEventType(t *testing.T) {
	envelope := validEnvelope(t, "raw_chat_message", map[string]any{
		"session_id": "session-1",
		"channel_id": "streamer",
	})

	err := envelope.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want unknown type error")
	}
	if !strings.Contains(err.Error(), ErrUnknownEventType.Error()) {
		t.Fatalf("Validate() error = %v, want unknown type", err)
	}
}

func TestEnvelopeValidationRejectsSchemaVersion(t *testing.T) {
	envelope := validEnvelope(t, EventTypeSystemMetric, map[string]any{
		"name":  "publish_latency_ms",
		"value": 42,
		"unit":  "ms",
	})
	envelope.EventSchemaVersion = "v2"

	err := envelope.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want schema version error")
	}
	if !strings.Contains(err.Error(), "event_schema_version") {
		t.Fatalf("Validate() error = %v, want event_schema_version error", err)
	}
}

func TestDeterministicBucketEventKey(t *testing.T) {
	envelope := validEnvelope(t, EventTypeChatBucket, map[string]any{
		"type":         "chat_bucket",
		"bucket_start": "2026-05-11T08:00:00-04:00",
		"bucket_end":   "2026-05-11T08:00:30-04:00",
	})
	envelope.SessionID = " session-1 "
	envelope.ChannelID = " streamer "

	key := envelope.EventKey()
	want := "chat_bucket:session-1:streamer:2026-05-11T12:00:00Z:2026-05-11T12:00:30Z"
	if key != want {
		t.Fatalf("EventKey() = %q, want %q", key, want)
	}

	start := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	end := start.Add(30 * time.Second)
	if got := DeterministicBucketEventKey(EventTypeChatBucket, "session-1", "streamer", start, end); got != want {
		t.Fatalf("DeterministicBucketEventKey() = %q, want %q", got, want)
	}
}

func validEnvelope(t *testing.T, eventType string, payload map[string]any) Envelope {
	t.Helper()
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return Envelope{
		SchemaVersion:      EnvelopeSchemaVersion,
		EventID:            "event-1",
		EventType:          eventType,
		EventSchemaVersion: EventSchemaVersionV1,
		Source:             "test",
		SessionID:          "session-1",
		ChannelID:          "streamer",
		OccurredAt:         time.Date(2026, 5, 11, 12, 0, 1, 0, time.UTC),
		Payload:            rawPayload,
	}
}
