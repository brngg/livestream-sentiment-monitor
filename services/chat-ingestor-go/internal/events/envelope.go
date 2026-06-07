package events

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

const (
	EnvelopeSchemaVersion = "v1"
	EventSchemaVersionV1  = "v1"

	EventTypeChatBucket       = "chat_bucket"
	EventTypeTranscriptBucket = "transcript_bucket"
	EventTypeAnalysisResult   = "analysis_result"
	EventTypeSystemMetric     = "system_metric"

	TopicChatBucketsV1       = "stream.chat_buckets.v1"
	TopicTranscriptBucketsV1 = "stream.transcript_buckets.v1"
	TopicAnalysisResultsV1   = "stream.analysis_results.v1"
	TopicSystemMetricsV1     = "stream.system_metrics.v1"

	TypeChatBucket       = EventTypeChatBucket
	TypeTranscriptBucket = EventTypeTranscriptBucket
	TypeAnalysisResult   = EventTypeAnalysisResult
	TypeSystemMetric     = EventTypeSystemMetric

	TopicChatBuckets       = TopicChatBucketsV1
	TopicTranscriptBuckets = TopicTranscriptBucketsV1
	TopicAnalysisResults   = TopicAnalysisResultsV1
	TopicSystemMetrics     = TopicSystemMetricsV1
)

var (
	ErrUnknownEventType = errors.New("unknown event_type")
	ErrUnknownType      = ErrUnknownEventType

	topicByEventType = map[string]string{
		EventTypeChatBucket:       TopicChatBucketsV1,
		EventTypeTranscriptBucket: TopicTranscriptBucketsV1,
		EventTypeAnalysisResult:   TopicAnalysisResultsV1,
		EventTypeSystemMetric:     TopicSystemMetricsV1,
	}

	payloadTypeAliases = map[string]map[string]struct{}{
		EventTypeChatBucket: {
			EventTypeChatBucket: {},
		},
		EventTypeTranscriptBucket: {
			EventTypeTranscriptBucket: {},
		},
		EventTypeAnalysisResult: {
			EventTypeAnalysisResult: {},
			"analysis_bucket":       {},
		},
		EventTypeSystemMetric: {
			EventTypeSystemMetric: {},
			"system_metrics":      {},
		},
	}
)

type Envelope struct {
	SchemaVersion      string          `json:"schema_version"`
	EventID            string          `json:"event_id"`
	EventType          string          `json:"event_type"`
	EventSchemaVersion string          `json:"event_schema_version"`
	Source             string          `json:"source"`
	SessionID          string          `json:"session_id,omitempty"`
	ChannelID          string          `json:"channel_id,omitempty"`
	OccurredAt         time.Time       `json:"occurred_at"`
	Payload            json.RawMessage `json:"payload"`
}

type PublishMessage struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

type Publisher interface {
	Publish(ctx context.Context, msg PublishMessage) error
}

type BucketKeyContext struct {
	SessionID string
	ChannelID string
	Start     time.Time
	End       time.Time
}

func DecodeEnvelope(data []byte) (Envelope, error) {
	var envelope Envelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode event envelope: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Envelope{}, errors.New("decode event envelope: trailing JSON values")
	}
	return envelope, nil
}

func NewEnvelope(eventType, source string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal payload: %w", err)
	}
	meta, err := payloadMeta(raw)
	if err != nil {
		return Envelope{}, err
	}
	occurredAt := meta.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = firstNonZeroTime(meta.TranscribedAt, meta.BucketStart, meta.WindowStart, time.Now().UTC())
	}
	envelope := Envelope{
		SchemaVersion:      EnvelopeSchemaVersion,
		EventType:          strings.TrimSpace(eventType),
		EventSchemaVersion: EventSchemaVersionV1,
		Source:             strings.TrimSpace(source),
		SessionID:          meta.SessionID,
		ChannelID:          meta.ChannelID,
		OccurredAt:         occurredAt.UTC(),
		Payload:            raw,
	}
	envelope.EventID = DeterministicEventID(envelope)
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func (e Envelope) Validate() error {
	if strings.TrimSpace(e.SchemaVersion) != EnvelopeSchemaVersion {
		return fmt.Errorf("schema_version must be %q", EnvelopeSchemaVersion)
	}
	if strings.TrimSpace(e.EventID) == "" {
		return errors.New("event_id is required")
	}
	if strings.TrimSpace(e.EventSchemaVersion) != EventSchemaVersionV1 {
		return fmt.Errorf("event_schema_version must be %q", EventSchemaVersionV1)
	}
	eventType := strings.TrimSpace(e.EventType)
	if _, ok := TopicForEventType(eventType); !ok {
		return fmt.Errorf("%w: %q", ErrUnknownEventType, e.EventType)
	}
	if strings.TrimSpace(e.Source) == "" {
		return errors.New("source is required")
	}
	if e.OccurredAt.IsZero() {
		return errors.New("occurred_at is required")
	}

	payload, err := e.payloadObject()
	if err != nil {
		return err
	}
	if err := validatePayloadType(eventType, payload); err != nil {
		return err
	}
	if bucketEventType(eventType) {
		if _, ok, err := e.BucketKeyContext(); err != nil {
			return err
		} else if !ok {
			return errors.New("bucket events require session_id, channel_id, and bucket/window start/end")
		}
	}
	return nil
}

func TopicForEventType(eventType string) (string, bool) {
	topic, ok := topicByEventType[strings.TrimSpace(eventType)]
	return topic, ok
}

func TopicForType(eventType string) (string, error) {
	topic, ok := TopicForEventType(eventType)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownEventType, eventType)
	}
	return topic, nil
}

func KnownEventTypes() []string {
	out := make([]string, 0, len(topicByEventType))
	for eventType := range topicByEventType {
		out = append(out, eventType)
	}
	sort.Strings(out)
	return out
}

func (e Envelope) Topic() (string, error) {
	topic, ok := TopicForEventType(e.EventType)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownEventType, e.EventType)
	}
	return topic, nil
}

func (e Envelope) PublishMessage(value []byte) (PublishMessage, error) {
	topic, err := e.Topic()
	if err != nil {
		return PublishMessage{}, err
	}
	key := e.EventKey()
	if key == "" {
		key = strings.TrimSpace(e.EventID)
	}
	return PublishMessage{
		Topic: topic,
		Key:   key,
		Value: append([]byte(nil), value...),
		Headers: map[string]string{
			"event_id":             strings.TrimSpace(e.EventID),
			"event_type":           strings.TrimSpace(e.EventType),
			"event_schema_version": strings.TrimSpace(e.EventSchemaVersion),
			"envelope_version":     strings.TrimSpace(e.SchemaVersion),
			"source":               strings.TrimSpace(e.Source),
		},
	}, nil
}

func (e Envelope) EventKey() string {
	if ctx, ok, err := e.BucketKeyContext(); err == nil && ok {
		return DeterministicBucketEventKey(e.EventType, ctx.SessionID, ctx.ChannelID, ctx.Start, ctx.End)
	}

	sessionID := strings.TrimSpace(e.SessionID)
	channelID := strings.TrimSpace(e.ChannelID)
	if sessionID != "" || channelID != "" {
		return sessionID + "|" + channelID
	}
	return strings.TrimSpace(e.EventID)
}

func Key(envelope Envelope) string {
	return envelope.EventKey()
}

func DeterministicEventID(envelope Envelope) string {
	key := envelope.EventKey()
	if key == "" {
		key = string(envelope.Payload)
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(envelope.EventType) + "|" + key + "|" + string(envelope.Payload)))
	return hex.EncodeToString(sum[:16])
}

func DeterministicBucketEventKey(eventType, sessionID, channelID string, start, end time.Time) string {
	return strings.Join([]string{
		strings.TrimSpace(eventType),
		strings.TrimSpace(sessionID),
		strings.TrimSpace(channelID),
		start.UTC().Format(time.RFC3339Nano),
		end.UTC().Format(time.RFC3339Nano),
	}, ":")
}

func (e Envelope) BucketKeyContext() (BucketKeyContext, bool, error) {
	payload, err := e.payloadObject()
	if err != nil {
		return BucketKeyContext{}, false, err
	}

	sessionID := firstNonEmpty(strings.TrimSpace(e.SessionID), payloadString(payload, "session_id"))
	channelID := firstNonEmpty(strings.TrimSpace(e.ChannelID), payloadString(payload, "channel_id"))
	if sessionID == "" || channelID == "" {
		return BucketKeyContext{}, false, nil
	}

	start, startOK, err := firstTime(payload, "bucket_start", "window_start")
	if err != nil {
		return BucketKeyContext{}, false, err
	}
	end, endOK, err := firstTime(payload, "bucket_end", "window_end")
	if err != nil {
		return BucketKeyContext{}, false, err
	}
	if !startOK || !endOK {
		return BucketKeyContext{}, false, nil
	}
	if !end.After(start) {
		return BucketKeyContext{}, false, errors.New("bucket/window end must be after start")
	}
	return BucketKeyContext{
		SessionID: sessionID,
		ChannelID: channelID,
		Start:     start,
		End:       end,
	}, true, nil
}

func (e Envelope) payloadObject() (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(e.Payload)) == 0 || bytes.Equal(bytes.TrimSpace(e.Payload), []byte("null")) {
		return nil, errors.New("payload is required")
	}
	var payload map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(e.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("payload must be a JSON object: %w", err)
	}
	if payload == nil {
		return nil, errors.New("payload must be a JSON object")
	}
	return payload, nil
}

func validatePayloadType(eventType string, payload map[string]json.RawMessage) error {
	raw, ok := payload["type"]
	if !ok || len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var payloadType string
	if err := json.Unmarshal(raw, &payloadType); err != nil {
		return fmt.Errorf("payload type must be a string: %w", err)
	}
	payloadType = strings.TrimSpace(payloadType)
	if payloadType == "" {
		return nil
	}
	aliases := payloadTypeAliases[eventType]
	if _, ok := aliases[payloadType]; !ok {
		return fmt.Errorf("payload type %q does not match event_type %q", payloadType, eventType)
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

type envelopePayloadMeta struct {
	SessionID     string    `json:"session_id"`
	ChannelID     string    `json:"channel_id"`
	BucketStart   time.Time `json:"bucket_start"`
	BucketEnd     time.Time `json:"bucket_end"`
	WindowStart   time.Time `json:"window_start"`
	WindowEnd     time.Time `json:"window_end"`
	TranscribedAt time.Time `json:"transcribed_at"`
	OccurredAt    time.Time `json:"occurred_at"`
}

func payloadMeta(raw []byte) (envelopePayloadMeta, error) {
	var meta envelopePayloadMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return envelopePayloadMeta{}, fmt.Errorf("payload must be a JSON object: %w", err)
	}
	meta.SessionID = strings.TrimSpace(meta.SessionID)
	meta.ChannelID = strings.TrimSpace(meta.ChannelID)
	return meta, nil
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func payloadString(payload map[string]json.RawMessage, field string) string {
	raw, ok := payload[field]
	if !ok {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func firstTime(payload map[string]json.RawMessage, fields ...string) (time.Time, bool, error) {
	for _, field := range fields {
		raw, ok := payload[field]
		if !ok || len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			continue
		}
		var value time.Time
		if err := value.UnmarshalJSON(raw); err != nil {
			return time.Time{}, false, fmt.Errorf("%s must be an RFC3339 timestamp: %w", field, err)
		}
		if value.IsZero() {
			return time.Time{}, false, fmt.Errorf("%s is required", field)
		}
		return value, true, nil
	}
	return time.Time{}, false, nil
}

func bucketEventType(eventType string) bool {
	switch eventType {
	case EventTypeChatBucket, EventTypeTranscriptBucket:
		return true
	default:
		return false
	}
}
