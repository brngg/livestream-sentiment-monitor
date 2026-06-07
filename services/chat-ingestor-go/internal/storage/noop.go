package storage

import (
	"context"
	"errors"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

var ErrNotFound = errors.New("storage record not found")

type NoopStore struct{}

func NewNoopStore() NoopStore {
	return NoopStore{}
}

func (NoopStore) CreateSession(context.Context, SessionRecord) error {
	return nil
}

func (NoopStore) FindSessionByTwitchStream(context.Context, string, string) (SessionRecord, error) {
	return SessionRecord{}, ErrNotFound
}

func (NoopStore) UpdateSessionStatus(context.Context, SessionStatusUpdate) error {
	return nil
}

func (NoopStore) SaveIngestionRun(context.Context, IngestionRunRecord) error {
	return nil
}

func (NoopStore) UpdateIngestionRun(context.Context, IngestionRunUpdate) error {
	return nil
}

func (NoopStore) SaveChatMessageSample(context.Context, ChatMessageSample) error {
	return nil
}

func (NoopStore) SaveChatBucket(context.Context, chat.ChatBucket) error {
	return nil
}

func (NoopStore) SaveTranscriptBucket(context.Context, TranscriptBucket) error {
	return nil
}

func (NoopStore) SaveAlignment(context.Context, AlignmentBucket) error {
	return nil
}

func (NoopStore) SaveHumanLabel(context.Context, HumanLabel) error {
	return nil
}

func (NoopStore) SaveSignalWindowLabel(context.Context, SignalWindowLabel) error {
	return nil
}

func (NoopStore) ListSignalWindowLabels(context.Context, string) ([]SignalWindowLabel, error) {
	return []SignalWindowLabel{}, nil
}

func (NoopStore) SaveEvaluationAgentReview(context.Context, EvaluationAgentReview) error {
	return nil
}

func (NoopStore) ListEvaluationAgentReviews(context.Context, string) ([]EvaluationAgentReview, error) {
	return []EvaluationAgentReview{}, nil
}

func (NoopStore) SaveMetric(context.Context, SystemMetric) error {
	return nil
}

func (NoopStore) ListSessions(context.Context, int) ([]SessionHistory, error) {
	return []SessionHistory{}, nil
}

func (NoopStore) GetSessionSummary(context.Context, string) (SessionSummary, error) {
	return SessionSummary{}, ErrNotFound
}

func (NoopStore) GetSessionReplay(context.Context, string, int) (SessionReplay, error) {
	return SessionReplay{}, ErrNotFound
}

func (NoopStore) Close() {}

var _ Store = NoopStore{}
