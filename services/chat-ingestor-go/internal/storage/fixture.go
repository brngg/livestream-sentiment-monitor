package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

type FixtureStore struct {
	NoopStore
	replays map[string]SessionReplay
}

func NewFixtureStoreFromFile(path string) (*FixtureStore, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var raw json.RawMessage
	if err := json.NewDecoder(file).Decode(&raw); err != nil {
		return nil, err
	}
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return nil, fmt.Errorf("fixture is empty")
	}

	store := &FixtureStore{replays: map[string]SessionReplay{}}
	switch raw[0] {
	case '[':
		var replays []SessionReplay
		if err := json.Unmarshal(raw, &replays); err != nil {
			return nil, err
		}
		for index, replay := range replays {
			if err := store.add(replay, fmt.Sprintf("fixture[%d]", index)); err != nil {
				return nil, err
			}
		}
	case '{':
		var replays map[string]SessionReplay
		if err := json.Unmarshal(raw, &replays); err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(replays))
		for key := range replays {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			replay := replays[key]
			if strings.TrimSpace(replay.Session.SessionID) == "" {
				replay.Session.SessionID = strings.TrimSpace(key)
			}
			if err := store.add(replay, fmt.Sprintf("fixture[%q]", key)); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("fixture must be a JSON array or object map")
	}
	if len(store.replays) == 0 {
		return nil, fmt.Errorf("fixture contains no session replays")
	}
	return store, nil
}

func (s *FixtureStore) add(replay SessionReplay, source string) error {
	sessionID := strings.TrimSpace(replay.Session.SessionID)
	if sessionID == "" {
		return fmt.Errorf("%s has empty session.session_id", source)
	}
	if _, exists := s.replays[sessionID]; exists {
		return fmt.Errorf("%s duplicates session %q", source, sessionID)
	}
	replay.Session.SessionID = sessionID
	if replay.LabelCount == 0 {
		replay.LabelCount = len(replay.WindowLabels)
	}
	if replay.Session.LabelCount == 0 {
		replay.Session.LabelCount = replay.LabelCount
	}
	s.replays[sessionID] = replay
	return nil
}

func (s *FixtureStore) SessionIDs() []string {
	sessionIDs := make([]string, 0, len(s.replays))
	for sessionID := range s.replays {
		sessionIDs = append(sessionIDs, sessionID)
	}
	sort.Strings(sessionIDs)
	return sessionIDs
}

func (s *FixtureStore) ListSessions(_ context.Context, limit int) ([]SessionHistory, error) {
	sessionIDs := s.SessionIDs()
	out := make([]SessionHistory, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		out = append(out, s.replays[sessionID].Session)
	}
	sort.Slice(out, func(left, right int) bool {
		return out[left].StartedAt.After(out[right].StartedAt)
	})
	if limit > 0 && len(out) > limit {
		return append([]SessionHistory(nil), out[:limit]...), nil
	}
	return out, nil
}

func (s *FixtureStore) GetSessionSummary(_ context.Context, sessionID string) (SessionSummary, error) {
	replay, ok := s.replays[sessionID]
	if !ok {
		return SessionSummary{}, ErrNotFound
	}
	return SessionSummary{
		Session:                 replay.Session,
		LatestChatBuckets:       latestChatBuckets(replay.ChatBuckets, 5),
		LatestTranscriptBuckets: latestTranscriptBuckets(replay.TranscriptBuckets, 5),
		LatestAlignments:        latestAlignments(replay.Alignments, 5),
		WindowLabels:            append([]SignalWindowLabel(nil), replay.WindowLabels...),
		LabelCount:              replay.LabelCount,
	}, nil
}

func (s *FixtureStore) GetSessionReplay(_ context.Context, sessionID string, limit int) (SessionReplay, error) {
	replay, ok := s.replays[sessionID]
	if !ok {
		return SessionReplay{}, ErrNotFound
	}
	if limit <= 0 {
		limit = 200
	} else if limit > 500 {
		limit = 500
	}
	if len(replay.ChatBuckets) > limit {
		replay.ChatBuckets = append([]chat.ChatBucket(nil), replay.ChatBuckets[:limit]...)
	}
	if len(replay.TranscriptBuckets) > limit {
		replay.TranscriptBuckets = append([]TranscriptBucket(nil), replay.TranscriptBuckets[:limit]...)
	}
	if len(replay.Alignments) > limit {
		replay.Alignments = append([]AlignmentBucket(nil), replay.Alignments[:limit]...)
	}
	return replay, nil
}

func (s *FixtureStore) ListSignalWindowLabels(_ context.Context, sessionID string) ([]SignalWindowLabel, error) {
	replay, ok := s.replays[sessionID]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]SignalWindowLabel(nil), replay.WindowLabels...), nil
}

func (s *FixtureStore) ListEvaluationAgentReviews(_ context.Context, sessionID string) ([]EvaluationAgentReview, error) {
	replay, ok := s.replays[sessionID]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]EvaluationAgentReview(nil), replay.AgentReviews...), nil
}

func latestChatBuckets(items []chat.ChatBucket, limit int) []chat.ChatBucket {
	if len(items) <= limit {
		return append([]chat.ChatBucket(nil), items...)
	}
	return append([]chat.ChatBucket(nil), items[len(items)-limit:]...)
}

func latestTranscriptBuckets(items []TranscriptBucket, limit int) []TranscriptBucket {
	if len(items) <= limit {
		return append([]TranscriptBucket(nil), items...)
	}
	return append([]TranscriptBucket(nil), items[len(items)-limit:]...)
}

func latestAlignments(items []AlignmentBucket, limit int) []AlignmentBucket {
	if len(items) <= limit {
		return append([]AlignmentBucket(nil), items...)
	}
	return append([]AlignmentBucket(nil), items[len(items)-limit:]...)
}

var _ Store = (*FixtureStore)(nil)
