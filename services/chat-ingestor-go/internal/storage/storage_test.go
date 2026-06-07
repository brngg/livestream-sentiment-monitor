package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestNoopStoreSafeDefaults(t *testing.T) {
	store := NewNoopStore()
	if err := store.CreateSession(context.Background(), SessionRecord{SessionID: "s1"}); err != nil {
		t.Fatalf("CreateSession error = %v", err)
	}
	history, err := store.ListSessions(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListSessions error = %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("history length = %d, want 0", len(history))
	}
	if _, err := store.GetSessionSummary(context.Background(), "missing"); !IsNotFound(err) {
		t.Fatalf("GetSessionSummary error = %v, want ErrNotFound", err)
	}
	if _, err := store.GetSessionReplay(context.Background(), "missing", 100); !IsNotFound(err) {
		t.Fatalf("GetSessionReplay error = %v, want ErrNotFound", err)
	}
	if _, err := store.FindSessionByTwitchStream(context.Background(), "channel", "stream"); !IsNotFound(err) {
		t.Fatalf("FindSessionByTwitchStream error = %v, want ErrNotFound", err)
	}
	if err := store.SaveIngestionRun(context.Background(), IngestionRunRecord{RunID: "run1", SessionID: "s1"}); err != nil {
		t.Fatalf("SaveIngestionRun error = %v", err)
	}
	if err := store.UpdateIngestionRun(context.Background(), IngestionRunUpdate{RunID: "run1", Status: "stopped"}); err != nil {
		t.Fatalf("UpdateIngestionRun error = %v", err)
	}
	if err := store.SaveEvaluationAgentReview(context.Background(), EvaluationAgentReview{SessionID: "s1", SuggestedEventLabel: "none"}); err != nil {
		t.Fatalf("SaveEvaluationAgentReview error = %v", err)
	}
	reviews, err := store.ListEvaluationAgentReviews(context.Background(), "s1")
	if err != nil {
		t.Fatalf("ListEvaluationAgentReviews error = %v", err)
	}
	if len(reviews) != 0 {
		t.Fatalf("agent reviews length = %d, want 0", len(reviews))
	}
}

func TestHashIdentityStableAndNormalized(t *testing.T) {
	left := HashIdentity("salt", "ViewerName")
	right := HashIdentity("salt", " viewername ")
	if left != right {
		t.Fatalf("hash should be stable after normalization")
	}
	if left == HashIdentity("other", "ViewerName") {
		t.Fatalf("hash should depend on salt")
	}
	if len(left) != 64 {
		t.Fatalf("hash length = %d, want sha256 hex length 64", len(left))
	}
}

func TestMigrationContainsCoreTablesAndUniqueness(t *testing.T) {
	for _, token := range []string{
		"CREATE TABLE IF NOT EXISTS stream_sessions",
		"twitch_stream_id TEXT NOT NULL DEFAULT ''",
		"twitch_user_id TEXT NOT NULL DEFAULT ''",
		"first_seen_at TIMESTAMPTZ",
		"last_seen_at TIMESTAMPTZ",
		"CREATE UNIQUE INDEX IF NOT EXISTS stream_sessions_channel_twitch_stream_uidx",
		"WHERE twitch_stream_id <> ''",
		"CREATE TABLE IF NOT EXISTS ingestion_runs",
		"run_id TEXT PRIMARY KEY",
		"session_id TEXT NOT NULL REFERENCES stream_sessions(session_id) ON DELETE CASCADE",
		"CREATE TABLE IF NOT EXISTS chat_message_samples",
		"CREATE TABLE IF NOT EXISTS chat_buckets",
		"peak_reaction_score DOUBLE PRECISION",
		"peak_reaction_type TEXT NOT NULL DEFAULT ''",
		"peak_target_type TEXT NOT NULL DEFAULT ''",
		"peak_target_text TEXT NOT NULL DEFAULT ''",
		"peak_source TEXT NOT NULL DEFAULT ''",
		"peak_event_hint TEXT NOT NULL DEFAULT ''",
		"peak_confidence DOUBLE PRECISION NOT NULL DEFAULT 0",
		"peak_evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
		"peak_time TIMESTAMPTZ",
		"peak_window_start TIMESTAMPTZ",
		"peak_window_end TIMESTAMPTZ",
		"subwindows JSONB NOT NULL DEFAULT '[]'::jsonb",
		"peak_evidence_messages JSONB NOT NULL DEFAULT '[]'::jsonb",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_reaction_score DOUBLE PRECISION",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_reaction_type TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_target_type TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_target_text TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_source TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_event_hint TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_confidence DOUBLE PRECISION NOT NULL DEFAULT 0",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_time TIMESTAMPTZ",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_window_start TIMESTAMPTZ",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_window_end TIMESTAMPTZ",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS subwindows JSONB NOT NULL DEFAULT '[]'::jsonb",
		"ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_evidence_messages JSONB NOT NULL DEFAULT '[]'::jsonb",
		"CREATE TABLE IF NOT EXISTS transcript_buckets",
		"audio_started_at TIMESTAMPTZ",
		"audio_ended_at TIMESTAMPTZ",
		"transcribed_at TIMESTAMPTZ",
		"asr_latency_ms BIGINT",
		"pipeline_latency_ms BIGINT",
		"transcript_status TEXT NOT NULL DEFAULT ''",
		"audio_seconds DOUBLE PRECISION NOT NULL DEFAULT 0",
		"segment_count INTEGER NOT NULL DEFAULT 0",
		"word_count INTEGER NOT NULL DEFAULT 0",
		"empty_ratio DOUBLE PRECISION NOT NULL DEFAULT 0",
		"repair_added_words INTEGER NOT NULL DEFAULT 0",
		"repair_changed_ratio DOUBLE PRECISION NOT NULL DEFAULT 0",
		"ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS audio_started_at TIMESTAMPTZ",
		"ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS pipeline_latency_ms BIGINT",
		"ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS transcript_status TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS repair_changed_ratio DOUBLE PRECISION NOT NULL DEFAULT 0",
		"segments JSONB NOT NULL DEFAULT '[]'::jsonb",
		"quality JSONB NOT NULL DEFAULT '{}'::jsonb",
		"CREATE TABLE IF NOT EXISTS alignment_buckets",
		"CREATE TABLE IF NOT EXISTS human_labels",
		"CREATE TABLE IF NOT EXISTS signal_window_labels",
		"reaction_type TEXT NOT NULL DEFAULT ''",
		"target_type TEXT NOT NULL DEFAULT ''",
		"target_text TEXT NOT NULL DEFAULT ''",
		"divergence_type TEXT NOT NULL DEFAULT ''",
		"event_start TIMESTAMPTZ",
		"event_peak TIMESTAMPTZ",
		"ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS reaction_type TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS target_type TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS target_text TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS divergence_type TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS event_start TIMESTAMPTZ",
		"ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS event_peak TIMESTAMPTZ",
		"CREATE TABLE IF NOT EXISTS evaluation_agent_reviews",
		"review_id TEXT PRIMARY KEY",
		"suggested_event_label TEXT NOT NULL",
		"streamer_usefulness DOUBLE PRECISION NOT NULL DEFAULT 0",
		"reason TEXT NOT NULL DEFAULT ''",
		"evidence JSONB NOT NULL DEFAULT '[]'::jsonb",
		"CREATE TABLE IF NOT EXISTS system_metrics",
		"UNIQUE (session_id, channel_id, bucket_start, bucket_end)",
		"UNIQUE (session_id, channel_id, window_start, window_end, chat_bucket_start, transcript_bucket_start)",
		"PRIMARY KEY (session_id, window_start, window_end)",
	} {
		if !strings.Contains(Migration001, token) {
			t.Fatalf("migration missing %q", token)
		}
	}
}

func TestTranscriptSegmentWordTimestampsJSONRoundTrip(t *testing.T) {
	probability := 0.91
	segments := []TranscriptSegment{
		{
			Start: 0,
			End:   1.2,
			Text:  "hello world",
			Words: []TranscriptWord{
				{Start: 0, End: 0.5, Text: "hello", Probability: &probability},
				{Start: 0.5, End: 1.2, Text: "world"},
			},
		},
	}

	var decoded []TranscriptSegment
	if err := json.Unmarshal([]byte(jsonString(segments)), &decoded); err != nil {
		t.Fatalf("unmarshal transcript segments: %v", err)
	}

	if len(decoded) != 1 || len(decoded[0].Words) != 2 {
		t.Fatalf("word timestamps did not round trip: %#v", decoded)
	}
	if decoded[0].Words[0].Text != "hello" || decoded[0].Words[0].Probability == nil || *decoded[0].Words[0].Probability != probability {
		t.Fatalf("unexpected first word: %#v", decoded[0].Words[0])
	}
	if decoded[0].Words[1].Start != 0.5 || decoded[0].Words[1].End != 1.2 || decoded[0].Words[1].Text != "world" {
		t.Fatalf("unexpected second word: %#v", decoded[0].Words[1])
	}
}

func TestChatBucketPeakMetadataJSONRoundTrip(t *testing.T) {
	start := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	score := 0.91
	peakStart := start.Add(12 * time.Second)
	peakEnd := start.Add(17 * time.Second)
	bucket := chat.ChatBucket{
		PeakReactionScore: &score,
		PeakReactionType:  "hype",
		PeakTime:          &peakEnd,
		PeakWindowStart:   &peakStart,
		PeakWindowEnd:     &peakEnd,
		Subwindows: []chat.ReactionSubwindow{
			{WindowStart: peakStart, WindowEnd: peakEnd, ReactionScore: score, ReactionType: "hype"},
		},
		PeakEvidenceMessages: []chat.ChatMessage{
			{MessageID: "m1", Timestamp: peakStart, Username: "viewer", Text: "NO WAY"},
		},
	}

	var subwindows []chat.ReactionSubwindow
	if err := json.Unmarshal([]byte(jsonString(bucket.Subwindows)), &subwindows); err != nil {
		t.Fatalf("unmarshal subwindows: %v", err)
	}
	var evidence []chat.ChatMessage
	if err := json.Unmarshal([]byte(jsonString(bucket.PeakEvidenceMessages)), &evidence); err != nil {
		t.Fatalf("unmarshal peak evidence: %v", err)
	}

	if len(subwindows) != 1 || subwindows[0].ReactionType != "hype" || subwindows[0].ReactionScore != score {
		t.Fatalf("unexpected subwindow round trip: %#v", subwindows)
	}
	if len(evidence) != 1 || evidence[0].MessageID != "m1" || evidence[0].Text != "NO WAY" {
		t.Fatalf("unexpected peak evidence round trip: %#v", evidence)
	}
}

func TestSignalWindowLabelOmitsZeroOptionalEventTimes(t *testing.T) {
	start := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	data, err := json.Marshal(SignalWindowLabel{
		SessionID:   "session-1",
		WindowStart: start,
		WindowEnd:   start.Add(5 * time.Second),
		Correctness: "correct",
		EventLabel:  "hype_spike",
	})
	if err != nil {
		t.Fatalf("marshal signal window label: %v", err)
	}
	text := string(data)
	if strings.Contains(text, "event_start") || strings.Contains(text, "event_peak") || strings.Contains(text, "0001-01-01") {
		t.Fatalf("zero optional event times should be omitted, got %s", text)
	}

	peak := start.Add(3 * time.Second)
	data, err = json.Marshal(SignalWindowLabel{
		SessionID:   "session-1",
		WindowStart: start,
		WindowEnd:   start.Add(5 * time.Second),
		EventPeak:   peak,
		Correctness: "correct",
		EventLabel:  "hype_spike",
	})
	if err != nil {
		t.Fatalf("marshal signal window label with peak: %v", err)
	}
	if !strings.Contains(string(data), `"event_peak":"2026-05-08T12:00:03Z"`) {
		t.Fatalf("non-zero event peak should be present, got %s", string(data))
	}
}

func TestPostgresChatBucketPeakMetadataRoundTrip(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STORAGE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STORAGE_TEST_DATABASE_URL to run Postgres peak metadata round-trip test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}
	defer store.Close()

	sessionID := fmt.Sprintf("test-peak-%d", time.Now().UnixNano())
	channelID := "storage-test"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.pool.Exec(cleanupCtx, `DELETE FROM chat_message_samples WHERE session_id = $1`, sessionID)
		_, _ = store.pool.Exec(cleanupCtx, `DELETE FROM chat_buckets WHERE session_id = $1`, sessionID)
		_, _ = store.pool.Exec(cleanupCtx, `DELETE FROM stream_sessions WHERE session_id = $1`, sessionID)
	})

	start := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	score := 0.91
	peakStart := start.Add(12 * time.Second)
	peakEnd := start.Add(17 * time.Second)
	if err := store.CreateSession(ctx, SessionRecord{
		SessionID:     sessionID,
		ChannelID:     channelID,
		Status:        "test",
		StartedAt:     start,
		BucketSeconds: 30,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err := store.SaveChatBucket(ctx, chat.ChatBucket{
		Type:                "chat_bucket",
		SessionID:           sessionID,
		ChannelID:           channelID,
		BucketStart:         start,
		BucketEnd:           start.Add(30 * time.Second),
		MessageCount:        2,
		UniqueChatters:      2,
		ChatSentiment:       0.4,
		SentimentConfidence: 0.8,
		PeakReactionScore:   &score,
		PeakReactionType:    "hype",
		PeakTargetType:      "unknown",
		PeakTargetText:      "boss",
		PeakSource:          "chat",
		PeakEventHint:       "hype:boss",
		PeakConfidence:      0.84,
		PeakEvidenceIDs:     []string{"m1"},
		PeakTime:            &peakEnd,
		PeakWindowStart:     &peakStart,
		PeakWindowEnd:       &peakEnd,
		Subwindows: []chat.ReactionSubwindow{
			{WindowStart: peakStart, WindowEnd: peakEnd, ReactionScore: score, ReactionType: "hype", TargetType: "unknown", TargetText: "boss", Source: "chat", EventHint: "hype:boss", Confidence: 0.84, EvidenceIDs: []string{"m1"}},
		},
		PeakEvidenceMessages: []chat.ChatMessage{
			{SessionID: sessionID, ChannelID: channelID, MessageID: "m1", Timestamp: peakStart, Username: "viewer", Text: "NO WAY"},
		},
	}); err != nil {
		t.Fatalf("save chat bucket: %v", err)
	}

	replay, err := store.GetSessionReplay(ctx, sessionID, 10)
	if err != nil {
		t.Fatalf("get session replay: %v", err)
	}
	if len(replay.ChatBuckets) != 1 {
		t.Fatalf("chat buckets = %d, want 1", len(replay.ChatBuckets))
	}
	got := replay.ChatBuckets[0]
	if got.PeakReactionScore == nil || *got.PeakReactionScore != score || got.PeakReactionType != "hype" {
		t.Fatalf("peak score/type did not round trip: %#v", got)
	}
	if got.PeakWindowStart == nil || !got.PeakWindowStart.Equal(peakStart) || got.PeakWindowEnd == nil || !got.PeakWindowEnd.Equal(peakEnd) {
		t.Fatalf("peak window did not round trip: %#v", got)
	}
	if len(got.Subwindows) != 1 || got.Subwindows[0].ReactionType != "hype" {
		t.Fatalf("subwindows did not round trip: %#v", got.Subwindows)
	}
	if got.PeakTargetType != "unknown" || got.PeakTargetText != "boss" || got.PeakSource != "chat" || got.PeakEventHint != "hype:boss" || got.PeakConfidence != 0.84 {
		t.Fatalf("peak context did not round trip: %#v", got)
	}
	if len(got.PeakEvidenceIDs) != 1 || got.PeakEvidenceIDs[0] != "m1" {
		t.Fatalf("peak evidence ids did not round trip: %#v", got.PeakEvidenceIDs)
	}
	if got.Subwindows[0].TargetText != "boss" || got.Subwindows[0].EventHint != "hype:boss" || len(got.Subwindows[0].EvidenceIDs) != 1 {
		t.Fatalf("subwindow context did not round trip: %#v", got.Subwindows[0])
	}
	if len(got.PeakEvidenceMessages) != 1 || got.PeakEvidenceMessages[0].Text != "NO WAY" {
		t.Fatalf("peak evidence did not round trip: %#v", got.PeakEvidenceMessages)
	}
}

func TestPostgresChatMessageSamplePreservesReservedPeakRank(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STORAGE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STORAGE_TEST_DATABASE_URL to run Postgres sample rank round-trip test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}
	defer store.Close()

	sessionID := fmt.Sprintf("test-sample-rank-%d", time.Now().UnixNano())
	channelID := "storage-test"
	messageID := "peak-message"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = store.pool.Exec(cleanupCtx, `DELETE FROM chat_message_samples WHERE session_id = $1`, sessionID)
	})

	start := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	if err := store.SaveChatMessageSample(ctx, ChatMessageSample{
		SessionID:    sessionID,
		ChannelID:    channelID,
		MessageID:    messageID,
		BucketStart:  start,
		BucketEnd:    start.Add(30 * time.Second),
		Timestamp:    start.Add(12 * time.Second),
		UserHash:     "peak-user",
		Text:         "NO WAY",
		EvidenceRank: 0,
	}); err != nil {
		t.Fatalf("save peak sample: %v", err)
	}
	if err := store.SaveChatMessageSample(ctx, ChatMessageSample{
		SessionID:      sessionID,
		ChannelID:      channelID,
		MessageID:      messageID,
		BucketStart:    start,
		BucketEnd:      start.Add(30 * time.Second),
		Timestamp:      start.Add(12 * time.Second),
		UserHash:       "peak-user",
		Text:           "NO WAY",
		Label:          "positive",
		Confidence:     0.9,
		SentimentScore: 0.9,
		EvidenceRank:   7,
	}); err != nil {
		t.Fatalf("save duplicate scored sample: %v", err)
	}

	var evidenceRank int
	var label string
	if err := store.pool.QueryRow(ctx, `
		SELECT evidence_rank, label
		FROM chat_message_samples
		WHERE session_id = $1 AND message_id = $2
	`, sessionID, messageID).Scan(&evidenceRank, &label); err != nil {
		t.Fatalf("query sample: %v", err)
	}
	if evidenceRank != 0 {
		t.Fatalf("evidence rank = %d, want reserved peak rank 0", evidenceRank)
	}
	if label != "positive" {
		t.Fatalf("label = %q, want later scored metadata to update", label)
	}
}
