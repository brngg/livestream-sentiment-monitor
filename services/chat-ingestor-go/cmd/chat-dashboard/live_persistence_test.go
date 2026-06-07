package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/bucket"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/nlp"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/twitchapi"
)

type failingStore struct {
	storage.NoopStore
	chatBuckets atomic.Int32
}

func (f *failingStore) CreateSession(context.Context, storage.SessionRecord) error {
	return errors.New("write failed")
}

func (f *failingStore) FindSessionByTwitchStream(context.Context, string, string) (storage.SessionRecord, error) {
	return storage.SessionRecord{}, storage.ErrNotFound
}

func (f *failingStore) UpdateSessionStatus(context.Context, storage.SessionStatusUpdate) error {
	return errors.New("write failed")
}

func (f *failingStore) SaveIngestionRun(context.Context, storage.IngestionRunRecord) error {
	return errors.New("write failed")
}

func (f *failingStore) UpdateIngestionRun(context.Context, storage.IngestionRunUpdate) error {
	return errors.New("write failed")
}

func (f *failingStore) SaveChatBucket(context.Context, chat.ChatBucket) error {
	f.chatBuckets.Add(1)
	return errors.New("write failed")
}

func (f *failingStore) SaveTranscriptBucket(context.Context, storage.TranscriptBucket) error {
	return errors.New("write failed")
}

func (f *failingStore) SaveAlignment(context.Context, storage.AlignmentBucket) error {
	return errors.New("write failed")
}

func (f *failingStore) SaveHumanLabel(context.Context, storage.HumanLabel) error {
	return errors.New("write failed")
}

type streamIdentityStore struct {
	storage.NoopStore
	found           storage.SessionRecord
	findErr         error
	findAfterCreate storage.SessionRecord
	created         []storage.SessionRecord
}

func (s *streamIdentityStore) FindSessionByTwitchStream(context.Context, string, string) (storage.SessionRecord, error) {
	if len(s.created) > 0 && s.findAfterCreate.SessionID != "" {
		return s.findAfterCreate, nil
	}
	if s.findErr != nil {
		return storage.SessionRecord{}, s.findErr
	}
	return s.found, nil
}

func (s *streamIdentityStore) CreateSession(_ context.Context, record storage.SessionRecord) error {
	s.created = append(s.created, record)
	return nil
}

type createFailingStreamIdentityStore struct {
	streamIdentityStore
}

func (s *createFailingStreamIdentityStore) CreateSession(context.Context, storage.SessionRecord) error {
	return errors.New("create failed")
}

type shutdownStatusStore struct {
	storage.NoopStore
	sessionUpdates []storage.SessionStatusUpdate
	runUpdates     []storage.IngestionRunUpdate
}

func (s *shutdownStatusStore) UpdateSessionStatus(_ context.Context, update storage.SessionStatusUpdate) error {
	s.sessionUpdates = append(s.sessionUpdates, update)
	return nil
}

func (s *shutdownStatusStore) UpdateIngestionRun(_ context.Context, update storage.IngestionRunUpdate) error {
	s.runUpdates = append(s.runUpdates, update)
	return nil
}

func TestTranscriptBucketTimingFieldsDecodeAndMapToStorage(t *testing.T) {
	raw := []byte(`{
		"type": "transcript_bucket",
		"session_id": "session",
		"channel_id": "channel",
		"bucket_start": "2026-05-01T12:00:00Z",
		"bucket_end": "2026-05-01T12:00:30Z",
		"audio_started_at": "2026-05-01T12:00:00Z",
		"audio_ended_at": "2026-05-01T12:00:30Z",
		"transcribed_at": "2026-05-01T12:00:31.200Z",
		"asr_latency_ms": 940,
		"pipeline_latency_ms": 1200,
		"transcript_status": "repairing",
		"audio_seconds": 30,
		"segment_count": 1,
		"word_count": 3,
		"empty_ratio": 0.2,
		"repair_added_words": 1,
		"repair_changed_ratio": 0.5,
		"text": "near live transcript",
		"language": "en",
		"transcript_confidence": 0.91,
		"segments": [{
			"start": 0,
			"end": 1.1,
			"text": "near live",
			"confidence": 0.92,
			"words": [
				{"start": 0, "end": 0.4, "text": "near", "probability": 0.9},
				{"start": 0.4, "end": 1.1, "text": "live", "probability": 0.95}
			]
		}]
	}`)
	var bucket transcriptBucket
	if err := json.Unmarshal(raw, &bucket); err != nil {
		t.Fatalf("decode transcript bucket: %v", err)
	}

	record := storageTranscriptBucket(bucket)

	if record.AudioStartedAt == nil || !record.AudioStartedAt.Equal(bucket.BucketStart) {
		t.Fatalf("audio_started_at was not decoded/mapped: %#v", record.AudioStartedAt)
	}
	if record.AudioEndedAt == nil || !record.AudioEndedAt.Equal(bucket.BucketEnd) {
		t.Fatalf("audio_ended_at was not decoded/mapped: %#v", record.AudioEndedAt)
	}
	if record.TranscribedAt == nil || record.TranscribedAt.Sub(bucket.BucketEnd) != 1200*time.Millisecond {
		t.Fatalf("transcribed_at was not decoded/mapped: %#v", record.TranscribedAt)
	}
	if record.ASRLatencyMS == nil || *record.ASRLatencyMS != 940 {
		t.Fatalf("asr_latency_ms = %v, want 940", record.ASRLatencyMS)
	}
	if record.PipelineLatencyMS == nil || *record.PipelineLatencyMS != 1200 {
		t.Fatalf("pipeline_latency_ms = %v, want 1200", record.PipelineLatencyMS)
	}
	if record.TranscriptStatus != "repairing" || record.AudioSeconds != 30 || record.SegmentCount != 1 || record.WordCount != 3 || record.EmptyRatio != 0.2 || record.RepairAddedWords != 1 || record.RepairChangedRatio != 0.5 {
		t.Fatalf("completeness fields were not decoded/mapped: %#v", record)
	}
	if len(record.Segments) != 1 || len(record.Segments[0].Words) != 2 {
		t.Fatalf("word timestamps were not decoded/mapped: %#v", record.Segments)
	}
	if record.Segments[0].Words[1].Text != "live" || record.Segments[0].Words[1].Probability == nil || *record.Segments[0].Words[1].Probability != 0.95 {
		t.Fatalf("unexpected mapped word: %#v", record.Segments[0].Words[1])
	}
}

func TestMakeChatMessageSamplesHashesIdentityAndAppliesLimit(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	bucket := chat.ChatBucket{
		SessionID:   "session",
		ChannelID:   "channel",
		BucketStart: start,
		BucketEnd:   start.Add(30 * time.Second),
		MessageScores: []chat.MessageScore{
			{MessageID: "m1", Timestamp: start.Add(time.Second), Username: "ViewerOne", DisplayName: "Viewer One", Text: "hello", Label: "positive", Confidence: 0.8, SentimentScore: 0.6},
			{MessageID: "m2", Timestamp: start.Add(2 * time.Second), Username: "ViewerTwo", Text: "wow", Label: "neutral", Confidence: 0.7, SentimentScore: 0.1},
		},
	}

	samples := makeChatMessageSamples(bucket, 1, "salt")
	if len(samples) != 1 {
		t.Fatalf("sample count = %d, want 1", len(samples))
	}
	if samples[0].UserHash == "" {
		t.Fatal("expected hashed identity")
	}
	if samples[0].UserHash == "ViewerOne" || samples[0].UserHash == "Viewer One" {
		t.Fatalf("identity was not hashed: %q", samples[0].UserHash)
	}
	if samples[0].MessageID != "m1" || samples[0].Text != "hello" {
		t.Fatalf("sample did not preserve message score fields: %#v", samples[0])
	}
}

func TestMakeChatMessageSamplesPersistsPeakEvidenceWhenTraceMissing(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	bucket := chat.ChatBucket{
		SessionID:   "session",
		ChannelID:   "channel",
		BucketStart: start,
		BucketEnd:   start.Add(30 * time.Second),
		PeakEvidenceMessages: []chat.ChatMessage{
			{
				SessionID:   "session",
				ChannelID:   "channel",
				MessageID:   "peak-1",
				Timestamp:   start.Add(12 * time.Second),
				Username:    "ViewerOne",
				DisplayName: "Viewer One",
				Text:        "NO WAY",
			},
		},
	}

	samples := makeChatMessageSamples(bucket, 2, "salt")

	if len(samples) != 1 {
		t.Fatalf("sample count = %d, want 1", len(samples))
	}
	if samples[0].MessageID != "peak-1" || samples[0].Text != "NO WAY" {
		t.Fatalf("peak evidence sample not preserved: %#v", samples[0])
	}
	if samples[0].Label != "" || samples[0].Confidence != 0 || samples[0].SentimentScore != 0 {
		t.Fatalf("unexpected model fields on peak-only sample: %#v", samples[0])
	}
}

func TestMakeChatMessageSamplesPrioritizesPeakEvidence(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	bucket := chat.ChatBucket{
		SessionID:   "session",
		ChannelID:   "channel",
		BucketStart: start,
		BucketEnd:   start.Add(30 * time.Second),
		MessageScores: []chat.MessageScore{
			{MessageID: "m1", Timestamp: start, Username: "viewer1", Text: "general one", Label: "neutral", Confidence: 0.8},
			{MessageID: "m2", Timestamp: start, Username: "viewer2", Text: "general two", Label: "neutral", Confidence: 0.8},
		},
		PeakEvidenceMessages: []chat.ChatMessage{
			{SessionID: "session", ChannelID: "channel", MessageID: "peak-1", Timestamp: start.Add(12 * time.Second), Username: "peak", Text: "NO WAY"},
		},
	}

	samples := makeChatMessageSamples(bucket, 2, "salt")

	if len(samples) != 2 {
		t.Fatalf("sample count = %d, want 2", len(samples))
	}
	if samples[0].MessageID != "peak-1" {
		t.Fatalf("peak evidence should be first, got %#v", samples)
	}
	if samples[1].MessageID != "m1" {
		t.Fatalf("remaining slot should use trace sample, got %#v", samples)
	}
}

func TestMakeChatMessageSamplesDoesNotDuplicatePeakTrace(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	bucket := chat.ChatBucket{
		SessionID:   "session",
		ChannelID:   "channel",
		BucketStart: start,
		BucketEnd:   start.Add(30 * time.Second),
		MessageScores: []chat.MessageScore{
			{MessageID: "peak-1", Timestamp: start.Add(12 * time.Second), Username: "peak", Text: "NO WAY", Label: "positive", Confidence: 0.9},
			{MessageID: "m2", Timestamp: start, Username: "viewer2", Text: "general two", Label: "neutral", Confidence: 0.8},
		},
		PeakEvidenceMessages: []chat.ChatMessage{
			{SessionID: "session", ChannelID: "channel", MessageID: "peak-1", Timestamp: start.Add(12 * time.Second), Username: "peak", Text: "NO WAY"},
		},
	}

	samples := makeChatMessageSamples(bucket, 3, "salt")

	if len(samples) != 2 {
		t.Fatalf("sample count = %d, want 2: %#v", len(samples), samples)
	}
	if samples[0].MessageID != "peak-1" || samples[0].EvidenceRank != 0 {
		t.Fatalf("peak evidence rank not reserved: %#v", samples)
	}
	for index, sample := range samples[1:] {
		if sample.MessageID == "peak-1" {
			t.Fatalf("duplicate peak sample at index %d: %#v", index+1, samples)
		}
	}
}

func TestStorageFailureDoesNotBlockInMemoryBucketUpdate(t *testing.T) {
	store := &failingStore{}
	server := &server{
		cfg: appConfig{
			DatabaseWriteTimeout:     25 * time.Millisecond,
			ChatSampleLimitPerBucket: 10,
			ChatIdentityHashSalt:     "salt",
		},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:       store,
		humanLabels: map[string]string{},
	}
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	bucket := chat.ChatBucket{
		Type:        "chat_bucket",
		SessionID:   "session",
		ChannelID:   "channel",
		BucketStart: start,
		BucketEnd:   start.Add(30 * time.Second),
		MessageScores: []chat.MessageScore{
			{MessageID: "m1", Timestamp: start, Username: "viewer", Text: "hello", Label: "positive", Confidence: 0.8, SentimentScore: 0.6},
		},
	}

	server.updateState(dashboardEvent{Type: "chat_bucket", Session: "session", Channel: "channel", Bucket: &bucket})

	server.mu.Lock()
	bucketCount := server.state.BucketCount
	memoryBuckets := len(server.state.Buckets)
	server.mu.Unlock()
	if bucketCount != 1 || memoryBuckets != 1 {
		t.Fatalf("in-memory bucket update failed: count=%d buckets=%d", bucketCount, memoryBuckets)
	}

	deadline := time.After(250 * time.Millisecond)
	for store.chatBuckets.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for async storage write")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestAnalyzerBroadcastsPendingFallbackThenReplacesWithLatePythonBucket(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	var requestBody nlp.AnalyzeRequest
	analyzer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Errorf("decode analyzer request: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
		writeJSON(w, http.StatusOK, nlp.AnalyzeResponse{
			SessionID:      "session",
			ChannelID:      "channel",
			BucketStart:    start,
			BucketEnd:      start.Add(30 * time.Second),
			MessageCount:   1,
			AnalyzedCount:  1,
			SentimentScore: 0.95,
			Positive:       1,
			Confidence:     0.92,
			Model:          "python-test",
			LatencyMS:      50,
			MessageScores: []nlp.MessageScoreResponse{
				{MessageID: "m1", Timestamp: start, Username: "viewer", Text: "great", Label: "positive", Confidence: 0.92, SentimentScore: 0.95},
			},
		})
	}))
	defer analyzer.Close()

	server := &server{
		cfg: appConfig{
			NLPTimeout:               20 * time.Millisecond,
			NLPLateTimeout:           200 * time.Millisecond,
			DatabaseWriteTimeout:     time.Second,
			ChatSampleLimitPerBucket: 10,
			ChatIdentityHashSalt:     "salt",
		},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:       storage.NewNoopStore(),
		hub:         newEventHub(),
		humanLabels: map[string]string{},
	}
	item := detailedTestBucket(start)
	peakStart := start.Add(10 * time.Second)
	peakEnd := start.Add(15 * time.Second)
	item.Bucket.PeakWindowStart = &peakStart
	item.Bucket.PeakWindowEnd = &peakEnd

	done := make(chan struct{})
	go func() {
		server.analyzeAndBroadcastBucket(context.Background(), nlp.Client{Endpoint: analyzer.URL, Client: analyzer.Client()}, item)
		close(done)
	}()

	waitForBucketStatus(t, server, "fallback_pending")
	if got := server.metrics.modelFallbacks.Load(); got != 0 {
		t.Fatalf("pending slow analyzer counted as final fallback: %d", got)
	}
	if got := server.metrics.modelSlow.Load(); got != 1 {
		t.Fatalf("model slow count = %d, want 1", got)
	}

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for analyzer late result")
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.state.BucketCount != 1 || len(server.state.Buckets) != 1 {
		t.Fatalf("bucket was not replaced in place: count=%d len=%d", server.state.BucketCount, len(server.state.Buckets))
	}
	got := server.state.Buckets[0]
	if got.AnalysisStatus != "python" || got.SentimentModel != "python-test" || got.ChatSentiment != 0.95 {
		t.Fatalf("late python bucket did not win: %#v", got)
	}
	if got := server.metrics.modelFallbacks.Load(); got != 0 {
		t.Fatalf("late python success counted as fallback: %d", got)
	}
	if requestBody.PeakWindowStart == nil || !requestBody.PeakWindowStart.Equal(peakStart) {
		t.Fatalf("peak_window_start not forwarded: %#v", requestBody.PeakWindowStart)
	}
	if requestBody.PeakWindowEnd == nil || !requestBody.PeakWindowEnd.Equal(peakEnd) {
		t.Fatalf("peak_window_end not forwarded: %#v", requestBody.PeakWindowEnd)
	}
}

func TestAnalyzerFinalizesFallbackWhenPythonExceedsLateTimeout(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	analyzer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		writeJSON(w, http.StatusOK, nlp.AnalyzeResponse{
			SessionID:      "session",
			ChannelID:      "channel",
			BucketStart:    start,
			BucketEnd:      start.Add(30 * time.Second),
			AnalyzedCount:  1,
			SentimentScore: 0.95,
			Confidence:     0.92,
			Model:          "python-test",
		})
	}))
	defer analyzer.Close()

	server := &server{
		cfg: appConfig{
			NLPTimeout:               15 * time.Millisecond,
			NLPLateTimeout:           60 * time.Millisecond,
			DatabaseWriteTimeout:     time.Second,
			ChatSampleLimitPerBucket: 10,
			ChatIdentityHashSalt:     "salt",
		},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:       storage.NewNoopStore(),
		hub:         newEventHub(),
		humanLabels: map[string]string{},
	}

	done := make(chan struct{})
	go func() {
		server.analyzeAndBroadcastBucket(context.Background(), nlp.Client{Endpoint: analyzer.URL, Client: analyzer.Client()}, detailedTestBucket(start))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timed out waiting for bounded analyzer fallback")
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.state.BucketCount != 1 || len(server.state.Buckets) != 1 {
		t.Fatalf("bucket was not replaced in place: count=%d len=%d", server.state.BucketCount, len(server.state.Buckets))
	}
	if got := server.state.Buckets[0].AnalysisStatus; got != "fallback" {
		t.Fatalf("analysis status = %q, want final fallback", got)
	}
	if got := server.metrics.modelFallbacks.Load(); got != 1 {
		t.Fatalf("model fallback count = %d, want 1", got)
	}
}

func TestStopActiveSessionPersistsCurrentSessionAndRunSynchronously(t *testing.T) {
	store := &shutdownStatusStore{}
	var canceled atomic.Bool
	server := &server{
		cfg:    appConfig{DatabaseWriteTimeout: time.Second},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:  store,
		state: dashboardState{
			Status:  "ingesting",
			Session: "session",
			Channel: "channel",
			Stream:  &streamInfo{ID: "stream-1", UserID: "user-1", Title: "Live", Game: "Game", ViewerCount: 7, StartedAt: "2026-05-01T12:00:00Z", Language: "en"},
		},
		activeRunID: "run-1",
		cancelActive: func() {
			canceled.Store(true)
		},
	}

	server.stopActiveSession()

	if !canceled.Load() {
		t.Fatal("active session cancel was not called")
	}
	if len(store.sessionUpdates) != 1 {
		t.Fatalf("session update count = %d, want 1", len(store.sessionUpdates))
	}
	sessionUpdate := store.sessionUpdates[0]
	if sessionUpdate.SessionID != "session" || sessionUpdate.ChannelID != "channel" || sessionUpdate.Status != "stopped" || sessionUpdate.EndedAt == nil {
		t.Fatalf("unexpected session update: %#v", sessionUpdate)
	}
	if len(store.runUpdates) != 1 {
		t.Fatalf("run update count = %d, want 1", len(store.runUpdates))
	}
	runUpdate := store.runUpdates[0]
	if runUpdate.RunID != "run-1" || runUpdate.SessionID != "session" || runUpdate.Status != "stopped" || runUpdate.StopReason != "server shutdown" || runUpdate.EndedAt == nil {
		t.Fatalf("unexpected run update: %#v", runUpdate)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.cancelActive != nil || server.activeRunID != "" || server.state.Status != "stopped" {
		t.Fatalf("active state was not cleared: run=%q status=%q cancel_nil=%t", server.activeRunID, server.state.Status, server.cancelActive == nil)
	}
}

func TestResolveStreamSessionReusesStoredTwitchStream(t *testing.T) {
	store := &streamIdentityStore{found: storage.SessionRecord{SessionID: "stored-session"}}
	server := &server{
		cfg:    appConfig{DatabaseWriteTimeout: time.Second},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:  store,
	}

	sessionID, reused := server.resolveStreamSession(context.Background(), "speed", twitchapi.StreamStatus{ID: "123", UserID: "u1"})

	if sessionID != "stored-session" || !reused {
		t.Fatalf("resolve = (%q, %t), want stored reused session", sessionID, reused)
	}
	if len(store.created) != 0 {
		t.Fatalf("created %d sessions, want none", len(store.created))
	}
}

func TestResolveStreamSessionCreatesStableIDForNewTwitchStream(t *testing.T) {
	store := &streamIdentityStore{findErr: storage.ErrNotFound}
	server := &server{
		cfg: appConfig{
			DatabaseWriteTimeout: time.Second,
			BucketEvery:          30 * time.Second,
			NLPAnalyzerURL:       "http://nlp.local",
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:  store,
	}
	startedAt := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)

	sessionID, reused := server.resolveStreamSession(context.Background(), "speed", twitchapi.StreamStatus{
		ID:        "123",
		UserID:    "u1",
		StartedAt: startedAt,
	})

	if sessionID != "speed-123" || reused {
		t.Fatalf("resolve = (%q, %t), want new stable stream session", sessionID, reused)
	}
	if len(store.created) != 1 {
		t.Fatalf("created %d sessions, want 1", len(store.created))
	}
	created := store.created[0]
	if created.SessionID != "speed-123" || created.ChannelID != "speed" || created.TwitchStreamID != "123" || created.TwitchUserID != "u1" {
		t.Fatalf("unexpected created session: %#v", created)
	}
	if !created.StartedAt.Equal(startedAt) {
		t.Fatalf("started_at = %s, want %s", created.StartedAt, startedAt)
	}
}

func TestResolveStreamSessionReturnsCanonicalSessionAfterCreateConflict(t *testing.T) {
	store := &streamIdentityStore{
		findErr:         storage.ErrNotFound,
		findAfterCreate: storage.SessionRecord{SessionID: "speed-existing-stream"},
	}
	server := &server{
		cfg:    appConfig{DatabaseWriteTimeout: time.Second, BucketEvery: 30 * time.Second},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:  store,
	}

	sessionID, reused := server.resolveStreamSession(context.Background(), "speed", twitchapi.StreamStatus{ID: "123"})

	if sessionID != "speed-existing-stream" || !reused {
		t.Fatalf("resolve = (%q, %t), want canonical existing session", sessionID, reused)
	}
	if len(store.created) != 1 {
		t.Fatalf("created %d sessions, want attempted create before canonical lookup", len(store.created))
	}
}

func TestResolveStreamSessionRecordsCreateFailure(t *testing.T) {
	store := &createFailingStreamIdentityStore{
		streamIdentityStore: streamIdentityStore{findErr: storage.ErrNotFound},
	}
	server := &server{
		cfg: appConfig{
			DatabaseWriteTimeout:           time.Second,
			DatabaseHealthFailureThreshold: 1,
		},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:       store,
		persistence: newTestPersistenceQueue(t, 4, 1),
	}

	sessionID, reused := server.resolveStreamSession(context.Background(), "speed", twitchapi.StreamStatus{ID: "123"})

	if sessionID != "speed-123" || reused {
		t.Fatalf("resolve = (%q, %t), want local fallback session", sessionID, reused)
	}
	stats := server.persistenceStats()
	if stats.WriteFailures != 1 || stats.ConsecutiveFailures != 1 {
		t.Fatalf("create failure was not recorded: %#v", stats)
	}
	if !server.storageDegraded(stats) {
		t.Fatalf("storage should be degraded after create failure: %#v", stats)
	}
}

func detailedTestBucket(start time.Time) bucket.DetailedBucket {
	message := chat.ChatMessage{
		SessionID: "session",
		ChannelID: "channel",
		MessageID: "m1",
		Timestamp: start,
		Username:  "viewer",
		Text:      "great",
		Language:  "en",
	}
	return bucket.DetailedBucket{
		Bucket: chat.ChatBucket{
			Type:                "chat_bucket",
			SessionID:           "session",
			ChannelID:           "channel",
			BucketStart:         start,
			BucketEnd:           start.Add(30 * time.Second),
			MessageCount:        1,
			UniqueChatters:      1,
			ChatSentiment:       0.4,
			SentimentConfidence: 0.7,
			AnalyzedCount:       1,
			PositiveRatio:       1,
			AnalysisStatus:      "local",
			MessageScores: []chat.MessageScore{
				{MessageID: "m1", Timestamp: start, Username: "viewer", Text: "great", Label: "positive", Confidence: 0.7, SentimentScore: 0.4},
			},
		},
		Messages: []chat.ChatMessage{message},
	}
}

func waitForBucketStatus(t *testing.T, server *server, status string) {
	t.Helper()
	deadline := time.After(300 * time.Millisecond)
	for {
		server.mu.Lock()
		matched := len(server.state.Buckets) == 1 && server.state.Buckets[0].AnalysisStatus == status
		server.mu.Unlock()
		if matched {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for bucket status %q", status)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
