package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

type historyStore struct {
	storage.NoopStore
	sessions []storage.SessionHistory
	summary  storage.SessionSummary
	replay   storage.SessionReplay
	metrics  []storage.SystemMetric
}

func (s *historyStore) ListSessions(context.Context, int) ([]storage.SessionHistory, error) {
	return s.sessions, nil
}

func (s *historyStore) GetSessionSummary(_ context.Context, sessionID string) (storage.SessionSummary, error) {
	if s.summary.Session.SessionID != sessionID {
		return storage.SessionSummary{}, storage.ErrNotFound
	}
	return s.summary, nil
}

func (s *historyStore) GetSessionReplay(_ context.Context, sessionID string, _ int) (storage.SessionReplay, error) {
	if s.replay.Session.SessionID != sessionID {
		return storage.SessionReplay{}, storage.ErrNotFound
	}
	return s.replay, nil
}

func (s *historyStore) SaveMetric(_ context.Context, metric storage.SystemMetric) error {
	s.metrics = append(s.metrics, metric)
	return nil
}

func TestHandleSessionHistoryReturnsStoredSessions(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store := &historyStore{
		sessions: []storage.SessionHistory{
			{
				SessionID:             "abc-1",
				ChannelID:             "abc",
				Status:                "ended",
				StartedAt:             start,
				ChatBucketCount:       2,
				TranscriptBucketCount: 1,
				AlignmentCount:        1,
				LabelCount:            3,
			},
		},
	}
	server := &server{cfg: appConfig{DatabaseWriteTimeout: time.Second}, store: store}

	request := httptest.NewRequest(http.MethodGet, "/sessions/history", nil)
	response := httptest.NewRecorder()
	server.handleSessionHistory(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var body struct {
		Sessions []storage.SessionHistory `json:"sessions"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Sessions) != 1 || body.Sessions[0].SessionID != "abc-1" {
		t.Fatalf("unexpected sessions: %#v", body.Sessions)
	}
}

func TestHandleSessionSummaryReturnsNotFoundForMissingStoredSession(t *testing.T) {
	server := &server{cfg: appConfig{DatabaseWriteTimeout: time.Second}, store: storage.NewNoopStore()}
	request := httptest.NewRequest(http.MethodGet, "/sessions/missing/summary", nil)
	request.SetPathValue("session_id", "missing")
	response := httptest.NewRecorder()

	server.handleSessionSummary(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestHandleSessionSummaryReturnsStoredSummary(t *testing.T) {
	store := &historyStore{
		summary: storage.SessionSummary{
			Session: storage.SessionHistory{
				SessionID:  "abc-1",
				ChannelID:  "abc",
				Status:     "ingesting",
				StartedAt:  time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
				LabelCount: 2,
			},
			LabelCount: 2,
		},
	}
	server := &server{cfg: appConfig{DatabaseWriteTimeout: time.Second}, store: store}
	request := httptest.NewRequest(http.MethodGet, "/sessions/abc-1/summary", nil)
	request.SetPathValue("session_id", "abc-1")
	response := httptest.NewRecorder()

	server.handleSessionSummary(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var body sessionSummaryResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Session.SessionID != "abc-1" || body.LabelCount != 2 {
		t.Fatalf("unexpected summary: %#v", body)
	}
}

func TestHandleSessionReplayReturnsOrderedBucketsAndSignalWindows(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store := &historyStore{
		replay: storage.SessionReplay{
			Session: storage.SessionHistory{
				SessionID:             "abc-1",
				ChannelID:             "abc",
				Status:                "ended",
				StartedAt:             start,
				ChatBucketCount:       2,
				TranscriptBucketCount: 1,
				AlignmentCount:        1,
				LabelCount:            4,
			},
			ChatBuckets: []chat.ChatBucket{
				{
					SessionID:     "abc-1",
					ChannelID:     "abc",
					BucketStart:   start,
					BucketEnd:     start.Add(30 * time.Second),
					MessageCount:  10,
					ChatSentiment: -0.2,
				},
				{
					SessionID:     "abc-1",
					ChannelID:     "abc",
					BucketStart:   start.Add(30 * time.Second),
					BucketEnd:     start.Add(60 * time.Second),
					MessageCount:  30,
					ChatSentiment: 0.5,
					PositiveRatio: 0.7,
				},
			},
			Alignments: []storage.AlignmentBucket{
				{
					SessionID:             "abc-1",
					ChannelID:             "abc",
					WindowStart:           start.Add(30 * time.Second),
					WindowEnd:             start.Add(60 * time.Second),
					ChatBucketStart:       start.Add(30 * time.Second),
					ChatBucketEnd:         start.Add(60 * time.Second),
					TranscriptBucketStart: start.Add(30 * time.Second),
					TranscriptBucketEnd:   start.Add(60 * time.Second),
					ChatSentiment:         0.5,
					ChatConfidence:        0.8,
					ChatMessageCount:      30,
					TranscriptSentiment:   -0.2,
					TranscriptConfidence:  0.9,
					Delta:                 0.7,
					Similarity:            0.65,
					Relationship:          "diverged",
					Quality:               0.9,
				},
			},
			TranscriptBuckets: []storage.TranscriptBucket{
				{
					SessionID:            "abc-1",
					ChannelID:            "abc",
					BucketStart:          start.Add(60 * time.Second),
					BucketEnd:            start.Add(90 * time.Second),
					TranscriptConfidence: 0.1,
				},
			},
			LabelCount: 1,
		},
	}
	server := &server{cfg: appConfig{DatabaseWriteTimeout: time.Second}, store: store}
	request := httptest.NewRequest(http.MethodGet, "/sessions/abc-1/replay", nil)
	request.SetPathValue("session_id", "abc-1")
	response := httptest.NewRecorder()

	server.handleSessionReplay(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var body sessionReplayResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.ChatBuckets) != 2 {
		t.Fatalf("expected replay chat buckets, got %#v", body.ChatBuckets)
	}
	if len(body.SignalWindows) != 2 {
		t.Fatalf("expected signal windows, got %#v", body.SignalWindows)
	}
	if len(body.SignalEvents) < 2 {
		t.Fatalf("expected generated signal events, got %#v", body.SignalEvents)
	}
	if len(body.AgentReviews) != 2 || body.AgentReviews[1].SuggestedEventLabel != "hype_spike" || body.AgentReviews[1].Reason == "" {
		t.Fatalf("expected fallback agent pre-scores with reasons, got %#v", body.AgentReviews)
	}
	for _, review := range body.AgentReviews {
		if len(review.Evidence) == 0 {
			t.Fatalf("agent review missing evidence: %#v", review)
		}
	}
	if len(body.Insights) < 3 {
		t.Fatalf("expected generated replay insights, got %#v", body.Insights)
	}
	if body.InsightSummary.Type != "session_insight_summary" || body.InsightSummary.InsightCount != len(body.Insights) {
		t.Fatalf("unexpected insight summary: %#v", body.InsightSummary)
	}
	if body.InsightSummary.TranscriptBucketCount != 1 {
		t.Fatalf("expected transcript bucket count in summary, got %#v", body.InsightSummary)
	}
}

func TestHandleSessionEvaluationReturnsWindowQualityMetrics(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store := &historyStore{
		replay: storage.SessionReplay{
			Session: storage.SessionHistory{
				SessionID:       "abc-1",
				ChannelID:       "abc",
				Status:          "ended",
				StartedAt:       start,
				ChatBucketCount: 2,
				LabelCount:      2,
			},
			ChatBuckets: []chat.ChatBucket{
				{
					SessionID:     "abc-1",
					ChannelID:     "abc",
					BucketStart:   start,
					BucketEnd:     start.Add(30 * time.Second),
					MessageCount:  20,
					ChatSentiment: 0.6,
					PositiveRatio: 0.8,
				},
				{
					SessionID:     "abc-1",
					ChannelID:     "abc",
					BucketStart:   start.Add(30 * time.Second),
					BucketEnd:     start.Add(60 * time.Second),
					MessageCount:  20,
					ChatSentiment: 0.3,
					PositiveRatio: 0.3,
					NeutralRatio:  0.7,
				},
			},
			WindowLabels: []storage.SignalWindowLabel{
				{
					SessionID:      "abc-1",
					WindowStart:    start,
					WindowEnd:      start.Add(30 * time.Second),
					PredictedEvent: "hype_spike",
					Correctness:    "correct",
					EventLabel:     "hype_spike",
				},
				{
					SessionID:      "abc-1",
					WindowStart:    start.Add(30 * time.Second),
					WindowEnd:      start.Add(60 * time.Second),
					PredictedEvent: "none",
					Correctness:    "correct",
					EventLabel:     "none",
				},
			},
			LabelCount: 2,
		},
	}
	server := &server{cfg: appConfig{DatabaseWriteTimeout: time.Second}, store: store}
	request := httptest.NewRequest(http.MethodGet, "/sessions/abc-1/evaluation", nil)
	request.SetPathValue("session_id", "abc-1")
	response := httptest.NewRecorder()

	server.handleSessionEvaluation(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var body sessionEvaluationResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Session.SessionID != "abc-1" || body.Partial {
		t.Fatalf("unexpected evaluation response metadata: %#v", body)
	}
	if body.Evaluation.TotalWindows != 2 || body.Evaluation.TotalLabeledWindows != 2 || body.Evaluation.EvaluatedWindows != 2 {
		t.Fatalf("unexpected evaluation counts: %#v", body.Evaluation)
	}
	if body.Evaluation.Coverage == nil || *body.Evaluation.Coverage != 1 {
		t.Fatalf("coverage = %v, want 1", body.Evaluation.Coverage)
	}
	if body.Evaluation.EventAccuracy == nil || *body.Evaluation.EventAccuracy != 1 {
		t.Fatalf("event accuracy = %v, want 1", body.Evaluation.EventAccuracy)
	}
	if body.Evaluation.EventCounts.TruePositive != 1 || body.Evaluation.EventCounts.TrueNegative != 1 {
		t.Fatalf("unexpected event counts: %#v", body.Evaluation.EventCounts)
	}
	if body.Evaluation.RelationshipAccuracy != nil {
		t.Fatalf("relationship accuracy should be nil without human relationship labels: %#v", body.Evaluation)
	}
}

func TestHandleSessionProofReturnsReplayBenchmarkMetrics(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	transcriptLatency := int64(125)
	store := &historyStore{
		replay: storage.SessionReplay{
			Session: storage.SessionHistory{
				SessionID:  "abc-1",
				ChannelID:  "abc",
				Status:     "ended",
				StartedAt:  start,
				LabelCount: 1,
			},
			ChatBuckets: []chat.ChatBucket{
				{
					SessionID:         "abc-1",
					ChannelID:         "abc",
					BucketStart:       start,
					BucketEnd:         start.Add(30 * time.Second),
					MessageCount:      10,
					AnalysisLatencyMS: 40,
					AnalysisStatus:    "python",
				},
				{
					SessionID:   "abc-1",
					ChannelID:   "abc",
					BucketStart: start.Add(30 * time.Second),
					BucketEnd:   start.Add(60 * time.Second),
				},
			},
			TranscriptBuckets: []storage.TranscriptBucket{
				{
					SessionID:          "abc-1",
					ChannelID:          "abc",
					BucketStart:        start,
					BucketEnd:          start.Add(30 * time.Second),
					SentimentLatencyMS: &transcriptLatency,
					SentimentStatus:    "python",
				},
			},
			Alignments: []storage.AlignmentBucket{
				{
					SessionID:             "abc-1",
					ChannelID:             "abc",
					WindowStart:           start,
					WindowEnd:             start.Add(30 * time.Second),
					ChatBucketStart:       start,
					ChatBucketEnd:         start.Add(30 * time.Second),
					TranscriptBucketStart: start,
					TranscriptBucketEnd:   start.Add(30 * time.Second),
				},
			},
			WindowLabels: []storage.SignalWindowLabel{
				{
					SessionID:   "abc-1",
					WindowStart: start,
					WindowEnd:   start.Add(30 * time.Second),
					Correctness: "correct",
					EventLabel:  "hype_spike",
				},
			},
			LabelCount: 4,
		},
	}
	server := &server{cfg: appConfig{DatabaseWriteTimeout: time.Second}, store: store}
	request := httptest.NewRequest(http.MethodGet, "/sessions/abc-1/proof?limit=9999", nil)
	request.SetPathValue("session_id", "abc-1")
	response := httptest.NewRecorder()

	server.handleSessionProof(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var body sessionProofResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Persisted {
		t.Fatalf("GET proof should not persist metrics")
	}
	if body.Proof.BucketCount != 4 || body.Proof.SourceBucketCount != 3 || body.Proof.SignalWindowCount != 2 || body.Proof.MatchedWindows != 1 {
		t.Fatalf("unexpected proof counts: %#v", body.Proof)
	}
	if body.Proof.ReplayLimit != 500 || body.Proof.Partial {
		t.Fatalf("unexpected proof limit/truncation metadata: %#v", body.Proof)
	}
	if body.Proof.LabelCoverage.Coverage == nil || *body.Proof.LabelCoverage.Coverage != 0.5 {
		t.Fatalf("label coverage = %v, want 0.5", body.Proof.LabelCoverage.Coverage)
	}
	if body.Proof.LabelCoverage.StoredLabelCount != 1 {
		t.Fatalf("stored label count = %d, want signal-window labels only", body.Proof.LabelCoverage.StoredLabelCount)
	}
	if len(body.Proof.Speeds) != 3 || body.Proof.Speeds[0].Speed != 1 || body.Proof.Speeds[1].Speed != 5 || body.Proof.Speeds[2].Speed != 10 {
		t.Fatalf("unexpected speed metrics: %#v", body.Proof.Speeds)
	}
	if body.Proof.DroppedEventRate != nil || len(body.Proof.UnsupportedMetrics) == 0 {
		t.Fatalf("dropped event rate should be null and marked unsupported: %#v", body.Proof)
	}
	if len(store.metrics) != 0 {
		t.Fatalf("GET proof persisted %d metrics, want 0", len(store.metrics))
	}
}

func TestHandleSessionProofPostRejectsPartialProof(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store := &historyStore{
		replay: storage.SessionReplay{
			Session: storage.SessionHistory{
				SessionID:             "abc-1",
				ChannelID:             "abc",
				StartedAt:             start,
				ChatBucketCount:       2,
				TranscriptBucketCount: 1,
				AlignmentCount:        0,
			},
			ChatBuckets: []chat.ChatBucket{
				{
					SessionID:   "abc-1",
					ChannelID:   "abc",
					BucketStart: start,
					BucketEnd:   start.Add(30 * time.Second),
				},
			},
		},
	}
	server := &server{cfg: appConfig{DatabaseWriteTimeout: time.Second}, store: store}
	request := httptest.NewRequest(http.MethodPost, "/sessions/abc-1/proof?limit=1", nil)
	request.SetPathValue("session_id", "abc-1")
	response := httptest.NewRecorder()

	server.handleSessionProofPersist(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusConflict, response.Body.String())
	}
	var body sessionProofResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.Proof.Partial || body.Persisted || body.Error == "" {
		t.Fatalf("expected rejected partial proof response, got %#v", body)
	}
	if len(store.metrics) != 0 {
		t.Fatalf("partial POST persisted %d metrics, want 0", len(store.metrics))
	}
}

func TestHandleSessionProofPostPersistsSystemMetrics(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	asrLatency := int64(90)
	pipelineLatency := int64(140)
	store := &historyStore{
		replay: storage.SessionReplay{
			Session: storage.SessionHistory{
				SessionID: "abc-1",
				ChannelID: "abc",
				StartedAt: start,
			},
			ChatBuckets: []chat.ChatBucket{
				{
					SessionID:         "abc-1",
					ChannelID:         "abc",
					BucketStart:       start,
					BucketEnd:         start.Add(30 * time.Second),
					AnalysisLatencyMS: 80,
					AnalysisStatus:    "python",
				},
			},
			TranscriptBuckets: []storage.TranscriptBucket{
				{
					SessionID:          "abc-1",
					ChannelID:          "abc",
					BucketStart:        start,
					BucketEnd:          start.Add(30 * time.Second),
					ASRLatencyMS:       &asrLatency,
					PipelineLatencyMS:  &pipelineLatency,
					TranscriptStatus:   "final",
					AudioSeconds:       27,
					SegmentCount:       2,
					WordCount:          8,
					EmptyRatio:         0.1,
					RepairAddedWords:   3,
					RepairChangedRatio: 0.25,
				},
			},
			LabelCount: 0,
		},
	}
	server := &server{cfg: appConfig{DatabaseWriteTimeout: time.Second}, store: store}
	request := httptest.NewRequest(http.MethodPost, "/sessions/abc-1/proof", nil)
	request.SetPathValue("session_id", "abc-1")
	response := httptest.NewRecorder()

	server.handleSessionProofPersist(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var body sessionProofResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.Persisted {
		t.Fatalf("POST proof response should report persisted")
	}
	for _, name := range []string{
		"replay_proof.bucket_count",
		"replay_proof.signal_window_count",
		"replay_proof.matched_windows",
		"replay_proof.chat_analysis_latency_ms",
		"replay_proof.transcript_asr_latency_ms",
		"replay_proof.transcript_pipeline_latency_ms",
		"replay_proof.transcript_audio_coverage",
		"replay_proof.transcript_empty_ratio",
		"replay_proof.transcript_word_count",
		"replay_proof.transcript_repair_added_words",
		"replay_proof.transcript_repair_changed_ratio",
		"replay_proof.transcript_repair_improvement",
		"replay_proof.estimated_replay_seconds",
		"replay_proof.windows_per_second",
	} {
		if !hasMetric(store.metrics, name) {
			t.Fatalf("missing persisted metric %q in %#v", name, store.metrics)
		}
	}
	for _, metric := range store.metrics {
		if metric.SessionID != "abc-1" || metric.RecordedAt.IsZero() {
			t.Fatalf("unexpected persisted metric metadata: %#v", metric)
		}
	}
	audioCoverage := metricByName(store.metrics, "replay_proof.transcript_audio_coverage")
	if audioCoverage == nil || audioCoverage.Value != 0.9 || audioCoverage.Meta["transcript_coverage_status_counts"] == nil || audioCoverage.Meta["transcript_status_counts"] == nil {
		t.Fatalf("transcript coverage metric missing status metadata: %#v", audioCoverage)
	}
	repairChanged := metricByName(store.metrics, "replay_proof.transcript_repair_changed_ratio")
	if repairChanged == nil || repairChanged.Value != 0.25 || repairChanged.Meta["repair_added_words"] != 3 {
		t.Fatalf("repair changed metric not persisted with expected metadata: %#v", repairChanged)
	}
}

func TestSignalWindowLabelFromRequestValidatesAndNormalizes(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	record, err := signalWindowLabelFromRequest(signalWindowLabelRequest{
		SessionID:             " abc-1 ",
		WindowStart:           start.Format(time.RFC3339),
		WindowEnd:             start.Add(30 * time.Second).Format(time.RFC3339),
		PredictedEvent:        " HYPE_SPIKE ",
		PredictedRelationship: " DIVERGED ",
		Correctness:           "CORRECT",
		EventLabel:            "hype_spike",
		ReactionType:          " HYPE ",
		TargetType:            " CHARACTER ",
		TargetText:            " Dragon ",
		DivergenceType:        " DIVERGED ",
		EventStart:            start.Add(2 * time.Second).Format(time.RFC3339),
		EventPeak:             start.Add(5 * time.Second).Format(time.RFC3339),
		Notes:                 " strong chat evidence ",
	})
	if err != nil {
		t.Fatalf("signalWindowLabelFromRequest error = %v", err)
	}
	if record.SessionID != "abc-1" || record.PredictedEvent != "hype_spike" || record.Correctness != "correct" || record.Notes != "strong chat evidence" {
		t.Fatalf("unexpected normalized record: %#v", record)
	}
	if record.ReactionType != "hype" || record.TargetType != "character" || record.TargetText != "Dragon" || record.DivergenceType != "diverged" {
		t.Fatalf("unexpected normalized context fields: %#v", record)
	}
	if !record.EventStart.Equal(start.Add(2*time.Second)) || !record.EventPeak.Equal(start.Add(5*time.Second)) {
		t.Fatalf("unexpected event timing fields: %#v", record)
	}

	noneRecord, err := signalWindowLabelFromRequest(signalWindowLabelRequest{
		SessionID:   "abc-1",
		WindowStart: start.Format(time.RFC3339),
		WindowEnd:   start.Add(30 * time.Second).Format(time.RFC3339),
		Correctness: "wrong",
		EventLabel:  " none ",
	})
	if err != nil {
		t.Fatalf("explicit none event_label should be accepted: %v", err)
	}
	if noneRecord.EventLabel != "none" {
		t.Fatalf("explicit none event_label normalized to %q, want none", noneRecord.EventLabel)
	}

	if _, err := signalWindowLabelFromRequest(signalWindowLabelRequest{
		SessionID:   "abc-1",
		WindowStart: start.Format(time.RFC3339),
		WindowEnd:   start.Add(30 * time.Second).Format(time.RFC3339),
		Correctness: "maybe",
		EventLabel:  "hype_spike",
	}); err == nil {
		t.Fatalf("expected invalid correctness error")
	}

	if _, err := signalWindowLabelFromRequest(signalWindowLabelRequest{
		SessionID:   "abc-1",
		WindowStart: start.Format(time.RFC3339),
		WindowEnd:   start.Add(30 * time.Second).Format(time.RFC3339),
		Correctness: "correct",
		EventLabel:  " ",
	}); err == nil {
		t.Fatalf("expected blank event_label error")
	}

	if _, err := signalWindowLabelFromRequest(signalWindowLabelRequest{
		SessionID:   "abc-1",
		WindowStart: start.Format(time.RFC3339),
		WindowEnd:   start.Add(30 * time.Second).Format(time.RFC3339),
		Correctness: "correct",
		EventLabel:  "hype_spike",
		EventPeak:   "not-a-time",
	}); err == nil {
		t.Fatalf("expected invalid event_peak error")
	}
}

func hasMetric(metrics []storage.SystemMetric, name string) bool {
	return metricByName(metrics, name) != nil
}

func metricByName(metrics []storage.SystemMetric, name string) *storage.SystemMetric {
	for _, metric := range metrics {
		if metric.Name == name {
			return &metric
		}
	}
	return nil
}
