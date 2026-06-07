package regression

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

type fakeReplayStore struct {
	replays map[string]storage.SessionReplay
}

func (s fakeReplayStore) GetSessionReplay(_ context.Context, sessionID string, _ int) (storage.SessionReplay, error) {
	replay, ok := s.replays[sessionID]
	if !ok {
		return storage.SessionReplay{}, storage.ErrNotFound
	}
	return replay, nil
}

func TestRunnerBuildsProofEvaluationAndMetricSummary(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	generatedAt := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	replay := regressionTestReplay(start)
	store := fakeReplayStore{replays: map[string]storage.SessionReplay{"session-1": replay}}

	report, err := Runner{Store: store}.Run(context.Background(), Config{
		SessionIDs:  []string{"session-1"},
		ReplayLimit: 500,
		GeneratedAt: generatedAt,
	})
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if report.Type != ReportType || !report.GeneratedAt.Equal(generatedAt) || len(report.Sessions) != 1 {
		t.Fatalf("unexpected report metadata: %#v", report)
	}

	session := report.Sessions[0]
	if session.Proof.Type != storage.ReplayProofType || session.Evaluation.Type != "session_evaluation" {
		t.Fatalf("runner should use proof/evaluation APIs: proof=%s evaluation=%s", session.Proof.Type, session.Evaluation.Type)
	}
	if session.ReplayCounts.ChatBuckets != 2 || session.ReplayCounts.SignalWindows != 2 {
		t.Fatalf("unexpected replay counts: %#v", session.ReplayCounts)
	}
	if session.Partial || session.Metrics.Partial {
		t.Fatalf("replay should be complete: %#v", session.Metrics)
	}
	if session.Metrics.ProofLabelCoverage == nil || *session.Metrics.ProofLabelCoverage != 1 {
		t.Fatalf("proof label coverage = %v, want 1", session.Metrics.ProofLabelCoverage)
	}
	if session.Metrics.EventPrecision == nil || *session.Metrics.EventPrecision != 1 {
		t.Fatalf("event precision = %v, want 1", session.Metrics.EventPrecision)
	}
	if session.Metrics.EventRecall == nil || *session.Metrics.EventRecall != 1 {
		t.Fatalf("event recall = %v, want 1", session.Metrics.EventRecall)
	}
	if session.Metrics.EventF1 == nil || *session.Metrics.EventF1 != 1 {
		t.Fatalf("event f1 = %v, want 1", session.Metrics.EventF1)
	}
	if session.Metrics.PeakRecall == nil || *session.Metrics.PeakRecall != 1 {
		t.Fatalf("peak recall = %v, want 1", session.Metrics.PeakRecall)
	}
	if session.Metrics.ReactionTypeF1 == nil || *session.Metrics.ReactionTypeF1 != 1 {
		t.Fatalf("reaction type f1 = %v, want 1", session.Metrics.ReactionTypeF1)
	}
	if session.Metrics.TargetAccuracy == nil || *session.Metrics.TargetAccuracy != 1 {
		t.Fatalf("target accuracy = %v, want 1", session.Metrics.TargetAccuracy)
	}
	if session.Metrics.ReactionWindowCount != 1 || session.Metrics.PeakReactionCount != 1 {
		t.Fatalf("reaction proof counts = windows %d peaks %d, want 1/1", session.Metrics.ReactionWindowCount, session.Metrics.PeakReactionCount)
	}
	if session.Metrics.TranscriptBucketCoverage == nil || *session.Metrics.TranscriptBucketCoverage != 1 || session.Metrics.TranscriptCoverage == nil || *session.Metrics.TranscriptCoverage != 0.8 {
		t.Fatalf("transcript coverage = %v, want 1", session.Metrics.TranscriptBucketCoverage)
	}
	if session.Metrics.TranscriptAudioCoverage == nil || *session.Metrics.TranscriptAudioCoverage != 0.8 || session.Metrics.TranscriptEmptyRatio == nil || *session.Metrics.TranscriptEmptyRatio != 0.2 {
		t.Fatalf("transcript completeness metrics not summarized: %#v", session.Metrics)
	}
	if session.Metrics.TranscriptWordCount != 5 || session.Metrics.TranscriptRepairAddedWords != 1 || session.Metrics.TranscriptRepairChangedRatio == nil || *session.Metrics.TranscriptRepairChangedRatio != 0.25 {
		t.Fatalf("transcript repair metrics not summarized: %#v", session.Metrics)
	}
	if session.Metrics.TranscriptRepairImprovement == nil || *session.Metrics.TranscriptRepairImprovement != 0.25 {
		t.Fatalf("transcript repair improvement = %v, want 0.25", session.Metrics.TranscriptRepairImprovement)
	}
	if session.Metrics.PersistenceMetricCount != 4 || session.Metrics.StorageWriteFailures == nil || *session.Metrics.StorageWriteFailures != 0 {
		t.Fatalf("persistence metrics not summarized: %#v", session.Metrics)
	}
	if session.Metrics.ReactionWindowMetricCount == nil || *session.Metrics.ReactionWindowMetricCount != 1 {
		t.Fatalf("reaction window metric count = %v, want 1", session.Metrics.ReactionWindowMetricCount)
	}
	if session.Metrics.LatencyP95MS["chat_analysis"] == nil || *session.Metrics.LatencyP95MS["chat_analysis"] != 20 {
		t.Fatalf("chat p95 latency = %#v, want 20", session.Metrics.LatencyP95MS)
	}
}

func TestBuildSessionResultEvaluatesReactionSubwindowLabels(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 123456789, time.UTC)
	replay := storage.SessionReplay{
		Session: storage.SessionHistory{
			SessionID:  "session-1",
			ChannelID:  "channel-1",
			StartedAt:  start,
			LabelCount: 1,
		},
		ChatBuckets: []chat.ChatBucket{
			{
				SessionID:     "session-1",
				ChannelID:     "channel-1",
				BucketStart:   start,
				BucketEnd:     start.Add(30 * time.Second),
				MessageCount:  8,
				ChatSentiment: 0.1,
				Subwindows: []chat.ReactionSubwindow{
					{
						WindowStart:   start.Add(2 * time.Second),
						WindowEnd:     start.Add(7 * time.Second),
						MessageCount:  5,
						ReactionScore: 0.62,
						ReactionType:  "confusion",
						TargetType:    "unknown",
						Source:        "chat",
						Confidence:    0.7,
					},
				},
			},
		},
		WindowLabels: []storage.SignalWindowLabel{
			{
				SessionID:      "session-1",
				WindowStart:    start.Add(2 * time.Second).Round(time.Microsecond),
				WindowEnd:      start.Add(7 * time.Second).Round(time.Microsecond),
				PredictedEvent: "audience_shift",
				ReactionType:   "confusion",
				TargetType:     "unknown",
				Correctness:    "correct",
				EventLabel:     "audience_shift",
			},
		},
		LabelCount: 1,
	}

	result := BuildSessionResult(replay, SessionResultOptions{GeneratedAt: start.Add(time.Hour)})

	if result.Evaluation.EvaluatedWindows != 1 || result.Evaluation.UnmatchedLabels != 0 {
		t.Fatalf("reaction subwindow label should be evaluated: %#v", result.Evaluation)
	}
	if result.Evaluation.EventAccuracy == nil || *result.Evaluation.EventAccuracy != 1 {
		t.Fatalf("reaction subwindow event accuracy = %v, want 1", result.Evaluation.EventAccuracy)
	}
	if result.Evaluation.ReactionTypeAccuracy == nil || *result.Evaluation.ReactionTypeAccuracy != 1 {
		t.Fatalf("reaction type accuracy = %v, want 1", result.Evaluation.ReactionTypeAccuracy)
	}
}

func TestCompareReportsFailsOnMetricDropsPartialAndLatencyIncrease(t *testing.T) {
	baseline := Report{Sessions: []SessionResult{
		{
			SessionID: "session-1",
			Metrics: MetricSummary{
				ProofLabelCoverage:           floatPtr(0.9),
				EvaluationCoverage:           floatPtr(0.8),
				EventPrecision:               floatPtr(0.7),
				EventRecall:                  floatPtr(0.6),
				EventF1:                      floatPtr(0.65),
				PeakRecall:                   floatPtr(0.8),
				HypePeakRecall:               floatPtr(0.8),
				OnsetLatencyMS:               floatPtr(2000),
				ReactionTypeF1:               floatPtr(0.9),
				ReactionTypeAccuracy:         floatPtr(0.9),
				TargetAccuracy:               floatPtr(0.9),
				ReactionWindowCount:          3,
				PeakReactionCount:            2,
				AlignmentCount:               2,
				MatchedWindowCount:           2,
				TotalWindows:                 4,
				TotalLabeledWindows:          4,
				EvaluatedWindows:             4,
				UnmatchedLabels:              0,
				FalsePositivesNormalChat:     0,
				PersistenceMetricCount:       3,
				TranscriptCoverage:           floatPtr(0.95),
				TranscriptAudioCoverage:      floatPtr(0.95),
				TranscriptEmptyRatio:         floatPtr(0.1),
				TranscriptWordCount:          100,
				TranscriptRepairAddedWords:   1,
				TranscriptRepairChangedRatio: floatPtr(0.1),
				TranscriptRepairImprovement:  floatPtr(0.2),
				StorageWriteFailures:         floatPtr(0),
				StorageQueueDepth:            floatPtr(0),
				ReactionWindowMetricCount:    floatPtr(3),
				LatencyP95MS:                 map[string]*float64{"chat_analysis": floatPtr(20)},
				TruncatedSourceCount:         0,
			},
		},
	}}
	current := Report{Sessions: []SessionResult{
		{
			SessionID: "session-1",
			Metrics: MetricSummary{
				Partial:                      true,
				ProofLabelCoverage:           floatPtr(0.7),
				EvaluationCoverage:           floatPtr(0.79),
				EventPrecision:               floatPtr(0.69),
				EventRecall:                  floatPtr(0.6),
				EventF1:                      floatPtr(0.5),
				PeakRecall:                   floatPtr(0.7),
				HypePeakRecall:               floatPtr(0.7),
				OnsetLatencyMS:               floatPtr(4000),
				ReactionTypeF1:               floatPtr(0.8),
				ReactionTypeAccuracy:         floatPtr(0.8),
				TargetAccuracy:               floatPtr(0.8),
				ReactionWindowCount:          2,
				PeakReactionCount:            1,
				AlignmentCount:               1,
				MatchedWindowCount:           1,
				TotalWindows:                 3,
				TotalLabeledWindows:          3,
				EvaluatedWindows:             3,
				UnmatchedLabels:              1,
				FalsePositivesNormalChat:     1,
				PersistenceMetricCount:       2,
				TranscriptCoverage:           floatPtr(0.9),
				TranscriptAudioCoverage:      floatPtr(0.9),
				TranscriptEmptyRatio:         floatPtr(0.2),
				TranscriptWordCount:          90,
				TranscriptRepairAddedWords:   3,
				TranscriptRepairChangedRatio: floatPtr(0.2),
				TranscriptRepairImprovement:  floatPtr(0.1),
				StorageWriteFailures:         floatPtr(1),
				StorageQueueDepth:            floatPtr(1),
				ReactionWindowMetricCount:    floatPtr(2),
				LatencyP95MS:                 map[string]*float64{"chat_analysis": floatPtr(27)},
				TruncatedSourceCount:         1,
			},
		},
	}}

	comparison := CompareReports(current, baseline, Thresholds{
		MaxProofLabelCoverageDrop: 0.05,
		MaxEvaluationCoverageDrop: 0.05,
		MaxPrecisionDrop:          0.05,
		MaxRecallDrop:             0.05,
		MaxF1Drop:                 0.05,
		MaxLatencyP95IncreaseMS:   5,
	})

	if comparison.Passed {
		t.Fatalf("comparison should fail: %#v", comparison)
	}
	for _, metric := range []string{"partial", "truncated_source_count", "proof_label_coverage", "event_f1", "peak_recall", "hype_peak_recall", "onset_latency_ms", "reaction_type_f1", "reaction_type_accuracy", "target_accuracy", "reaction_window_count", "peak_reaction_count", "transcript_coverage", "transcript_audio_coverage", "transcript_empty_ratio", "transcript_word_count", "transcript_repair_improvement", "transcript_repair_added_words", "transcript_repair_changed_ratio", "alignment_count", "matched_window_count", "persistence_metric_count", "storage_write_failures", "storage_queue_depth", "reaction_window_metric_count", "total_windows", "total_labeled_windows", "evaluated_windows", "unmatched_labels", "false_positives_normal_chat", "latency_p95_ms.chat_analysis"} {
		if !hasRegression(comparison, metric) {
			t.Fatalf("expected regression for %s, got %#v", metric, comparison.Regressions)
		}
	}
	if hasRegression(comparison, "evaluation_coverage") || hasRegression(comparison, "event_precision") || hasRegression(comparison, "event_recall") {
		t.Fatalf("metrics within threshold should not fail: %#v", comparison.Regressions)
	}
}

func TestWriteTableIncludesSessionMetricsAndRegressionCounts(t *testing.T) {
	report := Report{
		Sessions: []SessionResult{
			{
				SessionID: "session-1",
				Metrics: MetricSummary{
					EvaluatedWindows:   2,
					TotalWindows:       3,
					ProofLabelCoverage: floatPtr(0.5),
					EvaluationCoverage: floatPtr(0.6667),
					EventPrecision:     floatPtr(0.75),
					EventRecall:        floatPtr(0.6),
					EventF1:            floatPtr(0.6667),
					LatencyP95MS:       map[string]*float64{"chat_analysis": floatPtr(42)},
				},
			},
		},
		Comparison: &Comparison{
			Regressions: []Regression{{SessionID: "session-1", Metric: "event_f1", Message: "event_f1 dropped"}},
		},
	}

	var out bytes.Buffer
	if err := WriteTable(&out, report); err != nil {
		t.Fatalf("WriteTable error = %v", err)
	}
	text := out.String()
	for _, want := range []string{"SESSION", "session-1", "2/3", "0.6667", "42ms", "event_f1 dropped"} {
		if !strings.Contains(text, want) {
			t.Fatalf("table missing %q:\n%s", want, text)
		}
	}
}

func regressionTestReplay(start time.Time) storage.SessionReplay {
	return storage.SessionReplay{
		Session: storage.SessionHistory{
			SessionID:             "session-1",
			ChannelID:             "channel-1",
			StartedAt:             start,
			ChatBucketCount:       2,
			TranscriptBucketCount: 1,
			LabelCount:            3,
		},
		ChatBuckets: []chat.ChatBucket{
			{
				SessionID:         "session-1",
				ChannelID:         "channel-1",
				BucketStart:       start,
				BucketEnd:         start.Add(30 * time.Second),
				MessageCount:      20,
				ChatSentiment:     0.4,
				PositiveRatio:     0.8,
				AnalysisLatencyMS: 10,
				AnalysisStatus:    "python",
				PeakReactionScore: floatPtr(0.84),
				PeakReactionType:  "hype",
				PeakWindowStart:   timePtr(start.Add(5 * time.Second)),
				PeakWindowEnd:     timePtr(start.Add(10 * time.Second)),
				Subwindows: []chat.ReactionSubwindow{
					{
						WindowStart:   start.Add(5 * time.Second),
						WindowEnd:     start.Add(10 * time.Second),
						MessageCount:  9,
						ReactionScore: 0.84,
						ReactionType:  "hype",
						TargetType:    "unknown",
						Source:        "chat",
						Confidence:    0.8,
					},
				},
			},
			{
				SessionID:         "session-1",
				ChannelID:         "channel-1",
				BucketStart:       start.Add(30 * time.Second),
				BucketEnd:         start.Add(60 * time.Second),
				MessageCount:      20,
				ChatSentiment:     0.1,
				NeutralRatio:      1,
				AnalysisLatencyMS: 20,
				AnalysisStatus:    "python",
			},
		},
		TranscriptBuckets: []storage.TranscriptBucket{
			{
				SessionID:            "session-1",
				ChannelID:            "channel-1",
				BucketStart:          start,
				BucketEnd:            start.Add(30 * time.Second),
				Text:                 "That was a surprising phase change.",
				TranscriptConfidence: 0.9,
				TranscriptStatus:     "final",
				AudioSeconds:         24,
				SegmentCount:         2,
				WordCount:            5,
				EmptyRatio:           0.2,
				RepairAddedWords:     1,
				RepairChangedRatio:   0.25,
			},
		},
		WindowLabels: []storage.SignalWindowLabel{
			{
				SessionID:    "session-1",
				WindowStart:  start,
				WindowEnd:    start.Add(30 * time.Second),
				ReactionType: "hype",
				TargetType:   "unknown",
				Correctness:  "correct",
				EventLabel:   "hype_spike",
			},
			{
				SessionID:    "session-1",
				WindowStart:  start.Add(5 * time.Second),
				WindowEnd:    start.Add(10 * time.Second),
				ReactionType: "hype",
				TargetType:   "unknown",
				Correctness:  "correct",
				EventLabel:   "hype_spike",
			},
			{
				SessionID:   "session-1",
				WindowStart: start.Add(30 * time.Second),
				WindowEnd:   start.Add(60 * time.Second),
				Correctness: "correct",
				EventLabel:  "none",
			},
		},
		SystemMetrics: []storage.SystemMetric{
			{SessionID: "session-1", Name: "storage.write_failures", Value: 0, RecordedAt: start.Add(time.Minute)},
			{SessionID: "session-1", Name: "storage.dropped_writes", Value: 0, RecordedAt: start.Add(time.Minute)},
			{SessionID: "session-1", Name: "storage.queue_depth", Value: 0, RecordedAt: start.Add(time.Minute)},
			{SessionID: "session-1", Name: "reaction_window.count", Value: 1, RecordedAt: start.Add(time.Minute)},
		},
		LabelCount: 3,
	}
}

func timePtr(value time.Time) *time.Time {
	return &value
}

func hasRegression(comparison Comparison, metric string) bool {
	for _, regression := range comparison.Regressions {
		if regression.Metric == metric {
			return true
		}
	}
	return false
}
