package storage

import (
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestBuildReplayProofCalculatesDeterministicMetrics(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	generatedAt := time.Date(2026, 5, 1, 13, 0, 0, 0, time.UTC)
	transcriptLatency := int64(200)
	asrLatency := int64(940)
	pipelineLatency1 := int64(1200)
	pipelineLatency2 := int64(1800)

	replay := SessionReplay{
		Session: SessionHistory{
			SessionID:             "session-1",
			ChannelID:             "channel-1",
			StartedAt:             start,
			ChatBucketCount:       3,
			TranscriptBucketCount: 2,
			AlignmentCount:        2,
			LabelCount:            4,
		},
		ChatBuckets: []chat.ChatBucket{
			{
				SessionID:         "session-1",
				ChannelID:         "channel-1",
				BucketStart:       start,
				BucketEnd:         start.Add(30 * time.Second),
				AnalysisLatencyMS: 50,
				AnalysisStatus:    "python",
			},
			{
				SessionID:      "session-1",
				ChannelID:      "channel-1",
				BucketStart:    start.Add(30 * time.Second),
				BucketEnd:      start.Add(60 * time.Second),
				AnalysisStatus: "fallback",
			},
			{
				SessionID:         "session-1",
				ChannelID:         "channel-1",
				BucketStart:       start.Add(60 * time.Second),
				BucketEnd:         start.Add(90 * time.Second),
				AnalysisLatencyMS: 100,
				AnalysisStatus:    "python",
			},
		},
		TranscriptBuckets: []TranscriptBucket{
			{
				SessionID:          "session-1",
				ChannelID:          "channel-1",
				BucketStart:        start,
				BucketEnd:          start.Add(30 * time.Second),
				SentimentLatencyMS: &transcriptLatency,
				SentimentStatus:    "python",
				ASRLatencyMS:       &asrLatency,
				PipelineLatencyMS:  &pipelineLatency1,
				TranscriptStatus:   "repairing",
				AudioSeconds:       30,
				SegmentCount:       2,
				WordCount:          8,
				EmptyRatio:         0.1,
			},
			{
				SessionID:          "session-1",
				ChannelID:          "channel-1",
				BucketStart:        start.Add(30 * time.Second),
				BucketEnd:          start.Add(60 * time.Second),
				SentimentStatus:    "",
				PipelineLatencyMS:  &pipelineLatency2,
				TranscriptStatus:   "final",
				AudioSeconds:       24,
				SegmentCount:       1,
				WordCount:          5,
				EmptyRatio:         0.25,
				RepairAddedWords:   2,
				RepairChangedRatio: 0.4,
			},
		},
		Alignments: []AlignmentBucket{
			{
				SessionID:             "session-1",
				ChannelID:             "channel-1",
				WindowStart:           start,
				WindowEnd:             start.Add(30 * time.Second),
				ChatBucketStart:       start,
				ChatBucketEnd:         start.Add(30 * time.Second),
				TranscriptBucketStart: start,
				TranscriptBucketEnd:   start.Add(30 * time.Second),
			},
			{
				SessionID:             "session-1",
				ChannelID:             "channel-1",
				WindowStart:           start.Add(30 * time.Second),
				WindowEnd:             start.Add(60 * time.Second),
				ChatBucketStart:       start.Add(30 * time.Second),
				ChatBucketEnd:         start.Add(60 * time.Second),
				TranscriptBucketStart: start.Add(30 * time.Second),
				TranscriptBucketEnd:   start.Add(60 * time.Second),
			},
		},
		WindowLabels: []SignalWindowLabel{
			{
				SessionID:   "session-1",
				WindowStart: start.Add(30 * time.Second),
				WindowEnd:   start.Add(60 * time.Second),
				Correctness: "correct",
				EventLabel:  "hype_spike",
			},
			{
				SessionID:   "session-1",
				WindowStart: start.Add(5 * time.Minute),
				WindowEnd:   start.Add(5*time.Minute + 30*time.Second),
				Correctness: "uncertain",
				EventLabel:  "none",
			},
		},
		LabelCount: 4,
	}

	proof := BuildReplayProof(replay, ReplayProofOptions{GeneratedAt: generatedAt, ReplayLimit: 500})

	if proof.Type != ReplayProofType || proof.SessionID != "session-1" || proof.ChannelID != "channel-1" {
		t.Fatalf("unexpected proof identity: %#v", proof)
	}
	if !proof.GeneratedAt.Equal(generatedAt) {
		t.Fatalf("generated_at = %s, want %s", proof.GeneratedAt, generatedAt)
	}
	if proof.BucketCount != 7 || proof.SourceBucketCount != 5 || proof.ChatBucketCount != 3 || proof.TranscriptBucketCount != 2 || proof.AlignmentCount != 2 {
		t.Fatalf("unexpected bucket counts: %#v", proof)
	}
	if proof.ReplayLimit != 500 || proof.Partial || len(proof.TruncatedSources) != 0 {
		t.Fatalf("unexpected proof truncation metadata: %#v", proof)
	}
	if proof.SessionTotals.BucketCount != 7 || proof.SessionTotals.SignalWindowLabelCount != 2 {
		t.Fatalf("unexpected session totals: %#v", proof.SessionTotals)
	}
	if proof.SignalWindowCount != 3 || proof.MatchedWindows != 2 {
		t.Fatalf("unexpected window counts: signal=%d matched=%d", proof.SignalWindowCount, proof.MatchedWindows)
	}
	if proof.LabelCoverage.LabeledWindows != 1 || proof.LabelCoverage.UnmatchedLabels != 1 || proof.LabelCoverage.TotalWindows != 3 || proof.LabelCoverage.StoredLabelCount != 2 {
		t.Fatalf("unexpected label coverage counts: %#v", proof.LabelCoverage)
	}
	if proof.LabelCoverage.Coverage == nil || *proof.LabelCoverage.Coverage != 0.3333 {
		t.Fatalf("coverage = %v, want 0.3333", proof.LabelCoverage.Coverage)
	}
	if proof.TranscriptCoverage.BucketCount != 2 || proof.TranscriptCoverage.AudioCoverage == nil || *proof.TranscriptCoverage.AudioCoverage != 0.9 {
		t.Fatalf("unexpected transcript coverage: %#v", proof.TranscriptCoverage)
	}
	if proof.TranscriptCoverage.SegmentCount != 3 || proof.TranscriptCoverage.WordCount != 13 || proof.TranscriptCoverage.RepairAddedWords != 2 {
		t.Fatalf("unexpected transcript completeness totals: %#v", proof.TranscriptCoverage)
	}
	if proof.TranscriptCoverage.EmptyRatio == nil || *proof.TranscriptCoverage.EmptyRatio != 0.1667 || proof.TranscriptCoverage.AverageRepairChangedRatio == nil || *proof.TranscriptCoverage.AverageRepairChangedRatio != 0.4 {
		t.Fatalf("unexpected transcript ratios: %#v", proof.TranscriptCoverage)
	}
	if proof.TranscriptCoverage.RepairImprovement == nil || *proof.TranscriptCoverage.RepairImprovement != 0.4 {
		t.Fatalf("repair improvement = %v, want 0.4", proof.TranscriptCoverage.RepairImprovement)
	}
	if proof.TranscriptCoverage.StatusCounts["repairing"] != 1 || proof.TranscriptCoverage.StatusCounts["final"] != 1 {
		t.Fatalf("unexpected transcript status counts: %#v", proof.TranscriptCoverage.StatusCounts)
	}
	if proof.Timeline.Start == nil || !proof.Timeline.Start.Equal(start) {
		t.Fatalf("timeline start = %v, want %s", proof.Timeline.Start, start)
	}
	if proof.Timeline.End == nil || !proof.Timeline.End.Equal(start.Add(90*time.Second)) || proof.Timeline.SourceDurationMS != 90000 {
		t.Fatalf("unexpected timeline: %#v", proof.Timeline)
	}

	speed1 := findProofSpeed(t, proof.Speeds, 1)
	if speed1.EstimatedReplayDurationMS != 90000 || speed1.WindowsPerSecond != 0.0333 || speed1.BucketsPerSecond != 0.0778 {
		t.Fatalf("unexpected 1x speed metrics: %#v", speed1)
	}
	speed10 := findProofSpeed(t, proof.Speeds, 10)
	if speed10.EstimatedReplayDurationMS != 9000 || speed10.WindowsPerSecond != 0.3333 || speed10.BucketsPerSecond != 0.7778 {
		t.Fatalf("unexpected 10x speed metrics: %#v", speed10)
	}

	chatLatency := proof.Latency.ChatAnalysisLatencyMS
	if chatLatency.AvailableCount != 2 || chatLatency.MissingCount != 1 || chatLatency.Min == nil || *chatLatency.Min != 50 || chatLatency.Max == nil || *chatLatency.Max != 100 {
		t.Fatalf("unexpected chat latency summary: %#v", chatLatency)
	}
	if chatLatency.Average == nil || *chatLatency.Average != 75 || chatLatency.P50 == nil || *chatLatency.P50 != 50 || chatLatency.P95 == nil || *chatLatency.P95 != 100 {
		t.Fatalf("unexpected chat latency percentiles: %#v", chatLatency)
	}
	transcriptSummary := proof.Latency.TranscriptSentimentLatencyMS
	if transcriptSummary.AvailableCount != 1 || transcriptSummary.MissingCount != 1 || transcriptSummary.Min == nil || *transcriptSummary.Min != 200 {
		t.Fatalf("unexpected transcript latency summary: %#v", transcriptSummary)
	}
	asrSummary := proof.Latency.TranscriptASRLatencyMS
	if asrSummary.AvailableCount != 1 || asrSummary.MissingCount != 1 || asrSummary.Min == nil || *asrSummary.Min != 940 {
		t.Fatalf("unexpected ASR latency summary: %#v", asrSummary)
	}
	pipelineSummary := proof.Latency.TranscriptPipelineLatencyMS
	if pipelineSummary.AvailableCount != 2 || pipelineSummary.MissingCount != 0 || pipelineSummary.Average == nil || *pipelineSummary.Average != 1500 {
		t.Fatalf("unexpected pipeline latency summary: %#v", pipelineSummary)
	}
	if pipelineSummary.P50 == nil || *pipelineSummary.P50 != 1200 || pipelineSummary.P95 == nil || *pipelineSummary.P95 != 1800 {
		t.Fatalf("unexpected pipeline latency percentiles: %#v", pipelineSummary)
	}
	if proof.Latency.ChatAnalysisStatusCounts["python"] != 2 || proof.Latency.ChatAnalysisStatusCounts["fallback"] != 1 {
		t.Fatalf("unexpected chat status counts: %#v", proof.Latency.ChatAnalysisStatusCounts)
	}
	if proof.Latency.TranscriptSentimentStatusCounts["python"] != 1 || proof.Latency.TranscriptSentimentStatusCounts["missing"] != 1 {
		t.Fatalf("unexpected transcript status counts: %#v", proof.Latency.TranscriptSentimentStatusCounts)
	}
	if proof.DroppedEventRate != nil || len(proof.UnsupportedMetrics) != 1 || proof.UnsupportedMetrics[0].Name != "dropped_event_rate" {
		t.Fatalf("dropped event support should be null/unsupported: %#v", proof)
	}
}

func TestBuildReplayProofIncludesAlignmentOnlyWindowsWithChatBuckets(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	replay := SessionReplay{
		Session: SessionHistory{
			SessionID:       "session-1",
			ChannelID:       "channel-1",
			StartedAt:       start,
			ChatBucketCount: 1,
			AlignmentCount:  1,
			LabelCount:      1,
		},
		ChatBuckets: []chat.ChatBucket{
			{
				SessionID:   "session-1",
				ChannelID:   "channel-1",
				BucketStart: start,
				BucketEnd:   start.Add(30 * time.Second),
			},
		},
		Alignments: []AlignmentBucket{
			{
				SessionID:       "session-1",
				ChannelID:       "channel-1",
				WindowStart:     start.Add(10 * time.Second),
				WindowEnd:       start.Add(40 * time.Second),
				ChatBucketStart: start,
				ChatBucketEnd:   start.Add(30 * time.Second),
			},
		},
		WindowLabels: []SignalWindowLabel{
			{
				SessionID:   "session-1",
				WindowStart: start.Add(10 * time.Second),
				WindowEnd:   start.Add(40 * time.Second),
				Correctness: "correct",
				EventLabel:  "content_audience_divergence",
			},
		},
		LabelCount: 1,
	}

	proof := BuildReplayProof(replay, ReplayProofOptions{GeneratedAt: start, ReplayLimit: 500})

	if proof.SignalWindowCount != 2 || proof.MatchedWindows != 1 {
		t.Fatalf("proof should count chat and alignment windows: signal=%d matched=%d", proof.SignalWindowCount, proof.MatchedWindows)
	}
	if proof.LabelCoverage.LabeledWindows != 1 || proof.LabelCoverage.UnmatchedLabels != 0 || proof.LabelCoverage.TotalWindows != 2 {
		t.Fatalf("alignment-only label should be covered: %#v", proof.LabelCoverage)
	}
	if proof.LabelCoverage.Coverage == nil || *proof.LabelCoverage.Coverage != 0.5 {
		t.Fatalf("coverage = %v, want 0.5", proof.LabelCoverage.Coverage)
	}
}

func TestBuildReplayProofMarksTruncatedReplay(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	replay := SessionReplay{
		Session: SessionHistory{
			SessionID:             "session-1",
			ChannelID:             "channel-1",
			StartedAt:             start,
			ChatBucketCount:       3,
			TranscriptBucketCount: 2,
			AlignmentCount:        2,
			LabelCount:            99,
		},
		ChatBuckets: []chat.ChatBucket{
			{
				SessionID:   "session-1",
				ChannelID:   "channel-1",
				BucketStart: start,
				BucketEnd:   start.Add(30 * time.Second),
			},
		},
		TranscriptBuckets: []TranscriptBucket{
			{
				SessionID:   "session-1",
				ChannelID:   "channel-1",
				BucketStart: start,
				BucketEnd:   start.Add(30 * time.Second),
			},
		},
		WindowLabels: []SignalWindowLabel{
			{
				SessionID:   "session-1",
				WindowStart: start,
				WindowEnd:   start.Add(30 * time.Second),
			},
		},
		LabelCount: 99,
	}

	proof := BuildReplayProof(replay, ReplayProofOptions{GeneratedAt: start, ReplayLimit: 1})

	if !proof.Partial {
		t.Fatalf("proof should be partial: %#v", proof)
	}
	if proof.BucketCount != 2 || proof.SessionTotals.BucketCount != 7 {
		t.Fatalf("unexpected loaded/session counts: loaded=%d totals=%#v", proof.BucketCount, proof.SessionTotals)
	}
	if len(proof.TruncatedSources) != 3 {
		t.Fatalf("unexpected truncated sources: %#v", proof.TruncatedSources)
	}
	if proof.LabelCoverage.StoredLabelCount != 1 {
		t.Fatalf("stored proof labels = %d, want signal-window labels only", proof.LabelCoverage.StoredLabelCount)
	}
}

func TestBuildReplayProofHandlesEmptyReplay(t *testing.T) {
	proof := BuildReplayProof(SessionReplay{Session: SessionHistory{SessionID: "empty"}}, ReplayProofOptions{
		GeneratedAt: time.Date(2026, 5, 1, 13, 0, 0, 0, time.UTC),
	})

	if proof.BucketCount != 0 || proof.SignalWindowCount != 0 || proof.MatchedWindows != 0 {
		t.Fatalf("unexpected empty counts: %#v", proof)
	}
	if proof.LabelCoverage.Coverage != nil {
		t.Fatalf("empty replay coverage = %v, want nil", proof.LabelCoverage.Coverage)
	}
	if proof.Timeline.Start != nil || proof.Timeline.End != nil || proof.Timeline.SourceDurationMS != 0 {
		t.Fatalf("unexpected empty timeline: %#v", proof.Timeline)
	}
	for _, speed := range proof.Speeds {
		if speed.EstimatedReplayDurationMS != 0 || speed.WindowsPerSecond != 0 || speed.BucketsPerSecond != 0 {
			t.Fatalf("unexpected empty speed metric: %#v", speed)
		}
	}
}

func TestNormalizeTranscriptCompletenessBackfillsStoredQuality(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	bucket := normalizeTranscriptCompleteness(TranscriptBucket{
		BucketStart: start,
		BucketEnd:   start.Add(30 * time.Second),
		Text:        "old row text should not override quality word count",
		Quality: map[string]any{
			"audio_coverage_seconds": 24.0,
			"raw_segment_count":      3.0,
			"word_count":             6.0,
			"empty_ratio":            0.2,
			"repair_added_words":     2.0,
			"repair_changed_ratio":   0.4,
			"repair_status":          "completed",
		},
	})

	if bucket.AudioSeconds != 24 || bucket.SegmentCount != 3 || bucket.WordCount != 6 || bucket.EmptyRatio != 0.2 {
		t.Fatalf("quality completeness was not backfilled: %#v", bucket)
	}
	if bucket.RepairAddedWords != 2 || bucket.RepairChangedRatio != 0.4 || bucket.TranscriptStatus != "final" {
		t.Fatalf("repair completeness/status was not backfilled: %#v", bucket)
	}

	proof := BuildReplayProof(SessionReplay{TranscriptBuckets: []TranscriptBucket{bucket}}, ReplayProofOptions{GeneratedAt: start})
	if proof.TranscriptCoverage.AudioCoverage == nil || *proof.TranscriptCoverage.AudioCoverage != 0.8 {
		t.Fatalf("proof did not use backfilled audio coverage: %#v", proof.TranscriptCoverage)
	}
	if proof.TranscriptCoverage.EmptyRatio == nil || *proof.TranscriptCoverage.EmptyRatio != 0.2 || proof.TranscriptCoverage.StatusCounts["final"] != 1 {
		t.Fatalf("proof did not use backfilled quality fields: %#v", proof.TranscriptCoverage)
	}
}

func TestTranscriptCoverageKeepsZeroRepairChangedRatio(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	proof := BuildReplayProof(SessionReplay{TranscriptBuckets: []TranscriptBucket{
		{
			BucketStart:          start,
			BucketEnd:            start.Add(30 * time.Second),
			Text:                 "same repaired text",
			TranscriptStatus:     "final",
			AudioSeconds:         30,
			WordCount:            3,
			RepairChangedRatio:   0,
			RepairAddedWords:     0,
			TranscriptConfidence: 0.9,
			Quality: map[string]any{
				"repaired":             true,
				"repair_status":        "completed",
				"repair_changed_ratio": 0.0,
				"original_live_text":   "same repaired text",
			},
		},
	}}, ReplayProofOptions{GeneratedAt: start})

	if proof.TranscriptCoverage.AverageRepairChangedRatio == nil {
		t.Fatalf("zero repair changed ratio should be present: %#v", proof.TranscriptCoverage)
	}
	if *proof.TranscriptCoverage.AverageRepairChangedRatio != 0 {
		t.Fatalf("repair changed ratio = %v, want 0", *proof.TranscriptCoverage.AverageRepairChangedRatio)
	}
	if proof.TranscriptCoverage.RepairImprovement == nil || *proof.TranscriptCoverage.RepairImprovement != 0 {
		t.Fatalf("repair improvement = %v, want 0", proof.TranscriptCoverage.RepairImprovement)
	}
}

func findProofSpeed(t *testing.T, speeds []ReplayProofSpeed, speed float64) ReplayProofSpeed {
	t.Helper()
	for _, item := range speeds {
		if item.Speed == speed {
			return item
		}
	}
	t.Fatalf("missing speed %.1f in %#v", speed, speeds)
	return ReplayProofSpeed{}
}
