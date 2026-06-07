package correlation

import (
	"math"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func TestPearsonSpearmanAndPercentile(t *testing.T) {
	pearson, ok := pearsonCorrelation([]float64{1, 2, 3}, []float64{2, 4, 6})
	if !ok || math.Abs(pearson-1) > 0.0001 {
		t.Fatalf("pearson = %v, %v; want 1", pearson, ok)
	}
	spearman, ok := spearmanCorrelation([]float64{10, 30, 20}, []float64{1, 3, 2})
	if !ok || math.Abs(spearman-1) > 0.0001 {
		t.Fatalf("spearman = %v, %v; want 1", spearman, ok)
	}
	if got := roundMetric(percentile([]float64{0.1, 0.2, 0.3, 0.4, 0.5}, 0.95)); got != 0.48 {
		t.Fatalf("p95 = %v, want interpolated 0.48", got)
	}
}

func TestBuildReportComputesCalibrationAndSamples(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	replay := correlatedReplay("session-1", start)
	appendDivergencePair(&replay, start.Add(5*30*time.Second), 0.80, -0.20, "content_audience_divergence")
	appendDivergencePair(&replay, start.Add(6*30*time.Second), -0.80, 0.10, "content_audience_divergence")
	appendDivergencePair(&replay, start.Add(7*30*time.Second), 0.70, -0.10, "content_audience_divergence")

	report := BuildReport([]storage.SessionReplay{replay}, Config{
		GeneratedAt:           time.Date(2026, 5, 2, 15, 0, 0, 0, time.UTC),
		MinimumPairs:          3,
		ManualSamplePerCohort: 2,
	})

	baseline, ok := findCohort(report.Aggregate, CohortCalmBaseline)
	if !ok || baseline.Pearson == nil || *baseline.Pearson < 0.99 {
		t.Fatalf("baseline correlation not computed strongly: %#v", baseline)
	}
	if report.Calibration.Status != "computed" || report.Calibration.BaselineAbsDeltaP95 == nil {
		t.Fatalf("calibration not computed: %#v", report.Calibration)
	}
	if report.Calibration.DetectedAboveBaselineP95 != 3 {
		t.Fatalf("detected above p95 = %d, want 3", report.Calibration.DetectedAboveBaselineP95)
	}
	if report.Calibration.DetectedAboveBaselineP95Rate == nil || *report.Calibration.DetectedAboveBaselineP95Rate != 1 {
		t.Fatalf("detected above p95 rate = %v, want 1", report.Calibration.DetectedAboveBaselineP95Rate)
	}
	if len(report.ManualValidationSample) != 6 {
		t.Fatalf("manual sample rows = %d, want three cohorts capped at two rows each", len(report.ManualValidationSample))
	}
	if report.ManualValidationSample[0].ReviewStatus != "needs_human_review" || len(report.ManualValidationSample[0].Evidence) == 0 {
		t.Fatalf("manual sample missing placeholders/evidence: %#v", report.ManualValidationSample[0])
	}

	var lagZero LagSummary
	for _, lag := range report.LagAnalysis {
		if lag.LagSeconds == 0 {
			lagZero = lag
		}
	}
	if lagZero.PairCount != 8 {
		t.Fatalf("same-window lag pairs = %d, want 8", lagZero.PairCount)
	}
}

func TestLagAnalysisFindsKnownOffset(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	replay := storage.SessionReplay{
		Session: storage.SessionHistory{SessionID: "lag-session", ChannelID: "channel-1", StartedAt: start},
	}
	chatScores := []float64{-0.5, 0, 0.5}
	sameWindowTranscript := []float64{0.2, -0.5, 0}
	shiftedTranscript := []float64{-0.5, 0, 0.5}
	for index, score := range chatScores {
		bucketStart := start.Add(time.Duration(index) * 30 * time.Second)
		replay.ChatBuckets = append(replay.ChatBuckets, chatBucket("lag-session", "channel-1", bucketStart, score))
		replay.TranscriptBuckets = append(replay.TranscriptBuckets, transcriptBucket("lag-session", "channel-1", bucketStart, sameWindowTranscript[index]))
	}
	for index, score := range shiftedTranscript {
		bucketStart := start.Add(time.Duration(index+1) * 30 * time.Second)
		replay.TranscriptBuckets = append(replay.TranscriptBuckets, transcriptBucket("lag-session", "channel-1", bucketStart, score))
	}

	report := BuildReport([]storage.SessionReplay{replay}, Config{
		MinimumPairs: 3,
		LagOffsets:   []time.Duration{0, 30 * time.Second},
	})

	if report.BestLag == nil || report.BestLag.LagSeconds != 30 {
		t.Fatalf("best lag = %#v, want +30s", report.BestLag)
	}
	if report.BestLag.Pearson == nil || *report.BestLag.Pearson < 0.99 {
		t.Fatalf("best lag pearson = %v, want high positive correlation", report.BestLag.Pearson)
	}
}

func TestNegativeControlDropsForCorrelatedPairs(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	report := BuildReport([]storage.SessionReplay{correlatedReplay("session-1", start)}, Config{MinimumPairs: 3})

	if report.NegativeControl.Status != "computed" {
		t.Fatalf("negative control status = %s", report.NegativeControl.Status)
	}
	if report.NegativeControl.PearsonDrop == nil || *report.NegativeControl.PearsonDrop <= 0 {
		t.Fatalf("pearson drop = %v, want observed above shuffled", report.NegativeControl.PearsonDrop)
	}
}

func TestBuildReportMarksInsufficientData(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	replay := storage.SessionReplay{
		Session: storage.SessionHistory{SessionID: "small", ChannelID: "channel-1", StartedAt: start},
	}
	appendAlignmentPair(&replay, start, 0.1, 0.12, "converged", "none")

	report := BuildReport([]storage.SessionReplay{replay}, Config{MinimumPairs: 3})

	all, _ := findCohort(report.Aggregate, CohortAllAligned)
	if all.CorrelationStatus != "insufficient_pairs" || all.Pearson != nil {
		t.Fatalf("unexpected all-aligned summary: %#v", all)
	}
	if report.Calibration.Status != "insufficient_baseline_pairs" {
		t.Fatalf("calibration status = %s", report.Calibration.Status)
	}
	if len(report.Limitations) == 0 {
		t.Fatalf("expected limitations for insufficient report")
	}
}

func correlatedReplay(sessionID string, start time.Time) storage.SessionReplay {
	replay := storage.SessionReplay{
		Session: storage.SessionHistory{SessionID: sessionID, ChannelID: "channel-1", StartedAt: start},
	}
	values := []float64{-0.4, -0.2, 0, 0.2, 0.4}
	for index, value := range values {
		appendAlignmentPair(&replay, start.Add(time.Duration(index)*30*time.Second), value, value+0.01, "converged", "none")
	}
	return replay
}

func appendDivergencePair(replay *storage.SessionReplay, start time.Time, chatScore, transcriptScore float64, label string) {
	appendAlignmentPair(replay, start, chatScore, transcriptScore, "diverged", label)
}

func appendAlignmentPair(replay *storage.SessionReplay, start time.Time, chatScore, transcriptScore float64, relationship string, label string) {
	sessionID := replay.Session.SessionID
	channelID := replay.Session.ChannelID
	end := start.Add(30 * time.Second)
	replay.ChatBuckets = append(replay.ChatBuckets, chatBucket(sessionID, channelID, start, chatScore))
	replay.TranscriptBuckets = append(replay.TranscriptBuckets, transcriptBucket(sessionID, channelID, start, transcriptScore))
	replay.Alignments = append(replay.Alignments, storage.AlignmentBucket{
		Type:                  "alignment_bucket",
		SessionID:             sessionID,
		ChannelID:             channelID,
		WindowStart:           start,
		WindowEnd:             end,
		ChatBucketStart:       start,
		ChatBucketEnd:         end,
		TranscriptBucketStart: start,
		TranscriptBucketEnd:   end,
		ChatSentiment:         chatScore,
		ChatConfidence:        0.92,
		ChatMessageCount:      20,
		TranscriptSentiment:   transcriptScore,
		TranscriptConfidence:  0.91,
		TranscriptTextLength:  120,
		Delta:                 chatScore - transcriptScore,
		Similarity:            1 - math.Min(math.Abs(chatScore-transcriptScore), 2)/2,
		Relationship:          relationship,
		OverlapSeconds:        30,
		Quality:               0.90,
		QualityFlags:          []string{"good_overlap", "enough_chat_volume", "good_transcript_confidence", "enough_transcript_text"},
	})
	if label != "" {
		replay.WindowLabels = append(replay.WindowLabels, storage.SignalWindowLabel{
			SessionID:   sessionID,
			WindowStart: start,
			WindowEnd:   end,
			EventLabel:  label,
			Correctness: "correct",
		})
	}
}

func chatBucket(sessionID, channelID string, start time.Time, score float64) chat.ChatBucket {
	return chat.ChatBucket{
		Type:                "chat_bucket",
		SessionID:           sessionID,
		ChannelID:           channelID,
		BucketStart:         start,
		BucketEnd:           start.Add(30 * time.Second),
		MessageCount:        20,
		UniqueChatters:      10,
		ChatSentiment:       score,
		SentimentConfidence: 0.92,
		MessageScores: []chat.MessageScore{{
			MessageID:      sessionID + ":" + start.Format(time.RFC3339),
			Timestamp:      start.Add(5 * time.Second),
			Text:           "sample chat evidence",
			Label:          "neutral",
			Confidence:     0.9,
			SentimentScore: score,
		}},
		TopTerms: []string{"sample", "evidence"},
	}
}

func transcriptBucket(sessionID, channelID string, start time.Time, score float64) storage.TranscriptBucket {
	return storage.TranscriptBucket{
		Type:                 "transcript_bucket",
		SessionID:            sessionID,
		ChannelID:            channelID,
		BucketStart:          start,
		BucketEnd:            start.Add(30 * time.Second),
		Text:                 "this transcript text is intentionally long enough for baseline quality checks",
		Language:             "en",
		TranscriptConfidence: 0.91,
		TranscriptStatus:     "final",
		SentimentScore:       &score,
		SentimentConfidence:  floatPtr(0.90),
		SentimentLabel:       "neutral",
		SentimentStatus:      "python",
		WordCount:            12,
	}
}
