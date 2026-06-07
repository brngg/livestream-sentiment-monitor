package analysis

import (
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestGenerateSessionInsightsFromSignalWindowsIsDeterministic(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	windows := SignalWindowsFromChatAndAlignments(
		[]chat.ChatBucket{
			{
				SessionID:     "session",
				ChannelID:     "channel",
				BucketStart:   start.Add(30 * time.Second),
				BucketEnd:     start.Add(60 * time.Second),
				ChatSentiment: 0.5,
				PositiveRatio: 0.7,
			},
			{
				SessionID:     "session",
				ChannelID:     "channel",
				BucketStart:   start,
				BucketEnd:     start.Add(30 * time.Second),
				ChatSentiment: -0.3,
			},
		},
		[]AlignmentBucket{
			{
				SessionID:             "session",
				ChannelID:             "channel",
				WindowStart:           start.Add(30 * time.Second),
				WindowEnd:             start.Add(60 * time.Second),
				ChatBucketStart:       start.Add(30 * time.Second),
				ChatBucketEnd:         start.Add(60 * time.Second),
				TranscriptBucketStart: start.Add(30 * time.Second),
				TranscriptBucketEnd:   start.Add(60 * time.Second),
				ChatSentiment:         0.5,
				ChatConfidence:        0.8,
				TranscriptSentiment:   -0.25,
				TranscriptConfidence:  0.9,
				Delta:                 0.75,
				Relationship:          "diverged",
				Quality:               0.85,
			},
		},
	)

	first, firstSummary := GenerateSessionInsights("session", windows, nil, nil, nil)
	second, secondSummary := GenerateSessionInsights("session", windows, nil, nil, nil)

	if len(first) != 3 {
		t.Fatalf("expected audience shift, hype, and divergence insights, got %#v", first)
	}
	if first[0].ID != second[0].ID || first[1].ID != second[1].ID || first[2].ID != second[2].ID {
		t.Fatalf("expected deterministic insight IDs, got %#v and %#v", first, second)
	}
	if firstSummary.TopInsightID != secondSummary.TopInsightID || firstSummary.PrimaryInsightKind != secondSummary.PrimaryInsightKind {
		t.Fatalf("expected deterministic summaries, got %#v and %#v", firstSummary, secondSummary)
	}
	if firstSummary.Type != "session_insight_summary" || firstSummary.InsightCount != 3 {
		t.Fatalf("unexpected summary: %#v", firstSummary)
	}
	if firstSummary.PrimaryInsightKind != InsightHypeSpike {
		t.Fatalf("expected highest-severity hype spike, got %q", firstSummary.PrimaryInsightKind)
	}
	if len(firstSummary.TopMoments) != 3 {
		t.Fatalf("expected ranked top moments, got %#v", firstSummary.TopMoments)
	}
	if firstSummary.BiggestDivergence == nil || firstSummary.HighestHype == nil {
		t.Fatalf("expected typed insight spotlights, got %#v", firstSummary)
	}
}

func TestGenerateSessionInsightsAddsTranscriptGapEvidence(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	insights, summary := GenerateSessionInsights(
		"session",
		nil,
		nil,
		[]TranscriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start,
				BucketEnd:            start.Add(30 * time.Second),
				TranscriptConfidence: 0.1,
			},
		},
		nil,
	)

	if len(insights) != 1 {
		t.Fatalf("expected one transcript gap insight, got %#v", insights)
	}
	if insights[0].Kind != InsightTranscriptGap {
		t.Fatalf("expected transcript gap insight, got %q", insights[0].Kind)
	}
	if summary.TranscriptBucketCount != 1 || summary.HighSeverityCount != 1 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	if len(summary.LowConfidenceFlags) != 1 || summary.LowConfidenceFlags[0].Kind != InsightTranscriptGap {
		t.Fatalf("expected transcript gap low-confidence flag, got %#v", summary.LowConfidenceFlags)
	}
}
