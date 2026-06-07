package analysis

import (
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestAnalyzerAnalyzeBucketsBuildsCompleteBoundaryResult(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := -0.25
	analyzer := NewAnalyzer(AnalyzerConfig{AlignmentWindow: 30 * time.Second})

	result := analyzer.AnalyzeBuckets(BucketAnalysisInput{
		SessionID: "session",
		ChatBuckets: []chat.ChatBucket{
			{
				SessionID:     "session",
				ChannelID:     "channel",
				BucketStart:   start,
				BucketEnd:     start.Add(30 * time.Second),
				ChatSentiment: -0.3,
			},
			{
				SessionID:           "session",
				ChannelID:           "channel",
				BucketStart:         start.Add(30 * time.Second),
				BucketEnd:           start.Add(60 * time.Second),
				MessageCount:        30,
				ChatSentiment:       0.5,
				SentimentConfidence: 0.8,
				PositiveRatio:       0.7,
			},
		},
		TranscriptBuckets: []TranscriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start.Add(30 * time.Second),
				BucketEnd:            start.Add(60 * time.Second),
				Text:                 "the speaker tone is flat while chat reacts very positively to the moment",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
		},
	})

	if len(result.Alignments) != 1 {
		t.Fatalf("expected computed alignment, got %#v", result.Alignments)
	}
	if len(result.SignalWindows) != 2 {
		t.Fatalf("expected signal windows for both chat buckets, got %#v", result.SignalWindows)
	}
	if len(result.SignalEvents) != 3 {
		t.Fatalf("expected audience shift, hype, and divergence events, got %#v", result.SignalEvents)
	}
	if len(result.Insights) != len(result.SignalEvents) {
		t.Fatalf("expected one insight per signal event, got events=%#v insights=%#v", result.SignalEvents, result.Insights)
	}
	if result.InsightSummary.Type != "session_insight_summary" || result.InsightSummary.AlignmentCount != 1 {
		t.Fatalf("unexpected insight summary: %#v", result.InsightSummary)
	}
}

func TestAnalyzerAnalyzeSessionUsesProvidedAlignments(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := -0.8
	analyzer := NewAnalyzer(AnalyzerConfig{AlignmentWindow: 30 * time.Second})

	result := analyzer.AnalyzeSession(SessionAnalysisInput{
		SessionID: "session",
		ChatBuckets: []chat.ChatBucket{
			{
				SessionID:     "session",
				ChannelID:     "channel",
				BucketStart:   start,
				BucketEnd:     start.Add(30 * time.Second),
				MessageCount:  20,
				ChatSentiment: 0.7,
				PositiveRatio: 0.8,
			},
		},
		TranscriptBuckets: []TranscriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start,
				BucketEnd:            start.Add(30 * time.Second),
				Text:                 "stored replay transcript bucket should not imply a fresh alignment by itself",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
		},
	})

	if len(result.Alignments) != 0 {
		t.Fatalf("expected no recomputed alignments for stored session input, got %#v", result.Alignments)
	}
	if len(result.SignalWindows) != 1 {
		t.Fatalf("expected signal window from chat bucket, got %#v", result.SignalWindows)
	}
	if result.SignalWindows[0].AlignmentDelta != nil {
		t.Fatalf("expected no alignment fields without provided alignment, got %#v", result.SignalWindows[0])
	}
	if result.InsightSummary.AlignmentCount != 0 || result.InsightSummary.TranscriptBucketCount != 1 {
		t.Fatalf("unexpected summary counts: %#v", result.InsightSummary)
	}
}
