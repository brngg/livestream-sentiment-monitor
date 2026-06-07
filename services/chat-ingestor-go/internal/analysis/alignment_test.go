package analysis

import (
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestComputeAlignmentsMatchesOverlappingBuckets(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := 0.8
	alignments := ComputeAlignments(
		[]chat.ChatBucket{
			{
				SessionID:           "session",
				ChannelID:           "channel",
				BucketStart:         start,
				BucketEnd:           start.Add(30 * time.Second),
				MessageCount:        20,
				ChatSentiment:       0.2,
				SentimentConfidence: 0.7,
			},
		},
		[]TranscriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start.Add(time.Second),
				BucketEnd:            start.Add(31 * time.Second),
				Text:                 "this is enough transcript text to compare against the chat bucket sentiment",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
		},
		30*time.Second,
	)

	if len(alignments) != 1 {
		t.Fatalf("expected 1 alignment, got %d", len(alignments))
	}
	got := alignments[0]
	if got.Type != "alignment_bucket" {
		t.Fatalf("expected alignment_bucket type, got %q", got.Type)
	}
	if got.Relationship != "diverged" {
		t.Fatalf("expected diverged, got %q", got.Relationship)
	}
	if got.OverlapSeconds != 29 {
		t.Fatalf("expected 29s overlap, got %d", got.OverlapSeconds)
	}
	if got.Delta >= 0 {
		t.Fatalf("expected negative delta, got %f", got.Delta)
	}
	if got.Quality <= 0 {
		t.Fatalf("expected positive quality, got %f", got.Quality)
	}
}

func TestComputeAlignmentsUsesEachChatBucketOnce(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := 0.2
	alignments := ComputeAlignments(
		[]chat.ChatBucket{
			{
				SessionID:     "session",
				ChannelID:     "channel",
				BucketStart:   start,
				BucketEnd:     start.Add(30 * time.Second),
				MessageCount:  10,
				ChatSentiment: 0.2,
			},
		},
		[]TranscriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start,
				BucketEnd:            start.Add(30 * time.Second),
				Text:                 "first transcript bucket with matching sentiment",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start,
				BucketEnd:            start.Add(30 * time.Second),
				Text:                 "second transcript bucket should not reuse the same chat bucket",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
		},
		30*time.Second,
	)

	if len(alignments) != 1 {
		t.Fatalf("expected one alignment because chat buckets are single-use, got %d", len(alignments))
	}
}

func TestComputeAlignmentsScoresOverlapAgainstConfiguredWindow(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := 0.1
	alignments := ComputeAlignments(
		[]chat.ChatBucket{
			{
				SessionID:           "session",
				ChannelID:           "channel",
				BucketStart:         start,
				BucketEnd:           start.Add(60 * time.Second),
				MessageCount:        20,
				ChatSentiment:       0.1,
				SentimentConfidence: 0.8,
			},
		},
		[]TranscriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start.Add(30 * time.Second),
				BucketEnd:            start.Add(90 * time.Second),
				Text:                 "this transcript bucket overlaps half of the configured alignment window",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
		},
		60*time.Second,
	)

	if len(alignments) != 1 {
		t.Fatalf("expected 1 alignment, got %d", len(alignments))
	}
	if !hasQualityFlag(alignments[0].QualityFlags, "partial_overlap") {
		t.Fatalf("expected partial_overlap flag for half-window overlap, got %#v", alignments[0].QualityFlags)
	}
	if hasQualityFlag(alignments[0].QualityFlags, "good_overlap") {
		t.Fatalf("did not expect good_overlap flag for half-window overlap, got %#v", alignments[0].QualityFlags)
	}
}

func hasQualityFlag(flags []string, expected string) bool {
	for _, flag := range flags {
		if flag == expected {
			return true
		}
	}
	return false
}
