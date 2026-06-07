package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestComputeAlignmentsMatchesOverlappingBuckets(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := 0.8
	alignments := computeAlignments(
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
		[]transcriptBucket{
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

func TestComputeAlignmentsRequiresEnoughOverlap(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := 0.2
	alignments := computeAlignments(
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
		[]transcriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start.Add(20 * time.Second),
				BucketEnd:            start.Add(50 * time.Second),
				Text:                 "low overlap transcript text",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
		},
		30*time.Second,
	)

	if len(alignments) != 0 {
		t.Fatalf("expected no alignments, got %d", len(alignments))
	}
}

func TestStreamTranscriptEventsAddsTranscriptBucketFromSSE(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			t.Fatalf("path = %q, want /events", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"transcript_bucket","session_id":"other","channel_id":"channel","bucket_start":"2026-05-01T12:00:00Z","bucket_end":"2026-05-01T12:00:30Z"}` + "\n\n"))
		_, _ = w.Write([]byte(`data: {"type":"transcript_bucket","session_id":"session","channel_id":"channel","bucket_start":"2026-05-01T12:00:00Z","bucket_end":"2026-05-01T12:00:30Z","text":"streamed transcript bucket with enough spoken context to align against the chat reaction window","language":"en","transcript_confidence":0.92,"sentiment_score":0.35}` + "\n\n"))
	}))
	defer upstream.Close()

	s := &server{
		cfg:    appConfig{TranscriptURL: upstream.URL, BucketEvery: 30 * time.Second},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		hub:    newEventHub(),
		state: dashboardState{
			Session: "session",
			Channel: "channel",
			Buckets: []chat.ChatBucket{
				{
					SessionID:           "session",
					ChannelID:           "channel",
					BucketStart:         start,
					BucketEnd:           start.Add(30 * time.Second),
					MessageCount:        12,
					ChatSentiment:       0.3,
					SentimentConfidence: 0.8,
				},
			},
		},
		humanLabels: map[string]string{},
	}

	s.streamTranscriptEvents(context.Background(), "session", "channel")

	if len(s.state.Transcripts) != 1 {
		t.Fatalf("transcript buckets = %d, want 1", len(s.state.Transcripts))
	}
	if s.state.Transcripts[0].Text == "" {
		t.Fatalf("unexpected transcript bucket: %#v", s.state.Transcripts[0])
	}
	if len(s.state.Alignments) != 1 {
		t.Fatalf("alignments = %d, want 1", len(s.state.Alignments))
	}
}

func TestUpsertTranscriptBucketReportsReplacementChanges(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	buckets := []transcriptBucket{
		{
			SessionID:   "session",
			ChannelID:   "channel",
			BucketStart: start,
			BucketEnd:   start.Add(30 * time.Second),
			Text:        "first",
		},
	}

	unchanged := upsertTranscriptBucket(&buckets, buckets[0])
	if unchanged {
		t.Fatal("identical transcript bucket should not report a change")
	}

	replacement := buckets[0]
	replacement.Text = "first with word evidence"
	replacement.Segments = []transcriptSegment{{Start: 0, End: 1, Text: "first", Words: []transcriptWord{{Start: 0, End: 0.5, Text: "first"}}}}
	changed := upsertTranscriptBucket(&buckets, replacement)

	if !changed {
		t.Fatal("replacement transcript bucket should report a change")
	}
	if buckets[0].Text != replacement.Text || len(buckets[0].Segments[0].Words) != 1 {
		t.Fatalf("replacement was not stored: %#v", buckets[0])
	}
}

func TestDecodeTranscriptBucketEventPreservesRepairQuality(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	event := map[string]any{
		"type":                  "transcript_bucket",
		"session_id":            "session",
		"channel_id":            "channel",
		"bucket_start":          start.Format(time.RFC3339Nano),
		"bucket_end":            start.Add(30 * time.Second).Format(time.RFC3339Nano),
		"text":                  "repaired transcript",
		"language":              "en",
		"transcript_confidence": 0.86,
		"quality": map[string]any{
			"repaired":           true,
			"repair_status":      "completed",
			"repair_latency_ms":  float64(1234),
			"original_live_text": "live transcript",
		},
	}

	bucket, ok := decodeTranscriptBucketEvent(event, "session", "channel")
	if !ok {
		t.Fatal("repair bucket was not decoded")
	}
	if bucket.Quality["repaired"] != true || bucket.Quality["repair_status"] != "completed" || bucket.Quality["original_live_text"] != "live transcript" {
		t.Fatalf("repair quality was not preserved: %#v", bucket.Quality)
	}
}
