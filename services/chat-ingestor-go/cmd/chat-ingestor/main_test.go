package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/bucket"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/filter"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/publisher"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/reaction"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/sentiment"
)

type staticReader struct {
	messages []chat.ChatMessage
}

func (r staticReader) Read(ctx context.Context) (<-chan chat.ChatMessage, <-chan error) {
	out := make(chan chat.ChatMessage)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		for _, message := range r.messages {
			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			case out <- message:
			}
		}
	}()
	return out, errs
}

type collectingPublisher struct {
	buckets []chat.ChatBucket
}

func (p *collectingPublisher) Publish(_ context.Context, bucket chat.ChatBucket) error {
	p.buckets = append(p.buckets, bucket)
	return nil
}

var _ publisher.Publisher = (*collectingPublisher)(nil)

func TestRunPublishesBucketsWithPeakMetadata(t *testing.T) {
	start := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	collector := &collectingPublisher{}

	err := run(
		context.Background(),
		staticReader{messages: []chat.ChatMessage{
			{
				SessionID: "session",
				ChannelID: "channel",
				MessageID: "m1",
				Timestamp: start.Add(29 * time.Second),
				Username:  "viewer",
				Text:      "NO WAY",
				Language:  "en",
			},
			{
				SessionID: "session",
				ChannelID: "channel",
				MessageID: "m2",
				Timestamp: start.Add(30 * time.Second),
				Username:  "viewer2",
				Text:      "next bucket",
				Language:  "en",
			},
		}},
		filter.MessageFilter{},
		sentiment.NewLexiconAnalyzer(),
		bucket.NewStreamBucketizer(30*time.Second),
		reaction.NewAnalyzer(5*time.Second, 5*time.Minute),
		"session",
		"channel",
		[]publisher.Publisher{collector},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		false,
	)
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if len(collector.buckets) != 2 {
		t.Fatalf("published buckets = %d, want 2", len(collector.buckets))
	}
	first := collector.buckets[0]
	if first.PeakReactionScore == nil || first.PeakReactionType != "hype" {
		t.Fatalf("first bucket missing peak metadata: %#v", first)
	}
	if first.PeakWindowEnd == nil || first.PeakWindowEnd.After(first.BucketEnd) {
		t.Fatalf("peak window crosses bucket boundary: %#v", first)
	}
	if len(first.Subwindows) == 0 {
		t.Fatalf("expected subwindows: %#v", first)
	}
}
