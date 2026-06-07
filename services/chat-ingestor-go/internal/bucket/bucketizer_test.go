package bucket

import (
	"math"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestWindowForUsesHalfOpenThirtySecondBoundaries(t *testing.T) {
	b := Bucketizer{Window: 30 * time.Second}

	tests := []struct {
		name      string
		at        string
		wantStart string
		wantEnd   string
	}{
		{
			name:      "inside first bucket",
			at:        "2026-04-29T18:00:29.999999999Z",
			wantStart: "2026-04-29T18:00:00Z",
			wantEnd:   "2026-04-29T18:00:30Z",
		},
		{
			name:      "exact boundary starts next bucket",
			at:        "2026-04-29T18:00:30Z",
			wantStart: "2026-04-29T18:00:30Z",
			wantEnd:   "2026-04-29T18:01:00Z",
		},
		{
			name:      "next minute boundary",
			at:        "2026-04-29T18:01:00Z",
			wantStart: "2026-04-29T18:01:00Z",
			wantEnd:   "2026-04-29T18:01:30Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := b.WindowFor(mustTime(t, tt.at))
			if got := start.Format(time.RFC3339Nano); got != tt.wantStart {
				t.Fatalf("start = %s, want %s", got, tt.wantStart)
			}
			if got := end.Format(time.RFC3339Nano); got != tt.wantEnd {
				t.Fatalf("end = %s, want %s", got, tt.wantEnd)
			}
		})
	}
}

func TestBucketAggregatesMessages(t *testing.T) {
	b := Bucketizer{Window: 30 * time.Second}
	messages := []chat.ScoredMessage{
		scored("2026-04-29T18:00:02Z", "viewer1", "that was clutch PogChamp", []string{"PogChamp"}, "en", 0.8, 0.7),
		scored("2026-04-29T18:00:10Z", "viewer2", "great recovery", nil, "en", 0.6, 0.8),
		scored("2026-04-29T18:00:20Z", "viewer1", "rough timing", nil, "en", -0.4, 0.5),
	}

	got, ok := b.Bucket(messages)
	if !ok {
		t.Fatal("expected bucket")
	}

	if got.Type != "chat_bucket" {
		t.Fatalf("type = %q, want chat_bucket", got.Type)
	}
	if got.MessageCount != 3 {
		t.Fatalf("message count = %d, want 3", got.MessageCount)
	}
	if got.UniqueChatters != 2 {
		t.Fatalf("unique chatters = %d, want 2", got.UniqueChatters)
	}
	if math.Abs(got.ChatSentiment-((0.8+0.6-0.4)/3)) > 0.0000001 {
		t.Fatalf("chat sentiment = %f", got.ChatSentiment)
	}
	if math.Abs(got.PositiveRatio-(2.0/3.0)) > 0.0000001 {
		t.Fatalf("positive ratio = %f, want 2/3", got.PositiveRatio)
	}
	if got.NeutralRatio != 0 {
		t.Fatalf("neutral ratio = %f, want 0", got.NeutralRatio)
	}
	if math.Abs(got.NegativeRatio-(1.0/3.0)) > 0.0000001 {
		t.Fatalf("negative ratio = %f, want 1/3", got.NegativeRatio)
	}
	if got.LanguageMix["en"] != 1 {
		t.Fatalf("english language mix = %f, want 1", got.LanguageMix["en"])
	}
	if len(got.TopEmotes) != 1 || got.TopEmotes[0] != "PogChamp" {
		t.Fatalf("top emotes = %#v, want PogChamp", got.TopEmotes)
	}
	if got.BucketStart.Format(time.RFC3339) != "2026-04-29T18:00:00Z" {
		t.Fatalf("bucket start = %s", got.BucketStart.Format(time.RFC3339))
	}
}

func TestStreamBucketizerFlushesOnBoundary(t *testing.T) {
	b := NewStreamBucketizer(30 * time.Second)

	if flushed := b.Add(scored("2026-04-29T18:00:29Z", "viewer1", "great", nil, "en", 0.8, 0.7)); len(flushed) != 0 {
		t.Fatalf("unexpected early flush: %#v", flushed)
	}
	flushed := b.Add(scored("2026-04-29T18:00:30Z", "viewer2", "rough", nil, "en", -0.5, 0.6))
	if len(flushed) != 1 {
		t.Fatalf("flushed bucket count = %d, want 1", len(flushed))
	}
	if flushed[0].BucketStart.Format(time.RFC3339) != "2026-04-29T18:00:00Z" {
		t.Fatalf("flushed start = %s", flushed[0].BucketStart.Format(time.RFC3339))
	}

	final := b.Flush()
	if len(final) != 1 {
		t.Fatalf("final bucket count = %d, want 1", len(final))
	}
	if final[0].BucketStart.Format(time.RFC3339) != "2026-04-29T18:00:30Z" {
		t.Fatalf("final start = %s", final[0].BucketStart.Format(time.RFC3339))
	}
}

func scored(at, username, text string, emotes []string, language string, score, confidence float64) chat.ScoredMessage {
	return chat.ScoredMessage{
		Message: chat.ChatMessage{
			SessionID: "session-1",
			ChannelID: "channel-1",
			MessageID: username + "-" + at,
			Timestamp: mustParse(at),
			Username:  username,
			Text:      text,
			Emotes:    emotes,
			Language:  language,
		},
		Sentiment: chat.SentimentResult{Score: score, Confidence: confidence},
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func mustParse(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}
