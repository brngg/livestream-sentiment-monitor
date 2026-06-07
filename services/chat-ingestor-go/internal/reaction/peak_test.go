package reaction

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestAttachPeakMetadataSelectsPeakWindowInsideBucket(t *testing.T) {
	start := mustParse("2026-05-08T12:00:00Z")
	bucket := chat.ChatBucket{
		Type:        "chat_bucket",
		SessionID:   "session",
		ChannelID:   "channel",
		BucketStart: start,
		BucketEnd:   start.Add(30 * time.Second),
	}
	windows := []chat.ReactionWindow{
		{
			WindowStart:    start.Add(5 * time.Second),
			WindowEnd:      start.Add(10 * time.Second),
			MessageCount:   4,
			IntensityScore: 0.3,
			HypeScore:      0.2,
			ReactionType:   "intensity",
		},
		{
			WindowStart:      start.Add(12 * time.Second),
			WindowEnd:        start.Add(17 * time.Second),
			MessageCount:     8,
			HypeScore:        0.91,
			IntensityScore:   0.7,
			ReactionType:     "hype",
			TargetType:       "unknown",
			TargetText:       "dragon",
			Source:           "chat",
			EventHint:        "hype:dragon",
			Confidence:       0.91,
			EvidenceIDs:      []string{"m1"},
			EvidenceMessages: []chat.ChatMessage{testMessage(start.Add(13*time.Second), "NO WAY", "viewer", nil)},
		},
		{
			WindowStart:    start.Add(35 * time.Second),
			WindowEnd:      start.Add(40 * time.Second),
			MessageCount:   20,
			IntensityScore: 1,
			ReactionType:   "hype",
		},
	}

	got := AttachPeakMetadata(bucket, windows)

	if got.PeakReactionScore == nil || *got.PeakReactionScore != 0.91 {
		t.Fatalf("peak score = %v, want 0.91", got.PeakReactionScore)
	}
	if got.PeakReactionType != "hype" {
		t.Fatalf("peak type = %q, want hype", got.PeakReactionType)
	}
	if got.PeakTargetType != "unknown" || got.PeakTargetText != "dragon" || got.PeakSource != "chat" || got.PeakEventHint != "hype:dragon" || got.PeakConfidence != 0.91 {
		t.Fatalf("peak context = %#v", got)
	}
	if len(got.PeakEvidenceIDs) != 1 || got.PeakEvidenceIDs[0] != "m1" {
		t.Fatalf("peak evidence ids = %#v", got.PeakEvidenceIDs)
	}
	if got.Subwindows[1].TargetText != "dragon" || got.Subwindows[1].EventHint != "hype:dragon" {
		t.Fatalf("subwindow context = %#v", got.Subwindows[1])
	}
	if got.PeakTime == nil || !got.PeakTime.Equal(start.Add(17*time.Second)) {
		t.Fatalf("peak time = %v", got.PeakTime)
	}
	if len(got.Subwindows) != 2 {
		t.Fatalf("subwindows = %d, want 2", len(got.Subwindows))
	}
	if len(got.PeakEvidenceMessages) != 1 || got.PeakEvidenceMessages[0].Text != "NO WAY" {
		t.Fatalf("peak evidence = %#v", got.PeakEvidenceMessages)
	}
}

func TestAttachPeakMetadataRejectsWindowsThatCrossBucketBoundary(t *testing.T) {
	start := mustParse("2026-05-08T12:00:00Z")
	bucket := chat.ChatBucket{
		Type:        "chat_bucket",
		SessionID:   "session",
		ChannelID:   "channel",
		BucketStart: start,
		BucketEnd:   start.Add(30 * time.Second),
	}

	got := AttachPeakMetadata(bucket, []chat.ReactionWindow{
		{
			WindowStart:    start.Add(29 * time.Second),
			WindowEnd:      start.Add(34 * time.Second),
			MessageCount:   20,
			IntensityScore: 1,
			ReactionType:   "hype",
		},
	})

	if got.PeakReactionScore != nil || len(got.Subwindows) != 0 {
		t.Fatalf("cross-boundary window should not attach: %#v", got)
	}
	if got.Subwindows == nil {
		t.Fatal("subwindows should be an empty slice for JSON []")
	}
	if got.PeakEvidenceMessages == nil {
		t.Fatal("peak evidence should be an empty slice for JSON []")
	}
	payload, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal bucket: %v", err)
	}
	for _, token := range []string{`"peak_reaction_score":null`, `"peak_window_start":null`, `"subwindows":[]`, `"peak_evidence_messages":[]`} {
		if !strings.Contains(string(payload), token) {
			t.Fatalf("bucket JSON missing %s: %s", token, payload)
		}
	}
}
