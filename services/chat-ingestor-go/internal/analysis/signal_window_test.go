package analysis

import (
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestSignalWindowsFromChatBucketsDetectsAudienceShiftFirst(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	windows := SignalWindowsFromChatBuckets([]chat.ChatBucket{
		{
			SessionID:           "session",
			ChannelID:           "channel",
			BucketStart:         start.Add(30 * time.Second),
			BucketEnd:           start.Add(60 * time.Second),
			MessageCount:        18,
			UniqueChatters:      12,
			ChatSentiment:       0.4,
			SentimentConfidence: 0.8,
			PositiveRatio:       0.6,
			NeutralRatio:        0.2,
			NegativeRatio:       0.2,
		},
		{
			SessionID:     "session",
			ChannelID:     "channel",
			BucketStart:   start,
			BucketEnd:     start.Add(30 * time.Second),
			ChatSentiment: -0.3,
		},
	})

	if len(windows) != 2 {
		t.Fatalf("expected 2 signal windows, got %d", len(windows))
	}
	got := windows[0]
	if got.Type != "signal_window" {
		t.Fatalf("expected signal_window type, got %q", got.Type)
	}
	if got.FirstEventType != SignalEventAudienceShift {
		t.Fatalf("expected first event audience_shift, got %q", got.FirstEventType)
	}
	if got.TargetType != "unknown" || got.Source != "chat" {
		t.Fatalf("expected conservative context, got %#v", got)
	}
	if len(got.Events) != 2 {
		t.Fatalf("expected audience shift and hype spike events, got %#v", got.Events)
	}
	if got.Events[0].Confidence <= 0 || got.Events[0].TargetType != "unknown" || got.Events[0].Source == "" {
		t.Fatalf("expected event context, got %#v", got.Events[0])
	}
	if got.PreviousSentiment == nil || *got.PreviousSentiment != -0.3 {
		t.Fatalf("expected previous sentiment -0.3, got %#v", got.PreviousSentiment)
	}
}

func TestNewSignalWindowDetectsHypeAndFrustrationSpikes(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	hype := NewSignalWindow(chat.ChatBucket{
		SessionID:     "session",
		ChannelID:     "channel",
		BucketStart:   start,
		BucketEnd:     start.Add(30 * time.Second),
		ChatSentiment: 0.5,
		PositiveRatio: 0.7,
	}, nil)
	if hype.FirstEventType != SignalEventHypeSpike {
		t.Fatalf("expected hype spike, got %q", hype.FirstEventType)
	}

	frustration := NewSignalWindow(chat.ChatBucket{
		SessionID:     "session",
		ChannelID:     "channel",
		BucketStart:   start,
		BucketEnd:     start.Add(30 * time.Second),
		ChatSentiment: -0.6,
		NegativeRatio: 0.65,
	}, nil)
	if frustration.FirstEventType != SignalEventFrustrationSpike {
		t.Fatalf("expected frustration spike, got %q", frustration.FirstEventType)
	}
}

func TestNewSignalWindowIncludesTargetAndEvidenceContext(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	window := NewSignalWindow(chat.ChatBucket{
		SessionID:           "session",
		ChannelID:           "channel",
		BucketStart:         start,
		BucketEnd:           start.Add(30 * time.Second),
		ChatSentiment:       0.6,
		SentimentConfidence: 0.8,
		PositiveRatio:       0.7,
		TopTerms:            []string{"dragon"},
		MessageScores: []chat.MessageScore{
			{MessageID: "m1", Text: "dragon steal insane"},
			{MessageID: "m2", Text: "dragon fight was wild"},
		},
	}, nil)

	if window.ReactionType != "hype" {
		t.Fatalf("window reaction type = %q, want hype", window.ReactionType)
	}
	if window.TargetType != "unknown" || window.TargetText != "dragon" {
		t.Fatalf("expected unknown target type and dragon target text, got %#v", window)
	}
	if window.Source != "chat" || window.EventHint != "hype_spike:dragon" || window.Confidence != 0.8 {
		t.Fatalf("expected chat window context, got %#v", window)
	}
	if len(window.Events) != 1 || window.Events[0].ReactionType != "hype" {
		t.Fatalf("expected hype event context, got %#v", window.Events)
	}
	if window.Events[0].TargetText != "dragon" || len(window.Events[0].EvidenceIDs) != 2 {
		t.Fatalf("expected event evidence context, got %#v", window.Events[0])
	}
}

func TestNewSignalWindowSuppressesWeakNeutralTargetContext(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	window := NewSignalWindow(chat.ChatBucket{
		SessionID:           "session",
		ChannelID:           "channel",
		BucketStart:         start,
		BucketEnd:           start.Add(30 * time.Second),
		ChatSentiment:       0.05,
		SentimentConfidence: 0.9,
		TopTerms:            []string{"dragon"},
		MessageScores: []chat.MessageScore{
			{MessageID: "m1", Text: "dragon path through river"},
			{MessageID: "m2", Text: "dragon route through jungle"},
		},
	}, nil)

	if window.ReactionType != "neutral" || window.TargetType != "unknown" || window.TargetText != "" || window.EventHint != "neutral" {
		t.Fatalf("neutral window should not emit target context: %#v", window)
	}
}

func TestNewSignalWindowSuppressesLowConfidenceTargetContext(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	window := NewSignalWindow(chat.ChatBucket{
		SessionID:           "session",
		ChannelID:           "channel",
		BucketStart:         start,
		BucketEnd:           start.Add(30 * time.Second),
		ChatSentiment:       0.6,
		SentimentConfidence: 0.2,
		PositiveRatio:       0.7,
		TopTerms:            []string{"dragon"},
		MessageScores: []chat.MessageScore{
			{MessageID: "m1", Text: "dragon steal insane"},
			{MessageID: "m2", Text: "dragon fight was wild"},
		},
	}, nil)

	if window.ReactionType != "neutral" || window.TargetType != "unknown" || window.TargetText != "" || window.EventHint != "neutral" {
		t.Fatalf("low-confidence window should not emit target context: %#v", window)
	}
	for _, event := range window.Events {
		if event.TargetText != "" {
			t.Fatalf("low-confidence event should not inherit target text: %#v", event)
		}
	}
}

func TestSignalWindowsFromAlignmentsDetectsContentAudienceDivergence(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	windows := SignalWindowsFromAlignments([]AlignmentBucket{
		{
			SessionID:           "session",
			ChannelID:           "channel",
			WindowStart:         start,
			WindowEnd:           start.Add(30 * time.Second),
			ChatSentiment:       -0.4,
			ChatConfidence:      0.8,
			ChatMessageCount:    24,
			TranscriptSentiment: 0.35,
			Delta:               -0.75,
			Quality:             0.9,
		},
	})

	if len(windows) != 1 {
		t.Fatalf("expected 1 signal window, got %d", len(windows))
	}
	got := windows[0]
	if got.FirstEventType != SignalEventContentAudienceDivergence {
		t.Fatalf("expected content divergence, got %q", got.FirstEventType)
	}
	if got.TranscriptSentiment == nil || *got.TranscriptSentiment != 0.35 {
		t.Fatalf("expected transcript sentiment 0.35, got %#v", got.TranscriptSentiment)
	}
	if got.AlignmentDelta == nil || *got.AlignmentDelta != -0.75 {
		t.Fatalf("expected alignment delta -0.75, got %#v", got.AlignmentDelta)
	}
	if got.Source != "alignment" || got.TargetType != "unknown" || got.ReactionType != "frustration" {
		t.Fatalf("expected alignment context, got %#v", got)
	}
	if len(got.Events) != 1 || got.Events[0].Source != "alignment" || got.Events[0].ReactionType != "frustration" || got.Events[0].Confidence != 0.85 {
		t.Fatalf("expected alignment event context, got %#v", got.Events)
	}
}

func TestSignalWindowsFromChatAndAlignmentsMergesAudienceAndDivergenceEvents(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	windows := SignalWindowsFromChatAndAlignments(
		[]chat.ChatBucket{
			{
				SessionID:     "session",
				ChannelID:     "channel",
				BucketStart:   start,
				BucketEnd:     start.Add(30 * time.Second),
				ChatSentiment: -0.3,
			},
			{
				SessionID:     "session",
				ChannelID:     "channel",
				BucketStart:   start.Add(30 * time.Second),
				BucketEnd:     start.Add(60 * time.Second),
				ChatSentiment: 0.45,
				PositiveRatio: 0.7,
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
				ChatSentiment:         0.45,
				ChatConfidence:        0.8,
				TranscriptSentiment:   -0.3,
				TranscriptConfidence:  0.9,
				Delta:                 0.75,
				Similarity:            0.625,
				Relationship:          "diverged",
				Quality:               0.85,
			},
		},
	)

	if len(windows) != 2 {
		t.Fatalf("expected 2 signal windows, got %d", len(windows))
	}
	got := windows[1]
	if got.TranscriptSentiment == nil || *got.TranscriptSentiment != -0.3 {
		t.Fatalf("expected merged transcript sentiment, got %#v", got.TranscriptSentiment)
	}
	if got.Relationship != "diverged" {
		t.Fatalf("expected merged relationship, got %q", got.Relationship)
	}
	for _, expected := range []SignalEventType{SignalEventAudienceShift, SignalEventHypeSpike, SignalEventContentAudienceDivergence} {
		if !hasSignalEvent(got.Events, expected) {
			t.Fatalf("expected event %q in %#v", expected, got.Events)
		}
	}
	for _, event := range got.Events {
		if event.Source == "chat" && event.Confidence == got.Confidence {
			t.Fatalf("chat event inherited alignment confidence: event=%#v window=%#v", event, got)
		}
		if event.Source == "chat" && event.Confidence != event.Severity {
			t.Fatalf("zero-confidence chat event should fall back to severity: %#v", event)
		}
	}
}

func TestSignalWindowsFromTranscriptBucketsUsesAlignmentSemantics(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := 0.4
	windows := SignalWindowsFromTranscriptBuckets(
		[]chat.ChatBucket{
			{
				SessionID:           "session",
				ChannelID:           "channel",
				BucketStart:         start,
				BucketEnd:           start.Add(30 * time.Second),
				MessageCount:        20,
				ChatSentiment:       -0.3,
				SentimentConfidence: 0.8,
			},
		},
		[]TranscriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start,
				BucketEnd:            start.Add(30 * time.Second),
				Text:                 "the transcript bucket has enough text and sentiment for a meaningful alignment window",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
		},
		30*time.Second,
	)

	if len(windows) != 1 {
		t.Fatalf("expected 1 signal window, got %d", len(windows))
	}
	if windows[0].FirstEventType != SignalEventContentAudienceDivergence {
		t.Fatalf("expected content divergence, got %q", windows[0].FirstEventType)
	}
}

func TestSignalWindowsAttachTranscriptSnippetAndRepeatedTranscriptTarget(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	score := -0.4
	windows := SignalWindowsFromTranscriptBuckets(
		[]chat.ChatBucket{
			{
				SessionID:           "session",
				ChannelID:           "channel",
				BucketStart:         start,
				BucketEnd:           start.Add(30 * time.Second),
				MessageCount:        18,
				ChatSentiment:       0.5,
				SentimentConfidence: 0.7,
				PositiveRatio:       0.7,
				MessageScores: []chat.MessageScore{
					{MessageID: "m1", Text: "what happened"},
					{MessageID: "m2", Text: "no way"},
				},
			},
		},
		[]TranscriptBucket{
			{
				SessionID:            "session",
				ChannelID:            "channel",
				BucketStart:          start,
				BucketEnd:            start.Add(30 * time.Second),
				Text:                 "the boss fight started and the boss fight got much harder than expected",
				TranscriptConfidence: 0.9,
				SentimentScore:       &score,
			},
		},
		30*time.Second,
	)

	if len(windows) != 1 {
		t.Fatalf("expected 1 signal window, got %d", len(windows))
	}
	got := windows[0]
	if got.TargetType != "unknown" || got.TargetText != "boss fight" {
		t.Fatalf("expected conservative transcript target, got %#v", got)
	}
	if !hasString(got.EvidenceIDs, "transcript:"+start.Format(time.RFC3339Nano)) {
		t.Fatalf("expected transcript evidence id, got %#v", got.EvidenceIDs)
	}
	var divergence SignalEvent
	for _, event := range got.Events {
		if event.Type == SignalEventContentAudienceDivergence {
			divergence = event
			break
		}
	}
	if divergence.Type == "" || divergence.Text == "" || divergence.TargetText != "boss fight" {
		t.Fatalf("expected divergence event with transcript evidence text and target, got %#v", got.Events)
	}
}

func hasSignalEvent(events []SignalEvent, expected SignalEventType) bool {
	for _, event := range events {
		if event.Type == expected {
			return true
		}
	}
	return false
}

func hasString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
