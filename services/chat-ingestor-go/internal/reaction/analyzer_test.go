package reaction

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestAnalyzerScoresShortHypeBurstWithoutPositiveSentiment(t *testing.T) {
	analyzer := NewAnalyzer(5*time.Second, 5*time.Minute)
	start := mustParse("2026-05-08T12:00:00Z")
	for index, text := range []string{"W", "NO WAY", "HOLY", "INSANE!!!", "LFG"} {
		analyzer.Add(testMessage(start.Add(time.Duration(index)*time.Second), text, "viewer"+string(rune('a'+index)), nil))
	}

	window := analyzer.WindowAt(start.Add(5*time.Second), "session", "channel")

	if window.Type != "reaction_window" {
		t.Fatalf("type = %q, want reaction_window", window.Type)
	}
	if window.MessageCount != 5 || window.UniqueChatters != 5 {
		t.Fatalf("unexpected counts: %#v", window)
	}
	if window.ReactionType != "hype" {
		t.Fatalf("reaction type = %q, want hype", window.ReactionType)
	}
	if window.HypeScore <= 0.7 {
		t.Fatalf("hype score = %.3f, want > 0.7", window.HypeScore)
	}
	if window.MessagesPerMinute != 60 {
		t.Fatalf("messages per minute = %.3f, want 60", window.MessagesPerMinute)
	}
	if len(window.EvidenceMessages) == 0 {
		t.Fatal("expected evidence messages")
	}
	if window.Source != "chat" || window.TargetType != "unknown" || window.Confidence <= 0 {
		t.Fatalf("expected lightweight context fields, got %#v", window)
	}
	if len(window.EvidenceIDs) == 0 {
		t.Fatalf("expected evidence ids, got %#v", window.EvidenceIDs)
	}
}

func TestAnalyzerExtractsRepeatedTargetTextConservatively(t *testing.T) {
	analyzer := NewAnalyzer(5*time.Second, 5*time.Minute)
	start := mustParse("2026-05-08T12:00:00Z")
	analyzer.Add(testMessage(start, "dragon fight was insane", "viewer1", nil))
	analyzer.Add(testMessage(start.Add(time.Second), "dragon steal no way", "viewer2", nil))

	window := analyzer.WindowAt(start.Add(5*time.Second), "session", "channel")

	if window.TargetType != "unknown" {
		t.Fatalf("target type = %q, want unknown", window.TargetType)
	}
	if window.TargetText != "dragon" {
		t.Fatalf("target text = %q, want dragon", window.TargetText)
	}
	if window.EventHint != "hype:dragon" {
		t.Fatalf("event hint = %q, want hype:dragon", window.EventHint)
	}
}

func TestAnalyzerDropsRepeatedTargetForWeakNeutralWindow(t *testing.T) {
	analyzer := NewAnalyzer(5*time.Second, 5*time.Minute)
	start := mustParse("2026-05-08T12:00:00Z")
	analyzer.Add(testMessage(start, "dragon path through river", "viewer1", nil))
	analyzer.Add(testMessage(start.Add(time.Second), "dragon route through jungle", "viewer2", nil))

	window := analyzer.WindowAt(start.Add(5*time.Second), "session", "channel")

	if window.ReactionType != "neutral" {
		t.Fatalf("reaction type = %q, want neutral", window.ReactionType)
	}
	if window.TargetText != "" || window.EventHint != "neutral" {
		t.Fatalf("weak target should stay unknown, got target=%q hint=%q", window.TargetText, window.EventHint)
	}
}

func TestAnalyzerSeparatesConfusionAndFrustration(t *testing.T) {
	start := mustParse("2026-05-08T12:00:00Z")

	confusion := NewAnalyzer(5*time.Second, 5*time.Minute)
	confusion.Add(testMessage(start, "HUH???", "viewer1", nil))
	confusion.Add(testMessage(start.Add(time.Second), "wait what?", "viewer2", nil))
	confusionWindow := confusion.WindowAt(start.Add(5*time.Second), "session", "channel")
	if confusionWindow.ReactionType != "confusion" {
		t.Fatalf("confusion reaction type = %q", confusionWindow.ReactionType)
	}
	if confusionWindow.ConfusionScore <= confusionWindow.HypeScore {
		t.Fatalf("confusion score should dominate: %#v", confusionWindow)
	}

	frustration := NewAnalyzer(5*time.Second, 5*time.Minute)
	frustration.Add(testMessage(start, "L", "viewer1", nil))
	frustration.Add(testMessage(start.Add(time.Second), "bad throw", "viewer2", nil))
	frustration.Add(testMessage(start.Add(2*time.Second), "CHOKE", "viewer3", nil))
	frustrationWindow := frustration.WindowAt(start.Add(5*time.Second), "session", "channel")
	if frustrationWindow.ReactionType != "frustration" {
		t.Fatalf("frustration reaction type = %q", frustrationWindow.ReactionType)
	}
	if frustrationWindow.FrustrationScore <= frustrationWindow.HypeScore {
		t.Fatalf("frustration score should dominate: %#v", frustrationWindow)
	}
	if frustrationWindow.Valence >= 0 {
		t.Fatalf("frustration valence = %.3f, want negative", frustrationWindow.Valence)
	}
}

func TestAnalyzerTreatsCapsOnlyBurstAsHype(t *testing.T) {
	analyzer := NewAnalyzer(5*time.Second, 5*time.Minute)
	start := mustParse("2026-05-08T12:00:00Z")
	analyzer.Add(testMessage(start, "ARE YOU SERIOUS", "viewer1", nil))
	analyzer.Add(testMessage(start.Add(time.Second), "THAT WAS WILD", "viewer2", nil))

	window := analyzer.WindowAt(start.Add(5*time.Second), "session", "channel")

	if window.ReactionType != "hype" {
		t.Fatalf("reaction type = %q, want hype: %#v", window.ReactionType, window)
	}
	if window.HypeScore < 0.2 {
		t.Fatalf("hype score = %.3f, want >= 0.2", window.HypeScore)
	}
}

func TestAnalyzerSerializesEmptyEvidenceMessages(t *testing.T) {
	analyzer := NewAnalyzer(5*time.Second, 5*time.Minute)
	start := mustParse("2026-05-08T12:00:00Z")

	window := analyzer.WindowAt(start.Add(5*time.Second), "session", "channel")

	if window.EvidenceMessages == nil {
		t.Fatal("expected empty evidence slice, got nil")
	}
	payload, err := json.Marshal(window)
	if err != nil {
		t.Fatalf("marshal window: %v", err)
	}
	if !strings.Contains(string(payload), `"evidence_messages":[]`) {
		t.Fatalf("expected empty evidence_messages array in JSON, got %s", payload)
	}
}

func TestAnalyzerRetainsOnlyFiveMinutesOfWindows(t *testing.T) {
	analyzer := NewAnalyzer(5*time.Second, 5*time.Minute)
	start := mustParse("2026-05-08T12:00:00Z")

	for index := 0; index < 310; index++ {
		now := start.Add(time.Duration(index) * time.Second)
		analyzer.Add(testMessage(now, "W", "viewer", nil))
		analyzer.WindowAt(now.Add(time.Second), "session", "channel")
	}

	windows := analyzer.RecentWindows()
	if len(windows) > 301 {
		t.Fatalf("retained window count = %d, want <= 301", len(windows))
	}
	oldest := windows[len(windows)-1]
	if oldest.WindowEnd.Before(start.Add(10 * time.Second)) {
		t.Fatalf("oldest retained window too old: %s", oldest.WindowEnd)
	}
}

func testMessage(at time.Time, text, username string, emotes []string) chat.ChatMessage {
	return chat.ChatMessage{
		SessionID:   "session",
		ChannelID:   "channel",
		MessageID:   username + "-" + at.Format(time.RFC3339Nano),
		Timestamp:   at,
		Username:    username,
		DisplayName: username,
		Text:        text,
		Emotes:      emotes,
		Language:    "en",
	}
}

func mustParse(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}
