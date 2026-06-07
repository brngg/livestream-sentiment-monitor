package filter

import (
	"testing"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func TestMessageFilterAllowsShortReactionMessages(t *testing.T) {
	filter := MessageFilter{}

	for _, text := range []string{"W", "L", "?"} {
		if !filter.Allow(chat.ChatMessage{Text: text}) {
			t.Fatalf("expected %q to pass reaction filter", text)
		}
	}
}

func TestMessageFilterRejectsOtherSingleCharacterNoise(t *testing.T) {
	filter := MessageFilter{}

	if filter.Allow(chat.ChatMessage{Text: "x"}) {
		t.Fatal("expected unrelated one-character message to be rejected")
	}
}

func TestMessageFilterAllowsEmoteOnlyMessages(t *testing.T) {
	filter := MessageFilter{}

	if !filter.Allow(chat.ChatMessage{Emotes: []string{"PogChamp"}}) {
		t.Fatal("expected emote-only message to pass reaction filter")
	}
}
