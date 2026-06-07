package filter

import (
	"strings"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

type MessageFilter struct {
	MinTextLength int
}

func (f MessageFilter) Allow(msg chat.ChatMessage) bool {
	text := strings.TrimSpace(msg.Text)
	minLength := f.MinTextLength
	if minLength == 0 {
		minLength = 2
	}

	if msg.IsBotLikely || (text == "" && len(msg.Emotes) == 0) {
		return false
	}
	if strings.HasPrefix(text, "!") {
		return false
	}
	if len([]rune(text)) < minLength && !allowShortReactionText(text) && len(msg.Emotes) == 0 {
		return false
	}

	return true
}

func allowShortReactionText(text string) bool {
	switch strings.ToUpper(strings.TrimSpace(text)) {
	case "W", "L", "?":
		return true
	default:
		return false
	}
}

func Normalize(msg chat.ChatMessage) chat.ChatMessage {
	msg.Text = strings.Join(strings.Fields(msg.Text), " ")
	if msg.Language == "" {
		msg.Language = "other"
	}
	return msg
}
