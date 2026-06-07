package sentiment

import (
	"strings"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

type Analyzer interface {
	Analyze(msg chat.ChatMessage) chat.SentimentResult
}

type LexiconAnalyzer struct {
	Positive map[string]float64
	Negative map[string]float64
}

func NewLexiconAnalyzer() LexiconAnalyzer {
	return LexiconAnalyzer{
		Positive: map[string]float64{
			"amazing": 1, "clutch": 0.9, "funny": 0.4, "gg": 0.8, "go": 0.4,
			"great": 0.9, "hype": 0.8, "insane": 0.7, "love": 1, "pogchamp": 0.9,
			"recovery": 0.4,
		},
		Negative: map[string]float64{
			"awful": -1, "bad": -0.7, "hate": -1, "rough": -0.6, "sad": -0.5,
			"terrible": -1, "timing": -0.2,
		},
	}
}

func (a LexiconAnalyzer) Analyze(msg chat.ChatMessage) chat.SentimentResult {
	positive := a.Positive
	if positive == nil {
		positive = NewLexiconAnalyzer().Positive
	}
	negative := a.Negative
	if negative == nil {
		negative = NewLexiconAnalyzer().Negative
	}

	var score float64
	var hits int
	for _, token := range tokens(msg.Text) {
		if value, ok := positive[token]; ok {
			score += value
			hits++
		}
		if value, ok := negative[token]; ok {
			score += value
			hits++
		}
	}
	for _, emote := range msg.Emotes {
		if value, ok := positive[strings.ToLower(emote)]; ok {
			score += value
			hits++
		}
	}

	if hits == 0 {
		return chat.SentimentResult{Score: 0, Confidence: 0.25, Label: "neutral"}
	}

	normalized := clamp(score/float64(hits), -1, 1)
	label := "neutral"
	if normalized > 0.15 {
		label = "positive"
	}
	if normalized < -0.15 {
		label = "negative"
	}

	confidence := clamp(0.35+float64(hits)*0.12, 0.35, 0.85)
	return chat.SentimentResult{Score: normalized, Confidence: confidence, Label: label}
}

func tokens(text string) []string {
	parts := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	})
	out := parts[:0]
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}
