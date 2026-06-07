package chat

import "time"

// ChatMessage is the service's normalized internal message shape.
// Twitch-specific readers should translate raw chat events into this type.
type ChatMessage struct {
	SessionID   string    `json:"session_id"`
	ChannelID   string    `json:"channel_id"`
	MessageID   string    `json:"message_id"`
	Timestamp   time.Time `json:"timestamp"`
	Username    string    `json:"username"`
	DisplayName string    `json:"display_name"`
	Text        string    `json:"text"`
	Emotes      []string  `json:"emotes,omitempty"`
	Badges      []string  `json:"badges,omitempty"`
	Language    string    `json:"language"`
	IsMod       bool      `json:"is_mod"`
	IsBotLikely bool      `json:"is_bot_likely"`
}

type SentimentResult struct {
	Score      float64 `json:"score"`
	Confidence float64 `json:"confidence"`
	Label      string  `json:"label"`
}

type ScoredMessage struct {
	Message   ChatMessage
	Sentiment SentimentResult
}

type ReactionWindow struct {
	Type              string        `json:"type"`
	SessionID         string        `json:"session_id"`
	ChannelID         string        `json:"channel_id"`
	Source            string        `json:"source"`
	WindowStart       time.Time     `json:"window_start"`
	WindowEnd         time.Time     `json:"window_end"`
	MessageCount      int           `json:"message_count"`
	UniqueChatters    int           `json:"unique_chatters"`
	MessagesPerMinute float64       `json:"messages_per_minute"`
	VelocityScore     float64       `json:"velocity_score"`
	HypeScore         float64       `json:"hype_score"`
	IntensityScore    float64       `json:"intensity_score"`
	ConfusionScore    float64       `json:"confusion_score"`
	FrustrationScore  float64       `json:"frustration_score"`
	Valence           float64       `json:"valence"`
	ReactionType      string        `json:"reaction_type"`
	TargetType        string        `json:"target_type"`
	TargetText        string        `json:"target_text,omitempty"`
	EventHint         string        `json:"event_hint,omitempty"`
	Confidence        float64       `json:"confidence"`
	EvidenceIDs       []string      `json:"evidence_ids"`
	EvidenceMessages  []ChatMessage `json:"evidence_messages"`
}

type ChatBucket struct {
	Type                 string              `json:"type"`
	SessionID            string              `json:"session_id"`
	ChannelID            string              `json:"channel_id"`
	BucketStart          time.Time           `json:"bucket_start"`
	BucketEnd            time.Time           `json:"bucket_end"`
	MessageCount         int                 `json:"message_count"`
	UniqueChatters       int                 `json:"unique_chatters"`
	ChatSentiment        float64             `json:"chat_sentiment"`
	SentimentConfidence  float64             `json:"sentiment_confidence"`
	AnalyzedCount        int                 `json:"analyzed_count"`
	PositiveRatio        float64             `json:"positive"`
	NeutralRatio         float64             `json:"neutral"`
	NegativeRatio        float64             `json:"negative"`
	SentimentModel       string              `json:"sentiment_model,omitempty"`
	AnalysisLatencyMS    int64               `json:"analysis_latency_ms,omitempty"`
	AnalysisStatus       string              `json:"analysis_status,omitempty"`
	LanguageMix          map[string]float64  `json:"language_mix,omitempty"`
	TopTerms             []string            `json:"top_terms,omitempty"`
	TopEmotes            []string            `json:"top_emotes,omitempty"`
	MessageScores        []MessageScore      `json:"message_scores,omitempty"`
	PeakReactionScore    *float64            `json:"peak_reaction_score"`
	PeakReactionType     string              `json:"peak_reaction_type"`
	PeakTargetType       string              `json:"peak_target_type,omitempty"`
	PeakTargetText       string              `json:"peak_target_text,omitempty"`
	PeakSource           string              `json:"peak_source,omitempty"`
	PeakEventHint        string              `json:"peak_event_hint,omitempty"`
	PeakConfidence       float64             `json:"peak_confidence,omitempty"`
	PeakEvidenceIDs      []string            `json:"peak_evidence_ids,omitempty"`
	PeakTime             *time.Time          `json:"peak_time"`
	PeakWindowStart      *time.Time          `json:"peak_window_start"`
	PeakWindowEnd        *time.Time          `json:"peak_window_end"`
	Subwindows           []ReactionSubwindow `json:"subwindows"`
	PeakEvidenceMessages []ChatMessage       `json:"peak_evidence_messages"`
}

type MessageScore struct {
	MessageID      string    `json:"message_id"`
	Timestamp      time.Time `json:"timestamp,omitempty"`
	Username       string    `json:"username,omitempty"`
	DisplayName    string    `json:"display_name,omitempty"`
	Text           string    `json:"text"`
	Label          string    `json:"label"`
	Confidence     float64   `json:"confidence"`
	SentimentScore float64   `json:"sentiment_score"`
	HumanLabel     string    `json:"human_label,omitempty"`
}

type ReactionSubwindow struct {
	WindowStart      time.Time `json:"window_start"`
	WindowEnd        time.Time `json:"window_end"`
	MessageCount     int       `json:"message_count"`
	ReactionScore    float64   `json:"reaction_score"`
	HypeScore        float64   `json:"hype_score"`
	IntensityScore   float64   `json:"intensity_score"`
	ConfusionScore   float64   `json:"confusion_score"`
	FrustrationScore float64   `json:"frustration_score"`
	ReactionType     string    `json:"reaction_type"`
	TargetType       string    `json:"target_type"`
	TargetText       string    `json:"target_text,omitempty"`
	Source           string    `json:"source"`
	EventHint        string    `json:"event_hint,omitempty"`
	Confidence       float64   `json:"confidence"`
	EvidenceIDs      []string  `json:"evidence_ids,omitempty"`
}
