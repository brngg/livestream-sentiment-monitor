package storage

import (
	"context"
	"encoding/json"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

type Store interface {
	CreateSession(context.Context, SessionRecord) error
	FindSessionByTwitchStream(context.Context, string, string) (SessionRecord, error)
	UpdateSessionStatus(context.Context, SessionStatusUpdate) error
	SaveIngestionRun(context.Context, IngestionRunRecord) error
	UpdateIngestionRun(context.Context, IngestionRunUpdate) error
	SaveChatMessageSample(context.Context, ChatMessageSample) error
	SaveChatBucket(context.Context, chat.ChatBucket) error
	SaveTranscriptBucket(context.Context, TranscriptBucket) error
	SaveAlignment(context.Context, AlignmentBucket) error
	SaveHumanLabel(context.Context, HumanLabel) error
	SaveSignalWindowLabel(context.Context, SignalWindowLabel) error
	ListSignalWindowLabels(context.Context, string) ([]SignalWindowLabel, error)
	SaveEvaluationAgentReview(context.Context, EvaluationAgentReview) error
	ListEvaluationAgentReviews(context.Context, string) ([]EvaluationAgentReview, error)
	SaveMetric(context.Context, SystemMetric) error
	ListSessions(context.Context, int) ([]SessionHistory, error)
	GetSessionSummary(context.Context, string) (SessionSummary, error)
	GetSessionReplay(context.Context, string, int) (SessionReplay, error)
	Close()
}

type SessionRecord struct {
	SessionID               string
	ChannelID               string
	TwitchStreamID          string
	TwitchUserID            string
	Status                  string
	BucketSeconds           int
	TranscriptBucketSeconds int
	TranscriptChunkSeconds  int
	NLPAnalyzerURL          string
	SentimentModel          string
	StartedAt               time.Time
	FirstSeenAt             time.Time
	LastSeenAt              time.Time
}

type SessionStatusUpdate struct {
	SessionID         string
	ChannelID         string
	TwitchStreamID    string
	TwitchUserID      string
	Status            string
	Error             string
	StreamTitle       string
	StreamGame        string
	StreamViewerCount *int
	StreamStartedAt   *time.Time
	StreamLanguage    string
	EndedAt           *time.Time
}

type IngestionRunRecord struct {
	RunID      string
	SessionID  string
	StartedAt  time.Time
	EndedAt    *time.Time
	Status     string
	StopReason string
	Error      string
}

type IngestionRunUpdate struct {
	RunID      string
	SessionID  string
	EndedAt    *time.Time
	Status     string
	StopReason string
	Error      string
}

type ChatMessageSample struct {
	SessionID      string    `json:"session_id"`
	ChannelID      string    `json:"channel_id"`
	MessageID      string    `json:"message_id"`
	BucketStart    time.Time `json:"bucket_start"`
	BucketEnd      time.Time `json:"bucket_end"`
	Timestamp      time.Time `json:"timestamp"`
	UserHash       string    `json:"user_hash"`
	Text           string    `json:"text"`
	Label          string    `json:"label"`
	Confidence     float64   `json:"confidence"`
	SentimentScore float64   `json:"sentiment_score"`
	HumanLabel     string    `json:"human_label,omitempty"`
	EvidenceRank   int       `json:"evidence_rank"`
}

type TranscriptSegment struct {
	Start      float64          `json:"start"`
	End        float64          `json:"end"`
	Text       string           `json:"text"`
	Confidence *float64         `json:"confidence,omitempty"`
	Words      []TranscriptWord `json:"words,omitempty"`
}

type TranscriptWord struct {
	Start       float64  `json:"start"`
	End         float64  `json:"end"`
	Text        string   `json:"text"`
	Probability *float64 `json:"probability,omitempty"`
}

type TranscriptBucket struct {
	Type                 string              `json:"type"`
	SessionID            string              `json:"session_id"`
	ChannelID            string              `json:"channel_id"`
	BucketStart          time.Time           `json:"bucket_start"`
	BucketEnd            time.Time           `json:"bucket_end"`
	AudioStartedAt       *time.Time          `json:"audio_started_at,omitempty"`
	AudioEndedAt         *time.Time          `json:"audio_ended_at,omitempty"`
	TranscribedAt        *time.Time          `json:"transcribed_at,omitempty"`
	Text                 string              `json:"text"`
	Language             string              `json:"language"`
	TranscriptConfidence float64             `json:"transcript_confidence"`
	TranscriptStatus     string              `json:"transcript_status,omitempty"`
	SentimentScore       *float64            `json:"sentiment_score,omitempty"`
	SentimentConfidence  *float64            `json:"sentiment_confidence,omitempty"`
	SentimentLabel       string              `json:"sentiment_label,omitempty"`
	SentimentModel       string              `json:"sentiment_model,omitempty"`
	SentimentStatus      string              `json:"sentiment_status,omitempty"`
	SentimentLatencyMS   *int64              `json:"sentiment_latency_ms,omitempty"`
	ASRLatencyMS         *int64              `json:"asr_latency_ms,omitempty"`
	PipelineLatencyMS    *int64              `json:"pipeline_latency_ms,omitempty"`
	AudioSeconds         float64             `json:"audio_seconds,omitempty"`
	SegmentCount         int                 `json:"segment_count,omitempty"`
	WordCount            int                 `json:"word_count,omitempty"`
	EmptyRatio           float64             `json:"empty_ratio,omitempty"`
	RepairAddedWords     int                 `json:"repair_added_words,omitempty"`
	RepairChangedRatio   float64             `json:"repair_changed_ratio,omitempty"`
	Segments             []TranscriptSegment `json:"segments,omitempty"`
	Quality              map[string]any      `json:"quality,omitempty"`
}

type AlignmentBucket struct {
	Type                  string    `json:"type"`
	SessionID             string    `json:"session_id"`
	ChannelID             string    `json:"channel_id"`
	WindowStart           time.Time `json:"window_start"`
	WindowEnd             time.Time `json:"window_end"`
	ChatBucketStart       time.Time `json:"chat_bucket_start"`
	ChatBucketEnd         time.Time `json:"chat_bucket_end"`
	TranscriptBucketStart time.Time `json:"transcript_bucket_start"`
	TranscriptBucketEnd   time.Time `json:"transcript_bucket_end"`
	ChatSentiment         float64   `json:"chat_sentiment"`
	ChatConfidence        float64   `json:"chat_confidence"`
	ChatMessageCount      int       `json:"chat_message_count"`
	TranscriptSentiment   float64   `json:"transcript_sentiment"`
	TranscriptConfidence  float64   `json:"transcript_confidence"`
	TranscriptTextLength  int       `json:"transcript_text_length"`
	Delta                 float64   `json:"delta"`
	Similarity            float64   `json:"similarity"`
	Relationship          string    `json:"relationship"`
	OverlapSeconds        int       `json:"overlap_seconds"`
	Quality               float64   `json:"quality"`
	QualityFlags          []string  `json:"quality_flags"`
}

type HumanLabel struct {
	SessionID string    `json:"session_id"`
	MessageID string    `json:"message_id"`
	Label     string    `json:"label"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

type SignalWindowLabel struct {
	SessionID             string    `json:"session_id"`
	WindowStart           time.Time `json:"window_start"`
	WindowEnd             time.Time `json:"window_end"`
	PredictedEvent        string    `json:"predicted_event,omitempty"`
	PredictedRelationship string    `json:"predicted_relationship,omitempty"`
	ReactionType          string    `json:"reaction_type,omitempty"`
	TargetType            string    `json:"target_type,omitempty"`
	TargetText            string    `json:"target_text,omitempty"`
	DivergenceType        string    `json:"divergence_type,omitempty"`
	EventStart            time.Time `json:"event_start,omitempty"`
	EventPeak             time.Time `json:"event_peak,omitempty"`
	Correctness           string    `json:"correctness"`
	EventLabel            string    `json:"event_label"`
	Notes                 string    `json:"notes,omitempty"`
	CreatedAt             time.Time `json:"created_at,omitempty"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
}

type EvaluationAgentEvidence struct {
	ID        string         `json:"id,omitempty"`
	Source    string         `json:"source,omitempty"`
	Timestamp string         `json:"timestamp,omitempty"`
	Text      string         `json:"text,omitempty"`
	Meta      map[string]any `json:"meta,omitempty"`
}

type EvaluationAgentReview struct {
	ReviewID            string                    `json:"review_id,omitempty"`
	RunID               string                    `json:"run_id,omitempty"`
	SessionID           string                    `json:"session_id"`
	WindowStart         time.Time                 `json:"window_start"`
	WindowEnd           time.Time                 `json:"window_end"`
	SourceWindowType    string                    `json:"source_window_type,omitempty"`
	Reviewer            string                    `json:"reviewer,omitempty"`
	Model               string                    `json:"model,omitempty"`
	PromptVersion       string                    `json:"prompt_version,omitempty"`
	Status              string                    `json:"status,omitempty"`
	PredictedEvent      string                    `json:"predicted_event,omitempty"`
	SuggestedEventLabel string                    `json:"suggested_event_label"`
	Correctness         string                    `json:"correctness,omitempty"`
	ReactionType        string                    `json:"reaction_type,omitempty"`
	TargetType          string                    `json:"target_type,omitempty"`
	TargetText          string                    `json:"target_text,omitempty"`
	DivergenceType      string                    `json:"divergence_type,omitempty"`
	EventStart          *time.Time                `json:"event_start,omitempty"`
	EventPeak           *time.Time                `json:"event_peak,omitempty"`
	Confidence          float64                   `json:"confidence,omitempty"`
	StreamerUsefulness  float64                   `json:"streamer_usefulness,omitempty"`
	Reason              string                    `json:"reason,omitempty"`
	Evidence            []EvaluationAgentEvidence `json:"evidence,omitempty"`
	Notes               string                    `json:"notes,omitempty"`
	CreatedAt           time.Time                 `json:"created_at,omitempty"`
	UpdatedAt           time.Time                 `json:"updated_at,omitempty"`
}

func (l SignalWindowLabel) MarshalJSON() ([]byte, error) {
	type signalWindowLabelJSON struct {
		SessionID             string     `json:"session_id"`
		WindowStart           time.Time  `json:"window_start"`
		WindowEnd             time.Time  `json:"window_end"`
		PredictedEvent        string     `json:"predicted_event,omitempty"`
		PredictedRelationship string     `json:"predicted_relationship,omitempty"`
		ReactionType          string     `json:"reaction_type,omitempty"`
		TargetType            string     `json:"target_type,omitempty"`
		TargetText            string     `json:"target_text,omitempty"`
		DivergenceType        string     `json:"divergence_type,omitempty"`
		EventStart            *time.Time `json:"event_start,omitempty"`
		EventPeak             *time.Time `json:"event_peak,omitempty"`
		Correctness           string     `json:"correctness"`
		EventLabel            string     `json:"event_label"`
		Notes                 string     `json:"notes,omitempty"`
		CreatedAt             *time.Time `json:"created_at,omitempty"`
		UpdatedAt             *time.Time `json:"updated_at,omitempty"`
	}
	out := signalWindowLabelJSON{
		SessionID:             l.SessionID,
		WindowStart:           l.WindowStart,
		WindowEnd:             l.WindowEnd,
		PredictedEvent:        l.PredictedEvent,
		PredictedRelationship: l.PredictedRelationship,
		ReactionType:          l.ReactionType,
		TargetType:            l.TargetType,
		TargetText:            l.TargetText,
		DivergenceType:        l.DivergenceType,
		Correctness:           l.Correctness,
		EventLabel:            l.EventLabel,
		Notes:                 l.Notes,
	}
	if !l.EventStart.IsZero() {
		out.EventStart = &l.EventStart
	}
	if !l.EventPeak.IsZero() {
		out.EventPeak = &l.EventPeak
	}
	if !l.CreatedAt.IsZero() {
		out.CreatedAt = &l.CreatedAt
	}
	if !l.UpdatedAt.IsZero() {
		out.UpdatedAt = &l.UpdatedAt
	}
	return json.Marshal(out)
}

type SystemMetric struct {
	SessionID  string         `json:"session_id,omitempty"`
	Name       string         `json:"name"`
	Value      float64        `json:"value"`
	Unit       string         `json:"unit,omitempty"`
	RecordedAt time.Time      `json:"recorded_at,omitempty"`
	Meta       map[string]any `json:"meta,omitempty"`
}

type SessionHistory struct {
	SessionID             string     `json:"session_id"`
	ChannelID             string     `json:"channel_id"`
	TwitchStreamID        string     `json:"twitch_stream_id,omitempty"`
	TwitchUserID          string     `json:"twitch_user_id,omitempty"`
	Status                string     `json:"status"`
	StartedAt             time.Time  `json:"started_at"`
	EndedAt               *time.Time `json:"ended_at,omitempty"`
	StreamTitle           string     `json:"stream_title,omitempty"`
	StreamGame            string     `json:"stream_game,omitempty"`
	StreamViewerCount     *int       `json:"stream_viewer_count,omitempty"`
	ChatBucketCount       int        `json:"chat_bucket_count"`
	TranscriptBucketCount int        `json:"transcript_bucket_count"`
	AlignmentCount        int        `json:"alignment_count"`
	LabelCount            int        `json:"label_count"`
}

type SessionSummary struct {
	Session                 SessionHistory      `json:"session"`
	LatestChatBuckets       []chat.ChatBucket   `json:"latest_chat_buckets,omitempty"`
	LatestTranscriptBuckets []TranscriptBucket  `json:"latest_transcript_buckets,omitempty"`
	LatestAlignments        []AlignmentBucket   `json:"latest_alignments,omitempty"`
	WindowLabels            []SignalWindowLabel `json:"window_labels,omitempty"`
	LabelCount              int                 `json:"label_count"`
}

type SessionReplay struct {
	Session           SessionHistory          `json:"session"`
	ChatBuckets       []chat.ChatBucket       `json:"chat_buckets,omitempty"`
	TranscriptBuckets []TranscriptBucket      `json:"transcript_buckets,omitempty"`
	Alignments        []AlignmentBucket       `json:"alignments,omitempty"`
	WindowLabels      []SignalWindowLabel     `json:"window_labels,omitempty"`
	AgentReviews      []EvaluationAgentReview `json:"agent_reviews,omitempty"`
	SystemMetrics     []SystemMetric          `json:"system_metrics,omitempty"`
	LabelCount        int                     `json:"label_count"`
}

type ReplayProofOptions struct {
	GeneratedAt time.Time
	Speeds      []float64
	ReplayLimit int
}

type ReplayProof struct {
	Type                  string                        `json:"type"`
	SessionID             string                        `json:"session_id"`
	ChannelID             string                        `json:"channel_id,omitempty"`
	GeneratedAt           time.Time                     `json:"generated_at"`
	ReplayLimit           int                           `json:"replay_limit,omitempty"`
	Partial               bool                          `json:"partial"`
	SessionTotals         ReplayProofSessionTotals      `json:"session_totals"`
	TruncatedSources      []ReplayProofTruncation       `json:"truncated_sources,omitempty"`
	BucketCount           int                           `json:"bucket_count"`
	SourceBucketCount     int                           `json:"source_bucket_count"`
	ChatBucketCount       int                           `json:"chat_bucket_count"`
	TranscriptBucketCount int                           `json:"transcript_bucket_count"`
	AlignmentCount        int                           `json:"alignment_count"`
	SignalWindowCount     int                           `json:"signal_window_count"`
	MatchedWindows        int                           `json:"matched_windows"`
	LabelCoverage         ReplayProofLabelCoverage      `json:"label_coverage"`
	TranscriptCoverage    ReplayProofTranscriptCoverage `json:"transcript_coverage"`
	Timeline              ReplayProofTimeline           `json:"timeline"`
	Speeds                []ReplayProofSpeed            `json:"speeds"`
	Latency               ReplayProofLatency            `json:"latency"`
	DroppedEventRate      *float64                      `json:"dropped_event_rate"`
	UnsupportedMetrics    []ReplayProofUnsupported      `json:"unsupported_metrics,omitempty"`
}

type ReplayProofSessionTotals struct {
	BucketCount            int `json:"bucket_count"`
	SourceBucketCount      int `json:"source_bucket_count"`
	ChatBucketCount        int `json:"chat_bucket_count"`
	TranscriptBucketCount  int `json:"transcript_bucket_count"`
	AlignmentCount         int `json:"alignment_count"`
	SignalWindowLabelCount int `json:"signal_window_label_count"`
}

type ReplayProofTruncation struct {
	Source      string `json:"source"`
	LoadedCount int    `json:"loaded_count"`
	TotalCount  int    `json:"total_count"`
}

type ReplayProofLabelCoverage struct {
	LabeledWindows   int      `json:"labeled_windows"`
	UnmatchedLabels  int      `json:"unmatched_labels"`
	TotalWindows     int      `json:"total_windows"`
	Coverage         *float64 `json:"coverage"`
	StoredLabelCount int      `json:"stored_label_count"`
}

type ReplayProofTranscriptCoverage struct {
	BucketCount               int            `json:"bucket_count"`
	AudioSeconds              float64        `json:"audio_seconds"`
	ExpectedAudioSeconds      float64        `json:"expected_audio_seconds"`
	AudioCoverage             *float64       `json:"audio_coverage"`
	SegmentCount              int            `json:"segment_count"`
	WordCount                 int            `json:"word_count"`
	EmptyRatio                *float64       `json:"empty_ratio"`
	RepairAddedWords          int            `json:"repair_added_words"`
	AverageRepairChangedRatio *float64       `json:"average_repair_changed_ratio"`
	RepairImprovement         *float64       `json:"repair_improvement"`
	StatusCounts              map[string]int `json:"status_counts,omitempty"`
}

type ReplayProofTimeline struct {
	Start            *time.Time `json:"start"`
	End              *time.Time `json:"end"`
	SourceDurationMS int64      `json:"source_duration_ms"`
}

type ReplayProofSpeed struct {
	Speed                     float64 `json:"speed"`
	EstimatedReplayDurationMS int64   `json:"estimated_replay_duration_ms"`
	EstimatedReplaySeconds    float64 `json:"estimated_replay_seconds"`
	WindowsPerSecond          float64 `json:"windows_per_second"`
	BucketsPerSecond          float64 `json:"buckets_per_second"`
}

type ReplayProofLatency struct {
	ChatAnalysisLatencyMS           ReplayProofLatencySummary `json:"chat_analysis_latency_ms"`
	TranscriptSentimentLatencyMS    ReplayProofLatencySummary `json:"transcript_sentiment_latency_ms"`
	TranscriptASRLatencyMS          ReplayProofLatencySummary `json:"transcript_asr_latency_ms"`
	TranscriptPipelineLatencyMS     ReplayProofLatencySummary `json:"transcript_pipeline_latency_ms"`
	ChatAnalysisStatusCounts        map[string]int            `json:"chat_analysis_status_counts,omitempty"`
	TranscriptSentimentStatusCounts map[string]int            `json:"transcript_sentiment_status_counts,omitempty"`
}

type ReplayProofLatencySummary struct {
	AvailableCount int      `json:"available_count"`
	MissingCount   int      `json:"missing_count"`
	Min            *int64   `json:"min"`
	Max            *int64   `json:"max"`
	Average        *float64 `json:"average"`
	P50            *float64 `json:"p50"`
	P95            *float64 `json:"p95"`
}

type ReplayProofUnsupported struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}
