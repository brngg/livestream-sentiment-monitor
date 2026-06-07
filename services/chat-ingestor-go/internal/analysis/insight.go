package analysis

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

const insightType = "insight"

type InsightKind string

const (
	InsightAudienceShift             InsightKind = "audience_shift"
	InsightHypeSpike                 InsightKind = "hype_spike"
	InsightFrustrationSpike          InsightKind = "frustration_spike"
	InsightContentAudienceDivergence InsightKind = "content_audience_divergence"
	InsightTranscriptGap             InsightKind = "transcript_gap"
)

type InsightEvidence struct {
	Type      string    `json:"type"`
	Summary   string    `json:"summary"`
	Timestamp time.Time `json:"timestamp"`
	Value     *float64  `json:"value,omitempty"`
}

type Insight struct {
	Type        string            `json:"type"`
	ID          string            `json:"id"`
	SessionID   string            `json:"session_id"`
	ChannelID   string            `json:"channel_id"`
	Kind        InsightKind       `json:"kind"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Severity    float64           `json:"severity"`
	Confidence  float64           `json:"confidence"`
	WindowStart time.Time         `json:"window_start"`
	WindowEnd   time.Time         `json:"window_end"`
	Evidence    []InsightEvidence `json:"evidence,omitempty"`
}

type SessionInsightSummary struct {
	Type                  string      `json:"type"`
	SessionID             string      `json:"session_id"`
	InsightCount          int         `json:"insight_count"`
	HighSeverityCount     int         `json:"high_severity_count"`
	PrimaryInsightKind    InsightKind `json:"primary_insight_kind,omitempty"`
	TopInsightID          string      `json:"top_insight_id,omitempty"`
	MaxSeverity           float64     `json:"max_severity"`
	AverageSeverity       float64     `json:"average_severity"`
	ChatBucketCount       int         `json:"chat_bucket_count"`
	TranscriptBucketCount int         `json:"transcript_bucket_count"`
	AlignmentCount        int         `json:"alignment_count"`
	SignalWindowCount     int         `json:"signal_window_count"`
	EventCount            int         `json:"event_count"`
	TopMoments            []Insight   `json:"top_moments,omitempty"`
	BiggestDivergence     *Insight    `json:"biggest_divergence,omitempty"`
	HighestHype           *Insight    `json:"highest_hype,omitempty"`
	HighestFrustration    *Insight    `json:"highest_frustration,omitempty"`
	LowConfidenceFlags    []Insight   `json:"low_confidence_flags,omitempty"`
}

func GenerateSessionInsights(sessionID string, windows []SignalWindow, chatBuckets []chat.ChatBucket, transcriptBuckets []TranscriptBucket, alignments []AlignmentBucket) ([]Insight, SessionInsightSummary) {
	sessionID = firstNonEmpty(sessionID, sessionIDFromWindows(windows), sessionIDFromChatBuckets(chatBuckets), sessionIDFromTranscriptBuckets(transcriptBuckets), sessionIDFromAlignments(alignments))
	orderedWindows := append([]SignalWindow(nil), windows...)
	sort.SliceStable(orderedWindows, func(left, right int) bool {
		if orderedWindows[left].WindowStart.Equal(orderedWindows[right].WindowStart) {
			return orderedWindows[left].WindowEnd.Before(orderedWindows[right].WindowEnd)
		}
		return orderedWindows[left].WindowStart.Before(orderedWindows[right].WindowStart)
	})

	insights := make([]Insight, 0, len(orderedWindows))
	for _, window := range orderedWindows {
		for _, event := range orderedEvents(window.Events) {
			insights = append(insights, insightFromEvent(sessionID, window, event))
		}
	}
	insights = append(insights, transcriptGapInsights(sessionID, transcriptBuckets)...)
	sortInsights(insights)

	return insights, summarizeInsights(sessionID, insights, len(chatBuckets), len(transcriptBuckets), len(alignments), len(windows))
}

func insightFromEvent(sessionID string, window SignalWindow, event SignalEvent) Insight {
	kind := insightKindFromEvent(event.Type)
	title, description := insightCopy(kind, window)
	confidence := clamp01(window.SentimentConfidence)
	if window.AlignmentQuality != nil {
		confidence = clamp01((confidence + *window.AlignmentQuality) / 2)
	}
	if confidence == 0 && window.TranscriptConfidence != nil {
		confidence = clamp01(*window.TranscriptConfidence)
	}

	insight := Insight{
		Type:        insightType,
		SessionID:   firstNonEmpty(sessionID, window.SessionID),
		ChannelID:   window.ChannelID,
		Kind:        kind,
		Title:       title,
		Description: description,
		Severity:    clamp01(event.Severity),
		Confidence:  confidence,
		WindowStart: window.WindowStart,
		WindowEnd:   window.WindowEnd,
		Evidence:    insightEvidence(window),
	}
	insight.ID = deterministicInsightID(insight)
	return insight
}

func transcriptGapInsights(sessionID string, transcriptBuckets []TranscriptBucket) []Insight {
	var insights []Insight
	for _, bucket := range transcriptBuckets {
		if strings.TrimSpace(bucket.Text) != "" || bucket.TranscriptConfidence >= 0.35 {
			continue
		}
		insight := Insight{
			Type:        insightType,
			SessionID:   firstNonEmpty(sessionID, bucket.SessionID),
			ChannelID:   bucket.ChannelID,
			Kind:        InsightTranscriptGap,
			Title:       "Transcript evidence gap",
			Description: "Replay transcript evidence is missing or too low-confidence for this window.",
			Severity:    clamp01(1 - bucket.TranscriptConfidence),
			Confidence:  clamp01(1 - bucket.TranscriptConfidence),
			WindowStart: bucket.BucketStart,
			WindowEnd:   bucket.BucketEnd,
			Evidence: []InsightEvidence{
				{
					Type:      "transcript_confidence",
					Summary:   "Transcript confidence is below deterministic replay threshold.",
					Timestamp: bucket.BucketStart,
					Value:     floatPtr(bucket.TranscriptConfidence),
				},
			},
		}
		insight.ID = deterministicInsightID(insight)
		insights = append(insights, insight)
	}
	return insights
}

func insightEvidence(window SignalWindow) []InsightEvidence {
	evidence := []InsightEvidence{
		{
			Type:      "chat_sentiment",
			Summary:   "Chat sentiment in replay signal window.",
			Timestamp: window.WindowStart,
			Value:     floatPtr(window.ChatSentiment),
		},
	}
	if window.PreviousSentiment != nil {
		evidence = append(evidence, InsightEvidence{
			Type:      "previous_chat_sentiment",
			Summary:   "Previous chat sentiment for the same session and channel.",
			Timestamp: window.WindowStart,
			Value:     window.PreviousSentiment,
		})
	}
	if window.TranscriptSentiment != nil {
		evidence = append(evidence, InsightEvidence{
			Type:      "transcript_sentiment",
			Summary:   "Aligned transcript sentiment in replay window.",
			Timestamp: window.WindowStart,
			Value:     window.TranscriptSentiment,
		})
	}
	if window.AlignmentDelta != nil {
		evidence = append(evidence, InsightEvidence{
			Type:      "alignment_delta",
			Summary:   "Difference between chat and transcript sentiment.",
			Timestamp: window.WindowStart,
			Value:     window.AlignmentDelta,
		})
	}
	return evidence
}

func summarizeInsights(sessionID string, insights []Insight, chatBucketCount, transcriptBucketCount, alignmentCount, signalWindowCount int) SessionInsightSummary {
	summary := SessionInsightSummary{
		Type:                  "session_insight_summary",
		SessionID:             sessionID,
		InsightCount:          len(insights),
		ChatBucketCount:       chatBucketCount,
		TranscriptBucketCount: transcriptBucketCount,
		AlignmentCount:        alignmentCount,
		SignalWindowCount:     signalWindowCount,
	}
	if len(insights) == 0 {
		return summary
	}

	var totalSeverity float64
	top := insights[0]
	for _, insight := range insights {
		totalSeverity += insight.Severity
		if insight.Severity >= 0.7 {
			summary.HighSeverityCount++
		}
		if insight.Severity > top.Severity || (insight.Severity == top.Severity && insight.ID < top.ID) {
			top = insight
		}
		if insight.Kind != InsightTranscriptGap {
			summary.EventCount++
		}
		switch insight.Kind {
		case InsightContentAudienceDivergence:
			if summary.BiggestDivergence == nil || insightRank(insight) > insightRank(*summary.BiggestDivergence) {
				item := insight
				summary.BiggestDivergence = &item
			}
		case InsightHypeSpike:
			if summary.HighestHype == nil || insightRank(insight) > insightRank(*summary.HighestHype) {
				item := insight
				summary.HighestHype = &item
			}
		case InsightFrustrationSpike:
			if summary.HighestFrustration == nil || insightRank(insight) > insightRank(*summary.HighestFrustration) {
				item := insight
				summary.HighestFrustration = &item
			}
		}
		if insight.Confidence < 0.55 || insight.Kind == InsightTranscriptGap {
			summary.LowConfidenceFlags = append(summary.LowConfidenceFlags, insight)
		}
	}
	summary.PrimaryInsightKind = top.Kind
	summary.TopInsightID = top.ID
	summary.MaxSeverity = top.Severity
	summary.AverageSeverity = roundFloat(totalSeverity / float64(len(insights)))
	summary.TopMoments = topRankedInsights(insights, 3)
	summary.LowConfidenceFlags = topRankedInsights(summary.LowConfidenceFlags, 3)
	return summary
}

func topRankedInsights(insights []Insight, limit int) []Insight {
	if len(insights) == 0 || limit <= 0 {
		return nil
	}
	out := append([]Insight(nil), insights...)
	sort.SliceStable(out, func(left, right int) bool {
		leftRank := insightRank(out[left])
		rightRank := insightRank(out[right])
		if leftRank == rightRank {
			return out[left].WindowStart.Before(out[right].WindowStart)
		}
		return leftRank > rightRank
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func insightRank(insight Insight) float64 {
	return roundFloat((insight.Severity * 0.7) + (insight.Confidence * 0.3))
}

func orderedEvents(events []SignalEvent) []SignalEvent {
	out := append([]SignalEvent(nil), events...)
	sort.SliceStable(out, func(left, right int) bool {
		if out[left].Timestamp.Equal(out[right].Timestamp) {
			return string(out[left].Type) < string(out[right].Type)
		}
		return out[left].Timestamp.Before(out[right].Timestamp)
	})
	return out
}

func sortInsights(insights []Insight) {
	sort.SliceStable(insights, func(left, right int) bool {
		if insights[left].WindowStart.Equal(insights[right].WindowStart) {
			if insights[left].Severity == insights[right].Severity {
				return insights[left].ID < insights[right].ID
			}
			return insights[left].Severity > insights[right].Severity
		}
		return insights[left].WindowStart.Before(insights[right].WindowStart)
	})
}

func insightKindFromEvent(eventType SignalEventType) InsightKind {
	switch eventType {
	case SignalEventAudienceShift:
		return InsightAudienceShift
	case SignalEventHypeSpike:
		return InsightHypeSpike
	case SignalEventFrustrationSpike:
		return InsightFrustrationSpike
	case SignalEventContentAudienceDivergence:
		return InsightContentAudienceDivergence
	default:
		return InsightKind(eventType)
	}
}

func insightCopy(kind InsightKind, window SignalWindow) (string, string) {
	switch kind {
	case InsightAudienceShift:
		return "Audience sentiment shifted", "Chat sentiment changed sharply from the previous replay window."
	case InsightHypeSpike:
		return "Audience hype spike", "Positive chat reaction exceeded the deterministic spike threshold."
	case InsightFrustrationSpike:
		return "Audience frustration spike", "Negative chat reaction exceeded the deterministic spike threshold."
	case InsightContentAudienceDivergence:
		if window.Relationship != "" {
			return "Content and audience diverged", fmt.Sprintf("Chat and transcript sentiment relationship is %s in this replay window.", window.Relationship)
		}
		return "Content and audience diverged", "Chat and transcript sentiment moved apart in this replay window."
	default:
		return "Replay insight", "Deterministic replay analysis found a notable session signal."
	}
}

func deterministicInsightID(insight Insight) string {
	hash := sha1.Sum([]byte(fmt.Sprintf("%s|%s|%s|%s|%s|%.3f",
		insight.SessionID,
		insight.ChannelID,
		insight.Kind,
		insight.WindowStart.UTC().Format(time.RFC3339Nano),
		insight.WindowEnd.UTC().Format(time.RFC3339Nano),
		roundFloat(insight.Severity),
	)))
	return "ins_" + hex.EncodeToString(hash[:8])
}

func roundFloat(value float64) float64 {
	return math.Round(value*1000) / 1000
}

func floatPtr(value float64) *float64 {
	return &value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func sessionIDFromWindows(windows []SignalWindow) string {
	for _, window := range windows {
		if window.SessionID != "" {
			return window.SessionID
		}
	}
	return ""
}

func sessionIDFromChatBuckets(buckets []chat.ChatBucket) string {
	for _, bucket := range buckets {
		if bucket.SessionID != "" {
			return bucket.SessionID
		}
	}
	return ""
}

func sessionIDFromTranscriptBuckets(buckets []TranscriptBucket) string {
	for _, bucket := range buckets {
		if bucket.SessionID != "" {
			return bucket.SessionID
		}
	}
	return ""
}

func sessionIDFromAlignments(alignments []AlignmentBucket) string {
	for _, alignment := range alignments {
		if alignment.SessionID != "" {
			return alignment.SessionID
		}
	}
	return ""
}
