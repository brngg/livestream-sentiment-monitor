package analysis

import (
	"math"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

const (
	defaultAlignmentWindow = 30 * time.Second
	maxAlignmentBuckets    = 80
)

type TranscriptBucket struct {
	Type                 string    `json:"type"`
	SessionID            string    `json:"session_id"`
	ChannelID            string    `json:"channel_id"`
	BucketStart          time.Time `json:"bucket_start"`
	BucketEnd            time.Time `json:"bucket_end"`
	Text                 string    `json:"text"`
	Language             string    `json:"language"`
	TranscriptConfidence float64   `json:"transcript_confidence"`
	SentimentScore       *float64  `json:"sentiment_score"`
	SentimentConfidence  *float64  `json:"sentiment_confidence"`
	SentimentLabel       string    `json:"sentiment_label"`
	SentimentModel       string    `json:"sentiment_model"`
	SentimentStatus      string    `json:"sentiment_status"`
	SentimentLatencyMS   *int64    `json:"sentiment_latency_ms"`
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

func ComputeAlignments(chatBuckets []chat.ChatBucket, transcriptBuckets []TranscriptBucket, window time.Duration) []AlignmentBucket {
	if window <= 0 {
		window = defaultAlignmentWindow
	}

	alignments := make([]AlignmentBucket, 0, len(transcriptBuckets))
	usedChatBuckets := map[string]struct{}{}
	for _, transcript := range transcriptBuckets {
		if transcript.SentimentScore == nil {
			continue
		}

		chatBucket, overlap := bestChatBucket(transcript, chatBuckets, usedChatBuckets)
		if chatBucket == nil {
			continue
		}
		if overlap < window/2 {
			continue
		}
		usedChatBuckets[chatBucketKey(*chatBucket)] = struct{}{}
		alignments = append(alignments, makeAlignment(*chatBucket, transcript, overlap, window))
		if len(alignments) >= maxAlignmentBuckets {
			break
		}
	}
	return alignments
}

func bestChatBucket(transcript TranscriptBucket, buckets []chat.ChatBucket, used map[string]struct{}) (*chat.ChatBucket, time.Duration) {
	var best *chat.ChatBucket
	var bestOverlap time.Duration
	for index := range buckets {
		bucket := buckets[index]
		if bucket.SessionID != transcript.SessionID || bucket.ChannelID != transcript.ChannelID {
			continue
		}
		if _, exists := used[chatBucketKey(bucket)]; exists {
			continue
		}
		overlap := overlapDuration(bucket.BucketStart, bucket.BucketEnd, transcript.BucketStart, transcript.BucketEnd)
		if overlap > bestOverlap {
			best = &buckets[index]
			bestOverlap = overlap
		}
	}
	return best, bestOverlap
}

func makeAlignment(chatBucket chat.ChatBucket, transcript TranscriptBucket, overlap time.Duration, window time.Duration) AlignmentBucket {
	transcriptSentiment := *transcript.SentimentScore
	delta := chatBucket.ChatSentiment - transcriptSentiment
	similarity := math.Max(0, 1-(math.Min(math.Abs(delta), 2)/2))
	quality, flags := alignmentQuality(chatBucket, transcript, overlap, window)

	return AlignmentBucket{
		Type:                  "alignment_bucket",
		SessionID:             transcript.SessionID,
		ChannelID:             transcript.ChannelID,
		WindowStart:           maxTime(chatBucket.BucketStart, transcript.BucketStart),
		WindowEnd:             minTime(chatBucket.BucketEnd, transcript.BucketEnd),
		ChatBucketStart:       chatBucket.BucketStart,
		ChatBucketEnd:         chatBucket.BucketEnd,
		TranscriptBucketStart: transcript.BucketStart,
		TranscriptBucketEnd:   transcript.BucketEnd,
		ChatSentiment:         chatBucket.ChatSentiment,
		ChatConfidence:        chatBucket.SentimentConfidence,
		ChatMessageCount:      chatBucket.MessageCount,
		TranscriptSentiment:   transcriptSentiment,
		TranscriptConfidence:  transcript.TranscriptConfidence,
		TranscriptTextLength:  len(transcript.Text),
		Delta:                 delta,
		Similarity:            similarity,
		Relationship:          relationshipForDelta(delta),
		OverlapSeconds:        int(overlap.Seconds()),
		Quality:               quality,
		QualityFlags:          flags,
	}
}

func alignmentQuality(chatBucket chat.ChatBucket, transcript TranscriptBucket, overlap time.Duration, window time.Duration) (float64, []string) {
	if window <= 0 {
		window = defaultAlignmentWindow
	}
	var flags []string
	overlapScore := clamp01(overlap.Seconds() / window.Seconds())
	chatVolumeScore := clamp01(float64(chatBucket.MessageCount) / 20)
	chatConfidence := clamp01(chatBucket.SentimentConfidence)
	transcriptConfidence := clamp01(transcript.TranscriptConfidence)
	textScore := clamp01(float64(len(strings.TrimSpace(transcript.Text))) / 240)

	if overlapScore >= 0.9 {
		flags = append(flags, "good_overlap")
	} else {
		flags = append(flags, "partial_overlap")
	}
	if chatBucket.MessageCount >= 10 {
		flags = append(flags, "enough_chat_volume")
	} else {
		flags = append(flags, "low_chat_volume")
	}
	if transcript.TranscriptConfidence >= 0.7 {
		flags = append(flags, "good_transcript_confidence")
	} else {
		flags = append(flags, "low_transcript_confidence")
	}
	if len(strings.TrimSpace(transcript.Text)) >= 80 {
		flags = append(flags, "enough_transcript_text")
	} else {
		flags = append(flags, "short_transcript_text")
	}

	quality := overlapScore*0.35 + chatVolumeScore*0.2 + chatConfidence*0.15 + transcriptConfidence*0.2 + textScore*0.1
	return clamp01(quality), flags
}

func relationshipForDelta(delta float64) string {
	absDelta := math.Abs(delta)
	if absDelta < 0.15 {
		return "converged"
	}
	if absDelta < 0.45 {
		return "soft_split"
	}
	return "diverged"
}

func overlapDuration(leftStart, leftEnd, rightStart, rightEnd time.Time) time.Duration {
	start := maxTime(leftStart, rightStart)
	end := minTime(leftEnd, rightEnd)
	if !end.After(start) {
		return 0
	}
	return end.Sub(start)
}

func chatBucketKey(bucket chat.ChatBucket) string {
	return bucket.SessionID + ":" + bucket.ChannelID + ":" + bucket.BucketStart.Format(time.RFC3339Nano) + ":" + bucket.BucketEnd.Format(time.RFC3339Nano)
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
