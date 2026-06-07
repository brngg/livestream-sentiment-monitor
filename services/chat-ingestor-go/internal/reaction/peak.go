package reaction

import (
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

func AttachPeakMetadata(bucket chat.ChatBucket, windows []chat.ReactionWindow) chat.ChatBucket {
	subwindows := make([]chat.ReactionSubwindow, 0)
	var peak *chat.ReactionWindow
	var peakScore float64

	for _, window := range windows {
		if !inside(window.WindowStart, window.WindowEnd, bucket.BucketStart, bucket.BucketEnd) {
			continue
		}
		score := reactionScore(window)
		subwindows = append(subwindows, chat.ReactionSubwindow{
			WindowStart:      window.WindowStart,
			WindowEnd:        window.WindowEnd,
			MessageCount:     window.MessageCount,
			ReactionScore:    score,
			HypeScore:        window.HypeScore,
			IntensityScore:   window.IntensityScore,
			ConfusionScore:   window.ConfusionScore,
			FrustrationScore: window.FrustrationScore,
			ReactionType:     window.ReactionType,
			TargetType:       defaultString(window.TargetType, "unknown"),
			TargetText:       window.TargetText,
			Source:           defaultString(window.Source, "chat"),
			EventHint:        window.EventHint,
			Confidence:       window.Confidence,
			EvidenceIDs:      append([]string(nil), window.EvidenceIDs...),
		})
		if peak == nil || score > peakScore || (score == peakScore && window.WindowEnd.After(peak.WindowEnd)) {
			windowCopy := window
			peak = &windowCopy
			peakScore = score
		}
	}

	bucket.Subwindows = subwindows
	if peak == nil {
		bucket.PeakTargetType = "unknown"
		bucket.PeakSource = "chat"
		bucket.PeakEvidenceIDs = []string{}
		bucket.PeakEvidenceMessages = []chat.ChatMessage{}
		return bucket
	}

	score := peakScore
	peakTime := peak.WindowEnd
	peakStart := peak.WindowStart
	peakEnd := peak.WindowEnd
	bucket.PeakReactionScore = &score
	bucket.PeakReactionType = peak.ReactionType
	bucket.PeakTargetType = defaultString(peak.TargetType, "unknown")
	bucket.PeakTargetText = peak.TargetText
	bucket.PeakSource = defaultString(peak.Source, "chat")
	bucket.PeakEventHint = peak.EventHint
	bucket.PeakConfidence = peak.Confidence
	bucket.PeakEvidenceIDs = append([]string(nil), peak.EvidenceIDs...)
	bucket.PeakTime = &peakTime
	bucket.PeakWindowStart = &peakStart
	bucket.PeakWindowEnd = &peakEnd
	bucket.PeakEvidenceMessages = append([]chat.ChatMessage(nil), peak.EvidenceMessages...)
	return bucket
}

func reactionScore(window chat.ReactionWindow) float64 {
	score := window.IntensityScore
	for _, value := range []float64{window.HypeScore, window.ConfusionScore, window.FrustrationScore} {
		if value > score {
			score = value
		}
	}
	return score
}

func inside(leftStart, leftEnd, rightStart, rightEnd time.Time) bool {
	return !leftStart.Before(rightStart) && !leftEnd.After(rightEnd)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
