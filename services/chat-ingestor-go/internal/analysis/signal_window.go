package analysis

import (
	"math"
	"sort"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

const (
	signalWindowType = "signal_window"

	audienceShiftDeltaThreshold = 0.45
	spikeRatioThreshold         = 0.55
	spikeSentimentThreshold     = 0.35
	divergenceDeltaThreshold    = 0.45
	signalTargetMinConfidence   = 0.35
)

type SignalEventType string

const (
	SignalEventAudienceShift             SignalEventType = "audience_shift"
	SignalEventHypeSpike                 SignalEventType = "hype_spike"
	SignalEventFrustrationSpike          SignalEventType = "frustration_spike"
	SignalEventContentAudienceDivergence SignalEventType = "content_audience_divergence"
)

type SignalEvent struct {
	Type         SignalEventType `json:"type"`
	Severity     float64         `json:"severity"`
	Timestamp    time.Time       `json:"timestamp"`
	ReactionType string          `json:"reaction_type"`
	TargetType   string          `json:"target_type"`
	TargetText   string          `json:"target_text,omitempty"`
	Source       string          `json:"source"`
	EventHint    string          `json:"event_hint,omitempty"`
	Text         string          `json:"text,omitempty"`
	Confidence   float64         `json:"confidence"`
	EvidenceIDs  []string        `json:"evidence_ids,omitempty"`
}

type SignalWindow struct {
	Type                 string          `json:"type"`
	SessionID            string          `json:"session_id"`
	ChannelID            string          `json:"channel_id"`
	Source               string          `json:"source"`
	StreamID             string          `json:"stream_id"`
	WindowStart          time.Time       `json:"window_start"`
	WindowEnd            time.Time       `json:"window_end"`
	MessageCount         int             `json:"message_count"`
	UniqueChatters       int             `json:"unique_chatters"`
	ChatSentiment        float64         `json:"chat_sentiment"`
	ChatConfidence       float64         `json:"chat_confidence"`
	SentimentConfidence  float64         `json:"sentiment_confidence"`
	PositiveRatio        float64         `json:"positive"`
	NeutralRatio         float64         `json:"neutral"`
	NegativeRatio        float64         `json:"negative"`
	PreviousSentiment    *float64        `json:"previous_sentiment,omitempty"`
	TranscriptSentiment  *float64        `json:"transcript_sentiment,omitempty"`
	TranscriptConfidence *float64        `json:"transcript_confidence,omitempty"`
	AlignmentDelta       *float64        `json:"alignment_delta,omitempty"`
	Delta                *float64        `json:"delta,omitempty"`
	AlignmentQuality     *float64        `json:"alignment_quality,omitempty"`
	Quality              *float64        `json:"quality,omitempty"`
	Similarity           *float64        `json:"similarity,omitempty"`
	Relationship         string          `json:"relationship,omitempty"`
	QualityFlags         []string        `json:"quality_flags,omitempty"`
	AggregateSentiment   *float64        `json:"aggregate_sentiment,omitempty"`
	ReactionType         string          `json:"reaction_type,omitempty"`
	TargetType           string          `json:"target_type"`
	TargetText           string          `json:"target_text,omitempty"`
	EventHint            string          `json:"event_hint,omitempty"`
	Confidence           float64         `json:"confidence"`
	EvidenceIDs          []string        `json:"evidence_ids,omitempty"`
	FirstEventType       SignalEventType `json:"first_event_type,omitempty"`
	Events               []SignalEvent   `json:"events,omitempty"`
	transcriptSnippet    string
}

func NewSignalWindow(bucket chat.ChatBucket, previous *chat.ChatBucket) SignalWindow {
	window := signalWindowFromChatBucket(bucket)
	if previous != nil && sameSignalStream(bucket.SessionID, bucket.ChannelID, previous.SessionID, previous.ChannelID) {
		previousSentiment := previous.ChatSentiment
		window.PreviousSentiment = &previousSentiment
	}
	finalizeSignalWindow(&window)
	return window
}

func SignalWindowsFromChatBuckets(buckets []chat.ChatBucket) []SignalWindow {
	previousByBucket := previousChatBucketsByTime(buckets)
	windows := make([]SignalWindow, 0, len(buckets))
	for _, bucket := range buckets {
		var previous *chat.ChatBucket
		if item, ok := previousByBucket[chatBucketKey(bucket)]; ok {
			previous = &item
		}
		windows = append(windows, NewSignalWindow(bucket, previous))
	}
	return windows
}

func SignalWindowsFromAlignments(alignments []AlignmentBucket) []SignalWindow {
	windows := make([]SignalWindow, 0, len(alignments))
	for _, alignment := range alignments {
		window := signalWindowFromAlignment(alignment)
		finalizeSignalWindow(&window)
		windows = append(windows, window)
	}
	return windows
}

func SignalWindowsFromChatAndAlignments(chatBuckets []chat.ChatBucket, alignments []AlignmentBucket) []SignalWindow {
	return signalWindowsFromChatAndAlignments(chatBuckets, nil, alignments)
}

func SignalWindowsFromChatTranscriptAndAlignments(chatBuckets []chat.ChatBucket, transcriptBuckets []TranscriptBucket, alignments []AlignmentBucket) []SignalWindow {
	return signalWindowsFromChatAndAlignments(chatBuckets, transcriptBuckets, alignments)
}

func signalWindowsFromChatAndAlignments(chatBuckets []chat.ChatBucket, transcriptBuckets []TranscriptBucket, alignments []AlignmentBucket) []SignalWindow {
	if len(chatBuckets) == 0 {
		windows := make([]SignalWindow, 0, len(alignments))
		for _, alignment := range alignments {
			window := signalWindowFromAlignment(alignment)
			applyTranscriptContextToSignalWindow(&window, transcriptContextForAlignment(alignment, transcriptBuckets))
			finalizeSignalWindow(&window)
			windows = append(windows, window)
		}
		return windows
	}

	alignmentByChatBucket := map[string]AlignmentBucket{}
	for _, alignment := range alignments {
		key := alignment.SessionID + ":" + alignment.ChannelID + ":" + alignment.ChatBucketStart.Format(time.RFC3339Nano) + ":" + alignment.ChatBucketEnd.Format(time.RFC3339Nano)
		alignmentByChatBucket[key] = alignment
	}

	windows := SignalWindowsFromChatBuckets(chatBuckets)
	for index := range windows {
		bucket := chatBuckets[index]
		if alignment, ok := alignmentByChatBucket[chatBucketKey(bucket)]; ok {
			applyAlignmentToSignalWindow(&windows[index], alignment)
			applyTranscriptContextToSignalWindow(&windows[index], transcriptContextForAlignment(alignment, transcriptBuckets))
			finalizeSignalWindow(&windows[index])
		}
	}
	return windows
}

func SignalWindowsFromTranscriptBuckets(chatBuckets []chat.ChatBucket, transcriptBuckets []TranscriptBucket, window time.Duration) []SignalWindow {
	alignments := ComputeAlignments(chatBuckets, transcriptBuckets, window)
	return SignalWindowsFromChatTranscriptAndAlignments(chatBuckets, transcriptBuckets, alignments)
}

func DetectSignalEvents(window SignalWindow) []SignalEvent {
	var events []SignalEvent

	if window.PreviousSentiment != nil {
		shift := window.ChatSentiment - *window.PreviousSentiment
		if math.Abs(shift) >= audienceShiftDeltaThreshold {
			events = append(events, SignalEvent{
				Type:      SignalEventAudienceShift,
				Severity:  clamp01(math.Abs(shift) / 2),
				Timestamp: window.WindowStart,
				Source:    "chat",
			})
		}
	}

	if window.PositiveRatio >= spikeRatioThreshold && window.ChatSentiment >= spikeSentimentThreshold {
		events = append(events, SignalEvent{
			Type:         SignalEventHypeSpike,
			Severity:     clamp01((window.PositiveRatio + math.Max(window.ChatSentiment, 0)) / 2),
			Timestamp:    window.WindowStart,
			ReactionType: "hype",
			Source:       "chat",
		})
	}

	if window.NegativeRatio >= spikeRatioThreshold && window.ChatSentiment <= -spikeSentimentThreshold {
		events = append(events, SignalEvent{
			Type:         SignalEventFrustrationSpike,
			Severity:     clamp01((window.NegativeRatio + math.Abs(math.Min(window.ChatSentiment, 0))) / 2),
			Timestamp:    window.WindowStart,
			ReactionType: "frustration",
			Source:       "chat",
		})
	}

	if window.AlignmentDelta != nil && math.Abs(*window.AlignmentDelta) >= divergenceDeltaThreshold {
		events = append(events, SignalEvent{
			Type:      SignalEventContentAudienceDivergence,
			Severity:  clamp01(math.Abs(*window.AlignmentDelta) / 2),
			Timestamp: window.WindowStart,
			Source:    "alignment",
		})
	}

	for index := range events {
		events[index] = applySignalEventContext(events[index], window)
	}
	return events
}

func FirstSignalEventType(events []SignalEvent) SignalEventType {
	if len(events) == 0 {
		return ""
	}
	return events[0].Type
}

func signalWindowFromChatBucket(bucket chat.ChatBucket) SignalWindow {
	reactionType := signalReactionType(bucket)
	peak := signalPeakSubwindow(bucket)
	targetText := firstNonEmpty(peak.TargetText, bucket.PeakTargetText, signalTargetText(bucket))
	return SignalWindow{
		Type:                signalWindowType,
		SessionID:           bucket.SessionID,
		ChannelID:           bucket.ChannelID,
		Source:              "chat",
		StreamID:            bucket.ChannelID,
		WindowStart:         bucket.BucketStart,
		WindowEnd:           bucket.BucketEnd,
		MessageCount:        bucket.MessageCount,
		UniqueChatters:      bucket.UniqueChatters,
		ChatSentiment:       bucket.ChatSentiment,
		ChatConfidence:      bucket.SentimentConfidence,
		SentimentConfidence: bucket.SentimentConfidence,
		PositiveRatio:       bucket.PositiveRatio,
		NeutralRatio:        bucket.NeutralRatio,
		NegativeRatio:       bucket.NegativeRatio,
		ReactionType:        reactionType,
		TargetType:          firstNonEmpty(peak.TargetType, bucket.PeakTargetType, "unknown"),
		TargetText:          targetText,
		EventHint:           firstNonEmpty(peak.EventHint, signalContextHint(reactionType, targetText)),
		Confidence:          signalChatConfidence(bucket),
		EvidenceIDs:         signalEvidenceIDs(bucket),
	}
}

func signalWindowFromAlignment(alignment AlignmentBucket) SignalWindow {
	reactionType := signalReactionTypeFromSentiment(alignment.ChatSentiment)
	window := SignalWindow{
		Type:                signalWindowType,
		SessionID:           alignment.SessionID,
		ChannelID:           alignment.ChannelID,
		Source:              "alignment",
		StreamID:            alignment.ChannelID,
		WindowStart:         alignment.WindowStart,
		WindowEnd:           alignment.WindowEnd,
		MessageCount:        alignment.ChatMessageCount,
		ChatSentiment:       alignment.ChatSentiment,
		ChatConfidence:      alignment.ChatConfidence,
		SentimentConfidence: alignment.ChatConfidence,
		ReactionType:        reactionType,
		TargetType:          "unknown",
		EventHint:           signalContextHint(reactionType, ""),
		Confidence:          clamp01((alignment.ChatConfidence + alignment.Quality) / 2),
	}
	applyAlignmentToSignalWindow(&window, alignment)
	return window
}

func applyAlignmentToSignalWindow(window *SignalWindow, alignment AlignmentBucket) {
	transcriptSentiment := alignment.TranscriptSentiment
	transcriptConfidence := alignment.TranscriptConfidence
	alignmentDelta := alignment.Delta
	alignmentQuality := alignment.Quality
	similarity := alignment.Similarity
	aggregate := (alignment.ChatSentiment + alignment.TranscriptSentiment) / 2

	window.TranscriptSentiment = &transcriptSentiment
	window.TranscriptConfidence = &transcriptConfidence
	window.AlignmentDelta = &alignmentDelta
	window.Delta = &alignmentDelta
	window.AlignmentQuality = &alignmentQuality
	window.Quality = &alignmentQuality
	window.Similarity = &similarity
	window.Relationship = alignment.Relationship
	window.QualityFlags = append([]string(nil), alignment.QualityFlags...)
	window.AggregateSentiment = &aggregate
	window.Source = "alignment"
	if window.ReactionType == "" || window.ReactionType == "neutral" {
		window.ReactionType = signalReactionTypeFromSentiment(alignment.ChatSentiment)
	}
	window.TargetType = firstNonEmpty(window.TargetType, "unknown")
	window.Confidence = signalAlignmentConfidence(*window, alignment)
	window.EventHint = signalContextHint(window.ReactionType, window.TargetText)
}

func applyTranscriptContextToSignalWindow(window *SignalWindow, context signalTranscriptContext) {
	if context.Text == "" && context.TargetText == "" && context.EvidenceID == "" {
		return
	}
	if window.TargetText == "" && context.TargetText != "" {
		window.TargetText = context.TargetText
		window.TargetType = firstNonEmpty(window.TargetType, "unknown")
	}
	if context.Text != "" {
		window.transcriptSnippet = context.Text
	}
	if context.EvidenceID != "" {
		window.EvidenceIDs = appendUniqueStrings(window.EvidenceIDs, context.EvidenceID)
	}
	window.EventHint = signalContextHint(window.ReactionType, window.TargetText)
}

func finalizeSignalWindow(window *SignalWindow) {
	normalizeWeakSignalWindowContext(window)
	window.Events = DetectSignalEvents(*window)
	window.FirstEventType = FirstSignalEventType(window.Events)
	window.EventHint = signalWindowEventHint(*window)
	normalizeWeakSignalWindowContext(window)
}

func normalizeWeakSignalWindowContext(window *SignalWindow) {
	window.TargetType = firstNonEmpty(window.TargetType, "unknown")
	if !weakSignalWindowContext(*window) {
		return
	}
	if window.Confidence < signalTargetMinConfidence {
		window.ReactionType = "neutral"
	}
	window.TargetType = "unknown"
	window.TargetText = ""
	window.EventHint = "neutral"
}

func weakSignalWindowContext(window SignalWindow) bool {
	return window.ReactionType == "neutral" || window.Confidence < signalTargetMinConfidence
}

func applySignalEventContext(event SignalEvent, window SignalWindow) SignalEvent {
	event.ReactionType = signalEventReactionType(event, window)
	event.TargetType = firstNonEmpty(window.TargetType, "unknown")
	event.TargetText = window.TargetText
	if event.Source == "" {
		event.Source = firstNonEmpty(window.Source, "chat")
	}
	event.Confidence = roundFloat(signalEventConfidence(event, window))
	if event.EvidenceIDs == nil {
		event.EvidenceIDs = append([]string(nil), window.EvidenceIDs...)
	}
	if event.EventHint == "" {
		event.EventHint = signalEventHint(event.Type, event.TargetText)
	}
	if event.Source == "alignment" && event.Text == "" {
		event.Text = window.transcriptSnippet
	}
	return event
}

func signalEventReactionType(event SignalEvent, window SignalWindow) string {
	switch event.Type {
	case SignalEventHypeSpike:
		return "hype"
	case SignalEventFrustrationSpike:
		return "frustration"
	case SignalEventAudienceShift, SignalEventContentAudienceDivergence:
		return firstNonEmpty(window.ReactionType, signalReactionTypeFromSentiment(window.ChatSentiment))
	default:
		return firstNonEmpty(event.ReactionType, window.ReactionType, "neutral")
	}
}

func signalEventConfidence(event SignalEvent, window SignalWindow) float64 {
	if event.Source == "alignment" {
		if window.AlignmentQuality != nil && window.SentimentConfidence > 0 {
			return clamp01((*window.AlignmentQuality + window.SentimentConfidence) / 2)
		}
		if window.AlignmentQuality != nil {
			return clamp01(*window.AlignmentQuality)
		}
	}
	if event.Source == "chat" {
		if window.SentimentConfidence > 0 {
			return clamp01(window.SentimentConfidence)
		}
		if window.ChatConfidence > 0 {
			return clamp01(window.ChatConfidence)
		}
		if event.Confidence > 0 {
			return clamp01(event.Confidence)
		}
		return clamp01(event.Severity)
	}
	if window.Confidence > 0 {
		return clamp01(window.Confidence)
	}
	if event.Confidence > 0 {
		return clamp01(event.Confidence)
	}
	return clamp01(event.Severity)
}

func signalWindowEventHint(window SignalWindow) string {
	if len(window.Events) > 0 {
		return window.Events[0].EventHint
	}
	return signalContextHint(window.ReactionType, window.TargetText)
}

func signalEventHint(eventType SignalEventType, targetText string) string {
	hint := string(eventType)
	if targetText != "" {
		hint += ":" + targetText
	}
	return hint
}

func signalContextHint(reactionType, targetText string) string {
	if reactionType == "" {
		return ""
	}
	if targetText == "" {
		return reactionType
	}
	return reactionType + ":" + targetText
}

func signalReactionType(bucket chat.ChatBucket) string {
	if bucket.PeakReactionType != "" {
		return bucket.PeakReactionType
	}
	if peak := signalPeakSubwindow(bucket); peak.ReactionType != "" {
		return peak.ReactionType
	}
	return signalReactionTypeFromSentiment(bucket.ChatSentiment)
}

func signalReactionTypeFromSentiment(sentiment float64) string {
	if sentiment >= spikeSentimentThreshold {
		return "hype"
	}
	if sentiment <= -spikeSentimentThreshold {
		return "frustration"
	}
	return "neutral"
}

func signalChatConfidence(bucket chat.ChatBucket) float64 {
	if bucket.PeakConfidence > 0 {
		return clamp01(bucket.PeakConfidence)
	}
	if peak := signalPeakSubwindow(bucket); peak.Confidence > 0 {
		return clamp01(peak.Confidence)
	}
	return clamp01(bucket.SentimentConfidence)
}

func signalAlignmentConfidence(window SignalWindow, alignment AlignmentBucket) float64 {
	chatConfidence := firstPositive(window.SentimentConfidence, alignment.ChatConfidence)
	quality := alignment.Quality
	if window.AlignmentQuality != nil {
		quality = *window.AlignmentQuality
	}
	if chatConfidence == 0 {
		return clamp01(quality)
	}
	if quality == 0 {
		return clamp01(chatConfidence)
	}
	return clamp01((chatConfidence + quality) / 2)
}

func firstPositive(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func signalTargetText(bucket chat.ChatBucket) string {
	counts := map[string]int{}
	firstSeen := map[string]int{}
	position := 0
	for _, message := range bucket.PeakEvidenceMessages {
		position = countSignalTerms(message.Text, counts, firstSeen, position)
	}
	for _, score := range bucket.MessageScores {
		position = countSignalTerms(score.Text, counts, firstSeen, position)
	}
	for _, term := range bucket.TopTerms {
		position = countSignalTerms(term, counts, firstSeen, position)
	}

	best := ""
	bestCount := 0
	bestIndex := len(firstSeen) + 1
	for term, count := range counts {
		if count < 2 {
			continue
		}
		index := firstSeen[term]
		if count > bestCount || (count == bestCount && index < bestIndex) {
			best = term
			bestCount = count
			bestIndex = index
		}
	}
	return best
}

type signalTranscriptContext struct {
	Text       string
	TargetText string
	EvidenceID string
}

func transcriptContextForAlignment(alignment AlignmentBucket, buckets []TranscriptBucket) signalTranscriptContext {
	if len(buckets) == 0 {
		return signalTranscriptContext{}
	}
	bucket := bestTranscriptBucketForAlignment(alignment, buckets)
	if bucket == nil {
		return signalTranscriptContext{}
	}
	text := transcriptEvidenceSnippet(bucketText(*bucket))
	return signalTranscriptContext{
		Text:       text,
		TargetText: transcriptTargetText(bucketText(*bucket)),
		EvidenceID: "transcript:" + bucket.BucketStart.UTC().Format(time.RFC3339Nano),
	}
}

func bestTranscriptBucketForAlignment(alignment AlignmentBucket, buckets []TranscriptBucket) *TranscriptBucket {
	var best *TranscriptBucket
	var bestOverlap time.Duration
	for index := range buckets {
		bucket := buckets[index]
		if bucket.SessionID != alignment.SessionID || bucket.ChannelID != alignment.ChannelID {
			continue
		}
		if bucket.BucketStart.Equal(alignment.TranscriptBucketStart) && bucket.BucketEnd.Equal(alignment.TranscriptBucketEnd) {
			return &buckets[index]
		}
		overlap := overlapDuration(bucket.BucketStart, bucket.BucketEnd, alignment.TranscriptBucketStart, alignment.TranscriptBucketEnd)
		if overlap > bestOverlap {
			best = &buckets[index]
			bestOverlap = overlap
		}
	}
	return best
}

func bucketText(bucket TranscriptBucket) string {
	return strings.TrimSpace(bucket.Text)
}

func transcriptEvidenceSnippet(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len([]rune(text)) <= 180 {
		return text
	}
	runes := []rune(text)
	return string(runes[:177]) + "..."
}

func transcriptTargetText(text string) string {
	if phrase := repeatedTranscriptPhrase(text); phrase != "" {
		return phrase
	}
	counts := map[string]int{}
	firstSeen := map[string]int{}
	countSignalTerms(text, counts, firstSeen, 0)
	best := ""
	bestCount := 0
	bestIndex := len(firstSeen) + 1
	for term, count := range counts {
		if count < 2 {
			continue
		}
		index := firstSeen[term]
		if count > bestCount || (count == bestCount && index < bestIndex) {
			best = term
			bestCount = count
			bestIndex = index
		}
	}
	return best
}

func repeatedTranscriptPhrase(text string) string {
	tokens := signalTerms(text)
	if len(tokens) < 2 {
		return ""
	}
	counts := map[string]int{}
	firstSeen := map[string]int{}
	for index := 0; index < len(tokens)-1; index++ {
		phrase := tokens[index] + " " + tokens[index+1]
		counts[phrase]++
		if _, ok := firstSeen[phrase]; !ok {
			firstSeen[phrase] = index
		}
	}
	best := ""
	bestCount := 0
	bestIndex := len(tokens) + 1
	for phrase, count := range counts {
		if count < 2 {
			continue
		}
		index := firstSeen[phrase]
		if count > bestCount || (count == bestCount && index < bestIndex) {
			best = phrase
			bestCount = count
			bestIndex = index
		}
	}
	return best
}

func appendUniqueStrings(values []string, additions ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values)+len(additions))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, value := range additions {
		if strings.TrimSpace(value) == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func countSignalTerms(text string, counts map[string]int, firstSeen map[string]int, position int) int {
	for _, token := range signalTerms(text) {
		counts[token]++
		if _, ok := firstSeen[token]; !ok {
			firstSeen[token] = position
		}
		position++
	}
	return position
}

func signalTerms(text string) []string {
	var terms []string
	for _, token := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	}) {
		token = normalizeSignalTerm(token)
		if token != "" {
			terms = append(terms, token)
		}
	}
	return terms
}

func signalEvidenceIDs(bucket chat.ChatBucket) []string {
	seen := map[string]struct{}{}
	var ids []string
	for _, id := range bucket.PeakEvidenceIDs {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if peak := signalPeakSubwindow(bucket); len(peak.EvidenceIDs) > 0 {
		for _, id := range peak.EvidenceIDs {
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	for _, message := range bucket.PeakEvidenceMessages {
		if message.MessageID == "" {
			continue
		}
		if _, ok := seen[message.MessageID]; ok {
			continue
		}
		seen[message.MessageID] = struct{}{}
		ids = append(ids, message.MessageID)
	}
	for _, score := range bucket.MessageScores {
		if score.MessageID == "" {
			continue
		}
		if _, ok := seen[score.MessageID]; ok {
			continue
		}
		seen[score.MessageID] = struct{}{}
		ids = append(ids, score.MessageID)
		if len(ids) >= 5 {
			return ids
		}
	}
	return ids
}

func signalPeakSubwindow(bucket chat.ChatBucket) chat.ReactionSubwindow {
	if len(bucket.Subwindows) == 0 {
		return chat.ReactionSubwindow{}
	}
	if bucket.PeakWindowStart != nil && bucket.PeakWindowEnd != nil {
		for _, subwindow := range bucket.Subwindows {
			if subwindow.WindowStart.Equal(*bucket.PeakWindowStart) && subwindow.WindowEnd.Equal(*bucket.PeakWindowEnd) {
				return subwindow
			}
		}
	}
	best := bucket.Subwindows[0]
	for _, subwindow := range bucket.Subwindows[1:] {
		if subwindow.ReactionScore > best.ReactionScore || (subwindow.ReactionScore == best.ReactionScore && subwindow.WindowEnd.After(best.WindowEnd)) {
			best = subwindow
		}
	}
	return best
}

func normalizeSignalTerm(value string) string {
	value = strings.Trim(strings.ToLower(value), " \t\r\n.,!?;:\"'()[]{}")
	if len(value) < 3 {
		return ""
	}
	if _, skip := signalStopWords[value]; skip {
		return ""
	}
	return value
}

var signalStopWords = map[string]struct{}{
	"the": {}, "and": {}, "for": {}, "you": {}, "that": {}, "this": {}, "with": {}, "was": {}, "are": {}, "but": {},
	"what": {}, "wait": {}, "huh": {}, "why": {}, "how": {}, "wow": {}, "holy": {}, "bad": {}, "not": {}, "now": {},
}

func previousChatBucketsByTime(buckets []chat.ChatBucket) map[string]chat.ChatBucket {
	ordered := append([]chat.ChatBucket(nil), buckets...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].BucketStart.Before(ordered[right].BucketStart)
	})

	previousByBucket := map[string]chat.ChatBucket{}
	previousByStream := map[string]chat.ChatBucket{}
	for _, bucket := range ordered {
		key := signalStreamKey(bucket.SessionID, bucket.ChannelID)
		if previous, ok := previousByStream[key]; ok {
			previousByBucket[chatBucketKey(bucket)] = previous
		}
		previousByStream[key] = bucket
	}
	return previousByBucket
}

func sameSignalStream(leftSession, leftChannel, rightSession, rightChannel string) bool {
	return leftSession == rightSession && leftChannel == rightChannel
}

func signalStreamKey(sessionID, channelID string) string {
	return sessionID + ":" + channelID
}
