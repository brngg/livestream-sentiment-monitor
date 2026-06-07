package bucket

import (
	"sort"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

const DefaultWindow = 30 * time.Second

type Bucketizer struct {
	Window time.Duration
}

func (b Bucketizer) WindowFor(t time.Time) (time.Time, time.Time) {
	window := b.Window
	if window <= 0 {
		window = DefaultWindow
	}
	start := t.UTC().Truncate(window)
	return start, start.Add(window)
}

func (b Bucketizer) Bucket(messages []chat.ScoredMessage) (chat.ChatBucket, bool) {
	if len(messages) == 0 {
		return chat.ChatBucket{}, false
	}

	start, end := b.WindowFor(messages[0].Message.Timestamp)
	bucket := chat.ChatBucket{
		Type:        "chat_bucket",
		SessionID:   messages[0].Message.SessionID,
		ChannelID:   messages[0].Message.ChannelID,
		BucketStart: start,
		BucketEnd:   end,
		LanguageMix: map[string]float64{},
	}

	chatters := map[string]struct{}{}
	languages := map[string]int{}
	terms := map[string]int{}
	emotes := map[string]int{}

	var sentimentTotal float64
	var confidenceTotal float64
	var positiveCount int
	var neutralCount int
	var negativeCount int
	for _, item := range messages {
		msg := item.Message
		bucket.MessageCount++
		chatters[msg.Username] = struct{}{}
		language := msg.Language
		if language == "" {
			language = "other"
		}
		languages[language]++
		sentimentTotal += item.Sentiment.Score
		confidenceTotal += item.Sentiment.Confidence
		switch sentimentLabel(item.Sentiment) {
		case "positive":
			positiveCount++
		case "negative":
			negativeCount++
		default:
			neutralCount++
		}

		for _, term := range termsFromText(msg.Text) {
			terms[term]++
		}
		for _, emote := range msg.Emotes {
			if emote != "" {
				emotes[emote]++
			}
		}
	}

	bucket.UniqueChatters = len(chatters)
	bucket.ChatSentiment = sentimentTotal / float64(bucket.MessageCount)
	bucket.SentimentConfidence = confidenceTotal / float64(bucket.MessageCount)
	bucket.PositiveRatio = float64(positiveCount) / float64(bucket.MessageCount)
	bucket.NeutralRatio = float64(neutralCount) / float64(bucket.MessageCount)
	bucket.NegativeRatio = float64(negativeCount) / float64(bucket.MessageCount)
	for language, count := range languages {
		bucket.LanguageMix[language] = float64(count) / float64(bucket.MessageCount)
	}
	bucket.TopTerms = topN(terms, 5)
	bucket.TopEmotes = topN(emotes, 5)
	return bucket, true
}

type StreamBucketizer struct {
	bucketizer Bucketizer
	pending    []chat.ScoredMessage
	start      time.Time
	end        time.Time
}

type DetailedBucket struct {
	Bucket   chat.ChatBucket
	Messages []chat.ChatMessage
}

func NewStreamBucketizer(window time.Duration) *StreamBucketizer {
	if window <= 0 {
		window = DefaultWindow
	}
	return &StreamBucketizer{bucketizer: Bucketizer{Window: window}}
}

func (s *StreamBucketizer) Add(msg chat.ScoredMessage) []chat.ChatBucket {
	detailed := s.AddDetailed(msg)
	out := make([]chat.ChatBucket, 0, len(detailed))
	for _, item := range detailed {
		out = append(out, item.Bucket)
	}
	return out
}

func (s *StreamBucketizer) AddDetailed(msg chat.ScoredMessage) []DetailedBucket {
	start, end := s.bucketizer.WindowFor(msg.Message.Timestamp)
	if len(s.pending) == 0 {
		s.start = start
		s.end = end
		s.pending = append(s.pending, msg)
		return nil
	}

	var flushed []DetailedBucket
	for !msg.Message.Timestamp.Before(s.end) {
		if bucket, ok := s.makeDetailedBucket(); ok {
			flushed = append(flushed, bucket)
		}
		s.pending = nil
		s.start = s.end
		s.end = s.end.Add(s.bucketizer.Window)
	}

	if start.Equal(s.start) {
		s.pending = append(s.pending, msg)
	} else {
		s.start = start
		s.end = end
		s.pending = append(s.pending, msg)
	}

	return flushed
}

func (s *StreamBucketizer) Flush() []chat.ChatBucket {
	detailed := s.FlushDetailed()
	out := make([]chat.ChatBucket, 0, len(detailed))
	for _, item := range detailed {
		out = append(out, item.Bucket)
	}
	return out
}

func (s *StreamBucketizer) FlushDetailed() []DetailedBucket {
	if len(s.pending) == 0 {
		return nil
	}
	bucket, ok := s.makeDetailedBucket()
	s.pending = nil
	if !ok {
		return nil
	}
	return []DetailedBucket{bucket}
}

func (s *StreamBucketizer) makeDetailedBucket() (DetailedBucket, bool) {
	bucket, ok := s.bucketizer.Bucket(s.pending)
	if !ok {
		return DetailedBucket{}, false
	}

	messages := make([]chat.ChatMessage, 0, len(s.pending))
	for _, item := range s.pending {
		messages = append(messages, item.Message)
	}
	return DetailedBucket{Bucket: bucket, Messages: messages}, true
}

func termsFromText(text string) []string {
	parts := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	})
	var out []string
	for _, part := range parts {
		if len(part) >= 3 && !stopWords[part] {
			out = append(out, part)
		}
	}
	return out
}

func sentimentLabel(result chat.SentimentResult) string {
	if result.Label != "" {
		return result.Label
	}
	if result.Score > 0.15 {
		return "positive"
	}
	if result.Score < -0.15 {
		return "negative"
	}
	return "neutral"
}

func topN(values map[string]int, n int) []string {
	type entry struct {
		key   string
		count int
	}
	entries := make([]entry, 0, len(values))
	for key, count := range values {
		entries = append(entries, entry{key: key, count: count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count == entries[j].count {
			return entries[i].key < entries[j].key
		}
		return entries[i].count > entries[j].count
	})
	if len(entries) > n {
		entries = entries[:n]
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.key)
	}
	return out
}

var stopWords = map[string]bool{
	"and": true, "are": true, "but": true, "for": true, "lol": true,
	"that": true, "the": true, "this": true, "was": true, "with": true,
}
