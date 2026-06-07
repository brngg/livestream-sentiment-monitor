package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/analysis"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

const (
	maxTranscriptBuckets             = 80
	maxAlignmentBuckets              = 80
	maxTranscriptRecoveryCheckPeriod = 10 * time.Second
)

func (s *server) pollTranscriptBuckets(ctx context.Context, sessionID string, source streamSource) {
	if strings.TrimSpace(s.cfg.TranscriptURL) == "" {
		return
	}
	channel := source.ID

	interval := s.cfg.TranscriptPoll
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	recoveryInterval := transcriptRecoveryCheckInterval(interval)
	lastRecoveryCheck := time.Time{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bucket, ok := s.fetchLatestTranscriptBucket(ctx, sessionID, channel)
			if !ok {
				now := time.Now()
				if lastRecoveryCheck.IsZero() || now.Sub(lastRecoveryCheck) >= recoveryInterval {
					lastRecoveryCheck = now
					if err := s.ensureTranscriptSession(ctx, source, sessionID); err != nil && s.logger != nil {
						s.logger.Warn("transcript session recovery check failed", "session_id", sessionID, "channel", channel, "error", err)
					}
				}
				continue
			}
			s.addTranscriptBucket(*bucket)
		}
	}
}

func transcriptRecoveryCheckInterval(pollInterval time.Duration) time.Duration {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	interval := 10 * pollInterval
	if interval > maxTranscriptRecoveryCheckPeriod {
		return maxTranscriptRecoveryCheckPeriod
	}
	return interval
}

func (s *server) streamTranscriptEvents(ctx context.Context, sessionID, channel string) {
	if strings.TrimSpace(s.cfg.TranscriptURL) == "" {
		return
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.cfg.TranscriptURL, "/")+"/events", nil)
	if err != nil {
		return
	}
	request.Header.Set("Accept", "text/event-stream")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("transcript event stream unavailable; falling back to polling", "session_id", sessionID, "error", err)
		}
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if s.logger != nil {
			s.logger.Warn("transcript event stream failed; falling back to polling", "session_id", sessionID, "status", response.Status)
		}
		return
	}

	events := make(chan map[string]any, 32)
	errCh := make(chan error, 1)
	go scanTranscriptSSE(ctx, response.Body, sessionID, events, errCh)

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			s.recordTranscriptStreamMetric(event, sessionID, channel)
			if eventType, _ := event["type"].(string); eventType != "transcript_bucket" {
				continue
			}
			bucket, ok := decodeTranscriptBucketEvent(event, sessionID, channel)
			if !ok {
				continue
			}
			s.addTranscriptBucket(bucket)
		case err := <-errCh:
			if err != nil && s.logger != nil {
				s.logger.Warn("transcript event stream ended with error; polling remains active", "session_id", sessionID, "error", err)
			}
			return
		}
	}
}

func (s *server) recordTranscriptStreamMetric(event map[string]any, sessionID, channel string) {
	if strings.TrimSpace(fmt.Sprint(event["status"])) != "backpressure" {
		return
	}
	count := s.metrics.asrBackpressure.Add(1)
	recordedAt := time.Now().UTC()
	meta := map[string]any{
		"reason":      "asr_backpressure",
		"channel_id":  channel,
		"total_count": count,
	}
	if value, ok := event["asr_latency_ms"]; ok {
		meta["asr_latency_ms"] = value
	}
	if value, ok := event["asr_interval_ms"]; ok {
		meta["asr_interval_ms"] = value
	}
	metric := storage.SystemMetric{
		SessionID:  sessionID,
		Name:       "asr.backpressure_count",
		Value:      1,
		Unit:       "events",
		RecordedAt: recordedAt,
		Meta:       meta,
	}
	s.persistAsync("save_asr_backpressure_metric", func(ctx context.Context) error {
		return s.store.SaveMetric(ctx, metric)
	})
}

func decodeTranscriptBucketEvent(event map[string]any, sessionID, channel string) (transcriptBucket, bool) {
	raw, err := json.Marshal(event)
	if err != nil {
		return transcriptBucket{}, false
	}
	var bucket transcriptBucket
	if err := json.Unmarshal(raw, &bucket); err != nil {
		return transcriptBucket{}, false
	}
	if bucket.Type != "transcript_bucket" {
		return transcriptBucket{}, false
	}
	if bucket.SessionID != sessionID || bucket.ChannelID != channel {
		return transcriptBucket{}, false
	}
	if bucket.BucketStart.IsZero() || bucket.BucketEnd.IsZero() {
		return transcriptBucket{}, false
	}
	return bucket, true
}

func (s *server) fetchLatestTranscriptBucket(ctx context.Context, sessionID, channel string) (*transcriptBucket, bool) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.cfg.TranscriptURL, "/")+"/buckets", nil)
	if err != nil {
		return nil, false
	}

	client := http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return nil, false
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, false
	}

	var state transcriptServiceState
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		return nil, false
	}
	if state.SessionID != sessionID || state.ChannelID != channel || state.LatestBucket == nil {
		return nil, false
	}
	return state.LatestBucket, true
}

func (s *server) addTranscriptBucket(bucket transcriptBucket) {
	var alignments []alignmentBucket
	var signalWindows []analysis.SignalWindow
	var accepted bool
	var changed bool

	s.mu.Lock()

	if s.state.Session != "" && bucket.SessionID != s.state.Session {
		s.mu.Unlock()
		return
	}
	if s.state.Channel != "" && bucket.ChannelID != s.state.Channel {
		s.mu.Unlock()
		return
	}
	accepted = true

	if upsertTranscriptBucket(&s.state.Transcripts, bucket) {
		changed = true
		if len(s.state.Transcripts) > maxTranscriptBuckets {
			s.state.Transcripts = s.state.Transcripts[:maxTranscriptBuckets]
		}
		result := computeAnalysis(s.state.Buckets, s.state.Transcripts, s.cfg.BucketEvery)
		s.state.Alignments = dashboardAlignments(result.Alignments)
		s.state.SignalWindows = result.SignalWindows
		alignments = append([]alignmentBucket(nil), s.state.Alignments...)
		signalWindows = append([]analysis.SignalWindow(nil), s.state.SignalWindows...)
	}
	s.mu.Unlock()

	if accepted && changed {
		s.publishTranscriptBucketEvent(bucket)
		s.broadcast(dashboardEvent{
			Type:          "transcript_bucket",
			Session:       bucket.SessionID,
			Channel:       bucket.ChannelID,
			Transcript:    &bucket,
			Alignments:    alignments,
			SignalWindows: signalWindows,
		})
		s.persistTranscriptBucket(bucket)
		s.persistAlignments(alignments)
	}
}

func upsertTranscriptBucket(buckets *[]transcriptBucket, bucket transcriptBucket) bool {
	key := transcriptBucketKey(bucket)
	for index := range *buckets {
		if transcriptBucketKey((*buckets)[index]) == key {
			if reflect.DeepEqual((*buckets)[index], bucket) {
				return false
			}
			(*buckets)[index] = bucket
			return true
		}
	}
	*buckets = append([]transcriptBucket{bucket}, (*buckets)...)
	return true
}

func transcriptBucketKey(bucket transcriptBucket) string {
	return bucket.SessionID + ":" + bucket.ChannelID + ":" + bucket.BucketStart.Format(time.RFC3339Nano) + ":" + bucket.BucketEnd.Format(time.RFC3339Nano)
}

func computeAlignments(chatBuckets []chat.ChatBucket, transcriptBuckets []transcriptBucket, window time.Duration) []alignmentBucket {
	result := computeAnalysis(chatBuckets, transcriptBuckets, window)
	return dashboardAlignments(result.Alignments)
}

func computeAnalysis(chatBuckets []chat.ChatBucket, transcriptBuckets []transcriptBucket, window time.Duration) analysis.Result {
	analyzer := analysis.NewAnalyzer(analysis.AnalyzerConfig{AlignmentWindow: window})
	return analyzer.AnalyzeBuckets(analysis.BucketAnalysisInput{
		ChatBuckets:       chatBuckets,
		TranscriptBuckets: analysisTranscriptBuckets(transcriptBuckets),
	})
}

func analysisTranscriptBuckets(transcriptBuckets []transcriptBucket) []analysis.TranscriptBucket {
	input := make([]analysis.TranscriptBucket, 0, len(transcriptBuckets))
	for _, bucket := range transcriptBuckets {
		input = append(input, analysis.TranscriptBucket{
			Type:                 bucket.Type,
			SessionID:            bucket.SessionID,
			ChannelID:            bucket.ChannelID,
			BucketStart:          bucket.BucketStart,
			BucketEnd:            bucket.BucketEnd,
			Text:                 bucket.Text,
			Language:             bucket.Language,
			TranscriptConfidence: bucket.TranscriptConfidence,
			SentimentScore:       bucket.SentimentScore,
			SentimentConfidence:  bucket.SentimentConfidence,
			SentimentLabel:       bucket.SentimentLabel,
			SentimentModel:       bucket.SentimentModel,
			SentimentStatus:      bucket.SentimentStatus,
			SentimentLatencyMS:   bucket.SentimentLatencyMS,
		})
	}
	return input
}

func dashboardAlignments(items []analysis.AlignmentBucket) []alignmentBucket {
	alignments := make([]alignmentBucket, 0, len(items))
	for _, item := range items {
		alignments = append(alignments, alignmentBucket{
			Type:                  item.Type,
			SessionID:             item.SessionID,
			ChannelID:             item.ChannelID,
			WindowStart:           item.WindowStart,
			WindowEnd:             item.WindowEnd,
			ChatBucketStart:       item.ChatBucketStart,
			ChatBucketEnd:         item.ChatBucketEnd,
			TranscriptBucketStart: item.TranscriptBucketStart,
			TranscriptBucketEnd:   item.TranscriptBucketEnd,
			ChatSentiment:         item.ChatSentiment,
			ChatConfidence:        item.ChatConfidence,
			ChatMessageCount:      item.ChatMessageCount,
			TranscriptSentiment:   item.TranscriptSentiment,
			TranscriptConfidence:  item.TranscriptConfidence,
			TranscriptTextLength:  item.TranscriptTextLength,
			Delta:                 item.Delta,
			Similarity:            item.Similarity,
			Relationship:          item.Relationship,
			OverlapSeconds:        item.OverlapSeconds,
			Quality:               item.Quality,
			QualityFlags:          item.QualityFlags,
		})
	}
	return alignments
}
