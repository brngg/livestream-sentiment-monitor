package nlp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/bucket"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

type Client struct {
	Endpoint string
	Client   *http.Client
}

type AnalyzeRequest struct {
	SessionID       string             `json:"session_id"`
	ChannelID       string             `json:"channel_id"`
	BucketStart     time.Time          `json:"bucket_start"`
	BucketEnd       time.Time          `json:"bucket_end"`
	PeakWindowStart *time.Time         `json:"peak_window_start,omitempty"`
	PeakWindowEnd   *time.Time         `json:"peak_window_end,omitempty"`
	Messages        []chat.ChatMessage `json:"messages"`
}

type AnalyzeResponse struct {
	SessionID      string                 `json:"session_id"`
	ChannelID      string                 `json:"channel_id"`
	BucketStart    time.Time              `json:"bucket_start"`
	BucketEnd      time.Time              `json:"bucket_end"`
	MessageCount   int                    `json:"message_count"`
	AnalyzedCount  int                    `json:"analyzed_count"`
	SentimentScore float64                `json:"sentiment_score"`
	Positive       float64                `json:"positive"`
	Neutral        float64                `json:"neutral"`
	Negative       float64                `json:"negative"`
	Confidence     float64                `json:"confidence"`
	Model          string                 `json:"model"`
	LatencyMS      int64                  `json:"latency_ms"`
	MessageScores  []MessageScoreResponse `json:"message_scores"`
}

type MessageScoreResponse struct {
	MessageID      string    `json:"message_id"`
	Timestamp      time.Time `json:"timestamp"`
	Username       string    `json:"username"`
	DisplayName    string    `json:"display_name"`
	Text           string    `json:"text"`
	Label          string    `json:"label"`
	Confidence     float64   `json:"confidence"`
	SentimentScore float64   `json:"sentiment_score"`
}

func (c Client) Enabled() bool {
	return strings.TrimSpace(c.Endpoint) != ""
}

func (c Client) AnalyzeBucket(ctx context.Context, item bucket.DetailedBucket) (chat.ChatBucket, error) {
	if !c.Enabled() {
		return item.Bucket, nil
	}

	reqBody := AnalyzeRequest{
		SessionID:       item.Bucket.SessionID,
		ChannelID:       item.Bucket.ChannelID,
		BucketStart:     item.Bucket.BucketStart,
		BucketEnd:       item.Bucket.BucketEnd,
		PeakWindowStart: item.Bucket.PeakWindowStart,
		PeakWindowEnd:   item.Bucket.PeakWindowEnd,
		Messages:        item.Messages,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return item.Bucket, fmt.Errorf("marshal analyzer request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/analyze/chat-bucket"), bytes.NewReader(body))
	if err != nil {
		return item.Bucket, fmt.Errorf("create analyzer request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return item.Bucket, fmt.Errorf("analyze bucket: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return item.Bucket, fmt.Errorf("analyze bucket: unexpected status %s", resp.Status)
	}

	var analyzed AnalyzeResponse
	if err := json.NewDecoder(resp.Body).Decode(&analyzed); err != nil {
		return item.Bucket, fmt.Errorf("decode analyzer response: %w", err)
	}

	out := item.Bucket
	out.ChatSentiment = analyzed.SentimentScore
	out.SentimentConfidence = analyzed.Confidence
	out.AnalyzedCount = analyzed.AnalyzedCount
	out.PositiveRatio = analyzed.Positive
	out.NeutralRatio = analyzed.Neutral
	out.NegativeRatio = analyzed.Negative
	out.SentimentModel = analyzed.Model
	out.AnalysisLatencyMS = analyzed.LatencyMS
	out.AnalysisStatus = "python"
	out.MessageScores = make([]chat.MessageScore, 0, len(analyzed.MessageScores))
	for _, score := range analyzed.MessageScores {
		out.MessageScores = append(out.MessageScores, chat.MessageScore{
			MessageID:      score.MessageID,
			Timestamp:      score.Timestamp,
			Username:       score.Username,
			DisplayName:    score.DisplayName,
			Text:           score.Text,
			Label:          score.Label,
			Confidence:     score.Confidence,
			SentimentScore: score.SentimentScore,
		})
	}
	return out, nil
}

func (c Client) endpoint(path string) string {
	return strings.TrimRight(c.Endpoint, "/") + path
}
