package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

type Publisher interface {
	Publish(ctx context.Context, bucket chat.ChatBucket) error
}

type LogPublisher struct {
	Logger *slog.Logger
}

func (p LogPublisher) Publish(ctx context.Context, bucket chat.ChatBucket) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info(
		"chat bucket",
		"session_id", bucket.SessionID,
		"channel_id", bucket.ChannelID,
		"bucket_start", bucket.BucketStart.Format(time.RFC3339),
		"bucket_end", bucket.BucketEnd.Format(time.RFC3339),
		"message_count", bucket.MessageCount,
		"unique_chatters", bucket.UniqueChatters,
		"chat_sentiment", bucket.ChatSentiment,
		"sentiment_confidence", bucket.SentimentConfidence,
	)
	return nil
}

type HTTPPublisher struct {
	Endpoint string
	Client   *http.Client
}

func (p HTTPPublisher) Publish(ctx context.Context, bucket chat.ChatBucket) error {
	if p.Endpoint == "" {
		return fmt.Errorf("http publisher endpoint is required")
	}

	body, err := json.Marshal(bucket)
	if err != nil {
		return fmt.Errorf("marshal bucket: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("publish bucket: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("publish bucket: unexpected status %s", resp.Status)
	}
	return nil
}
