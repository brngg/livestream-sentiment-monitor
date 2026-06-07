package main

import (
	"context"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func (s *server) persistSystemMetricsSnapshot(reason string) {
	if s.store == nil {
		return
	}
	if _, ok := s.store.(storage.NoopStore); ok {
		return
	}
	sessionID, channel := s.currentSessionIdentity()
	stats := s.persistenceStats()
	recordedAt := time.Now().UTC()
	metrics := []storage.SystemMetric{
		systemMetric(sessionID, "storage.write_failures", float64(stats.WriteFailures), "writes", recordedAt, reason, channel),
		systemMetric(sessionID, "storage.dropped_writes", float64(stats.DroppedWrites), "writes", recordedAt, reason, channel),
		systemMetric(sessionID, "storage.queue_depth", float64(stats.QueueDepth), "writes", recordedAt, reason, channel),
		systemMetric(sessionID, "model.fallback_count", float64(s.metrics.modelFallbacks.Load()), "events", recordedAt, reason, channel),
		systemMetric(sessionID, "model.slow_count", float64(s.metrics.modelSlow.Load()), "events", recordedAt, reason, channel),
		systemMetric(sessionID, "asr.backpressure_count", float64(s.metrics.asrBackpressure.Load()), "events", recordedAt, reason, channel),
		systemMetric(sessionID, "reaction_window.count", float64(s.metrics.reactionWindows.Load()), "windows", recordedAt, reason, channel),
	}
	s.persistAsync("save_system_metrics_snapshot", func(ctx context.Context) error {
		for _, metric := range metrics {
			if err := s.store.SaveMetric(ctx, metric); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *server) currentSessionIdentity() (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Session, s.state.Channel
}

func systemMetric(sessionID, name string, value float64, unit string, recordedAt time.Time, reason, channel string) storage.SystemMetric {
	meta := map[string]any{"reason": reason}
	if channel != "" {
		meta["channel_id"] = channel
	}
	return storage.SystemMetric{
		SessionID:  sessionID,
		Name:       name,
		Value:      value,
		Unit:       unit,
		RecordedAt: recordedAt,
		Meta:       meta,
	}
}
