package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	storageStatus := "disabled"
	if s.cfg.DatabaseWriteEnabled && strings.TrimSpace(s.cfg.DatabaseURL) != "" {
		storageStatus = "enabled"
		if _, ok := s.store.(storage.NoopStore); ok {
			storageStatus = "unavailable"
		}
	}
	nlpStatus := s.nlpHealthStatus(r.Context())
	persistenceStats := s.persistenceStats()
	healthStatus := "ok"
	if storageStatus == "unavailable" || s.storageDegraded(persistenceStats) || nlpStatus.Status == "unavailable" {
		healthStatus = "degraded"
		if storageStatus == "enabled" {
			storageStatus = "degraded"
		}
	}
	statusCode := http.StatusOK
	if healthStatus == "degraded" {
		statusCode = http.StatusServiceUnavailable
	}
	writeJSON(w, statusCode, map[string]any{
		"status":      healthStatus,
		"storage":     storageStatus,
		"nlp":         nlpStatus,
		"persistence": persistenceStats,
		"metrics": map[string]any{
			"model_fallback_count":   s.metrics.modelFallbacks.Load(),
			"model_slow_count":       s.metrics.modelSlow.Load(),
			"asr_backpressure_count": s.metrics.asrBackpressure.Load(),
			"reaction_window_count":  s.metrics.reactionWindows.Load(),
		},
	})
}

type nlpHealthState struct {
	Status    string `json:"status"`
	Endpoint  string `json:"endpoint,omitempty"`
	Error     string `json:"error,omitempty"`
	LatencyMS int64  `json:"latency_ms,omitempty"`
}

func (s *server) nlpHealthStatus(ctx context.Context) nlpHealthState {
	endpoint := strings.TrimRight(strings.TrimSpace(s.cfg.NLPAnalyzerURL), "/")
	if endpoint == "" {
		return nlpHealthState{Status: "disabled"}
	}
	started := time.Now()
	probeCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, endpoint+"/health", nil)
	if err != nil {
		return nlpHealthState{Status: "unavailable", Endpoint: endpoint, Error: err.Error()}
	}
	resp, err := http.DefaultClient.Do(req)
	latencyMS := time.Since(started).Milliseconds()
	if err != nil {
		return nlpHealthState{Status: "unavailable", Endpoint: endpoint, Error: err.Error(), LatencyMS: latencyMS}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nlpHealthState{Status: "unavailable", Endpoint: endpoint, Error: resp.Status, LatencyMS: latencyMS}
	}
	return nlpHealthState{Status: "ok", Endpoint: endpoint, LatencyMS: latencyMS}
}

func (s *server) storageDegraded(stats persistenceQueueStats) bool {
	threshold := s.cfg.DatabaseHealthFailureThreshold
	if threshold <= 0 {
		threshold = defaultDatabaseHealthFailureThreshold
	}
	return stats.DroppedWrites > 0 || stats.ConsecutiveFailures >= uint64(threshold)
}
