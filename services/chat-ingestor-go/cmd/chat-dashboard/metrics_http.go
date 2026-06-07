package main

import (
	"fmt"
	"net/http"
	"strings"
)

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(s.prometheusMetrics()))
}

func (s *server) prometheusMetrics() string {
	stats := s.persistenceStats()
	_, dailyStarts, dailyLimit := s.liveStartCounterSnapshot()
	activeSessions := s.activeSessionCount()

	var out strings.Builder
	writePrometheusMetric(&out, "stream_dashboard_active_sessions", "Active dashboard ingestion sessions.", "gauge", float64(activeSessions))
	writePrometheusMetric(&out, "stream_dashboard_max_active_sessions", "Configured maximum active dashboard ingestion sessions.", "gauge", float64(s.maxActiveSessions()))
	writePrometheusMetric(&out, "stream_dashboard_daily_live_starts", "Accepted live starts for the current UTC day.", "gauge", float64(dailyStarts))
	writePrometheusMetric(&out, "stream_dashboard_daily_live_start_limit", "Configured accepted live start limit per UTC day; 0 means unlimited.", "gauge", float64(dailyLimit))
	writePrometheusMetric(&out, "stream_dashboard_model_fallbacks_total", "Chat bucket analyses that fell back from the Python model.", "counter", float64(s.metrics.modelFallbacks.Load()))
	writePrometheusMetric(&out, "stream_dashboard_model_slow_total", "Chat bucket analyses that exceeded the dashboard fallback timeout.", "counter", float64(s.metrics.modelSlow.Load()))
	writePrometheusMetric(&out, "stream_dashboard_asr_backpressure_total", "ASR backpressure events observed through transcript events.", "counter", float64(s.metrics.asrBackpressure.Load()))
	writePrometheusMetric(&out, "stream_dashboard_reaction_windows_total", "Reaction windows emitted by the dashboard.", "counter", float64(s.metrics.reactionWindows.Load()))
	writePrometheusMetric(&out, "stream_dashboard_event_publish_failures_total", "Failed attempts to publish durable bucket events to the event gateway.", "counter", float64(s.metrics.eventPublishFailures.Load()))
	writePrometheusMetric(&out, "stream_dashboard_storage_write_failures_total", "Persistent storage write failures.", "counter", float64(stats.WriteFailures))
	writePrometheusMetric(&out, "stream_dashboard_storage_dropped_writes_total", "Persistent storage writes dropped by the queue.", "counter", float64(stats.DroppedWrites))
	writePrometheusMetric(&out, "stream_dashboard_storage_queue_depth", "Current persistent storage queue depth.", "gauge", float64(stats.QueueDepth))
	writePrometheusMetric(&out, "stream_dashboard_storage_consecutive_failures", "Current consecutive persistent storage failures.", "gauge", float64(stats.ConsecutiveFailures))
	return out.String()
}

func writePrometheusMetric(out *strings.Builder, name, help, metricType string, value float64) {
	fmt.Fprintf(out, "# HELP %s %s\n", name, help)
	fmt.Fprintf(out, "# TYPE %s %s\n", name, metricType)
	fmt.Fprintf(out, "%s %g\n", name, value)
}
