package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/analysis"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/events"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

const (
	defaultAlignmentWindow = 30 * time.Second
	defaultStorageTimeout  = 2 * time.Second
	defaultMaxSessionItems = 500
	maxEventBodyBytes      = 1 << 20
)

type appConfig struct {
	Addr                 string
	AlignmentWindow      time.Duration
	StorageTimeout       time.Duration
	MaxBucketsPerSession int
	DatabaseURL          string
}

type alignmentStore interface {
	SaveAlignment(context.Context, storage.AlignmentBucket) error
}

type closeableStore interface {
	Close()
}

type server struct {
	cfg             appConfig
	logger          *slog.Logger
	analyzer        analysis.Analyzer
	store           alignmentStore
	resultPublisher events.Publisher
	storageStatus   string
	persist         bool
	hub             *eventHub
	metrics         metricCounters

	mu       sync.Mutex
	sessions map[string]*sessionState
}

type sessionState struct {
	SessionID         string
	ChannelID         string
	ChatBuckets       []chat.ChatBucket
	TranscriptBuckets []analysis.TranscriptBucket
	Result            analysis.Result
	UpdatedAt         time.Time
}

type sessionAnalysisResponse struct {
	Type                  string                         `json:"type"`
	SessionID             string                         `json:"session_id"`
	ChannelID             string                         `json:"channel_id,omitempty"`
	UpdatedAt             time.Time                      `json:"updated_at,omitempty"`
	ChatBucketCount       int                            `json:"chat_bucket_count"`
	TranscriptBucketCount int                            `json:"transcript_bucket_count"`
	AlignmentCount        int                            `json:"alignment_count"`
	SignalWindowCount     int                            `json:"signal_window_count"`
	SignalEventCount      int                            `json:"signal_event_count"`
	InsightCount          int                            `json:"insight_count"`
	ChatBuckets           []chat.ChatBucket              `json:"chat_buckets,omitempty"`
	TranscriptBuckets     []analysis.TranscriptBucket    `json:"transcript_buckets,omitempty"`
	Alignments            []analysis.AlignmentBucket     `json:"alignments,omitempty"`
	SignalWindows         []analysis.SignalWindow        `json:"signal_windows,omitempty"`
	SignalEvents          []analysis.SignalEvent         `json:"signal_events,omitempty"`
	Insights              []analysis.Insight             `json:"insights,omitempty"`
	InsightSummary        analysis.SessionInsightSummary `json:"insight_summary"`
}

type analysisSSEEvent struct {
	Type      string                  `json:"type"`
	SessionID string                  `json:"session_id"`
	Result    sessionAnalysisResponse `json:"result"`
	EmittedAt time.Time               `json:"emitted_at"`
}

type eventMeta struct {
	Type      string
	SessionID string
	ChannelID string
	Payload   []byte
}

type metricCounters struct {
	eventsReceived      atomic.Uint64
	eventsAccepted      atomic.Uint64
	eventsRejected      atomic.Uint64
	chatBuckets         atomic.Uint64
	transcriptBuckets   atomic.Uint64
	analysisRuns        atomic.Uint64
	persistedAlignments atomic.Uint64
	persistenceErrors   atomic.Uint64
	kafkaErrors         atomic.Uint64
	publishedResults    atomic.Uint64
	sseClients          atomic.Uint64
}

func newServer(cfg appConfig, store alignmentStore, storageStatus string, logger *slog.Logger) *server {
	if cfg.AlignmentWindow <= 0 {
		cfg.AlignmentWindow = defaultAlignmentWindow
	}
	if cfg.StorageTimeout <= 0 {
		cfg.StorageTimeout = defaultStorageTimeout
	}
	if cfg.MaxBucketsPerSession <= 0 {
		cfg.MaxBucketsPerSession = defaultMaxSessionItems
	}
	if storageStatus == "" {
		storageStatus = "disabled"
	}
	return &server{
		cfg:           cfg,
		logger:        logger,
		analyzer:      analysis.NewAnalyzer(analysis.AnalyzerConfig{AlignmentWindow: cfg.AlignmentWindow}),
		store:         store,
		storageStatus: storageStatus,
		persist:       store != nil && strings.TrimSpace(cfg.DatabaseURL) != "",
		hub:           newEventHub(),
		sessions:      map[string]*sessionState{},
	}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /sessions/{session_id}/analysis", s.handleSessionAnalysis)
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.HandleFunc("POST /events", s.handlePostEvents)
	return mux
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	if s.storageStatus == "unavailable" || s.metrics.persistenceErrors.Load() > 0 {
		status = "degraded"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":              status,
		"service":             "analysis-service",
		"storage":             s.storageStatus,
		"persistence_enabled": s.persist,
		"sessions":            s.sessionCount(),
		"metrics": map[string]uint64{
			"events_received":      s.metrics.eventsReceived.Load(),
			"events_accepted":      s.metrics.eventsAccepted.Load(),
			"events_rejected":      s.metrics.eventsRejected.Load(),
			"chat_buckets":         s.metrics.chatBuckets.Load(),
			"transcript_buckets":   s.metrics.transcriptBuckets.Load(),
			"analysis_runs":        s.metrics.analysisRuns.Load(),
			"persisted_alignments": s.metrics.persistedAlignments.Load(),
			"persistence_errors":   s.metrics.persistenceErrors.Load(),
			"kafka_errors":         s.metrics.kafkaErrors.Load(),
			"published_results":    s.metrics.publishedResults.Load(),
			"sse_clients":          s.metrics.sseClients.Load(),
		},
	})
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	current := s.currentGauges()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_events_received_total counter\nanalysis_service_events_received_total %d\n", s.metrics.eventsReceived.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_events_accepted_total counter\nanalysis_service_events_accepted_total %d\n", s.metrics.eventsAccepted.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_events_rejected_total counter\nanalysis_service_events_rejected_total %d\n", s.metrics.eventsRejected.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_chat_buckets_total counter\nanalysis_service_chat_buckets_total %d\n", s.metrics.chatBuckets.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_transcript_buckets_total counter\nanalysis_service_transcript_buckets_total %d\n", s.metrics.transcriptBuckets.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_analysis_runs_total counter\nanalysis_service_analysis_runs_total %d\n", s.metrics.analysisRuns.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_persisted_alignments_total counter\nanalysis_service_persisted_alignments_total %d\n", s.metrics.persistedAlignments.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_persistence_errors_total counter\nanalysis_service_persistence_errors_total %d\n", s.metrics.persistenceErrors.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_kafka_errors_total counter\nanalysis_service_kafka_errors_total %d\n", s.metrics.kafkaErrors.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_published_results_total counter\nanalysis_service_published_results_total %d\n", s.metrics.publishedResults.Load())
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_sessions gauge\nanalysis_service_sessions %d\n", current.sessions)
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_alignments gauge\nanalysis_service_alignments %d\n", current.alignments)
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_signal_windows gauge\nanalysis_service_signal_windows %d\n", current.signalWindows)
	_, _ = fmt.Fprintf(w, "# TYPE analysis_service_sse_clients gauge\nanalysis_service_sse_clients %d\n", s.metrics.sseClients.Load())
	if s.persist {
		_, _ = fmt.Fprint(w, "# TYPE analysis_service_persistence_enabled gauge\nanalysis_service_persistence_enabled 1\n")
	} else {
		_, _ = fmt.Fprint(w, "# TYPE analysis_service_persistence_enabled gauge\nanalysis_service_persistence_enabled 0\n")
	}
}

func (s *server) handleSessionAnalysis(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("session_id"))
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "session_id is required"})
		return
	}
	result, ok := s.snapshot(sessionID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session analysis not found"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	events := s.hub.add()
	s.metrics.sseClients.Add(1)
	defer func() {
		s.hub.remove(events)
		s.metrics.sseClients.Add(^uint64(0))
	}()

	_, _ = fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case event, ok := <-events:
			if !ok {
				return
			}
			if sessionID != "" && event.SessionID != sessionID {
				continue
			}
			payload, err := json.Marshal(event)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, payload)
			flusher.Flush()
		}
	}
}

func (s *server) handlePostEvents(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxEventBodyBytes))
	if err != nil {
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "event body is too large or unreadable"})
		return
	}
	result, err := s.ingestEvent(r.Context(), body)
	if err != nil {
		s.metrics.eventsRejected.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (s *server) ingestEvent(ctx context.Context, body []byte) (sessionAnalysisResponse, error) {
	s.metrics.eventsReceived.Add(1)
	meta, err := eventPayload(body)
	if err != nil {
		return sessionAnalysisResponse{}, err
	}

	var result sessionAnalysisResponse
	switch meta.Type {
	case "chat_bucket":
		var bucket chat.ChatBucket
		if err := json.Unmarshal(meta.Payload, &bucket); err != nil {
			return sessionAnalysisResponse{}, fmt.Errorf("decode chat_bucket: %w", err)
		}
		applyEventMetaToChatBucket(&bucket, meta)
		if err := validateChatBucket(bucket); err != nil {
			return sessionAnalysisResponse{}, err
		}
		s.setSessionChannel(bucket.SessionID, bucket.ChannelID)
		result = s.addChatBucket(bucket)
		s.metrics.chatBuckets.Add(1)
	case "transcript_bucket":
		var bucket analysis.TranscriptBucket
		if err := json.Unmarshal(meta.Payload, &bucket); err != nil {
			return sessionAnalysisResponse{}, fmt.Errorf("decode transcript_bucket: %w", err)
		}
		applyEventMetaToTranscriptBucket(&bucket, meta)
		if err := validateTranscriptBucket(bucket); err != nil {
			return sessionAnalysisResponse{}, err
		}
		s.setSessionChannel(bucket.SessionID, bucket.ChannelID)
		result = s.addTranscriptBucket(bucket)
		s.metrics.transcriptBuckets.Add(1)
	default:
		return sessionAnalysisResponse{}, fmt.Errorf("unsupported event type %q", meta.Type)
	}

	s.metrics.eventsAccepted.Add(1)
	s.persistAlignments(ctx, result.Alignments)
	s.broadcastAnalysisResult(result)
	s.publishAnalysisResult(ctx, result)
	return result, nil
}

func (s *server) setSessionChannel(sessionID, channelID string) {
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(channelID) == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessionState(sessionID)
	if state.ChannelID == "" {
		state.ChannelID = channelID
	}
}

func (s *server) addChatBucket(bucket chat.ChatBucket) sessionAnalysisResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.sessionState(bucket.SessionID)
	if upsertChatBucket(&state.ChatBuckets, bucket) {
		s.recomputeLocked(state)
	}
	return state.response()
}

func (s *server) addTranscriptBucket(bucket analysis.TranscriptBucket) sessionAnalysisResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.sessionState(bucket.SessionID)
	if upsertTranscriptBucket(&state.TranscriptBuckets, bucket) {
		s.recomputeLocked(state)
	}
	return state.response()
}

func (s *server) sessionState(sessionID string) *sessionState {
	state, ok := s.sessions[sessionID]
	if ok {
		return state
	}
	state = &sessionState{SessionID: sessionID}
	s.sessions[sessionID] = state
	return state
}

func (s *server) recomputeLocked(state *sessionState) {
	sortChatBuckets(state.ChatBuckets)
	sortTranscriptBuckets(state.TranscriptBuckets)
	trimChatBuckets(&state.ChatBuckets, s.cfg.MaxBucketsPerSession)
	trimTranscriptBuckets(&state.TranscriptBuckets, s.cfg.MaxBucketsPerSession)
	state.Result = s.analyzer.AnalyzeBuckets(analysis.BucketAnalysisInput{
		SessionID:         state.SessionID,
		ChatBuckets:       state.ChatBuckets,
		TranscriptBuckets: state.TranscriptBuckets,
	})
	state.UpdatedAt = time.Now().UTC()
	s.metrics.analysisRuns.Add(1)
}

func (s *server) snapshot(sessionID string) (sessionAnalysisResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[sessionID]
	if !ok {
		return sessionAnalysisResponse{}, false
	}
	return state.response(), true
}

func (s *server) sessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

type currentGauges struct {
	sessions      int
	alignments    int
	signalWindows int
}

func (s *server) currentGauges() currentGauges {
	s.mu.Lock()
	defer s.mu.Unlock()
	var gauges currentGauges
	gauges.sessions = len(s.sessions)
	for _, state := range s.sessions {
		gauges.alignments += len(state.Result.Alignments)
		gauges.signalWindows += len(state.Result.SignalWindows)
	}
	return gauges
}

func (state *sessionState) response() sessionAnalysisResponse {
	return sessionAnalysisResponse{
		Type:                  "analysis_result",
		SessionID:             state.SessionID,
		ChannelID:             state.ChannelID,
		UpdatedAt:             state.UpdatedAt,
		ChatBucketCount:       len(state.ChatBuckets),
		TranscriptBucketCount: len(state.TranscriptBuckets),
		AlignmentCount:        len(state.Result.Alignments),
		SignalWindowCount:     len(state.Result.SignalWindows),
		SignalEventCount:      len(state.Result.SignalEvents),
		InsightCount:          len(state.Result.Insights),
		ChatBuckets:           append([]chat.ChatBucket(nil), state.ChatBuckets...),
		TranscriptBuckets:     append([]analysis.TranscriptBucket(nil), state.TranscriptBuckets...),
		Alignments:            append([]analysis.AlignmentBucket(nil), state.Result.Alignments...),
		SignalWindows:         append([]analysis.SignalWindow(nil), state.Result.SignalWindows...),
		SignalEvents:          append([]analysis.SignalEvent(nil), state.Result.SignalEvents...),
		Insights:              append([]analysis.Insight(nil), state.Result.Insights...),
		InsightSummary:        state.Result.InsightSummary,
	}
}

func (s *server) persistAlignments(parent context.Context, alignments []analysis.AlignmentBucket) {
	if !s.persist || s.store == nil || len(alignments) == 0 {
		return
	}
	for _, item := range alignments {
		ctx, cancel := context.WithTimeout(parent, s.cfg.StorageTimeout)
		err := s.store.SaveAlignment(ctx, storageAlignment(item))
		cancel()
		if err != nil {
			s.metrics.persistenceErrors.Add(1)
			if s.logger != nil {
				s.logger.Warn("persist alignment", "session_id", item.SessionID, "error", err)
			}
			continue
		}
		s.metrics.persistedAlignments.Add(1)
	}
}

func (s *server) broadcastAnalysisResult(result sessionAnalysisResponse) {
	s.hub.broadcast(analysisSSEEvent{
		Type:      "analysis_result",
		SessionID: result.SessionID,
		Result:    result,
		EmittedAt: time.Now().UTC(),
	})
}

func (s *server) publishAnalysisResult(parent context.Context, result sessionAnalysisResponse) {
	if s.resultPublisher == nil {
		return
	}
	envelope, err := events.NewEnvelope(events.EventTypeAnalysisResult, "analysis-service", result)
	if err != nil {
		s.metrics.kafkaErrors.Add(1)
		if s.logger != nil {
			s.logger.Warn("build analysis result event", "session_id", result.SessionID, "error", err)
		}
		return
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		s.metrics.kafkaErrors.Add(1)
		return
	}
	msg, err := envelope.PublishMessage(raw)
	if err != nil {
		s.metrics.kafkaErrors.Add(1)
		return
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := s.resultPublisher.Publish(ctx, msg); err != nil {
		s.metrics.kafkaErrors.Add(1)
		if s.logger != nil {
			s.logger.Warn("publish analysis result", "session_id", result.SessionID, "error", err)
		}
		return
	}
	s.metrics.publishedResults.Add(1)
}

func eventPayload(body []byte) (eventMeta, error) {
	body = []byte(strings.TrimSpace(string(body)))
	if len(body) == 0 {
		return eventMeta{}, errors.New("event body is required")
	}

	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &fields); err != nil {
		return eventMeta{}, fmt.Errorf("decode event object: %w", err)
	}

	meta := eventMeta{
		Type:      normalizedEventType(firstJSONText(fields, "event_type", "type", "kind")),
		SessionID: firstJSONText(fields, "session_id", "session"),
		ChannelID: firstJSONText(fields, "channel_id", "channel"),
		Payload:   body,
	}
	for _, key := range []string{"payload", "data", "bucket", "chat_bucket", "transcript_bucket"} {
		if raw, ok := fields[key]; ok && len(raw) > 0 && string(raw) != "null" {
			meta.Payload = raw
			if meta.Type == "" || meta.Type == "event" {
				payloadFields := map[string]json.RawMessage{}
				if err := json.Unmarshal(raw, &payloadFields); err == nil {
					meta.Type = normalizedEventType(firstJSONText(payloadFields, "event_type", "type", "kind"))
				}
			}
			break
		}
	}

	if meta.Type == "" || meta.Type == "event" {
		meta.Type = inferEventType(meta.Payload)
	}
	if meta.Type == "" {
		return eventMeta{}, errors.New("event type is required")
	}
	return meta, nil
}

func firstJSONText(fields map[string]json.RawMessage, names ...string) string {
	for _, name := range names {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err == nil {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func normalizedEventType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "_")
	value = strings.ReplaceAll(value, ".", "_")
	switch value {
	case "chat_bucket", "chat_buckets", "bucket_chat", "chat":
		return "chat_bucket"
	case "transcript_bucket", "transcript_buckets", "bucket_transcript", "transcript":
		return "transcript_bucket"
	default:
		return value
	}
}

func inferEventType(payload []byte) string {
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(payload, &fields); err != nil {
		return ""
	}
	if _, ok := fields["chat_sentiment"]; ok {
		return "chat_bucket"
	}
	if _, ok := fields["message_count"]; ok {
		if _, hasStart := fields["bucket_start"]; hasStart {
			return "chat_bucket"
		}
	}
	if _, ok := fields["transcript_confidence"]; ok {
		return "transcript_bucket"
	}
	if _, ok := fields["text"]; ok {
		if _, hasStart := fields["bucket_start"]; hasStart {
			return "transcript_bucket"
		}
	}
	return ""
}

func applyEventMetaToChatBucket(bucket *chat.ChatBucket, meta eventMeta) {
	if bucket.Type == "" {
		bucket.Type = "chat_bucket"
	}
	if bucket.SessionID == "" {
		bucket.SessionID = meta.SessionID
	}
	if bucket.ChannelID == "" {
		bucket.ChannelID = meta.ChannelID
	}
}

func applyEventMetaToTranscriptBucket(bucket *analysis.TranscriptBucket, meta eventMeta) {
	if bucket.Type == "" {
		bucket.Type = "transcript_bucket"
	}
	if bucket.SessionID == "" {
		bucket.SessionID = meta.SessionID
	}
	if bucket.ChannelID == "" {
		bucket.ChannelID = meta.ChannelID
	}
}

func validateChatBucket(bucket chat.ChatBucket) error {
	if strings.TrimSpace(bucket.SessionID) == "" {
		return errors.New("chat_bucket session_id is required")
	}
	if bucket.BucketStart.IsZero() || bucket.BucketEnd.IsZero() {
		return errors.New("chat_bucket bucket_start and bucket_end are required")
	}
	if !bucket.BucketEnd.After(bucket.BucketStart) {
		return errors.New("chat_bucket bucket_end must be after bucket_start")
	}
	return nil
}

func validateTranscriptBucket(bucket analysis.TranscriptBucket) error {
	if strings.TrimSpace(bucket.SessionID) == "" {
		return errors.New("transcript_bucket session_id is required")
	}
	if bucket.BucketStart.IsZero() || bucket.BucketEnd.IsZero() {
		return errors.New("transcript_bucket bucket_start and bucket_end are required")
	}
	if !bucket.BucketEnd.After(bucket.BucketStart) {
		return errors.New("transcript_bucket bucket_end must be after bucket_start")
	}
	return nil
}

func upsertChatBucket(buckets *[]chat.ChatBucket, bucket chat.ChatBucket) bool {
	key := chatBucketKey(bucket)
	for index := range *buckets {
		if chatBucketKey((*buckets)[index]) != key {
			continue
		}
		if reflect.DeepEqual((*buckets)[index], bucket) {
			return false
		}
		(*buckets)[index] = bucket
		return true
	}
	*buckets = append(*buckets, bucket)
	return true
}

func upsertTranscriptBucket(buckets *[]analysis.TranscriptBucket, bucket analysis.TranscriptBucket) bool {
	key := transcriptBucketKey(bucket)
	for index := range *buckets {
		if transcriptBucketKey((*buckets)[index]) != key {
			continue
		}
		if reflect.DeepEqual((*buckets)[index], bucket) {
			return false
		}
		(*buckets)[index] = bucket
		return true
	}
	*buckets = append(*buckets, bucket)
	return true
}

func chatBucketKey(bucket chat.ChatBucket) string {
	return bucket.SessionID + ":" + bucket.ChannelID + ":" + bucket.BucketStart.UTC().Format(time.RFC3339Nano) + ":" + bucket.BucketEnd.UTC().Format(time.RFC3339Nano)
}

func transcriptBucketKey(bucket analysis.TranscriptBucket) string {
	return bucket.SessionID + ":" + bucket.ChannelID + ":" + bucket.BucketStart.UTC().Format(time.RFC3339Nano) + ":" + bucket.BucketEnd.UTC().Format(time.RFC3339Nano)
}

func sortChatBuckets(buckets []chat.ChatBucket) {
	sort.SliceStable(buckets, func(left, right int) bool {
		if buckets[left].BucketStart.Equal(buckets[right].BucketStart) {
			return buckets[left].BucketEnd.Before(buckets[right].BucketEnd)
		}
		return buckets[left].BucketStart.Before(buckets[right].BucketStart)
	})
}

func sortTranscriptBuckets(buckets []analysis.TranscriptBucket) {
	sort.SliceStable(buckets, func(left, right int) bool {
		if buckets[left].BucketStart.Equal(buckets[right].BucketStart) {
			return buckets[left].BucketEnd.Before(buckets[right].BucketEnd)
		}
		return buckets[left].BucketStart.Before(buckets[right].BucketStart)
	})
}

func trimChatBuckets(buckets *[]chat.ChatBucket, limit int) {
	if limit <= 0 || len(*buckets) <= limit {
		return
	}
	*buckets = append([]chat.ChatBucket(nil), (*buckets)[len(*buckets)-limit:]...)
}

func trimTranscriptBuckets(buckets *[]analysis.TranscriptBucket, limit int) {
	if limit <= 0 || len(*buckets) <= limit {
		return
	}
	*buckets = append([]analysis.TranscriptBucket(nil), (*buckets)[len(*buckets)-limit:]...)
}

func storageAlignment(item analysis.AlignmentBucket) storage.AlignmentBucket {
	return storage.AlignmentBucket{
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
		QualityFlags:          append([]string(nil), item.QualityFlags...),
	}
}

type eventHub struct {
	mu      sync.Mutex
	clients map[chan analysisSSEEvent]struct{}
}

func newEventHub() *eventHub {
	return &eventHub{clients: map[chan analysisSSEEvent]struct{}{}}
}

func (h *eventHub) add() chan analysisSSEEvent {
	ch := make(chan analysisSSEEvent, 128)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *eventHub) remove(ch chan analysisSSEEvent) {
	h.mu.Lock()
	delete(h.clients, ch)
	close(ch)
	h.mu.Unlock()
}

func (h *eventHub) broadcast(event analysisSSEEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- event:
		default:
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
