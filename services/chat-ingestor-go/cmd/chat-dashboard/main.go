package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/analysis"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/bucket"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/config"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/filter"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/nlp"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/reaction"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/sentiment"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/twitchapi"
)

const defaultNLPAnalyzerURL = "http://127.0.0.1:8091"
const defaultNLPTimeout = 5 * time.Second
const defaultNLPLateTimeout = 15 * time.Second
const maxNLPLateTimeout = 30 * time.Second

type appConfig struct {
	Addr                    string
	FrontendDir             string
	BucketEvery             time.Duration
	TwitchNick              string
	TwitchToken             string
	TwitchAddr              string
	TwitchClientID          string
	TwitchSecret            string
	TwitchAppToken          string
	NLPAnalyzerURL          string
	NLPTimeout              time.Duration
	NLPLateTimeout          time.Duration
	TranscriptURL           string
	TranscriptPoll          time.Duration
	TranscriptBucketSeconds int
	TranscriptChunkSeconds  int
	EventGatewayURL         string
	EventBusEnabled         bool
	AnalysisServiceURL      string
	AnalysisServiceRequired bool
	AdminToken              string
	PublicStartEnabled      bool
	PublicStartConfigured   bool
	MaxActiveSessions       int
	SessionMaxDuration      time.Duration
	LiveSourceAllowlist     string
	DailyLiveStartLimit     int

	DatabaseURL                    string
	DatabaseWriteTimeout           time.Duration
	DatabaseWriteEnabled           bool
	DatabaseWriteQueueSize         int
	DatabaseWriteMaxRetries        int
	DatabaseHealthFailureThreshold int
	ReplayFixturePath              string
	ChatSampleLimitPerBucket       int
	ChatIdentityHashSalt           string
}

type server struct {
	cfg           appConfig
	logger        *slog.Logger
	hub           *eventHub
	store         storage.Store
	persistence   *persistenceQueue
	metrics       systemMetricCounters
	persistenceMu sync.Mutex

	mu           sync.Mutex
	sessionID    string
	activeRunID  string
	cancelActive context.CancelFunc
	state        dashboardState
	humanLabels  map[string]string

	dailyLiveStartsDate string
	dailyLiveStarts     int
}

type startRequest struct {
	Channel string `json:"channel"`
}

type streamSource struct {
	Platform    string
	ID          string
	URL         string
	Label       string
	StreamID    string
	ChatEnabled bool
}

type dashboardEvent struct {
	Type           string                  `json:"type"`
	Status         string                  `json:"status,omitempty"`
	Session        string                  `json:"session_id,omitempty"`
	Channel        string                  `json:"channel,omitempty"`
	Stream         *streamInfo             `json:"stream,omitempty"`
	Message        *chat.ChatMessage       `json:"message,omitempty"`
	Bucket         *chat.ChatBucket        `json:"bucket,omitempty"`
	Transcript     *transcriptBucket       `json:"transcript_bucket,omitempty"`
	ReactionWindow *chat.ReactionWindow    `json:"reaction_window,omitempty"`
	Alignments     []alignmentBucket       `json:"alignments,omitempty"`
	SignalWindows  []analysis.SignalWindow `json:"signal_windows,omitempty"`
	Error          string                  `json:"error,omitempty"`
}

type transcriptBucket struct {
	Type                 string              `json:"type"`
	SessionID            string              `json:"session_id"`
	ChannelID            string              `json:"channel_id"`
	BucketStart          time.Time           `json:"bucket_start"`
	BucketEnd            time.Time           `json:"bucket_end"`
	AudioStartedAt       *time.Time          `json:"audio_started_at,omitempty"`
	AudioEndedAt         *time.Time          `json:"audio_ended_at,omitempty"`
	TranscribedAt        *time.Time          `json:"transcribed_at,omitempty"`
	Text                 string              `json:"text"`
	Language             string              `json:"language"`
	TranscriptConfidence float64             `json:"transcript_confidence"`
	TranscriptStatus     string              `json:"transcript_status,omitempty"`
	SentimentScore       *float64            `json:"sentiment_score"`
	SentimentConfidence  *float64            `json:"sentiment_confidence"`
	SentimentLabel       string              `json:"sentiment_label"`
	SentimentModel       string              `json:"sentiment_model"`
	SentimentStatus      string              `json:"sentiment_status"`
	SentimentLatencyMS   *int64              `json:"sentiment_latency_ms"`
	ASRLatencyMS         *int64              `json:"asr_latency_ms,omitempty"`
	PipelineLatencyMS    *int64              `json:"pipeline_latency_ms,omitempty"`
	AudioSeconds         float64             `json:"audio_seconds,omitempty"`
	SegmentCount         int                 `json:"segment_count,omitempty"`
	WordCount            int                 `json:"word_count,omitempty"`
	EmptyRatio           float64             `json:"empty_ratio,omitempty"`
	RepairAddedWords     int                 `json:"repair_added_words,omitempty"`
	RepairChangedRatio   float64             `json:"repair_changed_ratio,omitempty"`
	Segments             []transcriptSegment `json:"segments,omitempty"`
	Quality              map[string]any      `json:"quality,omitempty"`
}

type transcriptSegment struct {
	Start      float64          `json:"start"`
	End        float64          `json:"end"`
	Text       string           `json:"text"`
	Confidence *float64         `json:"confidence"`
	Words      []transcriptWord `json:"words,omitempty"`
}

type transcriptWord struct {
	Start       float64  `json:"start"`
	End         float64  `json:"end"`
	Text        string   `json:"text"`
	Probability *float64 `json:"probability,omitempty"`
}

type alignmentBucket struct {
	Type                  string    `json:"type"`
	SessionID             string    `json:"session_id"`
	ChannelID             string    `json:"channel_id"`
	WindowStart           time.Time `json:"window_start"`
	WindowEnd             time.Time `json:"window_end"`
	ChatBucketStart       time.Time `json:"chat_bucket_start"`
	ChatBucketEnd         time.Time `json:"chat_bucket_end"`
	TranscriptBucketStart time.Time `json:"transcript_bucket_start"`
	TranscriptBucketEnd   time.Time `json:"transcript_bucket_end"`
	ChatSentiment         float64   `json:"chat_sentiment"`
	ChatConfidence        float64   `json:"chat_confidence"`
	ChatMessageCount      int       `json:"chat_message_count"`
	TranscriptSentiment   float64   `json:"transcript_sentiment"`
	TranscriptConfidence  float64   `json:"transcript_confidence"`
	TranscriptTextLength  int       `json:"transcript_text_length"`
	Delta                 float64   `json:"delta"`
	Similarity            float64   `json:"similarity"`
	Relationship          string    `json:"relationship"`
	OverlapSeconds        int       `json:"overlap_seconds"`
	Quality               float64   `json:"quality"`
	QualityFlags          []string  `json:"quality_flags"`
}

type streamInfo struct {
	ID           string `json:"id,omitempty"`
	UserID       string `json:"user_id,omitempty"`
	Platform     string `json:"platform,omitempty"`
	URL          string `json:"url,omitempty"`
	Title        string `json:"title"`
	Game         string `json:"game"`
	ViewerCount  int    `json:"viewer_count"`
	StartedAt    string `json:"started_at"`
	Language     string `json:"language"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
}

type dashboardState struct {
	Status          string                  `json:"status"`
	Session         string                  `json:"session_id,omitempty"`
	Channel         string                  `json:"channel,omitempty"`
	Stream          *streamInfo             `json:"stream,omitempty"`
	MessageCount    int                     `json:"message_count"`
	BucketCount     int                     `json:"bucket_count"`
	Messages        []chat.ChatMessage      `json:"messages"`
	Buckets         []chat.ChatBucket       `json:"buckets"`
	ReactionWindows []chat.ReactionWindow   `json:"reaction_windows,omitempty"`
	Transcripts     []transcriptBucket      `json:"transcript_buckets,omitempty"`
	Alignments      []alignmentBucket       `json:"alignments,omitempty"`
	SignalWindows   []analysis.SignalWindow `json:"signal_windows,omitempty"`
	Error           string                  `json:"error,omitempty"`
}

type sessionSummaryResponse struct {
	storage.SessionSummary
	LatestSignalWindows []analysis.SignalWindow `json:"latest_signal_windows,omitempty"`
	SignalEvents        []analysis.SignalEvent  `json:"signal_events,omitempty"`
}

type sessionReplayResponse struct {
	storage.SessionReplay
	SignalWindows  []analysis.SignalWindow        `json:"signal_windows,omitempty"`
	SignalEvents   []analysis.SignalEvent         `json:"signal_events,omitempty"`
	Insights       []analysis.Insight             `json:"insights,omitempty"`
	InsightSummary analysis.SessionInsightSummary `json:"insight_summary"`
}

type sessionProofResponse struct {
	Proof     storage.ReplayProof `json:"proof"`
	Persisted bool                `json:"persisted"`
	Error     string              `json:"error,omitempty"`
}

type sessionEvaluationResponse struct {
	Session          storage.SessionHistory          `json:"session"`
	Evaluation       analysis.SessionEvaluation      `json:"evaluation"`
	ReplayLimit      int                             `json:"replay_limit,omitempty"`
	Partial          bool                            `json:"partial"`
	TruncatedSources []storage.ReplayProofTruncation `json:"truncated_sources,omitempty"`
}

type systemMetricCounters struct {
	modelFallbacks       atomic.Uint64
	modelSlow            atomic.Uint64
	asrBackpressure      atomic.Uint64
	reactionWindows      atomic.Uint64
	eventPublishFailures atomic.Uint64
}

type transcriptServiceState struct {
	Status       string            `json:"status"`
	SessionID    string            `json:"session_id"`
	ChannelID    string            `json:"channel_id"`
	BucketCount  int               `json:"bucket_count"`
	Error        string            `json:"error"`
	LatestBucket *transcriptBucket `json:"latest_bucket"`
}

type labelRequest struct {
	SessionID string `json:"session_id"`
	MessageID string `json:"message_id"`
	Label     string `json:"label"`
}

type signalWindowLabelRequest struct {
	SessionID             string `json:"session_id"`
	WindowStart           string `json:"window_start"`
	WindowEnd             string `json:"window_end"`
	PredictedEvent        string `json:"predicted_event"`
	PredictedRelationship string `json:"predicted_relationship"`
	ReactionType          string `json:"reaction_type"`
	TargetType            string `json:"target_type"`
	TargetText            string `json:"target_text"`
	DivergenceType        string `json:"divergence_type"`
	EventStart            string `json:"event_start"`
	EventPeak             string `json:"event_peak"`
	Correctness           string `json:"correctness"`
	EventLabel            string `json:"event_label"`
	Notes                 string `json:"notes"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := loadDashboardDotEnv(os.Args); err != nil {
		logger.Error("load .env", "error", err)
		os.Exit(1)
	}

	cfg := appConfig{}
	flag.StringVar(&cfg.Addr, "addr", ":8090", "HTTP dashboard address")
	flag.StringVar(&cfg.FrontendDir, "frontend-dir", envString("FRONTEND_DIR", defaultFrontendDir()), "frontend static asset directory")
	flag.DurationVar(&cfg.BucketEvery, "bucket-every", envDuration("CHAT_BUCKET_EVERY", bucket.DefaultWindow), "chat bucket window duration")
	flag.StringVar(&cfg.TwitchNick, "twitch-nick", envString("TWITCH_IRC_NICK", ""), "Twitch IRC nickname")
	flag.StringVar(&cfg.TwitchToken, "twitch-oauth", envString("TWITCH_IRC_OAUTH_TOKEN", ""), "Twitch IRC OAuth token")
	flag.StringVar(&cfg.TwitchAddr, "twitch-addr", envString("TWITCH_IRC_ADDR", chat.DefaultTwitchIRCAddr), "Twitch IRC address")
	flag.StringVar(&cfg.TwitchClientID, "twitch-client-id", envString("TWITCH_CLIENT_ID", ""), "Twitch API client ID")
	flag.StringVar(&cfg.TwitchSecret, "twitch-client-secret", envString("TWITCH_CLIENT_SECRET", ""), "Twitch API client secret")
	flag.StringVar(&cfg.TwitchAppToken, "twitch-app-access-token", envString("TWITCH_APP_ACCESS_TOKEN", ""), "Twitch app access token")
	flag.StringVar(&cfg.NLPAnalyzerURL, "nlp-analyzer-url", envString("NLP_ANALYZER_URL", defaultNLPAnalyzerURL), "Python NLP analyzer base URL")
	flag.DurationVar(&cfg.NLPTimeout, "nlp-timeout", envDuration("NLP_ANALYZER_TIMEOUT", 5*time.Second), "timeout for Python NLP analyzer requests")
	flag.DurationVar(&cfg.NLPLateTimeout, "nlp-late-timeout", envDuration("NLP_ANALYZER_LATE_TIMEOUT", defaultNLPLateTimeout), "maximum wait for late Python NLP analyzer results before final fallback")
	flag.StringVar(&cfg.TranscriptURL, "transcript-url", envString("TRANSCRIPT_INGESTOR_URL", "http://127.0.0.1:8092"), "optional transcript ingestor base URL")
	flag.DurationVar(&cfg.TranscriptPoll, "transcript-poll", envDuration("TRANSCRIPT_POLL_INTERVAL", time.Second), "poll interval for transcript buckets")
	flag.IntVar(&cfg.TranscriptBucketSeconds, "transcript-bucket-seconds", envInt("TRANSCRIPT_BUCKET_SECONDS", defaultTranscriptBucketSeconds), "transcript sentiment bucket size in seconds when starting transcript sessions")
	flag.IntVar(&cfg.TranscriptChunkSeconds, "transcript-chunk-seconds", envInt("TRANSCRIPT_DEFAULT_CHUNK_SECONDS", defaultTranscriptChunkSeconds), "transcript chunk size in seconds when starting transcript sessions")
	flag.StringVar(&cfg.EventGatewayURL, "event-gateway-url", envString("EVENT_GATEWAY_URL", "http://event-gateway:8093"), "optional event gateway base URL for durable bucket events")
	flag.BoolVar(&cfg.EventBusEnabled, "event-bus-enabled", envBool("EVENT_BUS_ENABLED", true), "publish chat/transcript bucket events to the event gateway")
	flag.StringVar(&cfg.AnalysisServiceURL, "analysis-service-url", envString("ANALYSIS_SERVICE_URL", "http://analysis-service:8094"), "optional analysis service base URL")
	flag.BoolVar(&cfg.AnalysisServiceRequired, "analysis-service-required", envBool("ANALYSIS_SERVICE_REQUIRED", true), "require dedicated analysis service in production deployments")
	flag.StringVar(&cfg.AdminToken, "admin-token", envString("ADMIN_TOKEN", ""), "bearer token required for mutating dashboard endpoints")
	flag.BoolVar(&cfg.PublicStartEnabled, "public-start-enabled", envBool("PUBLIC_START_ENABLED", false), "allow mutating dashboard starts without ADMIN_TOKEN in local development")
	flag.IntVar(&cfg.MaxActiveSessions, "max-active-sessions", envInt("MAX_ACTIVE_SESSIONS", 1), "maximum concurrently active dashboard sessions")
	flag.DurationVar(&cfg.SessionMaxDuration, "session-max-duration", envDuration("SESSION_MAX_DURATION", 20*time.Minute), "maximum duration for a dashboard live session")
	flag.StringVar(&cfg.LiveSourceAllowlist, "live-source-allowlist", envString("LIVE_SOURCE_ALLOWLIST", ""), "comma-separated list of allowed live source ids, platform:id values, or URLs")
	flag.IntVar(&cfg.DailyLiveStartLimit, "daily-live-start-limit", envInt("DAILY_LIVE_START_LIMIT", 0), "maximum accepted live starts per UTC day; 0 disables the limit")
	flag.StringVar(&cfg.DatabaseURL, "database-url", envString("DATABASE_URL", ""), "optional Postgres database URL")
	flag.DurationVar(&cfg.DatabaseWriteTimeout, "database-write-timeout", envDuration("DATABASE_WRITE_TIMEOUT", 10*time.Second), "timeout for storage reads and queued writes")
	flag.BoolVar(&cfg.DatabaseWriteEnabled, "database-write-enabled", envBool("DATABASE_WRITE_ENABLED", false), "enable persistent storage writes")
	flag.IntVar(&cfg.DatabaseWriteQueueSize, "database-write-queue-size", envInt("DATABASE_WRITE_QUEUE_SIZE", defaultDatabaseWriteQueueSize), "maximum queued persistent storage writes")
	flag.IntVar(&cfg.DatabaseWriteMaxRetries, "database-write-max-retries", envInt("DATABASE_WRITE_MAX_RETRIES", defaultDatabaseWriteMaxRetries), "maximum retry attempts for queued persistent storage writes")
	flag.IntVar(&cfg.DatabaseHealthFailureThreshold, "database-health-failure-threshold", envInt("DATABASE_HEALTH_FAILURE_THRESHOLD", defaultDatabaseHealthFailureThreshold), "consecutive persistent write failures before health degrades")
	flag.StringVar(&cfg.ReplayFixturePath, "replay-fixture", envString("REPLAY_FIXTURE_PATH", ""), "optional offline replay fixture JSON for history and replay endpoints")
	flag.IntVar(&cfg.ChatSampleLimitPerBucket, "chat-sample-limit-per-bucket", envInt("CHAT_SAMPLE_LIMIT_PER_BUCKET", 10), "maximum chat message samples persisted per bucket")
	flag.StringVar(&cfg.ChatIdentityHashSalt, "chat-identity-hash-salt", envString("CHAT_IDENTITY_HASH_SALT", ""), "salt used to hash persisted chat identities")
	flag.Parse()
	databaseWriteEnabledSet := false
	publicStartConfigured := envPresent("PUBLIC_START_ENABLED")
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "database-write-enabled" {
			databaseWriteEnabledSet = true
		}
		if f.Name == "public-start-enabled" {
			publicStartConfigured = true
		}
	})
	cfg.PublicStartConfigured = publicStartConfigured
	if !databaseWriteEnabledSet && strings.TrimSpace(os.Getenv("DATABASE_WRITE_ENABLED")) == "" {
		cfg.DatabaseWriteEnabled = strings.TrimSpace(cfg.DatabaseURL) != ""
	}
	if cfg.BucketEvery <= 0 {
		logger.Error("invalid chat bucket duration", "value", cfg.BucketEvery)
		os.Exit(1)
	}
	if cfg.TranscriptBucketSeconds <= 0 || cfg.TranscriptBucketSeconds > maxTranscriptBucketSeconds {
		logger.Error("invalid transcript bucket seconds", "value", cfg.TranscriptBucketSeconds, "min", 1, "max", maxTranscriptBucketSeconds)
		os.Exit(1)
	}
	if cfg.TranscriptChunkSeconds <= 0 || cfg.TranscriptChunkSeconds > cfg.TranscriptBucketSeconds {
		logger.Error("invalid transcript chunk seconds", "value", cfg.TranscriptChunkSeconds, "min", 1, "max", cfg.TranscriptBucketSeconds)
		os.Exit(1)
	}
	if cfg.MaxActiveSessions < 0 {
		logger.Error("invalid max active sessions", "value", cfg.MaxActiveSessions, "min", 0)
		os.Exit(1)
	}
	if cfg.SessionMaxDuration < 0 {
		logger.Error("invalid session max duration", "value", cfg.SessionMaxDuration, "min", 0)
		os.Exit(1)
	}
	if cfg.DailyLiveStartLimit < 0 {
		logger.Error("invalid daily live start limit", "value", cfg.DailyLiveStartLimit, "min", 0)
		os.Exit(1)
	}

	store := initStorage(context.Background(), &cfg, logger)
	persistence := newPersistenceQueue(cfg.DatabaseWriteQueueSize, cfg.DatabaseWriteTimeout, cfg.DatabaseWriteMaxRetries, logger)
	s := &server{
		cfg:         cfg,
		logger:      logger,
		hub:         newEventHub(),
		store:       store,
		persistence: persistence,
		humanLabels: map[string]string{},
	}
	defer func() {
		persistence.Close()
		store.Close()
	}()

	mux := http.NewServeMux()
	s.registerRoutes(mux, cfg.FrontendDir)

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		s.stopActiveSession()
	}()

	logger.Info("dashboard listening", "addr", cfg.Addr, "url", "http://localhost"+cfg.Addr, "frontend_dir", cfg.FrontendDir)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("dashboard stopped", "error", err)
		os.Exit(1)
	}
}

func loadDashboardDotEnv(args []string) error {
	var firstErr error
	if err := config.LoadDotEnv(".env"); err != nil {
		firstErr = err
	}
	if frontendDir := frontendDirArg(args); frontendDir != "" {
		if err := config.LoadDotEnv(filepath.Join(frontendDir, "..", "..", "..", "services", "chat-ingestor-go", ".env")); err == nil {
			return nil
		} else if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func frontendDirArg(args []string) string {
	for index, arg := range args {
		if strings.HasPrefix(arg, "-frontend-dir=") {
			return strings.TrimSpace(strings.TrimPrefix(arg, "-frontend-dir="))
		}
		if arg == "-frontend-dir" && index+1 < len(args) {
			return strings.TrimSpace(args[index+1])
		}
	}
	return ""
}

func (s *server) handleStartSession(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	source, err := parseStreamSource(req.Channel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.validateLiveStartSource(source); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	channel := source.ID

	s.mu.Lock()
	if s.cancelActive != nil && s.state.Channel == channel && isActiveSessionStatus(s.state.Status) {
		existingSession := s.state.Session
		s.mu.Unlock()
		transcriptWarning := ""
		if err := s.ensureTranscriptSession(r.Context(), source, existingSession); err != nil {
			transcriptWarning = err.Error()
			s.logger.Warn("transcript session recovery failed", "session_id", existingSession, "channel", channel, "error", err)
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"session_id":         existingSession,
			"channel":            channel,
			"status":             "already_ingesting",
			"transcript_warning": transcriptWarning,
		})
		return
	}
	s.mu.Unlock()

	var status twitchapi.StreamStatus
	var stream streamInfo
	reusedSession := false
	if source.Platform == "twitch" {
		status, err = s.getLiveStatus(r.Context(), channel)
		if err != nil {
			if !s.hasActiveSession() {
				s.broadcast(dashboardEvent{Type: "error", Status: "error", Channel: channel, Error: err.Error()})
			}
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if !status.Live {
			if !s.hasActiveSession() {
				s.broadcast(dashboardEvent{Type: "status", Status: "offline", Channel: channel})
			}
			writeJSON(w, http.StatusAccepted, map[string]string{
				"channel": channel,
				"status":  "offline",
			})
			return
		}
		stream = streamInfoFromStatus(status)
		stream.Platform = source.Platform
		stream.URL = source.URL
	} else {
		status = streamStatusFromSource(source)
		stream = streamInfoFromSource(source)
	}

	s.mu.Lock()
	if s.cancelActive != nil && s.state.Channel == channel && isActiveSessionStatus(s.state.Status) {
		existingSession := s.state.Session
		s.mu.Unlock()
		transcriptWarning := ""
		if err := s.ensureTranscriptSession(r.Context(), source, existingSession); err != nil {
			transcriptWarning = err.Error()
			s.logger.Warn("transcript session recovery failed", "session_id", existingSession, "channel", channel, "error", err)
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"session_id":         existingSession,
			"channel":            channel,
			"status":             "already_ingesting",
			"transcript_warning": transcriptWarning,
		})
		return
	}
	s.mu.Unlock()

	var sessionID string
	if source.Platform == "twitch" {
		sessionID, reusedSession = s.resolveStreamSession(r.Context(), channel, status)
	} else {
		sessionID = sessionIDForStream(channel, source.StreamID)
		s.persistSessionCreated(sessionID, channel, status)
	}
	runID := ingestionRunID(sessionID)
	if err := s.reserveDailyLiveStart(time.Now().UTC()); err != nil {
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}

	ctx, cancel := s.newSessionContext()

	s.mu.Lock()
	if s.cancelActive != nil && s.state.Channel == channel && isActiveSessionStatus(s.state.Status) {
		existingSession := s.state.Session
		s.mu.Unlock()
		cancel()
		transcriptWarning := ""
		if err := s.ensureTranscriptSession(r.Context(), source, existingSession); err != nil {
			transcriptWarning = err.Error()
			s.logger.Warn("transcript session recovery failed", "session_id", existingSession, "channel", channel, "error", err)
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"session_id":         existingSession,
			"channel":            channel,
			"status":             "already_ingesting",
			"transcript_warning": transcriptWarning,
		})
		return
	}
	previousSession := s.state.Session
	previousChannel := s.state.Channel
	previousRunID := s.activeRunID
	previousActive := s.cancelActive != nil && isActiveSessionStatus(s.state.Status)
	if s.cancelActive != nil {
		s.cancelActive()
	}
	s.sessionID = sessionID
	s.activeRunID = runID
	s.cancelActive = cancel
	s.state = dashboardState{
		Status:          "starting",
		Session:         sessionID,
		Channel:         channel,
		Stream:          &stream,
		Messages:        nil,
		Buckets:         nil,
		ReactionWindows: nil,
		Transcripts:     nil,
		Alignments:      nil,
		SignalWindows:   nil,
	}
	s.mu.Unlock()

	if previousActive && previousSession != "" && previousSession != sessionID {
		s.stopTranscriptServiceSession(context.Background(), previousSession)
		s.persistSessionStatus(previousSession, previousChannel, "stopped", nil, "superseded by "+sessionID)
		s.persistIngestionRunStatus(previousRunID, previousSession, "stopped", "superseded by "+sessionID, "")
	}

	transcriptWarning := ""
	if err := s.startTranscriptSession(r.Context(), source, sessionID); err != nil {
		transcriptWarning = err.Error()
		s.logger.Warn("transcript service unavailable; continuing with chat and stream preview", "session_id", sessionID, "channel", channel, "error", err)
	}

	s.persistIngestionRunStarted(runID, sessionID)

	if source.ChatEnabled {
		go s.runSession(ctx, runID, sessionID, channel, status)
	} else {
		go s.runTranscriptOnlySession(ctx, runID, sessionID, channel, stream)
	}
	go s.streamTranscriptEvents(ctx, sessionID, channel)
	go s.pollTranscriptBuckets(ctx, sessionID, source)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"session_id":         sessionID,
		"channel":            channel,
		"status":             "starting",
		"reused":             reusedSession,
		"run_id":             runID,
		"transcript_warning": transcriptWarning,
	})
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	events := s.hub.add()
	defer s.hub.remove(events)

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
		case event := <-events:
			payload, err := json.Marshal(event)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	state := s.state
	state.Messages = append([]chat.ChatMessage(nil), s.state.Messages...)
	state.Buckets = append([]chat.ChatBucket(nil), s.state.Buckets...)
	state.ReactionWindows = append([]chat.ReactionWindow(nil), s.state.ReactionWindows...)
	state.Transcripts = append([]transcriptBucket(nil), s.state.Transcripts...)
	state.Alignments = append([]alignmentBucket(nil), s.state.Alignments...)
	state.SignalWindows = append([]analysis.SignalWindow(nil), s.state.SignalWindows...)
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, state)
}

func (s *server) handleSessionHistory(w http.ResponseWriter, r *http.Request) {
	limit := envIntFromString(r.URL.Query().Get("limit"), 25)
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.DatabaseWriteTimeout)
	defer cancel()

	sessions, err := s.store.ListSessions(ctx, limit)
	if err != nil {
		s.logger.Warn("list stored sessions", "error", err)
		http.Error(w, "session history unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *server) handleSessionSummary(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("session_id"))
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.DatabaseWriteTimeout)
	defer cancel()
	summary, err := s.store.GetSessionSummary(ctx, sessionID)
	if storage.IsNotFound(err) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Warn("get stored session summary", "session_id", sessionID, "error", err)
		http.Error(w, "session summary unavailable", http.StatusServiceUnavailable)
		return
	}
	result := analyzeStoredSession(
		summary.Session.SessionID,
		summary.LatestChatBuckets,
		summary.LatestTranscriptBuckets,
		summary.LatestAlignments,
		s.cfg.BucketEvery,
	)
	writeJSON(w, http.StatusOK, sessionSummaryResponse{
		SessionSummary:      summary,
		LatestSignalWindows: result.SignalWindows,
		SignalEvents:        result.SignalEvents,
	})
}

func (s *server) handleSessionReplay(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("session_id"))
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	limit := envIntFromString(r.URL.Query().Get("limit"), 200)
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.DatabaseWriteTimeout)
	defer cancel()
	replay, err := s.store.GetSessionReplay(ctx, sessionID, limit)
	if storage.IsNotFound(err) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Warn("get stored session replay", "session_id", sessionID, "error", err)
		http.Error(w, "session replay unavailable", http.StatusServiceUnavailable)
		return
	}

	result := analyzeStoredSession(
		replay.Session.SessionID,
		replay.ChatBuckets,
		replay.TranscriptBuckets,
		replay.Alignments,
		s.cfg.BucketEvery,
	)
	replay.AgentReviews = mergeReplayAgentReviews(replay.AgentReviews, prescoreReplayAgentReviews(replay))
	writeJSON(w, http.StatusOK, sessionReplayResponse{
		SessionReplay:  replay,
		SignalWindows:  result.SignalWindows,
		SignalEvents:   result.SignalEvents,
		Insights:       result.Insights,
		InsightSummary: result.InsightSummary,
	})
}

func (s *server) handleSessionEvaluation(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("session_id"))
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	limit := proofReplayLimit(r.URL.Query().Get("limit"))
	ctx, cancel := context.WithTimeout(r.Context(), databaseWriteTimeout(s.cfg.DatabaseWriteTimeout))
	defer cancel()
	replay, err := s.store.GetSessionReplay(ctx, sessionID, limit)
	if storage.IsNotFound(err) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Warn("get stored session evaluation replay", "session_id", sessionID, "error", err)
		http.Error(w, "session evaluation unavailable", http.StatusServiceUnavailable)
		return
	}

	result := analyzeStoredSession(
		replay.Session.SessionID,
		replay.ChatBuckets,
		replay.TranscriptBuckets,
		replay.Alignments,
		s.cfg.BucketEvery,
	)
	generatedAt := time.Now().UTC()
	proof := storage.BuildReplayProof(replay, storage.ReplayProofOptions{GeneratedAt: generatedAt, ReplayLimit: limit})
	evaluation := analysis.EvaluateSession(analysis.EvaluationInput{
		SessionID:   replay.Session.SessionID,
		GeneratedAt: generatedAt,
		Windows:     evaluationWindows(result.SignalWindows, replay.ChatBuckets),
		Labels:      storedEvaluationLabels(replay.WindowLabels),
	})
	writeJSON(w, http.StatusOK, sessionEvaluationResponse{
		Session:          replay.Session,
		Evaluation:       evaluation,
		ReplayLimit:      limit,
		Partial:          proof.Partial,
		TruncatedSources: proof.TruncatedSources,
	})
}

func (s *server) handleSessionProof(w http.ResponseWriter, r *http.Request) {
	s.writeSessionProof(w, r, false)
}

func (s *server) handleSessionProofPersist(w http.ResponseWriter, r *http.Request) {
	s.writeSessionProof(w, r, true)
}

func (s *server) writeSessionProof(w http.ResponseWriter, r *http.Request, persist bool) {
	sessionID := strings.TrimSpace(r.PathValue("session_id"))
	if sessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	limit := proofReplayLimit(r.URL.Query().Get("limit"))
	ctx, cancel := context.WithTimeout(r.Context(), databaseWriteTimeout(s.cfg.DatabaseWriteTimeout))
	defer cancel()
	replay, err := s.store.GetSessionReplay(ctx, sessionID, limit)
	if storage.IsNotFound(err) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("get stored session proof replay", "session_id", sessionID, "error", err)
		}
		http.Error(w, "session proof unavailable", http.StatusServiceUnavailable)
		return
	}

	proof := storage.BuildReplayProof(replay, storage.ReplayProofOptions{GeneratedAt: time.Now().UTC(), ReplayLimit: limit})
	if persist {
		if proof.Partial {
			writeJSON(w, http.StatusConflict, sessionProofResponse{
				Proof:     proof,
				Persisted: false,
				Error:     "partial replay proof cannot be persisted as canonical system metrics",
			})
			return
		}
		if err := s.persistReplayProofMetrics(ctx, proof); err != nil {
			s.recordSynchronousPersistenceFailure("persist_replay_proof_metrics", err)
			if s.logger != nil {
				s.logger.Warn("persist replay proof metrics", "session_id", sessionID, "error", err)
			}
			http.Error(w, "unable to persist session proof", http.StatusServiceUnavailable)
			return
		}
	}

	writeJSON(w, http.StatusOK, sessionProofResponse{Proof: proof, Persisted: persist})
}

func proofReplayLimit(raw string) int {
	limit := envIntFromString(raw, 500)
	if limit <= 0 || limit > 500 {
		return 500
	}
	return limit
}

func (s *server) handleLabel(w http.ResponseWriter, r *http.Request) {
	var req labelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	req.Label = strings.ToLower(strings.TrimSpace(req.Label))
	if req.SessionID == "" || req.MessageID == "" {
		http.Error(w, "session_id and message_id are required", http.StatusBadRequest)
		return
	}
	if req.Label != "positive" && req.Label != "neutral" && req.Label != "negative" {
		http.Error(w, "label must be positive, neutral, or negative", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.humanLabels[labelKey(req.SessionID, req.MessageID)] = req.Label
	s.applyHumanLabelsLocked()
	s.mu.Unlock()

	s.persistHumanLabel(req)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "saved"})
}

func (s *server) handleSignalWindowLabel(w http.ResponseWriter, r *http.Request) {
	var req signalWindowLabelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	record, err := signalWindowLabelFromRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), databaseWriteTimeout(s.cfg.DatabaseWriteTimeout))
	defer cancel()
	if err := s.store.SaveSignalWindowLabel(ctx, record); err != nil {
		s.recordSynchronousPersistenceFailure("save_signal_window_label", err)
		if s.logger != nil {
			s.logger.Warn("save signal window label", "error", err)
		}
		http.Error(w, "unable to save signal window label", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusAccepted, record)
}

func (s *server) runSession(ctx context.Context, runID, sessionID, channel string, status twitchapi.StreamStatus) {
	stream := streamInfoFromStatus(status)
	s.broadcast(dashboardEvent{
		Type:    "status",
		Status:  "ingesting",
		Session: sessionID,
		Channel: channel,
		Stream:  &stream,
	})

	reader := chat.TwitchReader{
		SessionID:  sessionID,
		ChannelID:  channel,
		Nick:       s.cfg.TwitchNick,
		OAuthToken: s.cfg.TwitchToken,
		Addr:       s.cfg.TwitchAddr,
	}
	messageFilter := filter.MessageFilter{}
	analyzer := sentiment.NewLexiconAnalyzer()
	nlpClient := nlp.Client{Endpoint: s.cfg.NLPAnalyzerURL}
	bucketizer := bucket.NewStreamBucketizer(s.cfg.BucketEvery)
	reactionAnalyzer := reaction.NewAnalyzer(reaction.DefaultWindow, reaction.DefaultRetention)
	reactionTicker := time.NewTicker(time.Second)
	defer reactionTicker.Stop()

	messages, errs := reader.Read(ctx)
	for {
		select {
		case <-ctx.Done():
			s.flushBuckets(bucketizer, sessionID, channel, reactionAnalyzer)
			s.broadcast(dashboardEvent{Type: "status", Status: "stopped", Session: sessionID, Channel: channel})
			if s.isCurrentRun(runID, sessionID) {
				s.stopTranscriptService(context.Background(), runID, sessionID)
				s.persistIngestionRunStatus(runID, sessionID, "stopped", sessionStopReason(ctx), "")
				s.clearActiveRunIfCurrent(runID, sessionID)
			}
			return
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				s.broadcast(dashboardEvent{Type: "error", Status: "error", Session: sessionID, Channel: channel, Error: err.Error()})
				if s.isCurrentRun(runID, sessionID) {
					s.persistIngestionRunStatus(runID, sessionID, "error", "", err.Error())
					s.clearActiveRunIfCurrent(runID, sessionID)
				}
				return
			}
		case msg, ok := <-messages:
			if !ok {
				s.flushBuckets(bucketizer, sessionID, channel, reactionAnalyzer)
				s.broadcast(dashboardEvent{Type: "status", Status: "ended", Session: sessionID, Channel: channel})
				if s.isCurrentRun(runID, sessionID) {
					s.persistIngestionRunStatus(runID, sessionID, "ended", "", "")
					s.clearActiveRunIfCurrent(runID, sessionID)
				}
				return
			}

			msg = filter.Normalize(msg)
			if !messageFilter.Allow(msg) {
				continue
			}

			msgCopy := msg
			s.broadcast(dashboardEvent{Type: "chat_message", Session: sessionID, Channel: channel, Message: &msgCopy})
			reactionAnalyzer.Add(msg)

			scored := chat.ScoredMessage{Message: msg, Sentiment: analyzer.Analyze(msg)}
			items := attachPeakMetadata(bucketizer.AddDetailed(scored), reactionAnalyzer.RecentWindows())
			s.analyzeAndBroadcastBuckets(ctx, nlpClient, items)
		case tick := <-reactionTicker.C:
			window := reactionAnalyzer.WindowAt(tick, sessionID, channel)
			s.broadcastReactionWindow(window)
		}
	}
}

func (s *server) runTranscriptOnlySession(ctx context.Context, runID, sessionID, channel string, stream streamInfo) {
	streamCopy := stream
	s.broadcast(dashboardEvent{
		Type:    "status",
		Status:  "ingesting",
		Session: sessionID,
		Channel: channel,
		Stream:  &streamCopy,
	})

	<-ctx.Done()
	s.broadcast(dashboardEvent{Type: "status", Status: "stopped", Session: sessionID, Channel: channel})
	if s.isCurrentRun(runID, sessionID) {
		s.stopTranscriptService(context.Background(), runID, sessionID)
		s.persistIngestionRunStatus(runID, sessionID, "stopped", sessionStopReason(ctx), "")
		s.clearActiveRunIfCurrent(runID, sessionID)
	}
}

func sessionStopReason(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "session max duration reached"
	}
	return "cancelled"
}

func (s *server) flushBuckets(bucketizer *bucket.StreamBucketizer, sessionID, channel string, reactionAnalyzer *reaction.Analyzer) {
	items := bucketizer.FlushDetailed()
	if reactionAnalyzer != nil {
		items = attachPeakMetadata(items, reactionAnalyzer.RecentWindows())
	}
	s.analyzeAndBroadcastBuckets(context.Background(), nlp.Client{Endpoint: s.cfg.NLPAnalyzerURL}, items)
}

func attachPeakMetadata(items []bucket.DetailedBucket, windows []chat.ReactionWindow) []bucket.DetailedBucket {
	if len(items) == 0 {
		return items
	}
	out := make([]bucket.DetailedBucket, len(items))
	copy(out, items)
	for index := range out {
		out[index].Bucket = reaction.AttachPeakMetadata(out[index].Bucket, windows)
	}
	return out
}

func (s *server) analyzeAndBroadcastBuckets(ctx context.Context, client nlp.Client, items []bucket.DetailedBucket) {
	for _, item := range items {
		itemCopy := item
		if !client.Enabled() {
			itemCopy.Bucket.AnalysisStatus = "local"
			bucketCopy := itemCopy.Bucket
			s.broadcast(dashboardEvent{Type: "chat_bucket", Session: bucketCopy.SessionID, Channel: bucketCopy.ChannelID, Bucket: &bucketCopy})
			continue
		}

		go s.analyzeAndBroadcastBucket(ctx, client, itemCopy)
	}
}

func (s *server) analyzeAndBroadcastBucket(ctx context.Context, client nlp.Client, item bucket.DetailedBucket) {
	fallbackTimeout := s.nlpFallbackTimeout()
	lateTimeout := nlpLateTimeout(fallbackTimeout, s.cfg.NLPLateTimeout)
	analysisCtx, cancel := context.WithTimeout(ctx, lateTimeout)
	defer cancel()

	type analysisResult struct {
		bucket chat.ChatBucket
		err    error
	}
	resultCh := make(chan analysisResult, 1)
	go func() {
		analyzed, err := client.AnalyzeBucket(analysisCtx, item)
		resultCh <- analysisResult{bucket: analyzed, err: err}
	}()

	timer := time.NewTimer(fallbackTimeout)
	defer timer.Stop()

	select {
	case result := <-resultCh:
		cancel()
		if result.err != nil {
			s.broadcastAnalyzerFallback(item, "fallback", result.err)
			return
		}
		s.broadcastChatBucket(result.bucket)
	case <-timer.C:
		s.broadcastAnalyzerFallback(item, "fallback_pending", nil)
		result := <-resultCh
		cancel()
		if result.err != nil {
			s.broadcastAnalyzerFallback(item, "fallback", result.err)
			return
		}
		s.broadcastChatBucket(result.bucket)
	}
}

func (s *server) broadcastAnalyzerFallback(item bucket.DetailedBucket, status string, err error) {
	if err != nil {
		s.logger.Warn("python analyzer unavailable; using local bucket sentiment", "error", err, "session_id", item.Bucket.SessionID, "channel", item.Bucket.ChannelID)
	}
	if status == "fallback_pending" {
		s.metrics.modelSlow.Add(1)
		s.persistSystemMetricsSnapshot("model_slow")
	} else {
		s.metrics.modelFallbacks.Add(1)
		s.persistSystemMetricsSnapshot("model_fallback")
	}
	analyzed := item.Bucket
	analyzed.AnalysisStatus = status
	s.broadcastChatBucket(analyzed)
}

func (s *server) broadcastChatBucket(bucket chat.ChatBucket) {
	bucketCopy := bucket
	s.publishChatBucketEvent(bucketCopy)
	s.broadcast(dashboardEvent{Type: "chat_bucket", Session: bucketCopy.SessionID, Channel: bucketCopy.ChannelID, Bucket: &bucketCopy})
}

func (s *server) broadcastReactionWindow(window chat.ReactionWindow) {
	windowCopy := window
	count := s.metrics.reactionWindows.Add(1)
	if count == 1 || count%100 == 0 {
		s.persistSystemMetricsSnapshot("reaction_window")
	}
	s.broadcast(dashboardEvent{Type: "reaction_window", Session: windowCopy.SessionID, Channel: windowCopy.ChannelID, ReactionWindow: &windowCopy})
}

func (s *server) nlpFallbackTimeout() time.Duration {
	if s.cfg.NLPTimeout <= 0 {
		return defaultNLPTimeout
	}
	return s.cfg.NLPTimeout
}

func nlpLateTimeout(fallbackTimeout, configuredLateTimeout time.Duration) time.Duration {
	if fallbackTimeout <= 0 {
		fallbackTimeout = defaultNLPTimeout
	}
	lateTimeout := configuredLateTimeout
	if lateTimeout <= 0 {
		lateTimeout = defaultNLPLateTimeout
	}
	if lateTimeout > maxNLPLateTimeout {
		lateTimeout = maxNLPLateTimeout
	}
	if lateTimeout < fallbackTimeout {
		return fallbackTimeout
	}
	return lateTimeout
}

func (s *server) broadcast(event dashboardEvent) {
	if s.isStaleEvent(event) {
		s.logger.Debug("drop stale dashboard event", "type", event.Type, "session_id", event.Session, "channel", event.Channel)
		return
	}

	s.logger.Info("dashboard event", "type", event.Type, "status", event.Status, "channel", event.Channel)
	s.updateState(event)
	s.hub.broadcast(event)
}

func (s *server) isStaleEvent(event dashboardEvent) bool {
	if event.Session == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Session != "" && event.Session != s.state.Session
}

func (s *server) updateState(event dashboardEvent) {
	var alignments []alignmentBucket

	s.mu.Lock()

	if event.Session != "" {
		s.state.Session = event.Session
	}
	if event.Channel != "" {
		s.state.Channel = event.Channel
	}
	if event.Status != "" {
		s.state.Status = event.Status
	}
	if event.Stream != nil {
		streamCopy := *event.Stream
		s.state.Stream = &streamCopy
	}
	if event.Error != "" {
		s.state.Error = event.Error
	}
	if event.Message != nil {
		s.state.MessageCount++
		s.state.Messages = append([]chat.ChatMessage{*event.Message}, s.state.Messages...)
		if len(s.state.Messages) > 120 {
			s.state.Messages = s.state.Messages[:120]
		}
	}
	if event.ReactionWindow != nil {
		replaced := false
		for index := range s.state.ReactionWindows {
			if sameReactionWindow(s.state.ReactionWindows[index], *event.ReactionWindow) {
				s.state.ReactionWindows[index] = *event.ReactionWindow
				replaced = true
				break
			}
		}
		if !replaced {
			s.state.ReactionWindows = append([]chat.ReactionWindow{*event.ReactionWindow}, s.state.ReactionWindows...)
			s.state.ReactionWindows = retainDashboardReactionWindows(s.state.ReactionWindows, 5*time.Minute)
		}
	}
	if event.Bucket != nil {
		for index := range event.Bucket.MessageScores {
			key := labelKey(event.Bucket.SessionID, event.Bucket.MessageScores[index].MessageID)
			event.Bucket.MessageScores[index].HumanLabel = s.humanLabels[key]
		}
		replaced := false
		for index := range s.state.Buckets {
			if sameChatBucket(s.state.Buckets[index], *event.Bucket) {
				s.state.Buckets[index] = *event.Bucket
				replaced = true
				break
			}
		}
		if !replaced {
			s.state.BucketCount++
			s.state.Buckets = append([]chat.ChatBucket{*event.Bucket}, s.state.Buckets...)
			if len(s.state.Buckets) > 80 {
				s.state.Buckets = s.state.Buckets[:80]
			}
		}
		result := computeAnalysis(s.state.Buckets, s.state.Transcripts, s.cfg.BucketEvery)
		s.state.Alignments = dashboardAlignments(result.Alignments)
		s.state.SignalWindows = result.SignalWindows
		alignments = append([]alignmentBucket(nil), s.state.Alignments...)
	}
	s.mu.Unlock()

	s.persistDashboardEvent(event)
	if event.Bucket != nil {
		s.persistChatBucket(*event.Bucket)
		s.persistAlignments(alignments)
	}
}

func (s *server) applyHumanLabelsLocked() {
	for bucketIndex := range s.state.Buckets {
		bucket := &s.state.Buckets[bucketIndex]
		for scoreIndex := range bucket.MessageScores {
			score := &bucket.MessageScores[scoreIndex]
			score.HumanLabel = s.humanLabels[labelKey(bucket.SessionID, score.MessageID)]
		}
	}
}

func labelKey(sessionID, messageID string) string {
	return sessionID + ":" + messageID
}

func sameChatBucket(left, right chat.ChatBucket) bool {
	return left.SessionID == right.SessionID &&
		left.ChannelID == right.ChannelID &&
		left.BucketStart.Equal(right.BucketStart) &&
		left.BucketEnd.Equal(right.BucketEnd)
}

func sameReactionWindow(left, right chat.ReactionWindow) bool {
	return left.SessionID == right.SessionID &&
		left.ChannelID == right.ChannelID &&
		left.WindowStart.Equal(right.WindowStart) &&
		left.WindowEnd.Equal(right.WindowEnd)
}

func retainDashboardReactionWindows(windows []chat.ReactionWindow, retention time.Duration) []chat.ReactionWindow {
	if len(windows) == 0 || retention <= 0 {
		return windows
	}
	cutoff := windows[0].WindowEnd.Add(-retention)
	out := windows[:0]
	for _, window := range windows {
		if !window.WindowEnd.Before(cutoff) {
			out = append(out, window)
		}
	}
	return out
}

func analyzeStoredSession(sessionID string, chatBuckets []chat.ChatBucket, transcriptBuckets []storage.TranscriptBucket, alignments []storage.AlignmentBucket, window time.Duration) analysis.Result {
	analyzer := analysis.NewAnalyzer(analysis.AnalyzerConfig{AlignmentWindow: window})
	return analyzer.AnalyzeSession(analysis.SessionAnalysisInput{
		SessionID:         sessionID,
		ChatBuckets:       chatBuckets,
		TranscriptBuckets: storedAnalysisTranscriptBuckets(transcriptBuckets),
		Alignments:        storedAnalysisAlignments(alignments),
	})
}

func evaluationWindows(signalWindows []analysis.SignalWindow, chatBuckets []chat.ChatBucket) []analysis.SignalWindow {
	out := append([]analysis.SignalWindow(nil), signalWindows...)
	for _, bucket := range chatBuckets {
		for _, subwindow := range bucket.Subwindows {
			if subwindow.WindowStart.IsZero() || subwindow.WindowEnd.IsZero() {
				continue
			}
			eventType := evaluationEventTypeForReaction(subwindow)
			eventTimestamp := reactionSubwindowEventTimestamp(bucket, subwindow)
			window := analysis.SignalWindow{
				Type:           "reaction_window",
				SessionID:      bucket.SessionID,
				ChannelID:      bucket.ChannelID,
				Source:         firstNonEmpty(subwindow.Source, "chat"),
				StreamID:       bucket.ChannelID,
				WindowStart:    subwindow.WindowStart,
				WindowEnd:      subwindow.WindowEnd,
				MessageCount:   subwindow.MessageCount,
				ChatSentiment:  bucket.ChatSentiment,
				ReactionType:   subwindow.ReactionType,
				TargetType:     firstNonEmpty(subwindow.TargetType, "unknown"),
				TargetText:     subwindow.TargetText,
				EventHint:      subwindow.EventHint,
				Confidence:     subwindow.Confidence,
				EvidenceIDs:    append([]string(nil), subwindow.EvidenceIDs...),
				FirstEventType: eventType,
			}
			if eventType != "" {
				window.Events = []analysis.SignalEvent{{
					Type:         eventType,
					Severity:     subwindow.ReactionScore,
					Timestamp:    eventTimestamp,
					ReactionType: subwindow.ReactionType,
					TargetType:   firstNonEmpty(subwindow.TargetType, "unknown"),
					TargetText:   subwindow.TargetText,
					Source:       firstNonEmpty(subwindow.Source, "chat"),
					EventHint:    subwindow.EventHint,
					Confidence:   subwindow.Confidence,
					EvidenceIDs:  append([]string(nil), subwindow.EvidenceIDs...),
				}}
			}
			out = append(out, window)
		}
	}
	return out
}

func evaluationEventTypeForReaction(subwindow chat.ReactionSubwindow) analysis.SignalEventType {
	if subwindow.ReactionScore < 0.35 && subwindow.Confidence < 0.35 {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(subwindow.ReactionType)) {
	case "hype":
		return analysis.SignalEventHypeSpike
	case "frustration":
		return analysis.SignalEventFrustrationSpike
	case "confusion", "surprise":
		return analysis.SignalEventAudienceShift
	default:
		return ""
	}
}

func reactionSubwindowEventTimestamp(bucket chat.ChatBucket, subwindow chat.ReactionSubwindow) time.Time {
	if bucket.PeakTime != nil && !bucket.PeakTime.IsZero() && !bucket.PeakTime.Before(subwindow.WindowStart) && !bucket.PeakTime.After(subwindow.WindowEnd) {
		return *bucket.PeakTime
	}
	return subwindow.WindowEnd
}

func prescoreReplayAgentReviews(replay storage.SessionReplay) []storage.EvaluationAgentReview {
	var out []storage.EvaluationAgentReview
	for _, bucket := range replay.ChatBuckets {
		if bucket.BucketStart.IsZero() || bucket.BucketEnd.IsZero() {
			continue
		}
		out = append(out, prescoreChatBucketReview(replay, bucket))
		for _, subwindow := range bucket.Subwindows {
			if subwindow.WindowStart.IsZero() || subwindow.WindowEnd.IsZero() {
				continue
			}
			out = append(out, prescoreReactionSubwindowReview(bucket, subwindow))
		}
	}
	return out
}

func mergeReplayAgentReviews(stored, generated []storage.EvaluationAgentReview) []storage.EvaluationAgentReview {
	generatedByKey := map[string]storage.EvaluationAgentReview{}
	for _, review := range generated {
		generatedByKey[agentReviewMergeKey(review)] = review
	}

	out := make([]storage.EvaluationAgentReview, 0, len(stored)+len(generated))
	seen := map[string]struct{}{}
	for _, review := range stored {
		key := agentReviewMergeKey(review)
		if fallback, ok := generatedByKey[key]; ok {
			if strings.TrimSpace(review.Reason) == "" {
				review.Reason = fallback.Reason
			}
			if len(review.Evidence) == 0 {
				review.Evidence = fallback.Evidence
			}
			if review.Confidence == 0 {
				review.Confidence = fallback.Confidence
			}
			if review.StreamerUsefulness == 0 {
				review.StreamerUsefulness = fallback.StreamerUsefulness
			}
		}
		out = append(out, review)
		seen[key] = struct{}{}
	}
	for _, review := range generated {
		key := agentReviewMergeKey(review)
		if _, ok := seen[key]; ok {
			continue
		}
		out = append(out, review)
	}
	return out
}

func agentReviewMergeKey(review storage.EvaluationAgentReview) string {
	return strings.Join([]string{
		review.SessionID,
		review.WindowStart.UTC().Format(time.RFC3339Nano),
		review.WindowEnd.UTC().Format(time.RFC3339Nano),
		review.SourceWindowType,
	}, "\x00")
}

func prescoreChatBucketReview(replay storage.SessionReplay, bucket chat.ChatBucket) storage.EvaluationAgentReview {
	eventLabel := "none"
	reactionType := bucket.PeakReactionType
	targetType := bucket.PeakTargetType
	targetText := bucket.PeakTargetText
	confidence := bucket.SentimentConfidence
	usefulness := 0.1
	var eventStart, eventPeak *time.Time

	if bucket.PeakReactionScore != nil && *bucket.PeakReactionScore >= 0.35 {
		eventLabel = agentEventLabelForReaction(bucket.PeakReactionType)
		if eventLabel == "none" && bucket.ChatSentiment >= 0.35 {
			eventLabel = "hype_spike"
		}
		confidence = maxFloat(confidence, bucket.PeakConfidence)
		usefulness = clampFloat(0.35 + (*bucket.PeakReactionScore * 0.6))
		eventStart = bucket.PeakWindowStart
		eventPeak = bucket.PeakTime
	} else if bucket.ChatSentiment >= 0.35 {
		eventLabel = "hype_spike"
		reactionType = firstNonEmpty(reactionType, "hype")
		usefulness = clampFloat(0.25 + bucket.ChatSentiment)
	} else if bucket.ChatSentiment <= -0.35 {
		eventLabel = "frustration_spike"
		reactionType = firstNonEmpty(reactionType, "frustration")
		usefulness = clampFloat(0.25 + -bucket.ChatSentiment)
	}

	if eventLabel == "none" {
		confidence = maxFloat(confidence, 0.75)
		usefulness = 0.08
	}

	transcript := bestTranscriptForWindow(bucket.BucketStart, bucket.BucketEnd, replay.TranscriptBuckets)
	alignment := bestAlignmentForWindow(bucket.BucketStart, bucket.BucketEnd, replay.Alignments)
	return storage.EvaluationAgentReview{
		ReviewID:            agentReviewID(bucket.SessionID, "chat_bucket_30s", bucket.BucketStart, bucket.BucketEnd),
		RunID:               "auto-prescore",
		SessionID:           bucket.SessionID,
		WindowStart:         bucket.BucketStart,
		WindowEnd:           bucket.BucketEnd,
		SourceWindowType:    "chat_bucket_30s",
		Reviewer:            "auto-prescore-agent",
		Model:               "heuristic-replay-prescore",
		PromptVersion:       "stream-evaluation-v1",
		Status:              "suggested",
		PredictedEvent:      eventLabel,
		SuggestedEventLabel: eventLabel,
		Correctness:         "correct",
		ReactionType:        reactionType,
		TargetType:          targetType,
		TargetText:          targetText,
		EventStart:          eventStart,
		EventPeak:           eventPeak,
		Confidence:          clampFloat(confidence),
		StreamerUsefulness:  clampFloat(usefulness),
		Reason:              agentBucketReason(bucket, transcript, alignment, eventLabel),
		Evidence:            agentBucketEvidence(bucket, transcript, alignment),
	}
}

func prescoreReactionSubwindowReview(bucket chat.ChatBucket, subwindow chat.ReactionSubwindow) storage.EvaluationAgentReview {
	eventLabel := agentEventLabelForReaction(subwindow.ReactionType)
	if eventLabel == "none" && subwindow.ReactionScore >= 0.35 {
		eventLabel = "audience_shift"
	}
	eventStart := subwindow.WindowStart
	eventPeak := reactionSubwindowEventTimestamp(bucket, subwindow)
	return storage.EvaluationAgentReview{
		ReviewID:            agentReviewID(bucket.SessionID, "reaction_subwindow", subwindow.WindowStart, subwindow.WindowEnd),
		RunID:               "auto-prescore",
		SessionID:           bucket.SessionID,
		WindowStart:         subwindow.WindowStart,
		WindowEnd:           subwindow.WindowEnd,
		SourceWindowType:    "reaction_subwindow",
		Reviewer:            "auto-prescore-agent",
		Model:               "heuristic-replay-prescore",
		PromptVersion:       "stream-evaluation-v1",
		Status:              "suggested",
		PredictedEvent:      eventLabel,
		SuggestedEventLabel: eventLabel,
		Correctness:         "correct",
		ReactionType:        subwindow.ReactionType,
		TargetType:          subwindow.TargetType,
		TargetText:          subwindow.TargetText,
		EventStart:          &eventStart,
		EventPeak:           &eventPeak,
		Confidence:          clampFloat(maxFloat(subwindow.Confidence, subwindow.ReactionScore)),
		StreamerUsefulness:  clampFloat(0.35 + subwindow.ReactionScore*0.6),
		Reason:              agentSubwindowReason(subwindow),
		Evidence:            agentSubwindowEvidence(bucket, subwindow),
	}
}

func agentEventLabelForReaction(reactionType string) string {
	switch strings.ToLower(strings.TrimSpace(reactionType)) {
	case "hype", "joy", "celebration":
		return "hype_spike"
	case "frustration", "anger", "negative":
		return "frustration_spike"
	case "confusion", "surprise":
		return "audience_shift"
	default:
		return "none"
	}
}

func agentBucketReason(bucket chat.ChatBucket, transcript *storage.TranscriptBucket, alignment *storage.AlignmentBucket, eventLabel string) string {
	if eventLabel == "none" {
		return fmt.Sprintf("Chat appears routine for the full bucket: %d messages, sentiment %.2f, neutral ratio %.2f, and no strong reaction event requiring streamer attention.", bucket.MessageCount, bucket.ChatSentiment, bucket.NeutralRatio)
	}
	reason := fmt.Sprintf("Chat shows a %s candidate: %d messages, sentiment %.2f, peak reaction %s on %s.", eventLabel, bucket.MessageCount, bucket.ChatSentiment, firstNonEmpty(bucket.PeakReactionType, "unknown"), firstNonEmpty(bucket.PeakTargetText, "unknown target"))
	if transcript != nil && strings.TrimSpace(transcript.Text) != "" {
		reason += " Transcript context: " + strings.TrimSpace(transcript.Text)
	}
	if alignment != nil {
		reason += fmt.Sprintf(" Alignment relationship is %s with delta %.2f.", alignment.Relationship, alignment.Delta)
	}
	return reason
}

func agentSubwindowReason(subwindow chat.ReactionSubwindow) string {
	return fmt.Sprintf("Short reaction window with %d messages, reaction score %.2f, hype %.2f, confusion %.2f, frustration %.2f, and event hint %q.", subwindow.MessageCount, subwindow.ReactionScore, subwindow.HypeScore, subwindow.ConfusionScore, subwindow.FrustrationScore, subwindow.EventHint)
}

func agentBucketEvidence(bucket chat.ChatBucket, transcript *storage.TranscriptBucket, alignment *storage.AlignmentBucket) []storage.EvaluationAgentEvidence {
	evidence := []storage.EvaluationAgentEvidence{
		{
			ID:     "chat_bucket",
			Source: "chat",
			Text:   fmt.Sprintf("messages=%d, unique_chatters=%d, sentiment=%.2f, positive=%.2f, neutral=%.2f, negative=%.2f", bucket.MessageCount, bucket.UniqueChatters, bucket.ChatSentiment, bucket.PositiveRatio, bucket.NeutralRatio, bucket.NegativeRatio),
		},
	}
	for _, message := range bucket.PeakEvidenceMessages {
		evidence = append(evidence, storage.EvaluationAgentEvidence{
			ID:        message.MessageID,
			Source:    "chat",
			Timestamp: message.Timestamp.Format(time.RFC3339),
			Text:      message.Text,
		})
	}
	if len(evidence) == 0 {
		for _, score := range bucket.MessageScores {
			evidence = append(evidence, storage.EvaluationAgentEvidence{
				ID:        score.MessageID,
				Source:    "chat",
				Timestamp: score.Timestamp.Format(time.RFC3339),
				Text:      score.Text,
			})
			if len(evidence) >= 2 {
				break
			}
		}
	}
	if transcript != nil && strings.TrimSpace(transcript.Text) != "" {
		evidence = append(evidence, storage.EvaluationAgentEvidence{ID: "transcript_bucket", Source: "transcript", Text: strings.TrimSpace(transcript.Text)})
	}
	if alignment != nil {
		evidence = append(evidence, storage.EvaluationAgentEvidence{
			ID:     "alignment",
			Source: "alignment",
			Text:   fmt.Sprintf("relationship=%s, delta=%.2f, similarity=%.2f, quality=%.2f", alignment.Relationship, alignment.Delta, alignment.Similarity, alignment.Quality),
		})
	}
	return evidence
}

func agentSubwindowEvidence(bucket chat.ChatBucket, subwindow chat.ReactionSubwindow) []storage.EvaluationAgentEvidence {
	var evidence []storage.EvaluationAgentEvidence
	evidenceIDs := map[string]struct{}{}
	for _, id := range subwindow.EvidenceIDs {
		evidenceIDs[id] = struct{}{}
	}
	for _, message := range bucket.PeakEvidenceMessages {
		if len(evidenceIDs) > 0 {
			if _, ok := evidenceIDs[message.MessageID]; !ok {
				continue
			}
		}
		evidence = append(evidence, storage.EvaluationAgentEvidence{
			ID:        message.MessageID,
			Source:    "chat",
			Timestamp: message.Timestamp.Format(time.RFC3339),
			Text:      message.Text,
		})
	}
	evidence = append(evidence, storage.EvaluationAgentEvidence{
		ID:     "subwindow",
		Source: "reaction",
		Text:   fmt.Sprintf("reaction_score=%.2f, event_hint=%s, target=%s", subwindow.ReactionScore, subwindow.EventHint, subwindow.TargetText),
	})
	return evidence
}

func bestTranscriptForWindow(start, end time.Time, buckets []storage.TranscriptBucket) *storage.TranscriptBucket {
	var best *storage.TranscriptBucket
	var bestOverlap time.Duration
	for index := range buckets {
		bucket := &buckets[index]
		overlap := timeOverlap(start, end, bucket.BucketStart, bucket.BucketEnd)
		if overlap > bestOverlap {
			bestOverlap = overlap
			best = bucket
		}
	}
	return best
}

func bestAlignmentForWindow(start, end time.Time, alignments []storage.AlignmentBucket) *storage.AlignmentBucket {
	var best *storage.AlignmentBucket
	var bestOverlap time.Duration
	for index := range alignments {
		alignment := &alignments[index]
		overlap := timeOverlap(start, end, alignment.WindowStart, alignment.WindowEnd)
		if overlap > bestOverlap {
			bestOverlap = overlap
			best = alignment
		}
	}
	return best
}

func timeOverlap(firstStart, firstEnd, secondStart, secondEnd time.Time) time.Duration {
	if firstStart.IsZero() || firstEnd.IsZero() || secondStart.IsZero() || secondEnd.IsZero() {
		return 0
	}
	start := firstStart
	if secondStart.After(start) {
		start = secondStart
	}
	end := firstEnd
	if secondEnd.Before(end) {
		end = secondEnd
	}
	if !end.After(start) {
		return 0
	}
	return end.Sub(start)
}

func agentReviewID(sessionID, sourceType string, start, end time.Time) string {
	return fmt.Sprintf("auto-prescore:%s:%s:%s:%s", sessionID, sourceType, start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
}

func maxFloat(left, right float64) float64 {
	if right > left {
		return right
	}
	return left
}

func clampFloat(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func storedAnalysisAlignments(items []storage.AlignmentBucket) []analysis.AlignmentBucket {
	out := make([]analysis.AlignmentBucket, 0, len(items))
	for _, item := range items {
		out = append(out, analysis.AlignmentBucket{
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
	return out
}

func storedAnalysisTranscriptBuckets(items []storage.TranscriptBucket) []analysis.TranscriptBucket {
	out := make([]analysis.TranscriptBucket, 0, len(items))
	for _, item := range items {
		out = append(out, analysis.TranscriptBucket{
			Type:                 item.Type,
			SessionID:            item.SessionID,
			ChannelID:            item.ChannelID,
			BucketStart:          item.BucketStart,
			BucketEnd:            item.BucketEnd,
			Text:                 item.Text,
			Language:             item.Language,
			TranscriptConfidence: item.TranscriptConfidence,
			SentimentScore:       item.SentimentScore,
			SentimentConfidence:  item.SentimentConfidence,
			SentimentLabel:       item.SentimentLabel,
			SentimentModel:       item.SentimentModel,
			SentimentStatus:      item.SentimentStatus,
			SentimentLatencyMS:   item.SentimentLatencyMS,
		})
	}
	return out
}

func storedEvaluationLabels(items []storage.SignalWindowLabel) []analysis.EvaluationLabel {
	out := make([]analysis.EvaluationLabel, 0, len(items))
	for _, item := range items {
		out = append(out, analysis.EvaluationLabel{
			SessionID:             item.SessionID,
			WindowStart:           item.WindowStart,
			WindowEnd:             item.WindowEnd,
			PredictedEvent:        item.PredictedEvent,
			PredictedRelationship: item.PredictedRelationship,
			ReactionType:          item.ReactionType,
			TargetType:            item.TargetType,
			TargetText:            item.TargetText,
			DivergenceType:        item.DivergenceType,
			EventStart:            item.EventStart,
			EventPeak:             item.EventPeak,
			Correctness:           item.Correctness,
			EventLabel:            item.EventLabel,
			CreatedAt:             item.CreatedAt,
			UpdatedAt:             item.UpdatedAt,
		})
	}
	return out
}

func isActiveSessionStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "starting", "verifying_live", "ingesting":
		return true
	default:
		return false
	}
}

func (s *server) getLiveStatus(ctx context.Context, channel string) (twitchapi.StreamStatus, error) {
	liveStatusCtx, cancelLiveStatus := context.WithTimeout(ctx, 12*time.Second)
	defer cancelLiveStatus()
	return twitchapi.Client{
		ClientID:       s.cfg.TwitchClientID,
		ClientSecret:   s.cfg.TwitchSecret,
		AppAccessToken: s.cfg.TwitchAppToken,
	}.GetStreamStatus(liveStatusCtx, channel)
}

func (s *server) resolveStreamSession(ctx context.Context, channel string, status twitchapi.StreamStatus) (string, bool) {
	sessionID := sessionIDForStream(channel, status.ID)
	if s.store == nil || strings.TrimSpace(status.ID) == "" {
		s.persistSessionCreated(sessionID, channel, status)
		return sessionID, false
	}

	timeout := s.cfg.DatabaseWriteTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	resolveCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	record, err := s.store.FindSessionByTwitchStream(resolveCtx, channel, status.ID)
	if err == nil && strings.TrimSpace(record.SessionID) != "" {
		return record.SessionID, true
	}
	if err != nil && !storage.IsNotFound(err) {
		s.logger.Warn("find stream session by twitch stream id", "channel", channel, "twitch_stream_id", status.ID, "error", err)
	}

	record = s.sessionRecord(sessionID, channel, status)
	if err := s.store.CreateSession(resolveCtx, record); err != nil {
		s.recordSynchronousPersistenceFailure("create_session", err)
		s.logger.Warn("create stream session", "session_id", sessionID, "channel", channel, "twitch_stream_id", status.ID, "error", err)
	}
	if existing, findErr := s.store.FindSessionByTwitchStream(resolveCtx, channel, status.ID); findErr == nil && strings.TrimSpace(existing.SessionID) != "" {
		return existing.SessionID, existing.SessionID != sessionID
	}
	return sessionID, false
}

func (s *server) sessionRecord(sessionID, channel string, status twitchapi.StreamStatus) storage.SessionRecord {
	now := time.Now().UTC()
	startedAt := status.StartedAt
	if startedAt.IsZero() {
		startedAt = now
	}
	return storage.SessionRecord{
		SessionID:               sessionID,
		ChannelID:               channel,
		TwitchStreamID:          status.ID,
		TwitchUserID:            status.UserID,
		Status:                  "starting",
		StartedAt:               startedAt.UTC(),
		FirstSeenAt:             now,
		LastSeenAt:              now,
		BucketSeconds:           int(s.cfg.BucketEvery.Seconds()),
		TranscriptBucketSeconds: s.transcriptBucketSeconds(),
		TranscriptChunkSeconds:  s.transcriptChunkSeconds(),
		NLPAnalyzerURL:          s.cfg.NLPAnalyzerURL,
		SentimentModel:          envString("SENTIMENT_MODEL", ""),
	}
}

func sessionIDForStream(channel, twitchStreamID string) string {
	streamID := strings.TrimSpace(twitchStreamID)
	if streamID == "" {
		return fmt.Sprintf("%s-%d", channel, time.Now().Unix())
	}
	return fmt.Sprintf("%s-%s", channel, streamID)
}

func ingestionRunID(sessionID string) string {
	return fmt.Sprintf("%s-run-%d", sessionID, time.Now().UnixNano())
}

func (s *server) stopActiveSession() {
	var cancel context.CancelFunc
	var sessionID string
	var channel string
	var runID string
	var stream *streamInfo
	var wasActive bool

	s.mu.Lock()
	if s.cancelActive != nil {
		cancel = s.cancelActive
		s.cancelActive = nil
	}
	sessionID = s.state.Session
	channel = s.state.Channel
	runID = s.activeRunID
	wasActive = cancel != nil || isActiveSessionStatus(s.state.Status)
	if s.state.Stream != nil {
		streamCopy := *s.state.Stream
		stream = &streamCopy
	}
	if wasActive {
		s.state.Status = "stopped"
	}
	s.activeRunID = ""
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if !wasActive || sessionID == "" {
		return
	}
	s.persistSessionStatusSync(sessionID, channel, "stopped", stream, "")
	s.persistIngestionRunStatusSync(runID, sessionID, "stopped", "server shutdown", "")
}

func (s *server) isCurrentRun(runID, sessionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeRunID == runID && s.state.Session == sessionID
}

func (s *server) hasActiveSession() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelActive != nil && isActiveSessionStatus(s.state.Status)
}

func (s *server) clearActiveRunIfCurrent(runID, sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeRunID == runID && s.state.Session == sessionID {
		s.activeRunID = ""
		s.cancelActive = nil
	}
}

type eventHub struct {
	mu      sync.Mutex
	clients map[chan dashboardEvent]struct{}
}

func newEventHub() *eventHub {
	return &eventHub{clients: map[chan dashboardEvent]struct{}{}}
}

func (h *eventHub) add() chan dashboardEvent {
	ch := make(chan dashboardEvent, 128)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *eventHub) remove(ch chan dashboardEvent) {
	h.mu.Lock()
	delete(h.clients, ch)
	close(ch)
	h.mu.Unlock()
}

func (h *eventHub) broadcast(event dashboardEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- event:
		default:
		}
	}
}

func parseStreamSource(input string) (streamSource, error) {
	value := strings.TrimSpace(input)
	if value == "" {
		return streamSource{}, fmt.Errorf("Twitch channel or YouTube URL is required")
	}
	value = normalizeStreamInputURL(value)

	if parsed, err := url.Parse(value); err == nil && parsed.Host != "" {
		host := strings.ToLower(parsed.Host)
		switch {
		case strings.Contains(host, "twitch.tv"):
			channel, err := twitchChannelFromURL(parsed)
			if err != nil {
				return streamSource{}, err
			}
			return streamSource{
				Platform:    "twitch",
				ID:          channel,
				URL:         "https://www.twitch.tv/" + channel,
				Label:       channel,
				StreamID:    channel,
				ChatEnabled: true,
			}, nil
		case isYouTubeHost(host):
			id := youtubeSourceID(parsed)
			if id == "" {
				return streamSource{}, fmt.Errorf("YouTube URL must include a video, live, or channel path")
			}
			return streamSource{
				Platform:    "youtube",
				ID:          "youtube-" + sanitizeSourceID(id),
				URL:         value,
				Label:       youtubeSourceLabel(parsed, id),
				StreamID:    sanitizeSourceID(id),
				ChatEnabled: false,
			}, nil
		default:
			return streamSource{}, fmt.Errorf("only twitch.tv and youtube.com URLs are supported")
		}
	}

	value = strings.TrimPrefix(strings.ToLower(value), "#")
	value = strings.TrimPrefix(value, "@")
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, "/ ") {
		return streamSource{}, fmt.Errorf("invalid Twitch channel")
	}
	return streamSource{
		Platform:    "twitch",
		ID:          value,
		URL:         "https://www.twitch.tv/" + value,
		Label:       value,
		StreamID:    value,
		ChatEnabled: true,
	}, nil
}

func normalizeStreamInputURL(value string) string {
	lower := strings.ToLower(value)
	for _, prefix := range []string{"youtube.com/", "www.youtube.com/", "m.youtube.com/", "youtu.be/", "twitch.tv/", "www.twitch.tv/"} {
		if strings.HasPrefix(lower, prefix) {
			return "https://" + value
		}
	}
	return value
}

func extractChannel(input string) (string, error) {
	source, err := parseStreamSource(input)
	if err != nil {
		return "", err
	}
	return source.ID, nil
}

func twitchChannelFromURL(parsed *url.URL) (string, error) {
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return "", fmt.Errorf("Twitch URL must include a channel name")
	}
	channel := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(parts[0])), "@")
	if channel == "" || strings.ContainsAny(channel, "/ ") {
		return "", fmt.Errorf("invalid Twitch channel")
	}
	return channel, nil
}

func isYouTubeHost(host string) bool {
	host = strings.TrimPrefix(strings.ToLower(host), "www.")
	host = strings.TrimPrefix(host, "m.")
	return host == "youtube.com" || strings.HasSuffix(host, ".youtube.com") || host == "youtu.be" || host == "youtube-nocookie.com"
}

func youtubeSourceID(parsed *url.URL) string {
	host := strings.TrimPrefix(strings.TrimPrefix(strings.ToLower(parsed.Host), "www."), "m.")
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if host == "youtu.be" && len(parts) > 0 {
		return parts[0]
	}
	if id := strings.TrimSpace(parsed.Query().Get("v")); id != "" {
		return id
	}
	for index, part := range parts {
		if (part == "live" || part == "embed" || part == "shorts") && index+1 < len(parts) {
			return parts[index+1]
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, "-")
	}
	return ""
}

func youtubeSourceLabel(parsed *url.URL, id string) string {
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	for _, part := range parts {
		if strings.HasPrefix(part, "@") {
			return strings.TrimPrefix(part, "@")
		}
	}
	return "YouTube " + id
}

func sanitizeSourceID(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastDash = false
		case r == '_' || r == '-':
			builder.WriteRune(r)
			lastDash = r == '-'
		default:
			if !lastDash && builder.Len() > 0 {
				builder.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(builder.String(), "-")
}

func twitchPreviewURL(template string) string {
	value := strings.TrimSpace(template)
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, "{width}", "640")
	value = strings.ReplaceAll(value, "{height}", "360")
	return value
}

func streamInfoFromStatus(status twitchapi.StreamStatus) streamInfo {
	startedAt := ""
	if !status.StartedAt.IsZero() {
		startedAt = status.StartedAt.Format(time.RFC3339)
	}
	url := ""
	if status.UserLogin != "" {
		url = "https://www.twitch.tv/" + strings.ToLower(status.UserLogin)
	}
	return streamInfo{
		ID:           status.ID,
		UserID:       status.UserID,
		Platform:     "twitch",
		URL:          url,
		Title:        status.Title,
		Game:         status.GameName,
		ViewerCount:  status.ViewerCount,
		StartedAt:    startedAt,
		Language:     status.Language,
		ThumbnailURL: twitchPreviewURL(status.ThumbnailURL),
	}
}

func streamStatusFromSource(source streamSource) twitchapi.StreamStatus {
	return twitchapi.StreamStatus{
		Live:      true,
		ID:        source.StreamID,
		UserID:    source.Platform,
		UserLogin: source.ID,
		UserName:  source.Label,
		GameName:  "News",
		Title:     source.Label + " live stream",
		StartedAt: time.Now().UTC(),
		Language:  "en",
	}
}

func streamInfoFromSource(source streamSource) streamInfo {
	startedAt := time.Now().UTC().Format(time.RFC3339)
	return streamInfo{
		ID:        source.StreamID,
		UserID:    source.Platform,
		Platform:  source.Platform,
		URL:       source.URL,
		Title:     source.Label + " live stream",
		Game:      "News",
		StartedAt: startedAt,
		Language:  "en",
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func initStorage(ctx context.Context, cfg *appConfig, logger *slog.Logger) storage.Store {
	if strings.TrimSpace(cfg.ReplayFixturePath) != "" {
		store, err := storage.NewFixtureStoreFromFile(cfg.ReplayFixturePath)
		if err != nil {
			logger.Error("replay fixture storage unavailable", "path", cfg.ReplayFixturePath, "error", err)
			return storage.NewNoopStore()
		}
		logger.Info("replay fixture storage enabled", "path", cfg.ReplayFixturePath)
		return store
	}

	if !cfg.DatabaseWriteEnabled || strings.TrimSpace(cfg.DatabaseURL) == "" {
		logger.Info("persistent storage disabled")
		return storage.NewNoopStore()
	}

	if strings.TrimSpace(cfg.ChatIdentityHashSalt) == "" {
		logger.Warn("CHAT_IDENTITY_HASH_SALT is unset; using a session-local fallback salt for persisted chat identity hashes")
		cfg.ChatIdentityHashSalt = randomFallbackSalt()
	}

	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	store, err := storage.NewPostgresStore(initCtx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("persistent storage unavailable", "error", err)
		return storage.NewNoopStore()
	}
	logger.Info("persistent storage enabled")
	return store
}

func (s *server) persistAsync(operation string, fn func(context.Context) error) {
	if s.store == nil || fn == nil {
		return
	}
	s.ensurePersistenceQueue().Enqueue(operation, fn)
}

func (s *server) ensurePersistenceQueue() *persistenceQueue {
	if s.persistence != nil {
		return s.persistence
	}
	s.persistenceMu.Lock()
	defer s.persistenceMu.Unlock()
	if s.persistence == nil {
		s.persistence = newPersistenceQueue(s.cfg.DatabaseWriteQueueSize, s.cfg.DatabaseWriteTimeout, s.cfg.DatabaseWriteMaxRetries, s.logger)
	}
	return s.persistence
}

func (s *server) persistenceStats() persistenceQueueStats {
	if s.persistence == nil {
		return persistenceQueueStats{}
	}
	return s.persistence.Stats()
}

func (s *server) recordSynchronousPersistenceFailure(operation string, err error) {
	if err == nil {
		return
	}
	s.ensurePersistenceQueue().recordFailure(operation, 1, err)
}

func databaseWriteTimeout(value time.Duration) time.Duration {
	if value <= 0 {
		return 2 * time.Second
	}
	return value
}

func (s *server) persistSessionCreated(sessionID, channel string, status twitchapi.StreamStatus) {
	record := s.sessionRecord(sessionID, channel, status)
	s.persistAsync("create_session", func(ctx context.Context) error {
		return s.store.CreateSession(ctx, record)
	})
}

func (s *server) persistDashboardEvent(event dashboardEvent) {
	if event.Status == "" {
		return
	}
	s.persistSessionStatus(event.Session, event.Channel, event.Status, event.Stream, event.Error)
}

func (s *server) persistSessionStatus(sessionID, channel, status string, stream *streamInfo, errorMessage string) {
	update, ok := sessionStatusUpdate(sessionID, channel, status, stream, errorMessage)
	if !ok {
		return
	}

	s.persistAsync("update_session_status", func(ctx context.Context) error {
		return s.store.UpdateSessionStatus(ctx, update)
	})
}

func (s *server) persistSessionStatusSync(sessionID, channel, status string, stream *streamInfo, errorMessage string) {
	update, ok := sessionStatusUpdate(sessionID, channel, status, stream, errorMessage)
	if !ok || s.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), databaseWriteTimeout(s.cfg.DatabaseWriteTimeout))
	defer cancel()
	if err := s.store.UpdateSessionStatus(ctx, update); err != nil {
		s.recordSynchronousPersistenceFailure("update_session_status_sync", err)
		if s.logger != nil {
			s.logger.Warn("persistent storage write failed", "operation", "update_session_status_sync", "error", err)
		}
	}
}

func sessionStatusUpdate(sessionID, channel, status string, stream *streamInfo, errorMessage string) (storage.SessionStatusUpdate, bool) {
	if sessionID == "" {
		return storage.SessionStatusUpdate{}, false
	}

	update := storage.SessionStatusUpdate{
		SessionID: sessionID,
		ChannelID: channel,
		Status:    status,
		Error:     errorMessage,
	}
	if stream != nil {
		update.TwitchStreamID = stream.ID
		update.TwitchUserID = stream.UserID
		update.StreamTitle = stream.Title
		update.StreamGame = stream.Game
		update.StreamViewerCount = &stream.ViewerCount
		update.StreamLanguage = stream.Language
		if parsed, err := time.Parse(time.RFC3339, stream.StartedAt); err == nil {
			update.StreamStartedAt = &parsed
		}
	}
	if status == "stopped" || status == "ended" || status == "error" || status == "offline" {
		now := time.Now().UTC()
		update.EndedAt = &now
	}
	return update, true
}

func (s *server) persistIngestionRunStarted(runID, sessionID string) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(sessionID) == "" {
		return
	}
	record := storage.IngestionRunRecord{
		RunID:     runID,
		SessionID: sessionID,
		StartedAt: time.Now().UTC(),
		Status:    "ingesting",
	}
	s.persistAsync("create_ingestion_run", func(ctx context.Context) error {
		return s.store.SaveIngestionRun(ctx, record)
	})
}

func (s *server) persistIngestionRunStatus(runID, sessionID, status, stopReason, errorMessage string) {
	update, ok := ingestionRunStatusUpdate(runID, sessionID, status, stopReason, errorMessage)
	if !ok {
		return
	}
	s.persistAsync("update_ingestion_run", func(ctx context.Context) error {
		return s.store.UpdateIngestionRun(ctx, update)
	})
}

func (s *server) persistIngestionRunStatusSync(runID, sessionID, status, stopReason, errorMessage string) {
	update, ok := ingestionRunStatusUpdate(runID, sessionID, status, stopReason, errorMessage)
	if !ok || s.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), databaseWriteTimeout(s.cfg.DatabaseWriteTimeout))
	defer cancel()
	if err := s.store.UpdateIngestionRun(ctx, update); err != nil {
		s.recordSynchronousPersistenceFailure("update_ingestion_run_sync", err)
		if s.logger != nil {
			s.logger.Warn("persistent storage write failed", "operation", "update_ingestion_run_sync", "error", err)
		}
	}
}

func ingestionRunStatusUpdate(runID, sessionID, status, stopReason, errorMessage string) (storage.IngestionRunUpdate, bool) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(sessionID) == "" {
		return storage.IngestionRunUpdate{}, false
	}
	now := time.Now().UTC()
	return storage.IngestionRunUpdate{
		RunID:      runID,
		SessionID:  sessionID,
		EndedAt:    &now,
		Status:     status,
		StopReason: stopReason,
		Error:      errorMessage,
	}, true
}

func (s *server) persistChatBucket(bucket chat.ChatBucket) {
	samples := makeChatMessageSamples(bucket, s.cfg.ChatSampleLimitPerBucket, s.cfg.ChatIdentityHashSalt)
	s.persistAsync("upsert_chat_bucket", func(ctx context.Context) error {
		if err := s.store.SaveChatBucket(ctx, bucket); err != nil {
			return err
		}
		for _, sample := range samples {
			if err := s.store.SaveChatMessageSample(ctx, sample); err != nil {
				return err
			}
		}
		return nil
	})
}

func makeChatMessageSamples(bucket chat.ChatBucket, limit int, salt string) []storage.ChatMessageSample {
	if limit <= 0 {
		return nil
	}

	samples := make([]storage.ChatMessageSample, 0, limit)
	seen := map[string]struct{}{}
	for _, message := range bucket.PeakEvidenceMessages {
		if len(samples) >= limit {
			return samples
		}
		if message.MessageID == "" {
			continue
		}
		if _, ok := seen[message.MessageID]; ok {
			continue
		}
		seen[message.MessageID] = struct{}{}
		samples = append(samples, storage.ChatMessageSample{
			SessionID:    bucket.SessionID,
			ChannelID:    bucket.ChannelID,
			BucketStart:  bucket.BucketStart,
			BucketEnd:    bucket.BucketEnd,
			MessageID:    message.MessageID,
			Timestamp:    message.Timestamp,
			UserHash:     storage.HashIdentity(salt, firstNonEmpty(message.Username, message.DisplayName)),
			Text:         message.Text,
			EvidenceRank: len(samples),
		})
	}
	for _, score := range bucket.MessageScores {
		if len(samples) >= limit {
			return samples
		}
		if score.MessageID != "" {
			if _, ok := seen[score.MessageID]; ok {
				continue
			}
			seen[score.MessageID] = struct{}{}
		}
		samples = append(samples, storage.ChatMessageSample{
			SessionID:      bucket.SessionID,
			ChannelID:      bucket.ChannelID,
			BucketStart:    bucket.BucketStart,
			BucketEnd:      bucket.BucketEnd,
			MessageID:      score.MessageID,
			Timestamp:      score.Timestamp,
			UserHash:       storage.HashIdentity(salt, firstNonEmpty(score.Username, score.DisplayName)),
			Text:           score.Text,
			Label:          score.Label,
			Confidence:     score.Confidence,
			SentimentScore: score.SentimentScore,
			HumanLabel:     score.HumanLabel,
			EvidenceRank:   len(samples),
		})
	}
	return samples
}

func (s *server) persistTranscriptBucket(bucket transcriptBucket) {
	record := storageTranscriptBucket(bucket)

	s.persistAsync("upsert_transcript_bucket", func(ctx context.Context) error {
		return s.store.SaveTranscriptBucket(ctx, record)
	})
}

func storageTranscriptBucket(bucket transcriptBucket) storage.TranscriptBucket {
	return storage.TranscriptBucket{
		Type:                 bucket.Type,
		SessionID:            bucket.SessionID,
		ChannelID:            bucket.ChannelID,
		BucketStart:          bucket.BucketStart,
		BucketEnd:            bucket.BucketEnd,
		AudioStartedAt:       bucket.AudioStartedAt,
		AudioEndedAt:         bucket.AudioEndedAt,
		TranscribedAt:        bucket.TranscribedAt,
		Text:                 bucket.Text,
		Language:             bucket.Language,
		TranscriptConfidence: bucket.TranscriptConfidence,
		TranscriptStatus:     bucket.TranscriptStatus,
		SentimentScore:       bucket.SentimentScore,
		SentimentConfidence:  bucket.SentimentConfidence,
		SentimentLabel:       bucket.SentimentLabel,
		SentimentModel:       bucket.SentimentModel,
		SentimentStatus:      bucket.SentimentStatus,
		SentimentLatencyMS:   bucket.SentimentLatencyMS,
		ASRLatencyMS:         bucket.ASRLatencyMS,
		PipelineLatencyMS:    bucket.PipelineLatencyMS,
		AudioSeconds:         bucket.AudioSeconds,
		SegmentCount:         bucket.SegmentCount,
		WordCount:            bucket.WordCount,
		EmptyRatio:           bucket.EmptyRatio,
		RepairAddedWords:     bucket.RepairAddedWords,
		RepairChangedRatio:   bucket.RepairChangedRatio,
		Segments:             storageTranscriptSegments(bucket.Segments),
		Quality:              bucket.Quality,
	}
}

func (s *server) persistAlignments(alignments []alignmentBucket) {
	if len(alignments) == 0 {
		return
	}

	rows := make([]storage.AlignmentBucket, 0, len(alignments))
	for _, alignment := range alignments {
		rows = append(rows, storage.AlignmentBucket{
			Type:                  alignment.Type,
			SessionID:             alignment.SessionID,
			ChannelID:             alignment.ChannelID,
			WindowStart:           alignment.WindowStart,
			WindowEnd:             alignment.WindowEnd,
			ChatBucketStart:       alignment.ChatBucketStart,
			ChatBucketEnd:         alignment.ChatBucketEnd,
			TranscriptBucketStart: alignment.TranscriptBucketStart,
			TranscriptBucketEnd:   alignment.TranscriptBucketEnd,
			ChatSentiment:         alignment.ChatSentiment,
			ChatConfidence:        alignment.ChatConfidence,
			ChatMessageCount:      alignment.ChatMessageCount,
			TranscriptSentiment:   alignment.TranscriptSentiment,
			TranscriptConfidence:  alignment.TranscriptConfidence,
			TranscriptTextLength:  alignment.TranscriptTextLength,
			Delta:                 alignment.Delta,
			Similarity:            alignment.Similarity,
			Relationship:          alignment.Relationship,
			OverlapSeconds:        alignment.OverlapSeconds,
			Quality:               alignment.Quality,
			QualityFlags:          alignment.QualityFlags,
		})
	}

	s.persistAsync("upsert_alignment_buckets", func(ctx context.Context) error {
		for _, row := range rows {
			if err := s.store.SaveAlignment(ctx, row); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *server) persistHumanLabel(req labelRequest) {
	record := storage.HumanLabel{
		SessionID: req.SessionID,
		MessageID: req.MessageID,
		Label:     req.Label,
		CreatedAt: time.Now().UTC(),
	}
	s.persistAsync("upsert_human_label", func(ctx context.Context) error {
		return s.store.SaveHumanLabel(ctx, record)
	})
}

func (s *server) persistSignalWindowLabel(record storage.SignalWindowLabel) {
	s.persistAsync("upsert_signal_window_label", func(ctx context.Context) error {
		return s.store.SaveSignalWindowLabel(ctx, record)
	})
}

func (s *server) persistReplayProofMetrics(ctx context.Context, proof storage.ReplayProof) error {
	if s.store == nil {
		return nil
	}
	if proof.Partial {
		return errors.New("partial replay proof cannot be persisted")
	}
	for _, metric := range replayProofMetrics(proof) {
		if err := s.store.SaveMetric(ctx, metric); err != nil {
			return err
		}
	}
	return nil
}

func replayProofMetrics(proof storage.ReplayProof) []storage.SystemMetric {
	recordedAt := proof.GeneratedAt
	if recordedAt.IsZero() {
		recordedAt = time.Now().UTC()
	}
	baseMeta := replayProofMetricBaseMeta(proof)

	var metrics []storage.SystemMetric
	appendMetric := func(name string, value float64, unit string, extra map[string]any) {
		metrics = append(metrics, storage.SystemMetric{
			SessionID:  proof.SessionID,
			Name:       name,
			Value:      value,
			Unit:       unit,
			RecordedAt: recordedAt,
			Meta:       replayProofMetricMeta(baseMeta, extra),
		})
	}

	appendMetric("replay_proof.bucket_count", float64(proof.BucketCount), "buckets", nil)
	appendMetric("replay_proof.source_bucket_count", float64(proof.SourceBucketCount), "buckets", nil)
	appendMetric("replay_proof.signal_window_count", float64(proof.SignalWindowCount), "windows", nil)
	appendMetric("replay_proof.matched_windows", float64(proof.MatchedWindows), "windows", nil)
	if proof.LabelCoverage.Coverage != nil {
		appendMetric("replay_proof.label_coverage", *proof.LabelCoverage.Coverage, "ratio", map[string]any{
			"labeled_windows":  proof.LabelCoverage.LabeledWindows,
			"total_windows":    proof.LabelCoverage.TotalWindows,
			"unmatched_labels": proof.LabelCoverage.UnmatchedLabels,
		})
	}
	if proof.TranscriptCoverage.AudioCoverage != nil {
		appendMetric("replay_proof.transcript_audio_coverage", *proof.TranscriptCoverage.AudioCoverage, "ratio", map[string]any{
			"audio_seconds":            proof.TranscriptCoverage.AudioSeconds,
			"expected_audio_seconds":   proof.TranscriptCoverage.ExpectedAudioSeconds,
			"transcript_status_counts": proof.TranscriptCoverage.StatusCounts,
		})
	}
	if proof.TranscriptCoverage.EmptyRatio != nil {
		appendMetric("replay_proof.transcript_empty_ratio", *proof.TranscriptCoverage.EmptyRatio, "ratio", map[string]any{
			"bucket_count":             proof.TranscriptCoverage.BucketCount,
			"transcript_status_counts": proof.TranscriptCoverage.StatusCounts,
		})
	}
	appendMetric("replay_proof.transcript_word_count", float64(proof.TranscriptCoverage.WordCount), "words", map[string]any{
		"bucket_count":             proof.TranscriptCoverage.BucketCount,
		"segment_count":            proof.TranscriptCoverage.SegmentCount,
		"transcript_status_counts": proof.TranscriptCoverage.StatusCounts,
	})
	appendMetric("replay_proof.transcript_repair_added_words", float64(proof.TranscriptCoverage.RepairAddedWords), "words", map[string]any{
		"bucket_count":             proof.TranscriptCoverage.BucketCount,
		"transcript_status_counts": proof.TranscriptCoverage.StatusCounts,
	})
	if proof.TranscriptCoverage.AverageRepairChangedRatio != nil {
		appendMetric("replay_proof.transcript_repair_changed_ratio", *proof.TranscriptCoverage.AverageRepairChangedRatio, "ratio", map[string]any{
			"repair_added_words":       proof.TranscriptCoverage.RepairAddedWords,
			"transcript_status_counts": proof.TranscriptCoverage.StatusCounts,
		})
	}
	if proof.TranscriptCoverage.RepairImprovement != nil {
		appendMetric("replay_proof.transcript_repair_improvement", *proof.TranscriptCoverage.RepairImprovement, "ratio", map[string]any{
			"repair_added_words":                   proof.TranscriptCoverage.RepairAddedWords,
			"average_repair_changed_ratio":         proof.TranscriptCoverage.AverageRepairChangedRatio,
			"transcript_status_counts":             proof.TranscriptCoverage.StatusCounts,
			"transcript_word_count_after_repair":   proof.TranscriptCoverage.WordCount,
			"transcript_bucket_count_after_repair": proof.TranscriptCoverage.BucketCount,
		})
	}
	if proof.Latency.ChatAnalysisLatencyMS.Average != nil {
		appendMetric("replay_proof.chat_analysis_latency_ms", *proof.Latency.ChatAnalysisLatencyMS.Average, "ms", map[string]any{
			"available_count": proof.Latency.ChatAnalysisLatencyMS.AvailableCount,
			"missing_count":   proof.Latency.ChatAnalysisLatencyMS.MissingCount,
			"min":             proof.Latency.ChatAnalysisLatencyMS.Min,
			"max":             proof.Latency.ChatAnalysisLatencyMS.Max,
			"p50":             proof.Latency.ChatAnalysisLatencyMS.P50,
			"p95":             proof.Latency.ChatAnalysisLatencyMS.P95,
		})
	}
	if proof.Latency.TranscriptSentimentLatencyMS.Average != nil {
		appendMetric("replay_proof.transcript_sentiment_latency_ms", *proof.Latency.TranscriptSentimentLatencyMS.Average, "ms", map[string]any{
			"available_count": proof.Latency.TranscriptSentimentLatencyMS.AvailableCount,
			"missing_count":   proof.Latency.TranscriptSentimentLatencyMS.MissingCount,
			"min":             proof.Latency.TranscriptSentimentLatencyMS.Min,
			"max":             proof.Latency.TranscriptSentimentLatencyMS.Max,
			"p50":             proof.Latency.TranscriptSentimentLatencyMS.P50,
			"p95":             proof.Latency.TranscriptSentimentLatencyMS.P95,
		})
	}
	if proof.Latency.TranscriptASRLatencyMS.Average != nil {
		appendMetric("replay_proof.transcript_asr_latency_ms", *proof.Latency.TranscriptASRLatencyMS.Average, "ms", map[string]any{
			"available_count": proof.Latency.TranscriptASRLatencyMS.AvailableCount,
			"missing_count":   proof.Latency.TranscriptASRLatencyMS.MissingCount,
			"min":             proof.Latency.TranscriptASRLatencyMS.Min,
			"max":             proof.Latency.TranscriptASRLatencyMS.Max,
			"p50":             proof.Latency.TranscriptASRLatencyMS.P50,
			"p95":             proof.Latency.TranscriptASRLatencyMS.P95,
		})
	}
	if proof.Latency.TranscriptPipelineLatencyMS.Average != nil {
		appendMetric("replay_proof.transcript_pipeline_latency_ms", *proof.Latency.TranscriptPipelineLatencyMS.Average, "ms", map[string]any{
			"available_count": proof.Latency.TranscriptPipelineLatencyMS.AvailableCount,
			"missing_count":   proof.Latency.TranscriptPipelineLatencyMS.MissingCount,
			"min":             proof.Latency.TranscriptPipelineLatencyMS.Min,
			"max":             proof.Latency.TranscriptPipelineLatencyMS.Max,
			"p50":             proof.Latency.TranscriptPipelineLatencyMS.P50,
			"p95":             proof.Latency.TranscriptPipelineLatencyMS.P95,
		})
	}
	for _, speed := range proof.Speeds {
		extra := map[string]any{"speed": speed.Speed}
		appendMetric("replay_proof.estimated_replay_seconds", speed.EstimatedReplaySeconds, "seconds", extra)
		appendMetric("replay_proof.windows_per_second", speed.WindowsPerSecond, "windows/s", extra)
	}
	return metrics
}

func replayProofMetricBaseMeta(proof storage.ReplayProof) map[string]any {
	return map[string]any{
		"type":                               proof.Type,
		"channel_id":                         proof.ChannelID,
		"generated_at":                       proof.GeneratedAt.Format(time.RFC3339Nano),
		"replay_limit":                       proof.ReplayLimit,
		"partial":                            proof.Partial,
		"session_totals":                     proof.SessionTotals,
		"truncated_sources":                  proof.TruncatedSources,
		"chat_bucket_count":                  proof.ChatBucketCount,
		"transcript_bucket_count":            proof.TranscriptBucketCount,
		"alignment_count":                    proof.AlignmentCount,
		"source_duration_ms":                 proof.Timeline.SourceDurationMS,
		"dropped_event_rate_supported":       proof.DroppedEventRate != nil,
		"unsupported_metrics":                proof.UnsupportedMetrics,
		"chat_analysis_status_counts":        proof.Latency.ChatAnalysisStatusCounts,
		"transcript_sentiment_status_counts": proof.Latency.TranscriptSentimentStatusCounts,
		"transcript_coverage_status_counts":  proof.TranscriptCoverage.StatusCounts,
	}
}

func replayProofMetricMeta(base map[string]any, extra map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(extra))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range extra {
		out[key] = value
	}
	return out
}

func signalWindowLabelFromRequest(req signalWindowLabelRequest) (storage.SignalWindowLabel, error) {
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		return storage.SignalWindowLabel{}, errors.New("session_id is required")
	}
	windowStart, err := time.Parse(time.RFC3339, strings.TrimSpace(req.WindowStart))
	if err != nil {
		return storage.SignalWindowLabel{}, errors.New("window_start must be RFC3339")
	}
	windowEnd, err := time.Parse(time.RFC3339, strings.TrimSpace(req.WindowEnd))
	if err != nil {
		return storage.SignalWindowLabel{}, errors.New("window_end must be RFC3339")
	}
	if !windowEnd.After(windowStart) {
		return storage.SignalWindowLabel{}, errors.New("window_end must be after window_start")
	}
	correctness := strings.ToLower(strings.TrimSpace(req.Correctness))
	if correctness != "correct" && correctness != "wrong" && correctness != "uncertain" {
		return storage.SignalWindowLabel{}, errors.New("correctness must be correct, wrong, or uncertain")
	}
	eventLabel := strings.ToLower(strings.TrimSpace(req.EventLabel))
	if eventLabel == "" {
		return storage.SignalWindowLabel{}, errors.New("event_label is required")
	}
	if !validWindowEventLabel(eventLabel) {
		return storage.SignalWindowLabel{}, errors.New("event_label must be hype_spike, frustration_spike, audience_shift, content_audience_divergence, or none")
	}
	eventStart, err := optionalRFC3339Time(req.EventStart, "event_start")
	if err != nil {
		return storage.SignalWindowLabel{}, err
	}
	eventPeak, err := optionalRFC3339Time(req.EventPeak, "event_peak")
	if err != nil {
		return storage.SignalWindowLabel{}, err
	}
	return storage.SignalWindowLabel{
		SessionID:             sessionID,
		WindowStart:           windowStart,
		WindowEnd:             windowEnd,
		PredictedEvent:        strings.ToLower(strings.TrimSpace(req.PredictedEvent)),
		PredictedRelationship: strings.ToLower(strings.TrimSpace(req.PredictedRelationship)),
		ReactionType:          strings.ToLower(strings.TrimSpace(req.ReactionType)),
		TargetType:            strings.ToLower(strings.TrimSpace(req.TargetType)),
		TargetText:            strings.TrimSpace(req.TargetText),
		DivergenceType:        strings.ToLower(strings.TrimSpace(req.DivergenceType)),
		EventStart:            eventStart,
		EventPeak:             eventPeak,
		Correctness:           correctness,
		EventLabel:            eventLabel,
		Notes:                 strings.TrimSpace(req.Notes),
		CreatedAt:             time.Now().UTC(),
	}, nil
}

func optionalRFC3339Time(raw, field string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339", field)
	}
	return value, nil
}

func validWindowEventLabel(label string) bool {
	switch label {
	case "hype_spike", "frustration_spike", "audience_shift", "content_audience_divergence", "none":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func randomFallbackSalt() string {
	return fmt.Sprintf("dashboard-%d", time.Now().UnixNano())
}

func storageTranscriptSegments(segments []transcriptSegment) []storage.TranscriptSegment {
	out := make([]storage.TranscriptSegment, 0, len(segments))
	for _, segment := range segments {
		out = append(out, storage.TranscriptSegment{
			Start:      segment.Start,
			End:        segment.End,
			Text:       segment.Text,
			Confidence: segment.Confidence,
			Words:      storageTranscriptWords(segment.Words),
		})
	}
	return out
}

func storageTranscriptWords(words []transcriptWord) []storage.TranscriptWord {
	out := make([]storage.TranscriptWord, 0, len(words))
	for _, word := range words {
		out = append(out, storage.TranscriptWord{
			Start:       word.Start,
			End:         word.End,
			Text:        word.Text,
			Probability: word.Probability,
		})
	}
	return out
}

func staticHandler(frontendDir string) http.Handler {
	files := http.FileServer(http.Dir(frontendDir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			files.ServeHTTP(w, r)
			return
		}
		cleanPath := path.Clean("/" + strings.TrimPrefix(r.URL.Path, "/"))
		if cleanPath != "/" {
			requested := filepath.Join(frontendDir, filepath.FromSlash(strings.TrimPrefix(cleanPath, "/")))
			if info, err := os.Stat(requested); err == nil && !info.IsDir() {
				files.ServeHTTP(w, r)
				return
			}
			if strings.Contains(path.Base(r.URL.Path), ".") {
				files.ServeHTTP(w, r)
				return
			}
		}
		if !isFrontendRoute(cleanPath) {
			http.NotFound(w, r)
			return
		}
		indexPath := filepath.Join(frontendDir, "index.html")
		if _, err := os.Stat(indexPath); err == nil {
			http.ServeFile(w, r, indexPath)
			return
		}
		files.ServeHTTP(w, r)
	})
}

func isFrontendRoute(cleanPath string) bool {
	switch cleanPath {
	case "/", "/eval":
		return true
	default:
		return false
	}
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func defaultFrontendDir() string {
	candidates := []string{
		"../../apps/dashboard/dist",
		"apps/dashboard/dist",
		"../../../apps/dashboard/dist",
		"../../apps/dashboard",
		"apps/dashboard",
		"../../../apps/dashboard",
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate
		}
	}
	return "../../apps/dashboard/dist"
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return envIntFromString(value, fallback)
}

func envIntFromString(value string, fallback int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
