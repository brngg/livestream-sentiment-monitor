package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/config"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/events"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	_ = config.LoadDotEnv(".env")

	cfg := appConfig{}
	var kafkaBrokers string
	var eventBusEnabled bool
	flag.StringVar(&cfg.Addr, "addr", envString("ANALYSIS_ADDR", ":8094"), "analysis service HTTP address")
	flag.DurationVar(&cfg.AlignmentWindow, "alignment-window", envDuration("ANALYSIS_ALIGNMENT_WINDOW", defaultAlignmentWindow), "bucket overlap window used for analysis joins")
	flag.DurationVar(&cfg.StorageTimeout, "storage-timeout", envDuration("ANALYSIS_STORAGE_TIMEOUT", defaultStorageTimeout), "timeout for storage writes")
	flag.IntVar(&cfg.MaxBucketsPerSession, "max-buckets-per-session", envInt("ANALYSIS_MAX_BUCKETS_PER_SESSION", defaultMaxSessionItems), "maximum chat/transcript buckets retained per session")
	flag.StringVar(&cfg.DatabaseURL, "database-url", envString("DATABASE_URL", ""), "optional Postgres database URL")
	flag.StringVar(&kafkaBrokers, "kafka-brokers", envString("KAFKA_BROKERS", "127.0.0.1:19092"), "comma-separated Kafka/Redpanda brokers")
	flag.BoolVar(&eventBusEnabled, "event-bus-enabled", envBool("EVENT_BUS_ENABLED", true), "consume bucket events from Kafka/Redpanda")
	flag.Parse()

	store, storageStatus := initAnalysisStore(context.Background(), cfg.DatabaseURL, logger)
	if closeable, ok := store.(closeableStore); ok {
		defer closeable.Close()
	}
	s := newServer(cfg, store, storageStatus, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if eventBusEnabled {
		resultPublisher, err := events.NewKafkaPublisher(events.KafkaConfig{
			Brokers:  splitList(kafkaBrokers),
			ClientID: "stream-analysis-service",
		})
		if err != nil {
			logger.Warn("analysis result publisher disabled", "error", err)
		} else {
			s.resultPublisher = resultPublisher
			defer resultPublisher.Close()
		}
		startKafkaConsumers(ctx, s, splitList(kafkaBrokers), logger)
	}

	httpServer := &http.Server{Addr: cfg.Addr, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	logger.Info("analysis service listening", "addr", cfg.Addr, "event_bus_enabled", eventBusEnabled, "storage", storageStatus)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("analysis service stopped", "error", err)
		os.Exit(1)
	}
}

func initAnalysisStore(ctx context.Context, databaseURL string, logger *slog.Logger) (alignmentStore, string) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, "disabled"
	}
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	store, err := storage.NewPostgresStore(initCtx, databaseURL)
	if err != nil {
		if logger != nil {
			logger.Warn("analysis persistent storage unavailable", "error", err)
		}
		return nil, "unavailable"
	}
	return store, "enabled"
}

func startKafkaConsumers(ctx context.Context, s *server, brokers []string, logger *slog.Logger) {
	if len(brokers) == 0 {
		return
	}
	for _, topic := range []string{events.TopicChatBuckets, events.TopicTranscriptBuckets} {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: "stream-analysis-service",
		})
		go func(topic string, reader *kafka.Reader) {
			defer reader.Close()
			for {
				message, err := reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					s.metrics.kafkaErrors.Add(1)
					if logger != nil {
						logger.Warn("consume event", "topic", topic, "error", err)
					}
					continue
				}
				if _, err := s.ingestEvent(ctx, message.Value); err != nil {
					s.metrics.kafkaErrors.Add(1)
					if logger != nil {
						logger.Warn("apply consumed event", "topic", topic, "error", err)
					}
				}
			}
		}(topic, reader)
	}
}

func envString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	var parsed int
	if parsedValue, err := strconv.Atoi(value); err == nil {
		parsed = parsedValue
		return parsed
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func splitList(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
