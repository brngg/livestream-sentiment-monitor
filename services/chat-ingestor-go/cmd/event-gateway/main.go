package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/config"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/events"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := config.LoadDotEnv(".env"); err != nil {
		logger.Error("load .env", "error", err)
		os.Exit(1)
	}

	var (
		addr           = flag.String("addr", envString("EVENT_GATEWAY_ADDR", ":8093"), "HTTP listen address")
		kafkaBrokers   = flag.String("kafka-brokers", envString("EVENT_GATEWAY_KAFKA_BROKERS", "127.0.0.1:9092"), "comma-separated Kafka/Redpanda brokers")
		kafkaClientID  = flag.String("kafka-client-id", envString("EVENT_GATEWAY_KAFKA_CLIENT_ID", "stream-event-gateway"), "Kafka client id")
		publishTimeout = flag.Duration("publish-timeout", envDuration("EVENT_GATEWAY_PUBLISH_TIMEOUT", 5*time.Second), "Kafka publish timeout")
		shutdownWait   = flag.Duration("shutdown-timeout", envDuration("EVENT_GATEWAY_SHUTDOWN_TIMEOUT", 5*time.Second), "graceful shutdown timeout")
	)
	flag.Parse()

	publisher, err := events.NewKafkaPublisher(events.KafkaConfig{
		Brokers:      strings.Split(*kafkaBrokers, ","),
		ClientID:     *kafkaClientID,
		WriteTimeout: *publishTimeout,
	})
	if err != nil {
		logger.Error("configure Kafka publisher", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	gateway := newGatewayServer(publisher, logger, *publishTimeout)
	server := &http.Server{
		Addr:              *addr,
		Handler:           gateway.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownWait)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("shutdown event gateway", "error", err)
		}
	}()

	logger.Info("starting event gateway", "addr", *addr, "kafka_brokers", cleanLogValue(*kafkaBrokers))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("event gateway stopped", "error", err)
		os.Exit(1)
	}
}

func envString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
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

func cleanLogValue(value string) string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return strings.Join(out, ",")
}
