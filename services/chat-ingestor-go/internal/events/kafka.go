package events

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConfig struct {
	Brokers      []string
	ClientID     string
	WriteTimeout time.Duration
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(cfg KafkaConfig) (*KafkaPublisher, error) {
	brokers := cleanBrokers(cfg.Brokers)
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = 5 * time.Second
	}
	clientID := strings.TrimSpace(cfg.ClientID)
	if clientID == "" {
		clientID = "stream-event-gateway"
	}
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			AllowAutoTopicCreation: false,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireOne,
			Transport: &kafka.Transport{
				ClientID: clientID,
			},
			WriteTimeout: writeTimeout,
		},
	}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, msg PublishMessage) error {
	headers := make([]kafka.Header, 0, len(msg.Headers))
	for key, value := range msg.Headers {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   msg.Topic,
		Key:     []byte(msg.Key),
		Value:   msg.Value,
		Time:    time.Now().UTC(),
		Headers: headers,
	})
}

func (p *KafkaPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func cleanBrokers(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			if broker := strings.TrimSpace(part); broker != "" {
				out = append(out, broker)
			}
		}
	}
	return out
}
