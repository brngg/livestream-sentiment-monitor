package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/bucket"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/config"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/filter"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/publisher"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/reaction"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/sentiment"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/twitchapi"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := config.LoadDotEnv(".env"); err != nil {
		logger.Error("load .env", "error", err)
		os.Exit(1)
	}

	var (
		readerMode   = flag.String("reader", envString("CHAT_READER", "fake"), "chat reader to use: fake or twitch")
		sessionID    = flag.String("session", "local-session", "session id attached to emitted buckets")
		channelID    = flag.String("channel", "local-channel", "channel id attached to emitted buckets")
		twitchNick   = flag.String("twitch-nick", envString("TWITCH_IRC_NICK", ""), "Twitch IRC nickname; defaults to anonymous read-only nick")
		twitchToken  = flag.String("twitch-oauth", envString("TWITCH_IRC_OAUTH_TOKEN", ""), "Twitch IRC OAuth token; may include or omit oauth: prefix")
		twitchAddr   = flag.String("twitch-addr", envString("TWITCH_IRC_ADDR", chat.DefaultTwitchIRCAddr), "Twitch IRC address")
		liveOnly     = flag.Bool("live-only", envBool("TWITCH_LIVE_ONLY", true), "verify the Twitch channel is live before reading chat")
		twitchAPIID  = flag.String("twitch-client-id", envString("TWITCH_CLIENT_ID", ""), "Twitch API client ID for live status checks")
		twitchSecret = flag.String(
			"twitch-client-secret",
			envString("TWITCH_CLIENT_SECRET", ""),
			"Twitch API client secret for app access token generation",
		)
		twitchAppToken = flag.String("twitch-app-access-token", envString("TWITCH_APP_ACCESS_TOKEN", ""), "Twitch app access token for live status checks")
		fakeEvery      = flag.Duration("fake-every", 2*time.Second, "delay between fake chat messages")
		bucketEvery    = flag.Duration("bucket-every", bucket.DefaultWindow, "bucket window duration")
		httpURL        = flag.String("http-publisher", "", "optional HTTP endpoint for bucket POSTs")
		logMessages    = flag.Bool("log-messages", false, "log accepted chat messages before bucketization")
		runFor         = flag.Duration("run-for", 0, "optional local run duration; 0 runs until interrupted")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *runFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *runFor)
		defer cancel()
	}

	if strings.EqualFold(strings.TrimSpace(*readerMode), "twitch") && *liveOnly {
		status, err := twitchapi.Client{
			ClientID:       *twitchAPIID,
			ClientSecret:   *twitchSecret,
			AppAccessToken: *twitchAppToken,
		}.GetStreamStatus(ctx, *channelID)
		if err != nil {
			logger.Error("pre-verify live channel", "channel_id", *channelID, "error", err)
			os.Exit(1)
		}
		if !status.Live {
			logger.Error("channel is not live", "channel_id", *channelID)
			os.Exit(1)
		}
		logger.Info(
			"verified live stream",
			"channel_id", status.UserLogin,
			"display_name", status.UserName,
			"stream_id", status.ID,
			"title", status.Title,
			"game", status.GameName,
			"viewer_count", status.ViewerCount,
			"started_at", status.StartedAt.Format(time.RFC3339),
			"language", status.Language,
		)
	}

	reader, err := buildReader(*readerMode, *sessionID, *channelID, *fakeEvery, *twitchNick, *twitchToken, *twitchAddr)
	if err != nil {
		logger.Error("configure reader", "error", err)
		os.Exit(1)
	}
	analyzer := sentiment.NewLexiconAnalyzer()
	messageFilter := filter.MessageFilter{}
	bucketizer := bucket.NewStreamBucketizer(*bucketEvery)
	reactionAnalyzer := reaction.NewAnalyzer(reaction.DefaultWindow, reaction.DefaultRetention)
	publishers := []publisher.Publisher{publisher.LogPublisher{Logger: logger}}
	if *httpURL != "" {
		publishers = append(publishers, publisher.HTTPPublisher{Endpoint: *httpURL})
	}

	logger.Info("starting chat ingestor", "reader", *readerMode, "channel_id", *channelID, "bucket_every", bucketEvery.String())
	if err := run(ctx, reader, messageFilter, analyzer, bucketizer, reactionAnalyzer, *sessionID, *channelID, publishers, logger, *logMessages); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.Error("chat ingestor stopped", "error", err)
		os.Exit(1)
	}
}

func buildReader(mode, sessionID, channelID string, fakeEvery time.Duration, twitchNick, twitchToken, twitchAddr string) (chat.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "fake":
		return chat.FakeReader{
			SessionID: sessionID,
			ChannelID: channelID,
			Interval:  fakeEvery,
		}, nil
	case "twitch":
		return chat.TwitchReader{
			SessionID:  sessionID,
			ChannelID:  channelID,
			Nick:       twitchNick,
			OAuthToken: twitchToken,
			Addr:       twitchAddr,
		}, nil
	default:
		return nil, errors.New("reader must be fake or twitch")
	}
}

func run(
	ctx context.Context,
	reader chat.Reader,
	messageFilter filter.MessageFilter,
	analyzer sentiment.Analyzer,
	bucketizer *bucket.StreamBucketizer,
	reactionAnalyzer *reaction.Analyzer,
	sessionID string,
	channelID string,
	publishers []publisher.Publisher,
	logger *slog.Logger,
	logMessages bool,
) error {
	messages, errs := reader.Read(ctx)
	reactionTicker := time.NewTicker(time.Second)
	defer reactionTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := publishOnShutdown(detailedBucketsWithPeaks(bucketizer.FlushDetailed(), reactionAnalyzer), publishers); err != nil {
				return err
			}
			return ctx.Err()
		case tick := <-reactionTicker.C:
			if reactionAnalyzer != nil {
				reactionAnalyzer.WindowAt(tick, sessionID, channelID)
			}
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
		case msg, ok := <-messages:
			if !ok {
				return publishAll(ctx, detailedBucketsWithPeaks(bucketizer.FlushDetailed(), reactionAnalyzer), publishers)
			}
			msg = filter.Normalize(msg)
			if !messageFilter.Allow(msg) {
				continue
			}
			if reactionAnalyzer != nil {
				reactionAnalyzer.Add(msg)
				reactionAnalyzer.WindowAt(msg.Timestamp, sessionID, channelID)
			}
			if logMessages {
				logger.Info("chat message", "username", msg.Username, "text", msg.Text, "emotes", msg.Emotes)
			}
			scored := chat.ScoredMessage{Message: msg, Sentiment: analyzer.Analyze(msg)}
			if err := publishAll(ctx, detailedBucketsWithPeaks(bucketizer.AddDetailed(scored), reactionAnalyzer), publishers); err != nil {
				return err
			}
		}
	}
}

func detailedBucketsWithPeaks(items []bucket.DetailedBucket, reactionAnalyzer *reaction.Analyzer) []chat.ChatBucket {
	out := make([]chat.ChatBucket, 0, len(items))
	var windows []chat.ReactionWindow
	if reactionAnalyzer != nil {
		windows = reactionAnalyzer.RecentWindows()
	}
	for _, item := range items {
		item.Bucket = reaction.AttachPeakMetadata(item.Bucket, windows)
		out = append(out, item.Bucket)
	}
	return out
}

func publishOnShutdown(buckets []chat.ChatBucket, publishers []publisher.Publisher) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return publishAll(ctx, buckets, publishers)
}

func publishAll(ctx context.Context, buckets []chat.ChatBucket, publishers []publisher.Publisher) error {
	for _, bucket := range buckets {
		for _, p := range publishers {
			if err := p.Publish(ctx, bucket); err != nil {
				return err
			}
		}
	}
	return nil
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}
