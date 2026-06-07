package chat

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type FakeReader struct {
	SessionID string
	ChannelID string
	Interval  time.Duration
	Now       func() time.Time
}

func (r FakeReader) Read(ctx context.Context) (<-chan ChatMessage, <-chan error) {
	out := make(chan ChatMessage)
	errs := make(chan error, 1)

	interval := r.Interval
	if interval <= 0 {
		interval = 2 * time.Second
	}

	now := r.Now
	if now == nil {
		now = time.Now
	}

	sessionID := defaultString(r.SessionID, "local-session")
	channelID := defaultString(r.ChannelID, "local-channel")

	go func() {
		defer close(out)
		defer close(errs)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		samples := []struct {
			username string
			text     string
			emotes   []string
		}{
			{"viewer42", "that was actually insane PogChamp", []string{"PogChamp"}},
			{"quietfan", "i love this part", nil},
			{"mod_alex", "!discord", nil},
			{"hype_train", "LET'S GO clutch clutch", nil},
			{"skeptic", "that looked rough lol", nil},
			{"lurker7", "great recovery GG", []string{"GG"}},
			{"songbot", "Now playing: local dev track", nil},
			{"viewer99", "bad timing but funny", nil},
		}

		for i := 0; ; i++ {
			sample := samples[i%len(samples)]
			msg := ChatMessage{
				SessionID:   sessionID,
				ChannelID:   channelID,
				MessageID:   fmt.Sprintf("fake-%06d", i+1),
				Timestamp:   now().UTC(),
				Username:    sample.username,
				DisplayName: displayName(sample.username),
				Text:        sample.text,
				Emotes:      sample.emotes,
				Language:    "en",
				IsMod:       sample.username == "mod_alex",
				IsBotLikely: strings.Contains(sample.username, "bot"),
			}

			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			case out <- msg:
			}

			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			case <-ticker.C:
			}
		}
	}()

	return out, errs
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func displayName(username string) string {
	if username == "" {
		return ""
	}
	return strings.ToUpper(username[:1]) + username[1:]
}
