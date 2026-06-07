package chat

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const DefaultTwitchIRCAddr = "irc.chat.twitch.tv:6667"

type TwitchReader struct {
	SessionID   string
	ChannelID   string
	Nick        string
	OAuthToken  string
	Addr        string
	DialTimeout time.Duration
	Now         func() time.Time
}

func (r TwitchReader) Read(ctx context.Context) (<-chan ChatMessage, <-chan error) {
	out := make(chan ChatMessage)
	errs := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errs)

		if err := r.read(ctx, out); err != nil {
			errs <- err
		}
	}()

	return out, errs
}

func (r TwitchReader) read(ctx context.Context, out chan<- ChatMessage) error {
	channel := normalizeChannel(r.ChannelID)
	if channel == "" {
		return fmt.Errorf("twitch channel is required")
	}

	addr := r.Addr
	if addr == "" {
		addr = DefaultTwitchIRCAddr
	}

	timeout := r.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("connect twitch irc: %w", err)
	}
	defer conn.Close()

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	nick := r.Nick
	if nick == "" {
		nick = anonymousNick()
	}

	if r.OAuthToken != "" {
		if err := writeIRCLine(conn, "PASS "+formatOAuthToken(r.OAuthToken)); err != nil {
			return err
		}
	}
	if err := writeIRCLine(conn, "NICK "+nick); err != nil {
		return err
	}
	if err := writeIRCLine(conn, "CAP REQ :twitch.tv/tags twitch.tv/commands"); err != nil {
		return err
	}
	if err := writeIRCLine(conn, "JOIN #"+channel); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read twitch irc: %w", err)
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "PING ") {
			if err := writeIRCLine(conn, "PONG "+strings.TrimPrefix(line, "PING ")); err != nil {
				return err
			}
			continue
		}

		if strings.Contains(line, " RECONNECT") {
			return fmt.Errorf("twitch requested reconnect")
		}

		msg, ok, err := parseTwitchPrivmsg(line, defaultString(r.SessionID, "local-session"), channel, r.now)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- msg:
		}
	}
}

func writeIRCLine(conn net.Conn, line string) error {
	_, err := fmt.Fprintf(conn, "%s\r\n", line)
	return err
}

func parseTwitchPrivmsg(line, sessionID, fallbackChannel string, now func() time.Time) (ChatMessage, bool, error) {
	tags := map[string]string{}
	rest := line
	if strings.HasPrefix(rest, "@") {
		idx := strings.IndexByte(rest, ' ')
		if idx < 0 {
			return ChatMessage{}, false, nil
		}
		tags = parseTags(rest[1:idx])
		rest = strings.TrimSpace(rest[idx+1:])
	}

	prefix := ""
	if strings.HasPrefix(rest, ":") {
		idx := strings.IndexByte(rest, ' ')
		if idx < 0 {
			return ChatMessage{}, false, nil
		}
		prefix = rest[1:idx]
		rest = strings.TrimSpace(rest[idx+1:])
	}

	head, text, _ := strings.Cut(rest, " :")
	fields := strings.Fields(head)
	if len(fields) < 2 || fields[0] != "PRIVMSG" {
		return ChatMessage{}, false, nil
	}

	channel := normalizeChannel(fields[1])
	if channel == "" {
		channel = fallbackChannel
	}

	username := tags["login"]
	if username == "" {
		username = usernameFromPrefix(prefix)
	}
	userDisplayName := unescapeTagValue(tags["display-name"])
	if userDisplayName == "" {
		userDisplayName = displayName(username)
	}

	timestamp := timestampFromTags(tags, now)
	messageID := tags["id"]
	if messageID == "" {
		messageID = fmt.Sprintf("%s-%s-%d", channel, username, timestamp.UnixNano())
	}

	msg := ChatMessage{
		SessionID:   sessionID,
		ChannelID:   channel,
		MessageID:   messageID,
		Timestamp:   timestamp,
		Username:    username,
		DisplayName: userDisplayName,
		Text:        text,
		Emotes:      emotesFromTags(text, tags["emotes"]),
		Badges:      badgesFromTag(tags["badges"]),
		Language:    "other",
		IsMod:       isMod(tags),
		IsBotLikely: strings.Contains(strings.ToLower(username), "bot"),
	}
	return msg, true, nil
}

func parseTags(raw string) map[string]string {
	tags := map[string]string{}
	for _, item := range strings.Split(raw, ";") {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			tags[item] = ""
			continue
		}
		tags[key] = unescapeTagValue(value)
	}
	return tags
}

func unescapeTagValue(value string) string {
	replacer := strings.NewReplacer(`\s`, " ", `\:`, ";", `\r`, "\r", `\n`, "\n", `\\`, `\`)
	return replacer.Replace(value)
}

func timestampFromTags(tags map[string]string, now func() time.Time) time.Time {
	if raw := tags["tmi-sent-ts"]; raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return time.UnixMilli(ms).UTC()
		}
	}
	if now == nil {
		now = time.Now
	}
	return now().UTC()
}

func badgesFromTag(raw string) []string {
	if raw == "" {
		return nil
	}
	var badges []string
	for _, item := range strings.Split(raw, ",") {
		if item == "" {
			continue
		}
		name, _, _ := strings.Cut(item, "/")
		if name != "" {
			badges = append(badges, name)
		}
	}
	return badges
}

func emotesFromTags(text, raw string) []string {
	if raw == "" {
		return nil
	}

	runes := []rune(text)
	seen := map[string]struct{}{}
	var emotes []string
	for _, group := range strings.Split(raw, "/") {
		emoteID, ranges, ok := strings.Cut(group, ":")
		if !ok {
			continue
		}
		for _, item := range strings.Split(ranges, ",") {
			startRaw, endRaw, ok := strings.Cut(item, "-")
			if !ok {
				continue
			}
			start, startErr := strconv.Atoi(startRaw)
			end, endErr := strconv.Atoi(endRaw)
			if startErr != nil || endErr != nil || start < 0 || end < start || end >= len(runes) {
				if _, ok := seen[emoteID]; !ok {
					seen[emoteID] = struct{}{}
					emotes = append(emotes, emoteID)
				}
				continue
			}
			name := string(runes[start : end+1])
			if name == "" {
				name = emoteID
			}
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				emotes = append(emotes, name)
			}
		}
	}
	return emotes
}

func isMod(tags map[string]string) bool {
	if tags["mod"] == "1" || tags["user-type"] == "mod" {
		return true
	}
	for _, badge := range badgesFromTag(tags["badges"]) {
		if badge == "moderator" || badge == "broadcaster" {
			return true
		}
	}
	return false
}

func usernameFromPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	username, _, _ := strings.Cut(prefix, "!")
	return username
}

func normalizeChannel(channel string) string {
	channel = strings.TrimSpace(strings.ToLower(channel))
	channel = strings.TrimPrefix(channel, "#")
	return channel
}

func formatOAuthToken(token string) string {
	token = strings.TrimSpace(token)
	if strings.HasPrefix(token, "oauth:") {
		return token
	}
	return "oauth:" + token
}

func anonymousNick() string {
	return fmt.Sprintf("justinfan%d", time.Now().UnixNano()%1000000)
}

func (r TwitchReader) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
