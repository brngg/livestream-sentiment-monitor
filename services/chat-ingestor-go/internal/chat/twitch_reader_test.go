package chat

import (
	"testing"
	"time"
)

func TestParseTwitchPrivmsgWithTags(t *testing.T) {
	line := "@badge-info=;badges=moderator/1;color=#0000FF;display-name=TestUser;emotes=88:11-18;id=msg-123;login=testuser;mod=1;tmi-sent-ts=1777485600123;user-type=mod :testuser!testuser@testuser.tmi.twitch.tv PRIVMSG #SomeChannel :hello chat PogChamp"

	msg, ok, err := parseTwitchPrivmsg(line, "session-1", "fallback", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected PRIVMSG to parse")
	}
	if msg.SessionID != "session-1" {
		t.Fatalf("session id = %q", msg.SessionID)
	}
	if msg.ChannelID != "somechannel" {
		t.Fatalf("channel id = %q", msg.ChannelID)
	}
	if msg.MessageID != "msg-123" {
		t.Fatalf("message id = %q", msg.MessageID)
	}
	if msg.Username != "testuser" {
		t.Fatalf("username = %q", msg.Username)
	}
	if msg.DisplayName != "TestUser" {
		t.Fatalf("display name = %q", msg.DisplayName)
	}
	if msg.Text != "hello chat PogChamp" {
		t.Fatalf("text = %q", msg.Text)
	}
	if !msg.IsMod {
		t.Fatal("expected mod flag")
	}
	if len(msg.Badges) != 1 || msg.Badges[0] != "moderator" {
		t.Fatalf("badges = %#v", msg.Badges)
	}
	if len(msg.Emotes) != 1 || msg.Emotes[0] != "PogChamp" {
		t.Fatalf("emotes = %#v", msg.Emotes)
	}
	if got := msg.Timestamp.Format(time.RFC3339Nano); got != "2026-04-29T18:00:00.123Z" {
		t.Fatalf("timestamp = %s", got)
	}
}

func TestParseTwitchPrivmsgIgnoresNonPrivmsg(t *testing.T) {
	_, ok, err := parseTwitchPrivmsg(":tmi.twitch.tv 001 bot :Welcome, GLHF!", "session-1", "channel", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected non-PRIVMSG to be ignored")
	}
}

func TestFormatOAuthToken(t *testing.T) {
	if got := formatOAuthToken("abc"); got != "oauth:abc" {
		t.Fatalf("token = %q", got)
	}
	if got := formatOAuthToken("oauth:abc"); got != "oauth:abc" {
		t.Fatalf("token = %q", got)
	}
}
