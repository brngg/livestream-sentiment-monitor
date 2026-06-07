package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/regression"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func TestRunLoadsReplayFixtureWithoutDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	fixturePath := writeReplayFixture(t, []storage.SessionReplay{
		testReplay("session-1", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)),
	})

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"--fixture", fixturePath,
		"--session", "session-1",
		"--format", "json",
	}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("run exit = %d, stderr = %s", exitCode, stderr.String())
	}
	var report regression.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout.String())
	}
	if len(report.Sessions) != 1 || report.Sessions[0].SessionID != "session-1" {
		t.Fatalf("unexpected fixture sessions: %#v", report.Sessions)
	}
}

func TestRunFixtureMapSupportsSessionSelection(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := map[string]storage.SessionReplay{
		"session-1": testReplay("session-1", start),
		"session-2": testReplay("session-2", start.Add(time.Hour)),
	}
	fixturePath := writeJSONFile(t, fixture)

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"--fixture", fixturePath,
		"--session", "session-2",
		"--format", "table",
	}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("run exit = %d, stderr = %s", exitCode, stderr.String())
	}
	table := stdout.String()
	if !strings.Contains(table, "session-2") {
		t.Fatalf("table missing selected session:\n%s", table)
	}
	if strings.Contains(table, "session-1") {
		t.Fatalf("table includes unselected session:\n%s", table)
	}
}

func TestRunFixtureUsesDeterministicGeneratedAt(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	fixturePath := writeReplayFixture(t, []storage.SessionReplay{
		testReplay("session-1", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)),
	})

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"--fixture", fixturePath,
		"--session", "session-1",
		"--generated-at", "2026-05-08T15:00:00Z",
		"--format", "json",
	}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("run exit = %d, stderr = %s", exitCode, stderr.String())
	}
	var report regression.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout.String())
	}
	if !report.GeneratedAt.Equal(time.Date(2026, 5, 8, 15, 0, 0, 0, time.UTC)) {
		t.Fatalf("generated_at = %s, want deterministic timestamp", report.GeneratedAt)
	}
}

func TestRunFixtureReportsPartialWhenLimitTruncatesSources(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	replay := testReplay("session-1", start)
	replay.Session.ChatBucketCount = 2
	replay.ChatBuckets = append(replay.ChatBuckets, chat.ChatBucket{
		SessionID:     "session-1",
		ChannelID:     "channel-1",
		BucketStart:   start.Add(30 * time.Second),
		BucketEnd:     start.Add(60 * time.Second),
		MessageCount:  3,
		ChatSentiment: 0.1,
	})
	fixturePath := writeReplayFixture(t, []storage.SessionReplay{replay})

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"--fixture", fixturePath,
		"--session", "session-1",
		"--limit", "1",
		"--format", "json",
	}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("run exit = %d, stderr = %s", exitCode, stderr.String())
	}
	var report regression.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout.String())
	}
	if len(report.Sessions) != 1 || !report.Sessions[0].Partial || len(report.Sessions[0].TruncatedSources) == 0 {
		t.Fatalf("expected truncated fixture replay, got %#v", report.Sessions)
	}
}

func TestRunCheckedInGoldenFixtureMatchesBaseline(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	fixturePath := filepath.Join("..", "..", "testdata", "golden-replay", "sessions.json")
	baselinePath := filepath.Join("..", "..", "testdata", "golden-replay", "baseline.json")

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"--fixture", fixturePath,
		"--baseline", baselinePath,
		"--generated-at", "2026-05-08T15:00:00Z",
		"--format", "table",
	}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("run exit = %d, stderr = %s\nstdout:\n%s", exitCode, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "golden_hype_peak") || strings.Contains(stdout.String(), "REGRESSION\t") {
		t.Fatalf("unexpected golden replay output:\n%s", stdout.String())
	}
}

func writeReplayFixture(t *testing.T, replays []storage.SessionReplay) string {
	t.Helper()
	return writeJSONFile(t, replays)
}

func writeJSONFile(t *testing.T, value any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replay-fixture.json")
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func testReplay(sessionID string, start time.Time) storage.SessionReplay {
	return storage.SessionReplay{
		Session: storage.SessionHistory{
			SessionID:       sessionID,
			ChannelID:       "channel-1",
			Status:          "completed",
			StartedAt:       start,
			ChatBucketCount: 1,
			LabelCount:      1,
		},
		ChatBuckets: []chat.ChatBucket{
			{
				SessionID:     sessionID,
				ChannelID:     "channel-1",
				BucketStart:   start,
				BucketEnd:     start.Add(30 * time.Second),
				MessageCount:  5,
				ChatSentiment: 0.3,
			},
		},
		WindowLabels: []storage.SignalWindowLabel{
			{
				SessionID:   sessionID,
				WindowStart: start,
				WindowEnd:   start.Add(30 * time.Second),
				Correctness: "correct",
				EventLabel:  "none",
			},
		},
		LabelCount: 1,
	}
}
