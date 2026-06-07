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
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/correlation"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func TestRunWritesReportsFromFixtureWithoutDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixturePath := writeFixture(t, []storage.SessionReplay{testReplay("session-1", start)})
	outDir := t.TempDir()

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"--fixture", fixturePath,
		"--session", "session-1",
		"--out-dir", outDir,
		"--format", "none",
		"--manual-sample-per-cohort", "1",
		"--generated-at", "2026-05-02T15:00:00Z",
	}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("run exit = %d, stderr = %s", exitCode, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected no stdout for --format none, got %s", stdout.String())
	}

	var report correlation.Report
	decodeJSONFile(t, filepath.Join(outDir, "signal-correlation-report.json"), &report)
	if !report.GeneratedAt.Equal(time.Date(2026, 5, 2, 15, 0, 0, 0, time.UTC)) {
		t.Fatalf("generated_at = %s", report.GeneratedAt)
	}
	if len(report.Sessions) != 1 || report.Sessions[0].SessionID != "session-1" {
		t.Fatalf("unexpected sessions: %#v", report.Sessions)
	}
	if len(report.ManualValidationSample) == 0 {
		t.Fatalf("expected manual validation sample rows")
	}
	markdown := readFile(t, filepath.Join(outDir, "signal-correlation-report.md"))
	if !strings.Contains(markdown, "Baseline Calibration") || !strings.Contains(markdown, "Negative Control") {
		t.Fatalf("markdown missing credibility sections:\n%s", markdown)
	}
	sample := readFile(t, filepath.Join(outDir, "manual-validation-sample.jsonl"))
	if !strings.Contains(sample, `"review_status":"needs_human_review"`) {
		t.Fatalf("sample missing review placeholder:\n%s", sample)
	}
}

func TestRunFixtureUsesAllSessionsWhenOmitted(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixturePath := writeFixture(t, map[string]storage.SessionReplay{
		"session-1": testReplay("session-1", start),
		"session-2": testReplay("session-2", start.Add(time.Hour)),
	})

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"--fixture", fixturePath,
		"--out-dir", "",
		"--format", "json",
		"--generated-at", "2026-05-02T15:00:00Z",
	}, &stdout, &stderr)

	if exitCode != 0 {
		t.Fatalf("run exit = %d, stderr = %s", exitCode, stderr.String())
	}
	var report correlation.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode stdout report: %v\n%s", err, stdout.String())
	}
	if len(report.Sessions) != 2 {
		t.Fatalf("session count = %d, want 2", len(report.Sessions))
	}
}

func TestRunRejectsUnsupportedFormat(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	fixturePath := writeFixture(t, []storage.SessionReplay{testReplay("session-1", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))})

	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"--fixture", fixturePath,
		"--out-dir", "",
		"--format", "table",
	}, &stdout, &stderr)

	if exitCode != 2 {
		t.Fatalf("exit = %d, want 2", exitCode)
	}
	if !strings.Contains(stderr.String(), "unsupported --format") {
		t.Fatalf("stderr missing format error: %s", stderr.String())
	}
}

func writeFixture(t *testing.T, value any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replay.json")
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func decodeJSONFile(t *testing.T, path string, out any) {
	t.Helper()
	data := readFile(t, path)
	if err := json.Unmarshal([]byte(data), out); err != nil {
		t.Fatalf("decode %s: %v\n%s", path, err, data)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func testReplay(sessionID string, start time.Time) storage.SessionReplay {
	replay := storage.SessionReplay{
		Session: storage.SessionHistory{SessionID: sessionID, ChannelID: "channel-1", StartedAt: start},
	}
	scores := []float64{-0.3, 0, 0.3}
	for index, score := range scores {
		appendPair(&replay, start.Add(time.Duration(index)*30*time.Second), score, score+0.01, "converged", "none")
	}
	appendPair(&replay, start.Add(3*30*time.Second), 0.75, -0.10, "diverged", "content_audience_divergence")
	return replay
}

func appendPair(replay *storage.SessionReplay, start time.Time, chatScore, transcriptScore float64, relationship, label string) {
	sessionID := replay.Session.SessionID
	channelID := replay.Session.ChannelID
	end := start.Add(30 * time.Second)
	replay.ChatBuckets = append(replay.ChatBuckets, chat.ChatBucket{
		Type:                "chat_bucket",
		SessionID:           sessionID,
		ChannelID:           channelID,
		BucketStart:         start,
		BucketEnd:           end,
		MessageCount:        20,
		UniqueChatters:      10,
		ChatSentiment:       chatScore,
		SentimentConfidence: 0.9,
		MessageScores: []chat.MessageScore{{
			MessageID:      sessionID + ":" + start.Format(time.RFC3339),
			Timestamp:      start.Add(5 * time.Second),
			Text:           "fixture chat evidence",
			Label:          "neutral",
			Confidence:     0.9,
			SentimentScore: chatScore,
		}},
		TopTerms: []string{"fixture", "evidence"},
	})
	replay.TranscriptBuckets = append(replay.TranscriptBuckets, storage.TranscriptBucket{
		Type:                 "transcript_bucket",
		SessionID:            sessionID,
		ChannelID:            channelID,
		BucketStart:          start,
		BucketEnd:            end,
		Text:                 "fixture transcript text is long enough for calm baseline quality checks",
		Language:             "en",
		TranscriptConfidence: 0.9,
		TranscriptStatus:     "final",
		SentimentScore:       &transcriptScore,
		SentimentLabel:       "neutral",
		SentimentStatus:      "python",
		WordCount:            11,
	})
	replay.Alignments = append(replay.Alignments, storage.AlignmentBucket{
		Type:                  "alignment_bucket",
		SessionID:             sessionID,
		ChannelID:             channelID,
		WindowStart:           start,
		WindowEnd:             end,
		ChatBucketStart:       start,
		ChatBucketEnd:         end,
		TranscriptBucketStart: start,
		TranscriptBucketEnd:   end,
		ChatSentiment:         chatScore,
		ChatConfidence:        0.9,
		ChatMessageCount:      20,
		TranscriptSentiment:   transcriptScore,
		TranscriptConfidence:  0.9,
		TranscriptTextLength:  110,
		Delta:                 chatScore - transcriptScore,
		Similarity:            0.9,
		Relationship:          relationship,
		OverlapSeconds:        30,
		Quality:               0.9,
		QualityFlags:          []string{"good_overlap", "enough_chat_volume", "good_transcript_confidence", "enough_transcript_text"},
	})
	replay.WindowLabels = append(replay.WindowLabels, storage.SignalWindowLabel{
		SessionID:   sessionID,
		WindowStart: start,
		WindowEnd:   end,
		EventLabel:  label,
		Correctness: "correct",
	})
}
