package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/config"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/correlation"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout io.Writer, stderr io.Writer) int {
	var sessions stringListFlag
	var lags durationListFlag
	var envFile string
	var databaseURL string
	var fixturePath string
	var generatedAtRaw string
	var outputFormat string
	var outDir string
	var jsonName string
	var markdownName string
	var sampleName string
	var replayLimit int
	var sessionLimit int
	var minimumPairs int
	var minimumCalmChatMessages int
	var minimumCalmTranscriptChars int
	var manualSamplePerCohort int
	var divergenceThreshold float64
	var minimumCalmQuality float64
	var timeout time.Duration

	fs := flag.NewFlagSet("signal-correlation-report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Var(&sessions, "session", "session ID to evaluate; may be repeated or comma-separated")
	fs.IntVar(&replayLimit, "limit", correlation.DefaultReplayLimit, "maximum replay buckets per source to load (1-500)")
	fs.IntVar(&sessionLimit, "session-limit", 20, "number of recent sessions to use when --session is omitted")
	fs.StringVar(&databaseURL, "database-url", os.Getenv("DATABASE_URL"), "Postgres connection URL (defaults to DATABASE_URL)")
	fs.StringVar(&envFile, "env-file", "", "optional .env file to load before reading DATABASE_URL")
	fs.StringVar(&fixturePath, "fixture", "", "optional JSON replay fixture file; accepts an array or object map of stored session replays")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "overall database and report timeout")
	fs.StringVar(&generatedAtRaw, "generated-at", "", "optional RFC3339 timestamp for deterministic report generation")
	fs.StringVar(&outputFormat, "format", "json", "stdout format: json, markdown, or none")
	fs.StringVar(&outDir, "out-dir", filepath.Join("eval", "reports"), "directory to write report artifacts; empty disables file writes")
	fs.StringVar(&jsonName, "json-name", "signal-correlation-report.json", "JSON report filename")
	fs.StringVar(&markdownName, "markdown-name", "signal-correlation-report.md", "Markdown report filename")
	fs.StringVar(&sampleName, "sample-name", "manual-validation-sample.jsonl", "manual validation sample JSONL filename; empty disables sample file")
	fs.IntVar(&minimumPairs, "min-pairs", correlation.DefaultMinimumPairs, "minimum pairs needed before reporting correlation")
	fs.Float64Var(&divergenceThreshold, "divergence-threshold", correlation.DefaultDivergenceDeltaThreshold, "absolute sentiment delta threshold used to classify detector divergence")
	fs.Float64Var(&minimumCalmQuality, "min-calm-quality", correlation.DefaultMinimumCalmQuality, "minimum alignment quality for calm baseline pairs")
	fs.IntVar(&minimumCalmChatMessages, "min-calm-chat-messages", correlation.DefaultMinimumCalmChatMessages, "minimum chat messages for calm baseline pairs")
	fs.IntVar(&minimumCalmTranscriptChars, "min-calm-transcript-chars", correlation.DefaultMinimumCalmTranscriptLen, "minimum transcript characters for calm baseline pairs")
	fs.IntVar(&manualSamplePerCohort, "manual-sample-per-cohort", correlation.DefaultManualSamplePerCohort, "manual validation sample rows per cohort")
	fs.Var(&lags, "lags", "comma-separated lag offsets such as -60s,-30s,0s,30s,60s")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s [flags] <session_id> [session_id...]\n\n", fs.Name())
		fmt.Fprintln(fs.Output(), "Builds a defensible chat/transcript signal-correlation report from stored replay data.")
		fmt.Fprintln(fs.Output(), "\nFlags:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if strings.TrimSpace(envFile) != "" {
		if err := config.LoadDotEnv(envFile); err != nil {
			fmt.Fprintf(stderr, "load env file: %v\n", err)
			return 2
		}
		if strings.TrimSpace(databaseURL) == "" {
			databaseURL = os.Getenv("DATABASE_URL")
		}
	}

	generatedAt, err := optionalReportTime(generatedAtRaw)
	if err != nil {
		fmt.Fprintf(stderr, "parse generated-at: %v\n", err)
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var store replaySessionStore
	if strings.TrimSpace(fixturePath) != "" {
		fixtureStore, err := storage.NewFixtureStoreFromFile(fixturePath)
		if err != nil {
			fmt.Fprintf(stderr, "load replay fixture: %v\n", err)
			return 2
		}
		store = fixtureStore
	} else {
		if strings.TrimSpace(databaseURL) == "" {
			fmt.Fprintln(stderr, "database URL is required via --database-url, --env-file, or DATABASE_URL")
			return 2
		}
		postgresStore, err := storage.NewPostgresStore(ctx, databaseURL)
		if err != nil {
			fmt.Fprintf(stderr, "connect storage: %v\n", err)
			return 2
		}
		defer postgresStore.Close()
		store = postgresStore
	}

	sessionIDs := normalizeSessionIDs(append([]string(sessions), fs.Args()...))
	if len(sessionIDs) == 0 {
		sessionIDs, err = recentSessionIDs(ctx, store, sessionLimit)
		if err != nil {
			fmt.Fprintf(stderr, "resolve sessions: %v\n", err)
			return 2
		}
	}

	report, err := correlation.Runner{Store: store}.Run(ctx, correlation.Config{
		SessionIDs:                 sessionIDs,
		ReplayLimit:                replayLimit,
		GeneratedAt:                generatedAt,
		MinimumPairs:               minimumPairs,
		DivergenceDeltaThreshold:   divergenceThreshold,
		MinimumCalmQuality:         minimumCalmQuality,
		MinimumCalmChatMessages:    minimumCalmChatMessages,
		MinimumCalmTranscriptChars: minimumCalmTranscriptChars,
		ManualSamplePerCohort:      manualSamplePerCohort,
		LagOffsets:                 lags,
	})
	if err != nil {
		fmt.Fprintf(stderr, "run correlation report: %v\n", err)
		return 2
	}

	if strings.TrimSpace(outDir) != "" {
		if err := writeReportFiles(outDir, jsonName, markdownName, sampleName, report); err != nil {
			fmt.Fprintf(stderr, "write report files: %v\n", err)
			return 2
		}
	}

	switch strings.ToLower(strings.TrimSpace(outputFormat)) {
	case "json":
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			fmt.Fprintf(stderr, "write JSON: %v\n", err)
			return 2
		}
	case "markdown", "md":
		if _, err := io.WriteString(stdout, correlation.RenderMarkdown(report)); err != nil {
			fmt.Fprintf(stderr, "write Markdown: %v\n", err)
			return 2
		}
	case "none", "":
	default:
		fmt.Fprintf(stderr, "unsupported --format %q; expected json, markdown, or none\n", outputFormat)
		return 2
	}

	return 0
}

type replaySessionStore interface {
	correlation.ReplayStore
	ListSessions(context.Context, int) ([]storage.SessionHistory, error)
}

func recentSessionIDs(ctx context.Context, store replaySessionStore, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 20
	}
	sessions, err := store.ListSessions(ctx, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(sessions))
	for _, session := range sessions {
		if strings.TrimSpace(session.SessionID) != "" {
			ids = append(ids, session.SessionID)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no sessions found")
	}
	return ids, nil
}

func writeReportFiles(outDir, jsonName, markdownName, sampleName string, report correlation.Report) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	if strings.TrimSpace(jsonName) != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(outDir, jsonName), append(data, '\n'), 0o644); err != nil {
			return err
		}
	}
	if strings.TrimSpace(markdownName) != "" {
		if err := os.WriteFile(filepath.Join(outDir, markdownName), []byte(correlation.RenderMarkdown(report)), 0o644); err != nil {
			return err
		}
	}
	if strings.TrimSpace(sampleName) != "" {
		var builder strings.Builder
		encoder := json.NewEncoder(&builder)
		encoder.SetEscapeHTML(false)
		for _, row := range report.ManualValidationSample {
			if err := encoder.Encode(row); err != nil {
				return err
			}
		}
		if err := os.WriteFile(filepath.Join(outDir, sampleName), []byte(builder.String()), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func optionalReportTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("must be RFC3339")
	}
	return value, nil
}

func normalizeSessionIDs(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			sessionID := strings.TrimSpace(part)
			if sessionID == "" {
				continue
			}
			if _, ok := seen[sessionID]; ok {
				continue
			}
			seen[sessionID] = struct{}{}
			out = append(out, sessionID)
		}
	}
	return out
}

type stringListFlag []string

func (f *stringListFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type durationListFlag []time.Duration

func (f *durationListFlag) String() string {
	if len(*f) == 0 {
		return "-60s,-30s,0s,30s,60s"
	}
	parts := make([]string, 0, len(*f))
	for _, value := range *f {
		parts = append(parts, value.String())
	}
	return strings.Join(parts, ",")
}

func (f *durationListFlag) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, err := strconv.Atoi(part); err == nil {
			part += "s"
		}
		parsed, err := time.ParseDuration(part)
		if err != nil {
			return fmt.Errorf("invalid lag %q", part)
		}
		*f = append(*f, parsed)
	}
	return nil
}
