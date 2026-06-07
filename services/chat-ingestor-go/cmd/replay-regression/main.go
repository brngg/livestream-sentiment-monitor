package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/bucket"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/config"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/regression"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout io.Writer, stderr io.Writer) int {
	var sessions stringListFlag
	var speeds floatListFlag
	var envFile string
	var databaseURL string
	var outputFormat string
	var baselinePath string
	var fixturePath string
	var generatedAtRaw string
	var replayLimit int
	var timeout time.Duration
	var alignmentWindow time.Duration
	thresholds := regression.Thresholds{}

	fs := flag.NewFlagSet("replay-regression", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Var(&sessions, "session", "session ID to evaluate; may be repeated or comma-separated")
	fs.IntVar(&replayLimit, "limit", 500, "maximum replay buckets per source to load (1-500)")
	fs.StringVar(&databaseURL, "database-url", os.Getenv("DATABASE_URL"), "Postgres connection URL (defaults to DATABASE_URL)")
	fs.StringVar(&envFile, "env-file", "", "optional .env file to load before reading DATABASE_URL")
	fs.StringVar(&fixturePath, "fixture", "", "optional JSON replay fixture file; accepts an array or object map of stored session replays")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "overall database and evaluation timeout")
	fs.DurationVar(&alignmentWindow, "alignment-window", bucket.DefaultWindow, "alignment window used when rebuilding signal windows")
	fs.Var(&speeds, "speeds", "comma-separated replay speeds for proof metrics (default 1,5,10)")
	fs.StringVar(&outputFormat, "format", "json", "output format: json or table")
	fs.StringVar(&baselinePath, "baseline", "", "optional baseline report JSON to compare against")
	fs.StringVar(&generatedAtRaw, "generated-at", "", "optional RFC3339 timestamp for deterministic report generation")
	fs.Float64Var(&thresholds.MaxProofLabelCoverageDrop, "max-proof-label-coverage-drop", 0, "allowed proof label coverage drop versus baseline")
	fs.Float64Var(&thresholds.MaxEvaluationCoverageDrop, "max-evaluation-coverage-drop", 0, "allowed evaluation coverage drop versus baseline")
	fs.Float64Var(&thresholds.MaxEventAccuracyDrop, "max-event-accuracy-drop", 0, "allowed event accuracy drop versus baseline")
	fs.Float64Var(&thresholds.MaxPrecisionDrop, "max-precision-drop", 0, "allowed event precision drop versus baseline")
	fs.Float64Var(&thresholds.MaxRecallDrop, "max-recall-drop", 0, "allowed event recall drop versus baseline")
	fs.Float64Var(&thresholds.MaxF1Drop, "max-f1-drop", 0, "allowed event F1 drop versus baseline")
	fs.Float64Var(&thresholds.MaxOnsetLatencyIncreaseMS, "max-onset-latency-increase-ms", 0, "allowed onset latency increase in ms versus baseline")
	fs.Float64Var(&thresholds.MaxLatencyP95IncreaseMS, "max-latency-p95-increase-ms", 0, "allowed p95 latency increase in ms; set negative to ignore")
	fs.Float64Var(&thresholds.MaxTranscriptCoverageDrop, "max-transcript-coverage-drop", 0, "allowed transcript coverage drop versus baseline")
	fs.Float64Var(&thresholds.MaxTranscriptAudioCoverageDrop, "max-transcript-audio-coverage-drop", 0, "allowed transcript audio coverage drop versus baseline")
	fs.Float64Var(&thresholds.MaxTranscriptEmptyRatioIncrease, "max-transcript-empty-ratio-increase", 0, "allowed transcript empty ratio increase versus baseline")
	fs.Float64Var(&thresholds.MaxTranscriptRepairImprovementDrop, "max-transcript-repair-improvement-drop", 0, "allowed transcript repair improvement drop versus baseline")
	fs.Float64Var(&thresholds.MaxTranscriptRepairChangedIncrease, "max-transcript-repair-changed-ratio-increase", 0, "allowed transcript repair changed ratio increase versus baseline")
	fs.Float64Var(&thresholds.MaxTranscriptRepairAddedWordsIncrease, "max-transcript-repair-added-words-increase", 0, "allowed transcript repair added words increase versus baseline")
	fs.BoolVar(&thresholds.AllowNewPartial, "allow-new-partial", false, "do not fail if current replay becomes partial or more truncated than baseline")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: %s [flags] <session_id> [session_id...]\n\n", fs.Name())
		fmt.Fprintln(fs.Output(), "Runs stored-session replay proof and evaluation metrics, optionally comparing them with a baseline JSON report.")
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

	sessionIDs := regression.NormalizeSessionIDs(append([]string(sessions), fs.Args()...))
	var baseline regression.Report
	if strings.TrimSpace(baselinePath) != "" {
		loaded, err := loadReport(baselinePath)
		if err != nil {
			fmt.Fprintf(stderr, "load baseline: %v\n", err)
			return 2
		}
		baseline = loaded
		if len(sessionIDs) == 0 {
			sessionIDs = regression.SessionIDs(baseline)
		}
	}
	if len(sessionIDs) == 0 {
		if strings.TrimSpace(fixturePath) == "" {
			fmt.Fprintln(stderr, "at least one session ID is required")
			return 2
		}
	}
	generatedAt, err := optionalReportTime(generatedAtRaw)
	if err != nil {
		fmt.Fprintf(stderr, "parse generated-at: %v\n", err)
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var store regression.ReplayStore
	if strings.TrimSpace(fixturePath) != "" {
		fixtureStore, err := storage.NewFixtureStoreFromFile(fixturePath)
		if err != nil {
			fmt.Fprintf(stderr, "load replay fixture: %v\n", err)
			return 2
		}
		if len(sessionIDs) == 0 {
			sessionIDs = fixtureStore.SessionIDs()
		}
		store = fixtureStore
	} else {
		if strings.TrimSpace(databaseURL) == "" {
			fmt.Fprintln(stderr, "database URL is required via --database-url or DATABASE_URL")
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

	report, err := regression.Runner{Store: store}.Run(ctx, regression.Config{
		SessionIDs:      sessionIDs,
		ReplayLimit:     replayLimit,
		Speeds:          speeds,
		AlignmentWindow: alignmentWindow,
		GeneratedAt:     generatedAt,
	})
	if err != nil {
		fmt.Fprintf(stderr, "run regression: %v\n", err)
		return 2
	}

	exitCode := 0
	if strings.TrimSpace(baselinePath) != "" {
		comparison := regression.CompareReports(report, baseline, thresholds)
		comparison.BaselinePath = baselinePath
		report.Comparison = &comparison
		if !comparison.Passed {
			exitCode = 1
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
	case "table":
		if err := regression.WriteTable(stdout, report); err != nil {
			fmt.Fprintf(stderr, "write table: %v\n", err)
			return 2
		}
	default:
		fmt.Fprintf(stderr, "unsupported --format %q; expected json or table\n", outputFormat)
		return 2
	}

	return exitCode
}

func loadReport(path string) (regression.Report, error) {
	file, err := os.Open(path)
	if err != nil {
		return regression.Report{}, err
	}
	defer file.Close()

	var report regression.Report
	if err := json.NewDecoder(file).Decode(&report); err != nil {
		return regression.Report{}, err
	}
	if len(report.Sessions) == 0 {
		return regression.Report{}, fmt.Errorf("baseline contains no sessions")
	}
	return report, nil
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

type stringListFlag []string

func (f *stringListFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type floatListFlag []float64

func (f *floatListFlag) String() string {
	parts := make([]string, 0, len(*f))
	for _, value := range *f {
		parts = append(parts, strconv.FormatFloat(value, 'f', -1, 64))
	}
	return strings.Join(parts, ",")
}

func (f *floatListFlag) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		parsed, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return fmt.Errorf("invalid speed %q", part)
		}
		*f = append(*f, parsed)
	}
	return nil
}
