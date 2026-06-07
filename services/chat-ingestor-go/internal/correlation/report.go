package correlation

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

const (
	ReportType = "signal_correlation_report"

	CohortAllAligned         = "all_aligned"
	CohortCalmBaseline       = "calm_baseline"
	CohortDetectedDivergence = "detected_divergence"
	CohortLabeledEvents      = "labeled_events"
	CohortLabeledNone        = "labeled_none"

	DefaultReplayLimit              = 500
	DefaultMinimumPairs             = 3
	DefaultDivergenceDeltaThreshold = 0.45
	DefaultMinimumCalmQuality       = 0.60
	DefaultMinimumCalmChatMessages  = 5
	DefaultMinimumCalmTranscriptLen = 40
	DefaultManualSamplePerCohort    = 10
)

var defaultLagOffsets = []time.Duration{-60 * time.Second, -30 * time.Second, 0, 30 * time.Second, 60 * time.Second}

type ReplayStore interface {
	GetSessionReplay(context.Context, string, int) (storage.SessionReplay, error)
}

type Config struct {
	SessionIDs                 []string
	ReplayLimit                int
	GeneratedAt                time.Time
	MinimumPairs               int
	DivergenceDeltaThreshold   float64
	MinimumCalmQuality         float64
	MinimumCalmChatMessages    int
	MinimumCalmTranscriptChars int
	ManualSamplePerCohort      int
	LagOffsets                 []time.Duration
}

type Runner struct {
	Store ReplayStore
}

type Report struct {
	Type                   string                   `json:"type"`
	GeneratedAt            time.Time                `json:"generated_at"`
	Scope                  string                   `json:"scope"`
	ReplayLimit            int                      `json:"replay_limit"`
	CohortConfig           CohortConfig             `json:"cohort_config"`
	DataQuality            DataQualitySummary       `json:"data_quality"`
	Aggregate              []CohortSummary          `json:"aggregate"`
	Comparison             CorrelationComparison    `json:"comparison"`
	Calibration            DeltaCalibration         `json:"delta_calibration"`
	LagAnalysis            []LagSummary             `json:"lag_analysis"`
	BestLag                *LagSummary              `json:"best_lag,omitempty"`
	NegativeControl        NegativeControlSummary   `json:"negative_control"`
	ManualValidationSample []ManualValidationRecord `json:"manual_validation_sample,omitempty"`
	Sessions               []SessionResult          `json:"sessions"`
	Limitations            []string                 `json:"limitations,omitempty"`
}

type CohortConfig struct {
	MinimumPairs               int      `json:"minimum_pairs_for_correlation"`
	DivergenceDeltaThreshold   float64  `json:"divergence_delta_threshold"`
	MinimumCalmQuality         float64  `json:"minimum_calm_quality"`
	MinimumCalmChatMessages    int      `json:"minimum_calm_chat_messages"`
	MinimumCalmTranscriptChars int      `json:"minimum_calm_transcript_chars"`
	LagOffsetsSeconds          []int    `json:"lag_offsets_seconds"`
	ManualSamplePerCohort      int      `json:"manual_sample_per_cohort"`
	ManualSampleCohorts        []string `json:"manual_sample_cohorts"`
}

type SessionResult struct {
	SessionID       string                 `json:"session_id"`
	ChannelID       string                 `json:"channel_id,omitempty"`
	PairCount       int                    `json:"pair_count"`
	DataQuality     DataQualitySummary     `json:"data_quality"`
	Cohorts         []CohortSummary        `json:"cohorts"`
	Calibration     DeltaCalibration       `json:"delta_calibration"`
	LagAnalysis     []LagSummary           `json:"lag_analysis"`
	NegativeControl NegativeControlSummary `json:"negative_control"`
	Limitations     []string               `json:"limitations,omitempty"`
}

type CohortSummary struct {
	Name                    string         `json:"name"`
	Description             string         `json:"description"`
	PairCount               int            `json:"pair_count"`
	CorrelationStatus       string         `json:"correlation_status"`
	Pearson                 *float64       `json:"pearson"`
	Spearman                *float64       `json:"spearman"`
	MeanChatSentiment       *float64       `json:"mean_chat_sentiment"`
	MeanTranscriptSentiment *float64       `json:"mean_transcript_sentiment"`
	AverageDelta            *float64       `json:"average_delta"`
	AverageAbsDelta         *float64       `json:"average_abs_delta"`
	AbsDeltaMedian          *float64       `json:"abs_delta_median"`
	AbsDeltaP90             *float64       `json:"abs_delta_p90"`
	AbsDeltaP95             *float64       `json:"abs_delta_p95"`
	AverageQuality          *float64       `json:"average_quality"`
	RelationshipCounts      map[string]int `json:"relationship_counts,omitempty"`
	LabelCounts             map[string]int `json:"label_counts,omitempty"`
}

type CorrelationComparison struct {
	BaselineCohort        string   `json:"baseline_cohort"`
	EventCohort           string   `json:"event_cohort"`
	Status                string   `json:"status"`
	PearsonDrop           *float64 `json:"pearson_drop_from_baseline"`
	SpearmanDrop          *float64 `json:"spearman_drop_from_baseline"`
	AverageAbsDeltaChange *float64 `json:"average_abs_delta_change"`
}

type DeltaCalibration struct {
	Status                       string   `json:"status"`
	BaselinePairCount            int      `json:"baseline_pair_count"`
	DetectedDivergencePairCount  int      `json:"detected_divergence_pair_count"`
	CurrentThreshold             float64  `json:"current_threshold"`
	RecommendedThreshold         *float64 `json:"recommended_threshold"`
	BaselineAbsDeltaMedian       *float64 `json:"baseline_abs_delta_median"`
	BaselineAbsDeltaP90          *float64 `json:"baseline_abs_delta_p90"`
	BaselineAbsDeltaP95          *float64 `json:"baseline_abs_delta_p95"`
	DetectedAboveBaselineP95     int      `json:"detected_above_baseline_p95"`
	DetectedAboveBaselineP95Rate *float64 `json:"detected_above_baseline_p95_rate"`
	ThresholdAssessment          string   `json:"threshold_assessment"`
}

type LagSummary struct {
	LagSeconds        int      `json:"lag_seconds"`
	Description       string   `json:"description"`
	PairCount         int      `json:"pair_count"`
	CorrelationStatus string   `json:"correlation_status"`
	Pearson           *float64 `json:"pearson"`
	Spearman          *float64 `json:"spearman"`
	AverageAbsDelta   *float64 `json:"average_abs_delta"`
}

type NegativeControlSummary struct {
	Status           string   `json:"status"`
	Method           string   `json:"method"`
	PairCount        int      `json:"pair_count"`
	ObservedPearson  *float64 `json:"observed_pearson"`
	ObservedSpearman *float64 `json:"observed_spearman"`
	ShuffledPearson  *float64 `json:"shuffled_pearson"`
	ShuffledSpearman *float64 `json:"shuffled_spearman"`
	PearsonDrop      *float64 `json:"pearson_drop_vs_shuffled"`
	SpearmanDrop     *float64 `json:"spearman_drop_vs_shuffled"`
	Interpretation   string   `json:"interpretation"`
}

type DataQualitySummary struct {
	SessionCount               int            `json:"session_count"`
	ChatBucketCount            int            `json:"chat_bucket_count"`
	TranscriptBucketCount      int            `json:"transcript_bucket_count"`
	AlignmentPairCount         int            `json:"alignment_pair_count"`
	BaselinePairCount          int            `json:"baseline_pair_count"`
	DetectedDivergenceCount    int            `json:"detected_divergence_count"`
	LabeledEventPairCount      int            `json:"labeled_event_pair_count"`
	LabeledNonePairCount       int            `json:"labeled_none_pair_count"`
	LowQualityAlignmentCount   int            `json:"low_quality_alignment_count"`
	LowChatVolumeCount         int            `json:"low_chat_volume_count"`
	ShortTranscriptCount       int            `json:"short_transcript_count"`
	LowTranscriptConfidence    int            `json:"low_transcript_confidence_count"`
	EmptyTranscriptCount       int            `json:"empty_transcript_count"`
	MissingTranscriptSentiment int            `json:"missing_transcript_sentiment_count"`
	PartialOverlapCount        int            `json:"partial_overlap_count"`
	AverageAlignmentQuality    *float64       `json:"average_alignment_quality"`
	BaselineExclusionCounts    map[string]int `json:"baseline_exclusion_counts,omitempty"`
}

type ManualValidationRecord struct {
	SampleCohort         string            `json:"sample_cohort"`
	SessionID            string            `json:"session_id"`
	ChannelID            string            `json:"channel_id,omitempty"`
	WindowStart          time.Time         `json:"window_start"`
	WindowEnd            time.Time         `json:"window_end"`
	ChatSentiment        float64           `json:"chat_sentiment"`
	TranscriptSentiment  float64           `json:"transcript_sentiment"`
	Delta                float64           `json:"delta"`
	Relationship         string            `json:"relationship"`
	AlignmentQuality     float64           `json:"alignment_quality"`
	ChatMessageCount     int               `json:"chat_message_count"`
	TranscriptTextLength int               `json:"transcript_text_length"`
	ExistingLabel        string            `json:"existing_label,omitempty"`
	ReviewStatus         string            `json:"review_status"`
	HumanLabel           string            `json:"human_label"`
	ObservableCause      string            `json:"observable_cause"`
	FalsePositiveReason  string            `json:"false_positive_reason"`
	Evidence             []EvidenceSnippet `json:"evidence,omitempty"`
}

type EvidenceSnippet struct {
	Source string         `json:"source"`
	Text   string         `json:"text"`
	Meta   map[string]any `json:"meta,omitempty"`
}

type sentimentPair struct {
	SessionID             string
	ChannelID             string
	WindowStart           time.Time
	WindowEnd             time.Time
	ChatSentiment         float64
	TranscriptSentiment   float64
	Delta                 float64
	Relationship          string
	Quality               float64
	ChatMessageCount      int
	TranscriptTextLength  int
	TranscriptConfidence  float64
	OverlapSeconds        int
	QualityFlags          []string
	Label                 string
	DetectedDivergence    bool
	CandidateCalmBaseline bool
	Evidence              []EvidenceSnippet
}

type lagPair struct {
	ChatSentiment       float64
	TranscriptSentiment float64
	Delta               float64
}

func (r Runner) Run(ctx context.Context, config Config) (Report, error) {
	if r.Store == nil {
		return Report{}, fmt.Errorf("replay store is required")
	}
	config = normalizeConfig(config)
	sessionIDs := normalizeSessionIDs(config.SessionIDs)
	if len(sessionIDs) == 0 {
		return Report{}, fmt.Errorf("at least one session ID is required")
	}

	replays := make([]storage.SessionReplay, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		replay, err := r.Store.GetSessionReplay(ctx, sessionID, config.ReplayLimit)
		if err != nil {
			return Report{}, fmt.Errorf("load replay for session %q: %w", sessionID, err)
		}
		replays = append(replays, replay)
	}
	return BuildReport(replays, config), nil
}

func BuildReport(replays []storage.SessionReplay, config Config) Report {
	config = normalizeConfig(config)
	allPairs := []sentimentPair{}
	sessions := make([]SessionResult, 0, len(replays))
	for _, replay := range replays {
		result, pairs := BuildSessionResult(replay, config)
		sessions = append(sessions, result)
		allPairs = append(allPairs, pairs...)
	}

	aggregate := summarizeCohorts(allPairs, config)
	report := Report{
		Type:        ReportType,
		GeneratedAt: config.GeneratedAt,
		Scope:       "Aligned chat/transcript sentiment pairs from stored session replay data; correlations are descriptive evidence, not ground-truth causality.",
		ReplayLimit: config.ReplayLimit,
		CohortConfig: CohortConfig{
			MinimumPairs:               config.MinimumPairs,
			DivergenceDeltaThreshold:   roundMetric(config.DivergenceDeltaThreshold),
			MinimumCalmQuality:         roundMetric(config.MinimumCalmQuality),
			MinimumCalmChatMessages:    config.MinimumCalmChatMessages,
			MinimumCalmTranscriptChars: config.MinimumCalmTranscriptChars,
			LagOffsetsSeconds:          lagOffsetSeconds(config.LagOffsets),
			ManualSamplePerCohort:      config.ManualSamplePerCohort,
			ManualSampleCohorts:        []string{"detected_divergence", "labeled_event", "stable_baseline"},
		},
		DataQuality:            summarizeDataQuality(replays, allPairs, config),
		Aggregate:              aggregate,
		Comparison:             compareCohorts(aggregate),
		Calibration:            calibrateDeltaThreshold(allPairs, config),
		LagAnalysis:            summarizeLagAnalysis(replays, config),
		NegativeControl:        buildNegativeControl(allPairs, config.MinimumPairs),
		ManualValidationSample: BuildManualValidationSample(allPairs, config.ManualSamplePerCohort),
		Sessions:               sessions,
	}
	report.BestLag = bestLag(report.LagAnalysis)
	report.Limitations = reportLimitations(allPairs, aggregate, report.Calibration, report.NegativeControl, config)
	return report
}

func BuildSessionResult(replay storage.SessionReplay, config Config) (SessionResult, []sentimentPair) {
	config = normalizeConfig(config)
	pairs := buildPairs(replay, config)
	result := SessionResult{
		SessionID:       replay.Session.SessionID,
		ChannelID:       replay.Session.ChannelID,
		PairCount:       len(pairs),
		DataQuality:     summarizeDataQuality([]storage.SessionReplay{replay}, pairs, config),
		Cohorts:         summarizeCohorts(pairs, config),
		Calibration:     calibrateDeltaThreshold(pairs, config),
		LagAnalysis:     summarizeLagAnalysis([]storage.SessionReplay{replay}, config),
		NegativeControl: buildNegativeControl(pairs, config.MinimumPairs),
	}
	if len(pairs) == 0 {
		result.Limitations = append(result.Limitations, "No alignment buckets with chat and transcript sentiment were available.")
	}
	if len(pairs) > 0 && len(pairs) < config.MinimumPairs {
		result.Limitations = append(result.Limitations, "Fewer aligned pairs than the configured minimum for correlation.")
	}
	return result, pairs
}

func BuildManualValidationSample(pairs []sentimentPair, perCohort int) []ManualValidationRecord {
	if perCohort <= 0 {
		return nil
	}
	cohorts := []struct {
		name   string
		filter func(sentimentPair) bool
	}{
		{name: "detected_divergence", filter: func(pair sentimentPair) bool { return pair.DetectedDivergence }},
		{name: "labeled_event", filter: func(pair sentimentPair) bool { return pair.Label != "" && pair.Label != "none" }},
		{name: "stable_baseline", filter: func(pair sentimentPair) bool { return pair.CandidateCalmBaseline }},
	}

	out := []ManualValidationRecord{}
	for _, cohort := range cohorts {
		selected := make([]sentimentPair, 0, perCohort)
		for _, pair := range pairs {
			if cohort.filter(pair) {
				selected = append(selected, pair)
			}
		}
		sort.SliceStable(selected, func(left, right int) bool {
			leftScore := math.Abs(selected[left].Delta)
			rightScore := math.Abs(selected[right].Delta)
			if cohort.name == "stable_baseline" {
				if selected[left].Quality == selected[right].Quality {
					return selected[left].WindowStart.Before(selected[right].WindowStart)
				}
				return selected[left].Quality > selected[right].Quality
			}
			if leftScore == rightScore {
				return selected[left].WindowStart.Before(selected[right].WindowStart)
			}
			return leftScore > rightScore
		})
		for index, pair := range selected {
			if index >= perCohort {
				break
			}
			out = append(out, manualValidationRecord(cohort.name, pair))
		}
	}
	return out
}

func manualValidationRecord(cohort string, pair sentimentPair) ManualValidationRecord {
	return ManualValidationRecord{
		SampleCohort:         cohort,
		SessionID:            pair.SessionID,
		ChannelID:            pair.ChannelID,
		WindowStart:          pair.WindowStart,
		WindowEnd:            pair.WindowEnd,
		ChatSentiment:        roundMetric(pair.ChatSentiment),
		TranscriptSentiment:  roundMetric(pair.TranscriptSentiment),
		Delta:                roundMetric(pair.Delta),
		Relationship:         pair.Relationship,
		AlignmentQuality:     roundMetric(pair.Quality),
		ChatMessageCount:     pair.ChatMessageCount,
		TranscriptTextLength: pair.TranscriptTextLength,
		ExistingLabel:        pair.Label,
		ReviewStatus:         "needs_human_review",
		HumanLabel:           "",
		ObservableCause:      "",
		FalsePositiveReason:  "",
		Evidence:             pair.Evidence,
	}
}

func buildPairs(replay storage.SessionReplay, config Config) []sentimentPair {
	chatByKey := chatBucketsByKey(replay.ChatBuckets)
	transcriptByKey := transcriptBucketsByKey(replay.TranscriptBuckets)
	pairs := make([]sentimentPair, 0, len(replay.Alignments))
	for _, alignment := range replay.Alignments {
		label := labelForAlignment(alignment, replay.WindowLabels)
		relationship := normalizedDimension(alignment.Relationship)
		detectedDivergence := relationship == "diverged" || math.Abs(alignment.Delta) >= config.DivergenceDeltaThreshold
		chatBucket := chatByKey[bucketKey(alignment.SessionID, alignment.ChannelID, alignment.ChatBucketStart, alignment.ChatBucketEnd)]
		transcriptBucket := transcriptByKey[bucketKey(alignment.SessionID, alignment.ChannelID, alignment.TranscriptBucketStart, alignment.TranscriptBucketEnd)]
		pair := sentimentPair{
			SessionID:            firstNonEmpty(alignment.SessionID, replay.Session.SessionID),
			ChannelID:            firstNonEmpty(alignment.ChannelID, replay.Session.ChannelID),
			WindowStart:          alignment.WindowStart,
			WindowEnd:            alignment.WindowEnd,
			ChatSentiment:        alignment.ChatSentiment,
			TranscriptSentiment:  alignment.TranscriptSentiment,
			Delta:                alignment.Delta,
			Relationship:         relationship,
			Quality:              alignment.Quality,
			ChatMessageCount:     alignment.ChatMessageCount,
			TranscriptTextLength: alignment.TranscriptTextLength,
			TranscriptConfidence: alignment.TranscriptConfidence,
			OverlapSeconds:       alignment.OverlapSeconds,
			QualityFlags:         append([]string(nil), alignment.QualityFlags...),
			Label:                label,
			DetectedDivergence:   detectedDivergence,
			Evidence:             evidenceForPair(chatBucket, transcriptBucket, alignment),
		}
		pair.CandidateCalmBaseline = isCalmBaselinePair(pair, config)
		pairs = append(pairs, pair)
	}
	sortPairs(pairs)
	return pairs
}

func labelForAlignment(alignment storage.AlignmentBucket, labels []storage.SignalWindowLabel) string {
	best := ""
	for _, label := range labels {
		if label.WindowStart.IsZero() || label.WindowEnd.IsZero() {
			continue
		}
		if !rangesOverlap(alignment.WindowStart, alignment.WindowEnd, label.WindowStart, label.WindowEnd) {
			continue
		}
		eventLabel := normalizedDimension(label.EventLabel)
		if eventLabel == "" {
			continue
		}
		if eventLabel != "none" {
			return eventLabel
		}
		best = "none"
	}
	return best
}

func isCalmBaselinePair(pair sentimentPair, config Config) bool {
	if pair.DetectedDivergence || (pair.Label != "" && pair.Label != "none") {
		return false
	}
	if pair.Quality < config.MinimumCalmQuality {
		return false
	}
	if pair.ChatMessageCount < config.MinimumCalmChatMessages {
		return false
	}
	if pair.TranscriptTextLength < config.MinimumCalmTranscriptChars {
		return false
	}
	return true
}

func summarizeCohorts(pairs []sentimentPair, config Config) []CohortSummary {
	cohorts := []struct {
		name        string
		description string
		filter      func(sentimentPair) bool
	}{
		{
			name:        CohortAllAligned,
			description: "Every persisted alignment pair with chat and transcript sentiment.",
			filter:      func(sentimentPair) bool { return true },
		},
		{
			name:        CohortCalmBaseline,
			description: "High-quality, non-divergent windows with no overlapping non-none label.",
			filter:      func(pair sentimentPair) bool { return pair.CandidateCalmBaseline },
		},
		{
			name:        CohortDetectedDivergence,
			description: "Windows where the detector marks chat/transcript sentiment as diverged.",
			filter:      func(pair sentimentPair) bool { return pair.DetectedDivergence },
		},
		{
			name:        CohortLabeledEvents,
			description: "Windows overlapping a non-none manual or reviewed signal-window label.",
			filter:      func(pair sentimentPair) bool { return pair.Label != "" && pair.Label != "none" },
		},
		{
			name:        CohortLabeledNone,
			description: "Windows overlapping an explicit none label.",
			filter:      func(pair sentimentPair) bool { return pair.Label == "none" },
		},
	}

	out := make([]CohortSummary, 0, len(cohorts))
	for _, cohort := range cohorts {
		selected := make([]sentimentPair, 0, len(pairs))
		for _, pair := range pairs {
			if cohort.filter(pair) {
				selected = append(selected, pair)
			}
		}
		out = append(out, summarizePairCohort(cohort.name, cohort.description, selected, config.MinimumPairs))
	}
	return out
}

func summarizePairCohort(name, description string, pairs []sentimentPair, minimumPairs int) CohortSummary {
	summary := CohortSummary{
		Name:              name,
		Description:       description,
		PairCount:         len(pairs),
		CorrelationStatus: "insufficient_pairs",
	}
	if len(pairs) == 0 {
		return summary
	}

	chatScores := make([]float64, 0, len(pairs))
	transcriptScores := make([]float64, 0, len(pairs))
	absDeltas := make([]float64, 0, len(pairs))
	relationshipCounts := map[string]int{}
	labelCounts := map[string]int{}
	var chatTotal, transcriptTotal, deltaTotal, absDeltaTotal, qualityTotal float64
	for _, pair := range pairs {
		chatScores = append(chatScores, pair.ChatSentiment)
		transcriptScores = append(transcriptScores, pair.TranscriptSentiment)
		absDelta := math.Abs(pair.Delta)
		absDeltas = append(absDeltas, absDelta)
		chatTotal += pair.ChatSentiment
		transcriptTotal += pair.TranscriptSentiment
		deltaTotal += pair.Delta
		absDeltaTotal += absDelta
		qualityTotal += pair.Quality
		if pair.Relationship != "" {
			relationshipCounts[pair.Relationship]++
		}
		if pair.Label != "" {
			labelCounts[pair.Label]++
		}
	}
	summary.MeanChatSentiment = floatPtr(roundMetric(chatTotal / float64(len(pairs))))
	summary.MeanTranscriptSentiment = floatPtr(roundMetric(transcriptTotal / float64(len(pairs))))
	summary.AverageDelta = floatPtr(roundMetric(deltaTotal / float64(len(pairs))))
	summary.AverageAbsDelta = floatPtr(roundMetric(absDeltaTotal / float64(len(pairs))))
	summary.AbsDeltaMedian = percentilePtr(absDeltas, 0.50)
	summary.AbsDeltaP90 = percentilePtr(absDeltas, 0.90)
	summary.AbsDeltaP95 = percentilePtr(absDeltas, 0.95)
	summary.AverageQuality = floatPtr(roundMetric(qualityTotal / float64(len(pairs))))
	summary.RelationshipCounts = nonEmptyMap(relationshipCounts)
	summary.LabelCounts = nonEmptyMap(labelCounts)

	if len(pairs) < minimumPairs {
		return summary
	}
	pearson, pearsonOK := pearsonCorrelation(chatScores, transcriptScores)
	spearman, spearmanOK := spearmanCorrelation(chatScores, transcriptScores)
	switch {
	case pearsonOK && spearmanOK:
		summary.CorrelationStatus = "computed"
		summary.Pearson = floatPtr(roundMetric(pearson))
		summary.Spearman = floatPtr(roundMetric(spearman))
	case pearsonOK:
		summary.CorrelationStatus = "spearman_undefined"
		summary.Pearson = floatPtr(roundMetric(pearson))
	case spearmanOK:
		summary.CorrelationStatus = "pearson_undefined"
		summary.Spearman = floatPtr(roundMetric(spearman))
	default:
		summary.CorrelationStatus = "constant_series"
	}
	return summary
}

func compareCohorts(cohorts []CohortSummary) CorrelationComparison {
	comparison := CorrelationComparison{
		BaselineCohort: CohortCalmBaseline,
		EventCohort:    CohortDetectedDivergence,
		Status:         "insufficient_correlation_pairs",
	}
	baseline, baselineOK := findCohort(cohorts, CohortCalmBaseline)
	events, eventsOK := findCohort(cohorts, CohortDetectedDivergence)
	if !baselineOK || !eventsOK {
		return comparison
	}
	if baseline.Pearson != nil && events.Pearson != nil {
		comparison.PearsonDrop = floatPtr(roundMetric(*baseline.Pearson - *events.Pearson))
	}
	if baseline.Spearman != nil && events.Spearman != nil {
		comparison.SpearmanDrop = floatPtr(roundMetric(*baseline.Spearman - *events.Spearman))
	}
	if baseline.AverageAbsDelta != nil && events.AverageAbsDelta != nil {
		comparison.AverageAbsDeltaChange = floatPtr(roundMetric(*events.AverageAbsDelta - *baseline.AverageAbsDelta))
	}
	if comparison.PearsonDrop != nil || comparison.SpearmanDrop != nil || comparison.AverageAbsDeltaChange != nil {
		comparison.Status = "computed"
	}
	return comparison
}

func calibrateDeltaThreshold(pairs []sentimentPair, config Config) DeltaCalibration {
	baseline := filterPairs(pairs, func(pair sentimentPair) bool { return pair.CandidateCalmBaseline })
	events := filterPairs(pairs, func(pair sentimentPair) bool { return pair.DetectedDivergence })
	calibration := DeltaCalibration{
		Status:                      "insufficient_baseline_pairs",
		BaselinePairCount:           len(baseline),
		DetectedDivergencePairCount: len(events),
		CurrentThreshold:            roundMetric(config.DivergenceDeltaThreshold),
		ThresholdAssessment:         "insufficient_baseline_pairs",
	}
	if len(baseline) < config.MinimumPairs {
		return calibration
	}

	absDeltas := make([]float64, 0, len(baseline))
	for _, pair := range baseline {
		absDeltas = append(absDeltas, math.Abs(pair.Delta))
	}
	median := percentile(absDeltas, 0.50)
	p90 := percentile(absDeltas, 0.90)
	p95 := percentile(absDeltas, 0.95)
	calibration.Status = "computed"
	calibration.BaselineAbsDeltaMedian = floatPtr(roundMetric(median))
	calibration.BaselineAbsDeltaP90 = floatPtr(roundMetric(p90))
	calibration.BaselineAbsDeltaP95 = floatPtr(roundMetric(p95))
	calibration.RecommendedThreshold = floatPtr(roundMetric(p95))

	for _, pair := range events {
		if math.Abs(pair.Delta) > p95 {
			calibration.DetectedAboveBaselineP95++
		}
	}
	if len(events) > 0 {
		rate := float64(calibration.DetectedAboveBaselineP95) / float64(len(events))
		calibration.DetectedAboveBaselineP95Rate = floatPtr(roundMetric(rate))
	}
	switch {
	case config.DivergenceDeltaThreshold < p90:
		calibration.ThresholdAssessment = "current_threshold_below_baseline_p90"
	case config.DivergenceDeltaThreshold > p95*1.5 && p95 > 0:
		calibration.ThresholdAssessment = "current_threshold_much_higher_than_baseline_p95"
	default:
		calibration.ThresholdAssessment = "current_threshold_near_baseline_tail"
	}
	return calibration
}

func summarizeLagAnalysis(replays []storage.SessionReplay, config Config) []LagSummary {
	out := make([]LagSummary, 0, len(config.LagOffsets))
	for _, lag := range config.LagOffsets {
		pairs := []lagPair{}
		for _, replay := range replays {
			pairs = append(pairs, lagPairsForReplay(replay, lag)...)
		}
		out = append(out, summarizeLagPairs(lag, pairs, config.MinimumPairs))
	}
	return out
}

func summarizeLagPairs(lag time.Duration, pairs []lagPair, minimumPairs int) LagSummary {
	summary := LagSummary{
		LagSeconds:        int(lag.Seconds()),
		Description:       lagDescription(lag),
		PairCount:         len(pairs),
		CorrelationStatus: "insufficient_pairs",
	}
	if len(pairs) == 0 {
		return summary
	}
	chatScores := make([]float64, 0, len(pairs))
	transcriptScores := make([]float64, 0, len(pairs))
	var absDeltaTotal float64
	for _, pair := range pairs {
		chatScores = append(chatScores, pair.ChatSentiment)
		transcriptScores = append(transcriptScores, pair.TranscriptSentiment)
		absDelta := math.Abs(pair.Delta)
		absDeltaTotal += absDelta
	}
	summary.AverageAbsDelta = floatPtr(roundMetric(absDeltaTotal / float64(len(pairs))))
	if len(pairs) < minimumPairs {
		return summary
	}
	pearson, pearsonOK := pearsonCorrelation(chatScores, transcriptScores)
	spearman, spearmanOK := spearmanCorrelation(chatScores, transcriptScores)
	switch {
	case pearsonOK && spearmanOK:
		summary.CorrelationStatus = "computed"
		summary.Pearson = floatPtr(roundMetric(pearson))
		summary.Spearman = floatPtr(roundMetric(spearman))
	case pearsonOK:
		summary.CorrelationStatus = "spearman_undefined"
		summary.Pearson = floatPtr(roundMetric(pearson))
	case spearmanOK:
		summary.CorrelationStatus = "pearson_undefined"
		summary.Spearman = floatPtr(roundMetric(spearman))
	default:
		summary.CorrelationStatus = "constant_series"
	}
	return summary
}

func lagPairsForReplay(replay storage.SessionReplay, lag time.Duration) []lagPair {
	transcripts := append([]storage.TranscriptBucket(nil), replay.TranscriptBuckets...)
	sort.Slice(transcripts, func(left, right int) bool {
		return transcripts[left].BucketStart.Before(transcripts[right].BucketStart)
	})

	used := map[string]struct{}{}
	out := []lagPair{}
	for _, bucket := range replay.ChatBuckets {
		if bucket.BucketStart.IsZero() || bucket.BucketEnd.IsZero() {
			continue
		}
		targetStart := bucket.BucketStart.Add(lag)
		targetEnd := bucket.BucketEnd.Add(lag)
		transcript, overlap := bestTranscriptForShiftedWindow(bucket.SessionID, bucket.ChannelID, targetStart, targetEnd, transcripts, used)
		if transcript == nil || transcript.SentimentScore == nil {
			continue
		}
		window := bucket.BucketEnd.Sub(bucket.BucketStart)
		if window <= 0 {
			window = 30 * time.Second
		}
		if overlap < window/2 {
			continue
		}
		used[bucketKey(transcript.SessionID, transcript.ChannelID, transcript.BucketStart, transcript.BucketEnd)] = struct{}{}
		delta := bucket.ChatSentiment - *transcript.SentimentScore
		out = append(out, lagPair{
			ChatSentiment:       bucket.ChatSentiment,
			TranscriptSentiment: *transcript.SentimentScore,
			Delta:               delta,
		})
	}
	return out
}

func buildNegativeControl(pairs []sentimentPair, minimumPairs int) NegativeControlSummary {
	control := NegativeControlSummary{
		Status:    "insufficient_pairs",
		Method:    "Deterministically rotate transcript sentiment within each session before recomputing correlation.",
		PairCount: len(pairs),
	}
	if len(pairs) < minimumPairs {
		control.Interpretation = "Not enough aligned pairs to compare observed correlation against a shuffled control."
		return control
	}
	observedChat, observedTranscript := pairScores(pairs)
	if value, ok := pearsonCorrelation(observedChat, observedTranscript); ok {
		control.ObservedPearson = floatPtr(roundMetric(value))
	}
	if value, ok := spearmanCorrelation(observedChat, observedTranscript); ok {
		control.ObservedSpearman = floatPtr(roundMetric(value))
	}

	shuffled, effectiveShuffle := shuffledTranscriptScoresBySession(pairs)
	if !effectiveShuffle {
		control.Status = "insufficient_shuffle_pairs"
		control.Interpretation = "At least one session needs multiple aligned pairs for the within-session shuffled control to change the pairing."
		return control
	}
	if value, ok := pearsonCorrelation(observedChat, shuffled); ok {
		control.ShuffledPearson = floatPtr(roundMetric(value))
	}
	if value, ok := spearmanCorrelation(observedChat, shuffled); ok {
		control.ShuffledSpearman = floatPtr(roundMetric(value))
	}
	if control.ObservedPearson != nil && control.ShuffledPearson != nil {
		control.PearsonDrop = floatPtr(roundMetric(*control.ObservedPearson - *control.ShuffledPearson))
	}
	if control.ObservedSpearman != nil && control.ShuffledSpearman != nil {
		control.SpearmanDrop = floatPtr(roundMetric(*control.ObservedSpearman - *control.ShuffledSpearman))
	}
	if control.PearsonDrop == nil && control.SpearmanDrop == nil {
		control.Status = "constant_series"
		control.Interpretation = "Observed or shuffled series is constant, so the negative-control correlation is undefined."
		return control
	}
	control.Status = "computed"
	if control.PearsonDrop != nil && *control.PearsonDrop > 0 {
		control.Interpretation = "Observed pairing has stronger Pearson correlation than the shuffled control."
	} else {
		control.Interpretation = "Observed pairing does not exceed the shuffled control; treat cross-modal correlation claims cautiously."
	}
	return control
}

func summarizeDataQuality(replays []storage.SessionReplay, pairs []sentimentPair, config Config) DataQualitySummary {
	summary := DataQualitySummary{
		SessionCount:            len(replays),
		AlignmentPairCount:      len(pairs),
		BaselineExclusionCounts: map[string]int{},
	}
	var qualityTotal float64
	for _, replay := range replays {
		summary.ChatBucketCount += len(replay.ChatBuckets)
		summary.TranscriptBucketCount += len(replay.TranscriptBuckets)
		for _, transcript := range replay.TranscriptBuckets {
			if strings.TrimSpace(transcript.Text) == "" {
				summary.EmptyTranscriptCount++
			}
			if transcript.SentimentScore == nil {
				summary.MissingTranscriptSentiment++
			}
		}
	}
	for _, pair := range pairs {
		qualityTotal += pair.Quality
		if pair.CandidateCalmBaseline {
			summary.BaselinePairCount++
		}
		if pair.DetectedDivergence {
			summary.DetectedDivergenceCount++
			summary.BaselineExclusionCounts["detected_divergence"]++
		}
		if pair.Label != "" && pair.Label != "none" {
			summary.LabeledEventPairCount++
			summary.BaselineExclusionCounts["labeled_event"]++
		}
		if pair.Label == "none" {
			summary.LabeledNonePairCount++
		}
		if pair.Quality < config.MinimumCalmQuality {
			summary.LowQualityAlignmentCount++
			summary.BaselineExclusionCounts["low_quality"]++
		}
		if pair.ChatMessageCount < config.MinimumCalmChatMessages {
			summary.LowChatVolumeCount++
			summary.BaselineExclusionCounts["low_chat_volume"]++
		}
		if pair.TranscriptTextLength < config.MinimumCalmTranscriptChars {
			summary.ShortTranscriptCount++
			summary.BaselineExclusionCounts["short_transcript"]++
		}
		if pair.TranscriptConfidence < 0.7 {
			summary.LowTranscriptConfidence++
		}
		if hasQualityFlag(pair.QualityFlags, "partial_overlap") {
			summary.PartialOverlapCount++
		}
	}
	if len(pairs) > 0 {
		summary.AverageAlignmentQuality = floatPtr(roundMetric(qualityTotal / float64(len(pairs))))
	}
	summary.BaselineExclusionCounts = nonEmptyMap(summary.BaselineExclusionCounts)
	return summary
}

func reportLimitations(pairs []sentimentPair, cohorts []CohortSummary, calibration DeltaCalibration, control NegativeControlSummary, config Config) []string {
	var limitations []string
	if len(pairs) == 0 {
		return []string{"No aligned sentiment pairs were available, so no correlation can be computed."}
	}
	for _, cohort := range cohorts {
		if cohort.PairCount > 0 && cohort.PairCount < config.MinimumPairs {
			limitations = append(limitations, fmt.Sprintf("%s has %d pairs, below the configured minimum of %d.", cohort.Name, cohort.PairCount, config.MinimumPairs))
		}
	}
	if cohort, ok := findCohort(cohorts, CohortCalmBaseline); ok && cohort.PairCount == 0 {
		limitations = append(limitations, "No calm baseline pairs met the quality, chat-volume, transcript-length, and non-divergence filters.")
	}
	if cohort, ok := findCohort(cohorts, CohortDetectedDivergence); ok && cohort.PairCount == 0 {
		limitations = append(limitations, "No detected divergence pairs were present in the selected sessions.")
	}
	if calibration.Status != "computed" {
		limitations = append(limitations, "Baseline delta calibration is pending until enough calm baseline pairs are available.")
	}
	if control.Status != "computed" {
		limitations = append(limitations, "Negative-control correlation is pending until enough non-constant aligned pairs with multiple pairs per session are available.")
	}
	return limitations
}

func pearsonCorrelation(left, right []float64) (float64, bool) {
	if len(left) != len(right) || len(left) < 2 {
		return 0, false
	}
	var leftTotal, rightTotal float64
	for index := range left {
		leftTotal += left[index]
		rightTotal += right[index]
	}
	leftMean := leftTotal / float64(len(left))
	rightMean := rightTotal / float64(len(right))
	var numerator, leftSquares, rightSquares float64
	for index := range left {
		leftDelta := left[index] - leftMean
		rightDelta := right[index] - rightMean
		numerator += leftDelta * rightDelta
		leftSquares += leftDelta * leftDelta
		rightSquares += rightDelta * rightDelta
	}
	denominator := math.Sqrt(leftSquares * rightSquares)
	if denominator == 0 || math.IsNaN(denominator) || math.IsInf(denominator, 0) {
		return 0, false
	}
	value := numerator / denominator
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func spearmanCorrelation(left, right []float64) (float64, bool) {
	if len(left) != len(right) || len(left) < 2 {
		return 0, false
	}
	return pearsonCorrelation(averageRanks(left), averageRanks(right))
}

func averageRanks(values []float64) []float64 {
	type item struct {
		value float64
		index int
	}
	items := make([]item, 0, len(values))
	for index, value := range values {
		items = append(items, item{value: value, index: index})
	}
	sort.Slice(items, func(left, right int) bool {
		if items[left].value == items[right].value {
			return items[left].index < items[right].index
		}
		return items[left].value < items[right].value
	})

	ranks := make([]float64, len(values))
	for index := 0; index < len(items); {
		end := index + 1
		for end < len(items) && items[end].value == items[index].value {
			end++
		}
		rank := (float64(index+1) + float64(end)) / 2
		for rankIndex := index; rankIndex < end; rankIndex++ {
			ranks[items[rankIndex].index] = rank
		}
		index = end
	}
	return ranks
}

func percentile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	if quantile <= 0 {
		return sorted[0]
	}
	if quantile >= 1 {
		return sorted[len(sorted)-1]
	}
	position := quantile * float64(len(sorted)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return sorted[lower]
	}
	weight := position - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func percentilePtr(values []float64, quantile float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	return floatPtr(roundMetric(percentile(values, quantile)))
}

func findCohort(cohorts []CohortSummary, name string) (CohortSummary, bool) {
	for _, cohort := range cohorts {
		if cohort.Name == name {
			return cohort, true
		}
	}
	return CohortSummary{}, false
}

func filterPairs(pairs []sentimentPair, filter func(sentimentPair) bool) []sentimentPair {
	out := make([]sentimentPair, 0, len(pairs))
	for _, pair := range pairs {
		if filter(pair) {
			out = append(out, pair)
		}
	}
	return out
}

func bestLag(lags []LagSummary) *LagSummary {
	var best *LagSummary
	for index := range lags {
		if lags[index].Pearson == nil {
			continue
		}
		if best == nil || math.Abs(*lags[index].Pearson) > math.Abs(*best.Pearson) {
			item := lags[index]
			best = &item
		}
	}
	return best
}

func bestTranscriptForShiftedWindow(sessionID, channelID string, start, end time.Time, transcripts []storage.TranscriptBucket, used map[string]struct{}) (*storage.TranscriptBucket, time.Duration) {
	var best *storage.TranscriptBucket
	var bestOverlap time.Duration
	for index := range transcripts {
		transcript := &transcripts[index]
		if transcript.SessionID != sessionID || transcript.ChannelID != channelID {
			continue
		}
		key := bucketKey(transcript.SessionID, transcript.ChannelID, transcript.BucketStart, transcript.BucketEnd)
		if _, exists := used[key]; exists {
			continue
		}
		overlap := overlapDuration(start, end, transcript.BucketStart, transcript.BucketEnd)
		if overlap > bestOverlap {
			best = transcript
			bestOverlap = overlap
		}
	}
	return best, bestOverlap
}

func shuffledTranscriptScoresBySession(pairs []sentimentPair) ([]float64, bool) {
	out := make([]float64, len(pairs))
	indicesBySession := map[string][]int{}
	for index, pair := range pairs {
		indicesBySession[pair.SessionID] = append(indicesBySession[pair.SessionID], index)
	}
	effective := false
	for _, indices := range indicesBySession {
		sort.Slice(indices, func(left, right int) bool {
			return pairs[indices[left]].WindowStart.Before(pairs[indices[right]].WindowStart)
		})
		if len(indices) == 1 {
			out[indices[0]] = pairs[indices[0]].TranscriptSentiment
			continue
		}
		effective = true
		rotation := len(indices) / 2
		if rotation == 0 {
			rotation = 1
		}
		for offset, index := range indices {
			source := indices[(offset+rotation)%len(indices)]
			out[index] = pairs[source].TranscriptSentiment
		}
	}
	return out, effective
}

func pairScores(pairs []sentimentPair) ([]float64, []float64) {
	chatScores := make([]float64, 0, len(pairs))
	transcriptScores := make([]float64, 0, len(pairs))
	for _, pair := range pairs {
		chatScores = append(chatScores, pair.ChatSentiment)
		transcriptScores = append(transcriptScores, pair.TranscriptSentiment)
	}
	return chatScores, transcriptScores
}

func evidenceForPair(chatBucket *chat.ChatBucket, transcriptBucket *storage.TranscriptBucket, alignment storage.AlignmentBucket) []EvidenceSnippet {
	evidence := []EvidenceSnippet{{
		Source: "alignment",
		Text:   fmt.Sprintf("relationship=%s; delta=%.4f; quality=%.4f", alignment.Relationship, alignment.Delta, alignment.Quality),
		Meta: map[string]any{
			"window_start": alignment.WindowStart.Format(time.RFC3339),
			"window_end":   alignment.WindowEnd.Format(time.RFC3339),
		},
	}}
	if chatBucket != nil {
		for _, message := range chatEvidenceMessages(*chatBucket, 3) {
			evidence = append(evidence, EvidenceSnippet{
				Source: "chat",
				Text:   message.Text,
				Meta: map[string]any{
					"message_id":      message.MessageID,
					"sentiment_score": roundMetric(message.SentimentScore),
					"label":           message.Label,
				},
			})
		}
		if len(chatBucket.TopTerms) > 0 {
			terms := chatBucket.TopTerms
			if len(terms) > 5 {
				terms = terms[:5]
			}
			evidence = append(evidence, EvidenceSnippet{
				Source: "chat_terms",
				Text:   strings.Join(terms, ", "),
			})
		}
	}
	if transcriptBucket != nil {
		evidence = append(evidence, EvidenceSnippet{
			Source: "transcript",
			Text:   strings.TrimSpace(transcriptBucket.Text),
			Meta: map[string]any{
				"sentiment_score":       transcriptBucket.SentimentScore,
				"transcript_confidence": roundMetric(transcriptBucket.TranscriptConfidence),
				"word_count":            transcriptBucket.WordCount,
				"status":                transcriptBucket.TranscriptStatus,
			},
		})
	}
	return evidence
}

func chatEvidenceMessages(bucket chat.ChatBucket, limit int) []chat.MessageScore {
	scores := append([]chat.MessageScore(nil), bucket.MessageScores...)
	sort.SliceStable(scores, func(left, right int) bool {
		leftMagnitude := math.Abs(scores[left].SentimentScore)
		rightMagnitude := math.Abs(scores[right].SentimentScore)
		if leftMagnitude == rightMagnitude {
			return scores[left].Timestamp.Before(scores[right].Timestamp)
		}
		return leftMagnitude > rightMagnitude
	})
	if len(scores) > limit {
		return scores[:limit]
	}
	return scores
}

func chatBucketsByKey(buckets []chat.ChatBucket) map[string]*chat.ChatBucket {
	out := map[string]*chat.ChatBucket{}
	for index := range buckets {
		bucket := &buckets[index]
		out[bucketKey(bucket.SessionID, bucket.ChannelID, bucket.BucketStart, bucket.BucketEnd)] = bucket
	}
	return out
}

func transcriptBucketsByKey(buckets []storage.TranscriptBucket) map[string]*storage.TranscriptBucket {
	out := map[string]*storage.TranscriptBucket{}
	for index := range buckets {
		bucket := &buckets[index]
		out[bucketKey(bucket.SessionID, bucket.ChannelID, bucket.BucketStart, bucket.BucketEnd)] = bucket
	}
	return out
}

func lagOffsetSeconds(offsets []time.Duration) []int {
	out := make([]int, 0, len(offsets))
	for _, offset := range offsets {
		out = append(out, int(offset.Seconds()))
	}
	return out
}

func lagDescription(lag time.Duration) string {
	switch {
	case lag < 0:
		return fmt.Sprintf("chat bucket compared with transcript %d seconds earlier", int(math.Abs(lag.Seconds())))
	case lag > 0:
		return fmt.Sprintf("chat bucket compared with transcript %d seconds later", int(lag.Seconds()))
	default:
		return "chat bucket compared with the same transcript window"
	}
}

func rangesOverlap(leftStart, leftEnd, rightStart, rightEnd time.Time) bool {
	if leftStart.IsZero() || leftEnd.IsZero() || rightStart.IsZero() || rightEnd.IsZero() {
		return false
	}
	return leftEnd.After(rightStart) && rightEnd.After(leftStart)
}

func overlapDuration(leftStart, leftEnd, rightStart, rightEnd time.Time) time.Duration {
	start := maxTime(leftStart, rightStart)
	end := minTime(leftEnd, rightEnd)
	if !end.After(start) {
		return 0
	}
	return end.Sub(start)
}

func bucketKey(sessionID, channelID string, start, end time.Time) string {
	return sessionID + ":" + channelID + ":" + start.Format(time.RFC3339Nano) + ":" + end.Format(time.RFC3339Nano)
}

func hasQualityFlag(flags []string, expected string) bool {
	for _, flag := range flags {
		if flag == expected {
			return true
		}
	}
	return false
}

func sortPairs(pairs []sentimentPair) {
	sort.SliceStable(pairs, func(left, right int) bool {
		if pairs[left].SessionID == pairs[right].SessionID {
			return pairs[left].WindowStart.Before(pairs[right].WindowStart)
		}
		return pairs[left].SessionID < pairs[right].SessionID
	})
}

func normalizeConfig(config Config) Config {
	if config.GeneratedAt.IsZero() {
		config.GeneratedAt = time.Now().UTC()
	} else {
		config.GeneratedAt = config.GeneratedAt.UTC()
	}
	if config.ReplayLimit <= 0 || config.ReplayLimit > DefaultReplayLimit {
		config.ReplayLimit = DefaultReplayLimit
	}
	if config.MinimumPairs <= 0 {
		config.MinimumPairs = DefaultMinimumPairs
	}
	if config.DivergenceDeltaThreshold <= 0 {
		config.DivergenceDeltaThreshold = DefaultDivergenceDeltaThreshold
	}
	if config.MinimumCalmQuality <= 0 {
		config.MinimumCalmQuality = DefaultMinimumCalmQuality
	}
	if config.MinimumCalmChatMessages <= 0 {
		config.MinimumCalmChatMessages = DefaultMinimumCalmChatMessages
	}
	if config.MinimumCalmTranscriptChars <= 0 {
		config.MinimumCalmTranscriptChars = DefaultMinimumCalmTranscriptLen
	}
	if config.ManualSamplePerCohort < 0 {
		config.ManualSamplePerCohort = 0
	}
	if config.ManualSamplePerCohort == 0 {
		config.ManualSamplePerCohort = DefaultManualSamplePerCohort
	}
	if len(config.LagOffsets) == 0 {
		config.LagOffsets = append([]time.Duration(nil), defaultLagOffsets...)
	}
	sort.Slice(config.LagOffsets, func(left, right int) bool {
		return config.LagOffsets[left] < config.LagOffsets[right]
	})
	return config
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

func normalizedDimension(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func roundMetric(value float64) float64 {
	return math.Round(value*10000) / 10000
}

func floatPtr(value float64) *float64 {
	return &value
}

func nonEmptyMap(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	return values
}

func maxTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
