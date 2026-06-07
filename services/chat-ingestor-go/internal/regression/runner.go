package regression

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/analysis"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/bucket"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

const ReportType = "replay_regression_report"

type ReplayStore interface {
	GetSessionReplay(context.Context, string, int) (storage.SessionReplay, error)
}

type Config struct {
	SessionIDs      []string
	ReplayLimit     int
	Speeds          []float64
	AlignmentWindow time.Duration
	GeneratedAt     time.Time
}

type Runner struct {
	Store ReplayStore
}

type Report struct {
	Type            string          `json:"type"`
	GeneratedAt     time.Time       `json:"generated_at"`
	ReplayLimit     int             `json:"replay_limit"`
	AlignmentWindow string          `json:"alignment_window"`
	Speeds          []float64       `json:"speeds,omitempty"`
	Sessions        []SessionResult `json:"sessions"`
	Comparison      *Comparison     `json:"comparison,omitempty"`
}

type SessionResult struct {
	SessionID        string                          `json:"session_id"`
	ChannelID        string                          `json:"channel_id,omitempty"`
	Session          storage.SessionHistory          `json:"session"`
	Partial          bool                            `json:"partial"`
	TruncatedSources []storage.ReplayProofTruncation `json:"truncated_sources,omitempty"`
	ReplayCounts     ReplayCounts                    `json:"replay_counts"`
	Proof            storage.ReplayProof             `json:"proof"`
	Evaluation       analysis.SessionEvaluation      `json:"evaluation"`
	Metrics          MetricSummary                   `json:"metrics"`
}

type ReplayCounts struct {
	ChatBuckets       int `json:"chat_buckets"`
	TranscriptBuckets int `json:"transcript_buckets"`
	Alignments        int `json:"alignments"`
	SignalWindows     int `json:"signal_windows"`
	SignalEvents      int `json:"signal_events"`
}

type MetricSummary struct {
	Partial                        bool                                   `json:"partial"`
	TruncatedSourceCount           int                                    `json:"truncated_source_count"`
	ProofLabelCoverage             *float64                               `json:"proof_label_coverage"`
	EvaluationCoverage             *float64                               `json:"evaluation_coverage"`
	EventAccuracy                  *float64                               `json:"event_accuracy"`
	EventPrecision                 *float64                               `json:"event_precision"`
	EventRecall                    *float64                               `json:"event_recall"`
	EventF1                        *float64                               `json:"event_f1"`
	PeakRecall                     *float64                               `json:"peak_recall"`
	HypePeakRecall                 *float64                               `json:"hype_peak_recall"`
	OnsetLatencyMS                 *float64                               `json:"onset_latency_ms"`
	ReactionTypeAccuracy           *float64                               `json:"reaction_type_accuracy"`
	ReactionTypeF1                 *float64                               `json:"reaction_type_f1"`
	TargetAccuracy                 *float64                               `json:"target_accuracy"`
	TargetExtractionAccuracy       *float64                               `json:"target_extraction_accuracy"`
	DivergenceAccuracy             *float64                               `json:"divergence_accuracy"`
	FalsePositivesNormalChat       int                                    `json:"false_positives_normal_chat"`
	TotalWindows                   int                                    `json:"total_windows"`
	TotalLabeledWindows            int                                    `json:"total_labeled_windows"`
	EvaluatedWindows               int                                    `json:"evaluated_windows"`
	UnmatchedLabels                int                                    `json:"unmatched_labels"`
	UncertainLabels                int                                    `json:"uncertain_labels"`
	InvalidLabels                  int                                    `json:"invalid_labels"`
	ReactionWindowCount            int                                    `json:"reaction_window_count"`
	PeakReactionCount              int                                    `json:"peak_reaction_count"`
	TranscriptBucketCoverage       *float64                               `json:"transcript_bucket_coverage"`
	TranscriptCoverage             *float64                               `json:"transcript_coverage"`
	TranscriptAudioCoverage        *float64                               `json:"transcript_audio_coverage"`
	TranscriptEmptyRatio           *float64                               `json:"transcript_empty_ratio"`
	TranscriptWordCount            int                                    `json:"transcript_word_count"`
	TranscriptRepairAddedWords     int                                    `json:"transcript_repair_added_words"`
	TranscriptRepairChangedRatio   *float64                               `json:"transcript_repair_changed_ratio"`
	TranscriptRepairImprovement    *float64                               `json:"transcript_repair_improvement"`
	AlignmentCount                 int                                    `json:"alignment_count"`
	MatchedWindowCount             int                                    `json:"matched_window_count"`
	PersistenceMetricCount         int                                    `json:"persistence_metric_count"`
	StorageWriteFailures           *float64                               `json:"storage_write_failures"`
	StorageDroppedWrites           *float64                               `json:"storage_dropped_writes"`
	StorageQueueDepth              *float64                               `json:"storage_queue_depth"`
	ReactionWindowMetricCount      *float64                               `json:"reaction_window_metric_count"`
	LatencyAverageMS               map[string]*float64                    `json:"latency_average_ms,omitempty"`
	LatencyP50MS                   map[string]*float64                    `json:"latency_p50_ms,omitempty"`
	LatencyP95MS                   map[string]*float64                    `json:"latency_p95_ms,omitempty"`
	LatencyAvailableCounts         map[string]int                         `json:"latency_available_counts,omitempty"`
	LatencyMissingCounts           map[string]int                         `json:"latency_missing_counts,omitempty"`
	ChatAnalysisStatusCounts       map[string]int                         `json:"chat_analysis_status_counts,omitempty"`
	TranscriptStatusCounts         map[string]int                         `json:"transcript_sentiment_status_counts,omitempty"`
	TranscriptCoverageStatusCounts map[string]int                         `json:"transcript_status_counts,omitempty"`
	UnsupportedProofMetrics        []storage.ReplayProofUnsupported       `json:"unsupported_proof_metrics,omitempty"`
	UnsupportedEvaluationMetrics   []analysis.EvaluationUnsupportedMetric `json:"unsupported_evaluation_metrics,omitempty"`
}

func (r Runner) Run(ctx context.Context, config Config) (Report, error) {
	if r.Store == nil {
		return Report{}, fmt.Errorf("replay store is required")
	}
	generatedAt := config.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	} else {
		generatedAt = generatedAt.UTC()
	}
	limit := NormalizeReplayLimit(config.ReplayLimit)
	window := config.AlignmentWindow
	if window <= 0 {
		window = bucket.DefaultWindow
	}

	sessionIDs := NormalizeSessionIDs(config.SessionIDs)
	if len(sessionIDs) == 0 {
		return Report{}, fmt.Errorf("at least one session ID is required")
	}

	report := Report{
		Type:            ReportType,
		GeneratedAt:     generatedAt,
		ReplayLimit:     limit,
		AlignmentWindow: window.String(),
		Speeds:          normalizedSpeeds(config.Speeds),
		Sessions:        make([]SessionResult, 0, len(sessionIDs)),
	}
	for _, sessionID := range sessionIDs {
		replay, err := r.Store.GetSessionReplay(ctx, sessionID, limit)
		if err != nil {
			return Report{}, fmt.Errorf("load replay for session %q: %w", sessionID, err)
		}
		result := BuildSessionResult(replay, SessionResultOptions{
			GeneratedAt:     generatedAt,
			ReplayLimit:     limit,
			Speeds:          config.Speeds,
			AlignmentWindow: window,
		})
		report.Sessions = append(report.Sessions, result)
	}
	return report, nil
}

type SessionResultOptions struct {
	GeneratedAt     time.Time
	ReplayLimit     int
	Speeds          []float64
	AlignmentWindow time.Duration
}

func BuildSessionResult(replay storage.SessionReplay, opts SessionResultOptions) SessionResult {
	generatedAt := opts.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	} else {
		generatedAt = generatedAt.UTC()
	}
	window := opts.AlignmentWindow
	if window <= 0 {
		window = bucket.DefaultWindow
	}

	analyzer := analysis.NewAnalyzer(analysis.AnalyzerConfig{AlignmentWindow: window})
	analysisResult := analyzer.AnalyzeSession(analysis.SessionAnalysisInput{
		SessionID:         replay.Session.SessionID,
		ChatBuckets:       replay.ChatBuckets,
		TranscriptBuckets: storageTranscriptBuckets(replay.TranscriptBuckets),
		Alignments:        storageAlignments(replay.Alignments),
	})
	proof := storage.BuildReplayProof(replay, storage.ReplayProofOptions{
		GeneratedAt: generatedAt,
		ReplayLimit: NormalizeReplayLimit(opts.ReplayLimit),
		Speeds:      opts.Speeds,
	})
	evaluation := analysis.EvaluateSession(analysis.EvaluationInput{
		SessionID:   replay.Session.SessionID,
		GeneratedAt: generatedAt,
		Windows:     evaluationWindows(analysisResult.SignalWindows, replay.ChatBuckets),
		Labels:      storageEvaluationLabels(replay.WindowLabels),
	})

	return SessionResult{
		SessionID:        replay.Session.SessionID,
		ChannelID:        replay.Session.ChannelID,
		Session:          replay.Session,
		Partial:          proof.Partial,
		TruncatedSources: proof.TruncatedSources,
		ReplayCounts: ReplayCounts{
			ChatBuckets:       len(replay.ChatBuckets),
			TranscriptBuckets: len(replay.TranscriptBuckets),
			Alignments:        len(replay.Alignments),
			SignalWindows:     len(analysisResult.SignalWindows),
			SignalEvents:      len(analysisResult.SignalEvents),
		},
		Proof:      proof,
		Evaluation: evaluation,
		Metrics:    summarizeMetrics(replay, proof, evaluation),
	}
}

func NormalizeReplayLimit(limit int) int {
	if limit <= 0 || limit > 500 {
		return 500
	}
	return limit
}

func NormalizeSessionIDs(values []string) []string {
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

func SessionIDs(report Report) []string {
	out := make([]string, 0, len(report.Sessions))
	for _, session := range report.Sessions {
		if strings.TrimSpace(session.SessionID) != "" {
			out = append(out, session.SessionID)
		}
	}
	return NormalizeSessionIDs(out)
}

func evaluationWindows(signalWindows []analysis.SignalWindow, chatBuckets []chat.ChatBucket) []analysis.SignalWindow {
	out := append([]analysis.SignalWindow(nil), signalWindows...)
	for _, bucket := range chatBuckets {
		for _, subwindow := range bucket.Subwindows {
			if subwindow.WindowStart.IsZero() || subwindow.WindowEnd.IsZero() {
				continue
			}
			eventType := regressionEventTypeForReaction(subwindow)
			window := analysis.SignalWindow{
				Type:           "reaction_window",
				SessionID:      bucket.SessionID,
				ChannelID:      bucket.ChannelID,
				Source:         firstNonEmpty(subwindow.Source, "chat"),
				StreamID:       bucket.ChannelID,
				WindowStart:    subwindow.WindowStart,
				WindowEnd:      subwindow.WindowEnd,
				MessageCount:   subwindow.MessageCount,
				ChatSentiment:  bucket.ChatSentiment,
				ReactionType:   subwindow.ReactionType,
				TargetType:     firstNonEmpty(subwindow.TargetType, "unknown"),
				TargetText:     subwindow.TargetText,
				EventHint:      subwindow.EventHint,
				Confidence:     subwindow.Confidence,
				EvidenceIDs:    append([]string(nil), subwindow.EvidenceIDs...),
				FirstEventType: eventType,
			}
			if eventType != "" {
				window.Events = []analysis.SignalEvent{{
					Type:         eventType,
					Severity:     subwindow.ReactionScore,
					Timestamp:    reactionSubwindowEventTimestamp(bucket, subwindow),
					ReactionType: subwindow.ReactionType,
					TargetType:   firstNonEmpty(subwindow.TargetType, "unknown"),
					TargetText:   subwindow.TargetText,
					Source:       firstNonEmpty(subwindow.Source, "chat"),
					EventHint:    subwindow.EventHint,
					Confidence:   subwindow.Confidence,
					EvidenceIDs:  append([]string(nil), subwindow.EvidenceIDs...),
				}}
			}
			out = append(out, window)
		}
	}
	return out
}

func regressionEventTypeForReaction(subwindow chat.ReactionSubwindow) analysis.SignalEventType {
	if subwindow.ReactionScore < 0.35 && subwindow.Confidence < 0.35 {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(subwindow.ReactionType)) {
	case "hype":
		return analysis.SignalEventHypeSpike
	case "frustration":
		return analysis.SignalEventFrustrationSpike
	case "confusion", "surprise":
		return analysis.SignalEventAudienceShift
	default:
		return ""
	}
}

func reactionSubwindowEventTimestamp(bucket chat.ChatBucket, subwindow chat.ReactionSubwindow) time.Time {
	if bucket.PeakTime != nil && !bucket.PeakTime.IsZero() && !bucket.PeakTime.Before(subwindow.WindowStart) && !bucket.PeakTime.After(subwindow.WindowEnd) {
		return *bucket.PeakTime
	}
	return subwindow.WindowEnd
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func summarizeMetrics(replay storage.SessionReplay, proof storage.ReplayProof, evaluation analysis.SessionEvaluation) MetricSummary {
	return MetricSummary{
		Partial:                        proof.Partial,
		TruncatedSourceCount:           len(proof.TruncatedSources),
		ProofLabelCoverage:             cloneFloat64(proof.LabelCoverage.Coverage),
		EvaluationCoverage:             cloneFloat64(evaluation.Coverage),
		EventAccuracy:                  cloneFloat64(evaluation.EventAccuracy),
		EventPrecision:                 cloneFloat64(evaluation.EventPrecision),
		EventRecall:                    cloneFloat64(evaluation.EventRecall),
		EventF1:                        cloneFloat64(evaluation.EventF1),
		PeakRecall:                     cloneFloat64(evaluation.PeakRecall),
		HypePeakRecall:                 cloneFloat64(evaluation.HypePeakRecall),
		OnsetLatencyMS:                 cloneFloat64(evaluation.OnsetLatencyMS),
		ReactionTypeAccuracy:           cloneFloat64(evaluation.ReactionTypeAccuracy),
		ReactionTypeF1:                 cloneFloat64(evaluation.ReactionTypeF1),
		TargetAccuracy:                 cloneFloat64(evaluation.TargetAccuracy),
		TargetExtractionAccuracy:       cloneFloat64(evaluation.TargetExtractionAccuracy),
		DivergenceAccuracy:             cloneFloat64(evaluation.DivergenceAccuracy),
		FalsePositivesNormalChat:       evaluation.FalsePositivesNormalChat,
		TotalWindows:                   evaluation.TotalWindows,
		TotalLabeledWindows:            evaluation.TotalLabeledWindows,
		EvaluatedWindows:               evaluation.EvaluatedWindows,
		UnmatchedLabels:                evaluation.UnmatchedLabels,
		UncertainLabels:                evaluation.UncertainLabels,
		InvalidLabels:                  evaluation.InvalidLabels,
		ReactionWindowCount:            reactionWindowCount(replay.ChatBuckets),
		PeakReactionCount:              peakReactionCount(replay.ChatBuckets),
		TranscriptBucketCoverage:       transcriptBucketCoverage(replay),
		TranscriptCoverage:             transcriptCoverage(proof),
		TranscriptAudioCoverage:        cloneFloat64(proof.TranscriptCoverage.AudioCoverage),
		TranscriptEmptyRatio:           cloneFloat64(proof.TranscriptCoverage.EmptyRatio),
		TranscriptWordCount:            proof.TranscriptCoverage.WordCount,
		TranscriptRepairAddedWords:     proof.TranscriptCoverage.RepairAddedWords,
		TranscriptRepairChangedRatio:   cloneFloat64(proof.TranscriptCoverage.AverageRepairChangedRatio),
		TranscriptRepairImprovement:    cloneFloat64(proof.TranscriptCoverage.RepairImprovement),
		AlignmentCount:                 len(replay.Alignments),
		MatchedWindowCount:             proof.MatchedWindows,
		PersistenceMetricCount:         len(replay.SystemMetrics),
		StorageWriteFailures:           latestMetricValue(replay.SystemMetrics, "storage.write_failures"),
		StorageDroppedWrites:           latestMetricValue(replay.SystemMetrics, "storage.dropped_writes"),
		StorageQueueDepth:              latestMetricValue(replay.SystemMetrics, "storage.queue_depth"),
		ReactionWindowMetricCount:      latestMetricValue(replay.SystemMetrics, "reaction_window.count"),
		LatencyAverageMS:               latencyAverageMS(proof),
		LatencyP50MS:                   latencyP50MS(proof),
		LatencyP95MS:                   latencyP95MS(proof),
		LatencyAvailableCounts:         latencyAvailableCounts(proof),
		LatencyMissingCounts:           latencyMissingCounts(proof),
		ChatAnalysisStatusCounts:       cloneStringIntMap(proof.Latency.ChatAnalysisStatusCounts),
		TranscriptStatusCounts:         cloneStringIntMap(proof.Latency.TranscriptSentimentStatusCounts),
		TranscriptCoverageStatusCounts: cloneStringIntMap(proof.TranscriptCoverage.StatusCounts),
		UnsupportedProofMetrics:        append([]storage.ReplayProofUnsupported(nil), proof.UnsupportedMetrics...),
		UnsupportedEvaluationMetrics:   append([]analysis.EvaluationUnsupportedMetric(nil), evaluation.UnsupportedMetrics...),
	}
}

func reactionWindowCount(buckets []chat.ChatBucket) int {
	var count int
	for _, bucket := range buckets {
		count += len(bucket.Subwindows)
	}
	return count
}

func peakReactionCount(buckets []chat.ChatBucket) int {
	var count int
	for _, bucket := range buckets {
		if bucket.PeakReactionScore != nil || (bucket.PeakWindowStart != nil && !bucket.PeakWindowStart.IsZero()) || bucket.PeakReactionType != "" || len(bucket.PeakEvidenceMessages) > 0 {
			count++
		}
	}
	return count
}

func transcriptBucketCoverage(replay storage.SessionReplay) *float64 {
	total := replay.Session.TranscriptBucketCount
	if total <= 0 {
		total = len(replay.TranscriptBuckets)
	}
	if total <= 0 {
		return nil
	}
	value := roundMetricFloat(float64(len(replay.TranscriptBuckets)) / float64(total))
	return &value
}

func transcriptCoverage(proof storage.ReplayProof) *float64 {
	if proof.TranscriptCoverage.AudioCoverage != nil {
		return cloneFloat64(proof.TranscriptCoverage.AudioCoverage)
	}
	total := proof.SessionTotals.TranscriptBucketCount
	if total <= 0 {
		return nil
	}
	value := roundMetricFloat(float64(proof.TranscriptBucketCount) / float64(total))
	return &value
}

func latestMetricValue(metrics []storage.SystemMetric, name string) *float64 {
	for _, metric := range metrics {
		if metric.Name == name {
			return floatPtr(metric.Value)
		}
	}
	return nil
}

func latencyAverageMS(proof storage.ReplayProof) map[string]*float64 {
	return nonEmptyLatencyMap(map[string]*float64{
		"chat_analysis":        cloneFloat64(proof.Latency.ChatAnalysisLatencyMS.Average),
		"transcript_sentiment": cloneFloat64(proof.Latency.TranscriptSentimentLatencyMS.Average),
		"transcript_asr":       cloneFloat64(proof.Latency.TranscriptASRLatencyMS.Average),
		"transcript_pipeline":  cloneFloat64(proof.Latency.TranscriptPipelineLatencyMS.Average),
	})
}

func latencyP50MS(proof storage.ReplayProof) map[string]*float64 {
	return nonEmptyLatencyMap(map[string]*float64{
		"chat_analysis":        cloneFloat64(proof.Latency.ChatAnalysisLatencyMS.P50),
		"transcript_sentiment": cloneFloat64(proof.Latency.TranscriptSentimentLatencyMS.P50),
		"transcript_asr":       cloneFloat64(proof.Latency.TranscriptASRLatencyMS.P50),
		"transcript_pipeline":  cloneFloat64(proof.Latency.TranscriptPipelineLatencyMS.P50),
	})
}

func latencyP95MS(proof storage.ReplayProof) map[string]*float64 {
	return nonEmptyLatencyMap(map[string]*float64{
		"chat_analysis":        cloneFloat64(proof.Latency.ChatAnalysisLatencyMS.P95),
		"transcript_sentiment": cloneFloat64(proof.Latency.TranscriptSentimentLatencyMS.P95),
		"transcript_asr":       cloneFloat64(proof.Latency.TranscriptASRLatencyMS.P95),
		"transcript_pipeline":  cloneFloat64(proof.Latency.TranscriptPipelineLatencyMS.P95),
	})
}

func latencyAvailableCounts(proof storage.ReplayProof) map[string]int {
	return nonEmptyIntMap(map[string]int{
		"chat_analysis":        proof.Latency.ChatAnalysisLatencyMS.AvailableCount,
		"transcript_sentiment": proof.Latency.TranscriptSentimentLatencyMS.AvailableCount,
		"transcript_asr":       proof.Latency.TranscriptASRLatencyMS.AvailableCount,
		"transcript_pipeline":  proof.Latency.TranscriptPipelineLatencyMS.AvailableCount,
	})
}

func latencyMissingCounts(proof storage.ReplayProof) map[string]int {
	return nonEmptyIntMap(map[string]int{
		"chat_analysis":        proof.Latency.ChatAnalysisLatencyMS.MissingCount,
		"transcript_sentiment": proof.Latency.TranscriptSentimentLatencyMS.MissingCount,
		"transcript_asr":       proof.Latency.TranscriptASRLatencyMS.MissingCount,
		"transcript_pipeline":  proof.Latency.TranscriptPipelineLatencyMS.MissingCount,
	})
}

func storageTranscriptBuckets(items []storage.TranscriptBucket) []analysis.TranscriptBucket {
	out := make([]analysis.TranscriptBucket, 0, len(items))
	for _, item := range items {
		out = append(out, analysis.TranscriptBucket{
			Type:                 item.Type,
			SessionID:            item.SessionID,
			ChannelID:            item.ChannelID,
			BucketStart:          item.BucketStart,
			BucketEnd:            item.BucketEnd,
			Text:                 item.Text,
			Language:             item.Language,
			TranscriptConfidence: item.TranscriptConfidence,
			SentimentScore:       item.SentimentScore,
			SentimentConfidence:  item.SentimentConfidence,
			SentimentLabel:       item.SentimentLabel,
			SentimentModel:       item.SentimentModel,
			SentimentStatus:      item.SentimentStatus,
			SentimentLatencyMS:   item.SentimentLatencyMS,
		})
	}
	return out
}

func storageAlignments(items []storage.AlignmentBucket) []analysis.AlignmentBucket {
	out := make([]analysis.AlignmentBucket, 0, len(items))
	for _, item := range items {
		out = append(out, analysis.AlignmentBucket{
			Type:                  item.Type,
			SessionID:             item.SessionID,
			ChannelID:             item.ChannelID,
			WindowStart:           item.WindowStart,
			WindowEnd:             item.WindowEnd,
			ChatBucketStart:       item.ChatBucketStart,
			ChatBucketEnd:         item.ChatBucketEnd,
			TranscriptBucketStart: item.TranscriptBucketStart,
			TranscriptBucketEnd:   item.TranscriptBucketEnd,
			ChatSentiment:         item.ChatSentiment,
			ChatConfidence:        item.ChatConfidence,
			ChatMessageCount:      item.ChatMessageCount,
			TranscriptSentiment:   item.TranscriptSentiment,
			TranscriptConfidence:  item.TranscriptConfidence,
			TranscriptTextLength:  item.TranscriptTextLength,
			Delta:                 item.Delta,
			Similarity:            item.Similarity,
			Relationship:          item.Relationship,
			OverlapSeconds:        item.OverlapSeconds,
			Quality:               item.Quality,
			QualityFlags:          append([]string(nil), item.QualityFlags...),
		})
	}
	return out
}

func storageEvaluationLabels(items []storage.SignalWindowLabel) []analysis.EvaluationLabel {
	out := make([]analysis.EvaluationLabel, 0, len(items))
	for _, item := range items {
		out = append(out, analysis.EvaluationLabel{
			SessionID:             item.SessionID,
			WindowStart:           item.WindowStart,
			WindowEnd:             item.WindowEnd,
			PredictedEvent:        item.PredictedEvent,
			PredictedRelationship: item.PredictedRelationship,
			ReactionType:          item.ReactionType,
			TargetType:            item.TargetType,
			TargetText:            item.TargetText,
			DivergenceType:        item.DivergenceType,
			EventStart:            item.EventStart,
			EventPeak:             item.EventPeak,
			Correctness:           item.Correctness,
			EventLabel:            item.EventLabel,
			CreatedAt:             item.CreatedAt,
			UpdatedAt:             item.UpdatedAt,
		})
	}
	return out
}

func normalizedSpeeds(values []float64) []float64 {
	if len(values) == 0 {
		return storage.DefaultReplayProofSpeeds()
	}
	seen := map[float64]struct{}{}
	out := make([]float64, 0, len(values))
	for _, value := range values {
		if value <= 0 || math.IsInf(value, 0) || math.IsNaN(value) {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return storage.DefaultReplayProofSpeeds()
	}
	sort.Float64s(out)
	return out
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func cloneStringIntMap(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]int, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func nonEmptyLatencyMap(values map[string]*float64) map[string]*float64 {
	for _, value := range values {
		if value != nil {
			return values
		}
	}
	return nil
}

func nonEmptyIntMap(values map[string]int) map[string]int {
	for _, value := range values {
		if value != 0 {
			return values
		}
	}
	return nil
}

func roundMetricFloat(value float64) float64 {
	return math.Round(value*10000) / 10000
}
