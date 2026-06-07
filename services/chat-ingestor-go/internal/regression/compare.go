package regression

import (
	"fmt"
	"math"
	"sort"
)

type Thresholds struct {
	MaxProofLabelCoverageDrop             float64 `json:"max_proof_label_coverage_drop"`
	MaxEvaluationCoverageDrop             float64 `json:"max_evaluation_coverage_drop"`
	MaxEventAccuracyDrop                  float64 `json:"max_event_accuracy_drop"`
	MaxPrecisionDrop                      float64 `json:"max_precision_drop"`
	MaxRecallDrop                         float64 `json:"max_recall_drop"`
	MaxF1Drop                             float64 `json:"max_f1_drop"`
	MaxOnsetLatencyIncreaseMS             float64 `json:"max_onset_latency_increase_ms"`
	MaxLatencyP95IncreaseMS               float64 `json:"max_latency_p95_increase_ms"`
	MaxTranscriptAudioCoverageDrop        float64 `json:"max_transcript_audio_coverage_drop"`
	MaxTranscriptCoverageDrop             float64 `json:"max_transcript_coverage_drop"`
	MaxTranscriptEmptyRatioIncrease       float64 `json:"max_transcript_empty_ratio_increase"`
	MaxTranscriptRepairImprovementDrop    float64 `json:"max_transcript_repair_improvement_drop"`
	MaxTranscriptRepairChangedIncrease    float64 `json:"max_transcript_repair_changed_ratio_increase"`
	MaxTranscriptRepairAddedWordsIncrease float64 `json:"max_transcript_repair_added_words_increase"`
	AllowNewPartial                       bool    `json:"allow_new_partial"`
}

type Comparison struct {
	BaselinePath string       `json:"baseline_path,omitempty"`
	Thresholds   Thresholds   `json:"thresholds"`
	Passed       bool         `json:"passed"`
	Regressions  []Regression `json:"regressions,omitempty"`
}

type Regression struct {
	SessionID string   `json:"session_id"`
	Metric    string   `json:"metric"`
	Baseline  *float64 `json:"baseline,omitempty"`
	Current   *float64 `json:"current,omitempty"`
	Threshold *float64 `json:"threshold,omitempty"`
	Message   string   `json:"message"`
}

func CompareReports(current Report, baseline Report, thresholds Thresholds) Comparison {
	comparison := Comparison{
		Thresholds: thresholds,
		Passed:     true,
	}

	currentBySession := sessionsByID(current)
	baselineBySession := sessionsByID(baseline)
	sessionIDs := make([]string, 0, len(baselineBySession))
	for sessionID := range baselineBySession {
		sessionIDs = append(sessionIDs, sessionID)
	}
	sort.Strings(sessionIDs)

	for _, sessionID := range sessionIDs {
		baselineSession := baselineBySession[sessionID]
		currentSession, ok := currentBySession[sessionID]
		if !ok {
			comparison.add(Regression{
				SessionID: sessionID,
				Metric:    "session",
				Message:   "session is present in baseline but missing from current report",
			})
			continue
		}
		compareSession(&comparison, currentSession, baselineSession, thresholds)
	}

	comparison.Passed = len(comparison.Regressions) == 0
	return comparison
}

func compareSession(comparison *Comparison, current SessionResult, baseline SessionResult, thresholds Thresholds) {
	if !thresholds.AllowNewPartial && !baseline.Metrics.Partial && current.Metrics.Partial {
		comparison.add(Regression{
			SessionID: current.SessionID,
			Metric:    "partial",
			Message:   "current replay is partial but baseline replay was complete",
		})
	}
	if !thresholds.AllowNewPartial && current.Metrics.TruncatedSourceCount > baseline.Metrics.TruncatedSourceCount {
		comparison.add(Regression{
			SessionID: current.SessionID,
			Metric:    "truncated_source_count",
			Baseline:  floatPtr(float64(baseline.Metrics.TruncatedSourceCount)),
			Current:   floatPtr(float64(current.Metrics.TruncatedSourceCount)),
			Threshold: floatPtr(0),
			Message:   "current replay has more truncated sources than baseline",
		})
	}

	compareDrop(comparison, current.SessionID, "proof_label_coverage", current.Metrics.ProofLabelCoverage, baseline.Metrics.ProofLabelCoverage, thresholds.MaxProofLabelCoverageDrop)
	compareDrop(comparison, current.SessionID, "evaluation_coverage", current.Metrics.EvaluationCoverage, baseline.Metrics.EvaluationCoverage, thresholds.MaxEvaluationCoverageDrop)
	compareDrop(comparison, current.SessionID, "event_accuracy", current.Metrics.EventAccuracy, baseline.Metrics.EventAccuracy, thresholds.MaxEventAccuracyDrop)
	compareDrop(comparison, current.SessionID, "event_precision", current.Metrics.EventPrecision, baseline.Metrics.EventPrecision, thresholds.MaxPrecisionDrop)
	compareDrop(comparison, current.SessionID, "event_recall", current.Metrics.EventRecall, baseline.Metrics.EventRecall, thresholds.MaxRecallDrop)
	compareDrop(comparison, current.SessionID, "event_f1", current.Metrics.EventF1, baseline.Metrics.EventF1, thresholds.MaxF1Drop)
	compareDrop(comparison, current.SessionID, "peak_recall", current.Metrics.PeakRecall, baseline.Metrics.PeakRecall, thresholds.MaxRecallDrop)
	compareDrop(comparison, current.SessionID, "hype_peak_recall", current.Metrics.HypePeakRecall, baseline.Metrics.HypePeakRecall, thresholds.MaxRecallDrop)
	compareIncrease(comparison, current.SessionID, "onset_latency_ms", current.Metrics.OnsetLatencyMS, baseline.Metrics.OnsetLatencyMS, thresholds.MaxOnsetLatencyIncreaseMS)
	compareDrop(comparison, current.SessionID, "reaction_type_f1", current.Metrics.ReactionTypeF1, baseline.Metrics.ReactionTypeF1, thresholds.MaxF1Drop)
	compareDrop(comparison, current.SessionID, "reaction_type_accuracy", current.Metrics.ReactionTypeAccuracy, baseline.Metrics.ReactionTypeAccuracy, thresholds.MaxEventAccuracyDrop)
	compareDrop(comparison, current.SessionID, "target_accuracy", current.Metrics.TargetAccuracy, baseline.Metrics.TargetAccuracy, thresholds.MaxEventAccuracyDrop)
	compareDrop(comparison, current.SessionID, "target_extraction_accuracy", current.Metrics.TargetExtractionAccuracy, baseline.Metrics.TargetExtractionAccuracy, thresholds.MaxEventAccuracyDrop)
	compareDrop(comparison, current.SessionID, "divergence_accuracy", current.Metrics.DivergenceAccuracy, baseline.Metrics.DivergenceAccuracy, thresholds.MaxEventAccuracyDrop)
	compareDrop(comparison, current.SessionID, "transcript_bucket_coverage", current.Metrics.TranscriptBucketCoverage, baseline.Metrics.TranscriptBucketCoverage, thresholds.MaxEvaluationCoverageDrop)
	compareDrop(comparison, current.SessionID, "transcript_coverage", current.Metrics.TranscriptCoverage, baseline.Metrics.TranscriptCoverage, thresholds.MaxTranscriptCoverageDrop)
	compareDrop(comparison, current.SessionID, "transcript_audio_coverage", current.Metrics.TranscriptAudioCoverage, baseline.Metrics.TranscriptAudioCoverage, thresholds.MaxTranscriptAudioCoverageDrop)
	compareIncrease(comparison, current.SessionID, "transcript_empty_ratio", current.Metrics.TranscriptEmptyRatio, baseline.Metrics.TranscriptEmptyRatio, thresholds.MaxTranscriptEmptyRatioIncrease)
	compareDrop(comparison, current.SessionID, "transcript_repair_improvement", current.Metrics.TranscriptRepairImprovement, baseline.Metrics.TranscriptRepairImprovement, thresholds.MaxTranscriptRepairImprovementDrop)
	compareIncrease(comparison, current.SessionID, "transcript_repair_changed_ratio", current.Metrics.TranscriptRepairChangedRatio, baseline.Metrics.TranscriptRepairChangedRatio, thresholds.MaxTranscriptRepairChangedIncrease)
	compareIncrease(comparison, current.SessionID, "transcript_repair_added_words", floatPtr(float64(current.Metrics.TranscriptRepairAddedWords)), floatPtr(float64(baseline.Metrics.TranscriptRepairAddedWords)), thresholds.MaxTranscriptRepairAddedWordsIncrease)
	compareCountDrop(comparison, current.SessionID, "total_windows", current.Metrics.TotalWindows, baseline.Metrics.TotalWindows)
	compareCountDrop(comparison, current.SessionID, "total_labeled_windows", current.Metrics.TotalLabeledWindows, baseline.Metrics.TotalLabeledWindows)
	compareCountDrop(comparison, current.SessionID, "evaluated_windows", current.Metrics.EvaluatedWindows, baseline.Metrics.EvaluatedWindows)
	compareCountDrop(comparison, current.SessionID, "reaction_window_count", current.Metrics.ReactionWindowCount, baseline.Metrics.ReactionWindowCount)
	compareCountDrop(comparison, current.SessionID, "peak_reaction_count", current.Metrics.PeakReactionCount, baseline.Metrics.PeakReactionCount)
	compareCountDrop(comparison, current.SessionID, "transcript_word_count", current.Metrics.TranscriptWordCount, baseline.Metrics.TranscriptWordCount)
	compareCountDrop(comparison, current.SessionID, "alignment_count", current.Metrics.AlignmentCount, baseline.Metrics.AlignmentCount)
	compareCountDrop(comparison, current.SessionID, "matched_window_count", current.Metrics.MatchedWindowCount, baseline.Metrics.MatchedWindowCount)
	compareCountDrop(comparison, current.SessionID, "persistence_metric_count", current.Metrics.PersistenceMetricCount, baseline.Metrics.PersistenceMetricCount)
	compareIncrease(comparison, current.SessionID, "unmatched_labels", floatPtr(float64(current.Metrics.UnmatchedLabels)), floatPtr(float64(baseline.Metrics.UnmatchedLabels)), 0)
	compareIncrease(comparison, current.SessionID, "invalid_labels", floatPtr(float64(current.Metrics.InvalidLabels)), floatPtr(float64(baseline.Metrics.InvalidLabels)), 0)
	compareIncrease(comparison, current.SessionID, "false_positives_normal_chat", floatPtr(float64(current.Metrics.FalsePositivesNormalChat)), floatPtr(float64(baseline.Metrics.FalsePositivesNormalChat)), 0)
	compareIncrease(comparison, current.SessionID, "storage_write_failures", current.Metrics.StorageWriteFailures, baseline.Metrics.StorageWriteFailures, 0)
	compareIncrease(comparison, current.SessionID, "storage_dropped_writes", current.Metrics.StorageDroppedWrites, baseline.Metrics.StorageDroppedWrites, 0)
	compareIncrease(comparison, current.SessionID, "storage_queue_depth", current.Metrics.StorageQueueDepth, baseline.Metrics.StorageQueueDepth, 0)
	compareDrop(comparison, current.SessionID, "reaction_window_metric_count", current.Metrics.ReactionWindowMetricCount, baseline.Metrics.ReactionWindowMetricCount, 0)

	if thresholds.MaxLatencyP95IncreaseMS >= 0 {
		compareLatencyP95(comparison, current, baseline, thresholds.MaxLatencyP95IncreaseMS)
	}
}

func compareDrop(comparison *Comparison, sessionID string, metric string, current *float64, baseline *float64, threshold float64) {
	if baseline == nil {
		return
	}
	if current == nil {
		comparison.add(Regression{
			SessionID: sessionID,
			Metric:    metric,
			Baseline:  cloneFloat64(baseline),
			Message:   fmt.Sprintf("%s was present in baseline but missing from current report", metric),
		})
		return
	}
	drop := *baseline - *current
	if drop > threshold+floatTolerance {
		comparison.add(Regression{
			SessionID: sessionID,
			Metric:    metric,
			Baseline:  cloneFloat64(baseline),
			Current:   cloneFloat64(current),
			Threshold: floatPtr(threshold),
			Message:   fmt.Sprintf("%s dropped by %.4f, exceeding threshold %.4f", metric, drop, threshold),
		})
	}
}

func compareCountDrop(comparison *Comparison, sessionID string, metric string, current int, baseline int) {
	if baseline <= 0 {
		return
	}
	if current < baseline {
		comparison.add(Regression{
			SessionID: sessionID,
			Metric:    metric,
			Baseline:  floatPtr(float64(baseline)),
			Current:   floatPtr(float64(current)),
			Threshold: floatPtr(0),
			Message:   fmt.Sprintf("%s dropped from %d to %d", metric, baseline, current),
		})
	}
}

func compareIncrease(comparison *Comparison, sessionID string, metric string, current *float64, baseline *float64, threshold float64) {
	if baseline == nil {
		return
	}
	if current == nil {
		comparison.add(Regression{
			SessionID: sessionID,
			Metric:    metric,
			Baseline:  cloneFloat64(baseline),
			Message:   fmt.Sprintf("%s was present in baseline but missing from current report", metric),
		})
		return
	}
	increase := *current - *baseline
	if increase > threshold+floatTolerance {
		comparison.add(Regression{
			SessionID: sessionID,
			Metric:    metric,
			Baseline:  cloneFloat64(baseline),
			Current:   cloneFloat64(current),
			Threshold: floatPtr(threshold),
			Message:   fmt.Sprintf("%s increased by %.4f, exceeding threshold %.4f", metric, increase, threshold),
		})
	}
}

func compareLatencyP95(comparison *Comparison, current SessionResult, baseline SessionResult, threshold float64) {
	keys := mapKeys(baseline.Metrics.LatencyP95MS)
	for _, key := range keys {
		metric := "latency_p95_ms." + key
		baselineValue := baseline.Metrics.LatencyP95MS[key]
		if baselineValue == nil {
			continue
		}
		currentValue := current.Metrics.LatencyP95MS[key]
		if currentValue == nil {
			comparison.add(Regression{
				SessionID: current.SessionID,
				Metric:    metric,
				Baseline:  cloneFloat64(baselineValue),
				Message:   fmt.Sprintf("%s was present in baseline but missing from current report", metric),
			})
			continue
		}
		increase := *currentValue - *baselineValue
		if increase > threshold+floatTolerance {
			comparison.add(Regression{
				SessionID: current.SessionID,
				Metric:    metric,
				Baseline:  cloneFloat64(baselineValue),
				Current:   cloneFloat64(currentValue),
				Threshold: floatPtr(threshold),
				Message:   fmt.Sprintf("%s increased by %.1fms, exceeding threshold %.1fms", metric, increase, threshold),
			})
		}
	}
}

func sessionsByID(report Report) map[string]SessionResult {
	out := make(map[string]SessionResult, len(report.Sessions))
	for _, session := range report.Sessions {
		if session.SessionID != "" {
			out[session.SessionID] = session
		}
	}
	return out
}

func (c *Comparison) add(regression Regression) {
	c.Regressions = append(c.Regressions, regression)
}

func mapKeys(values map[string]*float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func floatPtr(value float64) *float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}

const floatTolerance = 0.0000001
