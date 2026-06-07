package analysis

import (
	"math"
	"sort"
	"strings"
	"time"
)

const SessionEvaluationType = "session_evaluation"

const EventLabelNone = "none"

var evaluationEventLabelOrder = []string{
	string(SignalEventHypeSpike),
	string(SignalEventFrustrationSpike),
	string(SignalEventAudienceShift),
	string(SignalEventContentAudienceDivergence),
	EventLabelNone,
}

type EvaluationInput struct {
	SessionID   string
	GeneratedAt time.Time
	Windows     []SignalWindow
	Labels      []EvaluationLabel
}

type EvaluationLabel struct {
	SessionID             string
	WindowStart           time.Time
	WindowEnd             time.Time
	PredictedEvent        string
	PredictedRelationship string
	Correctness           string
	EventLabel            string
	ReactionType          string
	TargetType            string
	TargetText            string
	DivergenceType        string
	EventStart            time.Time
	EventPeak             time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type SessionEvaluation struct {
	Type                     string                        `json:"type"`
	SessionID                string                        `json:"session_id"`
	GeneratedAt              time.Time                     `json:"generated_at"`
	TotalWindows             int                           `json:"total_windows"`
	TotalLabeledWindows      int                           `json:"total_labeled_windows"`
	EvaluatedWindows         int                           `json:"evaluated_windows"`
	Coverage                 *float64                      `json:"coverage"`
	UnmatchedLabels          int                           `json:"unmatched_labels"`
	UncertainLabels          int                           `json:"uncertain_labels"`
	InvalidLabels            int                           `json:"invalid_labels"`
	EventConfusionCounts     []EventConfusionCount         `json:"event_confusion_counts"`
	EventCounts              EventBinaryCounts             `json:"event_counts"`
	EventAccuracy            *float64                      `json:"event_accuracy"`
	EventPrecision           *float64                      `json:"event_precision"`
	EventRecall              *float64                      `json:"event_recall"`
	EventF1                  *float64                      `json:"event_f1"`
	EventLabelMetrics        []EventLabelMetric            `json:"event_label_metrics"`
	PeakRecall               *float64                      `json:"peak_recall"`
	HypePeakRecall           *float64                      `json:"hype_peak_recall"`
	OnsetLatencyMS           *float64                      `json:"onset_latency_ms"`
	EventOnsetLatencyMS      *float64                      `json:"event_onset_latency_ms"`
	ReactionTypeAccuracy     *float64                      `json:"reaction_type_accuracy"`
	ReactionTypeF1           *float64                      `json:"reaction_type_f1"`
	ReactionTypeMetrics      []EventLabelMetric            `json:"reaction_type_metrics,omitempty"`
	TargetAccuracy           *float64                      `json:"target_accuracy"`
	TargetExtractionAccuracy *float64                      `json:"target_extraction_accuracy"`
	DivergenceAccuracy       *float64                      `json:"divergence_accuracy"`
	FalsePositivesNormalChat int                           `json:"false_positives_normal_chat"`
	RelationshipAccuracy     *float64                      `json:"relationship_accuracy"`
	UnsupportedMetrics       []EvaluationUnsupportedMetric `json:"unsupported_metrics,omitempty"`
	CorrectnessCounts        map[string]int                `json:"correctness_counts,omitempty"`
}

type EventConfusionCount struct {
	Actual    string `json:"actual"`
	Predicted string `json:"predicted"`
	Count     int    `json:"count"`
}

type EventBinaryCounts struct {
	TruePositive  int `json:"true_positive"`
	FalsePositive int `json:"false_positive"`
	FalseNegative int `json:"false_negative"`
	TrueNegative  int `json:"true_negative"`
}

type EventLabelMetric struct {
	Label          string   `json:"label"`
	Support        int      `json:"support"`
	PredictedCount int      `json:"predicted_count"`
	TruePositive   int      `json:"true_positive"`
	FalsePositive  int      `json:"false_positive"`
	FalseNegative  int      `json:"false_negative"`
	Precision      *float64 `json:"precision"`
	Recall         *float64 `json:"recall"`
	F1             *float64 `json:"f1"`
}

type EvaluationUnsupportedMetric struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

func EvaluateSession(input EvaluationInput) SessionEvaluation {
	generatedAt := input.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	} else {
		generatedAt = generatedAt.UTC()
	}

	windowsByKey := evaluationWindowsByKey(input.Windows)
	labelsByKey := evaluationLabelsByKey(input.Labels)
	confusion := map[string]map[string]int{}
	labelStats := map[string]*eventClassStats{}
	reactionStats := map[string]*eventClassStats{}
	correctnessCounts := map[string]int{}
	knownLabels := evaluationKnownLabelSet()
	knownReactionTypes := map[string]struct{}{}
	var counts EventBinaryCounts
	var exactMatches int
	var evaluatedWindows int
	var unmatchedLabels int
	var uncertainLabels int
	var invalidLabels int
	var actualPeakLabels int
	var detectedPeakLabels int
	var actualHypePeaks int
	var detectedHypePeaks int
	var latencyTotalMS float64
	var latencyCount int
	var reactionTypeMatches int
	var reactionTypeEvaluated int
	var targetMatches int
	var targetEvaluated int
	var divergenceMatches int
	var divergenceEvaluated int
	var falsePositivesNormalChat int

	for key, label := range labelsByKey {
		correctness := normalizeCorrectnessLabel(label.Correctness)
		if correctness != "" {
			correctnessCounts[correctness]++
		}

		window, ok := windowsByKey[key]
		if !ok {
			unmatchedLabels++
			continue
		}
		if correctness == "uncertain" {
			uncertainLabels++
			continue
		}

		actual, valid := normalizeActualEventLabel(label.EventLabel)
		if !valid {
			invalidLabels++
			continue
		}
		predicted := normalizePredictedEventLabel(firstNonEmptyString(label.PredictedEvent, windowPredictedEvent(window)))

		knownLabels[actual] = struct{}{}
		knownLabels[predicted] = struct{}{}
		incrementConfusion(confusion, actual, predicted)
		incrementEventClassStats(labelStats, actual, predicted)
		evaluatedWindows++

		actualHasEvent := actual != EventLabelNone
		predictedHasEvent := predicted != EventLabelNone
		switch {
		case actualHasEvent && predictedHasEvent && actual == predicted:
			counts.TruePositive++
		case actualHasEvent && predictedHasEvent:
			counts.FalsePositive++
			counts.FalseNegative++
		case actualHasEvent:
			counts.FalseNegative++
		case predictedHasEvent:
			counts.FalsePositive++
		default:
			counts.TrueNegative++
		}
		if actual == predicted {
			exactMatches++
		}

		if actual == string(SignalEventHypeSpike) {
			actualHypePeaks++
			if windowHasEvent(window, SignalEventHypeSpike) {
				detectedHypePeaks++
			}
		}
		if actual != EventLabelNone && !label.EventPeak.IsZero() {
			actualPeakLabels++
			if windowHasEvent(window, SignalEventType(actual)) {
				detectedPeakLabels++
			}
		}
		if actual != EventLabelNone && actual == predicted {
			if predictedTimestamp := predictedEventTimestamp(window, actual); !predictedTimestamp.IsZero() {
				actualTimestamp := firstNonZeroTime(label.EventStart, label.EventPeak, label.WindowStart)
				if !actualTimestamp.IsZero() {
					latencyTotalMS += float64(predictedTimestamp.Sub(actualTimestamp).Milliseconds())
					latencyCount++
				}
			}
		}
		if actual == EventLabelNone && predicted != EventLabelNone {
			falsePositivesNormalChat++
		}

		if actualReaction := normalizeEvaluationDimension(label.ReactionType); actualReaction != "" {
			predictedReaction := normalizeEvaluationDimension(windowPredictedReactionType(window))
			knownReactionTypes[actualReaction] = struct{}{}
			knownReactionTypes[predictedReaction] = struct{}{}
			incrementEventClassStats(reactionStats, actualReaction, predictedReaction)
			reactionTypeEvaluated++
			if actualReaction == predictedReaction {
				reactionTypeMatches++
			}
		}

		if labelHasTarget(label) {
			targetEvaluated++
			if labelTargetMatchesWindow(label, window) {
				targetMatches++
			}
		}

		if actualDivergence, ok := actualDivergenceLabel(label, actual); ok {
			divergenceEvaluated++
			if actualDivergence == windowHasEvent(window, SignalEventContentAudienceDivergence) {
				divergenceMatches++
			}
		}
	}

	evaluation := SessionEvaluation{
		Type:                     SessionEvaluationType,
		SessionID:                firstNonEmptyString(input.SessionID, firstWindowSession(input.Windows), firstLabelSession(input.Labels)),
		GeneratedAt:              generatedAt,
		TotalWindows:             len(windowsByKey),
		TotalLabeledWindows:      len(labelsByKey),
		EvaluatedWindows:         evaluatedWindows,
		UnmatchedLabels:          unmatchedLabels,
		UncertainLabels:          uncertainLabels,
		InvalidLabels:            invalidLabels,
		EventConfusionCounts:     sortedConfusionCounts(confusion),
		EventCounts:              counts,
		EventLabelMetrics:        sortedEventLabelMetrics(labelStats, knownLabels),
		ReactionTypeMetrics:      sortedDimensionMetrics(reactionStats, knownReactionTypes),
		FalsePositivesNormalChat: falsePositivesNormalChat,
		RelationshipAccuracy:     nil,
		UnsupportedMetrics: []EvaluationUnsupportedMetric{
			{
				Name:   "relationship_accuracy",
				Reason: "signal window labels store predicted relationships but do not yet store human relationship labels",
			},
		},
		CorrectnessCounts: emptyStringIntMapAsNil(correctnessCounts),
	}
	if len(windowsByKey) > 0 {
		coverage := roundEvaluationFloat(float64(evaluatedWindows) / float64(len(windowsByKey)))
		evaluation.Coverage = &coverage
	}
	if evaluatedWindows > 0 {
		accuracy := roundEvaluationFloat(float64(exactMatches) / float64(evaluatedWindows))
		precision := roundedRatioOrZero(counts.TruePositive, counts.TruePositive+counts.FalsePositive)
		recall := roundedRatioOrZero(counts.TruePositive, counts.TruePositive+counts.FalseNegative)
		f1 := roundedF1(precision, recall)
		evaluation.EventAccuracy = &accuracy
		evaluation.EventPrecision = &precision
		evaluation.EventRecall = &recall
		evaluation.EventF1 = &f1
	}
	if actualHypePeaks > 0 {
		hypePeakRecall := roundedRatioOrZero(detectedHypePeaks, actualHypePeaks)
		evaluation.HypePeakRecall = &hypePeakRecall
	}
	if actualPeakLabels > 0 {
		peakRecall := roundedRatioOrZero(detectedPeakLabels, actualPeakLabels)
		evaluation.PeakRecall = &peakRecall
	} else if evaluation.HypePeakRecall != nil {
		peakRecall := *evaluation.HypePeakRecall
		evaluation.PeakRecall = &peakRecall
	}
	if latencyCount > 0 {
		eventOnsetLatencyMS := roundEvaluationFloat(latencyTotalMS / float64(latencyCount))
		evaluation.EventOnsetLatencyMS = &eventOnsetLatencyMS
		onsetLatencyMS := eventOnsetLatencyMS
		evaluation.OnsetLatencyMS = &onsetLatencyMS
	}
	if reactionTypeEvaluated > 0 {
		reactionTypeAccuracy := roundEvaluationFloat(float64(reactionTypeMatches) / float64(reactionTypeEvaluated))
		evaluation.ReactionTypeAccuracy = &reactionTypeAccuracy
		if reactionTypeF1 := macroF1(evaluation.ReactionTypeMetrics); reactionTypeF1 != nil {
			evaluation.ReactionTypeF1 = reactionTypeF1
		}
	}
	if targetEvaluated > 0 {
		targetExtractionAccuracy := roundEvaluationFloat(float64(targetMatches) / float64(targetEvaluated))
		evaluation.TargetExtractionAccuracy = &targetExtractionAccuracy
		targetAccuracy := targetExtractionAccuracy
		evaluation.TargetAccuracy = &targetAccuracy
	}
	if divergenceEvaluated > 0 {
		divergenceAccuracy := roundEvaluationFloat(float64(divergenceMatches) / float64(divergenceEvaluated))
		evaluation.DivergenceAccuracy = &divergenceAccuracy
	}
	return evaluation
}

type eventClassStats struct {
	support        int
	predictedCount int
	truePositive   int
	falsePositive  int
	falseNegative  int
}

func evaluationWindowsByKey(windows []SignalWindow) map[string]SignalWindow {
	out := map[string]SignalWindow{}
	for _, window := range windows {
		key := evaluationWindowKey(window.SessionID, window.WindowStart, window.WindowEnd)
		if key == "" {
			continue
		}
		out[key] = window
	}
	return out
}

func evaluationLabelsByKey(labels []EvaluationLabel) map[string]EvaluationLabel {
	out := map[string]EvaluationLabel{}
	for _, label := range labels {
		key := evaluationWindowKey(label.SessionID, label.WindowStart, label.WindowEnd)
		if key == "" {
			continue
		}
		if existing, ok := out[key]; ok && !labelIsNewer(label, existing) {
			continue
		}
		out[key] = label
	}
	return out
}

func labelIsNewer(left, right EvaluationLabel) bool {
	leftTime := firstNonZeroTime(left.UpdatedAt, left.CreatedAt)
	rightTime := firstNonZeroTime(right.UpdatedAt, right.CreatedAt)
	if leftTime.IsZero() || rightTime.IsZero() {
		return !leftTime.IsZero()
	}
	return leftTime.After(rightTime)
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func evaluationWindowKey(sessionID string, start, end time.Time) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || start.IsZero() || end.IsZero() {
		return ""
	}
	return strings.Join([]string{
		sessionID,
		normalizeEvaluationTime(start).Format(time.RFC3339Nano),
		normalizeEvaluationTime(end).Format(time.RFC3339Nano),
	}, ":")
}

func normalizeEvaluationTime(value time.Time) time.Time {
	return value.UTC().Round(time.Microsecond)
}

func windowPredictedEvent(window SignalWindow) string {
	if window.FirstEventType != "" {
		return string(window.FirstEventType)
	}
	for _, event := range window.Events {
		if event.Type != "" {
			return string(event.Type)
		}
	}
	return EventLabelNone
}

func windowHasEvent(window SignalWindow, eventType SignalEventType) bool {
	if window.FirstEventType == eventType {
		return true
	}
	for _, event := range window.Events {
		if event.Type == eventType {
			return true
		}
	}
	return false
}

func predictedEventTimestamp(window SignalWindow, eventLabel string) time.Time {
	eventLabel = normalizePredictedEventLabel(eventLabel)
	for _, event := range window.Events {
		if normalizePredictedEventLabel(string(event.Type)) == eventLabel && !event.Timestamp.IsZero() {
			return event.Timestamp.UTC()
		}
	}
	if normalizePredictedEventLabel(string(window.FirstEventType)) == eventLabel && !window.WindowStart.IsZero() {
		return window.WindowStart.UTC()
	}
	return time.Time{}
}

func windowPredictedReactionType(window SignalWindow) string {
	if reactionType := normalizeEvaluationDimension(window.ReactionType); reactionType != "" {
		return reactionType
	}
	for _, event := range window.Events {
		if reactionType := normalizeEvaluationDimension(event.ReactionType); reactionType != "" {
			return reactionType
		}
	}
	return "neutral"
}

func normalizeActualEventLabel(value string) (string, bool) {
	label := strings.ToLower(strings.TrimSpace(value))
	if label == "" {
		label = EventLabelNone
	}
	switch label {
	case string(SignalEventHypeSpike), string(SignalEventFrustrationSpike), string(SignalEventAudienceShift), string(SignalEventContentAudienceDivergence), EventLabelNone:
		return label, true
	default:
		return label, false
	}
}

func normalizePredictedEventLabel(value string) string {
	label := strings.ToLower(strings.TrimSpace(value))
	if label == "" {
		return EventLabelNone
	}
	return label
}

func normalizeEvaluationDimension(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func labelHasTarget(label EvaluationLabel) bool {
	return normalizeEvaluationDimension(label.TargetType) != "" || normalizeEvaluationDimension(label.TargetText) != ""
}

func labelTargetMatchesWindow(label EvaluationLabel, window SignalWindow) bool {
	actualType := normalizeEvaluationDimension(label.TargetType)
	actualText := normalizeEvaluationDimension(label.TargetText)
	predictedType := normalizeEvaluationDimension(window.TargetType)
	predictedText := normalizeEvaluationDimension(window.TargetText)

	for _, event := range window.Events {
		if predictedType == "" {
			predictedType = normalizeEvaluationDimension(event.TargetType)
		}
		if predictedText == "" {
			predictedText = normalizeEvaluationDimension(event.TargetText)
		}
	}

	if actualType != "" && actualType != predictedType {
		return false
	}
	if actualText != "" && actualText != predictedText {
		return false
	}
	return true
}

func actualDivergenceLabel(label EvaluationLabel, actualEvent string) (bool, bool) {
	switch normalizeEvaluationDimension(label.DivergenceType) {
	case "diverged", "divergence", "content_audience_divergence":
		return true, true
	case "converged", "aligned", "none", "normal":
		return false, true
	}
	return actualEvent == string(SignalEventContentAudienceDivergence), true
}

func normalizeCorrectnessLabel(value string) string {
	label := strings.ToLower(strings.TrimSpace(value))
	switch label {
	case "correct", "wrong", "uncertain":
		return label
	default:
		return ""
	}
}

func incrementConfusion(confusion map[string]map[string]int, actual, predicted string) {
	if _, ok := confusion[actual]; !ok {
		confusion[actual] = map[string]int{}
	}
	confusion[actual][predicted]++
}

func incrementEventClassStats(stats map[string]*eventClassStats, actual, predicted string) {
	actualStats := eventStatsForLabel(stats, actual)
	actualStats.support++
	if actual == predicted {
		actualStats.truePositive++
	} else {
		actualStats.falseNegative++
	}

	predictedStats := eventStatsForLabel(stats, predicted)
	predictedStats.predictedCount++
	if actual != predicted {
		predictedStats.falsePositive++
	}
}

func eventStatsForLabel(stats map[string]*eventClassStats, label string) *eventClassStats {
	if item, ok := stats[label]; ok {
		return item
	}
	item := &eventClassStats{}
	stats[label] = item
	return item
}

func sortedConfusionCounts(confusion map[string]map[string]int) []EventConfusionCount {
	var out []EventConfusionCount
	for actual, predictedCounts := range confusion {
		for predicted, count := range predictedCounts {
			out = append(out, EventConfusionCount{Actual: actual, Predicted: predicted, Count: count})
		}
	}
	sort.Slice(out, func(left, right int) bool {
		leftActual := evaluationLabelRank(out[left].Actual)
		rightActual := evaluationLabelRank(out[right].Actual)
		if leftActual != rightActual {
			return leftActual < rightActual
		}
		leftPredicted := evaluationLabelRank(out[left].Predicted)
		rightPredicted := evaluationLabelRank(out[right].Predicted)
		if leftPredicted != rightPredicted {
			return leftPredicted < rightPredicted
		}
		if out[left].Actual != out[right].Actual {
			return out[left].Actual < out[right].Actual
		}
		return out[left].Predicted < out[right].Predicted
	})
	return out
}

func sortedEventLabelMetrics(stats map[string]*eventClassStats, knownLabels map[string]struct{}) []EventLabelMetric {
	labels := make([]string, 0, len(knownLabels))
	for label := range knownLabels {
		labels = append(labels, label)
	}
	sort.Slice(labels, func(left, right int) bool {
		leftRank := evaluationLabelRank(labels[left])
		rightRank := evaluationLabelRank(labels[right])
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return labels[left] < labels[right]
	})

	out := make([]EventLabelMetric, 0, len(labels))
	for _, label := range labels {
		item := eventStatsForLabel(stats, label)
		precision := roundedRatioOrZero(item.truePositive, item.truePositive+item.falsePositive)
		recall := roundedRatioOrZero(item.truePositive, item.truePositive+item.falseNegative)
		f1 := roundedF1(precision, recall)
		out = append(out, EventLabelMetric{
			Label:          label,
			Support:        item.support,
			PredictedCount: item.predictedCount,
			TruePositive:   item.truePositive,
			FalsePositive:  item.falsePositive,
			FalseNegative:  item.falseNegative,
			Precision:      &precision,
			Recall:         &recall,
			F1:             &f1,
		})
	}
	return out
}

func sortedDimensionMetrics(stats map[string]*eventClassStats, knownLabels map[string]struct{}) []EventLabelMetric {
	if len(knownLabels) == 0 {
		return nil
	}
	labels := make([]string, 0, len(knownLabels))
	for label := range knownLabels {
		if label != "" {
			labels = append(labels, label)
		}
	}
	sort.Strings(labels)

	out := make([]EventLabelMetric, 0, len(labels))
	for _, label := range labels {
		item := eventStatsForLabel(stats, label)
		precision := roundedRatioOrZero(item.truePositive, item.truePositive+item.falsePositive)
		recall := roundedRatioOrZero(item.truePositive, item.truePositive+item.falseNegative)
		f1 := roundedF1(precision, recall)
		out = append(out, EventLabelMetric{
			Label:          label,
			Support:        item.support,
			PredictedCount: item.predictedCount,
			TruePositive:   item.truePositive,
			FalsePositive:  item.falsePositive,
			FalseNegative:  item.falseNegative,
			Precision:      &precision,
			Recall:         &recall,
			F1:             &f1,
		})
	}
	return out
}

func macroF1(metrics []EventLabelMetric) *float64 {
	var total float64
	var count int
	for _, metric := range metrics {
		if metric.Support == 0 && metric.PredictedCount == 0 {
			continue
		}
		if metric.F1 == nil {
			continue
		}
		total += *metric.F1
		count++
	}
	if count == 0 {
		return nil
	}
	value := roundEvaluationFloat(total / float64(count))
	return &value
}

func evaluationKnownLabelSet() map[string]struct{} {
	out := make(map[string]struct{}, len(evaluationEventLabelOrder))
	for _, label := range evaluationEventLabelOrder {
		out[label] = struct{}{}
	}
	return out
}

func evaluationLabelRank(label string) int {
	for index, value := range evaluationEventLabelOrder {
		if label == value {
			return index
		}
	}
	return len(evaluationEventLabelOrder)
}

func roundedRatioOrZero(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return roundEvaluationFloat(float64(numerator) / float64(denominator))
}

func roundedF1(precision, recall float64) float64 {
	if precision+recall == 0 {
		return 0
	}
	return roundEvaluationFloat(2 * precision * recall / (precision + recall))
}

func roundEvaluationFloat(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return math.Round(value*10000) / 10000
}

func firstWindowSession(windows []SignalWindow) string {
	for _, window := range windows {
		if strings.TrimSpace(window.SessionID) != "" {
			return window.SessionID
		}
	}
	return ""
}

func firstLabelSession(labels []EvaluationLabel) string {
	for _, label := range labels {
		if strings.TrimSpace(label.SessionID) != "" {
			return label.SessionID
		}
	}
	return ""
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func emptyStringIntMapAsNil(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	return values
}
