package analysis

import (
	"testing"
	"time"
)

func TestEvaluateSessionCalculatesEventQualityWithoutInflatingNone(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	generatedAt := time.Date(2026, 5, 1, 13, 0, 0, 0, time.UTC)
	windows := []SignalWindow{
		evaluationTestWindow(start, SignalEventHypeSpike),
		evaluationTestWindow(start.Add(30*time.Second), ""),
		evaluationTestWindow(start.Add(60*time.Second), ""),
		evaluationTestWindow(start.Add(90*time.Second), SignalEventContentAudienceDivergence),
		evaluationTestWindow(start.Add(120*time.Second), SignalEventHypeSpike),
		evaluationTestWindow(start.Add(150*time.Second), SignalEventFrustrationSpike),
	}
	labels := []EvaluationLabel{
		evaluationTestLabel(start, "hype_spike", "correct"),
		evaluationTestLabel(start.Add(30*time.Second), "none", "correct"),
		evaluationTestLabel(start.Add(60*time.Second), "frustration_spike", "wrong"),
		evaluationTestLabel(start.Add(90*time.Second), "none", "wrong"),
		evaluationTestLabel(start.Add(120*time.Second), "frustration_spike", "wrong"),
		evaluationTestLabel(start.Add(150*time.Second), "hype_spike", "uncertain"),
		evaluationTestLabel(start.Add(10*time.Minute), "hype_spike", "correct"),
	}

	evaluation := EvaluateSession(EvaluationInput{
		SessionID:   "session-1",
		GeneratedAt: generatedAt,
		Windows:     windows,
		Labels:      labels,
	})

	if evaluation.Type != SessionEvaluationType || evaluation.SessionID != "session-1" {
		t.Fatalf("unexpected evaluation identity: %#v", evaluation)
	}
	if !evaluation.GeneratedAt.Equal(generatedAt) {
		t.Fatalf("generated_at = %s, want %s", evaluation.GeneratedAt, generatedAt)
	}
	if evaluation.TotalWindows != 6 || evaluation.TotalLabeledWindows != 7 || evaluation.EvaluatedWindows != 5 {
		t.Fatalf("unexpected coverage counts: %#v", evaluation)
	}
	if evaluation.Coverage == nil || *evaluation.Coverage != 0.8333 {
		t.Fatalf("coverage = %v, want 0.8333", evaluation.Coverage)
	}
	if evaluation.UnmatchedLabels != 1 || evaluation.UncertainLabels != 1 || evaluation.InvalidLabels != 0 {
		t.Fatalf("unexpected unevaluated label counts: %#v", evaluation)
	}
	if evaluation.EventCounts.TruePositive != 1 || evaluation.EventCounts.FalsePositive != 2 || evaluation.EventCounts.FalseNegative != 2 || evaluation.EventCounts.TrueNegative != 1 {
		t.Fatalf("event counts should exclude true none matches from positive precision/recall: %#v", evaluation.EventCounts)
	}
	if evaluation.EventAccuracy == nil || *evaluation.EventAccuracy != 0.4 {
		t.Fatalf("event accuracy = %v, want 0.4", evaluation.EventAccuracy)
	}
	if evaluation.EventPrecision == nil || *evaluation.EventPrecision != 0.3333 {
		t.Fatalf("event precision = %v, want 0.3333", evaluation.EventPrecision)
	}
	if evaluation.EventRecall == nil || *evaluation.EventRecall != 0.3333 {
		t.Fatalf("event recall = %v, want 0.3333", evaluation.EventRecall)
	}
	if evaluation.EventF1 == nil || *evaluation.EventF1 != 0.3333 {
		t.Fatalf("event f1 = %v, want 0.3333", evaluation.EventF1)
	}
	if confusionCount(evaluation.EventConfusionCounts, "none", "none") != 1 {
		t.Fatalf("expected explicit none/none confusion count, got %#v", evaluation.EventConfusionCounts)
	}
	if confusionCount(evaluation.EventConfusionCounts, "none", "content_audience_divergence") != 1 {
		t.Fatalf("expected none false-positive confusion count, got %#v", evaluation.EventConfusionCounts)
	}
	if evaluation.RelationshipAccuracy != nil || len(evaluation.UnsupportedMetrics) != 1 || evaluation.UnsupportedMetrics[0].Name != "relationship_accuracy" {
		t.Fatalf("relationship accuracy should be unsupported without human relationship labels: %#v", evaluation)
	}
}

func TestEvaluateSessionReturnsNilScoresWithoutEvaluatedWindows(t *testing.T) {
	evaluation := EvaluateSession(EvaluationInput{
		SessionID: "empty",
		Windows: []SignalWindow{
			evaluationTestWindow(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), ""),
		},
	})

	if evaluation.EvaluatedWindows != 0 {
		t.Fatalf("evaluated windows = %d, want 0", evaluation.EvaluatedWindows)
	}
	if evaluation.Coverage == nil || *evaluation.Coverage != 0 {
		t.Fatalf("coverage = %v, want 0", evaluation.Coverage)
	}
	if evaluation.EventAccuracy != nil || evaluation.EventPrecision != nil || evaluation.EventRecall != nil || evaluation.EventF1 != nil {
		t.Fatalf("scores should be nil when there are no evaluated labels: %#v", evaluation)
	}
}

func TestEvaluateSessionCalculatesV1SignalMetrics(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	windows := []SignalWindow{
		evaluationTestWindow(start, SignalEventHypeSpike),
		evaluationTestWindow(start.Add(30*time.Second), SignalEventAudienceShift),
		evaluationTestWindow(start.Add(60*time.Second), ""),
		evaluationTestWindow(start.Add(90*time.Second), SignalEventHypeSpike),
	}
	windows[0].ReactionType = "hype"
	windows[0].TargetType = "character"
	windows[0].TargetText = "dragon"
	windows[0].Events[0].Timestamp = start.Add(2 * time.Second)
	windows[1].ReactionType = "neutral"
	windows[1].Events = append(windows[1].Events, SignalEvent{
		Type:      SignalEventContentAudienceDivergence,
		Timestamp: start.Add(31 * time.Second),
		Source:    "alignment",
	})
	windows[2].ReactionType = "frustration"
	windows[2].TargetType = "item"
	windows[2].TargetText = "sword"

	labels := []EvaluationLabel{
		evaluationTestLabel(start, "hype_spike", "correct"),
		evaluationTestLabel(start.Add(30*time.Second), "content_audience_divergence", "wrong"),
		evaluationTestLabel(start.Add(60*time.Second), "hype_spike", "wrong"),
		evaluationTestLabel(start.Add(90*time.Second), "none", "wrong"),
	}
	labels[0].EventPeak = start
	labels[0].ReactionType = "hype"
	labels[0].TargetType = "character"
	labels[0].TargetText = "dragon"
	labels[2].ReactionType = "hype"
	labels[2].TargetType = "item"
	labels[2].TargetText = "shield"

	evaluation := EvaluateSession(EvaluationInput{
		SessionID: "session-1",
		Windows:   windows,
		Labels:    labels,
	})

	if evaluation.HypePeakRecall == nil || *evaluation.HypePeakRecall != 0.5 {
		t.Fatalf("hype peak recall = %v, want 0.5", evaluation.HypePeakRecall)
	}
	if evaluation.PeakRecall == nil || *evaluation.PeakRecall != 1 {
		t.Fatalf("peak recall = %v, want 1", evaluation.PeakRecall)
	}
	if evaluation.EventOnsetLatencyMS == nil || *evaluation.EventOnsetLatencyMS != 2000 {
		t.Fatalf("event onset latency = %v, want 2000", evaluation.EventOnsetLatencyMS)
	}
	if evaluation.OnsetLatencyMS == nil || *evaluation.OnsetLatencyMS != 2000 {
		t.Fatalf("onset latency alias = %v, want 2000", evaluation.OnsetLatencyMS)
	}
	if evaluation.ReactionTypeAccuracy == nil || *evaluation.ReactionTypeAccuracy != 0.5 {
		t.Fatalf("reaction type accuracy = %v, want 0.5", evaluation.ReactionTypeAccuracy)
	}
	if evaluation.ReactionTypeF1 == nil || *evaluation.ReactionTypeF1 != 0.3334 {
		t.Fatalf("reaction type f1 = %v, want 0.3334", evaluation.ReactionTypeF1)
	}
	hypeMetric := metricForLabel(evaluation.ReactionTypeMetrics, "hype")
	if hypeMetric == nil || hypeMetric.Support != 2 || hypeMetric.PredictedCount != 1 || hypeMetric.Recall == nil || *hypeMetric.Recall != 0.5 {
		t.Fatalf("unexpected hype reaction metric: %#v", hypeMetric)
	}
	if evaluation.TargetExtractionAccuracy == nil || *evaluation.TargetExtractionAccuracy != 0.5 {
		t.Fatalf("target extraction accuracy = %v, want 0.5", evaluation.TargetExtractionAccuracy)
	}
	if evaluation.TargetAccuracy == nil || *evaluation.TargetAccuracy != 0.5 {
		t.Fatalf("target accuracy alias = %v, want 0.5", evaluation.TargetAccuracy)
	}
	if evaluation.DivergenceAccuracy == nil || *evaluation.DivergenceAccuracy != 1 {
		t.Fatalf("divergence accuracy = %v, want 1", evaluation.DivergenceAccuracy)
	}
	if evaluation.FalsePositivesNormalChat != 1 {
		t.Fatalf("false positives normal chat = %d, want 1", evaluation.FalsePositivesNormalChat)
	}
}

func TestEvaluateSessionMatchesRoundedTimestampLabelsAndUsesStoredPrediction(t *testing.T) {
	start := time.Date(2026, 5, 1, 12, 0, 0, 123456789, time.UTC)
	window := evaluationTestWindow(start, SignalEventHypeSpike)
	label := evaluationTestLabel(start.Round(time.Microsecond), "none", "correct")
	label.WindowEnd = start.Add(30 * time.Second).Round(time.Microsecond)
	label.PredictedEvent = "none"

	evaluation := EvaluateSession(EvaluationInput{
		SessionID: "session-1",
		Windows:   []SignalWindow{window},
		Labels:    []EvaluationLabel{label},
	})

	if evaluation.EvaluatedWindows != 1 || evaluation.UnmatchedLabels != 0 {
		t.Fatalf("rounded timestamp label should match window: %#v", evaluation)
	}
	if evaluation.EventCounts.TrueNegative != 1 || evaluation.EventAccuracy == nil || *evaluation.EventAccuracy != 1 {
		t.Fatalf("stored predicted event should drive evaluation counts: %#v", evaluation)
	}
}

func evaluationTestWindow(start time.Time, eventType SignalEventType) SignalWindow {
	window := SignalWindow{
		Type:        "signal_window",
		SessionID:   "session-1",
		ChannelID:   "channel-1",
		WindowStart: start,
		WindowEnd:   start.Add(30 * time.Second),
	}
	if eventType != "" {
		window.FirstEventType = eventType
		window.Events = []SignalEvent{{Type: eventType, Timestamp: start, Severity: 0.8}}
	}
	return window
}

func evaluationTestLabel(start time.Time, eventLabel, correctness string) EvaluationLabel {
	return EvaluationLabel{
		SessionID:   "session-1",
		WindowStart: start,
		WindowEnd:   start.Add(30 * time.Second),
		EventLabel:  eventLabel,
		Correctness: correctness,
		CreatedAt:   start.Add(time.Hour),
	}
}

func confusionCount(counts []EventConfusionCount, actual, predicted string) int {
	for _, count := range counts {
		if count.Actual == actual && count.Predicted == predicted {
			return count.Count
		}
	}
	return 0
}

func metricForLabel(metrics []EventLabelMetric, label string) *EventLabelMetric {
	for index := range metrics {
		if metrics[index].Label == label {
			return &metrics[index]
		}
	}
	return nil
}
