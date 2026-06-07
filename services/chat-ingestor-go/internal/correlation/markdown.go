package correlation

import (
	"fmt"
	"strings"
)

func RenderMarkdown(report Report) string {
	var lines []string
	lines = append(lines,
		"# Signal Correlation Report",
		"",
		fmt.Sprintf("Generated at: `%s`", report.GeneratedAt.Format("2006-01-02T15:04:05Z07:00")),
		"",
		"Scope: aligned chat/transcript sentiment pairs from stored replay data. Correlation is descriptive evidence, not a causality or accuracy claim.",
		"",
		"## Summary",
		"",
		fmt.Sprintf("- Sessions: %d", report.DataQuality.SessionCount),
		fmt.Sprintf("- Aligned pairs: %d", report.DataQuality.AlignmentPairCount),
		fmt.Sprintf("- Calm baseline pairs: %d", report.DataQuality.BaselinePairCount),
		fmt.Sprintf("- Detected divergence pairs: %d", report.DataQuality.DetectedDivergenceCount),
		fmt.Sprintf("- Manual validation sample rows: %d", len(report.ManualValidationSample)),
		"",
		"## Aggregate Cohorts",
		"",
		"| Cohort | Pairs | Pearson | Spearman | Avg abs delta | Abs delta p95 | Avg quality | Status |",
		"| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
	)
	for _, cohort := range report.Aggregate {
		lines = append(lines, cohortRow(cohort))
	}

	lines = append(lines,
		"",
		"## Baseline Calibration",
		"",
		fmt.Sprintf("- Status: `%s`", report.Calibration.Status),
		fmt.Sprintf("- Current divergence threshold: %.4f", report.Calibration.CurrentThreshold),
		fmt.Sprintf("- Recommended threshold from calm-window p95: %s", formatFloat(report.Calibration.RecommendedThreshold)),
		fmt.Sprintf("- Baseline abs-delta median / p90 / p95: %s / %s / %s", formatFloat(report.Calibration.BaselineAbsDeltaMedian), formatFloat(report.Calibration.BaselineAbsDeltaP90), formatFloat(report.Calibration.BaselineAbsDeltaP95)),
		fmt.Sprintf("- Detected divergence rows above baseline p95: %d (%s)", report.Calibration.DetectedAboveBaselineP95, formatFloat(report.Calibration.DetectedAboveBaselineP95Rate)),
		fmt.Sprintf("- Threshold assessment: `%s`", report.Calibration.ThresholdAssessment),
		"",
		"## Baseline vs Divergence",
		"",
		fmt.Sprintf("- Status: `%s`", report.Comparison.Status),
		fmt.Sprintf("- Pearson drop from `%s` to `%s`: %s", report.Comparison.BaselineCohort, report.Comparison.EventCohort, formatFloat(report.Comparison.PearsonDrop)),
		fmt.Sprintf("- Spearman drop from `%s` to `%s`: %s", report.Comparison.BaselineCohort, report.Comparison.EventCohort, formatFloat(report.Comparison.SpearmanDrop)),
		fmt.Sprintf("- Average absolute delta change: %s", formatFloat(report.Comparison.AverageAbsDeltaChange)),
		"",
		"## Lag Analysis",
		"",
		"| Lag seconds | Pair count | Pearson | Spearman | Avg abs delta | Status |",
		"| ---: | ---: | ---: | ---: | ---: | --- |",
	)
	for _, lag := range report.LagAnalysis {
		lines = append(lines, fmt.Sprintf(
			"| %d | %d | %s | %s | %s | %s |",
			lag.LagSeconds,
			lag.PairCount,
			formatFloat(lag.Pearson),
			formatFloat(lag.Spearman),
			formatFloat(lag.AverageAbsDelta),
			escapeTable(lag.CorrelationStatus),
		))
	}
	if report.BestLag != nil {
		lines = append(lines, "", fmt.Sprintf("Best absolute Pearson lag: `%d` seconds (%s).", report.BestLag.LagSeconds, formatFloat(report.BestLag.Pearson)))
	}

	lines = append(lines,
		"",
		"## Negative Control",
		"",
		fmt.Sprintf("- Status: `%s`", report.NegativeControl.Status),
		fmt.Sprintf("- Observed Pearson / shuffled Pearson: %s / %s", formatFloat(report.NegativeControl.ObservedPearson), formatFloat(report.NegativeControl.ShuffledPearson)),
		fmt.Sprintf("- Observed Spearman / shuffled Spearman: %s / %s", formatFloat(report.NegativeControl.ObservedSpearman), formatFloat(report.NegativeControl.ShuffledSpearman)),
		fmt.Sprintf("- Interpretation: %s", report.NegativeControl.Interpretation),
		"",
		"## Data Quality",
		"",
		"| Metric | Count |",
		"| --- | ---: |",
		fmt.Sprintf("| Chat buckets | %d |", report.DataQuality.ChatBucketCount),
		fmt.Sprintf("| Transcript buckets | %d |", report.DataQuality.TranscriptBucketCount),
		fmt.Sprintf("| Low-quality alignments | %d |", report.DataQuality.LowQualityAlignmentCount),
		fmt.Sprintf("| Low chat volume | %d |", report.DataQuality.LowChatVolumeCount),
		fmt.Sprintf("| Short transcript text | %d |", report.DataQuality.ShortTranscriptCount),
		fmt.Sprintf("| Low transcript confidence | %d |", report.DataQuality.LowTranscriptConfidence),
		fmt.Sprintf("| Empty transcripts | %d |", report.DataQuality.EmptyTranscriptCount),
		fmt.Sprintf("| Missing transcript sentiment | %d |", report.DataQuality.MissingTranscriptSentiment),
		fmt.Sprintf("| Partial overlap alignments | %d |", report.DataQuality.PartialOverlapCount),
		"",
		"## Sessions",
		"",
		"| Session | Channel | Pairs | Calm Pearson | Divergence Pearson | Calm pairs | Divergence pairs |",
		"| --- | --- | ---: | ---: | ---: | ---: | ---: |",
	)
	for _, session := range report.Sessions {
		calm, _ := findCohort(session.Cohorts, CohortCalmBaseline)
		divergence, _ := findCohort(session.Cohorts, CohortDetectedDivergence)
		lines = append(lines, fmt.Sprintf(
			"| %s | %s | %d | %s | %s | %d | %d |",
			escapeTable(session.SessionID),
			escapeTable(session.ChannelID),
			session.PairCount,
			formatFloat(calm.Pearson),
			formatFloat(divergence.Pearson),
			calm.PairCount,
			divergence.PairCount,
		))
	}

	if len(report.Limitations) > 0 {
		lines = append(lines, "", "## Limitations", "")
		for _, limitation := range report.Limitations {
			lines = append(lines, "- "+limitation)
		}
	}
	lines = append(lines, "")
	return strings.Join(lines, "\n")
}

func cohortRow(cohort CohortSummary) string {
	return fmt.Sprintf(
		"| %s | %d | %s | %s | %s | %s | %s | %s |",
		escapeTable(cohort.Name),
		cohort.PairCount,
		formatFloat(cohort.Pearson),
		formatFloat(cohort.Spearman),
		formatFloat(cohort.AverageAbsDelta),
		formatFloat(cohort.AbsDeltaP95),
		formatFloat(cohort.AverageQuality),
		escapeTable(cohort.CorrelationStatus),
	)
}

func formatFloat(value *float64) string {
	if value == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.4f", *value)
}

func escapeTable(value string) string {
	if strings.TrimSpace(value) == "" {
		return "n/a"
	}
	return strings.ReplaceAll(value, "|", "\\|")
}
