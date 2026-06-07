package storage

import (
	"math"
	"sort"
	"strings"
	"time"
)

const ReplayProofType = "replay_proof"

var defaultReplayProofSpeeds = []float64{1, 5, 10}

func BuildReplayProof(replay SessionReplay, opts ReplayProofOptions) ReplayProof {
	generatedAt := opts.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	} else {
		generatedAt = generatedAt.UTC()
	}

	signalWindowKeys := replaySignalWindowKeys(replay)
	bucketCount := len(replay.ChatBuckets) + len(replay.TranscriptBuckets) + len(replay.Alignments)
	sourceBucketCount := len(replay.ChatBuckets) + len(replay.TranscriptBuckets)
	timelineStart, timelineEnd, sourceDurationMS := replayTimeline(replay)
	sessionTotals := replayProofSessionTotals(replay)
	truncatedSources := replayProofTruncatedSources(replay, sessionTotals)

	proof := ReplayProof{
		Type:                  ReplayProofType,
		SessionID:             replay.Session.SessionID,
		ChannelID:             replay.Session.ChannelID,
		GeneratedAt:           generatedAt,
		ReplayLimit:           opts.ReplayLimit,
		Partial:               len(truncatedSources) > 0,
		SessionTotals:         sessionTotals,
		TruncatedSources:      truncatedSources,
		BucketCount:           bucketCount,
		SourceBucketCount:     sourceBucketCount,
		ChatBucketCount:       len(replay.ChatBuckets),
		TranscriptBucketCount: len(replay.TranscriptBuckets),
		AlignmentCount:        len(replay.Alignments),
		SignalWindowCount:     len(signalWindowKeys),
		MatchedWindows:        replayMatchedWindowCount(replay.Alignments),
		LabelCoverage:         replayLabelCoverage(replay, signalWindowKeys),
		TranscriptCoverage:    replayTranscriptCoverage(replay.TranscriptBuckets),
		Timeline: ReplayProofTimeline{
			Start:            timelineStart,
			End:              timelineEnd,
			SourceDurationMS: sourceDurationMS,
		},
		Latency: replayLatency(replay),
		UnsupportedMetrics: []ReplayProofUnsupported{
			{
				Name:   "dropped_event_rate",
				Reason: "stored replay data does not include expected event counts or event drop counters",
			},
		},
	}

	for _, speed := range replayProofSpeeds(opts.Speeds) {
		proof.Speeds = append(proof.Speeds, replayProofSpeed(speed, sourceDurationMS, bucketCount, len(signalWindowKeys)))
	}
	return proof
}

func replayProofSessionTotals(replay SessionReplay) ReplayProofSessionTotals {
	chatBucketCount := maxInt(replay.Session.ChatBucketCount, len(replay.ChatBuckets))
	transcriptBucketCount := maxInt(replay.Session.TranscriptBucketCount, len(replay.TranscriptBuckets))
	alignmentCount := maxInt(replay.Session.AlignmentCount, len(replay.Alignments))
	return ReplayProofSessionTotals{
		BucketCount:            chatBucketCount + transcriptBucketCount + alignmentCount,
		SourceBucketCount:      chatBucketCount + transcriptBucketCount,
		ChatBucketCount:        chatBucketCount,
		TranscriptBucketCount:  transcriptBucketCount,
		AlignmentCount:         alignmentCount,
		SignalWindowLabelCount: replaySignalWindowLabelCount(replay.WindowLabels),
	}
}

func replayProofTruncatedSources(replay SessionReplay, totals ReplayProofSessionTotals) []ReplayProofTruncation {
	var out []ReplayProofTruncation
	if loaded := len(replay.ChatBuckets); loaded < totals.ChatBucketCount {
		out = append(out, ReplayProofTruncation{Source: "chat_buckets", LoadedCount: loaded, TotalCount: totals.ChatBucketCount})
	}
	if loaded := len(replay.TranscriptBuckets); loaded < totals.TranscriptBucketCount {
		out = append(out, ReplayProofTruncation{Source: "transcript_buckets", LoadedCount: loaded, TotalCount: totals.TranscriptBucketCount})
	}
	if loaded := len(replay.Alignments); loaded < totals.AlignmentCount {
		out = append(out, ReplayProofTruncation{Source: "alignments", LoadedCount: loaded, TotalCount: totals.AlignmentCount})
	}
	return out
}

func replayTranscriptCoverage(buckets []TranscriptBucket) ReplayProofTranscriptCoverage {
	out := ReplayProofTranscriptCoverage{
		BucketCount:  len(buckets),
		StatusCounts: map[string]int{},
	}
	var emptyRatioWeightedSum float64
	var emptyRatioWeight float64
	var repairChangedRatioSum float64
	var repairChangedRatioCount int
	for _, bucket := range buckets {
		audioSeconds := bucket.AudioSeconds
		if audioSeconds == 0 && bucket.AudioStartedAt != nil && bucket.AudioEndedAt != nil && bucket.AudioEndedAt.After(*bucket.AudioStartedAt) {
			audioSeconds = bucket.AudioEndedAt.Sub(*bucket.AudioStartedAt).Seconds()
		}
		expectedSeconds := bucket.BucketEnd.Sub(bucket.BucketStart).Seconds()
		if expectedSeconds < 0 {
			expectedSeconds = 0
		}
		segmentCount := bucket.SegmentCount
		if segmentCount == 0 {
			segmentCount = len(bucket.Segments)
		}
		wordCount := bucket.WordCount
		if wordCount == 0 && strings.TrimSpace(bucket.Text) != "" {
			wordCount = len(strings.Fields(bucket.Text))
		}

		out.AudioSeconds += audioSeconds
		out.ExpectedAudioSeconds += expectedSeconds
		out.SegmentCount += segmentCount
		out.WordCount += wordCount
		out.RepairAddedWords += bucket.RepairAddedWords
		weight := audioSeconds
		if weight <= 0 {
			weight = expectedSeconds
		}
		if weight > 0 {
			emptyRatioWeightedSum += bucket.EmptyRatio * weight
			emptyRatioWeight += weight
		}
		if transcriptRepairChangedRatioObserved(bucket) {
			repairChangedRatioSum += bucket.RepairChangedRatio
			repairChangedRatioCount++
		}
		status := proofStatus(bucket.TranscriptStatus)
		if status == "missing" {
			status = transcriptStatusFromQuality(bucket.Quality)
			if status == "" {
				status = proofStatus(bucket.SentimentStatus)
			}
		}
		out.StatusCounts[proofStatus(status)]++
	}
	if out.ExpectedAudioSeconds > 0 {
		coverage := roundProofFloat(out.AudioSeconds / out.ExpectedAudioSeconds)
		out.AudioCoverage = &coverage
	}
	if emptyRatioWeight > 0 {
		emptyRatio := roundProofFloat(emptyRatioWeightedSum / emptyRatioWeight)
		out.EmptyRatio = &emptyRatio
	}
	if repairChangedRatioCount > 0 {
		changedRatio := roundProofFloat(repairChangedRatioSum / float64(repairChangedRatioCount))
		out.AverageRepairChangedRatio = &changedRatio
	}
	out.RepairImprovement = transcriptRepairImprovement(out)
	out.AudioSeconds = roundProofFloat(out.AudioSeconds)
	out.ExpectedAudioSeconds = roundProofFloat(out.ExpectedAudioSeconds)
	out.StatusCounts = emptyMapAsNil(out.StatusCounts)
	return out
}

func transcriptRepairImprovement(coverage ReplayProofTranscriptCoverage) *float64 {
	var values []float64
	if coverage.WordCount > 0 && coverage.RepairAddedWords > 0 {
		values = append(values, float64(coverage.RepairAddedWords)/float64(coverage.WordCount))
	}
	if coverage.AverageRepairChangedRatio != nil {
		values = append(values, *coverage.AverageRepairChangedRatio)
	}
	if len(values) == 0 {
		return nil
	}
	var best float64
	for _, value := range values {
		if value > best {
			best = value
		}
	}
	value := roundProofFloat(best)
	return &value
}

func DefaultReplayProofSpeeds() []float64 {
	return append([]float64(nil), defaultReplayProofSpeeds...)
}

func replayProofSpeeds(input []float64) []float64 {
	if len(input) == 0 {
		return DefaultReplayProofSpeeds()
	}
	seen := map[float64]struct{}{}
	out := make([]float64, 0, len(input))
	for _, speed := range input {
		if speed <= 0 || math.IsInf(speed, 0) || math.IsNaN(speed) {
			continue
		}
		if _, ok := seen[speed]; ok {
			continue
		}
		seen[speed] = struct{}{}
		out = append(out, speed)
	}
	if len(out) == 0 {
		return DefaultReplayProofSpeeds()
	}
	sort.Float64s(out)
	return out
}

func replayProofSpeed(speed float64, sourceDurationMS int64, bucketCount, signalWindowCount int) ReplayProofSpeed {
	item := ReplayProofSpeed{Speed: speed}
	if sourceDurationMS <= 0 {
		return item
	}

	estimatedMS := int64(math.Ceil(float64(sourceDurationMS) / speed))
	estimatedSeconds := float64(estimatedMS) / 1000
	item.EstimatedReplayDurationMS = estimatedMS
	item.EstimatedReplaySeconds = roundProofFloat(estimatedSeconds)
	if estimatedSeconds > 0 {
		item.WindowsPerSecond = roundProofFloat(float64(signalWindowCount) / estimatedSeconds)
		item.BucketsPerSecond = roundProofFloat(float64(bucketCount) / estimatedSeconds)
	}
	return item
}

func replaySignalWindowKeys(replay SessionReplay) map[string]struct{} {
	out := map[string]struct{}{}
	if len(replay.ChatBuckets) > 0 {
		for _, bucket := range replay.ChatBuckets {
			out[proofWindowKey(bucket.SessionID, bucket.BucketStart, bucket.BucketEnd)] = struct{}{}
			for _, subwindow := range bucket.Subwindows {
				if subwindow.WindowStart.IsZero() || subwindow.WindowEnd.IsZero() {
					continue
				}
				out[proofWindowKey(bucket.SessionID, subwindow.WindowStart, subwindow.WindowEnd)] = struct{}{}
			}
		}
	}
	for _, alignment := range replay.Alignments {
		out[proofWindowKey(alignment.SessionID, alignment.WindowStart, alignment.WindowEnd)] = struct{}{}
	}
	return out
}

func replayMatchedWindowCount(alignments []AlignmentBucket) int {
	keys := map[string]struct{}{}
	for _, alignment := range alignments {
		key := strings.Join([]string{
			alignment.SessionID,
			alignment.ChannelID,
			alignment.WindowStart.UTC().Format(time.RFC3339Nano),
			alignment.WindowEnd.UTC().Format(time.RFC3339Nano),
			alignment.ChatBucketStart.UTC().Format(time.RFC3339Nano),
			alignment.ChatBucketEnd.UTC().Format(time.RFC3339Nano),
			alignment.TranscriptBucketStart.UTC().Format(time.RFC3339Nano),
			alignment.TranscriptBucketEnd.UTC().Format(time.RFC3339Nano),
		}, ":")
		keys[key] = struct{}{}
	}
	return len(keys)
}

func replayLabelCoverage(replay SessionReplay, signalWindowKeys map[string]struct{}) ReplayProofLabelCoverage {
	labelKeys := map[string]struct{}{}
	for _, label := range replay.WindowLabels {
		labelKeys[proofWindowKey(label.SessionID, label.WindowStart, label.WindowEnd)] = struct{}{}
	}

	var labeledWindows int
	var unmatchedLabels int
	for key := range labelKeys {
		if _, ok := signalWindowKeys[key]; ok {
			labeledWindows++
			continue
		}
		unmatchedLabels++
	}

	coverage := ReplayProofLabelCoverage{
		LabeledWindows:   labeledWindows,
		UnmatchedLabels:  unmatchedLabels,
		TotalWindows:     len(signalWindowKeys),
		StoredLabelCount: len(labelKeys),
	}
	if len(signalWindowKeys) > 0 {
		value := roundProofFloat(float64(labeledWindows) / float64(len(signalWindowKeys)))
		coverage.Coverage = &value
	}
	return coverage
}

func replaySignalWindowLabelCount(labels []SignalWindowLabel) int {
	labelKeys := map[string]struct{}{}
	for _, label := range labels {
		labelKeys[proofWindowKey(label.SessionID, label.WindowStart, label.WindowEnd)] = struct{}{}
	}
	return len(labelKeys)
}

func replayTimeline(replay SessionReplay) (*time.Time, *time.Time, int64) {
	var start time.Time
	var end time.Time
	var observed bool

	observe := func(value time.Time) {
		if value.IsZero() {
			return
		}
		value = value.UTC()
		if !observed || value.Before(start) {
			start = value
		}
		if !observed || value.After(end) {
			end = value
		}
		observed = true
	}

	for _, bucket := range replay.ChatBuckets {
		observe(bucket.BucketStart)
		observe(bucket.BucketEnd)
	}
	for _, bucket := range replay.TranscriptBuckets {
		observe(bucket.BucketStart)
		observe(bucket.BucketEnd)
	}
	for _, alignment := range replay.Alignments {
		observe(alignment.WindowStart)
		observe(alignment.WindowEnd)
		observe(alignment.ChatBucketStart)
		observe(alignment.ChatBucketEnd)
		observe(alignment.TranscriptBucketStart)
		observe(alignment.TranscriptBucketEnd)
	}
	if !observed {
		observe(replay.Session.StartedAt)
		if replay.Session.EndedAt != nil {
			observe(*replay.Session.EndedAt)
		}
	}
	if !observed {
		return nil, nil, 0
	}

	duration := end.Sub(start)
	if duration < 0 {
		duration = 0
	}
	return &start, &end, int64(duration / time.Millisecond)
}

func replayLatency(replay SessionReplay) ReplayProofLatency {
	chatLatencies := make([]int64, 0, len(replay.ChatBuckets))
	chatStatusCounts := map[string]int{}
	for _, bucket := range replay.ChatBuckets {
		if bucket.AnalysisLatencyMS > 0 {
			chatLatencies = append(chatLatencies, bucket.AnalysisLatencyMS)
		}
		chatStatusCounts[proofStatus(bucket.AnalysisStatus)]++
	}

	transcriptLatencies := make([]int64, 0, len(replay.TranscriptBuckets))
	transcriptASRLatencies := make([]int64, 0, len(replay.TranscriptBuckets))
	transcriptPipelineLatencies := make([]int64, 0, len(replay.TranscriptBuckets))
	transcriptStatusCounts := map[string]int{}
	for _, bucket := range replay.TranscriptBuckets {
		if bucket.SentimentLatencyMS != nil {
			transcriptLatencies = append(transcriptLatencies, *bucket.SentimentLatencyMS)
		}
		if bucket.ASRLatencyMS != nil {
			transcriptASRLatencies = append(transcriptASRLatencies, *bucket.ASRLatencyMS)
		}
		if bucket.PipelineLatencyMS != nil {
			transcriptPipelineLatencies = append(transcriptPipelineLatencies, *bucket.PipelineLatencyMS)
		}
		transcriptStatusCounts[proofStatus(bucket.SentimentStatus)]++
	}

	return ReplayProofLatency{
		ChatAnalysisLatencyMS:           replayLatencySummary(chatLatencies, len(replay.ChatBuckets)),
		TranscriptSentimentLatencyMS:    replayLatencySummary(transcriptLatencies, len(replay.TranscriptBuckets)),
		TranscriptASRLatencyMS:          replayLatencySummary(transcriptASRLatencies, len(replay.TranscriptBuckets)),
		TranscriptPipelineLatencyMS:     replayLatencySummary(transcriptPipelineLatencies, len(replay.TranscriptBuckets)),
		ChatAnalysisStatusCounts:        emptyMapAsNil(chatStatusCounts),
		TranscriptSentimentStatusCounts: emptyMapAsNil(transcriptStatusCounts),
	}
}

func replayLatencySummary(values []int64, total int) ReplayProofLatencySummary {
	summary := ReplayProofLatencySummary{
		AvailableCount: len(values),
		MissingCount:   total - len(values),
	}
	if len(values) == 0 {
		return summary
	}

	ordered := append([]int64(nil), values...)
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left] < ordered[right]
	})

	var sum int64
	for _, value := range ordered {
		sum += value
	}
	minValue := ordered[0]
	maxValue := ordered[len(ordered)-1]
	average := roundProofFloat(float64(sum) / float64(len(ordered)))
	p50 := replayPercentile(ordered, 0.50)
	p95 := replayPercentile(ordered, 0.95)

	summary.Min = &minValue
	summary.Max = &maxValue
	summary.Average = &average
	summary.P50 = &p50
	summary.P95 = &p95
	return summary
}

func replayPercentile(ordered []int64, percentile float64) float64 {
	if len(ordered) == 0 {
		return 0
	}
	index := int(math.Ceil(percentile*float64(len(ordered)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return float64(ordered[index])
}

func proofWindowKey(sessionID string, start, end time.Time) string {
	return strings.Join([]string{
		strings.TrimSpace(sessionID),
		start.UTC().Format(time.RFC3339Nano),
		end.UTC().Format(time.RFC3339Nano),
	}, ":")
}

func proofStatus(value string) string {
	status := strings.ToLower(strings.TrimSpace(value))
	if status == "" {
		return "missing"
	}
	return status
}

func transcriptRepairChangedRatioObserved(bucket TranscriptBucket) bool {
	if bucket.RepairChangedRatio != 0 || bucket.RepairAddedWords != 0 {
		return true
	}
	if bucket.Quality == nil {
		return false
	}
	if qualityBool(bucket.Quality, "repaired") {
		return true
	}
	if strings.TrimSpace(qualityString(bucket.Quality, "original_live_text")) != "" {
		return true
	}
	repairStatus := strings.ToLower(strings.TrimSpace(qualityString(bucket.Quality, "repair_status")))
	return repairStatus == "completed" && qualityHasKey(bucket.Quality, "repair_changed_ratio")
}

func emptyMapAsNil(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	return values
}

func roundProofFloat(value float64) float64 {
	return math.Round(value*10000) / 10000
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
