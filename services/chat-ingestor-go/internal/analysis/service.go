package analysis

import (
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

type AnalyzerConfig struct {
	AlignmentWindow time.Duration
}

type Analyzer struct {
	alignmentWindow time.Duration
}

type BucketAnalysisInput struct {
	SessionID         string
	ChatBuckets       []chat.ChatBucket
	TranscriptBuckets []TranscriptBucket
}

type SessionAnalysisInput struct {
	SessionID         string
	ChatBuckets       []chat.ChatBucket
	TranscriptBuckets []TranscriptBucket
	Alignments        []AlignmentBucket
}

type Result struct {
	Alignments     []AlignmentBucket
	SignalWindows  []SignalWindow
	SignalEvents   []SignalEvent
	Insights       []Insight
	InsightSummary SessionInsightSummary
}

func NewAnalyzer(config AnalyzerConfig) Analyzer {
	return Analyzer{alignmentWindow: config.AlignmentWindow}
}

func (a Analyzer) AnalyzeBuckets(input BucketAnalysisInput) Result {
	alignments := ComputeAlignments(input.ChatBuckets, input.TranscriptBuckets, a.alignmentWindow)
	return buildResult(input.SessionID, input.ChatBuckets, input.TranscriptBuckets, alignments)
}

func (a Analyzer) AnalyzeSession(input SessionAnalysisInput) Result {
	alignments := append([]AlignmentBucket(nil), input.Alignments...)
	return buildResult(input.SessionID, input.ChatBuckets, input.TranscriptBuckets, alignments)
}

func buildResult(sessionID string, chatBuckets []chat.ChatBucket, transcriptBuckets []TranscriptBucket, alignments []AlignmentBucket) Result {
	windows := SignalWindowsFromChatTranscriptAndAlignments(chatBuckets, transcriptBuckets, alignments)
	events := SignalEventsFromWindows(windows)
	insights, insightSummary := GenerateSessionInsights(sessionID, windows, chatBuckets, transcriptBuckets, alignments)
	return Result{
		Alignments:     alignments,
		SignalWindows:  windows,
		SignalEvents:   events,
		Insights:       insights,
		InsightSummary: insightSummary,
	}
}

func SignalEventsFromWindows(windows []SignalWindow) []SignalEvent {
	var events []SignalEvent
	for _, window := range windows {
		events = append(events, window.Events...)
	}
	return events
}
