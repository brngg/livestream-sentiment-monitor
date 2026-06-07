export type StreamInfo = {
  id?: string;
  user_id?: string;
  platform?: string;
  url?: string;
  title?: string;
  game?: string;
  viewer_count?: number;
  started_at?: string;
  language?: string;
  thumbnail_url?: string;
};

export type ChatMessage = {
  session_id?: string;
  channel_id?: string;
  message_id?: string;
  timestamp?: string;
  username?: string;
  display_name?: string;
  text?: string;
};

export type MessageScore = {
  message_id?: string;
  timestamp?: string;
  username?: string;
  display_name?: string;
  text?: string;
  label?: string;
  confidence?: number;
  sentiment_score?: number;
  human_label?: string;
};

export type TranscriptQuality = {
  raw_segment_count?: number;
  retained_segment_count?: number;
  dropped_low_confidence_count?: number;
  dropped_repeat_count?: number;
  retained_ratio?: number;
  language_probability?: number;
  stable?: boolean;
  backpressure?: boolean;
  source_partial_text?: string;
  source?: string;
  final?: boolean;
  repaired?: boolean;
  audio_coverage_seconds?: number;
  audio_seconds?: number;
  bucket_duration_seconds?: number;
  char_count?: number;
  word_count?: number;
  empty_ratio?: number;
  repair_added_words?: number;
  repair_changed_ratio?: number;
  repair_latency_ms?: number;
  repair_action?: string;
  repair_status?: string;
  status?: string;
};

export type TranscriptWord = {
  start?: number;
  end?: number;
  text?: string;
  probability?: number | null;
};

export type ChatBucketSubwindow = {
  window_start?: string;
  window_end?: string;
  message_count?: number;
  reaction_score?: number;
  hype_score?: number;
  intensity_score?: number;
  confusion_score?: number;
  frustration_score?: number;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  divergence_type?: string;
  event_start?: string;
  event_peak?: string;
  source?: string;
  event_hint?: string;
  confidence?: number;
  evidence_ids?: string[];
};

export type ChatBucket = {
  type?: string;
  session_id?: string;
  channel_id?: string;
  bucket_start?: string;
  bucket_end?: string;
  message_count?: number;
  unique_chatters?: number;
  chat_sentiment?: number;
  sentiment_confidence?: number;
  analyzed_count?: number;
  analysis_status?: string;
  sentiment_model?: string;
  positive?: number;
  neutral?: number;
  negative?: number;
  analysis_latency_ms?: number;
  language_mix?: Record<string, number>;
  top_terms?: string[];
  top_emotes?: string[];
  message_scores?: MessageScore[];
  peak_reaction_score?: number;
  peak_reaction_type?: string;
  peak_target_type?: string;
  peak_target_text?: string;
  peak_source?: string;
  peak_event_hint?: string;
  peak_confidence?: number;
  peak_evidence_ids?: string[];
  peak_time?: string;
  peak_window_start?: string;
  peak_window_end?: string;
  subwindows?: ChatBucketSubwindow[];
  peak_evidence_messages?: ChatMessage[];
};

export type ReactionWindow = {
  type?: string;
  session_id?: string;
  channel_id?: string;
  source?: "chat" | "transcript" | string;
  provisional?: boolean;
  window_start?: string;
  window_end?: string;
  message_count?: number;
  unique_chatters?: number;
  messages_per_minute?: number;
  velocity_score?: number;
  hype_score?: number;
  intensity_score?: number;
  confusion_score?: number;
  frustration_score?: number;
  valence?: number;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  event_hint?: string;
  confidence?: number;
  evidence_ids?: string[];
  transcript_text?: string;
  transcript_confidence?: number;
  sentiment_confidence?: number;
  evidence_messages?: ChatMessage[];
};

export type TranscriptSegment = {
  type?: string;
  session_id?: string;
  channel_id?: string;
  transcript_start?: string;
  transcript_end?: string;
  audio_started_at?: string | null;
  audio_ended_at?: string | null;
  transcribed_at?: string | null;
  asr_latency_ms?: number | null;
  pipeline_latency_ms?: number | null;
  text?: string;
  language?: string;
  confidence?: number;
  words?: TranscriptWord[];
  transcript_confidence?: number;
  quality?: TranscriptQuality;
};

export type TranscriptBucket = {
  type?: string;
  session_id?: string;
  channel_id?: string;
  bucket_start?: string;
  bucket_end?: string;
  audio_started_at?: string | null;
  audio_ended_at?: string | null;
  transcribed_at?: string | null;
  asr_latency_ms?: number | null;
  pipeline_latency_ms?: number | null;
  text?: string;
  language?: string;
  transcript_confidence?: number;
  transcript_status?: "live" | "repairing" | "final" | "degraded" | string;
  sentiment_score?: number;
  sentiment_confidence?: number;
  sentiment_label?: string;
  sentiment_model?: string;
  sentiment_status?: string;
  sentiment_latency_ms?: number;
  audio_seconds?: number;
  segment_count?: number;
  word_count?: number;
  empty_ratio?: number;
  repair_added_words?: number;
  repair_changed_ratio?: number;
  segments?: Array<{ text?: string; start?: number; end?: number; confidence?: number; words?: TranscriptWord[] }>;
  quality?: TranscriptQuality;
};

export type AlignmentBucket = {
  type?: string;
  session_id?: string;
  channel_id?: string;
  window_start?: string;
  window_end?: string;
  chat_bucket_start?: string;
  chat_bucket_end?: string;
  transcript_bucket_start?: string;
  transcript_bucket_end?: string;
  chat_sentiment?: number;
  chat_confidence?: number;
  chat_message_count?: number;
  transcript_sentiment?: number;
  transcript_confidence?: number;
  transcript_text_length?: number;
  delta?: number;
  similarity?: number;
  relationship?: "converged" | "soft_split" | "diverged" | string;
  overlap_seconds?: number;
  quality?: number;
  quality_flags?: string[];
};

export type SignalEvent = {
  type?: string;
  source?: "chat" | "transcript" | "alignment" | string;
  timestamp?: string;
  severity?: number;
  label?: string;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  event_hint?: string;
  text?: string;
  score?: number;
  confidence?: number;
  weight?: number;
  evidence_ids?: string[];
  meta?: Record<string, unknown>;
  message?: MessageScore;
  transcript_segment?: TranscriptSegment;
};

export type SignalWindow = {
  type?: string;
  session_id?: string;
  channel_id?: string;
  source?: string;
  stream_id?: string;
  window_start?: string;
  window_end?: string;
  message_count?: number;
  unique_chatters?: number;
  chat_sentiment?: number;
  chat_confidence?: number;
  sentiment_confidence?: number;
  positive?: number;
  neutral?: number;
  negative?: number;
  previous_sentiment?: number;
  chat_message_count?: number;
  transcript_sentiment?: number;
  transcript_confidence?: number;
  transcript_text_length?: number;
  aggregate_sentiment?: number;
  alignment_delta?: number;
  delta?: number;
  similarity?: number;
  relationship?: "converged" | "soft_split" | "diverged" | string;
  alignment_quality?: number;
  quality?: number;
  quality_flags?: string[];
  first_event_type?: string;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  event_hint?: string;
  confidence?: number;
  evidence_ids?: string[];
  events?: SignalEvent[];
};

export type SignalWindowLabel = {
  session_id: string;
  window_start: string;
  window_end: string;
  predicted_event?: string;
  predicted_relationship?: string;
  correctness: "correct" | "wrong" | "uncertain" | string;
  event_label: "hype_spike" | "frustration_spike" | "audience_shift" | "content_audience_divergence" | "none" | string;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  divergence_type?: string;
  event_start?: string;
  event_peak?: string;
  notes?: string;
  created_at?: string;
  updated_at?: string;
};

export type EvaluationAgentEvidence = {
  id?: string;
  source?: "chat" | "transcript" | "reaction" | "alignment" | "signal" | string;
  timestamp?: string;
  text?: string;
  meta?: Record<string, unknown>;
};

export type EvaluationAgentReview = {
  review_id?: string;
  run_id?: string;
  session_id: string;
  window_start: string;
  window_end: string;
  source_window_type?: string;
  reviewer?: string;
  model?: string;
  prompt_version?: string;
  status?: string;
  predicted_event?: string;
  suggested_event_label: string;
  correctness?: string;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  divergence_type?: string;
  event_start?: string;
  event_peak?: string;
  confidence?: number;
  streamer_usefulness?: number;
  reason?: string;
  evidence?: EvaluationAgentEvidence[];
  notes?: string;
  created_at?: string;
  updated_at?: string;
};

export type DashboardState = {
  status?: string;
  session_id?: string;
  channel?: string;
  stream?: StreamInfo;
  message_count?: number;
  bucket_count?: number;
  messages?: ChatMessage[];
  buckets?: ChatBucket[];
  reaction_windows?: ReactionWindow[];
  transcript_buckets?: TranscriptBucket[];
  alignments?: AlignmentBucket[];
  signal_windows?: SignalWindow[];
  signal_events?: SignalEvent[];
  error?: string;
};

export type TranscriptState = {
  mode?: "all" | "live" | "buckets" | string;
  status?: string;
  session_id?: string;
  channel_id?: string;
  bucket_seconds?: number;
  chunk_seconds?: number;
  segment_count?: number;
  bucket_count?: number;
  error?: string;
  partial_count?: number;
  partials?: TranscriptSegment[];
  segments?: TranscriptSegment[];
  buckets?: TranscriptBucket[];
  latest_partial?: TranscriptSegment;
  latest_segment?: TranscriptSegment;
  latest_bucket?: TranscriptBucket;
};

export type TranscriptHealth = {
  status?: string;
  default_chunk_seconds?: number;
  asr?: {
    profile?: string;
    model?: string;
    device?: string;
    compute_type?: string;
    language?: string | null;
    beam_size?: number;
    best_of?: number;
    no_fallback?: boolean;
    vad_filter?: boolean;
    cpu_threads?: number;
    num_workers?: number;
    model_loaded?: boolean;
    thread_env?: Record<string, string>;
  };
  repair?: {
    enabled?: boolean;
    queue_size?: number;
    asr?: {
      profile?: string;
      model?: string;
      device?: string;
      compute_type?: string;
      language?: string | null;
      beam_size?: number;
      best_of?: number;
      no_fallback?: boolean;
      vad_filter?: boolean;
      cpu_threads?: number;
      num_workers?: number;
      model_loaded?: boolean;
      thread_env?: Record<string, string>;
    } | null;
  };
  transcript_summary?: {
    bucket_count?: number;
    audio_seconds?: number;
    expected_audio_seconds?: number;
    audio_coverage?: number;
    segment_count?: number;
    word_count?: number;
    empty_ratio?: number;
    repair_added_words?: number;
    status_counts?: Record<string, number>;
  };
  quality?: {
    min_segment_confidence?: number;
    max_segment_repeats?: number;
  };
  warmup?: {
    enabled?: boolean;
    status?: string;
    error?: string;
  };
};

export type DashboardEvent = {
  type?: string;
  status?: string;
  session_id?: string;
  channel?: string;
  stream?: StreamInfo;
  message?: ChatMessage;
  bucket?: ChatBucket;
  transcript_bucket?: TranscriptBucket;
  reaction_window?: ReactionWindow;
  alignments?: AlignmentBucket[];
  signal_windows?: SignalWindow[];
  signal_events?: SignalEvent[];
  error?: string;
};

export type SessionHistory = {
  session_id: string;
  channel_id?: string;
  twitch_stream_id?: string;
  twitch_user_id?: string;
  status?: string;
  started_at?: string;
  ended_at?: string;
  stream_title?: string;
  stream_game?: string;
  stream_viewer_count?: number;
  chat_bucket_count?: number;
  transcript_bucket_count?: number;
  alignment_count?: number;
  label_count?: number;
};

export type SessionHistoryResponse = {
  sessions?: SessionHistory[];
};

export type SessionSummary = {
  session?: SessionHistory;
  latest_chat_buckets?: ChatBucket[];
  latest_transcript_buckets?: TranscriptBucket[];
  latest_alignments?: AlignmentBucket[];
  latest_signal_windows?: SignalWindow[];
  signal_windows?: SignalWindow[];
  signal_events?: SignalEvent[];
  window_labels?: SignalWindowLabel[];
  label_count?: number;
};

export type SessionInsightSeverity = "info" | "low" | "medium" | "high" | "critical" | string;

export type SessionInsightEvidence =
  | string
  | {
      source?: "chat" | "transcript" | "alignment" | "signal" | string;
      type?: string;
      timestamp?: string;
      label?: string;
      text?: string;
      summary?: string;
      value?: number;
      score?: number;
      confidence?: number;
      meta?: Record<string, unknown>;
    };

export type SessionInsight = {
  id?: string;
  type?: string;
  kind?: string;
  title?: string;
  description?: string;
  severity?: number | SessionInsightSeverity;
  confidence?: number;
  explanation?: string;
  evidence?: SessionInsightEvidence | SessionInsightEvidence[];
  uncertainty?: string;
  window_start?: string;
  window_end?: string;
};

export type SessionInsightSummary = {
  insight_count?: number;
  top_moments?: SessionInsight[];
  biggest_divergence?: SessionInsight;
  highest_hype?: SessionInsight;
  highest_frustration?: SessionInsight;
  low_confidence_flags?: SessionInsight[];
};

export type SessionReplay = {
  session?: SessionHistory;
  chat_buckets?: ChatBucket[];
  transcript_buckets?: TranscriptBucket[];
  alignments?: AlignmentBucket[];
  signal_windows?: SignalWindow[];
  signal_events?: SignalEvent[];
  insights?: SessionInsight[];
  insight_summary?: SessionInsightSummary;
  window_labels?: SignalWindowLabel[];
  agent_reviews?: EvaluationAgentReview[];
  label_count?: number;
};

export type ReplayProofSessionTotals = {
  bucket_count?: number;
  source_bucket_count?: number;
  chat_bucket_count?: number;
  transcript_bucket_count?: number;
  alignment_count?: number;
  signal_window_label_count?: number;
};

export type ReplayProofTruncation = {
  source?: string;
  loaded_count?: number;
  total_count?: number;
};

export type ReplayProofLabelCoverage = {
  labeled_windows?: number;
  unmatched_labels?: number;
  total_windows?: number;
  coverage?: number | null;
  stored_label_count?: number;
};

export type ReplayProofTranscriptCoverage = {
  bucket_count?: number;
  audio_seconds?: number;
  expected_audio_seconds?: number;
  audio_coverage?: number | null;
  segment_count?: number;
  word_count?: number;
  empty_ratio?: number | null;
  repair_added_words?: number;
  average_repair_changed_ratio?: number | null;
  repair_improvement?: number | null;
  status_counts?: Record<string, number>;
};

export type ReplayProofTimeline = {
  start?: string | null;
  end?: string | null;
  source_duration_ms?: number;
};

export type ReplayProofSpeed = {
  speed?: number;
  estimated_replay_duration_ms?: number;
  estimated_replay_seconds?: number;
  windows_per_second?: number;
  buckets_per_second?: number;
};

export type ReplayProofLatencySummary = {
  available_count?: number;
  missing_count?: number;
  min?: number | null;
  max?: number | null;
  average?: number | null;
  p50?: number | null;
  p95?: number | null;
};

export type ReplayProofLatency = {
  chat_analysis_latency_ms?: ReplayProofLatencySummary;
  transcript_sentiment_latency_ms?: ReplayProofLatencySummary;
  transcript_asr_latency_ms?: ReplayProofLatencySummary;
  transcript_pipeline_latency_ms?: ReplayProofLatencySummary;
  chat_analysis_status_counts?: Record<string, number>;
  transcript_sentiment_status_counts?: Record<string, number>;
};

export type ReplayProofUnsupported = {
  name?: string;
  reason?: string;
};

export type ReplayProof = {
  type?: string;
  session_id?: string;
  channel_id?: string;
  generated_at?: string;
  replay_limit?: number;
  partial?: boolean;
  session_totals?: ReplayProofSessionTotals;
  truncated_sources?: ReplayProofTruncation[];
  bucket_count?: number;
  source_bucket_count?: number;
  chat_bucket_count?: number;
  transcript_bucket_count?: number;
  alignment_count?: number;
  signal_window_count?: number;
  matched_windows?: number;
  label_coverage?: ReplayProofLabelCoverage;
  transcript_coverage?: ReplayProofTranscriptCoverage;
  timeline?: ReplayProofTimeline;
  speeds?: ReplayProofSpeed[];
  latency?: ReplayProofLatency;
  dropped_event_rate?: number | null;
  unsupported_metrics?: ReplayProofUnsupported[];
};

export type SessionProofResponse = {
  proof?: ReplayProof;
  persisted?: boolean;
  error?: string;
};

export type EventConfusionCount = {
  actual?: string;
  predicted?: string;
  count?: number;
};

export type EventBinaryCounts = {
  true_positive?: number;
  false_positive?: number;
  false_negative?: number;
  true_negative?: number;
};

export type EventLabelMetric = {
  label?: string;
  support?: number;
  predicted_count?: number;
  true_positive?: number;
  false_positive?: number;
  false_negative?: number;
  precision?: number | null;
  recall?: number | null;
  f1?: number | null;
};

export type EvaluationUnsupportedMetric = {
  name?: string;
  reason?: string;
};

export type SessionEvaluation = {
  type?: string;
  session_id?: string;
  generated_at?: string;
  total_windows?: number;
  total_labeled_windows?: number;
  evaluated_windows?: number;
  coverage?: number | null;
  unmatched_labels?: number;
  uncertain_labels?: number;
  invalid_labels?: number;
  event_confusion_counts?: EventConfusionCount[];
  event_counts?: EventBinaryCounts;
  event_accuracy?: number | null;
  event_precision?: number | null;
  event_recall?: number | null;
  event_f1?: number | null;
  event_label_metrics?: EventLabelMetric[];
  peak_recall?: number | null;
  hype_peak_recall?: number | null;
  onset_latency_ms?: number | null;
  event_onset_latency_ms?: number | null;
  reaction_type_accuracy?: number | null;
  reaction_type_f1?: number | null;
  reaction_type_metrics?: EventLabelMetric[];
  target_accuracy?: number | null;
  target_extraction_accuracy?: number | null;
  divergence_accuracy?: number | null;
  false_positives_normal_chat?: number;
  relationship_accuracy?: number | null;
  unsupported_metrics?: EvaluationUnsupportedMetric[];
  correctness_counts?: Record<string, number>;
};

export type SessionEvaluationResponse = {
  session?: SessionHistory;
  evaluation?: SessionEvaluation;
  replay_limit?: number;
  partial?: boolean;
  truncated_sources?: ReplayProofTruncation[];
};

export type DashboardSessionStartResponse = {
  session_id?: string;
  channel: string;
  status: string;
  reused?: boolean;
  run_id?: string;
  transcript_warning?: string;
};
