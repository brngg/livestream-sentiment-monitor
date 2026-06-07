package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *PostgresStore) CreateSession(ctx context.Context, session SessionRecord) error {
	startedAt := session.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	firstSeenAt := session.FirstSeenAt
	if firstSeenAt.IsZero() {
		firstSeenAt = startedAt
	}
	lastSeenAt := session.LastSeenAt
	if lastSeenAt.IsZero() {
		lastSeenAt = startedAt
	}
	if session.TwitchStreamID != "" {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO stream_sessions (
				session_id, channel_id, twitch_stream_id, twitch_user_id, status, started_at,
				first_seen_at, last_seen_at, bucket_seconds, transcript_bucket_seconds,
				transcript_chunk_seconds, nlp_analyzer_url, sentiment_model
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			ON CONFLICT (channel_id, twitch_stream_id) WHERE twitch_stream_id <> '' DO UPDATE SET
				twitch_user_id = COALESCE(NULLIF(EXCLUDED.twitch_user_id, ''), stream_sessions.twitch_user_id),
				status = EXCLUDED.status,
				first_seen_at = COALESCE(stream_sessions.first_seen_at, EXCLUDED.first_seen_at),
				last_seen_at = GREATEST(COALESCE(stream_sessions.last_seen_at, EXCLUDED.last_seen_at), EXCLUDED.last_seen_at),
				bucket_seconds = EXCLUDED.bucket_seconds,
				transcript_bucket_seconds = EXCLUDED.transcript_bucket_seconds,
				transcript_chunk_seconds = EXCLUDED.transcript_chunk_seconds,
				nlp_analyzer_url = EXCLUDED.nlp_analyzer_url,
				sentiment_model = EXCLUDED.sentiment_model,
				updated_at = now()
		`, session.SessionID, session.ChannelID, session.TwitchStreamID, session.TwitchUserID, defaultString(session.Status, "unknown"), startedAt,
			firstSeenAt, lastSeenAt, session.BucketSeconds, session.TranscriptBucketSeconds, session.TranscriptChunkSeconds,
			session.NLPAnalyzerURL, session.SentimentModel)
		return err
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO stream_sessions (
			session_id, channel_id, twitch_stream_id, twitch_user_id, status, started_at,
			first_seen_at, last_seen_at, bucket_seconds,
			transcript_bucket_seconds, transcript_chunk_seconds, nlp_analyzer_url, sentiment_model
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (session_id) DO UPDATE SET
			channel_id = EXCLUDED.channel_id,
			twitch_stream_id = COALESCE(NULLIF(EXCLUDED.twitch_stream_id, ''), stream_sessions.twitch_stream_id),
			twitch_user_id = COALESCE(NULLIF(EXCLUDED.twitch_user_id, ''), stream_sessions.twitch_user_id),
			status = EXCLUDED.status,
			started_at = EXCLUDED.started_at,
			first_seen_at = COALESCE(stream_sessions.first_seen_at, EXCLUDED.first_seen_at),
			last_seen_at = GREATEST(COALESCE(stream_sessions.last_seen_at, EXCLUDED.last_seen_at), EXCLUDED.last_seen_at),
			bucket_seconds = EXCLUDED.bucket_seconds,
			transcript_bucket_seconds = EXCLUDED.transcript_bucket_seconds,
			transcript_chunk_seconds = EXCLUDED.transcript_chunk_seconds,
			nlp_analyzer_url = EXCLUDED.nlp_analyzer_url,
			sentiment_model = EXCLUDED.sentiment_model,
			updated_at = now()
	`, session.SessionID, session.ChannelID, session.TwitchStreamID, session.TwitchUserID, defaultString(session.Status, "unknown"), startedAt,
		firstSeenAt, lastSeenAt, session.BucketSeconds, session.TranscriptBucketSeconds, session.TranscriptChunkSeconds,
		session.NLPAnalyzerURL, session.SentimentModel)
	return err
}

func (s *PostgresStore) FindSessionByTwitchStream(ctx context.Context, channelID, twitchStreamID string) (SessionRecord, error) {
	if channelID == "" || twitchStreamID == "" {
		return SessionRecord{}, ErrNotFound
	}
	var session SessionRecord
	var firstSeenAt, lastSeenAt sql.NullTime
	err := s.pool.QueryRow(ctx, `
		SELECT session_id, channel_id, twitch_stream_id, twitch_user_id, status, bucket_seconds,
			transcript_bucket_seconds, transcript_chunk_seconds, nlp_analyzer_url, sentiment_model,
			started_at, first_seen_at, last_seen_at
		FROM stream_sessions
		WHERE channel_id = $1 AND twitch_stream_id = $2
		LIMIT 1
	`, channelID, twitchStreamID).Scan(
		&session.SessionID,
		&session.ChannelID,
		&session.TwitchStreamID,
		&session.TwitchUserID,
		&session.Status,
		&session.BucketSeconds,
		&session.TranscriptBucketSeconds,
		&session.TranscriptChunkSeconds,
		&session.NLPAnalyzerURL,
		&session.SentimentModel,
		&session.StartedAt,
		&firstSeenAt,
		&lastSeenAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return SessionRecord{}, ErrNotFound
	}
	if err != nil {
		return SessionRecord{}, err
	}
	if firstSeenAt.Valid {
		session.FirstSeenAt = firstSeenAt.Time
	}
	if lastSeenAt.Valid {
		session.LastSeenAt = lastSeenAt.Time
	}
	return session, nil
}

func (s *PostgresStore) UpdateSessionStatus(ctx context.Context, update SessionStatusUpdate) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO stream_sessions (
			session_id, channel_id, twitch_stream_id, twitch_user_id, status, error, stream_title, stream_game,
			stream_viewer_count, stream_started_at, stream_language, ended_at
		) VALUES (
			$1, COALESCE(NULLIF($2, ''), 'unknown'), $3, $4, COALESCE(NULLIF($5, ''), 'unknown'),
			$6, $7, $8, $9::integer, $10::timestamptz, $11, $12::timestamptz
		)
		ON CONFLICT (session_id) DO UPDATE SET
			status = COALESCE(NULLIF($5, ''), stream_sessions.status),
			channel_id = COALESCE(NULLIF($2, ''), stream_sessions.channel_id),
			twitch_stream_id = COALESCE(NULLIF($3, ''), stream_sessions.twitch_stream_id),
			twitch_user_id = COALESCE(NULLIF($4, ''), stream_sessions.twitch_user_id),
			error = COALESCE(NULLIF($6, ''), stream_sessions.error),
			stream_title = COALESCE(NULLIF($7, ''), stream_sessions.stream_title),
			stream_game = COALESCE(NULLIF($8, ''), stream_sessions.stream_game),
			stream_viewer_count = COALESCE($9::integer, stream_sessions.stream_viewer_count),
			stream_started_at = COALESCE($10::timestamptz, stream_sessions.stream_started_at),
			stream_language = COALESCE(NULLIF($11, ''), stream_sessions.stream_language),
			ended_at = COALESCE($12::timestamptz, stream_sessions.ended_at),
			last_seen_at = now(),
			updated_at = now()
	`, update.SessionID, update.ChannelID, update.TwitchStreamID, update.TwitchUserID, update.Status, update.Error, update.StreamTitle, update.StreamGame,
		update.StreamViewerCount, update.StreamStartedAt, update.StreamLanguage, update.EndedAt)
	return err
}

func (s *PostgresStore) SaveIngestionRun(ctx context.Context, run IngestionRunRecord) error {
	startedAt := run.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ingestion_runs (run_id, session_id, started_at, ended_at, status, stop_reason, error)
		VALUES ($1,$2,$3,$4,COALESCE(NULLIF($5, ''), 'unknown'),$6,$7)
		ON CONFLICT (run_id) DO UPDATE SET
			session_id = EXCLUDED.session_id,
			started_at = EXCLUDED.started_at,
			ended_at = COALESCE(EXCLUDED.ended_at, ingestion_runs.ended_at),
			status = EXCLUDED.status,
			stop_reason = COALESCE(NULLIF(EXCLUDED.stop_reason, ''), ingestion_runs.stop_reason),
			error = COALESCE(NULLIF(EXCLUDED.error, ''), ingestion_runs.error),
			updated_at = now()
	`, run.RunID, run.SessionID, startedAt, run.EndedAt, run.Status, run.StopReason, run.Error)
	return err
}

func (s *PostgresStore) UpdateIngestionRun(ctx context.Context, update IngestionRunUpdate) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE ingestion_runs SET
			session_id = COALESCE(NULLIF($2, ''), session_id),
			ended_at = COALESCE($3::timestamptz, ended_at),
			status = COALESCE(NULLIF($4, ''), status),
			stop_reason = COALESCE(NULLIF($5, ''), stop_reason),
			error = COALESCE(NULLIF($6, ''), error),
			updated_at = now()
		WHERE run_id = $1
	`, update.RunID, update.SessionID, update.EndedAt, update.Status, update.StopReason, update.Error)
	return err
}

func (s *PostgresStore) SaveChatMessageSample(ctx context.Context, sample ChatMessageSample) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO chat_message_samples (
			session_id, channel_id, message_id, bucket_start, bucket_end, timestamp,
			user_hash, text, label, confidence, sentiment_score, human_label, evidence_rank
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (session_id, message_id) DO UPDATE SET
			channel_id = EXCLUDED.channel_id,
			bucket_start = EXCLUDED.bucket_start,
			bucket_end = EXCLUDED.bucket_end,
			timestamp = EXCLUDED.timestamp,
			user_hash = EXCLUDED.user_hash,
			text = EXCLUDED.text,
			label = EXCLUDED.label,
			confidence = EXCLUDED.confidence,
			sentiment_score = EXCLUDED.sentiment_score,
			human_label = EXCLUDED.human_label,
			evidence_rank = LEAST(chat_message_samples.evidence_rank, EXCLUDED.evidence_rank),
			updated_at = now()
	`, sample.SessionID, sample.ChannelID, sample.MessageID, sample.BucketStart, sample.BucketEnd, sample.Timestamp,
		sample.UserHash, sample.Text, sample.Label, sample.Confidence, sample.SentimentScore, sample.HumanLabel, sample.EvidenceRank)
	return err
}

func (s *PostgresStore) SaveChatBucket(ctx context.Context, bucket chat.ChatBucket) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO chat_buckets (
			session_id, channel_id, bucket_start, bucket_end, message_count, unique_chatters,
			chat_sentiment, sentiment_confidence, analyzed_count, positive_ratio, neutral_ratio,
				negative_ratio, sentiment_model, analysis_latency_ms, analysis_status,
				language_mix, top_terms, top_emotes, message_scores, peak_reaction_score,
				peak_reaction_type, peak_target_type, peak_target_text, peak_source, peak_event_hint,
				peak_confidence, peak_evidence_ids, peak_time, peak_window_start, peak_window_end,
				subwindows, peak_evidence_messages
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17::jsonb,$18::jsonb,$19::jsonb,$20,$21,$22,$23,$24,$25,$26,$27::jsonb,$28,$29,$30,$31::jsonb,$32::jsonb)
		ON CONFLICT (session_id, channel_id, bucket_start, bucket_end) DO UPDATE SET
			message_count = EXCLUDED.message_count,
			unique_chatters = EXCLUDED.unique_chatters,
			chat_sentiment = EXCLUDED.chat_sentiment,
			sentiment_confidence = EXCLUDED.sentiment_confidence,
			analyzed_count = EXCLUDED.analyzed_count,
			positive_ratio = EXCLUDED.positive_ratio,
			neutral_ratio = EXCLUDED.neutral_ratio,
			negative_ratio = EXCLUDED.negative_ratio,
			sentiment_model = EXCLUDED.sentiment_model,
			analysis_latency_ms = EXCLUDED.analysis_latency_ms,
			analysis_status = EXCLUDED.analysis_status,
			language_mix = EXCLUDED.language_mix,
			top_terms = EXCLUDED.top_terms,
			top_emotes = EXCLUDED.top_emotes,
				message_scores = EXCLUDED.message_scores,
				peak_reaction_score = EXCLUDED.peak_reaction_score,
				peak_reaction_type = EXCLUDED.peak_reaction_type,
				peak_target_type = EXCLUDED.peak_target_type,
				peak_target_text = EXCLUDED.peak_target_text,
				peak_source = EXCLUDED.peak_source,
				peak_event_hint = EXCLUDED.peak_event_hint,
				peak_confidence = EXCLUDED.peak_confidence,
				peak_evidence_ids = EXCLUDED.peak_evidence_ids,
				peak_time = EXCLUDED.peak_time,
			peak_window_start = EXCLUDED.peak_window_start,
			peak_window_end = EXCLUDED.peak_window_end,
			subwindows = EXCLUDED.subwindows,
			peak_evidence_messages = EXCLUDED.peak_evidence_messages,
			updated_at = now()
	`, bucket.SessionID, bucket.ChannelID, bucket.BucketStart, bucket.BucketEnd, bucket.MessageCount, bucket.UniqueChatters,
		bucket.ChatSentiment, bucket.SentimentConfidence, bucket.AnalyzedCount, bucket.PositiveRatio, bucket.NeutralRatio,
		bucket.NegativeRatio, bucket.SentimentModel, bucket.AnalysisLatencyMS, bucket.AnalysisStatus,
		jsonString(bucket.LanguageMix), jsonString(bucket.TopTerms), jsonString(bucket.TopEmotes), jsonString(bucket.MessageScores),
		bucket.PeakReactionScore, bucket.PeakReactionType, bucket.PeakTargetType, bucket.PeakTargetText, bucket.PeakSource,
		bucket.PeakEventHint, bucket.PeakConfidence, jsonString(bucket.PeakEvidenceIDs), bucket.PeakTime, bucket.PeakWindowStart, bucket.PeakWindowEnd,
		jsonString(bucket.Subwindows), jsonString(bucket.PeakEvidenceMessages))
	return err
}

func (s *PostgresStore) SaveTranscriptBucket(ctx context.Context, bucket TranscriptBucket) error {
	bucket = normalizeTranscriptCompleteness(bucket)
	quality := bucket.Quality
	if quality == nil {
		quality = map[string]any{}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO transcript_buckets (
			session_id, channel_id, bucket_start, bucket_end, text, language, transcript_confidence,
			transcript_status, sentiment_score, sentiment_confidence, sentiment_label, sentiment_model, sentiment_status,
			sentiment_latency_ms, audio_started_at, audio_ended_at, transcribed_at,
			asr_latency_ms, pipeline_latency_ms, audio_seconds, segment_count, word_count, empty_ratio,
			repair_added_words, repair_changed_ratio, segments, quality
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26::jsonb,$27::jsonb)
		ON CONFLICT (session_id, channel_id, bucket_start, bucket_end) DO UPDATE SET
			text = EXCLUDED.text,
			language = EXCLUDED.language,
			transcript_confidence = EXCLUDED.transcript_confidence,
			transcript_status = EXCLUDED.transcript_status,
			sentiment_score = EXCLUDED.sentiment_score,
			sentiment_confidence = EXCLUDED.sentiment_confidence,
			sentiment_label = EXCLUDED.sentiment_label,
			sentiment_model = EXCLUDED.sentiment_model,
			sentiment_status = EXCLUDED.sentiment_status,
			sentiment_latency_ms = EXCLUDED.sentiment_latency_ms,
			audio_started_at = EXCLUDED.audio_started_at,
			audio_ended_at = EXCLUDED.audio_ended_at,
			transcribed_at = EXCLUDED.transcribed_at,
			asr_latency_ms = EXCLUDED.asr_latency_ms,
			pipeline_latency_ms = EXCLUDED.pipeline_latency_ms,
			audio_seconds = EXCLUDED.audio_seconds,
			segment_count = EXCLUDED.segment_count,
			word_count = EXCLUDED.word_count,
			empty_ratio = EXCLUDED.empty_ratio,
			repair_added_words = EXCLUDED.repair_added_words,
			repair_changed_ratio = EXCLUDED.repair_changed_ratio,
			segments = EXCLUDED.segments,
			quality = EXCLUDED.quality,
			updated_at = now()
	`, bucket.SessionID, bucket.ChannelID, bucket.BucketStart, bucket.BucketEnd, bucket.Text, bucket.Language, bucket.TranscriptConfidence,
		bucket.TranscriptStatus, bucket.SentimentScore, bucket.SentimentConfidence, bucket.SentimentLabel, bucket.SentimentModel, bucket.SentimentStatus,
		bucket.SentimentLatencyMS, bucket.AudioStartedAt, bucket.AudioEndedAt, bucket.TranscribedAt,
		bucket.ASRLatencyMS, bucket.PipelineLatencyMS, bucket.AudioSeconds, bucket.SegmentCount, bucket.WordCount, bucket.EmptyRatio,
		bucket.RepairAddedWords, bucket.RepairChangedRatio, jsonString(bucket.Segments), jsonString(quality))
	return err
}

func normalizeTranscriptCompleteness(bucket TranscriptBucket) TranscriptBucket {
	if bucket.AudioSeconds == 0 {
		bucket.AudioSeconds = qualityFloat(bucket.Quality, "audio_seconds", "audio_coverage_seconds")
	}
	if bucket.AudioSeconds == 0 && bucket.AudioStartedAt != nil && bucket.AudioEndedAt != nil && bucket.AudioEndedAt.After(*bucket.AudioStartedAt) {
		bucket.AudioSeconds = bucket.AudioEndedAt.Sub(*bucket.AudioStartedAt).Seconds()
	}
	if bucket.SegmentCount == 0 {
		bucket.SegmentCount = qualityInt(bucket.Quality, "raw_segment_count", "retained_segment_count")
		if bucket.SegmentCount == 0 {
			bucket.SegmentCount = len(bucket.Segments)
		}
	}
	if bucket.WordCount == 0 {
		bucket.WordCount = qualityInt(bucket.Quality, "word_count")
		if bucket.WordCount == 0 && strings.TrimSpace(bucket.Text) != "" {
			bucket.WordCount = len(strings.Fields(bucket.Text))
		}
	}
	if bucket.EmptyRatio == 0 {
		bucket.EmptyRatio = qualityFloat(bucket.Quality, "empty_ratio")
	}
	if bucket.EmptyRatio == 0 && bucket.AudioSeconds > 0 && bucket.WordCount == 0 {
		bucket.EmptyRatio = 1
	}
	if bucket.RepairAddedWords == 0 {
		bucket.RepairAddedWords = qualityInt(bucket.Quality, "repair_added_words")
	}
	if bucket.RepairChangedRatio == 0 {
		bucket.RepairChangedRatio = qualityFloat(bucket.Quality, "repair_changed_ratio")
	}
	if bucket.TranscriptStatus == "" {
		bucket.TranscriptStatus = transcriptStatusFromQuality(bucket.Quality)
	}
	return bucket
}

func qualityFloat(quality map[string]any, keys ...string) float64 {
	for _, key := range keys {
		switch value := quality[key].(type) {
		case float64:
			return value
		case float32:
			return float64(value)
		case int:
			return float64(value)
		case int64:
			return float64(value)
		case json.Number:
			parsed, _ := value.Float64()
			return parsed
		}
	}
	return 0
}

func qualityInt(quality map[string]any, keys ...string) int {
	for _, key := range keys {
		switch value := quality[key].(type) {
		case float64:
			return int(value)
		case float32:
			return int(value)
		case int:
			return value
		case int64:
			return int(value)
		case json.Number:
			parsed, _ := value.Int64()
			return int(parsed)
		}
	}
	return 0
}

func qualityBool(quality map[string]any, key string) bool {
	switch value := quality[key].(type) {
	case bool:
		return value
	case string:
		normalized := strings.ToLower(strings.TrimSpace(value))
		return normalized == "true" || normalized == "1" || normalized == "yes"
	default:
		return false
	}
}

func qualityString(quality map[string]any, key string) string {
	if value, ok := quality[key].(string); ok {
		return value
	}
	return ""
}

func qualityHasKey(quality map[string]any, key string) bool {
	_, ok := quality[key]
	return ok
}

func transcriptStatusFromQuality(quality map[string]any) string {
	if quality == nil {
		return ""
	}
	if status, ok := quality["status"].(string); ok {
		return status
	}
	if status, ok := quality["repair_status"].(string); ok {
		switch strings.ToLower(strings.TrimSpace(status)) {
		case "pending", "queued":
			return "repairing"
		case "completed":
			return "final"
		case "failed", "queue_full", "audio_read_failed", "audio_write_failed", "empty":
			return "degraded"
		}
	}
	return ""
}

func (s *PostgresStore) SaveAlignment(ctx context.Context, item AlignmentBucket) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO alignment_buckets (
			session_id, channel_id, window_start, window_end, chat_bucket_start, chat_bucket_end,
			transcript_bucket_start, transcript_bucket_end, chat_sentiment, chat_confidence,
			chat_message_count, transcript_sentiment, transcript_confidence, transcript_text_length,
			delta, similarity, relationship, overlap_seconds, quality, quality_flags
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20::jsonb)
		ON CONFLICT (session_id, channel_id, window_start, window_end, chat_bucket_start, transcript_bucket_start) DO UPDATE SET
			chat_bucket_end = EXCLUDED.chat_bucket_end,
			transcript_bucket_end = EXCLUDED.transcript_bucket_end,
			chat_sentiment = EXCLUDED.chat_sentiment,
			chat_confidence = EXCLUDED.chat_confidence,
			chat_message_count = EXCLUDED.chat_message_count,
			transcript_sentiment = EXCLUDED.transcript_sentiment,
			transcript_confidence = EXCLUDED.transcript_confidence,
			transcript_text_length = EXCLUDED.transcript_text_length,
			delta = EXCLUDED.delta,
			similarity = EXCLUDED.similarity,
			relationship = EXCLUDED.relationship,
			overlap_seconds = EXCLUDED.overlap_seconds,
			quality = EXCLUDED.quality,
			quality_flags = EXCLUDED.quality_flags,
			updated_at = now()
	`, item.SessionID, item.ChannelID, item.WindowStart, item.WindowEnd, item.ChatBucketStart, item.ChatBucketEnd,
		item.TranscriptBucketStart, item.TranscriptBucketEnd, item.ChatSentiment, item.ChatConfidence, item.ChatMessageCount,
		item.TranscriptSentiment, item.TranscriptConfidence, item.TranscriptTextLength, item.Delta, item.Similarity,
		item.Relationship, item.OverlapSeconds, item.Quality, jsonString(item.QualityFlags))
	return err
}

func (s *PostgresStore) SaveHumanLabel(ctx context.Context, label HumanLabel) error {
	createdAt := label.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO human_labels (session_id, message_id, label, created_at)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (session_id, message_id) DO UPDATE SET
			label = EXCLUDED.label,
			updated_at = now()
	`, label.SessionID, label.MessageID, label.Label, createdAt)
	return err
}

func (s *PostgresStore) SaveSignalWindowLabel(ctx context.Context, label SignalWindowLabel) error {
	createdAt := label.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
			INSERT INTO signal_window_labels (
				session_id, window_start, window_end, predicted_event, predicted_relationship,
				reaction_type, target_type, target_text, divergence_type, event_start, event_peak,
				correctness, event_label, notes, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
			ON CONFLICT (session_id, window_start, window_end) DO UPDATE SET
				predicted_event = EXCLUDED.predicted_event,
				predicted_relationship = EXCLUDED.predicted_relationship,
				reaction_type = EXCLUDED.reaction_type,
				target_type = EXCLUDED.target_type,
				target_text = EXCLUDED.target_text,
				divergence_type = EXCLUDED.divergence_type,
				event_start = EXCLUDED.event_start,
				event_peak = EXCLUDED.event_peak,
				correctness = EXCLUDED.correctness,
				event_label = EXCLUDED.event_label,
				notes = EXCLUDED.notes,
				updated_at = now()
		`, label.SessionID, label.WindowStart, label.WindowEnd, label.PredictedEvent, label.PredictedRelationship,
		label.ReactionType, label.TargetType, label.TargetText, label.DivergenceType, nullableTime(label.EventStart),
		nullableTime(label.EventPeak), label.Correctness, label.EventLabel, label.Notes, createdAt)
	return err
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func (s *PostgresStore) ListSignalWindowLabels(ctx context.Context, sessionID string) ([]SignalWindowLabel, error) {
	rows, err := s.pool.Query(ctx, `
			SELECT session_id, window_start, window_end, predicted_event, predicted_relationship,
				reaction_type, target_type, target_text, divergence_type, event_start, event_peak,
				correctness, event_label, notes, created_at, updated_at
			FROM signal_window_labels
			WHERE session_id = $1
		ORDER BY window_start ASC
	`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SignalWindowLabel
	for rows.Next() {
		var item SignalWindowLabel
		var eventStart, eventPeak sql.NullTime
		if err := rows.Scan(&item.SessionID, &item.WindowStart, &item.WindowEnd, &item.PredictedEvent,
			&item.PredictedRelationship, &item.ReactionType, &item.TargetType, &item.TargetText, &item.DivergenceType,
			&eventStart, &eventPeak, &item.Correctness, &item.EventLabel, &item.Notes, &item.CreatedAt,
			&item.UpdatedAt); err != nil {
			return nil, err
		}
		if eventStart.Valid {
			item.EventStart = eventStart.Time
		}
		if eventPeak.Valid {
			item.EventPeak = eventPeak.Time
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) SaveEvaluationAgentReview(ctx context.Context, review EvaluationAgentReview) error {
	reviewID := strings.TrimSpace(review.ReviewID)
	if reviewID == "" {
		reviewID = defaultEvaluationAgentReviewID(review)
	}
	createdAt := review.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO evaluation_agent_reviews (
			review_id, run_id, session_id, window_start, window_end, source_window_type,
			reviewer, model, prompt_version, status, predicted_event, suggested_event_label,
			correctness, reaction_type, target_type, target_text, divergence_type, event_start,
			event_peak, confidence, streamer_usefulness, reason, evidence, notes, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23::jsonb,$24,$25)
		ON CONFLICT (review_id) DO UPDATE SET
			run_id = EXCLUDED.run_id,
			session_id = EXCLUDED.session_id,
			window_start = EXCLUDED.window_start,
			window_end = EXCLUDED.window_end,
			source_window_type = EXCLUDED.source_window_type,
			reviewer = EXCLUDED.reviewer,
			model = EXCLUDED.model,
			prompt_version = EXCLUDED.prompt_version,
			status = EXCLUDED.status,
			predicted_event = EXCLUDED.predicted_event,
			suggested_event_label = EXCLUDED.suggested_event_label,
			correctness = EXCLUDED.correctness,
			reaction_type = EXCLUDED.reaction_type,
			target_type = EXCLUDED.target_type,
			target_text = EXCLUDED.target_text,
			divergence_type = EXCLUDED.divergence_type,
			event_start = EXCLUDED.event_start,
			event_peak = EXCLUDED.event_peak,
			confidence = EXCLUDED.confidence,
			streamer_usefulness = EXCLUDED.streamer_usefulness,
			reason = EXCLUDED.reason,
			evidence = EXCLUDED.evidence,
			notes = EXCLUDED.notes,
			updated_at = now()
	`, reviewID, review.RunID, review.SessionID, review.WindowStart, review.WindowEnd, review.SourceWindowType,
		review.Reviewer, review.Model, review.PromptVersion, defaultString(review.Status, "suggested"), review.PredictedEvent,
		review.SuggestedEventLabel, review.Correctness, review.ReactionType, review.TargetType, review.TargetText,
		review.DivergenceType, nullableTimePtr(review.EventStart), nullableTimePtr(review.EventPeak), review.Confidence,
		review.StreamerUsefulness, review.Reason, jsonString(review.Evidence), review.Notes, createdAt)
	return err
}

func (s *PostgresStore) ListEvaluationAgentReviews(ctx context.Context, sessionID string) ([]EvaluationAgentReview, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT review_id, run_id, session_id, window_start, window_end, source_window_type,
			reviewer, model, prompt_version, status, predicted_event, suggested_event_label,
			correctness, reaction_type, target_type, target_text, divergence_type, event_start,
			event_peak, confidence, streamer_usefulness, reason, evidence, notes, created_at, updated_at
		FROM evaluation_agent_reviews
		WHERE session_id = $1
		ORDER BY window_start ASC, source_window_type ASC, confidence DESC, review_id ASC
	`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EvaluationAgentReview
	for rows.Next() {
		var item EvaluationAgentReview
		var eventStart, eventPeak sql.NullTime
		var evidence []byte
		if err := rows.Scan(&item.ReviewID, &item.RunID, &item.SessionID, &item.WindowStart, &item.WindowEnd,
			&item.SourceWindowType, &item.Reviewer, &item.Model, &item.PromptVersion, &item.Status, &item.PredictedEvent,
			&item.SuggestedEventLabel, &item.Correctness, &item.ReactionType, &item.TargetType, &item.TargetText,
			&item.DivergenceType, &eventStart, &eventPeak, &item.Confidence, &item.StreamerUsefulness, &item.Reason,
			&evidence, &item.Notes, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		if eventStart.Valid {
			value := eventStart.Time
			item.EventStart = &value
		}
		if eventPeak.Valid {
			value := eventPeak.Time
			item.EventPeak = &value
		}
		_ = json.Unmarshal(evidence, &item.Evidence)
		out = append(out, item)
	}
	return out, rows.Err()
}

func defaultEvaluationAgentReviewID(review EvaluationAgentReview) string {
	parts := []string{
		defaultString(review.RunID, "agent-review"),
		review.SessionID,
		review.WindowStart.UTC().Format(time.RFC3339Nano),
		review.WindowEnd.UTC().Format(time.RFC3339Nano),
		defaultString(review.SourceWindowType, "window"),
		defaultString(review.Reviewer, "agent"),
	}
	return strings.Join(parts, ":")
}

func nullableTimePtr(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return *value
}

func (s *PostgresStore) SaveMetric(ctx context.Context, metric SystemMetric) error {
	recordedAt := metric.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO system_metrics (session_id, name, value, unit, recorded_at, meta)
		VALUES ($1,$2,$3,$4,$5,$6::jsonb)
	`, metric.SessionID, metric.Name, metric.Value, metric.Unit, recordedAt, jsonString(metric.Meta))
	return err
}

func (s *PostgresStore) ListSessions(ctx context.Context, limit int) ([]SessionHistory, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	rows, err := s.pool.Query(ctx, sessionHistoryQuery+` ORDER BY COALESCE(s.last_seen_at, s.updated_at, s.started_at) DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSessionHistories(rows)
}

func (s *PostgresStore) GetSessionSummary(ctx context.Context, sessionID string) (SessionSummary, error) {
	history, err := s.sessionHistory(ctx, sessionID)
	if err != nil {
		return SessionSummary{}, err
	}

	chatBuckets, err := s.chatBuckets(ctx, sessionID, 5, false)
	if err != nil {
		return SessionSummary{}, err
	}
	transcriptBuckets, err := s.transcriptBuckets(ctx, sessionID, 5, false)
	if err != nil {
		return SessionSummary{}, err
	}
	alignments, err := s.alignments(ctx, sessionID, 5, false)
	if err != nil {
		return SessionSummary{}, err
	}
	windowLabels, err := s.ListSignalWindowLabels(ctx, sessionID)
	if err != nil {
		return SessionSummary{}, err
	}
	return SessionSummary{
		Session:                 history,
		LatestChatBuckets:       chatBuckets,
		LatestTranscriptBuckets: transcriptBuckets,
		LatestAlignments:        alignments,
		WindowLabels:            windowLabels,
		LabelCount:              history.LabelCount,
	}, nil
}

func (s *PostgresStore) GetSessionReplay(ctx context.Context, sessionID string, limit int) (SessionReplay, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	history, err := s.sessionHistory(ctx, sessionID)
	if err != nil {
		return SessionReplay{}, err
	}
	chatBuckets, err := s.chatBuckets(ctx, sessionID, limit, true)
	if err != nil {
		return SessionReplay{}, err
	}
	transcriptBuckets, err := s.transcriptBuckets(ctx, sessionID, limit, true)
	if err != nil {
		return SessionReplay{}, err
	}
	alignments, err := s.alignments(ctx, sessionID, limit, true)
	if err != nil {
		return SessionReplay{}, err
	}
	windowLabels, err := s.ListSignalWindowLabels(ctx, sessionID)
	if err != nil {
		return SessionReplay{}, err
	}
	agentReviews, err := s.ListEvaluationAgentReviews(ctx, sessionID)
	if err != nil {
		return SessionReplay{}, err
	}
	systemMetrics, err := s.systemMetrics(ctx, sessionID, 200)
	if err != nil {
		return SessionReplay{}, err
	}
	return SessionReplay{
		Session:           history,
		ChatBuckets:       chatBuckets,
		TranscriptBuckets: transcriptBuckets,
		Alignments:        alignments,
		WindowLabels:      windowLabels,
		AgentReviews:      agentReviews,
		SystemMetrics:     systemMetrics,
		LabelCount:        history.LabelCount,
	}, nil
}

func (s *PostgresStore) systemMetrics(ctx context.Context, sessionID string, limit int) ([]SystemMetric, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT session_id, name, value, unit, recorded_at, meta
		FROM system_metrics
		WHERE session_id = $1
		ORDER BY recorded_at DESC, name ASC
		LIMIT $2
	`, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SystemMetric
	for rows.Next() {
		var item SystemMetric
		var meta []byte
		if err := rows.Scan(&item.SessionID, &item.Name, &item.Value, &item.Unit, &item.RecordedAt, &meta); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(meta, &item.Meta)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) sessionHistory(ctx context.Context, sessionID string) (SessionHistory, error) {
	rows, err := s.pool.Query(ctx, sessionHistoryQuery+` WHERE s.session_id = $1`, sessionID)
	if err != nil {
		return SessionHistory{}, err
	}
	histories, err := scanSessionHistories(rows)
	if err != nil {
		return SessionHistory{}, err
	}
	if len(histories) == 0 {
		return SessionHistory{}, ErrNotFound
	}
	return histories[0], nil
}

const sessionHistoryQuery = `
	SELECT
		s.session_id, s.channel_id, s.twitch_stream_id, s.twitch_user_id, s.status, s.started_at, s.ended_at,
		s.stream_title, s.stream_game, s.stream_viewer_count,
		(SELECT count(*) FROM chat_buckets cb WHERE cb.session_id = s.session_id),
		(SELECT count(*) FROM transcript_buckets tb WHERE tb.session_id = s.session_id),
		(SELECT count(*) FROM alignment_buckets ab WHERE ab.session_id = s.session_id),
		((SELECT count(*) FROM human_labels hl WHERE hl.session_id = s.session_id) +
			(SELECT count(*) FROM signal_window_labels swl WHERE swl.session_id = s.session_id))
	FROM stream_sessions s`

func scanSessionHistories(rows pgx.Rows) ([]SessionHistory, error) {
	defer rows.Close()
	var out []SessionHistory
	for rows.Next() {
		var item SessionHistory
		var endedAt sql.NullTime
		var viewerCount sql.NullInt64
		if err := rows.Scan(
			&item.SessionID,
			&item.ChannelID,
			&item.TwitchStreamID,
			&item.TwitchUserID,
			&item.Status,
			&item.StartedAt,
			&endedAt,
			&item.StreamTitle,
			&item.StreamGame,
			&viewerCount,
			&item.ChatBucketCount,
			&item.TranscriptBucketCount,
			&item.AlignmentCount,
			&item.LabelCount,
		); err != nil {
			return nil, err
		}
		if endedAt.Valid {
			item.EndedAt = &endedAt.Time
		}
		if viewerCount.Valid {
			value := int(viewerCount.Int64)
			item.StreamViewerCount = &value
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) chatBuckets(ctx context.Context, sessionID string, limit int, ascending bool) ([]chat.ChatBucket, error) {
	order := "DESC"
	if ascending {
		order = "ASC"
	}
	rows, err := s.pool.Query(ctx, `
		SELECT session_id, channel_id, bucket_start, bucket_end, message_count, unique_chatters,
			chat_sentiment, sentiment_confidence, analyzed_count, positive_ratio, neutral_ratio,
				negative_ratio, sentiment_model, analysis_latency_ms, analysis_status,
				language_mix, top_terms, top_emotes, message_scores, peak_reaction_score,
				peak_reaction_type, peak_target_type, peak_target_text, peak_source, peak_event_hint,
				peak_confidence, peak_evidence_ids, peak_time, peak_window_start, peak_window_end,
				subwindows, peak_evidence_messages
		FROM chat_buckets WHERE session_id = $1 ORDER BY bucket_start `+order+` LIMIT $2
	`, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []chat.ChatBucket
	for rows.Next() {
		var item chat.ChatBucket
		var languageMix, topTerms, topEmotes, messageScores, peakEvidenceIDs, subwindows, peakEvidenceMessages []byte
		if err := rows.Scan(&item.SessionID, &item.ChannelID, &item.BucketStart, &item.BucketEnd, &item.MessageCount,
			&item.UniqueChatters, &item.ChatSentiment, &item.SentimentConfidence, &item.AnalyzedCount,
			&item.PositiveRatio, &item.NeutralRatio, &item.NegativeRatio, &item.SentimentModel,
			&item.AnalysisLatencyMS, &item.AnalysisStatus, &languageMix, &topTerms, &topEmotes, &messageScores,
			&item.PeakReactionScore, &item.PeakReactionType, &item.PeakTargetType, &item.PeakTargetText, &item.PeakSource,
			&item.PeakEventHint, &item.PeakConfidence, &peakEvidenceIDs, &item.PeakTime, &item.PeakWindowStart, &item.PeakWindowEnd,
			&subwindows, &peakEvidenceMessages); err != nil {
			return nil, err
		}
		item.Type = "chat_bucket"
		_ = json.Unmarshal(languageMix, &item.LanguageMix)
		_ = json.Unmarshal(topTerms, &item.TopTerms)
		_ = json.Unmarshal(topEmotes, &item.TopEmotes)
		_ = json.Unmarshal(messageScores, &item.MessageScores)
		_ = json.Unmarshal(peakEvidenceIDs, &item.PeakEvidenceIDs)
		_ = json.Unmarshal(subwindows, &item.Subwindows)
		_ = json.Unmarshal(peakEvidenceMessages, &item.PeakEvidenceMessages)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) transcriptBuckets(ctx context.Context, sessionID string, limit int, ascending bool) ([]TranscriptBucket, error) {
	order := "DESC"
	if ascending {
		order = "ASC"
	}
	rows, err := s.pool.Query(ctx, `
		SELECT session_id, channel_id, bucket_start, bucket_end, text, language, transcript_confidence,
			transcript_status, sentiment_score, sentiment_confidence, sentiment_label, sentiment_model, sentiment_status,
			sentiment_latency_ms, audio_started_at, audio_ended_at, transcribed_at,
			asr_latency_ms, pipeline_latency_ms, audio_seconds, segment_count, word_count, empty_ratio,
			repair_added_words, repair_changed_ratio, segments, quality
		FROM transcript_buckets WHERE session_id = $1 ORDER BY bucket_start `+order+` LIMIT $2
	`, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TranscriptBucket
	for rows.Next() {
		var item TranscriptBucket
		var segments []byte
		var quality []byte
		if err := rows.Scan(&item.SessionID, &item.ChannelID, &item.BucketStart, &item.BucketEnd, &item.Text,
			&item.Language, &item.TranscriptConfidence, &item.TranscriptStatus, &item.SentimentScore, &item.SentimentConfidence,
			&item.SentimentLabel, &item.SentimentModel, &item.SentimentStatus, &item.SentimentLatencyMS,
			&item.AudioStartedAt, &item.AudioEndedAt, &item.TranscribedAt, &item.ASRLatencyMS, &item.PipelineLatencyMS,
			&item.AudioSeconds, &item.SegmentCount, &item.WordCount, &item.EmptyRatio, &item.RepairAddedWords,
			&item.RepairChangedRatio, &segments, &quality); err != nil {
			return nil, err
		}
		item.Type = "transcript_bucket"
		_ = json.Unmarshal(segments, &item.Segments)
		_ = json.Unmarshal(quality, &item.Quality)
		item = normalizeTranscriptCompleteness(item)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) alignments(ctx context.Context, sessionID string, limit int, ascending bool) ([]AlignmentBucket, error) {
	order := "DESC"
	if ascending {
		order = "ASC"
	}
	rows, err := s.pool.Query(ctx, `
		SELECT session_id, channel_id, window_start, window_end, chat_bucket_start, chat_bucket_end,
			transcript_bucket_start, transcript_bucket_end, chat_sentiment, chat_confidence,
			chat_message_count, transcript_sentiment, transcript_confidence, transcript_text_length,
			delta, similarity, relationship, overlap_seconds, quality, quality_flags
		FROM alignment_buckets WHERE session_id = $1 ORDER BY window_start `+order+` LIMIT $2
	`, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AlignmentBucket
	for rows.Next() {
		var item AlignmentBucket
		var flags []byte
		if err := rows.Scan(&item.SessionID, &item.ChannelID, &item.WindowStart, &item.WindowEnd, &item.ChatBucketStart,
			&item.ChatBucketEnd, &item.TranscriptBucketStart, &item.TranscriptBucketEnd, &item.ChatSentiment,
			&item.ChatConfidence, &item.ChatMessageCount, &item.TranscriptSentiment, &item.TranscriptConfidence,
			&item.TranscriptTextLength, &item.Delta, &item.Similarity, &item.Relationship, &item.OverlapSeconds,
			&item.Quality, &flags); err != nil {
			return nil, err
		}
		item.Type = "alignment_bucket"
		_ = json.Unmarshal(flags, &item.QualityFlags)
		out = append(out, item)
	}
	return out, rows.Err()
}

func jsonString(value any) string {
	if value == nil {
		return "null"
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(data)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

var _ Store = (*PostgresStore)(nil)
