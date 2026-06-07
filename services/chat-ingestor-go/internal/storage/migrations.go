package storage

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const Migration001 = `
CREATE TABLE IF NOT EXISTS storage_migrations (
	version TEXT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stream_sessions (
	session_id TEXT PRIMARY KEY,
	channel_id TEXT NOT NULL,
	twitch_stream_id TEXT NOT NULL DEFAULT '',
	twitch_user_id TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL,
	started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	ended_at TIMESTAMPTZ,
	first_seen_at TIMESTAMPTZ,
	last_seen_at TIMESTAMPTZ,
	bucket_seconds INTEGER NOT NULL DEFAULT 30,
	transcript_bucket_seconds INTEGER NOT NULL DEFAULT 30,
	transcript_chunk_seconds INTEGER NOT NULL DEFAULT 1,
	nlp_analyzer_url TEXT NOT NULL DEFAULT '',
	sentiment_model TEXT NOT NULL DEFAULT '',
	stream_title TEXT NOT NULL DEFAULT '',
	stream_game TEXT NOT NULL DEFAULT '',
	stream_viewer_count INTEGER,
	stream_started_at TIMESTAMPTZ,
	stream_language TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE stream_sessions ADD COLUMN IF NOT EXISTS twitch_stream_id TEXT NOT NULL DEFAULT '';
ALTER TABLE stream_sessions ADD COLUMN IF NOT EXISTS twitch_user_id TEXT NOT NULL DEFAULT '';
ALTER TABLE stream_sessions ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ;
ALTER TABLE stream_sessions ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

UPDATE stream_sessions
SET
	first_seen_at = COALESCE(first_seen_at, stream_started_at, started_at),
	last_seen_at = COALESCE(last_seen_at, started_at)
WHERE first_seen_at IS NULL OR last_seen_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS stream_sessions_channel_twitch_stream_uidx
	ON stream_sessions (channel_id, twitch_stream_id)
	WHERE twitch_stream_id <> '';

CREATE INDEX IF NOT EXISTS stream_sessions_channel_status_idx
	ON stream_sessions (channel_id, status, started_at DESC);

CREATE TABLE IF NOT EXISTS ingestion_runs (
	run_id TEXT PRIMARY KEY,
	session_id TEXT NOT NULL REFERENCES stream_sessions(session_id) ON DELETE CASCADE,
	started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	ended_at TIMESTAMPTZ,
	status TEXT NOT NULL,
	stop_reason TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ingestion_runs_session_time_idx ON ingestion_runs (session_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ingestion_runs_status_idx ON ingestion_runs (status, started_at DESC);

CREATE TABLE IF NOT EXISTS chat_buckets (
	id BIGSERIAL PRIMARY KEY,
	session_id TEXT NOT NULL,
	channel_id TEXT NOT NULL,
	bucket_start TIMESTAMPTZ NOT NULL,
	bucket_end TIMESTAMPTZ NOT NULL,
	message_count INTEGER NOT NULL,
	unique_chatters INTEGER NOT NULL,
	chat_sentiment DOUBLE PRECISION NOT NULL,
	sentiment_confidence DOUBLE PRECISION NOT NULL,
	analyzed_count INTEGER NOT NULL DEFAULT 0,
	positive_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
	neutral_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
	negative_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
	sentiment_model TEXT NOT NULL DEFAULT '',
	analysis_latency_ms BIGINT NOT NULL DEFAULT 0,
	analysis_status TEXT NOT NULL DEFAULT '',
	language_mix JSONB NOT NULL DEFAULT '{}'::jsonb,
	top_terms JSONB NOT NULL DEFAULT '[]'::jsonb,
	top_emotes JSONB NOT NULL DEFAULT '[]'::jsonb,
	message_scores JSONB NOT NULL DEFAULT '[]'::jsonb,
	peak_reaction_score DOUBLE PRECISION,
	peak_reaction_type TEXT NOT NULL DEFAULT '',
	peak_target_type TEXT NOT NULL DEFAULT '',
	peak_target_text TEXT NOT NULL DEFAULT '',
	peak_source TEXT NOT NULL DEFAULT '',
	peak_event_hint TEXT NOT NULL DEFAULT '',
	peak_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
	peak_evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
	peak_time TIMESTAMPTZ,
	peak_window_start TIMESTAMPTZ,
	peak_window_end TIMESTAMPTZ,
	subwindows JSONB NOT NULL DEFAULT '[]'::jsonb,
	peak_evidence_messages JSONB NOT NULL DEFAULT '[]'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE (session_id, channel_id, bucket_start, bucket_end)
);

ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_reaction_score DOUBLE PRECISION;
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_reaction_type TEXT NOT NULL DEFAULT '';
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_target_type TEXT NOT NULL DEFAULT '';
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_target_text TEXT NOT NULL DEFAULT '';
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_source TEXT NOT NULL DEFAULT '';
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_event_hint TEXT NOT NULL DEFAULT '';
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_confidence DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_time TIMESTAMPTZ;
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_window_start TIMESTAMPTZ;
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_window_end TIMESTAMPTZ;
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS subwindows JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE chat_buckets ADD COLUMN IF NOT EXISTS peak_evidence_messages JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS chat_message_samples (
	id BIGSERIAL PRIMARY KEY,
	session_id TEXT NOT NULL,
	channel_id TEXT NOT NULL,
	message_id TEXT NOT NULL,
	bucket_start TIMESTAMPTZ NOT NULL,
	bucket_end TIMESTAMPTZ NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL,
	user_hash TEXT NOT NULL,
	text TEXT NOT NULL,
	label TEXT NOT NULL DEFAULT '',
	confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
	sentiment_score DOUBLE PRECISION NOT NULL DEFAULT 0,
	human_label TEXT NOT NULL DEFAULT '',
	evidence_rank INTEGER NOT NULL DEFAULT 0,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE (session_id, message_id)
);

CREATE TABLE IF NOT EXISTS transcript_buckets (
	id BIGSERIAL PRIMARY KEY,
	session_id TEXT NOT NULL,
	channel_id TEXT NOT NULL,
	bucket_start TIMESTAMPTZ NOT NULL,
	bucket_end TIMESTAMPTZ NOT NULL,
	audio_started_at TIMESTAMPTZ,
	audio_ended_at TIMESTAMPTZ,
	transcribed_at TIMESTAMPTZ,
	text TEXT NOT NULL DEFAULT '',
	language TEXT NOT NULL DEFAULT '',
	transcript_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
	transcript_status TEXT NOT NULL DEFAULT '',
	sentiment_score DOUBLE PRECISION,
	sentiment_confidence DOUBLE PRECISION,
	sentiment_label TEXT NOT NULL DEFAULT '',
	sentiment_model TEXT NOT NULL DEFAULT '',
	sentiment_status TEXT NOT NULL DEFAULT '',
	sentiment_latency_ms BIGINT,
	asr_latency_ms BIGINT,
	pipeline_latency_ms BIGINT,
	audio_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
	segment_count INTEGER NOT NULL DEFAULT 0,
	word_count INTEGER NOT NULL DEFAULT 0,
	empty_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
	repair_added_words INTEGER NOT NULL DEFAULT 0,
	repair_changed_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
	segments JSONB NOT NULL DEFAULT '[]'::jsonb,
	quality JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE (session_id, channel_id, bucket_start, bucket_end)
);

ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS audio_started_at TIMESTAMPTZ;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS audio_ended_at TIMESTAMPTZ;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS transcribed_at TIMESTAMPTZ;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS asr_latency_ms BIGINT;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS pipeline_latency_ms BIGINT;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS transcript_status TEXT NOT NULL DEFAULT '';
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS audio_seconds DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS segment_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS word_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS empty_ratio DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS repair_added_words INTEGER NOT NULL DEFAULT 0;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS repair_changed_ratio DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE transcript_buckets ADD COLUMN IF NOT EXISTS quality JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS alignment_buckets (
	id BIGSERIAL PRIMARY KEY,
	session_id TEXT NOT NULL,
	channel_id TEXT NOT NULL,
	window_start TIMESTAMPTZ NOT NULL,
	window_end TIMESTAMPTZ NOT NULL,
	chat_bucket_start TIMESTAMPTZ NOT NULL,
	chat_bucket_end TIMESTAMPTZ NOT NULL,
	transcript_bucket_start TIMESTAMPTZ NOT NULL,
	transcript_bucket_end TIMESTAMPTZ NOT NULL,
	chat_sentiment DOUBLE PRECISION NOT NULL,
	chat_confidence DOUBLE PRECISION NOT NULL,
	chat_message_count INTEGER NOT NULL,
	transcript_sentiment DOUBLE PRECISION NOT NULL,
	transcript_confidence DOUBLE PRECISION NOT NULL,
	transcript_text_length INTEGER NOT NULL,
	delta DOUBLE PRECISION NOT NULL,
	similarity DOUBLE PRECISION NOT NULL,
	relationship TEXT NOT NULL,
	overlap_seconds INTEGER NOT NULL,
	quality DOUBLE PRECISION NOT NULL,
	quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE (session_id, channel_id, window_start, window_end, chat_bucket_start, transcript_bucket_start)
);

CREATE TABLE IF NOT EXISTS human_labels (
	session_id TEXT NOT NULL,
	message_id TEXT NOT NULL,
	label TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY (session_id, message_id)
);

CREATE TABLE IF NOT EXISTS signal_window_labels (
	session_id TEXT NOT NULL,
	window_start TIMESTAMPTZ NOT NULL,
	window_end TIMESTAMPTZ NOT NULL,
	predicted_event TEXT NOT NULL DEFAULT '',
	predicted_relationship TEXT NOT NULL DEFAULT '',
	reaction_type TEXT NOT NULL DEFAULT '',
	target_type TEXT NOT NULL DEFAULT '',
	target_text TEXT NOT NULL DEFAULT '',
	divergence_type TEXT NOT NULL DEFAULT '',
	event_start TIMESTAMPTZ,
	event_peak TIMESTAMPTZ,
	correctness TEXT NOT NULL,
	event_label TEXT NOT NULL,
	notes TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY (session_id, window_start, window_end)
);
ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS reaction_type TEXT NOT NULL DEFAULT '';
ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS target_type TEXT NOT NULL DEFAULT '';
ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS target_text TEXT NOT NULL DEFAULT '';
ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS divergence_type TEXT NOT NULL DEFAULT '';
ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS event_start TIMESTAMPTZ;
ALTER TABLE signal_window_labels ADD COLUMN IF NOT EXISTS event_peak TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS evaluation_agent_reviews (
	review_id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL DEFAULT '',
	session_id TEXT NOT NULL,
	window_start TIMESTAMPTZ NOT NULL,
	window_end TIMESTAMPTZ NOT NULL,
	source_window_type TEXT NOT NULL DEFAULT '',
	reviewer TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL DEFAULT '',
	prompt_version TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'suggested',
	predicted_event TEXT NOT NULL DEFAULT '',
	suggested_event_label TEXT NOT NULL,
	correctness TEXT NOT NULL DEFAULT '',
	reaction_type TEXT NOT NULL DEFAULT '',
	target_type TEXT NOT NULL DEFAULT '',
	target_text TEXT NOT NULL DEFAULT '',
	divergence_type TEXT NOT NULL DEFAULT '',
	event_start TIMESTAMPTZ,
	event_peak TIMESTAMPTZ,
	confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
	streamer_usefulness DOUBLE PRECISION NOT NULL DEFAULT 0,
	reason TEXT NOT NULL DEFAULT '',
	evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
	notes TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS system_metrics (
	id BIGSERIAL PRIMARY KEY,
	session_id TEXT NOT NULL DEFAULT '',
	name TEXT NOT NULL,
	value DOUBLE PRECISION NOT NULL,
	unit TEXT NOT NULL DEFAULT '',
	recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS chat_buckets_session_time_idx ON chat_buckets (session_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS transcript_buckets_session_time_idx ON transcript_buckets (session_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS alignment_buckets_session_time_idx ON alignment_buckets (session_id, window_start DESC);
CREATE INDEX IF NOT EXISTS chat_message_samples_session_time_idx ON chat_message_samples (session_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS signal_window_labels_session_time_idx ON signal_window_labels (session_id, window_start DESC);
CREATE INDEX IF NOT EXISTS evaluation_agent_reviews_session_time_idx ON evaluation_agent_reviews (session_id, window_start DESC);
CREATE INDEX IF NOT EXISTS evaluation_agent_reviews_run_idx ON evaluation_agent_reviews (run_id, session_id);
CREATE INDEX IF NOT EXISTS system_metrics_session_time_idx ON system_metrics (session_id, recorded_at DESC);

INSERT INTO storage_migrations (version) VALUES ('001_persistent_storage_v1')
ON CONFLICT (version) DO NOTHING;
`

func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, Migration001)
	return err
}
