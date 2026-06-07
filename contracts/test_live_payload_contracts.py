import json
import re
import unittest
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "contracts" / "schemas" / "live-payloads.schema.json"


def load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text())


def schema_def(schema: dict, name: str) -> dict:
    return schema["$defs"][name]


def schema_properties(schema: dict, name: str) -> set[str]:
    return set(schema_def(schema, name).get("properties", {}))


class ContractValidationError(AssertionError):
    pass


class MiniSchemaValidator:
    """Small contract checker for the JSON Schema subset used in this repo."""

    def __init__(self, schema: dict):
        self.schema = schema

    def validate_ref(self, ref: str, value: object) -> None:
        self.validate({"$ref": ref}, value, ref)

    def validate(self, spec: dict, value: object, path: str = "$") -> None:
        if "$ref" in spec:
            self.validate(self.resolve_ref(spec["$ref"]), value, path)
            return
        if "anyOf" in spec:
            errors = []
            for option in spec["anyOf"]:
                try:
                    self.validate(option, value, path)
                    return
                except ContractValidationError as exc:
                    errors.append(str(exc))
            raise ContractValidationError(f"{path}: no anyOf option matched: {errors}")
        if "const" in spec and value != spec["const"]:
            raise ContractValidationError(f"{path}: expected const {spec['const']!r}, got {value!r}")
        if "enum" in spec and value not in spec["enum"]:
            raise ContractValidationError(f"{path}: expected one of {spec['enum']!r}, got {value!r}")

        expected_type = spec.get("type")
        if expected_type:
            self.validate_type(expected_type, value, path)

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            minimum = spec.get("minimum")
            maximum = spec.get("maximum")
            if minimum is not None and value < minimum:
                raise ContractValidationError(f"{path}: expected >= {minimum}, got {value}")
            if maximum is not None and value > maximum:
                raise ContractValidationError(f"{path}: expected <= {maximum}, got {value}")

        if spec.get("format") == "date-time":
            self.validate_timestamp(value, path)

        if expected_type == "object":
            self.validate_object(spec, value, path)
        if expected_type == "array":
            item_spec = spec.get("items", {})
            for index, item in enumerate(value):
                self.validate(item_spec, item, f"{path}[{index}]")

    def resolve_ref(self, ref: str) -> dict:
        if not ref.startswith("#/$defs/"):
            raise ContractValidationError(f"unsupported ref {ref}")
        return self.schema["$defs"][ref.removeprefix("#/$defs/")]

    def validate_type(self, expected_type: str, value: object, path: str) -> None:
        checks = {
            "object": lambda item: isinstance(item, dict),
            "array": lambda item: isinstance(item, list),
            "string": lambda item: isinstance(item, str),
            "number": lambda item: isinstance(item, (int, float)) and not isinstance(item, bool),
            "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
            "boolean": lambda item: isinstance(item, bool),
            "null": lambda item: item is None,
        }
        if not checks[expected_type](value):
            raise ContractValidationError(f"{path}: expected {expected_type}, got {type(value).__name__}")

    def validate_timestamp(self, value: object, path: str) -> None:
        if not isinstance(value, str):
            raise ContractValidationError(f"{path}: expected timestamp string")
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ContractValidationError(f"{path}: invalid timestamp {value!r}") from exc

    def validate_object(self, spec: dict, value: object, path: str) -> None:
        properties = spec.get("properties", {})
        for key in spec.get("required", []):
            if key not in value:
                raise ContractValidationError(f"{path}: missing required property {key!r}")

        additional = spec.get("additionalProperties", True)
        for key, item in value.items():
            if key in properties:
                self.validate(properties[key], item, f"{path}.{key}")
                continue
            if additional is False:
                raise ContractValidationError(f"{path}: unexpected property {key!r}")
            if isinstance(additional, dict):
                self.validate(additional, item, f"{path}.{key}")


def extract_go_json_fields(relative_path: str, struct_name: str) -> set[str]:
    source = (ROOT / relative_path).read_text()
    match = re.search(rf"type\s+{re.escape(struct_name)}\s+struct\s*\{{(?P<body>.*?)\n\}}", source, re.S)
    if not match:
        raise AssertionError(f"Go struct {struct_name} not found in {relative_path}")
    fields = set()
    for json_name in re.findall(r'`json:"([^",]+)', match.group("body")):
        if json_name != "-":
            fields.add(json_name)
    return fields


def extract_ts_type_body(source: str, type_name: str) -> str:
    marker = f"export type {type_name} = {{"
    start = source.find(marker)
    if start == -1:
        raise AssertionError(f"TypeScript type {type_name} not found")
    cursor = start + len(marker)
    depth = 1
    while cursor < len(source):
        char = source[cursor]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start + len(marker) : cursor]
        cursor += 1
    raise AssertionError(f"TypeScript type {type_name} was not closed")


def extract_ts_fields(type_name: str) -> set[str]:
    source = (ROOT / "apps/dashboard/src/types.ts").read_text()
    body = extract_ts_type_body(source, type_name)
    fields = set()
    depth = 1
    current_line = []
    for char in body:
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        if char == "\n":
            line = "".join(current_line).strip()
            current_line = []
            if depth == 1:
                match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\??:", line)
                if match:
                    fields.add(match.group(1))
            continue
        current_line.append(char)
    return fields


def assert_schema_covers_fields(test_case: unittest.TestCase, schema: dict, schema_name: str, fields: set[str], source: str) -> None:
    allowed = schema_properties(schema, schema_name)
    missing = sorted(fields - allowed)
    test_case.assertFalse(missing, f"{schema_name} schema is missing {source} fields: {missing}")


def assert_cpp_source_contains_fields(test_case: unittest.TestCase, fields: set[str], source: str) -> None:
    cpp_source = (ROOT / "services/transcript-ingestor-cpp/src/main.cpp").read_text()
    missing = sorted(field for field in fields if f'"{field}"' not in cpp_source and f'\\"{field}\\"' not in cpp_source)
    test_case.assertFalse(missing, f"{source} is missing expected C++ JSON fields: {missing}")


def assert_fields_match_schema(
    test_case: unittest.TestCase,
    schema: dict,
    schema_name: str,
    fields: set[str],
    source: str,
    *,
    schema_only: dict[str, str] | None = None,
    source_only: dict[str, str] | None = None,
) -> None:
    schema_only = schema_only or {}
    source_only = source_only or {}
    schema_fields = schema_properties(schema, schema_name)
    missing_from_schema = fields - schema_fields
    missing_from_source = schema_fields - fields

    undocumented_source_only = sorted(missing_from_schema - set(source_only))
    undocumented_schema_only = sorted(missing_from_source - set(schema_only))
    test_case.assertFalse(
        undocumented_source_only,
        f"{schema_name} schema is missing {source} fields without an allowlist reason: {undocumented_source_only}",
    )
    test_case.assertFalse(
        undocumented_schema_only,
        f"{source} is missing {schema_name} schema fields without an allowlist reason: {undocumented_schema_only}",
    )

    stale_source_only = sorted(set(source_only) - missing_from_schema)
    stale_schema_only = sorted(set(schema_only) - missing_from_source)
    test_case.assertFalse(
        stale_source_only,
        f"{source} source-only allowlist entries no longer differ from {schema_name}: {stale_source_only}",
    )
    test_case.assertFalse(
        stale_schema_only,
        f"{source} schema-only allowlist entries no longer differ from {schema_name}: {stale_schema_only}",
    )


def sample_chat_message() -> dict:
    return {
        "session_id": "session_123",
        "channel_id": "streamer_name",
        "message_id": "twitch_message_id_1",
        "timestamp": "2026-04-29T18:00:26Z",
        "username": "viewer42",
        "display_name": "Viewer42",
        "text": "NO WAY dragon",
        "emotes": [],
        "badges": [],
        "language": "en",
        "is_mod": False,
        "is_bot_likely": False,
    }


class LivePayloadSchemaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = load_schema()
        self.validator = MiniSchemaValidator(self.schema)

    def test_schema_json_loads_and_definitions_exist(self) -> None:
        expected_defs = {
            "ChatBucket",
            "TranscriptBucket",
            "TranscriptUpdate",
            "AlignmentBucket",
            "DashboardEvent",
            "TranscriptState",
            "SignalWindow",
            "SignalEvent",
        }
        self.assertTrue(expected_defs.issubset(self.schema["$defs"]))

    def test_representative_payloads_validate(self) -> None:
        chat_bucket = {
            "type": "chat_bucket",
            "session_id": "session_123",
            "channel_id": "streamer_name",
            "bucket_start": "2026-04-29T18:00:00Z",
            "bucket_end": "2026-04-29T18:00:30Z",
            "message_count": 184,
            "unique_chatters": 91,
            "chat_sentiment": 0.44,
            "sentiment_confidence": 0.73,
            "analyzed_count": 105,
            "positive": 0.11,
            "neutral": 0.88,
            "negative": 0.01,
            "analysis_status": "python",
            "sentiment_model": "cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual",
            "analysis_latency_ms": 1,
            "language_mix": {"en": 0.82, "other": 0.18},
            "top_terms": ["boss", "clutch"],
            "top_emotes": ["PogChamp"],
            "message_scores": [
                {
                    "message_id": "twitch_message_id_1",
                    "timestamp": "2026-04-29T18:00:26Z",
                    "username": "viewer42",
                    "display_name": "Viewer42",
                    "text": "NO WAY dragon",
                    "label": "positive",
                    "confidence": 0.94,
                    "sentiment_score": 0.94,
                }
            ],
            "peak_reaction_score": 0.84,
            "peak_reaction_type": "hype",
            "peak_target_type": "unknown",
            "peak_target_text": "boss",
            "peak_source": "chat",
            "peak_event_hint": "hype:boss",
            "peak_confidence": 0.84,
            "peak_evidence_ids": ["twitch_message_id_1"],
            "peak_time": "2026-04-29T18:00:17Z",
            "peak_window_start": "2026-04-29T18:00:12Z",
            "peak_window_end": "2026-04-29T18:00:17Z",
            "subwindows": [
                {
                    "window_start": "2026-04-29T18:00:12Z",
                    "window_end": "2026-04-29T18:00:17Z",
                    "message_count": 34,
                    "reaction_score": 0.84,
                    "hype_score": 0.84,
                    "intensity_score": 0.76,
                    "confusion_score": 0.07,
                    "frustration_score": 0.02,
                    "reaction_type": "hype",
                    "target_type": "unknown",
                    "source": "chat",
                    "confidence": 0.84,
                    "evidence_ids": ["twitch_message_id_1"],
                }
            ],
            "peak_evidence_messages": [sample_chat_message()],
        }
        transcript_bucket = {
            "type": "transcript_bucket",
            "session_id": "session_123",
            "channel_id": "streamer_name",
            "bucket_start": "2026-04-29T18:00:00Z",
            "bucket_end": "2026-04-29T18:00:30Z",
            "audio_started_at": "2026-04-29T18:00:00Z",
            "audio_ended_at": "2026-04-29T18:00:30Z",
            "transcribed_at": "2026-04-29T18:00:31.200Z",
            "asr_latency_ms": 940,
            "pipeline_latency_ms": 1200,
            "text": "I did not expect that fight to go this badly.",
            "language": "en",
            "transcript_confidence": 0.88,
            "sentiment_score": -0.27,
            "sentiment_confidence": 0.69,
            "sentiment_label": "negative",
            "sentiment_model": "local",
            "sentiment_status": "python",
            "sentiment_latency_ms": 18,
            "segments": [
                {
                    "start": 0.0,
                    "end": 4.2,
                    "text": "I did not expect that fight to go this badly.",
                    "confidence": 0.84,
                    "words": [{"start": 0.0, "end": 0.3, "text": "I", "probability": 0.96}],
                }
            ],
            "quality": {
                "raw_segment_count": 1,
                "retained_segment_count": 1,
                "dropped_low_confidence_count": 0,
                "dropped_repeat_count": 0,
                "retained_ratio": 1,
            },
        }
        alignment = {
            "type": "alignment_bucket",
            "session_id": "session_123",
            "channel_id": "streamer_name",
            "window_start": "2026-04-29T18:00:00Z",
            "window_end": "2026-04-29T18:00:30Z",
            "chat_bucket_start": "2026-04-29T18:00:00Z",
            "chat_bucket_end": "2026-04-29T18:00:30Z",
            "transcript_bucket_start": "2026-04-29T18:00:00Z",
            "transcript_bucket_end": "2026-04-29T18:00:30Z",
            "chat_sentiment": 0.44,
            "chat_confidence": 0.73,
            "chat_message_count": 184,
            "transcript_sentiment": -0.27,
            "transcript_confidence": 0.69,
            "transcript_text_length": 48,
            "delta": 0.71,
            "similarity": 0.64,
            "relationship": "diverged",
            "overlap_seconds": 30,
            "quality": 0.82,
            "quality_flags": [],
        }
        event = {
            "type": "transcript_bucket",
            "session_id": "session_123",
            "channel": "streamer_name",
            "bucket": chat_bucket,
            "transcript_bucket": transcript_bucket,
            "alignments": [alignment],
            "signal_windows": [
                {
                    "type": "signal_window",
                    "session_id": "session_123",
                    "channel_id": "streamer_name",
                    "source": "alignment",
                    "stream_id": "streamer_name",
                    "window_start": "2026-04-29T18:00:00Z",
                    "window_end": "2026-04-29T18:00:30Z",
                    "message_count": 184,
                    "unique_chatters": 91,
                    "chat_sentiment": 0.44,
                    "chat_confidence": 0.73,
                    "sentiment_confidence": 0.73,
                    "positive": 0.11,
                    "neutral": 0.88,
                    "negative": 0.01,
                    "transcript_sentiment": -0.27,
                    "transcript_confidence": 0.69,
                    "delta": 0.71,
                    "similarity": 0.64,
                    "relationship": "diverged",
                    "quality": 0.82,
                    "target_type": "unknown",
                    "confidence": 0.78,
                    "events": [
                        {
                            "type": "content_audience_divergence",
                            "severity": 0.355,
                            "timestamp": "2026-04-29T18:00:00Z",
                            "source": "alignment",
                            "confidence": 0.78,
                            "target_type": "unknown",
                        }
                    ],
                }
            ],
        }
        transcript_state = {
            "status": "ingesting",
            "mode": "all",
            "session_id": "session_123",
            "channel_id": "streamer_name",
            "bucket_seconds": 30,
            "chunk_seconds": 5,
            "caption": {
                "rolling_window_seconds": 12,
                "asr_interval_seconds": 2,
                "stability_passes": 2,
                "rolling_buffer_seconds": 45,
                "commit_lag_seconds": 2,
            },
            "partial_count": 1,
            "segment_count": 1,
            "bucket_count": 1,
            "partials": [
                {
                    "type": "transcript_partial",
                    "session_id": "session_123",
                    "channel_id": "streamer_name",
                    "transcript_start": "2026-04-29T18:00:00Z",
                    "transcript_end": "2026-04-29T18:00:05Z",
                    "audio_started_at": None,
                    "audio_ended_at": None,
                    "transcribed_at": None,
                    "asr_latency_ms": None,
                    "pipeline_latency_ms": None,
                    "text": "hello",
                    "language": "en",
                    "transcript_confidence": 0.85,
                    "segments": [],
                    "quality": {},
                }
            ],
            "buckets": [transcript_bucket],
            "latest_bucket": transcript_bucket,
        }

        self.validator.validate_ref("#/$defs/ChatBucket", chat_bucket)
        self.validator.validate_ref("#/$defs/TranscriptBucket", transcript_bucket)
        self.validator.validate_ref("#/$defs/AlignmentBucket", alignment)
        self.validator.validate_ref("#/$defs/DashboardEvent", event)
        self.validator.validate_ref("#/$defs/TranscriptState", transcript_state)

    def test_schema_rejects_unknown_core_fields(self) -> None:
        payload = {
            "type": "alignment_bucket",
            "session_id": "session_123",
            "channel_id": "streamer_name",
            "window_start": "2026-04-29T18:00:00Z",
            "window_end": "2026-04-29T18:00:30Z",
            "chat_bucket_start": "2026-04-29T18:00:00Z",
            "chat_bucket_end": "2026-04-29T18:00:30Z",
            "transcript_bucket_start": "2026-04-29T18:00:00Z",
            "transcript_bucket_end": "2026-04-29T18:00:30Z",
            "chat_sentiment": 0.44,
            "chat_confidence": 0.73,
            "chat_message_count": 184,
            "transcript_sentiment": -0.27,
            "transcript_confidence": 0.69,
            "transcript_text_length": 48,
            "delta": 0.71,
            "similarity": 0.64,
            "relationship": "diverged",
            "overlap_seconds": 30,
            "quality": 0.82,
            "quality_flags": [],
            "uncontracted_field": True,
        }
        with self.assertRaises(ContractValidationError):
            self.validator.validate_ref("#/$defs/AlignmentBucket", payload)

    def test_go_live_payload_fields_are_covered_by_schema(self) -> None:
        mappings = [
            ("services/chat-ingestor-go/internal/chat/types.go", "ChatMessage", "ChatMessage"),
            ("services/chat-ingestor-go/internal/chat/types.go", "MessageScore", "MessageScore"),
            ("services/chat-ingestor-go/internal/chat/types.go", "ReactionSubwindow", "ReactionSubwindow"),
            ("services/chat-ingestor-go/internal/chat/types.go", "ChatBucket", "ChatBucket"),
            ("services/chat-ingestor-go/internal/chat/types.go", "ReactionWindow", "ReactionWindow"),
            ("services/chat-ingestor-go/internal/analysis/alignment.go", "AlignmentBucket", "AlignmentBucket"),
            ("services/chat-ingestor-go/internal/analysis/signal_window.go", "SignalEvent", "SignalEvent"),
            ("services/chat-ingestor-go/internal/analysis/signal_window.go", "SignalWindow", "SignalWindow"),
            ("services/chat-ingestor-go/cmd/chat-dashboard/main.go", "dashboardEvent", "DashboardEvent"),
            ("services/chat-ingestor-go/cmd/chat-dashboard/main.go", "transcriptBucket", "TranscriptBucket"),
            ("services/chat-ingestor-go/cmd/chat-dashboard/main.go", "transcriptSegment", "TranscriptSegment"),
            ("services/chat-ingestor-go/cmd/chat-dashboard/main.go", "transcriptWord", "TranscriptWord"),
        ]
        for path, struct_name, schema_name in mappings:
            with self.subTest(struct=struct_name):
                fields = extract_go_json_fields(path, struct_name)
                assert_schema_covers_fields(self, self.schema, schema_name, fields, f"{path}:{struct_name}")

    def test_typescript_live_payload_fields_are_covered_by_schema(self) -> None:
        mappings = [
            ("ChatMessage", "ChatMessage"),
            ("MessageScore", "MessageScore"),
            ("ChatBucketSubwindow", "ReactionSubwindow"),
            ("ChatBucket", "ChatBucket"),
            ("ReactionWindow", "ReactionWindow"),
            ("TranscriptWord", "TranscriptWord"),
            ("TranscriptSegment", "TranscriptSegment"),
            ("TranscriptBucket", "TranscriptBucket"),
            ("AlignmentBucket", "AlignmentBucket"),
            ("SignalEvent", "SignalEvent"),
            ("SignalWindow", "SignalWindow"),
            ("DashboardEvent", "DashboardEvent"),
            ("TranscriptState", "TranscriptState"),
        ]
        schema_only_allowlists = {
            ("ChatMessage", "ChatMessage"): {
                "emotes": "The dashboard ChatMessage type is a render projection for message identity/text; full evidence metadata remains in the backend contract.",
                "badges": "The dashboard ChatMessage type is a render projection for message identity/text; badge metadata is not consumed today.",
                "language": "The dashboard ChatMessage type is a render projection for message identity/text; language is consumed at bucket level.",
                "is_mod": "The dashboard ChatMessage type is a render projection for message identity/text; moderation flags are not rendered today.",
                "is_bot_likely": "The dashboard ChatMessage type is a render projection for message identity/text; bot-likelihood flags are not rendered today.",
            },
            ("TranscriptSegment", "TranscriptSegment"): {
                "start": "Frontend TranscriptSegment models live transcript_partial/transcript_segment events; timed bucket segment items are modeled inline on TranscriptBucket.segments.",
                "end": "Frontend TranscriptSegment models live transcript_partial/transcript_segment events; timed bucket segment items are modeled inline on TranscriptBucket.segments.",
            },
            ("TranscriptState", "TranscriptState"): {
                "caption": "Python exposes caption tuning metadata for diagnostics; the dashboard does not consume or render that object today.",
            },
        }
        for ts_name, schema_name in mappings:
            with self.subTest(type=ts_name):
                fields = extract_ts_fields(ts_name)
                assert_fields_match_schema(
                    self,
                    self.schema,
                    schema_name,
                    fields,
                    f"apps/dashboard/src/types.ts:{ts_name}",
                    schema_only=schema_only_allowlists.get((ts_name, schema_name)),
                )

    def test_cpp_transcript_payload_fields_are_covered_by_schema(self) -> None:
        segment_keys = {"start", "end", "text", "confidence", "words"}
        update_keys = {
            "type",
            "session_id",
            "channel_id",
            "transcript_start",
            "transcript_end",
            "audio_started_at",
            "audio_ended_at",
            "transcribed_at",
            "asr_latency_ms",
            "pipeline_latency_ms",
            "text",
            "language",
            "transcript_confidence",
            "segments",
            "quality",
        }
        bucket_keys = {
            "type",
            "session_id",
            "channel_id",
            "bucket_start",
            "bucket_end",
            "audio_started_at",
            "audio_ended_at",
            "transcribed_at",
            "asr_latency_ms",
            "pipeline_latency_ms",
            "text",
            "language",
            "transcript_confidence",
            "sentiment_score",
            "sentiment_confidence",
            "sentiment_label",
            "sentiment_model",
            "sentiment_status",
            "sentiment_latency_ms",
            "segments",
            "quality",
        }
        state_keys = {
            "status",
            "session_id",
            "channel_id",
            "bucket_seconds",
            "chunk_seconds",
            "caption",
            "partial_count",
            "bucket_count",
            "segment_count",
            "error",
            "mode",
            "partials",
            "segments",
            "latest_partial",
            "latest_segment",
            "buckets",
            "latest_bucket",
        }
        caption_keys = {
            "rolling_window_seconds",
            "asr_interval_seconds",
            "stability_passes",
            "rolling_buffer_seconds",
            "commit_lag_seconds",
        }
        assert_schema_covers_fields(
            self,
            self.schema,
            "TranscriptSegment",
            segment_keys,
            "services/transcript-ingestor-cpp/src/main.cpp:segment_json",
        )
        assert_cpp_source_contains_fields(self, segment_keys, "services/transcript-ingestor-cpp/src/main.cpp:segment_json")
        assert_schema_covers_fields(
            self,
            self.schema,
            "TranscriptUpdate",
            update_keys,
            "services/transcript-ingestor-cpp/src/main.cpp:UpdateJson",
        )
        assert_cpp_source_contains_fields(self, update_keys, "services/transcript-ingestor-cpp/src/main.cpp:update_json")
        self.assertTrue({"transcript_start", "transcript_end"}.issubset(update_keys))

        assert_schema_covers_fields(
            self,
            self.schema,
            "TranscriptBucket",
            bucket_keys,
            "services/transcript-ingestor-cpp/src/main.cpp:bucket_json",
        )
        assert_cpp_source_contains_fields(self, bucket_keys, "services/transcript-ingestor-cpp/src/main.cpp:bucket_json")
        assert_schema_covers_fields(
            self,
            self.schema,
            "TranscriptState",
            state_keys,
            "services/transcript-ingestor-cpp/src/main.cpp:state_json",
        )
        assert_cpp_source_contains_fields(self, state_keys, "services/transcript-ingestor-cpp/src/main.cpp:state_json")
        caption_schema = schema_def(self.schema, "TranscriptState")["properties"]["caption"]
        missing_caption = sorted(caption_keys - set(caption_schema["properties"]))
        self.assertFalse(
            missing_caption,
            f"TranscriptState.caption schema is missing C++ state fields: {missing_caption}",
        )


if __name__ == "__main__":
    unittest.main()
