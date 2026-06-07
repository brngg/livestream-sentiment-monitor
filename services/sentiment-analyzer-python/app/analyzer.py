from __future__ import annotations

from collections import Counter
from datetime import datetime
from functools import lru_cache
import os
import threading
from typing import Any, Protocol


DEFAULT_MODEL = "cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual"
DEFAULT_BATCH_SIZE = 32
DEFAULT_TRACE_LIMIT = 18
DEFAULT_MAX_MESSAGES = 32
DEFAULT_MAX_TEXT_CHARS = 280

LABEL_TO_SCORE = {
    "negative": -1.0,
    "neutral": 0.0,
    "positive": 1.0,
    "label_0": -1.0,
    "label_1": 0.0,
    "label_2": 1.0,
}

LABEL_ALIASES = {
    "label_0": "negative",
    "label_1": "neutral",
    "label_2": "positive",
}


class TextClassifier(Protocol):
    def __call__(self, texts: list[str], **kwargs: Any) -> list[dict[str, Any]]:
        ...


class TransformersSentimentAnalyzer:
    def __init__(
        self,
        model_name: str | None = None,
        classifier: TextClassifier | None = None,
        batch_size: int | None = None,
        trace_limit: int | None = None,
        max_messages: int | None = None,
        max_text_chars: int | None = None,
    ) -> None:
        self.model_name = model_name or os.getenv("SENTIMENT_MODEL", DEFAULT_MODEL)
        self.batch_size = batch_size or int(os.getenv("SENTIMENT_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
        self.trace_limit = trace_limit or int(os.getenv("SENTIMENT_TRACE_LIMIT", str(DEFAULT_TRACE_LIMIT)))
        self.max_messages = max_messages if max_messages is not None else int(os.getenv("SENTIMENT_MAX_MESSAGES", str(DEFAULT_MAX_MESSAGES)))
        self.max_text_chars = max_text_chars if max_text_chars is not None else int(os.getenv("SENTIMENT_MAX_TEXT_CHARS", str(DEFAULT_MAX_TEXT_CHARS)))
        self._classifier = classifier
        self._inference_lock = threading.Lock()

    def analyze_bucket(
        self,
        messages: list[dict[str, Any]],
        peak_window_start: Any | None = None,
        peak_window_end: Any | None = None,
    ) -> dict[str, Any]:
        analyzable_messages = normalize_messages(messages, self.max_text_chars)
        analyzable_messages = select_analysis_messages(
            analyzable_messages,
            self.max_messages,
            peak_window_start=peak_window_start,
            peak_window_end=peak_window_end,
        )
        if not analyzable_messages:
            return {
                "message_count": len(messages),
                "analyzed_count": 0,
                "analysis_message_limit": active_limit(self.max_messages),
                "sentiment_score": 0.0,
                "positive": 0.0,
                "neutral": 1.0,
                "negative": 0.0,
                "confidence": 0.0,
                "model": self.model_name,
                "message_scores": [],
            }

        texts = [str(message["text"]) for message in analyzable_messages]
        counts = Counter(texts)
        unique_texts = list(counts.keys())
        with self._inference_lock:
            predictions = self.classifier(
                unique_texts,
                batch_size=self.batch_size,
                truncation=True,
            )
        scored = dict(zip(unique_texts, predictions, strict=True))
        return aggregate_predictions(messages, analyzable_messages, counts, scored, self.model_name, self.trace_limit, self.max_messages)

    @property
    def classifier(self) -> TextClassifier:
        if self._classifier is None:
            self._classifier = load_transformers_pipeline(self.model_name)
        return self._classifier


def analyze_bucket(
    messages: list[dict[str, Any]],
    peak_window_start: Any | None = None,
    peak_window_end: Any | None = None,
) -> dict[str, Any]:
    return get_analyzer().analyze_bucket(messages, peak_window_start=peak_window_start, peak_window_end=peak_window_end)


def aggregate_predictions(
    messages: list[dict[str, Any]],
    analyzable_messages: list[dict[str, Any]],
    counts: Counter[str],
    predictions: dict[str, dict[str, Any]],
    model_name: str,
    trace_limit: int,
    analysis_message_limit: int,
) -> dict[str, Any]:
    weighted_score = 0.0
    weighted_confidence = 0.0
    labels = Counter()
    analyzed_count = 0
    scored_messages = []

    for text, count in counts.items():
        prediction = predictions[text]
        label = normalize_label(prediction.get("label", "neutral"))
        confidence = float(prediction.get("score", 0.0))
        weighted_score += label_score(label) * confidence * count
        weighted_confidence += confidence * count
        labels[label] += count
        analyzed_count += count

    for message in analyzable_messages:
        text = normalize_text(message.get("text", ""))
        prediction = predictions[text]
        label = normalize_label(prediction.get("label", "neutral"))
        confidence = float(prediction.get("score", 0.0))
        scored_messages.append(
            {
                "message_id": str(message.get("message_id", "")),
                "timestamp": message.get("timestamp", ""),
                "username": str(message.get("username", "")),
                "display_name": str(message.get("display_name", "")),
                "text": text,
                "label": label,
                "confidence": confidence,
                "sentiment_score": label_score(label) * confidence,
            }
        )

    if analyzed_count == 0:
        sentiment_score = 0.0
        confidence = 0.0
    else:
        sentiment_score = weighted_score / analyzed_count
        confidence = weighted_confidence / analyzed_count

    return {
        "message_count": len(messages),
        "analyzed_count": analyzed_count,
        "analysis_message_limit": active_limit(analysis_message_limit),
        "sentiment_score": sentiment_score,
        "positive": labels["positive"] / analyzed_count if analyzed_count else 0.0,
        "neutral": labels["neutral"] / analyzed_count if analyzed_count else 1.0,
        "negative": labels["negative"] / analyzed_count if analyzed_count else 0.0,
        "confidence": confidence,
        "model": model_name,
        "message_scores": select_trace_messages(scored_messages, trace_limit),
    }


def normalize_messages(messages: list[dict[str, Any]], max_text_chars: int) -> list[dict[str, Any]]:
    normalized = []
    for message in messages:
        text = normalize_text(message.get("text", ""))
        if not text:
            continue
        if max_text_chars > 0:
            text = normalize_text(text[:max_text_chars])
            if not text:
                continue
        normalized_message = dict(message)
        normalized_message["text"] = text
        normalized.append(normalized_message)
    return normalized


def select_analysis_messages(
    messages: list[dict[str, Any]],
    max_messages: int,
    peak_window_start: Any | None = None,
    peak_window_end: Any | None = None,
) -> list[dict[str, Any]]:
    if max_messages <= 0 or len(messages) <= max_messages:
        return messages
    if max_messages == 1:
        peak_messages = messages_in_peak_window(messages, peak_window_start, peak_window_end)
        return [peak_messages[0] if peak_messages else messages[len(messages) // 2]]

    peak_messages = messages_in_peak_window(messages, peak_window_start, peak_window_end)
    selected = []
    selected_ids = set()
    for message in peak_messages[:max_messages]:
        selected.append(message)
        selected_ids.add(message_identity(message))
    remaining_slots = max_messages - len(selected)
    if remaining_slots <= 0:
        return selected

    candidates = [message for message in messages if message_identity(message) not in selected_ids]
    selected.extend(evenly_sample_messages(candidates, remaining_slots))
    return selected


def evenly_sample_messages(messages: list[dict[str, Any]], max_messages: int) -> list[dict[str, Any]]:
    if max_messages <= 0:
        return []
    if len(messages) <= max_messages:
        return messages
    if max_messages == 1:
        return [messages[len(messages) // 2]]
    step = (len(messages) - 1) / (max_messages - 1)
    selected = []
    previous_index = -1
    for offset in range(max_messages):
        index = round(offset * step)
        if index == previous_index:
            index = min(previous_index + 1, len(messages) - 1)
        selected.append(messages[index])
        previous_index = index
    return selected


def messages_in_peak_window(
    messages: list[dict[str, Any]],
    peak_window_start: Any | None,
    peak_window_end: Any | None,
) -> list[dict[str, Any]]:
    start = parse_timestamp(peak_window_start)
    end = parse_timestamp(peak_window_end)
    if start is None or end is None or end <= start:
        return []
    selected = []
    for message in messages:
        timestamp = parse_timestamp(message.get("timestamp"))
        if timestamp is not None and start <= timestamp < end:
            selected.append(message)
    return selected


def parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def message_identity(message: dict[str, Any]) -> str:
    return str(message.get("message_id") or message.get("timestamp") or message.get("text") or id(message))


def active_limit(limit: int) -> int:
    return max(0, limit)


def select_trace_messages(messages: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    if len(messages) <= limit:
        return messages

    selected = []
    selected_ids = set()
    per_label = max(1, limit // 3)
    for label in ("positive", "neutral", "negative"):
        label_messages = sorted(
            [message for message in messages if message["label"] == label],
            key=lambda message: message["confidence"],
            reverse=True,
        )
        for message in label_messages[:per_label]:
            key = message_key(message)
            selected.append(message)
            selected_ids.add(key)

    remaining_slots = limit - len(selected)
    if remaining_slots > 0:
        remaining = sorted(messages, key=lambda message: message["confidence"], reverse=True)
        for message in remaining:
            key = message_key(message)
            if key in selected_ids:
                continue
            selected.append(message)
            selected_ids.add(key)
            if len(selected) >= limit:
                break

    return selected[:limit]


def message_key(message: dict[str, Any]) -> str:
    return str(message.get("message_id") or message.get("text") or id(message))


def normalize_label(value: Any) -> str:
    label = str(value or "neutral").strip().lower()
    return LABEL_ALIASES.get(label, label)


def label_score(label: str) -> float:
    return LABEL_TO_SCORE.get(label, 0.0)


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


@lru_cache(maxsize=1)
def get_analyzer() -> TransformersSentimentAnalyzer:
    return TransformersSentimentAnalyzer()


def load_transformers_pipeline(model_name: str) -> TextClassifier:
    try:
        from transformers import pipeline
    except ImportError as exc:
        raise RuntimeError(
            "Missing Python dependencies. Run `pip install -r requirements.txt` "
            "from services/sentiment-analyzer-python."
        ) from exc

    return pipeline(
        "sentiment-analysis",
        model=model_name,
        tokenizer=model_name,
        device=resolve_device(),
    )


def resolve_device() -> int | str:
    configured = os.getenv("SENTIMENT_DEVICE", "").strip()
    if configured:
        if configured.lstrip("-").isdigit():
            return int(configured)
        return configured

    try:
        import torch
    except ImportError:
        return -1

    if torch.cuda.is_available():
        return 0
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return "mps"
    return -1
