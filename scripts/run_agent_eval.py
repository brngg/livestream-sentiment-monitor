#!/usr/bin/env python3
"""Generate agent-assisted replay evaluation reviews.

The default judge is deterministic and offline. It behaves like a conservative
agent reviewer over replay artifacts, but it does not claim human ground truth
or call an external LLM. The output is intended to bootstrap review queues and
make eval coverage measurable before human verification.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


EVENT_LABELS = {
    "hype_spike",
    "frustration_spike",
    "audience_shift",
    "content_audience_divergence",
    "none",
}


@dataclass(frozen=True)
class EvalWindow:
    session_id: str
    channel_id: str
    source_window_type: str
    window_start: str
    window_end: str
    payload: dict[str, Any]
    chat_bucket: dict[str, Any] | None = None
    transcript_bucket: dict[str, Any] | None = None
    alignment: dict[str, Any] | None = None
    subwindow: dict[str, Any] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic agent-assisted eval reviews from replay fixtures.")
    parser.add_argument("--replay-fixture", required=True, help="Path to a SessionReplay fixture JSON file.")
    parser.add_argument("--session-id", action="append", default=[], help="Session id to review. Repeatable. Defaults to all sessions.")
    parser.add_argument("--out-dir", default="eval/reports", help="Directory to write agent eval reports.")
    parser.add_argument("--max-windows", type=int, default=200, help="Maximum windows to review across selected sessions.")
    parser.add_argument("--generated-at", default="1970-01-01T00:00:00Z", help="Deterministic report timestamp.")
    parser.add_argument("--run-id", default="", help="Review run id. Defaults to agent-eval-{generated-at}.")
    parser.add_argument("--reviewer", default="heuristic-agent-evaluator", help="Reviewer name stored in generated reviews.")
    parser.add_argument("--model", default="deterministic-replay-heuristic-v1", help="Model/method label stored in generated reviews.")
    parser.add_argument("--prompt-version", default="stream-evaluation-agent-v1", help="Prompt/rubric version stored in generated reviews.")
    parser.add_argument("--json-name", default="agent-eval-report.json", help="JSON report filename.")
    parser.add_argument("--markdown-name", default="agent-eval-report.md", help="Markdown report filename. Pass an empty value to skip Markdown output.")
    parser.add_argument("--reviews-name", default="agent-reviews.jsonl", help="JSONL reviews filename.")
    parser.add_argument("--reviewed-fixture", default="", help="Optional path to write a copy of the replay fixture with generated agent_reviews embedded.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fixture_path = Path(args.replay_fixture)
    replays = load_replays(fixture_path)
    selected = select_replays(replays, set(args.session_id))
    run_id = args.run_id or f"agent-eval-{safe_id(args.generated_at)}"
    reviews: list[dict[str, Any]] = []
    session_summaries: list[dict[str, Any]] = []

    remaining = args.max_windows
    for replay in selected:
        session_id = str(replay.get("session", {}).get("session_id", "")).strip()
        labels = replay.get("window_labels") or []
        windows = build_eval_windows(replay)
        if remaining >= 0:
            windows = windows[:remaining]
        session_reviews = [review_window(window, labels, args, run_id) for window in windows]
        reviews.extend(session_reviews)
        session_summaries.append(session_summary(replay, windows, session_reviews, labels))
        if remaining >= 0:
            remaining -= len(windows)
        if args.max_windows >= 0 and remaining <= 0:
            break
        if not session_id:
            continue

    report = build_report(args, fixture_path, selected, session_summaries, reviews)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / args.json_name
    reviews_path = out_dir / args.reviews_name
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = None
    if args.markdown_name.strip():
        markdown_path = out_dir / args.markdown_name
        markdown_path.write_text(render_markdown(report), encoding="utf-8")
    with reviews_path.open("w", encoding="utf-8") as handle:
        for review in reviews:
            handle.write(json.dumps(review, sort_keys=True, separators=(",", ":")) + "\n")
    if args.reviewed_fixture:
        write_reviewed_fixture(Path(args.reviewed_fixture), replays, reviews)
    print(f"Wrote {json_path}")
    if markdown_path is not None:
        print(f"Wrote {markdown_path}")
    print(f"Wrote {reviews_path}")
    if args.reviewed_fixture:
        print(f"Wrote {args.reviewed_fixture}")


def load_replays(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        replays = raw
    elif isinstance(raw, dict):
        if all(isinstance(value, dict) and "session" in value for value in raw.values()):
            replays = list(raw.values())
        elif "session" in raw:
            replays = [raw]
        else:
            raise ValueError("Replay fixture object must be a SessionReplay or a map of SessionReplay objects.")
    else:
        raise ValueError("Replay fixture must be a JSON object or array.")
    out = []
    for replay in replays:
        if not isinstance(replay, dict) or not isinstance(replay.get("session"), dict):
            raise ValueError("Each replay must be an object with a session object.")
        out.append(replay)
    return out


def select_replays(replays: list[dict[str, Any]], session_ids: set[str]) -> list[dict[str, Any]]:
    if not session_ids:
        return replays
    selected = [replay for replay in replays if str(replay.get("session", {}).get("session_id", "")) in session_ids]
    missing = sorted(session_ids - {str(replay.get("session", {}).get("session_id", "")) for replay in selected})
    if missing:
        raise ValueError(f"Session ids not found in fixture: {', '.join(missing)}")
    return selected


def build_eval_windows(replay: dict[str, Any]) -> list[EvalWindow]:
    session = replay.get("session") or {}
    session_id = str(session.get("session_id", "")).strip()
    channel_id = str(session.get("channel_id", "")).strip()
    transcript_buckets = replay.get("transcript_buckets") or []
    alignments = replay.get("alignments") or []
    windows: list[EvalWindow] = []

    for bucket in replay.get("chat_buckets") or []:
        if not isinstance(bucket, dict):
            continue
        windows.append(EvalWindow(
            session_id=session_id,
            channel_id=channel_id,
            source_window_type="chat_bucket_30s",
            window_start=str(bucket.get("bucket_start", "")),
            window_end=str(bucket.get("bucket_end", "")),
            payload=bucket,
            chat_bucket=bucket,
            transcript_bucket=best_transcript_for_range(bucket.get("bucket_start"), bucket.get("bucket_end"), transcript_buckets),
            alignment=best_alignment_for_range(bucket.get("bucket_start"), bucket.get("bucket_end"), alignments),
        ))
        for subwindow in bucket.get("subwindows") or []:
            if not isinstance(subwindow, dict):
                continue
            windows.append(EvalWindow(
                session_id=session_id,
                channel_id=channel_id,
                source_window_type="reaction_subwindow",
                window_start=str(subwindow.get("window_start") or bucket.get("bucket_start", "")),
                window_end=str(subwindow.get("window_end") or bucket.get("bucket_end", "")),
                payload=subwindow,
                chat_bucket=bucket,
                transcript_bucket=best_transcript_for_range(subwindow.get("window_start"), subwindow.get("window_end"), transcript_buckets),
                alignment=best_alignment_for_range(subwindow.get("window_start"), subwindow.get("window_end"), alignments),
                subwindow=subwindow,
            ))

    for alignment in alignments:
        if not isinstance(alignment, dict):
            continue
        windows.append(EvalWindow(
            session_id=session_id,
            channel_id=channel_id,
            source_window_type="alignment_bucket",
            window_start=str(alignment.get("window_start", "")),
            window_end=str(alignment.get("window_end", "")),
            payload=alignment,
            chat_bucket=best_chat_for_range(alignment.get("window_start"), alignment.get("window_end"), replay.get("chat_buckets") or []),
            transcript_bucket=best_transcript_for_range(alignment.get("window_start"), alignment.get("window_end"), transcript_buckets),
            alignment=alignment,
        ))

    if not replay.get("chat_buckets"):
        for bucket in transcript_buckets:
            if not isinstance(bucket, dict):
                continue
            windows.append(EvalWindow(
                session_id=session_id,
                channel_id=channel_id,
                source_window_type="transcript_bucket_30s",
                window_start=str(bucket.get("bucket_start", "")),
                window_end=str(bucket.get("bucket_end", "")),
                payload=bucket,
                transcript_bucket=bucket,
            ))

    return sorted(dedupe_windows(windows), key=lambda item: (item.window_start, item.source_window_type))


def dedupe_windows(windows: list[EvalWindow]) -> list[EvalWindow]:
    seen: set[tuple[str, str, str, str]] = set()
    out = []
    for window in windows:
        key = (window.session_id, window.source_window_type, window.window_start, window.window_end)
        if key in seen or not window.window_start or not window.window_end:
            continue
        seen.add(key)
        out.append(window)
    return out


def review_window(window: EvalWindow, labels: list[dict[str, Any]], args: argparse.Namespace, run_id: str) -> dict[str, Any]:
    suggested = suggested_event_label(window)
    gold = gold_label_for_window(window, labels)
    correctness = correctness_for_suggestion(suggested, gold)
    confidence = confidence_for_window(window, suggested, gold)
    event_start = event_start_for_window(window)
    event_peak = event_peak_for_window(window)
    review = {
        "review_id": review_id(run_id, window),
        "run_id": run_id,
        "session_id": window.session_id,
        "window_start": window.window_start,
        "window_end": window.window_end,
        "source_window_type": window.source_window_type,
        "reviewer": args.reviewer,
        "model": args.model,
        "prompt_version": args.prompt_version,
        "status": "agent_reviewed",
        "predicted_event": predicted_event(window),
        "suggested_event_label": suggested,
        "correctness": correctness,
        "reaction_type": reaction_type_for_window(window),
        "target_type": target_type_for_window(window),
        "target_text": target_text_for_window(window),
        "divergence_type": relationship_for_window(window),
        "confidence": confidence,
        "streamer_usefulness": streamer_usefulness(window, suggested),
        "reason": reason_for_window(window, suggested, gold),
        "evidence": evidence_for_window(window),
        "notes": "Agent-assisted heuristic review. Treat as review queue input until human_verified.",
        "created_at": args.generated_at,
    }
    if gold:
        review["reference_event_label"] = gold
    if event_start:
        review["event_start"] = event_start
    if event_peak:
        review["event_peak"] = event_peak
    return {key: value for key, value in review.items() if value not in ("", None, [], {})}


def suggested_event_label(window: EvalWindow) -> str:
    reaction = reaction_type_for_window(window)
    event_hint = str((window.subwindow or window.chat_bucket or {}).get("event_hint") or (window.chat_bucket or {}).get("peak_event_hint") or "")
    label = event_label_for_reaction(reaction, event_hint)
    if label != "none":
        return label

    relationship = relationship_for_window(window)
    delta = abs(number((window.alignment or {}).get("delta")))
    if relationship == "diverged" or delta >= 0.45:
        return "content_audience_divergence"

    chat_score = number((window.chat_bucket or {}).get("chat_sentiment"))
    transcript_score = number((window.transcript_bucket or {}).get("sentiment_score"))
    score = first_number(number_or_none((window.subwindow or {}).get("reaction_score")), average_present(chat_score, transcript_score), chat_score, transcript_score)
    if score >= 0.35:
        return "hype_spike"
    if score <= -0.35:
        return "frustration_spike"
    return "none"


def predicted_event(window: EvalWindow) -> str:
    if window.alignment:
        relationship = relationship_for_window(window)
        delta = abs(number(window.alignment.get("delta")))
        if relationship == "diverged" or delta >= 0.45:
            return "content_audience_divergence"
    return suggested_event_label(window)


def event_label_for_reaction(reaction: str, event_hint: str = "") -> str:
    text = f"{reaction} {event_hint}".lower()
    if any(token in text for token in ("hype", "joy", "celebration", "clutch")):
        return "hype_spike"
    if any(token in text for token in ("frustration", "anger", "negative", "fail")):
        return "frustration_spike"
    if any(token in text for token in ("confusion", "surprise", "unclear", "why")):
        return "audience_shift"
    return "none"


def gold_label_for_window(window: EvalWindow, labels: list[dict[str, Any]]) -> str | None:
    exact = [label for label in labels if same_range(window.window_start, window.window_end, label.get("window_start"), label.get("window_end"))]
    candidates = exact or [
        label for label in labels
        if ranges_overlap(window.window_start, window.window_end, label.get("window_start"), label.get("window_end"))
    ]
    if not candidates:
        return None
    candidates = sorted(candidates, key=lambda item: (label_rank(str(item.get("event_label", "none"))), str(item.get("window_start", ""))))
    return normalize_event_label(candidates[0].get("event_label"))


def label_rank(label: str) -> int:
    return 1 if normalize_event_label(label) == "none" else 0


def correctness_for_suggestion(suggested: str, gold: str | None) -> str:
    if not gold:
        return "uncertain"
    return "correct" if normalize_event_label(suggested) == normalize_event_label(gold) else "wrong"


def confidence_for_window(window: EvalWindow, suggested: str, gold: str | None) -> float:
    values = [
        number_or_none((window.subwindow or {}).get("confidence")),
        number_or_none((window.subwindow or {}).get("reaction_score")),
        number_or_none((window.chat_bucket or {}).get("peak_confidence")),
        number_or_none((window.chat_bucket or {}).get("sentiment_confidence")),
        number_or_none((window.alignment or {}).get("quality")),
        number_or_none((window.transcript_bucket or {}).get("transcript_confidence")),
    ]
    usable = [value for value in values if value is not None and value > 0]
    base = sum(usable) / len(usable) if usable else 0.55
    if gold and normalize_event_label(suggested) == normalize_event_label(gold):
        base = min(0.98, base + 0.08)
    elif gold:
        base = max(0.2, base - 0.15)
    elif suggested == "none":
        base = min(base, 0.72)
    return round(clamp(base), 4)


def streamer_usefulness(window: EvalWindow, suggested: str) -> float:
    if suggested == "none":
        return 0.08
    volume = number((window.chat_bucket or {}).get("message_count"))
    quality = number_or_none((window.alignment or {}).get("quality"))
    reaction = number_or_none((window.subwindow or {}).get("reaction_score")) or number_or_none((window.chat_bucket or {}).get("peak_reaction_score")) or 0.4
    score = 0.25 + min(volume / 100, 0.25) + reaction * 0.4
    if quality is not None:
        score += quality * 0.1
    return round(clamp(score), 4)


def reason_for_window(window: EvalWindow, suggested: str, gold: str | None) -> str:
    parts = []
    if window.chat_bucket:
        parts.append(f"chat sentiment {number(window.chat_bucket.get('chat_sentiment')):.2f} from {int(number(window.chat_bucket.get('message_count')))} messages")
    if window.subwindow:
        parts.append(f"{window.subwindow.get('reaction_type', 'reaction')} subwindow score {number(window.subwindow.get('reaction_score')):.2f}")
    if window.transcript_bucket:
        text = compact_text(transcript_text(window.transcript_bucket), 90)
        if text:
            parts.append(f"transcript: {text}")
    if window.alignment:
        parts.append(f"relationship {relationship_for_window(window)} with delta {number(window.alignment.get('delta')):.2f}")
    verdict = f"suggested {suggested}"
    if gold:
        verdict += f" against gold label {gold}"
    else:
        verdict += " with no human label on this exact window"
    return "; ".join([verdict, *parts])


def evidence_for_window(window: EvalWindow) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    messages = []
    if window.chat_bucket:
        messages.extend(window.chat_bucket.get("message_scores") or [])
        messages.extend(window.chat_bucket.get("peak_evidence_messages") or [])
    for message in messages[:5]:
        if not isinstance(message, dict):
            continue
        evidence.append({
            "id": str(message.get("message_id") or message.get("id") or ""),
            "source": "chat",
            "timestamp": str(message.get("timestamp") or ""),
            "text": compact_text(str(message.get("text") or ""), 180),
            "meta": compact_meta({
                "label": message.get("label"),
                "confidence": message.get("confidence"),
                "sentiment_score": message.get("sentiment_score"),
            }),
        })
    if window.subwindow:
        evidence.append({
            "id": "reaction_subwindow",
            "source": "reaction",
            "timestamp": str(window.subwindow.get("window_start") or window.window_start),
            "text": f"{window.subwindow.get('message_count', 0)} messages; reaction_type={window.subwindow.get('reaction_type', '')}; score={window.subwindow.get('reaction_score', 0)}",
        })
    if window.transcript_bucket:
        text = transcript_text(window.transcript_bucket)
        if text:
            evidence.append({
                "id": f"transcript:{window.transcript_bucket.get('bucket_start', window.window_start)}",
                "source": "transcript",
                "timestamp": str(window.transcript_bucket.get("bucket_start") or window.window_start),
                "text": compact_text(text, 220),
                "meta": compact_meta({
                    "sentiment_score": window.transcript_bucket.get("sentiment_score"),
                    "transcript_confidence": window.transcript_bucket.get("transcript_confidence"),
                }),
            })
    if window.alignment:
        evidence.append({
            "id": f"alignment:{window.alignment.get('window_start', window.window_start)}",
            "source": "alignment",
            "timestamp": str(window.alignment.get("window_start") or window.window_start),
            "text": f"relationship={relationship_for_window(window)}; delta={window.alignment.get('delta', 0)}; quality={window.alignment.get('quality', 0)}",
        })
    return dedupe_evidence([item for item in evidence if item.get("text")])[:8]


def dedupe_evidence(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    out = []
    for item in items:
        key = (
            str(item.get("source", "")),
            str(item.get("id", "")),
            str(item.get("text", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def session_summary(replay: dict[str, Any], windows: list[EvalWindow], reviews: list[dict[str, Any]], labels: list[dict[str, Any]]) -> dict[str, Any]:
    session = replay.get("session") or {}
    return {
        "session_id": session.get("session_id", ""),
        "channel_id": session.get("channel_id", ""),
        "window_count": len(windows),
        "review_count": len(reviews),
        "human_label_count": len(labels),
        "source_counts": dict(sorted(Counter(window.source_window_type for window in windows).items())),
        "suggested_event_counts": dict(sorted(Counter(review.get("suggested_event_label", "none") for review in reviews).items())),
        "correctness_counts": dict(sorted(Counter(review.get("correctness", "uncertain") for review in reviews).items())),
        "metrics": classification_metrics(reviews, labels_present=bool(labels)),
    }


def build_report(args: argparse.Namespace, fixture_path: Path, selected: list[dict[str, Any]], session_summaries: list[dict[str, Any]], reviews: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": "agent_eval_report",
        "generated_at": args.generated_at,
        "fixture": str(fixture_path),
        "reviewer": args.reviewer,
        "model": args.model,
        "prompt_version": args.prompt_version,
        "label_status": "agent_reviewed",
        "human_verified": False,
        "limitations": [
            "Default judge is deterministic and offline; it is review assistance, not human ground truth.",
            "Metrics compare agent suggestions to available window_labels only; unlabeled windows remain uncertain.",
            "Use human verification before claiming final accuracy or benchmark quality.",
        ],
        "selected_session_count": len(selected),
        "review_count": len(reviews),
        "suggested_event_counts": dict(sorted(Counter(review.get("suggested_event_label", "none") for review in reviews).items())),
        "correctness_counts": dict(sorted(Counter(review.get("correctness", "uncertain") for review in reviews).items())),
        "average_confidence": round(sum(number(review.get("confidence")) for review in reviews) / len(reviews), 4) if reviews else None,
        "metrics": classification_metrics(reviews, labels_present=any(summary["human_label_count"] for summary in session_summaries)),
        "sessions": session_summaries,
        "agent_reviews": reviews,
    }


def classification_metrics(reviews: list[dict[str, Any]], labels_present: bool) -> dict[str, Any]:
    evaluated = [review for review in reviews if review.get("reference_event_label")]
    if not labels_present or not evaluated:
        return {
            "status": "pending_human_labels",
            "evaluated_windows": 0,
            "precision": None,
            "recall": None,
            "f1": None,
        }
    true_positive = 0
    false_positive = 0
    false_negative = 0
    for review in evaluated:
        suggested = normalize_event_label(review.get("suggested_event_label"))
        reference = normalize_event_label(review.get("reference_event_label"))
        if suggested == reference and suggested != "none":
            true_positive += 1
            continue
        if suggested != "none":
            false_positive += 1
        if reference != "none":
            false_negative += 1
    precision = safe_div(true_positive, true_positive + false_positive)
    recall = safe_div(true_positive, true_positive + false_negative)
    f1 = safe_div(2 * precision * recall, precision + recall) if precision is not None and recall is not None else None
    return {
        "status": "computed_against_available_window_labels",
        "evaluated_windows": len(evaluated),
        "note": "Precision/recall/F1 treat non-none event labels as detections and count wrong event classes as both false positive and false negative.",
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "precision": round(precision, 4) if precision is not None else None,
        "recall": round(recall, 4) if recall is not None else None,
        "f1": round(f1, 4) if f1 is not None else None,
    }


def render_markdown(report: dict[str, Any]) -> str:
    metrics = report["metrics"]
    lines = [
        "# Agent Evaluation Report",
        "",
        f"Generated at: `{report['generated_at']}`",
        "",
        "Scope: agent-assisted replay review. This is not human-verified ground truth.",
        "",
        "## Summary",
        "",
        f"- Reviews generated: {report['review_count']}",
        f"- Selected sessions: {report['selected_session_count']}",
        f"- Average confidence: {format_optional(report['average_confidence'])}",
        f"- Metric status: `{metrics['status']}`",
        f"- Precision / recall / F1: {format_optional(metrics.get('precision'))} / {format_optional(metrics.get('recall'))} / {format_optional(metrics.get('f1'))}",
        "",
        "## Suggested Events",
        "",
        "| Event label | Count |",
        "| --- | ---: |",
    ]
    for label, count in report["suggested_event_counts"].items():
        lines.append(f"| {label} | {count} |")
    lines.extend([
        "",
        "## Sessions",
        "",
        "| Session | Reviews | Human labels | Correctness | Events |",
        "| --- | ---: | ---: | --- | --- |",
    ])
    for session in report["sessions"]:
        correctness = ", ".join(f"{key}: {value}" for key, value in session["correctness_counts"].items()) or "none"
        events = ", ".join(f"{key}: {value}" for key, value in session["suggested_event_counts"].items()) or "none"
        lines.append(f"| {session['session_id']} | {session['review_count']} | {session['human_label_count']} | {correctness} | {events} |")
    lines.extend([
        "",
        "## Limitations",
        "",
    ])
    lines.extend(f"- {item}" for item in report["limitations"])
    return "\n".join(lines) + "\n"


def write_reviewed_fixture(path: Path, replays: list[dict[str, Any]], reviews: list[dict[str, Any]]) -> None:
    reviews_by_session: dict[str, list[dict[str, Any]]] = {}
    for review in reviews:
        reviews_by_session.setdefault(str(review.get("session_id", "")), []).append(review)
    out = []
    for replay in replays:
        clone = json.loads(json.dumps(replay))
        session_id = str(clone.get("session", {}).get("session_id", ""))
        clone["agent_reviews"] = reviews_by_session.get(session_id, clone.get("agent_reviews") or [])
        out.append(clone)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def review_id(run_id: str, window: EvalWindow) -> str:
    return ":".join([
        run_id,
        safe_id(window.session_id),
        safe_id(window.source_window_type),
        safe_id(window.window_start),
        safe_id(window.window_end),
    ])


def reaction_type_for_window(window: EvalWindow) -> str:
    return str(
        (window.subwindow or {}).get("reaction_type")
        or (window.chat_bucket or {}).get("peak_reaction_type")
        or ""
    ).strip()


def target_type_for_window(window: EvalWindow) -> str:
    return str(
        (window.subwindow or {}).get("target_type")
        or (window.chat_bucket or {}).get("peak_target_type")
        or "unknown"
    ).strip() or "unknown"


def target_text_for_window(window: EvalWindow) -> str:
    return str(
        (window.subwindow or {}).get("target_text")
        or (window.chat_bucket or {}).get("peak_target_text")
        or ""
    ).strip()


def relationship_for_window(window: EvalWindow) -> str:
    return str((window.alignment or {}).get("relationship") or "").strip()


def event_start_for_window(window: EvalWindow) -> str:
    return str(
        (window.subwindow or {}).get("window_start")
        or (window.chat_bucket or {}).get("peak_window_start")
        or window.window_start
    )


def event_peak_for_window(window: EvalWindow) -> str:
    return str((window.chat_bucket or {}).get("peak_time") or event_start_for_window(window))


def best_chat_for_range(start: Any, end: Any, buckets: list[dict[str, Any]]) -> dict[str, Any] | None:
    return best_bucket_for_range(start, end, buckets, "bucket_start", "bucket_end")


def best_transcript_for_range(start: Any, end: Any, buckets: list[dict[str, Any]]) -> dict[str, Any] | None:
    return best_bucket_for_range(start, end, buckets, "bucket_start", "bucket_end")


def best_alignment_for_range(start: Any, end: Any, alignments: list[dict[str, Any]]) -> dict[str, Any] | None:
    return best_bucket_for_range(start, end, alignments, "window_start", "window_end")


def best_bucket_for_range(start: Any, end: Any, buckets: list[dict[str, Any]], start_key: str, end_key: str) -> dict[str, Any] | None:
    best = None
    best_overlap = 0
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        overlap = overlap_seconds(start, end, bucket.get(start_key), bucket.get(end_key))
        if overlap > best_overlap:
            best = bucket
            best_overlap = overlap
    return best


def same_range(left_start: Any, left_end: Any, right_start: Any, right_end: Any) -> bool:
    return normalize_time(left_start) == normalize_time(right_start) and normalize_time(left_end) == normalize_time(right_end)


def ranges_overlap(left_start: Any, left_end: Any, right_start: Any, right_end: Any) -> bool:
    return overlap_seconds(left_start, left_end, right_start, right_end) > 0


def overlap_seconds(left_start: Any, left_end: Any, right_start: Any, right_end: Any) -> float:
    a_start = timestamp_seconds(left_start)
    a_end = timestamp_seconds(left_end)
    b_start = timestamp_seconds(right_start)
    b_end = timestamp_seconds(right_end)
    if None in (a_start, a_end, b_start, b_end):
        return 0
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


def timestamp_seconds(value: Any) -> float | None:
    text = normalize_time(value)
    if not text:
        return None
    from datetime import datetime

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def normalize_time(value: Any) -> str:
    return str(value or "").strip()


def transcript_text(bucket: dict[str, Any]) -> str:
    text = str(bucket.get("text") or "").strip()
    if text:
        return text
    return " ".join(str(segment.get("text") or "").strip() for segment in bucket.get("segments") or [] if isinstance(segment, dict)).strip()


def normalize_event_label(value: Any) -> str:
    text = str(value or "none").strip().lower().replace("-", "_").replace(" ", "_")
    return text if text in EVENT_LABELS else "none"


def compact_text(text: str, limit: int) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)].rstrip() + "..."


def compact_meta(values: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value not in (None, "", [], {})}


def safe_id(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", str(value or "").strip()).strip("-") or "unknown"


def number(value: Any) -> float:
    parsed = number_or_none(value)
    return parsed if parsed is not None else 0.0


def number_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def first_number(*values: float | None) -> float:
    for value in values:
        if value is not None:
            return value
    return 0.0


def average_present(*values: float | None) -> float | None:
    usable = [value for value in values if value is not None]
    if not usable:
        return None
    return sum(usable) / len(usable)


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def safe_div(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def format_optional(value: Any) -> str:
    if value is None:
        return "pending"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


if __name__ == "__main__":
    main()
