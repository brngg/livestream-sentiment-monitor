#!/usr/bin/env python3
"""Generate deterministic seed evaluation reports from JSONL manifests.

The script intentionally treats the checked-in manifests as seed evaluation
data. It reports counts, label distribution, and missing model-output slots
without inventing accuracy, WER, or scale claims when predictions are absent.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


MANIFESTS = (
    ("sentiment_chat_labels.jsonl", "chat_sentiment"),
    ("signal_window_labels.jsonl", "signal_window_event"),
    ("asr_manifest.jsonl", "asr_transcription"),
)

EXPECTED_OUTPUT_FIELDS = {
    "chat_sentiment": ["model_output.label", "model_output.score", "model_output.confidence"],
    "signal_window_event": ["model_output.event_label", "model_output.relationship", "model_output.confidence"],
    "asr_transcription": ["model_output.text", "model_output.confidence", "model_output.asr_latency_ms"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate seed evaluation Markdown and JSON reports.")
    parser.add_argument("--manifest-dir", default="eval", help="Directory containing seed JSONL manifests.")
    parser.add_argument("--out-dir", default="eval/reports", help="Directory to write report files.")
    parser.add_argument(
        "--generated-at",
        default="1970-01-01T00:00:00Z",
        help="Deterministic report timestamp. Pass an explicit value for checked-in reports.",
    )
    parser.add_argument("--json-name", default="seed-evaluation-report.json", help="JSON report filename.")
    parser.add_argument("--markdown-name", default="seed-evaluation-report.md", help="Markdown report filename. Pass an empty value to skip Markdown output.")
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number}: expected object, got {type(value).__name__}")
            records.append(value)
    return records


def model_output(record: dict[str, Any], task: str) -> dict[str, Any] | None:
    output = record.get("model_output")
    if isinstance(output, dict) and any(value not in (None, "", []) for value in output.values()):
        return output
    if task == "signal_window_event" and record.get("predicted_event"):
        return {
            "event_label": record.get("predicted_event"),
            "relationship": record.get("predicted_relationship"),
        }
    return None


def gold_label(record: dict[str, Any], task: str) -> str | None:
    if task == "chat_sentiment":
        return string_or_none(record.get("gold_label"))
    if task == "signal_window_event":
        return string_or_none(record.get("event_label"))
    if task == "asr_transcription":
        return string_or_none(record.get("language"))
    return None


def predicted_label(output: dict[str, Any] | None, task: str) -> str | None:
    if not output:
        return None
    if task == "chat_sentiment":
        return string_or_none(output.get("label") or output.get("sentiment_label"))
    if task == "signal_window_event":
        return string_or_none(output.get("event_label") or output.get("predicted_event"))
    return None


def output_text(output: dict[str, Any] | None) -> str | None:
    if not output:
        return None
    return string_or_none(output.get("text") or output.get("transcript") or output.get("hypothesis"))


def string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def summarize_manifest(name: str, task: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    labels = Counter(label for record in records if (label := gold_label(record, task)))
    outputs = [model_output(record, task) for record in records]
    present_outputs = sum(1 for output in outputs if output)
    missing_outputs = len(records) - present_outputs
    missing_examples = [
        record.get("example_id") or record.get("message_id") or f"{name}:{index + 1}"
        for index, (record, output) in enumerate(zip(records, outputs))
        if not output
    ]

    summary: dict[str, Any] = {
        "manifest": name,
        "task": task,
        "records": len(records),
        "label_counts": dict(sorted(labels.items())),
        "model_outputs_present": present_outputs,
        "model_outputs_missing": missing_outputs,
        "missing_model_output_examples": missing_examples[:10],
        "expected_model_output_fields": EXPECTED_OUTPUT_FIELDS[task],
        "metrics": metric_summary(task, records, outputs),
    }

    if task == "asr_transcription":
        audio_available = sum(1 for record in records if bool(record.get("audio_available")))
        reference_text_count = sum(1 for record in records if string_or_none(record.get("reference_text")))
        summary["audio_available"] = audio_available
        summary["audio_missing"] = len(records) - audio_available
        summary["reference_text_count"] = reference_text_count

    return summary


def metric_summary(task: str, records: list[dict[str, Any]], outputs: list[dict[str, Any] | None]) -> dict[str, Any]:
    if not records:
        return {"status": "empty_manifest"}

    if task in {"chat_sentiment", "signal_window_event"}:
        evaluated = 0
        correct = 0
        confusion: Counter[tuple[str, str]] = Counter()
        for record, output in zip(records, outputs):
            actual = gold_label(record, task)
            predicted = predicted_label(output, task)
            if not actual or not predicted:
                continue
            evaluated += 1
            if actual == predicted:
                correct += 1
            confusion[(actual, predicted)] += 1
        if evaluated == 0:
            return {
                "status": "pending_model_outputs",
                "accuracy": None,
                "evaluated_records": 0,
                "unsupported_reason": "No model_output label fields are present in the seed manifest.",
            }
        return {
            "status": "computed",
            "accuracy": round(correct / evaluated, 4),
            "evaluated_records": evaluated,
            "confusion_counts": [
                {"actual": actual, "predicted": predicted, "count": count}
                for (actual, predicted), count in sorted(confusion.items())
            ],
        }

    if task == "asr_transcription":
        wers: list[float] = []
        evaluated = 0
        for record, output in zip(records, outputs):
            reference = string_or_none(record.get("reference_text"))
            hypothesis = output_text(output)
            if not reference or not hypothesis:
                continue
            evaluated += 1
            wers.append(word_error_rate(reference, hypothesis))
        if evaluated == 0:
            return {
                "status": "pending_audio_and_model_outputs",
                "wer": None,
                "evaluated_records": 0,
                "unsupported_reason": "ASR WER requires reference_text and model_output.text; bundled seed rows do not include audio.",
            }
        return {
            "status": "computed",
            "wer": round(sum(wers) / len(wers), 4),
            "evaluated_records": evaluated,
        }

    return {"status": "unsupported_task"}


def word_error_rate(reference: str, hypothesis: str) -> float:
    ref_words = normalize_words(reference)
    hyp_words = normalize_words(hypothesis)
    if not ref_words:
        return 0.0 if not hyp_words else 1.0
    distance = levenshtein_distance(ref_words, hyp_words)
    return distance / len(ref_words)


def normalize_words(text: str) -> list[str]:
    return re.findall(r"[a-z0-9']+", text.lower())


def levenshtein_distance(left: list[str], right: list[str]) -> int:
    previous = list(range(len(right) + 1))
    for left_index, left_word in enumerate(left, start=1):
        current = [left_index]
        for right_index, right_word in enumerate(right, start=1):
            insert_cost = current[right_index - 1] + 1
            delete_cost = previous[right_index] + 1
            substitute_cost = previous[right_index - 1] + (0 if left_word == right_word else 1)
            current.append(min(insert_cost, delete_cost, substitute_cost))
        previous = current
    return previous[-1]


def build_report(manifest_dir: Path, generated_at: str) -> dict[str, Any]:
    manifest_summaries = []
    for filename, task in MANIFESTS:
        path = manifest_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing manifest: {path}")
        records = load_jsonl(path)
        manifest_summaries.append(summarize_manifest(filename, task, records))

    total_records = sum(item["records"] for item in manifest_summaries)
    total_outputs = sum(item["model_outputs_present"] for item in manifest_summaries)
    total_missing = sum(item["model_outputs_missing"] for item in manifest_summaries)
    return {
        "type": "seed_evaluation_report",
        "generated_at": generated_at,
        "scope": "seed manifests only; no production accuracy, WER, or scale claim",
        "manifest_dir": manifest_dir.as_posix(),
        "manifest_count": len(manifest_summaries),
        "total_records": total_records,
        "model_outputs_present": total_outputs,
        "model_outputs_missing": total_missing,
        "manifests": manifest_summaries,
        "limitations": [
            "Seed rows are intentionally small and are not a statistically meaningful evaluation set.",
            "Metrics remain pending for manifests without model_output fields.",
            "ASR WER remains pending until audio_uri values and ASR hypotheses are supplied.",
        ],
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Seed Evaluation Report",
        "",
        f"Generated at: `{report['generated_at']}`",
        "",
        "Scope: seed manifests only. This report does not claim production accuracy, WER, or 500-label coverage.",
        "",
        "## Summary",
        "",
        "| Manifest | Task | Records | Labels / references | Model outputs | Metric status |",
        "| --- | --- | ---: | --- | ---: | --- |",
    ]
    for item in report["manifests"]:
        labels = ", ".join(f"{label}: {count}" for label, count in item["label_counts"].items()) or "n/a"
        if item["task"] == "asr_transcription":
            labels = f"reference_text: {item['reference_text_count']}; audio_available: {item['audio_available']}"
        lines.append(
            "| {manifest} | {task} | {records} | {labels} | {present}/{records} | {status} |".format(
                manifest=item["manifest"],
                task=item["task"],
                records=item["records"],
                labels=labels,
                present=item["model_outputs_present"],
                status=item["metrics"]["status"],
            )
        )

    lines.extend([
        "",
        "## Model Output Placeholders",
        "",
        "| Manifest | Missing outputs | Expected fields | Example IDs needing output |",
        "| --- | ---: | --- | --- |",
    ])
    for item in report["manifests"]:
        examples = ", ".join(item["missing_model_output_examples"]) or "none"
        fields = ", ".join(item["expected_model_output_fields"])
        lines.append(f"| {item['manifest']} | {item['model_outputs_missing']} | `{fields}` | {examples} |")

    lines.extend([
        "",
        "## Limitations",
        "",
    ])
    for limitation in report["limitations"]:
        lines.append(f"- {limitation}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    manifest_dir = Path(args.manifest_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    report = build_report(manifest_dir, args.generated_at)
    json_path = out_dir / args.json_name
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {json_path}")
    if args.markdown_name.strip():
        markdown_path = out_dir / args.markdown_name
        markdown_path.write_text(render_markdown(report), encoding="utf-8")
        print(f"Wrote {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
