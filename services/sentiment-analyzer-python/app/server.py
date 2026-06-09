from __future__ import annotations

import argparse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import time
from typing import Any

from app.analyzer import analyze_bucket, get_analyzer


MAX_BODY_BYTES = 10 * 1024 * 1024


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests_total = 0
        self.analyze_requests_total = 0
        self.analyze_errors_total = 0
        self.analyze_latency_ms_sum = 0
        self.analyze_latency_ms_count = 0

    def record_request(self) -> None:
        with self._lock:
            self.requests_total += 1

    def record_analyze(self, latency_ms: int, error: bool = False) -> None:
        with self._lock:
            self.analyze_requests_total += 1
            self.analyze_latency_ms_sum += latency_ms
            self.analyze_latency_ms_count += 1
            if error:
                self.analyze_errors_total += 1

    def render(self) -> str:
        with self._lock:
            values = {
                "sentiment_analyzer_requests_total": self.requests_total,
                "sentiment_analyzer_analyze_requests_total": self.analyze_requests_total,
                "sentiment_analyzer_analyze_errors_total": self.analyze_errors_total,
                "sentiment_analyzer_analyze_latency_ms_sum": self.analyze_latency_ms_sum,
                "sentiment_analyzer_analyze_latency_ms_count": self.analyze_latency_ms_count,
            }
        lines = [
            "# HELP sentiment_analyzer_requests_total Total HTTP requests handled by the sentiment analyzer.",
            "# TYPE sentiment_analyzer_requests_total counter",
            f"sentiment_analyzer_requests_total {values['sentiment_analyzer_requests_total']}",
            "# HELP sentiment_analyzer_analyze_requests_total Chat bucket analysis requests handled.",
            "# TYPE sentiment_analyzer_analyze_requests_total counter",
            f"sentiment_analyzer_analyze_requests_total {values['sentiment_analyzer_analyze_requests_total']}",
            "# HELP sentiment_analyzer_analyze_errors_total Chat bucket analysis requests that returned an error.",
            "# TYPE sentiment_analyzer_analyze_errors_total counter",
            f"sentiment_analyzer_analyze_errors_total {values['sentiment_analyzer_analyze_errors_total']}",
            "# HELP sentiment_analyzer_analyze_latency_ms Analysis latency in milliseconds.",
            "# TYPE sentiment_analyzer_analyze_latency_ms summary",
            f"sentiment_analyzer_analyze_latency_ms_sum {values['sentiment_analyzer_analyze_latency_ms_sum']}",
            f"sentiment_analyzer_analyze_latency_ms_count {values['sentiment_analyzer_analyze_latency_ms_count']}",
        ]
        return "\n".join(lines) + "\n"


METRICS = Metrics()


class Handler(BaseHTTPRequestHandler):
    server_version = "SentimentAnalyzer/0.1"

    def do_GET(self) -> None:
        METRICS.record_request()
        if self.path == "/metrics":
            self.write_text(HTTPStatus.OK, METRICS.render(), "text/plain; version=0.0.4; charset=utf-8")
            return
        if self.path == "/health":
            analyzer = get_analyzer()
            self.write_json(
                HTTPStatus.OK,
                {
                    "status": "ok",
                    "model": analyzer.model_name,
                    "backend": analyzer.backend,
                    "batch_size": analyzer.batch_size,
                    "max_messages": analyzer.max_messages,
                    "max_text_chars": analyzer.max_text_chars,
                },
            )
            return
        self.write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:
        METRICS.record_request()
        if self.path != "/analyze/chat-bucket":
            self.write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return

        started = time.perf_counter()
        try:
            payload = self.read_json()
            messages = payload.get("messages")
            if not isinstance(messages, list):
                self.write_json(HTTPStatus.BAD_REQUEST, {"error": "messages must be an array"})
                return
            if not all(isinstance(message, dict) for message in messages):
                self.write_json(HTTPStatus.BAD_REQUEST, {"error": "messages must contain only objects"})
                return

            result = analyze_bucket(
                messages,
                peak_window_start=payload.get("peak_window_start"),
                peak_window_end=payload.get("peak_window_end"),
            )
            result.update(
                {
                    "session_id": payload.get("session_id", ""),
                    "channel_id": payload.get("channel_id", ""),
                    "bucket_start": payload.get("bucket_start", ""),
                    "bucket_end": payload.get("bucket_end", ""),
                    "latency_ms": int((time.perf_counter() - started) * 1000),
                }
            )
            METRICS.record_analyze(result["latency_ms"])
            self.write_json(HTTPStatus.OK, result)
        except ValueError as exc:
            METRICS.record_analyze(int((time.perf_counter() - started) * 1000), error=True)
            self.write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except RuntimeError as exc:
            METRICS.record_analyze(int((time.perf_counter() - started) * 1000), error=True)
            self.write_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": str(exc)})

    def read_json(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0:
            raise ValueError("request body is required")
        if content_length > MAX_BODY_BYTES:
            raise ValueError("request body is too large")

        raw = self.rfile.read(content_length)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("invalid JSON body") from exc
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        return value

    def write_json(self, status: HTTPStatus, value: dict[str, Any]) -> None:
        body = json.dumps(value, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def write_text(self, status: HTTPStatus, value: str, content_type: str) -> None:
        body = value.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    args = parser.parse_args()

    if env_bool("SENTIMENT_WARMUP", True):
        started = time.perf_counter()
        analyzer = get_analyzer()
        analyzer.warmup()
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        print(f"sentiment analyzer warmup completed in {elapsed_ms}ms", flush=True)

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"sentiment analyzer listening on http://{args.host}:{args.port}", flush=True)
    httpd.serve_forever()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    main()
