#!/usr/bin/env python3
"""Continuously transcribe Twitch audio with NVIDIA hosted ASR NIM/Riva.

This helper owns the live media pipe:

    streamlink -> ffmpeg 16kHz mono pcm16 -> NVIDIA StreamingRecognize

It writes newline-delimited JSON transcript events to stdout so the C++
transcript service can keep its existing HTTP/SSE/bucket contract.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


DEFAULT_SERVER = "grpc.nvcf.nvidia.com:443"
DEFAULT_FUNCTION_ID = "bb0837de-8c7b-481f-9ec8-ef5663e9c1fa"
SAMPLE_RATE_HZ = 16_000
BYTES_PER_SAMPLE = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Twitch audio to NVIDIA hosted ASR NIM.")
    parser.add_argument("--twitch-url", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--channel-id", required=True)
    parser.add_argument("--server", default=os.getenv("NVIDIA_NIM_ASR_SERVER", DEFAULT_SERVER))
    parser.add_argument("--function-id", default=os.getenv("NVIDIA_NIM_ASR_FUNCTION_ID", DEFAULT_FUNCTION_ID))
    parser.add_argument("--api-key", default=os.getenv("NVIDIA_API_KEY", ""))
    parser.add_argument("--language-code", default=os.getenv("NVIDIA_NIM_ASR_LANGUAGE_CODE", "en-US"))
    parser.add_argument("--model-name", default=os.getenv("NVIDIA_NIM_ASR_MODEL_NAME", ""))
    parser.add_argument(
        "--file-streaming-chunk",
        type=int,
        default=int(os.getenv("NVIDIA_NIM_ASR_FILE_STREAMING_CHUNK", "1600")),
        help="Frames per gRPC streaming request. 1600 frames is 100ms at 16kHz.",
    )
    parser.add_argument("--automatic-punctuation", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def emit(event: dict) -> None:
    print(json.dumps(event, separators=(",", ":")), flush=True)


def fail(message: str, code: int = 2) -> None:
    emit({"type": "error", "error": message})
    raise SystemExit(code)


class AudioPipe:
    def __init__(self, twitch_url: str, frames_per_chunk: int) -> None:
        self.twitch_url = twitch_url
        self.frames_per_chunk = frames_per_chunk
        self.bytes_per_chunk = frames_per_chunk * BYTES_PER_SAMPLE
        self.stop = threading.Event()
        self.audio_seconds_sent = 0.0
        self.first_audio_wall_time: float | None = None
        self._lock = threading.Lock()
        self._streamlink: subprocess.Popen[bytes] | None = None
        self._ffmpeg: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        self._streamlink = subprocess.Popen(
            ["streamlink", "--stdout", self.twitch_url, "audio_only,best"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        if self._streamlink.stdout is None:
            fail("streamlink did not expose stdout")

        self._ffmpeg = subprocess.Popen(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                "pipe:0",
                "-vn",
                "-ar",
                str(SAMPLE_RATE_HZ),
                "-ac",
                "1",
                "-f",
                "s16le",
                "pipe:1",
            ],
            stdin=self._streamlink.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self._streamlink.stdout.close()
        if self._ffmpeg.stdout is None:
            fail("ffmpeg did not expose stdout")

    def close(self) -> None:
        self.stop.set()
        for proc in (self._ffmpeg, self._streamlink):
            if proc is None or proc.poll() is not None:
                continue
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.time() + 2
        for proc in (self._ffmpeg, self._streamlink):
            if proc is None:
                continue
            while proc.poll() is None and time.time() < deadline:
                time.sleep(0.05)
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def chunks(self) -> Iterator[bytes]:
        if self._ffmpeg is None or self._ffmpeg.stdout is None:
            fail("audio pipe has not started")
        while not self.stop.is_set():
            chunk = self._ffmpeg.stdout.read(self.bytes_per_chunk)
            if not chunk:
                break
            with self._lock:
                if self.first_audio_wall_time is None:
                    self.first_audio_wall_time = time.time()
                self.audio_seconds_sent += len(chunk) / float(SAMPLE_RATE_HZ * BYTES_PER_SAMPLE)
            yield chunk

    def sent_seconds(self) -> float:
        with self._lock:
            return self.audio_seconds_sent


def main() -> None:
    args = parse_args()
    if not args.api_key:
        fail("NVIDIA_API_KEY is required for hosted NVIDIA ASR NIM")
    if not args.function_id:
        fail("NVIDIA_NIM_ASR_FUNCTION_ID is required")

    try:
        import riva.client
    except ModuleNotFoundError:
        fail("missing dependency: pip install nvidia-riva-client")

    auth = riva.client.Auth(
        use_ssl=True,
        uri=args.server,
        metadata_args=[
            ["function-id", args.function_id],
            ["authorization", f"Bearer {args.api_key}"],
        ],
    )
    asr_service = riva.client.ASRService(auth)
    config = riva.client.StreamingRecognitionConfig(
        config=riva.client.RecognitionConfig(
            encoding=riva.client.AudioEncoding.LINEAR_PCM,
            sample_rate_hertz=SAMPLE_RATE_HZ,
            audio_channel_count=1,
            language_code=args.language_code,
            model=args.model_name,
            max_alternatives=1,
            enable_automatic_punctuation=args.automatic_punctuation,
            verbatim_transcripts=True,
        ),
        interim_results=True,
    )

    audio_pipe = AudioPipe(args.twitch_url, args.file_streaming_chunk)
    started = time.time()
    last_final_audio_end = 0.0
    latest_partial = ""
    try:
        audio_pipe.start()
        emit(
            {
                "type": "status",
                "status": "streaming",
                "session_id": args.session_id,
                "channel_id": args.channel_id,
                "sample_rate_hz": SAMPLE_RATE_HZ,
                "frames_per_chunk": args.file_streaming_chunk,
                "started_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
        )
        responses = asr_service.streaming_response_generator(
            audio_chunks=audio_pipe.chunks(),
            streaming_config=config,
        )
        for response in responses:
            for result in response.results:
                if not result.alternatives:
                    continue
                transcript = result.alternatives[0].transcript.strip()
                if not transcript:
                    continue
                audio_end = max(audio_pipe.sent_seconds(), last_final_audio_end)
                confidence = float(result.alternatives[0].confidence or 0.86)
                if result.is_final:
                    audio_start = min(last_final_audio_end, audio_end)
                    if audio_end <= audio_start:
                        audio_end = audio_start + max(0.1, len(transcript.split()) * 0.35)
                    last_final_audio_end = audio_end
                    emit(
                        {
                            "type": "transcript_segment",
                            "session_id": args.session_id,
                            "channel_id": args.channel_id,
                            "text": transcript,
                            "audio_start_seconds": round(audio_start, 3),
                            "audio_end_seconds": round(audio_end, 3),
                            "confidence": confidence,
                            "asr_latency_ms": int((time.time() - started) * 1000),
                            "source": "nvidia-nim-streaming",
                        }
                    )
                    latest_partial = ""
                elif transcript != latest_partial:
                    latest_partial = transcript
                    emit(
                        {
                            "type": "transcript_partial",
                            "session_id": args.session_id,
                            "channel_id": args.channel_id,
                            "text": transcript,
                            "audio_end_seconds": round(audio_end, 3),
                            "confidence": confidence,
                            "source": "nvidia-nim-streaming",
                        }
                    )
    except KeyboardInterrupt:
        pass
    except Exception as exc:  # noqa: BLE001 - surface as JSON for the parent service.
        fail(f"nvidia streaming asr failed: {exc}", code=3)
    finally:
        audio_pipe.close()


if __name__ == "__main__":
    main()
