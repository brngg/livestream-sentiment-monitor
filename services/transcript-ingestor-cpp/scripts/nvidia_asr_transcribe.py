#!/usr/bin/env python3
"""Transcribe a WAV file with NVIDIA hosted ASR NIM/Riva gRPC.

The C++ transcript service shells out to this helper for chunked NVIDIA ASR.
Keep stdout machine-readable for the caller: final transcript text only, with
diagnostics on stderr.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


DEFAULT_SERVER = "grpc.nvcf.nvidia.com:443"
DEFAULT_FUNCTION_ID = "bb0837de-8c7b-481f-9ec8-ef5663e9c1fa"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transcribe a PCM WAV through NVIDIA hosted ASR NIM.")
    parser.add_argument("--input-file", required=True, help="Path to a WAV file to stream to ASR.")
    parser.add_argument("--server", default=os.getenv("NVIDIA_NIM_ASR_SERVER", DEFAULT_SERVER))
    parser.add_argument("--function-id", default=os.getenv("NVIDIA_NIM_ASR_FUNCTION_ID", DEFAULT_FUNCTION_ID))
    parser.add_argument("--api-key", default=os.getenv("NVIDIA_API_KEY", ""))
    parser.add_argument("--language-code", default=os.getenv("NVIDIA_NIM_ASR_LANGUAGE_CODE", "en-US"))
    parser.add_argument("--model-name", default=os.getenv("NVIDIA_NIM_ASR_MODEL_NAME", ""))
    parser.add_argument(
        "--file-streaming-chunk",
        type=int,
        default=int(os.getenv("NVIDIA_NIM_ASR_FILE_STREAMING_CHUNK", "1600")),
        help="Frames per streaming request. 1600 frames is 100ms at 16kHz.",
    )
    parser.add_argument("--automatic-punctuation", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def fail(message: str, code: int = 2) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def main() -> None:
    args = parse_args()
    wav_path = Path(args.input_file)
    if not wav_path.is_file():
        fail(f"input file not found: {wav_path}")
    if not args.api_key:
        fail("NVIDIA_API_KEY is required for hosted NVIDIA ASR NIM")
    if not args.function_id:
        fail("NVIDIA_NIM_ASR_FUNCTION_ID is required")

    try:
        import riva.client
    except ModuleNotFoundError as exc:
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
            language_code=args.language_code,
            model=args.model_name,
            max_alternatives=1,
            enable_automatic_punctuation=args.automatic_punctuation,
            verbatim_transcripts=True,
        ),
        interim_results=True,
    )

    final_parts: list[str] = []
    latest_partial = ""
    try:
        with riva.client.AudioChunkFileIterator(str(wav_path), args.file_streaming_chunk) as audio_chunks:
            for response in asr_service.streaming_response_generator(
                audio_chunks=audio_chunks,
                streaming_config=config,
            ):
                for result in response.results:
                    if not result.alternatives:
                        continue
                    transcript = result.alternatives[0].transcript.strip()
                    if not transcript:
                        continue
                    if result.is_final:
                        final_parts.append(transcript)
                    else:
                        latest_partial = transcript
    except Exception as exc:  # noqa: BLE001 - convert API errors to a clean process failure.
        fail(f"nvidia asr request failed: {exc}", code=3)

    text = " ".join(part for part in final_parts if part).strip()
    if not text:
        text = latest_partial.strip()
    print(text)


if __name__ == "__main__":
    main()
