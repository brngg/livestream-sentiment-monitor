FROM debian:bookworm-slim AS service-builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt ./
COPY src ./src
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --config Release -j"$(nproc)"

FROM debian:bookworm-slim AS runtime

ARG STREAMLINK_VERSION=8.4.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg curl python3-pip \
    && python3 -m pip install --break-system-packages --no-cache-dir "nvidia-riva-client==2.25.1" \
    && python3 -m pip install --break-system-packages --no-cache-dir "streamlink==${STREAMLINK_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=service-builder /src/build/transcript-ingestor /app/transcript-ingestor
COPY scripts /app/scripts

ENV TRANSCRIPT_HOST=0.0.0.0 \
    TRANSCRIPT_PORT=8092 \
    TRANSCRIPT_DEFAULT_CHUNK_SECONDS=1 \
    TRANSCRIPT_ASR_PROVIDER=nvidia_streaming \
    TRANSCRIPT_ASR_QUEUE_MAX_DEPTH=4 \
    TRANSCRIPT_REPAIR_ENABLED=false \
    CAPTION_ROLLING_WINDOW_SECONDS=10 \
    CAPTION_ASR_INTERVAL_SECONDS=1 \
    NVIDIA_NIM_ASR_PYTHON=python3 \
    NVIDIA_NIM_ASR_HELPER=/app/scripts/nvidia_asr_transcribe.py \
    NVIDIA_NIM_ASR_STREAMING_HELPER=/app/scripts/nvidia_live_stream_asr.py \
    NVIDIA_NIM_ASR_SERVER=grpc.nvcf.nvidia.com:443 \
    NVIDIA_NIM_ASR_FUNCTION_ID=bb0837de-8c7b-481f-9ec8-ef5663e9c1fa \
    NVIDIA_NIM_ASR_LANGUAGE_CODE=en-US \
    NVIDIA_NIM_ASR_FILE_STREAMING_CHUNK=1600 \
    NLP_ANALYZER_TIMEOUT=15

EXPOSE 8092
CMD ["/bin/sh", "-c", "export TRANSCRIPT_PORT=${PORT:-8092}; if [ -z \"${NLP_ANALYZER_URL:-}\" ] && [ -n \"${NLP_ANALYZER_HOSTPORT:-}\" ]; then export NLP_ANALYZER_URL=\"http://${NLP_ANALYZER_HOSTPORT}\"; fi; exec /app/transcript-ingestor"]
