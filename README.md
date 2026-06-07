# Live Stream Sentiment Monitor

A full-stack livestream project that tracks audience sentiment in chat, aligns
it with live speech transcripts, and saves replayable evidence for later
review.

The project is built around a practical question: during a live stream, which
moments coincided with audience reactions, and what evidence supports those
signals?

## The Problem

Livestream feedback is fast, noisy, and easy to lose. A chat spike may happen
while the streamer is saying something important, but the raw chat log and the
spoken content usually live in separate places. After the stream ends, it is
hard to answer basic questions:

- When did audience sentiment shift?
- What was being said when the reaction happened?
- Was the reaction supported by enough chat evidence?
- Can the moment be replayed and reviewed later?

The core challenge is connecting live audience reaction with spoken stream
content without overstating what the model knows. Sentiment alone is not ground
truth, so the system needs evidence, replay, and evaluation hooks.

## The Solution

Live Stream Sentiment Monitor captures live chat and transcript windows,
scores sentiment, aligns the two timelines, and stores candidate reaction
signals for review.

It focuses on inspectable output instead of black-box conclusions. Each signal
window keeps timing, sentiment, chat samples, transcript context, and proof
metrics so a reviewer can decide whether the signal is meaningful.

The public demo path is a controlled live dashboard: visitors can start a
short Twitch livestream session, watch chat sentiment and speech transcript
signals update in real time, and inspect the aligned evidence while the
session is active.

## Core Features

### Live Stream Input

- Twitch live-channel verification through Twitch Helix.
- Twitch IRC chat ingestion through Go.
- Transcript capture for Twitch audio using Streamlink, ffmpeg, and hosted
  NVIDIA ASR NIM.
- Transcript-only YouTube live URL support when chat is unavailable.

### Sentiment And Signal Detection

- Chat buckets with message counts, unique chatters, top terms, emotes,
  language mix, sentiment, and sampled evidence.
- Transcript buckets with sentiment scoring.
- Chat/transcript alignment by timestamp overlap.
- Signal windows for hype spikes, frustration spikes, audience shifts, and
  content/audience divergence candidates.

### Replay And Review

- Stored session history for replay after a stream.
- Proof endpoints for checking whether a signal has enough supporting data.
- Human label endpoints for reviewing signal windows.
- Seed reports and regression checks for validating the evaluation workflow.

### Demo Safety

- Admin-token controls.
- Max active session limits.
- Session duration limits.
- Daily live-start limits.
- Source allowlists for controlled live demos.

## Technology Overview

- **Frontend:** Vite, React, TypeScript, Tailwind CSS.
- **Go services:** control plane, Twitch chat ingestion, bucketization, event
  gateway, analysis service, replay regression CLI, signal correlation CLI,
  storage, and tests.
- **Python service:** multilingual sentiment scoring with
  `cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual`.
- **C++ service:** native transcript ingestion for live audio capture and
  hosted NVIDIA ASR.
- **Event bus:** Redpanda / Kafka-compatible topics.
- **Persistence:** optional Postgres / Neon storage for sessions, buckets,
  alignments, labels, evidence, replay, and evaluation data.
- **Ops:** Docker Compose, nginx, Prometheus, Grafana, and blackbox probes.

## Architecture

```text
Twitch channel or YouTube livestream URL
  -> Go control plane
       -> Twitch live verification
       -> Twitch chat ingestion
       -> chat bucket scoring
       -> transcript proxy/upsert
       -> React web UI
       -> replay/evaluation/proof APIs
       -> optional Postgres persistence
       -> event publishing

Twitch audio
  -> C++ transcript ingestor
       -> Streamlink + ffmpeg
       -> hosted NVIDIA ASR NIM
       -> transcript buckets
       -> transcript sentiment scoring

chat buckets + transcript buckets
  -> Redpanda / Kafka-compatible event bus
  -> Go analysis service
       -> event-driven joins
       -> alignment buckets
       -> SignalWindow records
       -> analysis_result events
       -> SSE/API updates

Postgres / Neon
  -> sessions
  -> chat buckets
  -> transcript buckets
  -> alignments
  -> labels
  -> sampled evidence
  -> replay/proof/evaluation
```

## Quick Start: Offline Replay Demo

This path runs without Twitch, NVIDIA, or Postgres.

```bash
cd apps/dashboard
npm install
npm run build
```

```bash
cd ../../services/chat-ingestor-go
go run ./cmd/chat-dashboard \
  --database-write-enabled=false \
  --replay-fixture testdata/golden-replay/sessions.json \
  --nlp-analyzer-url= \
  --transcript-url= \
  --event-bus-enabled=false \
  --analysis-service-required=false
```

Open:

```text
http://localhost:8090
```

Use `/eval` to inspect stored replay sessions, label coverage, proof metrics,
and signal-window evidence.

## Full Local Stack

Copy the local environment template:

```bash
cp .env.example .env
```

Set at least:

```env
TWITCH_CLIENT_ID=your_client_id
TWITCH_CLIENT_SECRET=your_client_secret
TWITCH_CHANNEL=example_channel_for_smoke_tests
NVIDIA_API_KEY=your_build_nvidia_com_api_key
DATABASE_URL=postgresql://user:password@ep-example.us-east-1.aws.neon.tech/neondb?sslmode=require
DATABASE_WRITE_ENABLED=true
CHAT_IDENTITY_HASH_SALT=replace-with-a-stable-random-secret
```

Start:

```bash
docker compose up --build
```

Open:

```text
http://localhost:8090
```

Verify:

```bash
docker compose ps
curl -fsS http://localhost:8090/health
curl -fsS http://localhost:8091/health
curl -fsS http://localhost:8092/health
curl -fsS http://localhost:8090/sessions/history
```

## Public Demo

The public demo is a live frontend experience for trying the monitor against a
Twitch livestream. A visitor enters a Twitch channel or livestream URL, starts
a short session, and sees chat sentiment, transcript snippets, timeline
charts, and chat/transcript alignment update while the stream is running.

The hosted demo is intentionally lightweight. It does not need database-backed
storage, Eval Lab, replay history, Redpanda/Kafka, or production observability.
Session state can stay in memory for the current run, with live starts capped
by active-session, duration, and daily limits. Twitch and NVIDIA credentials
remain server-side service secrets.

## Routes And APIs

- `/`: live monitor UI.
- `/eval`: replay and evaluation view.
- `GET /state`: current live state.
- `GET /events`: server-sent event stream.
- `POST /sessions`: start a live session.
- `GET /sessions/history`: stored or fixture-backed session history.
- `GET /sessions/{session_id}/replay`: replay evidence.
- `GET /sessions/{session_id}/proof`: replay proof metrics.
- `GET /sessions/{session_id}/evaluation`: label coverage and metrics.
- `POST /signal-window-labels`: save human signal-window labels.
- `GET /transcript/live`: live transcript proxy.
- `GET /transcript/buckets`: transcript bucket proxy.
- `GET /transcript/health`: transcript service health proxy.

## Evaluation And Evidence

The checked-in evaluation data is intentionally small seed coverage. It exists
to validate schemas, report generation, replay flow, and review workflow. It
should not be described as a statistically meaningful benchmark.

Generate seed evaluation JSON:

```bash
python3 scripts/evaluate_seed_manifests.py \
  --manifest-dir eval \
  --out-dir eval/reports \
  --generated-at 2026-05-11T00:00:00Z \
  --markdown-name ""
```

Generate agent-assisted review JSON/JSONL:

```bash
python3 scripts/run_agent_eval.py \
  --replay-fixture services/chat-ingestor-go/testdata/golden-replay/sessions.json \
  --out-dir eval/reports/agent-review \
  --max-windows 50 \
  --generated-at 2026-05-12T00:00:00Z \
  --markdown-name ""
```

Run replay regression:

```bash
cd services/chat-ingestor-go
go run ./cmd/replay-regression \
  --fixture testdata/golden-replay/sessions.json \
  --baseline testdata/golden-replay/baseline.json \
  --generated-at 2026-05-08T15:00:00Z \
  --limit 500 \
  --format table
```

Run signal correlation report:

```bash
cd services/chat-ingestor-go
go run ./cmd/signal-correlation-report \
  --fixture testdata/golden-replay/sessions.json \
  --out-dir ../../eval/reports \
  --generated-at 2026-05-23T00:00:00Z \
  --format json \
  --markdown-name ""
```

## Project Structure

```text
apps/dashboard
  Vite React TypeScript web UI.

services/chat-ingestor-go
  Go control plane, Twitch chat ingestion, bucketization, event gateway,
  analysis service, replay regression CLI, signal correlation CLI, storage,
  and tests.

services/sentiment-analyzer-python
  Python sentiment model service for chat and transcript windows.

services/transcript-ingestor-cpp
  Native transcript service for live audio capture and NVIDIA ASR.

contracts/schemas
  JSON Schema contract for live payloads.

eval
  Seed JSONL manifests and generated JSON/JSONL reports.

ops
  nginx, Prometheus, Grafana, and blackbox probe config.

docker-compose.yml
  Local full-stack Compose graph.

docker-compose.prod.yml
  Single-node production-demo Compose graph.
```

## Tests

```bash
cd services/chat-ingestor-go
go test ./...
go vet ./...
```

```bash
cd apps/dashboard
npm test
npm run build
npm run typecheck
```

```bash
cd services/sentiment-analyzer-python
python3 -m unittest discover -s tests
```

```bash
python3 contracts/test_live_payload_contracts.py
```

```bash
cmake -S services/transcript-ingestor-cpp -B /tmp/transcript-ingestor-cpp-build
cmake --build /tmp/transcript-ingestor-cpp-build -j4
```
