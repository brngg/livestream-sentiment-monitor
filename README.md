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
short Twitch livestream session, watch transcript and timeline signals update
in real time, and inspect the aligned evidence while the session is active.

## Live Demo

![Full live dashboard demo](docs/demo/live-full-package-stableronaldo.gif)

This capture shows a real Twitch livestream flowing through the full live
pipeline: stream preview, public chat ingestion, NVIDIA ASR transcript capture,
Python sentiment scoring, chat/transcript alignment, sentiment timeline, and
signal evidence.

[Watch the higher-quality MP4](docs/demo/live-full-package-stableronaldo.mp4)

For capture notes and reproducibility details, see [docs/demo.md](docs/demo.md).

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

The core product path is stream source -> ingestion -> analysis -> dashboard.
Kafka/Redpanda and Postgres/Neon are optional durability and evaluation layers,
not required for the offline replay demo.

```mermaid
flowchart LR
    subgraph Sources["Live Stream Sources"]
        direction TB
        Twitch["Twitch channel<br/>chat + audio"]
        YouTube["YouTube livestream URL<br/>transcript only"]
    end

    subgraph Ingestion["Ingestion Services"]
        direction TB
        ChatIngestor["Go dashboard service<br/>Helix verification + Twitch IRC"]
        TranscriptIngestor["C++ transcript service<br/>Streamlink + ffmpeg + NVIDIA ASR"]
    end

    subgraph Intelligence["Analysis Pipeline"]
        direction TB
        Sentiment["Python sentiment analyzer<br/>chat + transcript buckets"]
        Alignment["Go alignment service<br/>timestamp overlap"]
        Signals["Signal windows<br/>hype, frustration, shifts, divergence"]
    end

    subgraph Product["User-Facing Product"]
        direction TB
        Dashboard["React live dashboard<br/>video, chat, transcript, graph"]
        EvalLab["Replay / Eval Lab<br/>proof + labels"]
    end

    subgraph OptionalInfra["Optional Durable Infrastructure"]
        direction TB
        Kafka[("Redpanda / Kafka<br/>event topics")]
        Postgres[("Postgres / Neon<br/>sessions, evidence, labels")]
    end

    Twitch -- "public chat" --> ChatIngestor
    Twitch -- "audio stream" --> TranscriptIngestor
    YouTube -- "audio stream" --> TranscriptIngestor

    ChatIngestor -- "chat buckets" --> Sentiment
    TranscriptIngestor -- "transcript buckets" --> Sentiment
    Sentiment -- "scored windows" --> Alignment
    Alignment --> Signals

    ChatIngestor -- "live state" --> Dashboard
    TranscriptIngestor -- "live captions" --> Dashboard
    Signals -- "aligned evidence" --> Dashboard
    Postgres -- "stored sessions" --> EvalLab

    ChatIngestor -. "publish events" .-> Kafka
    TranscriptIngestor -. "publish events" .-> Kafka
    Signals -. "persist evidence" .-> Postgres
    Dashboard -. "labels + proof" .-> Postgres
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
a short session, and sees the live dashboard, transcript snippets, timeline
charts, and chat/transcript alignment update while the stream is running.

The hosted demo is a limited feature preview. Hosted sentiment scoring is
currently disabled while the sentiment model deployment is tuned for Render
resource limits and project budget. The full local stack remains the intended
path for end-to-end sentiment scoring, Eval Lab, replay history,
Redpanda/Kafka, Postgres, and production observability. Session state can stay
in memory for the hosted run, and Twitch and NVIDIA credentials remain
server-side service secrets.

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

## Limitations And Bias

Sentiment and signal windows are evidence aids, not ground truth. The
limitations framing follows the same spirit as projects such as
[FerroEduardo/TwitchSentimentAnalysis](https://github.com/FerroEduardo/TwitchSentimentAnalysis):
model output can reflect training-data bias, and accuracy varies with language,
domain, and context. This project adds a few live-stream-specific caveats:

- Twitch chat often uses slang, sarcasm, spam, emotes, raids, memes, and
  community-specific shorthand that may not map cleanly to positive, neutral,
  or negative sentiment.
- Political, news, or identity-related streams can amplify dataset and
  annotation bias. Treat aggregate sentiment as a review cue, not as a claim
  about a group, person, or event.
- ASR can mishear names, accents, overlapping speech, music, game audio, or
  rapid speech. Alignment quality depends on transcript timing and chat volume.
- A detected signal means the system found enough timing and evidence to
  inspect. It does not prove causality between what was said and how chat
  reacted.
- Public demos that show third-party livestream footage or chat should be kept
  short and replaced with owned or permissioned footage for broad distribution
  when possible.

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
