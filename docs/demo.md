# Demo Notes

This project has two demo paths:

- `docs/demo/live-full-package-stableronaldo.gif` shows the full live pipeline:
  real Twitch video, public chat ingestion, NVIDIA ASR transcript capture,
  Python sentiment scoring, chat/transcript alignment, sentiment graphing, and
  signal evidence.
- `docs/demo/eval-lab-demo.gif` shows the offline replay and evaluation lab
  using checked-in fixture data.

The live capture was recorded from a real Twitch session on June 9, 2026. It is
useful for showing the product surface, but it depends on external services and
the streamer being live. The offline replay path is the reproducible demo path
for reviewers who do not have Twitch, NVIDIA, or database credentials.

## Recommended README Embed

```md
![Full live dashboard demo](docs/demo/live-full-package-stableronaldo.gif)

[Watch the higher-quality MP4](docs/demo/live-full-package-stableronaldo.mp4)
```

## What The Full Demo Shows

- A live Twitch stream preview.
- Real Twitch IRC chat messages entering the dashboard.
- Live transcript text from Streamlink, ffmpeg, and hosted NVIDIA ASR.
- Python sentiment scoring from
  `cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual`.
- Chat/transcript alignment and signal windows.
- A visible sentiment timeline graph and signal evidence panel.

During the capture, the live run produced 1,934 chat messages, 60 Python
sentiment buckets, 48 transcript buckets, 48 alignments, and 60 signal windows.

## Reproducible Offline Demo

The offline replay demo runs without Twitch, NVIDIA, or Postgres:

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

Open `http://localhost:8090/eval`.

## Limitations And Bias

This section follows the practical limitation style used by projects such as
[FerroEduardo/TwitchSentimentAnalysis](https://github.com/FerroEduardo/TwitchSentimentAnalysis),
but is specific to this repo's chat, ASR, and alignment workflow.

Sentiment analysis is not ground truth. Model scores can reflect bias in the
model's training data and may perform unevenly across languages, dialects,
slang, identity terms, political topics, sarcasm, and community-specific memes.

Twitch chat is especially noisy. Repeated emotes, copy-pasta, raids, bot-like
behavior, clipped quotes, and ironic language can look like sentiment shifts
even when the audience is joking or repeating a meme.

Transcript quality also matters. ASR can mishear names, accents, music, game
audio, overlapping voices, or fast speech. If transcript text or timestamps are
wrong, downstream alignment and signal windows can also be wrong.

Signal windows should be treated as review candidates. A signal means the
system found a time window with enough chat, transcript, and timing evidence to
inspect; it does not prove that the streamer caused the audience reaction or
that the detected reaction is objectively correct.

For public demos, prefer owned or permissioned footage. Short third-party
captures can demonstrate integration behavior, but they may include copyrighted
video, personal chat messages, or creator/community context that should not be
over-interpreted.
