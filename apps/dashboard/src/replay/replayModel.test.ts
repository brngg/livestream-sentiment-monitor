import { describe, expect, it } from "vitest";
import type { ChatBucket, SessionReplay, TranscriptBucket } from "../types";
import {
  buildReplayWindows,
  clampTranscriptOffset,
  findBestChatBucket,
  findBestTranscriptBucket,
  replayWindowDelta,
  replayWindowQuality,
  replayWindowRelationship,
  replayWindowScore,
  resolveReplayEvidenceRefs,
  transcriptContextForBucket
} from "./replayModel";

const t0 = "2026-05-08T12:00:00.000Z";
const t5 = "2026-05-08T12:00:05.000Z";
const t10 = "2026-05-08T12:00:10.000Z";
const t15 = "2026-05-08T12:00:15.000Z";
const t20 = "2026-05-08T12:00:20.000Z";
const t25 = "2026-05-08T12:00:25.000Z";
const t30 = "2026-05-08T12:00:30.000Z";

function chatBucket(start: string, end: string, sentiment: number): ChatBucket {
  return {
    bucket_start: start,
    bucket_end: end,
    chat_sentiment: sentiment,
    sentiment_confidence: 0.8,
    message_count: 12
  };
}

function transcriptBucket(start: string, end: string, sentiment: number): TranscriptBucket {
  return {
    bucket_start: start,
    bucket_end: end,
    sentiment_score: sentiment,
    sentiment_confidence: 0.6,
    text: `${start} transcript`
  };
}

describe("replay model helpers", () => {
  it("builds sorted replay windows and carries nearby transcript context", () => {
    const earlyTranscript = transcriptBucket(t0, t10, 0.1);
    const currentTranscript = transcriptBucket(t10, t20, -0.4);
    const nextTranscript = transcriptBucket(t20, t30, 0.3);
    const replay: SessionReplay = {
      session: { session_id: "session-1" },
      chat_buckets: [chatBucket(t10, t20, 0.25)],
      transcript_buckets: [nextTranscript, currentTranscript, earlyTranscript],
      alignments: [
        {
          window_start: t10,
          window_end: t20,
          chat_bucket_start: t10,
          transcript_bucket_start: t10,
          chat_sentiment: 0.25,
          transcript_sentiment: -0.4,
          delta: 0.65,
          quality: 0.7,
          relationship: "diverged"
        }
      ],
      signal_events: [
        { timestamp: t15, source: "chat", label: "spike", text: "inside" },
        { timestamp: t25, source: "chat", label: "late", text: "outside" }
      ]
    };

    const windows = buildReplayWindows(replay);

    expect(windows).toHaveLength(1);
    expect(windows[0].start).toBe(t10);
    expect(windows[0].chatBucket?.bucket_start).toBe(t10);
    expect(windows[0].transcriptBucket?.bucket_start).toBe(t10);
    expect(windows[0].events.map((event) => event.label)).toEqual(["spike"]);
    expect(windows[0].transcriptContext.map((context) => context.role)).toEqual(["previous", "current", "next"]);
    expect(replayWindowScore(windows[0])).toBeCloseTo(-0.075);
    expect(replayWindowDelta(windows[0])).toBe(0.65);
    expect(replayWindowQuality(windows[0])).toBe(0.7);
    expect(replayWindowRelationship(windows[0])).toBe("diverged");
  });

  it("uses transcript offset when matching transcript buckets", () => {
    const buckets = [
      transcriptBucket(t0, t10, 0.2),
      transcriptBucket(t10, t20, -0.3),
      transcriptBucket(t20, t30, 0.5)
    ];

    expect(findBestTranscriptBucket({ start: t0, end: t10 }, buckets)?.bucket_start).toBe(t0);
    expect(findBestTranscriptBucket({ start: t0, end: t10 }, buckets, undefined, 12)?.bucket_start).toBe(t10);
    expect(findBestTranscriptBucket({ start: t20, end: t30 }, buckets, undefined, -12)?.bucket_start).toBe(t10);
  });

  it("falls back to nearest buckets when exact overlap is unavailable", () => {
    const buckets = [
      chatBucket(t0, t5, 0.1),
      chatBucket(t20, t25, 0.5)
    ];

    expect(findBestChatBucket({ start: t10, end: t15 }, buckets)?.bucket_start).toBe(t0);
    expect(findBestChatBucket({ start: t15, end: t20 }, buckets)?.bucket_start).toBe(t20);
  });

  it("clamps transcript offsets and preserves context order", () => {
    expect(clampTranscriptOffset(Number.NaN)).toBe(0);
    expect(clampTranscriptOffset(121.4)).toBe(120);
    expect(clampTranscriptOffset(-121.4)).toBe(-120);
    expect(clampTranscriptOffset(3.6)).toBe(4);

    const buckets = [
      transcriptBucket(t20, t30, 0.3),
      transcriptBucket(t0, t10, 0.1),
      transcriptBucket(t10, t20, 0.2)
    ];

    expect(transcriptContextForBucket(buckets[2], buckets).map((context) => context.bucket.bucket_start)).toEqual([
      t0,
      t10,
      t20
    ]);
  });

  it("resolves synthetic transcript evidence ids to replay transcript snippets", () => {
    const currentTranscript = transcriptBucket(t10, t20, -0.4);
    currentTranscript.text = "boss fight transcript snippet";
    const replay: SessionReplay = {
      session: { session_id: "session-1" },
      chat_buckets: [chatBucket(t10, t20, 0.25)],
      transcript_buckets: [currentTranscript],
      signal_windows: [
        {
          type: "signal_window",
          window_start: t10,
          window_end: t20,
          evidence_ids: ["transcript:2026-05-08T12:00:10Z", "chat-message-1"]
        }
      ]
    };

    const [window] = buildReplayWindows(replay);
    const refs = resolveReplayEvidenceRefs(window, window.signalWindow?.evidence_ids);

    expect(refs[0]).toMatchObject({
      id: "transcript:2026-05-08T12:00:10Z",
      source: "transcript",
      text: "boss fight transcript snippet"
    });
    expect(refs[0].label).toContain("transcript");
    expect(refs[1]).toMatchObject({ id: "chat-message-1", source: "chat", label: "chat-message-1" });
  });
});
