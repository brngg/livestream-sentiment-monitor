import { describe, expect, it } from "vitest";
import { dashboardChartHeadings, dashboardUITestAccess } from "./App";
import type { ChatBucket, ReactionWindow, TranscriptBucket } from "./types";

describe("dashboard product-facing UI labels", () => {
  it("summarizes a live transcript reaction without backend-only labels", () => {
    const reaction: ReactionWindow = {
      source: "transcript",
      provisional: true,
      reaction_type: "voice_hype",
      valence: 0.62,
      confidence: 0.71,
      transcript_text: "that was incredible"
    };

    expect(dashboardUITestAccess.primaryInsightForWindow(reaction, undefined)).toMatchObject({
      reaction: "voice hype",
      target: "unknown",
      intensity: "medium 62%",
      agreement: "waiting",
      delay: "waiting"
    });
  });

  it("keeps live reaction chart copy product-facing", () => {
    const copy = dashboardUITestAccess.reactionChartDescription(true, false, true);

    expect(copy).toContain("live transcript reactions");
    expect(copy).not.toMatch(/provisional/i);
  });

  it("keeps both analytics chart headings available", () => {
    expect(dashboardChartHeadings.sentiment).toBe("Sentiment Timeline");
    expect(dashboardChartHeadings.reaction).toBe("Live Reaction Timeline");
  });

  it("keeps scored transcript sentiment visible before alignment exists", () => {
    const chatBucket: ChatBucket = {
      session_id: "session-1",
      channel_id: "channel-1",
      bucket_start: "2026-06-09T12:00:00Z",
      bucket_end: "2026-06-09T12:00:30Z",
      chat_sentiment: 0.42,
      message_count: 8
    };
    const transcriptBucket: TranscriptBucket = {
      session_id: "session-1",
      channel_id: "channel-1",
      bucket_start: "2026-06-09T12:00:00Z",
      bucket_end: "2026-06-09T12:00:30Z",
      text: "that was a great clutch play",
      sentiment_score: 0.66,
      sentiment_confidence: 0.81,
      sentiment_status: "python",
      word_count: 6
    };

    const points = dashboardUITestAccess.buildSentimentPoints([], [chatBucket], [transcriptBucket]);

    expect(points).toHaveLength(1);
    expect(points[0]).toMatchObject({
      chat: 0.42,
      transcript: 0.66,
      aggregate: 0.54,
      messages: 8
    });
  });

  it("labels scored transcript buckets as scored instead of finalizing", () => {
    expect(dashboardUITestAccess.transcriptFinalityForBucket({
      sentiment_score: 0,
      sentiment_status: "python",
      transcript_status: "live"
    })).toBe("Scored");
  });

  it("shows scored transcript health when sentiment latency is present", () => {
    const metrics = dashboardUITestAccess.buildHealthMetrics(
      {
        status: "live",
        messages: [],
        buckets: [],
        reaction_windows: [],
        transcript_buckets: [],
        alignments: [],
        message_count: 0,
        bucket_count: 0
      },
      [{
        sentiment_score: 0.31,
        sentiment_status: "python",
        sentiment_latency_ms: 9,
        text: "streamer speech with enough words to score",
        word_count: 34
      }],
      "NVIDIA_STREAMING stream",
      false
    );

    expect(metrics.find((metric) => metric.label === "Transcript")).toMatchObject({
      value: "Scored",
      meta: "34 words",
      tone: "ok"
    });
  });
});
