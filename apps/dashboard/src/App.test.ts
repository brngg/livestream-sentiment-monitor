import { describe, expect, it } from "vitest";
import { dashboardChartHeadings, dashboardUITestAccess } from "./App";
import type { ReactionWindow } from "./types";

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
});
