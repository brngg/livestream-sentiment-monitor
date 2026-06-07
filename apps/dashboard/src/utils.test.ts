import { describe, expect, it } from "vitest";
import {
  extractChannel,
  formatLanguageMix,
  formatLatency,
  formatList,
  formatPercent,
  formatSignedNumber,
  relationshipLabel
} from "./utils";

describe("dashboard utility formatting", () => {
  it("extracts channel names from common Twitch inputs", () => {
    expect(extractChannel(" https://www.twitch.tv/example_channel?ref=nav ")).toBe("example_channel");
    expect(extractChannel("@creator")).toBe("creator");
    expect(extractChannel("creator/videos")).toBe("creator");
    expect(extractChannel("")).toBe("");
  });

  it("extracts stable ids from YouTube live inputs", () => {
    expect(extractChannel("https://www.youtube.com/watch?v=ABC123xyz")).toBe("youtube-abc123xyz");
    expect(extractChannel("youtube.com/watch?v=ABC123xyz")).toBe("youtube-abc123xyz");
    expect(extractChannel("https://youtu.be/ABC123xyz")).toBe("youtube-abc123xyz");
    expect(extractChannel("https://www.youtube.com/live/ABC123xyz")).toBe("youtube-abc123xyz");
  });

  it("formats optional values into stable dashboard labels", () => {
    expect(formatSignedNumber(0.42)).toBe("+0.42");
    expect(formatSignedNumber(-0.2)).toBe("-0.20");
    expect(formatSignedNumber()).toBe("-");
    expect(formatPercent(0.625)).toBe("63%");
    expect(formatLatency(125)).toBe("125 ms");
    expect(formatLatency(0)).toBe("-");
    expect(formatList(["pog", "hype"])).toBe("pog, hype");
    expect(formatList([])).toBe("-");
  });

  it("formats language mix while ignoring invalid ratios", () => {
    expect(formatLanguageMix({ en: 0.82, es: 0.18 })).toBe("en 82%, es 18%");
    expect(formatLanguageMix({ en: 0.8, unknown: Number.NaN })).toBe("en 80%");
    expect(formatLanguageMix()).toBe("-");
  });

  it("maps relationship identifiers to display labels and style keys", () => {
    expect(relationshipLabel("soft_split")).toEqual({ label: "soft split", key: "soft-split" });
    expect(relationshipLabel("unexpected")).toEqual({ label: "waiting", key: "muted" });
  });
});
