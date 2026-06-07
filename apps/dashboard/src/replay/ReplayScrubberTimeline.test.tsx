import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ReplayScrubberTimeline } from "./ReplayScrubberTimeline";
import type { ReplayWindowPoint } from "./replayModel";

function replayWindow(id: string, start: string, end: string, chat: number, transcript: number): ReplayWindowPoint {
  return {
    id,
    source: "alignment",
    start,
    end,
    alignment: {
      window_start: start,
      window_end: end,
      chat_sentiment: chat,
      transcript_sentiment: transcript,
      delta: chat - transcript,
      quality: 0.75
    },
    chatBucket: {
      bucket_start: start,
      bucket_end: end,
      chat_sentiment: chat,
      message_count: 24
    },
    transcriptBucket: {
      bucket_start: start,
      bucket_end: end,
      sentiment_score: transcript
    },
    transcriptContext: [],
    transcriptOffsetSeconds: 0,
    events: []
  };
}

describe("ReplayScrubberTimeline", () => {
  it("renders an empty scrubber with disabled navigation", () => {
    render(
      <ReplayScrubberTimeline
        windows={[]}
        selectedIndex={0}
        onSelect={vi.fn()}
        onPrevious={vi.fn()}
        onNext={vi.fn()}
      />
    );

    expect(screen.getByText("0 windows")).toBeInTheDocument();
    expect(screen.getByLabelText("Previous evidence window")).toBeDisabled();
    expect(screen.getByLabelText("Next evidence window")).toBeDisabled();
    expect(screen.getByLabelText("Evidence window scrubber")).toBeDisabled();
  });

  it("selects windows through buttons and range input while showing selected metrics", () => {
    const onSelect = vi.fn();
    const onPrevious = vi.fn();
    const onNext = vi.fn();
    const windows = [
      replayWindow("first", "2026-05-08T12:00:00.000Z", "2026-05-08T12:00:05.000Z", 0.4, 0.1),
      replayWindow("second", "2026-05-08T12:00:05.000Z", "2026-05-08T12:00:10.000Z", -0.2, 0.3)
    ];

    render(
      <ReplayScrubberTimeline
        windows={windows}
        selectedIndex={1}
        onSelect={onSelect}
        onPrevious={onPrevious}
        onNext={onNext}
      />
    );

    expect(screen.getByText("2 / 2")).toBeInTheDocument();
    expect(screen.getByLabelText("Previous evidence window")).toBeEnabled();
    expect(screen.getByLabelText("Next evidence window")).toBeDisabled();
    expect(screen.getByText("Chat").nextSibling).toHaveTextContent("-0.20");
    expect(screen.getByText("Transcript").nextSibling).toHaveTextContent("+0.30");
    expect(screen.getByText("Delta").nextSibling).toHaveTextContent("-0.50");
    expect(screen.getByText("Messages").nextSibling).toHaveTextContent("24");
    expect(screen.getByText("Quality").nextSibling).toHaveTextContent("75%");

    fireEvent.click(screen.getByLabelText("Previous evidence window"));
    expect(onPrevious).toHaveBeenCalledTimes(1);

    fireEvent.change(screen.getByLabelText("Evidence window scrubber"), { target: { value: "0" } });
    expect(onSelect).toHaveBeenCalledWith(0);

    fireEvent.click(screen.getAllByRole("listitem")[0]);
    expect(onSelect).toHaveBeenLastCalledWith(0);
  });
});
