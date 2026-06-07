import { ChevronLeft, ChevronRight } from "lucide-react";
import type { ReplayWindowPoint } from "./replayModel";
import {
  replayWindowChatSentiment,
  replayWindowDelta,
  replayWindowMessageCount,
  replayWindowScore,
  replayWindowTranscriptSentiment
} from "./replayModel";
import { formatPercent, formatSignedNumber } from "../utils";

export function ReplayScrubberTimeline({
  windows,
  selectedIndex,
  onSelect,
  onPrevious,
  onNext
}: {
  windows: ReplayWindowPoint[];
  selectedIndex: number;
  onSelect: (index: number) => void;
  onPrevious: () => void;
  onNext: () => void;
}) {
  const selected = windows[selectedIndex];
  const disabled = windows.length === 0;

  return (
    <div className="replay-scrubber">
      <div className="replay-step-controls">
        <button type="button" onClick={onPrevious} disabled={disabled || selectedIndex <= 0} aria-label="Previous evidence window">
          <ChevronLeft size={14} />
        </button>
        <div>
          <strong>{selected ? bucketWindowLabel(selected.start, selected.end) : "--:-----:--"}</strong>
          <span>{windows.length > 0 ? `${selectedIndex + 1} / ${windows.length}` : "0 windows"}</span>
        </div>
        <button type="button" onClick={onNext} disabled={disabled || selectedIndex >= windows.length - 1} aria-label="Next evidence window">
          <ChevronRight size={14} />
        </button>
      </div>

      <input
        className="replay-range"
        type="range"
        min={0}
        max={Math.max(0, windows.length - 1)}
        value={Math.min(selectedIndex, Math.max(0, windows.length - 1))}
        disabled={disabled}
        onChange={(event) => onSelect(Number(event.target.value))}
        aria-label="Evidence window scrubber"
      />

      <div className="replay-window-rail" role="list" aria-label="Stored sentiment windows">
        {windows.map((window, index) => {
          const score = replayWindowScore(window);
          const selectedWindow = index === selectedIndex;
          const height = 18 + Math.round(Math.abs(score || 0) * 34);
          return (
            <button
              className={`replay-window-tick ${selectedWindow ? "selected" : ""} ${scoreTone(score)}`}
              key={window.id}
              type="button"
              role="listitem"
              style={{ height }}
              onClick={() => onSelect(index)}
              title={`${bucketWindowLabel(window.start, window.end)} ${formatSignedNumber(score)}`}
            />
          );
        })}
      </div>

      {selected ? (
        <div className="replay-scrubber-readout">
          <div><span>Chat</span><strong className="chat-color">{formatSignedNumber(replayWindowChatSentiment(selected))}</strong></div>
          <div><span>Transcript</span><strong className="transcript-color">{formatSignedNumber(replayWindowTranscriptSentiment(selected))}</strong></div>
          <div><span>Delta</span><strong>{formatSignedNumber(replayWindowDelta(selected))}</strong></div>
          <div><span>Messages</span><strong>{replayWindowMessageCount(selected).toLocaleString()}</strong></div>
          <div><span>Quality</span><strong>{formatPercent(selected.signalWindow?.quality ?? selected.alignment?.quality)}</strong></div>
        </div>
      ) : null}
    </div>
  );
}

function bucketWindowLabel(start?: string, end?: string) {
  return `${formatCompactTime(start)}-${formatCompactTime(end)}`;
}

function formatCompactTime(value?: string) {
  if (!value) return "--:--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--:--";
  return date.toLocaleTimeString([], { minute: "2-digit", second: "2-digit" });
}

function scoreTone(value?: number) {
  if (typeof value !== "number") return "neutral";
  if (value > 0.05) return "positive";
  if (value < -0.05) return "negative";
  return "neutral";
}
