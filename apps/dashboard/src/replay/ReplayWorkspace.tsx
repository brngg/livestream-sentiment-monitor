import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { ChevronLeft, ChevronRight, History, Radio, RotateCcw } from "lucide-react";
import type { ReplayProof, SessionHistory, SessionReplay } from "../types";
import { ReplayEvidencePanel } from "./ReplayEvidencePanel";
import { ReplayInsightPanel } from "./ReplayInsightPanel";
import { ReplayProofPanel } from "./ReplayProofPanel";
import { ReplayScrubberTimeline } from "./ReplayScrubberTimeline";
import { buildReplayWindows, clampTranscriptOffset } from "./replayModel";

export function ReplayWorkspace({
  liveSessionID,
  selectedSessionID,
  sessions,
  replay,
  proof,
  proofLoading = false,
  proofError = "",
  loading,
  error,
  onSelect,
  onLive,
  evaluation
}: {
  liveSessionID?: string;
  selectedSessionID: string;
  sessions: SessionHistory[];
  replay?: SessionReplay;
  proof?: ReplayProof;
  proofLoading?: boolean;
  proofError?: string;
  loading: boolean;
  error: string;
  onSelect: (sessionID: string) => void;
  onLive: () => void;
  evaluation?: ReactNode;
}) {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [transcriptOffsetSeconds, setTranscriptOffsetSeconds] = useState(0);
  const windows = useMemo(
    () => buildReplayWindows(replay, { transcriptOffsetSeconds }),
    [replay, transcriptOffsetSeconds]
  );
  const selectedSession = replay?.session || sessions.find((session) => session.session_id === selectedSessionID);
  const selectedWindow = windows[selectedIndex];
  const meta = selectedSessionID ? "Stored Evidence" : liveSessionID ? "Live armed" : "Stored";

  useEffect(() => {
    setSelectedIndex(0);
  }, [selectedSessionID]);

  useEffect(() => {
    setSelectedIndex((current) => Math.min(current, Math.max(0, windows.length - 1)));
  }, [windows.length]);

  return (
    <section className="modern-panel replay-history-panel replay-workspace replay-review-panel">
      <div className="modern-panel-header">Replay Review <span>{meta}</span></div>
      <div className="replay-controls">
        <button className={!selectedSessionID ? "selected" : ""} type="button" onClick={onLive}>
          <Radio size={13} />
          Live
        </button>
        <span>{loading ? "Loading" : error || `${sessions.length} stored`}</span>
      </div>

      <div className={evaluation ? "replay-review-body" : "replay-review-body replay-review-body-single"}>
        <div className="replay-history-scroll replay-workspace-scroll">
          <SessionList sessions={sessions} selectedSessionID={selectedSessionID} error={error} onSelect={onSelect} />

          {selectedSession ? (
            <div className="replay-summary-block">
              <div className="metric-label-wrapper"><div className="metric-label">Stored Summary</div></div>
              <div className="replay-summary-grid">
                <div><span>Chat</span><strong>{compactNumber(selectedSession.chat_bucket_count || replay?.chat_buckets?.length || 0)}</strong></div>
                <div><span>Transcript</span><strong>{compactNumber(selectedSession.transcript_bucket_count || replay?.transcript_buckets?.length || 0)}</strong></div>
                <div><span>Matched</span><strong>{compactNumber(selectedSession.alignment_count || replay?.alignments?.length || 0)}</strong></div>
                <div><span>Windows</span><strong>{compactNumber(windows.length)}</strong></div>
              </div>
            </div>
          ) : null}

          <ReplayProofPanel
            selectedSessionID={selectedSessionID}
            proof={proof}
            replay={replay}
            loading={proofLoading}
            error={proofError}
          />

          {selectedSessionID ? (
            <ReplayInsightPanel insights={replay?.insights} summary={replay?.insight_summary} />
          ) : null}

          {selectedSessionID ? (
            <div className="replay-scrubber-block">
              <div className="metric-label-wrapper"><div className="metric-label">Sentiment Evidence Scrubber</div></div>
              {windows.length === 0 ? (
                <p className="empty-modern">No stored signal, alignment, chat, or transcript buckets returned for this session.</p>
              ) : (
                <>
                  <ReplayScrubberTimeline
                    windows={windows}
                    selectedIndex={selectedIndex}
                    onSelect={setSelectedIndex}
                    onPrevious={() => setSelectedIndex((current) => Math.max(0, current - 1))}
                    onNext={() => setSelectedIndex((current) => Math.min(windows.length - 1, current + 1))}
                  />
                  <TranscriptOffsetControl
                    value={transcriptOffsetSeconds}
                    onChange={setTranscriptOffsetSeconds}
                  />
                </>
              )}
            </div>
          ) : null}

          <ReplayEvidencePanel window={selectedWindow} />
        </div>
        {evaluation ? <div className="replay-review-evaluation">{evaluation}</div> : null}
      </div>
    </section>
  );
}

function TranscriptOffsetControl({
  value,
  onChange
}: {
  value: number;
  onChange: (value: number) => void;
}) {
  const clampedValue = clampTranscriptOffset(value);

  return (
    <div className="replay-offset-control">
      <div>
        <span>Transcript offset</span>
        <strong>{formatOffsetSeconds(clampedValue)}</strong>
      </div>
      <button
        type="button"
        onClick={() => onChange(clampTranscriptOffset(clampedValue - 5))}
        disabled={clampedValue <= -120}
        aria-label="Shift transcript evidence earlier"
        title="Shift transcript evidence earlier"
      >
        <ChevronLeft size={13} />
      </button>
      <input
        type="range"
        min={-120}
        max={120}
        step={5}
        value={clampedValue}
        onChange={(event) => onChange(clampTranscriptOffset(Number(event.target.value)))}
        aria-label="Transcript replay evidence offset"
      />
      <button
        type="button"
        onClick={() => onChange(clampTranscriptOffset(clampedValue + 5))}
        disabled={clampedValue >= 120}
        aria-label="Shift transcript evidence later"
        title="Shift transcript evidence later"
      >
        <ChevronRight size={13} />
      </button>
      <button
        type="button"
        onClick={() => onChange(0)}
        disabled={clampedValue === 0}
        aria-label="Reset transcript evidence offset"
        title="Reset transcript evidence offset"
      >
        <RotateCcw size={12} />
      </button>
    </div>
  );
}

function SessionList({
  sessions,
  selectedSessionID,
  error,
  onSelect
}: {
  sessions: SessionHistory[];
  selectedSessionID: string;
  error: string;
  onSelect: (sessionID: string) => void;
}) {
  if (sessions.length === 0) {
    return <p className="empty-modern">{error || "No stored sessions returned yet."}</p>;
  }

  return (
    <div className="session-history-list">
      {sessions.slice(0, 12).map((session) => (
        <button
          className={session.session_id === selectedSessionID ? "session-history-row selected" : "session-history-row"}
          key={session.session_id}
          type="button"
          onClick={() => onSelect(session.session_id)}
        >
          <History size={13} />
          <span>
            <strong>{session.channel_id || "unknown channel"}</strong>
            <em>{historySessionTitle(session)}</em>
          </span>
          <small>{formatHistoryDate(session.started_at)}</small>
        </button>
      ))}
    </div>
  );
}

function historySessionTitle(session: SessionHistory) {
  const counts = [
    session.chat_bucket_count ? `${session.chat_bucket_count} chat` : "",
    session.transcript_bucket_count ? `${session.transcript_bucket_count} voice` : "",
    session.alignment_count ? `${session.alignment_count} matched` : ""
  ].filter(Boolean);
  return session.stream_title || counts.join(" / ") || session.status || session.session_id;
}

function formatHistoryDate(value?: string) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleDateString([], { month: "short", day: "2-digit" });
}

function compactNumber(value: number) {
  if (!Number.isFinite(value)) return "0";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
  return Math.round(value).toLocaleString();
}

function formatOffsetSeconds(value: number) {
  if (value === 0) return "0s";
  return `${value > 0 ? "+" : ""}${value}s`;
}
