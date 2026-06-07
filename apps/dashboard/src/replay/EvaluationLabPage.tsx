import { ArrowLeft, History, ListChecks } from "lucide-react";
import { useMemo } from "react";
import type { ReplayProofTruncation, SessionEvaluation, SessionHistory, SessionReplay, SignalWindowLabel } from "../types";
import { ReplayEvaluationPanel } from "./ReplayEvaluationPanel";
import type { SaveReplayEvaluationLabel } from "./ReplayEvaluationPanel";
import { buildReplayWindows } from "./replayModel";

export function EvaluationLabPage({
  sessions,
  selectedSessionID,
  replay,
  loading,
  error,
  labels,
  evaluation,
  evaluationLoading = false,
  evaluationError = "",
  evaluationPartial = false,
  evaluationTruncations,
  onSelect,
  onBack,
  onSaveEvaluation
}: {
  sessions: SessionHistory[];
  selectedSessionID: string;
  replay?: SessionReplay;
  loading: boolean;
  error: string;
  labels: SignalWindowLabel[];
  evaluation?: SessionEvaluation;
  evaluationLoading?: boolean;
  evaluationError?: string;
  evaluationPartial?: boolean;
  evaluationTruncations?: ReplayProofTruncation[];
  onSelect: (sessionID: string) => void;
  onBack: () => void;
  onSaveEvaluation: (label: SaveReplayEvaluationLabel) => void | Promise<unknown>;
}) {
  const windows = useMemo(() => buildReplayWindows(replay), [replay]);
  const selectedSession = replay?.session || sessions.find((session) => session.session_id === selectedSessionID);
  const summary = evaluationLabSummary(selectedSession, replay, windows.length, labels.length);

  return (
    <main className="eval-lab-main">
      <section className="eval-lab-hero">
        <div>
          <span>Dataset Review</span>
          <h1>Evaluation Lab</h1>
          <p>Label stored signal and reaction windows with full replay context, evidence buckets, transcript snippets, and model predictions.</p>
        </div>
        <button type="button" onClick={onBack}>
          <ArrowLeft size={14} />
          Dashboard
        </button>
      </section>

      <section className="eval-lab-grid" aria-label="Replay evaluation lab">
        <section className="modern-panel eval-lab-session-panel">
          <div className="modern-panel-header">Sessions <span>{loading ? "Loading" : `${sessions.length} stored`}</span></div>
          <div className="eval-lab-scroll">
            <SessionPicker
              sessions={sessions}
              selectedSessionID={selectedSessionID}
              error={error}
              onSelect={onSelect}
            />
          </div>
        </section>

        <section className="modern-panel eval-lab-context-panel">
          <div className="modern-panel-header">Review Context <span>{selectedSession ? "Evidence" : "Select session"}</span></div>
          <div className="eval-lab-scroll">
            {selectedSession ? (
              <>
                <div className="eval-lab-session-title">
                  <span>{selectedSession.channel_id || "unknown channel"}</span>
                  <strong>{selectedSession.stream_title || selectedSession.session_id}</strong>
                </div>
                <div className="eval-lab-summary-grid">
                  {summary.map((item) => (
                    <div key={item.label}>
                      <span>{item.label}</span>
                      <strong>{item.value}</strong>
                    </div>
                  ))}
                </div>
                <div className="eval-lab-guidance">
                  <ListChecks size={15} />
                  <p>Prioritize windows where a reaction was detected, confidence is low, or chat and transcript disagree. The queue includes the messages, target context, and transcript evidence needed to explain each label.</p>
                </div>
              </>
            ) : (
              <p className="empty-modern">Select a stored session to start labeling.</p>
            )}
          </div>
        </section>

        <ReplayEvaluationPanel
          sessionID={selectedSessionID}
          replay={replay}
          loading={loading}
          error={error}
          labels={labels}
          evaluation={evaluation}
          evaluationLoading={evaluationLoading}
          evaluationError={evaluationError}
          evaluationPartial={evaluationPartial}
          evaluationTruncations={evaluationTruncations}
          onSaveEvaluation={onSaveEvaluation}
        />
      </section>
    </main>
  );
}

function SessionPicker({
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
      {sessions.map((session) => (
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

function evaluationLabSummary(
  session: SessionHistory | undefined,
  replay: SessionReplay | undefined,
  windowCount: number,
  labelCount: number
) {
  return [
    { label: "Chat buckets", value: compactNumber(session?.chat_bucket_count || replay?.chat_buckets?.length || 0) },
    { label: "Transcript buckets", value: compactNumber(session?.transcript_bucket_count || replay?.transcript_buckets?.length || 0) },
    { label: "Matched windows", value: compactNumber(session?.alignment_count || replay?.alignments?.length || 0) },
    { label: "Review windows", value: compactNumber(windowCount) },
    { label: "Labels", value: compactNumber(labelCount) },
    { label: "Coverage", value: windowCount > 0 ? `${Math.round((labelCount / windowCount) * 100)}%` : "-" }
  ];
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
