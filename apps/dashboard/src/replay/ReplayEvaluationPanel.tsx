import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { AlertTriangle, BarChart3, Gauge, Info } from "lucide-react";
import type {
  EventBinaryCounts,
  ReplayProofTruncation,
  SessionEvaluation,
  SignalWindowLabel,
  SessionReplay
} from "../types";
import type { SaveSignalWindowLabelRequest } from "../api";
import { buildReplayWindows } from "./replayModel";
import type { ReplayWindowPoint } from "./replayModel";
import { ReplayEvaluationQueue } from "./ReplayEvaluationQueue";
import type { ReplayEvaluationQueueMode, ReplayEvaluationQueueSavePayload } from "./ReplayEvaluationQueue";

export type SaveReplayEvaluationLabel = SaveSignalWindowLabelRequest;

export function ReplayEvaluationPanel({
  embedded = false,
  sessionID,
  replay,
  loading = false,
  error = "",
  labels,
  evaluation,
  evaluationLoading = false,
  evaluationError = "",
  evaluationPartial = false,
  evaluationTruncations = [],
  onSaveEvaluation
}: {
  embedded?: boolean;
  sessionID: string;
  replay?: SessionReplay;
  loading?: boolean;
  error?: string;
  labels: SignalWindowLabel[];
  evaluation?: SessionEvaluation;
  evaluationLoading?: boolean;
  evaluationError?: string;
  evaluationPartial?: boolean;
  evaluationTruncations?: ReplayProofTruncation[];
  onSaveEvaluation?: (label: SaveReplayEvaluationLabel) => void | Promise<unknown>;
}) {
  const [queueMode, setQueueMode] = useState<ReplayEvaluationQueueMode>("unlabeled");
  const [selectedQueueWindow, setSelectedQueueWindow] = useState<ReplayWindowPoint | undefined>();
  const windows = useMemo(() => buildReplayWindows(replay), [replay]);
  const summary = useMemo(() => summarizeEvaluations(labels), [labels]);
  const agentReviews = replay?.agent_reviews || [];

  useEffect(() => {
    setSelectedQueueWindow(undefined);
  }, [sessionID, queueMode]);

  useEffect(() => {
    if (queueMode !== "unlabeled") return;
    if (agentReviews.length === 0 || windows.length === 0) return;
    if (labels.length >= windows.length) setQueueMode("agent_suggested");
  }, [agentReviews.length, labels.length, queueMode, windows.length]);

  const handleQueueModeChange = (mode: ReplayEvaluationQueueMode) => {
    setQueueMode(mode);
  };

  return (
    <section className={embedded ? "replay-evaluation-panel replay-evaluation-panel-embedded" : "modern-panel replay-evaluation-panel"}>
      <div className="modern-panel-header">Evaluation <span>{sessionID ? `${summary.total} labels` : "Replay required"}</span></div>
      <div className="replay-evaluation-scroll">
        {sessionID ? (
          <>
            <SessionEvaluationMetrics
              sessionID={sessionID}
              evaluation={evaluation}
              loading={evaluationLoading}
              error={evaluationError}
              partial={evaluationPartial}
              truncations={evaluationTruncations}
            />
            <EvaluationMetrics summary={summary} />
          </>
        ) : (
          <p className="empty-modern replay-evaluation-empty">Select a stored replay session to label signal windows.</p>
        )}

        {sessionID && error ? (
          <p className="empty-modern replay-evaluation-empty">{error}</p>
        ) : null}

        {sessionID && loading && windows.length === 0 ? (
          <p className="empty-modern replay-evaluation-empty">Loading replay windows for evaluation.</p>
        ) : null}

        {sessionID ? (
          <ReplayEvaluationQueue
            windows={windows}
            windowLabels={labels}
            agentReviews={agentReviews}
            mode={queueMode}
            selectedWindow={selectedQueueWindow}
            onModeChange={handleQueueModeChange}
            onSelectedWindowChange={(window) => setSelectedQueueWindow(window)}
            onSaveLabel={(payload) => {
              const label = replayEvaluationLabelFromPayload(sessionID, payload);
              if (label) return onSaveEvaluation?.(label);
              return undefined;
            }}
          />
        ) : null}
      </div>
    </section>
  );
}

function SessionEvaluationMetrics({
  sessionID,
  evaluation,
  loading,
  error,
  partial,
  truncations
}: {
  sessionID: string;
  evaluation?: SessionEvaluation;
  loading: boolean;
  error: string;
  partial: boolean;
  truncations: ReplayProofTruncation[];
}) {
  if (!sessionID) return null;

  if (loading && !evaluation) {
    return (
      <div className="replay-evaluation-metrics-block">
        <div className="metric-label-wrapper"><div className="metric-label">Backend Quality</div></div>
        <EvaluationMetricState icon={<Gauge size={14} />} title="Loading evaluation" text="Fetching backend evaluation metrics." />
      </div>
    );
  }

  if (error) {
    return (
      <div className="replay-evaluation-metrics-block">
        <div className="metric-label-wrapper"><div className="metric-label">Backend Quality</div></div>
        <EvaluationMetricState icon={<AlertTriangle size={14} />} title="Evaluation unavailable" text={error} tone="error" />
      </div>
    );
  }

  if (!evaluation) {
    return (
      <div className="replay-evaluation-metrics-block">
        <div className="metric-label-wrapper"><div className="metric-label">Backend Quality</div></div>
        <EvaluationMetricState icon={<Info size={14} />} title="No evaluation returned" text="The evaluation endpoint returned no metrics for this session." />
      </div>
    );
  }

  const evaluated = evaluation.evaluated_windows ?? 0;
  const labelMetrics = evaluation.event_label_metrics || [];
  const reactionMetrics = evaluation.reaction_type_metrics || [];
  const confusionCounts = evaluation.event_confusion_counts || [];
  const correctnessCounts = Object.entries(evaluation.correctness_counts || {})
    .filter((entry): entry is [string, number] => typeof entry[1] === "number")
    .sort((left, right) => right[1] - left[1]);

  return (
    <div className="replay-evaluation-metrics-block">
      <div className="metric-label-wrapper"><div className="metric-label">Backend Quality</div></div>

      {partial || truncations.length > 0 ? (
        <div className="replay-evaluation-metric-banner warn">
          <AlertTriangle size={14} />
          <div>
            <span>Partial evaluation input</span>
            <p>{truncationSummary(truncations) || "The backend marked the replay source as partial."}</p>
          </div>
        </div>
      ) : null}

      {evaluated === 0 ? (
        <EvaluationMetricState
          icon={<Info size={14} />}
          title="No scored labels"
          text="Save correct or wrong event labels to calculate accuracy, precision, recall, and F1."
        />
      ) : null}

      <div className="replay-evaluation-score-grid" aria-label="Event detector quality metrics">
        <EvaluationMetric label="Event accuracy" value={formatPercentValue(evaluation.event_accuracy)} />
        <EvaluationMetric label="Precision" value={formatPercentValue(evaluation.event_precision)} />
        <EvaluationMetric label="Recall" value={formatPercentValue(evaluation.event_recall)} />
        <EvaluationMetric label="F1" value={formatPercentValue(evaluation.event_f1)} />
        <EvaluationMetric label="Coverage" value={formatPercentValue(evaluation.coverage)} />
      </div>

      <div className="replay-evaluation-score-grid" aria-label="Reaction context quality metrics">
        <EvaluationMetric label="Peak recall" value={formatPercentValue(evaluation.peak_recall ?? evaluation.hype_peak_recall)} />
        <EvaluationMetric label="Onset latency" value={formatMillisecondsValue(evaluation.onset_latency_ms ?? evaluation.event_onset_latency_ms)} />
        <EvaluationMetric label="Reaction F1" value={formatPercentValue(evaluation.reaction_type_f1)} />
        <EvaluationMetric label="Reaction accuracy" value={formatPercentValue(evaluation.reaction_type_accuracy)} />
        <EvaluationMetric label="Target accuracy" value={formatPercentValue(evaluation.target_accuracy ?? evaluation.target_extraction_accuracy)} />
        <EvaluationMetric label="Divergence accuracy" value={formatPercentValue(evaluation.divergence_accuracy)} />
      </div>

      <div className="replay-evaluation-count-grid" aria-label="Evaluation coverage counts">
        <EvaluationMetric label="Evaluated" value={compactNumber(evaluation.evaluated_windows)} />
        <EvaluationMetric label="Labeled" value={compactNumber(evaluation.total_labeled_windows)} />
        <EvaluationMetric label="Total windows" value={compactNumber(evaluation.total_windows)} />
        <EvaluationMetric label="Unmatched" value={compactNumber(evaluation.unmatched_labels)} />
        <EvaluationMetric label="Uncertain" value={compactNumber(evaluation.uncertain_labels)} />
        <EvaluationMetric label="Invalid labels" value={compactNumber(evaluation.invalid_labels)} />
        <EvaluationMetric label="Normal false positives" value={compactNumber(evaluation.false_positives_normal_chat)} />
      </div>

      <BinaryConfusionCounts counts={evaluation.event_counts} />

      {correctnessCounts.length > 0 ? (
        <div className="replay-evaluation-pill-row" aria-label="Correctness counts">
          {correctnessCounts.map(([label, count]) => (
            <span key={label}>{formatMetricName(label)} {compactNumber(count)}</span>
          ))}
        </div>
      ) : null}

      <EvaluationConfusionTable items={confusionCounts} />
      {evaluated > 0 ? <EventLabelMetricsTable items={labelMetrics} /> : null}
      {evaluated > 0 ? <ReactionTypeMetricsTable items={reactionMetrics} /> : null}
      <UnsupportedEvaluationMetrics items={evaluation.unsupported_metrics || []} />
    </div>
  );
}

function EvaluationMetricState({
  icon,
  title,
  text,
  tone = "muted"
}: {
  icon: ReactNode;
  title: string;
  text: string;
  tone?: "muted" | "error";
}) {
  return (
    <div className={`replay-evaluation-metric-state ${tone}`}>
      {icon}
      <div>
        <span>{title}</span>
        <p>{text}</p>
      </div>
    </div>
  );
}

function EvaluationMetric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function BinaryConfusionCounts({ counts }: { counts?: EventBinaryCounts }) {
  const rows = [
    { label: "True positive", value: counts?.true_positive },
    { label: "False positive", value: counts?.false_positive },
    { label: "False negative", value: counts?.false_negative },
    { label: "True negative", value: counts?.true_negative }
  ];

  return (
    <div className="replay-evaluation-confusion-grid" aria-label="Event confusion counts">
      {rows.map((row) => <EvaluationMetric key={row.label} label={row.label} value={compactNumber(row.value)} />)}
    </div>
  );
}

function EvaluationConfusionTable({ items }: { items: NonNullable<SessionEvaluation["event_confusion_counts"]> }) {
  return (
    <div className="replay-evaluation-table-block">
      <div className="replay-section-title">Actual / Predicted Counts</div>
      {items.length === 0 ? (
        <p className="replay-evaluation-table-empty">No event confusion rows returned.</p>
      ) : (
        <div className="replay-evaluation-confusion-list">
          {items.map((item, index) => (
            <div key={`${item.actual}-${item.predicted}-${index}`}>
              <span>{formatEventLabel(item.actual)}{" -> "}{formatEventLabel(item.predicted)}</span>
              <strong>{compactNumber(item.count)}</strong>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function EventLabelMetricsTable({ items }: { items: NonNullable<SessionEvaluation["event_label_metrics"]> }) {
  return (
    <div className="replay-evaluation-table-block">
      <div className="replay-section-title">Event Label Metrics</div>
      {items.length === 0 ? (
        <p className="replay-evaluation-table-empty">No per-label metrics returned.</p>
      ) : (
        <div className="replay-evaluation-label-table" role="table" aria-label="Event label metrics">
          <div role="row" className="replay-evaluation-label-table-head">
            <span role="columnheader">Label</span>
            <span role="columnheader">Support</span>
            <span role="columnheader">Pred.</span>
            <span role="columnheader">TP</span>
            <span role="columnheader">FP</span>
            <span role="columnheader">FN</span>
            <span role="columnheader">P</span>
            <span role="columnheader">R</span>
            <span role="columnheader">F1</span>
          </div>
          {items.map((item) => (
            <div role="row" key={item.label || "unknown"}>
              <strong role="cell">{formatEventLabel(item.label)}</strong>
              <span role="cell">{compactNumber(item.support)}</span>
              <span role="cell">{compactNumber(item.predicted_count)}</span>
              <span role="cell">{compactNumber(item.true_positive)}</span>
              <span role="cell">{compactNumber(item.false_positive)}</span>
              <span role="cell">{compactNumber(item.false_negative)}</span>
              <span role="cell">{formatPercentValue(item.precision)}</span>
              <span role="cell">{formatPercentValue(item.recall)}</span>
              <span role="cell">{formatPercentValue(item.f1)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ReactionTypeMetricsTable({ items }: { items: NonNullable<SessionEvaluation["reaction_type_metrics"]> }) {
  if (items.length === 0) return null;
  return (
    <div className="replay-evaluation-table-block">
      <div className="replay-section-title">Reaction Type Metrics</div>
      <div className="replay-evaluation-label-table" role="table" aria-label="Reaction type metrics">
        <div role="row" className="replay-evaluation-label-table-head">
          <span role="columnheader">Type</span>
          <span role="columnheader">Support</span>
          <span role="columnheader">Pred.</span>
          <span role="columnheader">TP</span>
          <span role="columnheader">FP</span>
          <span role="columnheader">FN</span>
          <span role="columnheader">P</span>
          <span role="columnheader">R</span>
          <span role="columnheader">F1</span>
        </div>
        {items.map((item) => (
          <div role="row" key={item.label || "unknown"}>
            <strong role="cell">{formatEventLabel(item.label)}</strong>
            <span role="cell">{compactNumber(item.support)}</span>
            <span role="cell">{compactNumber(item.predicted_count)}</span>
            <span role="cell">{compactNumber(item.true_positive)}</span>
            <span role="cell">{compactNumber(item.false_positive)}</span>
            <span role="cell">{compactNumber(item.false_negative)}</span>
            <span role="cell">{formatPercentValue(item.precision)}</span>
            <span role="cell">{formatPercentValue(item.recall)}</span>
            <span role="cell">{formatPercentValue(item.f1)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function UnsupportedEvaluationMetrics({ items }: { items: NonNullable<SessionEvaluation["unsupported_metrics"]> }) {
  return (
    <div className="replay-evaluation-table-block">
      <div className="replay-section-title">Unsupported Metrics</div>
      {items.length === 0 ? (
        <p className="replay-evaluation-table-empty">No unsupported metrics reported.</p>
      ) : (
        <div className="replay-evaluation-unsupported-list">
          {items.map((item, index) => (
            <div key={`${item.name || "metric"}-${index}`}>
              <BarChart3 size={12} />
              <span>{formatMetricName(item.name)}</span>
              <strong>{item.reason || "Metric unavailable"}</strong>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function EvaluationMetrics({
  summary
}: {
  summary: {
    total: number;
    correct: number;
    wrong: number;
    uncertain: number;
    correctRate?: number;
  };
}) {
  return (
    <div className="replay-evaluation-summary-block">
      <div className="metric-label-wrapper"><div className="metric-label">Label Summary</div></div>
      <div className="replay-summary-grid">
        <div><span>Agreement</span><strong>{formatPercentValue(summary.correctRate)}</strong></div>
        <div><span>Labels</span><strong>{compactNumber(summary.total)}</strong></div>
        <div><span>Correct</span><strong>{compactNumber(summary.correct)}</strong></div>
        <div><span>Wrong</span><strong>{compactNumber(summary.wrong)}</strong></div>
        <div><span>Uncertain</span><strong>{compactNumber(summary.uncertain)}</strong></div>
      </div>
    </div>
  );
}

function summarizeEvaluations(labels: SignalWindowLabel[]) {
  const correct = labels.filter((label) => label.correctness === "correct").length;
  const wrong = labels.filter((label) => label.correctness === "wrong").length;
  const uncertain = labels.filter((label) => label.correctness === "uncertain").length;
  const scored = correct + wrong;
  return {
    total: labels.length,
    correct,
    wrong,
    uncertain,
    correctRate: scored > 0 ? correct / scored : undefined
  };
}

function replayEvaluationLabelFromPayload(
  sessionID: string,
  payload: ReplayEvaluationQueueSavePayload
): SaveReplayEvaluationLabel | undefined {
  const { draft, window } = payload;
  if (!sessionID || !window.start || !window.end || !draft.correctness || !draft.eventLabel) return undefined;
  return {
    session_id: sessionID,
    window_start: window.start,
    window_end: window.end,
    predicted_event: payload.predicted_event,
    predicted_relationship: payload.predicted_relationship || "",
    correctness: draft.correctness,
    event_label: draft.eventLabel,
    reaction_type: payload.reaction_type,
    target_type: payload.target_type,
    target_text: payload.target_text,
    divergence_type: payload.divergence_type,
    event_start: payload.event_start,
    event_peak: payload.event_peak,
    notes: draft.notes
  };
}

function formatPercentValue(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return `${Math.round(value * 100)}%`;
}

function formatMillisecondsValue(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return `${Math.round(value).toLocaleString()}ms`;
}

function compactNumber(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
  return Math.round(value).toLocaleString();
}

function formatEventLabel(value?: string) {
  if (!value) return "-";
  return value.replace(/_/g, " ");
}

function formatMetricName(value?: string) {
  if (!value) return "Metric";
  return value.replace(/_/g, " ");
}

function truncationSummary(truncations: ReplayProofTruncation[]) {
  if (truncations.length === 0) return "";
  return truncations
    .slice(0, 2)
    .map((item) => `${formatMetricName(item.source)} ${compactNumber(item.loaded_count)} / ${compactNumber(item.total_count)}`)
    .join(" / ");
}
