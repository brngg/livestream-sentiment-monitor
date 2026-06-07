import { useEffect, useMemo, useState } from "react";
import { Check, HelpCircle, Save, X } from "lucide-react";
import { formatPercent, formatSignedNumber, relationshipLabel } from "../utils";
import type { ReplayWindowPoint } from "./replayModel";
import {
  replayWindowDelta,
  replayWindowRelationship,
  replayWindowScore
} from "./replayModel";

export type ReplayCorrectness = "correct" | "wrong" | "uncertain";

export type ReplayEventLabel =
  | "hype_spike"
  | "frustration_spike"
  | "audience_shift"
  | "content_audience_divergence"
  | "none";

export type ReplayEvaluationDraft = {
  correctness?: ReplayCorrectness;
  eventLabel?: ReplayEventLabel;
  notes: string;
};

export type ReplayEvaluationSummary = ReplayEvaluationDraft & {
  evaluator?: string;
  updatedAt?: string;
};

export type ReplayEvaluationPrediction = {
  summary: string;
  eventLabel?: ReplayEventLabel;
  score?: number;
  confidence?: number;
  relationship?: string;
};

export function ReplayEvaluationControls({
  window,
  evaluation,
  onChange,
  onSubmit
}: {
  window: ReplayWindowPoint;
  evaluation?: ReplayEvaluationSummary;
  onChange?: (draft: ReplayEvaluationDraft, window: ReplayWindowPoint) => void;
  onSubmit?: (draft: ReplayEvaluationDraft, window: ReplayWindowPoint) => void;
}) {
  const prediction = useMemo(() => predictionForWindow(window), [window]);
  const [draft, setDraft] = useState<ReplayEvaluationDraft>(() => draftFromEvaluation(evaluation));
  const resolvedEventLabel = resolvedDraftEventLabel(draft, prediction.eventLabel);
  const saveBlockedReason = evaluationSaveBlockedReason(draft, prediction.eventLabel);

  useEffect(() => {
    const nextDraft = draftFromEvaluation(evaluation);
    if (nextDraft.correctness === "correct" && !nextDraft.eventLabel) {
      nextDraft.eventLabel = predictionForWindow(window).eventLabel;
    }
    setDraft(nextDraft);
  }, [evaluation?.correctness, evaluation?.eventLabel, evaluation?.notes, evaluation?.updatedAt, window.id]);

  const updateDraft = (patch: Partial<ReplayEvaluationDraft>) => {
    const next = { ...draft, ...patch };
    if (patch.correctness === "correct" && !next.eventLabel) {
      next.eventLabel = prediction.eventLabel;
    }
    if (
      patch.correctness
      && patch.correctness !== "correct"
      && draft.correctness === "correct"
      && draft.eventLabel === prediction.eventLabel
    ) {
      next.eventLabel = undefined;
    }
    setDraft(next);
    onChange?.(next, window);
  };

  const submitDraft = () => {
    if (!onSubmit || saveBlockedReason || !resolvedEventLabel) return;
    onSubmit({ ...draft, eventLabel: resolvedEventLabel }, window);
  };

  return (
    <section className="replay-evaluation">
      <div className="replay-eval-header">
        <div>
          <div className="replay-section-title">Human Evaluation</div>
          <p>{window.signalWindow ? "Stored SignalWindow label review" : "Replay window label review"}</p>
        </div>
        {onSubmit ? (
          <button
            className="replay-eval-save"
            type="button"
            onClick={submitDraft}
            disabled={Boolean(saveBlockedReason)}
            aria-label="Save replay evaluation label"
            title={saveBlockedReason || "Save evaluation"}
          >
            <Save size={13} />
          </button>
        ) : null}
      </div>

      <PredictionSummary prediction={prediction} />

      <ChoiceGroup
        label="Correctness"
        value={draft.correctness}
        options={correctnessOptions}
        onSelect={(correctness) => updateDraft({ correctness })}
      />

      <ChoiceGroup
        label="Event label"
        value={draft.eventLabel}
        options={eventLabelOptions}
        onSelect={(eventLabel) => updateDraft({ eventLabel })}
      />

      <p className={saveBlockedReason ? "replay-eval-hint warn" : "replay-eval-hint"}>
        {saveBlockedReason || (draft.correctness === "correct"
          ? `Correct saves the predicted event: ${formatEventLabel(prediction.eventLabel)}.`
          : `Event label selected: ${formatEventLabel(draft.eventLabel)}.`)}
      </p>

      <label className="replay-eval-notes">
        <span>Notes</span>
        <textarea
          value={draft.notes}
          onChange={(event) => updateDraft({ notes: event.target.value })}
          rows={3}
          placeholder="Add evaluator notes..."
        />
      </label>

      {evaluation ? <EvaluationSummary evaluation={evaluation} /> : null}
    </section>
  );
}

function PredictionSummary({ prediction }: { prediction: ReplayEvaluationPrediction }) {
  return (
    <div className="replay-eval-prediction">
      <div>
        <span>Prediction</span>
        <strong>{prediction.summary}</strong>
      </div>
      <div>
        <span>Event</span>
        <strong>{formatEventLabel(prediction.eventLabel)}</strong>
      </div>
      <div>
        <span>Score</span>
        <strong>{formatSignedNumber(prediction.score)}</strong>
      </div>
      <div>
        <span>Confidence</span>
        <strong>{formatOptionalPercent(prediction.confidence)}</strong>
      </div>
    </div>
  );
}

function formatOptionalPercent(value?: number) {
  return typeof value === "number" ? formatPercent(value) : "unknown";
}

function ChoiceGroup<T extends string>({
  label,
  value,
  options,
  onSelect
}: {
  label: string;
  value?: T;
  options: Array<{ value: T; label: string; icon?: "check" | "x" | "help" }>;
  onSelect: (value: T) => void;
}) {
  return (
    <div className="replay-eval-choice-group">
      <div className="replay-section-title">{label}</div>
      <div className="replay-eval-choices">
        {options.map((option) => (
          <button
            className={value === option.value ? "selected" : ""}
            key={option.value}
            type="button"
            onClick={() => onSelect(option.value)}
            aria-pressed={value === option.value}
          >
            {option.icon ? <ChoiceIcon icon={option.icon} /> : null}
            <span>{option.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
}

function ChoiceIcon({ icon }: { icon: "check" | "x" | "help" }) {
  if (icon === "check") return <Check size={12} />;
  if (icon === "x") return <X size={12} />;
  return <HelpCircle size={12} />;
}

function EvaluationSummary({ evaluation }: { evaluation: ReplayEvaluationSummary }) {
  const details = [
    evaluation.correctness ? `Correctness: ${evaluation.correctness}` : "",
    evaluation.eventLabel ? `Event: ${formatEventLabel(evaluation.eventLabel)}` : "",
    evaluation.evaluator ? `By: ${evaluation.evaluator}` : "",
    evaluation.updatedAt ? `Updated: ${formatEvaluationDate(evaluation.updatedAt)}` : ""
  ].filter(Boolean);

  return (
    <div className="replay-eval-summary">
      <div className="replay-section-title">Evaluation Summary</div>
      {details.length > 0 ? <p>{details.join(" / ")}</p> : <p>No saved label metadata.</p>}
      {evaluation.notes ? <blockquote>{evaluation.notes}</blockquote> : null}
    </div>
  );
}

function predictionForWindow(window: ReplayWindowPoint): ReplayEvaluationPrediction {
  const relationship = relationshipLabel(replayWindowRelationship(window));
  const eventLabel = predictedEventLabel(window);
  const score = replayWindowScore(window);

  return {
    summary: relationship.label,
    eventLabel,
    score,
    confidence: window.signalWindow?.confidence,
    relationship: relationship.label
  };
}

function predictedEventLabel(window: ReplayWindowPoint): ReplayEventLabel {
  const detectedLabel = window.events
    .map((event) => normalizeEventLabel(event.label || event.type || event.source))
    .find((label): label is ReplayEventLabel => Boolean(label));

  if (detectedLabel) return detectedLabel;

  const relationship = replayWindowRelationship(window);
  const delta = Math.abs(replayWindowDelta(window) || 0);
  const score = replayWindowScore(window) || 0;

  if (relationship === "diverged" || delta >= 0.45) return "content_audience_divergence";
  if (score >= 0.35) return "hype_spike";
  if (score <= -0.35) return "frustration_spike";
  return "none";
}

function normalizeEventLabel(value?: string): ReplayEventLabel | undefined {
  if (!value) return undefined;
  const normalized = value.toLowerCase().replace(/[\s-]+/g, "_");
  return eventLabelOptions.some((option) => option.value === normalized)
    ? normalized as ReplayEventLabel
    : undefined;
}

function draftFromEvaluation(evaluation?: ReplayEvaluationSummary): ReplayEvaluationDraft {
  return {
    correctness: evaluation?.correctness,
    eventLabel: evaluation?.eventLabel,
    notes: evaluation?.notes || ""
  };
}

function formatEventLabel(value?: ReplayEventLabel) {
  return value ? value.replace(/_/g, " ") : "none";
}

function resolvedDraftEventLabel(
  draft: ReplayEvaluationDraft,
  predictedEvent?: ReplayEventLabel
): ReplayEventLabel | undefined {
  if (draft.eventLabel) return draft.eventLabel;
  if (draft.correctness === "correct") return predictedEvent;
  return undefined;
}

function evaluationSaveBlockedReason(
  draft: ReplayEvaluationDraft,
  predictedEvent?: ReplayEventLabel
) {
  if (!draft.correctness) return "Select correctness before saving.";
  if (!resolvedDraftEventLabel(draft, predictedEvent)) return "Select an event label before saving.";
  return "";
}

function formatEvaluationDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString([], { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

const correctnessOptions: Array<{ value: ReplayCorrectness; label: string; icon: "check" | "x" | "help" }> = [
  { value: "correct", label: "Correct", icon: "check" },
  { value: "wrong", label: "Wrong", icon: "x" },
  { value: "uncertain", label: "Uncertain", icon: "help" }
];

const eventLabelOptions: Array<{ value: ReplayEventLabel; label: string }> = [
  { value: "hype_spike", label: "Hype spike" },
  { value: "frustration_spike", label: "Frustration spike" },
  { value: "audience_shift", label: "Audience shift" },
  { value: "content_audience_divergence", label: "Content divergence" },
  { value: "none", label: "None" }
];
