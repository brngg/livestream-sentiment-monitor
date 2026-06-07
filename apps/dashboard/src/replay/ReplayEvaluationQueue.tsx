import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { Check, ChevronsRight, HelpCircle, ListFilter, Save, X } from "lucide-react";
import type { ChatMessage, EvaluationAgentReview, MessageScore, SignalWindowLabel, TranscriptBucket } from "../types";
import { formatList, formatPercent, formatSignedNumber, relationshipLabel } from "../utils";
import type { ReplayWindowPoint, TranscriptContextBucket } from "./replayModel";
import {
  replayWindowChatSentiment,
  replayWindowDelta,
  replayWindowMessageCount,
  replayWindowQuality,
  replayWindowRelationship,
  resolveReplayEvidenceRefs,
  replayWindowScore,
  replayWindowTranscriptSentiment
} from "./replayModel";
import type { ReplayCorrectness, ReplayEventLabel } from "./ReplayEvaluationControls";

export type ReplayEvaluationQueueMode = "unlabeled" | "agent_suggested" | "detected" | "low_confidence" | "all";

export type ReplayEvaluationQueueDraft = {
  correctness?: ReplayCorrectness;
  eventLabel?: ReplayEventLabel;
  reactionType?: string;
  targetType?: string;
  targetText?: string;
  divergenceType?: string;
  eventStart?: string;
  eventPeak?: string;
  notes: string;
};

export type ReplayEvaluationQueueSavePayload = {
  window: ReplayWindowPoint;
  draft: ReplayEvaluationQueueDraft;
  predicted_event: ReplayEventLabel;
  predicted_relationship?: string;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  divergence_type?: string;
  event_start?: string;
  event_peak?: string;
  window_start?: string;
  window_end?: string;
  existingLabel?: SignalWindowLabel;
};

export type ReplayEvaluationQueueProps = {
  windows: ReplayWindowPoint[];
  windowLabels: SignalWindowLabel[];
  agentReviews?: EvaluationAgentReview[];
  selectedWindow?: ReplayWindowPoint;
  mode?: ReplayEvaluationQueueMode;
  lowConfidenceThreshold?: number;
  onModeChange?: (mode: ReplayEvaluationQueueMode) => void;
  onSelectedWindowChange?: (window: ReplayWindowPoint | undefined, index: number) => void;
  onDraftChange?: (draft: ReplayEvaluationQueueDraft, window: ReplayWindowPoint) => void;
  onSaveLabel?: (payload: ReplayEvaluationQueueSavePayload) => void | Promise<unknown>;
};

export function ReplayEvaluationQueue({
  windows,
  windowLabels,
  agentReviews = [],
  selectedWindow,
  mode = "unlabeled",
  lowConfidenceThreshold = 0.65,
  onModeChange,
  onSelectedWindowChange,
  onDraftChange,
  onSaveLabel
}: ReplayEvaluationQueueProps) {
  const labelLookup = useMemo(() => buildWindowLabelLookup(windowLabels), [windowLabels]);
  const agentReviewLookup = useMemo(() => buildAgentReviewLookup(agentReviews), [agentReviews]);
  const queueWindows = useMemo(
    () => filterReplayEvaluationQueueWindows(windows, labelLookup, mode, lowConfidenceThreshold, agentReviewLookup),
    [windows, labelLookup, mode, lowConfidenceThreshold, agentReviewLookup]
  );
  const activeWindow = selectedWindow && queueWindows.some((window) => sameReplayWindow(window, selectedWindow))
    ? selectedWindow
    : queueWindows[0];
  const activeIndex = activeWindow ? queueWindows.findIndex((window) => sameReplayWindow(window, activeWindow)) : -1;
  const existingLabel = useMemo(() => labelForWindow(activeWindow, labelLookup), [activeWindow, labelLookup]);
  const agentReview = useMemo(() => agentReviewForWindow(activeWindow, agentReviewLookup), [activeWindow, agentReviewLookup]);
  const [draft, setDraft] = useState<ReplayEvaluationQueueDraft>(() => draftForWindow(activeWindow, existingLabel));
  const [saving, setSaving] = useState(false);
  const summary = useMemo(() => queueSummary(windows, labelLookup, queueWindows), [windows, labelLookup, queueWindows]);
  const predictedEvent = activeWindow ? predictedEventLabel(activeWindow) : undefined;
  const saveBlockedReason = activeWindow ? evaluationSaveBlockedReason(draft, predictedEvent) : "No replay window selected.";

  useEffect(() => {
    const nextDraft = draftForWindow(activeWindow, existingLabel);
    if (activeWindow && nextDraft.correctness === "correct" && !nextDraft.eventLabel) {
      nextDraft.eventLabel = predictedEventLabel(activeWindow);
    }
    setDraft(nextDraft);
  }, [
    activeWindow?.id,
    existingLabel?.correctness,
    existingLabel?.event_label,
    existingLabel?.reaction_type,
    existingLabel?.target_type,
    existingLabel?.target_text,
    existingLabel?.divergence_type,
    existingLabel?.event_start,
    existingLabel?.event_peak,
    existingLabel?.notes,
    existingLabel?.updated_at
  ]);

  const updateDraft = (patch: Partial<ReplayEvaluationQueueDraft>) => {
    if (!activeWindow) return;
    const predictedEventLabelForWindow = predictedEventLabel(activeWindow);
    const next = { ...draft, ...patch };
    if (patch.correctness === "correct" && !next.eventLabel) {
      next.eventLabel = predictedEventLabelForWindow;
    }
    if (
      patch.correctness
      && patch.correctness !== "correct"
      && draft.correctness === "correct"
      && draft.eventLabel === predictedEventLabelForWindow
    ) {
      next.eventLabel = undefined;
    }
    setDraft(next);
    onDraftChange?.(next, activeWindow);
  };

  const selectWindow = (window: ReplayWindowPoint | undefined) => {
    const index = window ? windows.findIndex((candidate) => sameReplayWindow(candidate, window)) : -1;
    onSelectedWindowChange?.(window, index);
  };

  const saveAndNext = async () => {
    if (!activeWindow || !onSaveLabel) return;
    if (saveBlockedReason) return;
    const payload = savePayloadForWindow(activeWindow, draft, existingLabel);
    setSaving(true);
    try {
      await onSaveLabel(payload);
      selectWindow(nextQueueWindow(queueWindows, activeIndex));
    } finally {
      setSaving(false);
    }
  };

  if (windows.length === 0) {
    return (
      <section className="replay-evaluation-queue-card">
        <QueueHeader mode={mode} onModeChange={onModeChange} />
        <p className="replay-evaluation-queue-empty">No replay windows are available for evaluation.</p>
      </section>
    );
  }

  return (
    <section className="replay-evaluation-queue-card">
      <QueueHeader mode={mode} onModeChange={onModeChange} />

      <div className="replay-evaluation-queue-progress">
        <div>
          <span>Progress</span>
          <strong>{summary.labeled} / {summary.total} labeled</strong>
        </div>
        <div>
          <span>Queue</span>
          <strong>{queueWindows.length === 0 ? "0 queued" : `${activeIndex + 1} / ${queueWindows.length}`}</strong>
        </div>
        <div>
          <span>Coverage</span>
          <strong>{formatPercent(summary.coverage)}</strong>
        </div>
      </div>

      {activeWindow ? (
        <>
          <WindowReviewCard window={activeWindow} label={existingLabel} />
          <AgentSuggestionCard
            review={agentReview}
            onApply={(review) => updateDraft(draftPatchFromAgentReview(review, draft, activeWindow))}
          />
          <DraftControls
            draft={draft}
            predictedEvent={predictedEvent}
            saveBlockedReason={saveBlockedReason}
            onChange={updateDraft}
          />
          <div className="replay-evaluation-queue-actions">
            <button type="button" onClick={() => selectWindow(previousQueueWindow(queueWindows, activeIndex))} disabled={activeIndex <= 0}>
              Previous
            </button>
            <button type="button" onClick={saveAndNext} disabled={!onSaveLabel || saving || Boolean(saveBlockedReason)}>
              <Save size={13} />
              <span>{saving ? "Saving" : "Save + Next"}</span>
            </button>
            <button type="button" onClick={() => selectWindow(nextQueueWindow(queueWindows, activeIndex))} disabled={activeIndex < 0 || activeIndex >= queueWindows.length - 1}>
              <ChevronsRight size={13} />
              <span>Next</span>
            </button>
          </div>
        </>
      ) : (
        <p className="replay-evaluation-queue-empty">No windows match this queue mode.</p>
      )}
    </section>
  );
}

export function filterReplayEvaluationQueueWindows(
  windows: ReplayWindowPoint[],
  labels: WindowLabelLookup,
  mode: ReplayEvaluationQueueMode,
  lowConfidenceThreshold = 0.65,
  agentReviews: AgentReviewLookup = emptyAgentReviewLookup()
) {
  return windows.filter((window) => {
    if (mode === "all") return true;
    if (mode === "agent_suggested") return Boolean(agentReviewForWindow(window, agentReviews));
    if (mode === "unlabeled") return !labelForWindow(window, labels);
    if (mode === "detected") return detectedLabelsForWindow(window).length > 0 || predictedEventLabel(window) !== "none";
    const confidence = replayWindowQuality(window);
    return typeof confidence === "number" && confidence < lowConfidenceThreshold;
  });
}

function QueueHeader({
  mode,
  onModeChange
}: {
  mode: ReplayEvaluationQueueMode;
  onModeChange?: (mode: ReplayEvaluationQueueMode) => void;
}) {
  return (
    <div className="replay-evaluation-queue-header">
      <div>
        <div className="replay-section-title">Evaluation Queue</div>
        <p>Review stored replay windows with synchronized chat and transcript evidence.</p>
      </div>
      <div className="replay-evaluation-queue-modes" role="group" aria-label="Evaluation queue mode">
        {queueModes.map((option) => (
          <button
            className={mode === option.value ? "selected" : ""}
            key={option.value}
            type="button"
            onClick={() => onModeChange?.(option.value)}
            aria-pressed={mode === option.value}
          >
            <ListFilter size={12} />
            <span>{option.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
}

function WindowReviewCard({ window, label }: { window: ReplayWindowPoint; label?: SignalWindowLabel }) {
  const relationship = relationshipLabel(replayWindowRelationship(window));
  const predictedEvent = predictedEventLabel(window);
  const chatBucket = window.chatBucket;
  const transcriptBucket = window.transcriptBucket;
  const detectedLabels = detectedLabelsForWindow(window);
  const context = windowContext(window);
  const evidenceRefs = resolveReplayEvidenceRefs(window, context.evidenceIDs || []);
  const messages = representativeMessages(
    chatBucket?.message_scores || [],
    chatBucket?.peak_evidence_messages || [],
    context.evidenceIDs,
    window
  );

  return (
    <div className="replay-evaluation-queue-window">
      <div className="replay-evaluation-queue-window-header">
        <div>
          <span>{window.source.toUpperCase()} / {windowRangeLabel(window.start, window.end)}</span>
          <strong>{relationship.label} / {formatEventLabel(predictedEvent)}</strong>
        </div>
        <div>
          <span>{label ? "Saved label" : "Unlabeled"}</span>
          <strong>{label ? `${label.correctness} / ${formatEventLabel(normalizeEventLabel(label.event_label) || "none")}` : "Needs review"}</strong>
        </div>
      </div>

      <div className="replay-evaluation-queue-metrics">
        <Metric label="Prediction" value={relationship.label} />
        <Metric label="Score" value={formatSignedNumber(replayWindowScore(window))} />
        <Metric label="Signal confidence" value={formatOptionalPercent(context.confidence)} />
        <Metric label="Reaction" value={knownText(context.reactionType)} />
        <Metric label="Possible target" value={targetContextLabel(context.targetType, context.targetText)} wide />
        <Metric label="Hint" value={knownText(context.eventHint)} wide />
        <Metric label="Evidence refs" value={evidenceRefsLabel(evidenceRefs)} />
        <Metric label="Chat" value={formatSignedNumber(replayWindowChatSentiment(window))} />
        <Metric label="Transcript" value={formatSignedNumber(replayWindowTranscriptSentiment(window))} />
        <Metric label="Delta" value={formatSignedNumber(replayWindowDelta(window))} />
        <Metric label="Messages" value={replayWindowMessageCount(window).toLocaleString()} />
        <Metric label="Unique chatters" value={formatInteger(chatBucket?.unique_chatters ?? window.signalWindow?.unique_chatters)} />
        <Metric label="Top terms" value={formatList(chatBucket?.top_terms?.slice(0, 6))} wide />
        <Metric label="Top emotes" value={formatList(chatBucket?.top_emotes?.slice(0, 6))} wide />
      </div>

      <EvidenceSection title="Detected labels">
        {detectedLabels.length > 0 ? (
          <div className="replay-evaluation-queue-labels">
            {detectedLabels.map((labelValue) => <span key={labelValue}>{labelValue}</span>)}
          </div>
        ) : (
          <p>No detected labels are attached to this window.</p>
        )}
      </EvidenceSection>

      <EvidenceSection title="Representative chat messages">
        {messages.length > 0 ? (
          <div className="replay-evaluation-queue-messages">
            {messages.map((message, index) => (
              <div key={message.message_id || `${message.timestamp || "message"}-${index}`}>
                <span>{compactTime(message.timestamp)} / {message.display_name || message.username || "unknown"} / {formatSignedNumber(message.sentiment_score)}</span>
                <p>{message.text || "No message text stored."}</p>
              </div>
            ))}
          </div>
        ) : (
          <p>No representative chat messages are available.</p>
        )}
      </EvidenceSection>

      <EvidenceSection title="Transcript context">
        {window.transcriptContext.length > 0 ? (
          <div className="replay-evaluation-queue-transcript">
            {window.transcriptContext.map((context) => <TranscriptContext context={context} key={`${context.role}-${context.bucket.bucket_start || ""}`} />)}
          </div>
        ) : (
          <p>{transcriptText(transcriptBucket) || "No transcript context is available."}</p>
        )}
      </EvidenceSection>
    </div>
  );
}

function AgentSuggestionCard({
  review,
  onApply
}: {
  review?: EvaluationAgentReview;
  onApply: (review: EvaluationAgentReview) => void;
}) {
  if (!review) {
    return (
      <section className="replay-evaluation-agent-card empty">
        <div className="replay-section-title">Agent suggestion</div>
        <p>No agent pre-score is attached to this window.</p>
      </section>
    );
  }

  const label = normalizeEventLabel(review.suggested_event_label) || "none";
  const evidence = review.evidence || [];

  return (
    <section className="replay-evaluation-agent-card">
      <div className="replay-evaluation-agent-header">
        <div>
          <div className="replay-section-title">Agent suggestion</div>
          <p>{review.reviewer || "agent"} / {review.status || "suggested"}</p>
        </div>
        <button type="button" onClick={() => onApply(review)}>Apply to draft</button>
      </div>

      <div className="replay-evaluation-agent-metrics">
        <Metric label="Suggested label" value={formatEventLabel(label)} />
        <Metric label="Correctness" value={knownText(review.correctness)} />
        <Metric label="Confidence" value={formatOptionalPercent(review.confidence)} />
        <Metric label="Usefulness" value={formatOptionalPercent(review.streamer_usefulness)} />
        <Metric label="Reaction" value={knownText(review.reaction_type)} />
        <Metric label="Target" value={targetContextLabel(review.target_type, review.target_text)} wide />
      </div>

      {review.reason ? <p className="replay-evaluation-agent-reason">{review.reason}</p> : null}

      {evidence.length > 0 ? (
        <div className="replay-evaluation-agent-evidence">
          {evidence.slice(0, 4).map((item, index) => (
            <div key={item.id || `${item.source || "evidence"}-${index}`}>
              <span>{item.source || "evidence"}{item.timestamp ? ` / ${compactTime(item.timestamp)}` : ""}</span>
              <p>{item.text || item.id || "Evidence reference"}</p>
            </div>
          ))}
        </div>
      ) : null}
    </section>
  );
}

function DraftControls({
  draft,
  predictedEvent,
  saveBlockedReason,
  onChange
}: {
  draft: ReplayEvaluationQueueDraft;
  predictedEvent?: ReplayEventLabel;
  saveBlockedReason?: string;
  onChange: (patch: Partial<ReplayEvaluationQueueDraft>) => void;
}) {
  const hint = draft.correctness === "correct"
    ? `Correct saves the predicted event: ${formatEventLabel(predictedEvent)}.`
    : draft.eventLabel
      ? `Event label selected: ${formatEventLabel(draft.eventLabel)}.`
      : "Select an event label before saving; use None only when no event occurred.";

  return (
    <div className="replay-evaluation-queue-draft">
      <ChoiceGroup label="Correctness" value={draft.correctness} options={correctnessOptions} onSelect={(correctness) => onChange({ correctness })} />
      <ChoiceGroup label="Event label" value={draft.eventLabel} options={eventLabelOptions} onSelect={(eventLabel) => onChange({ eventLabel })} />
      <LabelPayloadControls draft={draft} onChange={onChange} />
      <p className={saveBlockedReason ? "replay-evaluation-queue-hint warn" : "replay-evaluation-queue-hint"}>{saveBlockedReason || hint}</p>
      <label>
        <span>Notes</span>
        <textarea value={draft.notes} rows={3} onChange={(event) => onChange({ notes: event.target.value })} placeholder="Add evaluator notes..." />
      </label>
    </div>
  );
}

function LabelPayloadControls({
  draft,
  onChange
}: {
  draft: ReplayEvaluationQueueDraft;
  onChange: (patch: Partial<ReplayEvaluationQueueDraft>) => void;
}) {
  return (
    <div className="replay-evaluation-queue-choice-group">
      <div className="replay-section-title">Label context</div>
      <label>
        <span>Reaction type</span>
        <input value={draft.reactionType || ""} onChange={(event) => onChange({ reactionType: event.target.value })} placeholder="hype, frustration, confusion..." />
      </label>
      <label>
        <span>Target type</span>
        <input value={draft.targetType || ""} onChange={(event) => onChange({ targetType: event.target.value })} placeholder="gameplay, creator, unknown..." />
      </label>
      <label>
        <span>Target text</span>
        <textarea value={draft.targetText || ""} rows={2} onChange={(event) => onChange({ targetText: event.target.value })} placeholder="What the reaction was about..." />
      </label>
      <label>
        <span>Divergence type</span>
        <input value={draft.divergenceType || ""} onChange={(event) => onChange({ divergenceType: event.target.value })} placeholder="converged, diverged, soft_split..." />
      </label>
      <label>
        <span>Event start</span>
        <input value={draft.eventStart || ""} onChange={(event) => onChange({ eventStart: event.target.value })} placeholder="ISO timestamp" />
      </label>
      <label>
        <span>Event peak</span>
        <input value={draft.eventPeak || ""} onChange={(event) => onChange({ eventPeak: event.target.value })} placeholder="ISO timestamp" />
      </label>
    </div>
  );
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
    <div className="replay-evaluation-queue-choice-group">
      <div className="replay-section-title">{label}</div>
      <div>
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

function EvidenceSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="replay-evaluation-queue-section">
      <div className="replay-section-title">{title}</div>
      {children}
    </section>
  );
}

function TranscriptContext({ context }: { context: TranscriptContextBucket }) {
  return (
    <div className={`replay-evaluation-queue-transcript-row ${context.role}`}>
      <div>
        <span>{context.role}</span>
        <strong>{windowRangeLabel(context.bucket.bucket_start, context.bucket.bucket_end)}</strong>
        <em>{formatOptionalPercent(context.bucket.sentiment_confidence ?? context.bucket.transcript_confidence)}</em>
      </div>
      <p>{transcriptText(context.bucket) || "No transcript text stored."}</p>
    </div>
  );
}

function Metric({ label, value, wide = false }: { label: string; value: string; wide?: boolean }) {
  return (
    <div className={wide ? "wide" : ""}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

type WindowLabelLookup = Map<string, SignalWindowLabel>;

type AgentReviewLookup = {
  byRange: Map<string, EvaluationAgentReview>;
  bySourceRange: Map<string, EvaluationAgentReview>;
};

function queueSummary(windows: ReplayWindowPoint[], labels: WindowLabelLookup, queueWindows: ReplayWindowPoint[]) {
  const labeled = windows.filter((window) => labelForWindow(window, labels)).length;
  return {
    total: windows.length,
    labeled,
    queued: queueWindows.length,
    coverage: windows.length > 0 ? labeled / windows.length : undefined
  };
}

function savePayloadForWindow(
  window: ReplayWindowPoint,
  draft: ReplayEvaluationQueueDraft,
  existingLabel?: SignalWindowLabel
): ReplayEvaluationQueueSavePayload {
  const predictedEvent = predictedEventLabel(window);
  const eventLabel = resolvedDraftEventLabel(draft, predictedEvent);

  return {
    window,
    draft: { ...draft, eventLabel },
    predicted_event: predictedEvent,
    predicted_relationship: replayWindowRelationship(window),
    reaction_type: cleanLabelText(draft.reactionType),
    target_type: cleanLabelText(draft.targetType),
    target_text: cleanLabelText(draft.targetText),
    divergence_type: cleanLabelText(draft.divergenceType),
    event_start: cleanLabelText(draft.eventStart),
    event_peak: cleanLabelText(draft.eventPeak),
    window_start: window.start,
    window_end: window.end,
    existingLabel
  };
}

function nextQueueWindow(queueWindows: ReplayWindowPoint[], activeIndex: number) {
  if (activeIndex < 0) return queueWindows[0];
  return queueWindows[Math.min(queueWindows.length - 1, activeIndex + 1)];
}

function previousQueueWindow(queueWindows: ReplayWindowPoint[], activeIndex: number) {
  if (activeIndex < 0) return queueWindows[0];
  return queueWindows[Math.max(0, activeIndex - 1)];
}

function buildWindowLabelLookup(labels: SignalWindowLabel[]): WindowLabelLookup {
  const lookup: WindowLabelLookup = new Map();
  labels.forEach((label) => {
    const key = windowRangeKey(label.window_start, label.window_end);
    if (key) lookup.set(key, label);
  });
  return lookup;
}

function labelForWindow(window: ReplayWindowPoint | undefined, labels: WindowLabelLookup) {
  if (!window) return undefined;
  return labels.get(windowRangeKey(window.start, window.end));
}

function buildAgentReviewLookup(reviews: EvaluationAgentReview[]): AgentReviewLookup {
  const lookup = emptyAgentReviewLookup();
  reviews.forEach((review) => {
    const rangeKey = windowRangeKey(review.window_start, review.window_end);
    if (!rangeKey) return;
    setBestAgentReview(lookup.byRange, rangeKey, review);
    const sourceKey = agentReviewSourceKey(review.source_window_type);
    if (sourceKey) setBestAgentReview(lookup.bySourceRange, `${sourceKey}|${rangeKey}`, review);
  });
  return lookup;
}

function emptyAgentReviewLookup(): AgentReviewLookup {
  return {
    byRange: new Map(),
    bySourceRange: new Map()
  };
}

function setBestAgentReview(lookup: Map<string, EvaluationAgentReview>, key: string, review: EvaluationAgentReview) {
  const existing = lookup.get(key);
  if (!existing || (review.confidence || 0) > (existing.confidence || 0)) {
    lookup.set(key, review);
  }
}

function agentReviewForWindow(window: ReplayWindowPoint | undefined, reviews: AgentReviewLookup) {
  if (!window) return undefined;
  const rangeKey = windowRangeKey(window.start, window.end);
  if (!rangeKey) return undefined;
  const sourceKey = agentReviewSourceKeyForWindow(window);
  return reviews.bySourceRange.get(`${sourceKey}|${rangeKey}`) || reviews.byRange.get(rangeKey);
}

function agentReviewSourceKey(sourceWindowType?: string) {
  const source = (sourceWindowType || "").toLowerCase();
  if (!source) return "";
  if (source.includes("reaction")) return "reaction";
  if (source.includes("signal")) return "signal";
  if (source.includes("transcript")) return "transcript";
  if (source.includes("alignment")) return "alignment";
  if (source.includes("chat") || source.includes("bucket")) return "chat";
  return source;
}

function agentReviewSourceKeyForWindow(window: ReplayWindowPoint) {
  if (window.source === "alignment") return "alignment";
  if (window.source === "reaction") return "reaction";
  if (window.source === "signal") return "signal";
  if (window.source === "transcript") return "transcript";
  return "chat";
}

function draftPatchFromAgentReview(
  review: EvaluationAgentReview,
  draft: ReplayEvaluationQueueDraft,
  window: ReplayWindowPoint
): Partial<ReplayEvaluationQueueDraft> {
  const suggested = normalizeEventLabel(review.suggested_event_label) || "none";
  const predicted = predictedEventLabel(window);
  const correctness = normalizeCorrectness(review.correctness)
    || (suggested === predicted ? "correct" : "wrong");

  return {
    correctness,
    eventLabel: suggested,
    reactionType: review.reaction_type || draft.reactionType || "",
    targetType: review.target_type || draft.targetType || "",
    targetText: review.target_text || draft.targetText || "",
    divergenceType: review.divergence_type || draft.divergenceType || "",
    eventStart: review.event_start || draft.eventStart || "",
    eventPeak: review.event_peak || draft.eventPeak || "",
    notes: appendAgentReason(draft.notes, review)
  };
}

function appendAgentReason(notes: string, review: EvaluationAgentReview) {
  const reason = review.reason?.trim();
  if (!reason) return notes;
  const prefix = `Agent reason (${review.reviewer || "agent"}, ${formatOptionalPercent(review.confidence)} confidence)`;
  const addition = `${prefix}: ${reason}`;
  const current = notes.trim();
  if (!current) return addition;
  if (current.includes(reason)) return current;
  return `${current}\n\n${addition}`;
}

function draftForWindow(window?: ReplayWindowPoint, label?: SignalWindowLabel): ReplayEvaluationQueueDraft {
  const context = window ? windowContext(window) : undefined;
  return {
    correctness: normalizeCorrectness(label?.correctness),
    eventLabel: normalizeEventLabel(label?.event_label),
    reactionType: label?.reaction_type || context?.reactionType || "",
    targetType: label?.target_type || context?.targetType || "",
    targetText: label?.target_text || context?.targetText || "",
    divergenceType: label?.divergence_type || context?.divergenceType || "",
    eventStart: label?.event_start || context?.eventStart || "",
    eventPeak: label?.event_peak || context?.eventPeak || "",
    notes: label?.notes || ""
  };
}

function predictedEventLabel(window: ReplayWindowPoint): ReplayEventLabel {
  const context = windowContext(window);
  const reactionLabel = eventLabelForReaction(context.reactionType, context.eventHint);
  if (reactionLabel) return reactionLabel;

  const detected = detectedLabelsForWindow(window)
    .map(normalizeEventLabel)
    .find((label): label is ReplayEventLabel => Boolean(label));
  if (detected) return detected;

  const relationship = replayWindowRelationship(window);
  const delta = Math.abs(replayWindowDelta(window) || 0);
  const score = replayWindowScore(window) || 0;
  if (relationship === "diverged" || delta >= 0.45) return "content_audience_divergence";
  if (score >= 0.35) return "hype_spike";
  if (score <= -0.35) return "frustration_spike";
  return "none";
}

function resolvedDraftEventLabel(
  draft: ReplayEvaluationQueueDraft,
  predictedEvent?: ReplayEventLabel
): ReplayEventLabel | undefined {
  if (draft.eventLabel) return draft.eventLabel;
  if (draft.correctness === "correct") return predictedEvent;
  return undefined;
}

function evaluationSaveBlockedReason(
  draft: ReplayEvaluationQueueDraft,
  predictedEvent?: ReplayEventLabel
) {
  if (!draft.correctness) return "Select correctness before saving.";
  if (!resolvedDraftEventLabel(draft, predictedEvent)) return "Select an event label before saving.";
  return "";
}

function detectedLabelsForWindow(window: ReplayWindowPoint) {
  const labels = [
    window.signalWindow?.first_event_type,
    window.signalWindow?.reaction_type,
    window.reactionWindow?.reaction_type,
    window.signalWindow?.target_type && window.signalWindow.target_type !== "unknown" ? `target:${window.signalWindow.target_type}` : undefined,
    window.reactionWindow?.target_type && window.reactionWindow.target_type !== "unknown" ? `target:${window.reactionWindow.target_type}` : undefined,
    window.reactionWindow?.event_hint,
    ...window.events.map((event) => event.reaction_type || event.label || event.type || event.source)
  ].filter((label): label is string => Boolean(label));
  return Array.from(new Set(labels)).slice(0, 8);
}

function representativeMessages(
  messages: MessageScore[],
  peakEvidenceMessages: ChatMessage[],
  evidenceIDs: string[] | undefined,
  window: ReplayWindowPoint
) {
  const evidence = new Set((evidenceIDs || []).filter(Boolean));
  const candidates = messages.filter((message) => {
    if (evidence.size > 0 && message.message_id) return evidence.has(message.message_id);
    return isTimestampInWindow(message.timestamp || "", window);
  });
  const sourceMessages = candidates.length > 0
    ? candidates
    : peakEvidenceMessages.length > 0
      ? peakEvidenceMessages.map(messageScoreFromChatMessage)
      : messages;
  return sourceMessages
    .slice()
    .sort((first, second) => Math.abs(second.sentiment_score || 0) - Math.abs(first.sentiment_score || 0))
    .slice(0, 5);
}

function messageScoreFromChatMessage(message: ChatMessage): MessageScore {
  return {
    message_id: message.message_id,
    timestamp: message.timestamp,
    username: message.username,
    display_name: message.display_name,
    text: message.text
  };
}

function transcriptText(bucket?: TranscriptBucket) {
  if (!bucket) return "";
  if (bucket.text?.trim()) return bucket.text.trim();
  return (bucket.segments || []).map((segment) => segment.text).filter(Boolean).join(" ").trim();
}

function normalizeCorrectness(value?: string): ReplayCorrectness | undefined {
  if (value === "correct" || value === "wrong" || value === "uncertain") return value;
  return undefined;
}

function normalizeEventLabel(value?: string): ReplayEventLabel | undefined {
  if (!value) return undefined;
  const normalized = value.toLowerCase().replace(/[\s-]+/g, "_");
  return eventLabelOptions.some((option) => option.value === normalized) ? normalized as ReplayEventLabel : undefined;
}

function formatEventLabel(value?: ReplayEventLabel) {
  return value ? value.replace(/_/g, " ") : "none";
}

function formatInteger(value?: number) {
  return typeof value === "number" && Number.isFinite(value) ? Math.round(value).toLocaleString() : "-";
}

function windowContext(window: ReplayWindowPoint) {
  const event = window.events.find((candidate) => (
    candidate.reaction_type
    || candidate.target_type
    || candidate.target_text
    || candidate.event_hint
    || candidate.confidence
    || candidate.evidence_ids?.length
  ));
  const reaction = window.reactionWindow;
  return {
    confidence: window.signalWindow?.confidence ?? reaction?.confidence ?? event?.confidence,
    reactionType: window.signalWindow?.reaction_type || reaction?.reaction_type || event?.reaction_type,
    targetType: window.signalWindow?.target_type || reaction?.target_type || event?.target_type,
    targetText: window.signalWindow?.target_text || reaction?.target_text || event?.target_text,
    divergenceType: reaction?.divergence_type || window.signalWindow?.relationship || window.alignment?.relationship,
    eventStart: reaction?.event_start || reaction?.window_start || window.start,
    eventPeak: reaction?.event_peak || eventPeakForWindow(window, event),
    eventHint: window.signalWindow?.event_hint || reaction?.event_hint || event?.event_hint,
    evidenceIDs: window.signalWindow?.evidence_ids?.length ? window.signalWindow.evidence_ids : reaction?.evidence_ids?.length ? reaction.evidence_ids : event?.evidence_ids
  };
}

function eventPeakForWindow(window: ReplayWindowPoint, event?: { timestamp?: string }) {
  if (window.chatBucket?.peak_time && isTimestampInWindow(window.chatBucket.peak_time, window)) return window.chatBucket.peak_time;
  return event?.timestamp || window.start;
}

function eventLabelForReaction(reactionType?: string, eventHint?: string): ReplayEventLabel | undefined {
  const value = `${reactionType || ""} ${eventHint || ""}`.toLowerCase();
  if (!value.trim()) return undefined;
  if (value.includes("diverg")) return "content_audience_divergence";
  if (value.includes("frustrat") || value.includes("anger") || value.includes("negative")) return "frustration_spike";
  if (value.includes("hype") || value.includes("joy") || value.includes("celebrat") || value.includes("positive")) return "hype_spike";
  if (value.includes("confusion") || value.includes("surprise") || value.includes("shift")) return "audience_shift";
  return undefined;
}

function cleanLabelText(value?: string) {
  const trimmed = value?.trim();
  return trimmed || undefined;
}

function formatOptionalPercent(value?: number) {
  return typeof value === "number" ? formatPercent(value) : "unknown";
}

function knownText(value?: string | null) {
  const trimmed = value?.trim();
  return trimmed || "unknown";
}

function targetContextLabel(targetType?: string, targetText?: string) {
  const target = knownText(targetType) === "unknown" && targetText?.trim() ? "possible" : knownText(targetType);
  const text = targetText?.trim();
  return text ? `${target}: ${truncateText(text, 64)}` : target;
}

function evidenceRefsLabel(refs: ReturnType<typeof resolveReplayEvidenceRefs>) {
  if (refs.length === 0) return "unknown";
  const visible = refs.slice(0, 2).map((ref) => {
    const text = ref.text ? `: ${truncateText(ref.text, 42)}` : "";
    return `${ref.label}${text}`;
  });
  const suffix = refs.length > visible.length ? ` +${refs.length - visible.length}` : "";
  return `${visible.join(", ")}${suffix}`;
}

function truncateText(value: string, maxLength: number) {
  return value.length > maxLength ? `${value.slice(0, maxLength - 1)}...` : value;
}

function windowRangeLabel(start?: string, end?: string) {
  return `${compactTime(start)}-${compactTime(end)}`;
}

function compactTime(value?: string) {
  if (!value) return "--:--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--:--";
  return date.toLocaleTimeString([], { minute: "2-digit", second: "2-digit" });
}

function sameReplayWindow(first: ReplayWindowPoint, second: ReplayWindowPoint) {
  return first.id === second.id || sameWindowRange(first.start, first.end, second.start, second.end);
}

function windowRangeKey(start?: string, end?: string) {
  const startValue = timestampValue(start);
  const endValue = timestampValue(end);
  if (!startValue && !endValue) return "";
  return `${startValue}:${endValue}`;
}

function sameWindowRange(firstStart?: string, firstEnd?: string, secondStart?: string, secondEnd?: string) {
  return timestampValue(firstStart) === timestampValue(secondStart) && timestampValue(firstEnd) === timestampValue(secondEnd);
}

function isTimestampInWindow(timestamp: string, window: ReplayWindowPoint) {
  const value = timestampValue(timestamp);
  const start = timestampValue(window.start);
  const end = timestampValue(window.end);
  if (!value) return false;
  if (start && value < start) return false;
  if (end && value > end) return false;
  return true;
}

function timestampValue(value?: string) {
  if (!value) return 0;
  const time = new Date(value).getTime();
  return Number.isFinite(time) ? time : 0;
}

const queueModes: Array<{ value: ReplayEvaluationQueueMode; label: string }> = [
  { value: "unlabeled", label: "Unlabeled" },
  { value: "agent_suggested", label: "Agent suggested" },
  { value: "detected", label: "Detected events" },
  { value: "low_confidence", label: "Low confidence" },
  { value: "all", label: "All" }
];

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
