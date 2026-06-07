import type { MessageScore, SignalEvent, TranscriptBucket } from "../types";
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

export function ReplayEvidencePanel({
  window
}: {
  window?: ReplayWindowPoint;
}) {
  if (!window) {
    return <p className="empty-modern replay-empty">Select a stored session to inspect sentiment evidence windows.</p>;
  }

  const chatBucket = window.chatBucket;
  const transcriptBucket = window.transcriptBucket;
  const score = replayWindowScore(window);
  const chatSentiment = replayWindowChatSentiment(window);
  const transcriptSentiment = replayWindowTranscriptSentiment(window);
  const relationship = relationshipLabel(replayWindowRelationship(window));
  const eventLabels = eventLabelsForWindow(window.events);
  const transcriptSnippet = transcriptSnippetForBucket(transcriptBucket);
  const transcriptContext = window.transcriptContext || [];
  const messageScores = representativeScores(chatBucket?.message_scores || []);
  const reactionContext = windowReactionContext(window);
  const evidenceRefs = resolveReplayEvidenceRefs(window, reactionContext.evidenceIDs);

  return (
    <div className="replay-evidence">
      <div className="replay-evidence-hero">
        <div>
          <span>Selected Window</span>
          <strong className={`sentiment-read ${sentimentTone(score)}`}>{formatSignedNumber(score)}</strong>
          <em>{relationship.label}</em>
        </div>
        <div className="replay-evidence-copy">
          <span>{window.source.toUpperCase()} / {bucketWindowLabel(window.start, window.end)}</span>
          <p>{transcriptSnippet || "No transcript text stored for this window."}</p>
        </div>
      </div>

      <div className="replay-evidence-grid">
        <EvidenceMetric label="Chat sentiment" value={formatSignedNumber(chatSentiment)} tone="chat-color" />
        <EvidenceMetric label="Transcript sentiment" value={formatSignedNumber(transcriptSentiment)} tone="transcript-color" />
        <EvidenceMetric label="Delta" value={formatSignedNumber(replayWindowDelta(window))} />
        <EvidenceMetric label="Messages" value={replayWindowMessageCount(window).toLocaleString()} />
        <EvidenceMetric label="Quality" value={formatPercent(replayWindowQuality(window))} />
        <EvidenceMetric label="Signal confidence" value={formatOptionalPercent(reactionContext.confidence)} />
        <EvidenceMetric label="Chat confidence" value={formatOptionalPercent(chatBucket?.sentiment_confidence ?? window.signalWindow?.chat_confidence)} />
        <EvidenceMetric label="Transcript conf." value={formatOptionalPercent(transcriptBucket?.sentiment_confidence ?? window.signalWindow?.transcript_confidence)} />
        <EvidenceMetric label="Similarity" value={formatPercent(window.signalWindow?.similarity ?? window.alignment?.similarity)} />
        {reactionContext.reactionType ? <EvidenceMetric label="Reaction" value={reactionContext.reactionType} /> : null}
        {reactionContext.eventHint ? <EvidenceMetric label="Event hint" value={reactionContext.eventHint} /> : null}
        {reactionContext.target !== "unknown" ? <EvidenceMetric label="Possible target" value={reactionContext.target} wide /> : null}
        {evidenceRefs.length > 0 ? <EvidenceMetric label="Evidence refs" value={formatEvidenceRefs(evidenceRefs)} wide /> : null}
        <EvidenceMetric label="Top terms" value={formatList(chatBucket?.top_terms?.slice(0, 5))} wide />
        <EvidenceMetric label="Top emotes" value={formatList(chatBucket?.top_emotes?.slice(0, 5))} wide />
      </div>

      {eventLabels.length > 0 ? (
        <section className="replay-evidence-section">
          <div className="replay-section-title">Detected labels</div>
          <div className="replay-label-list">
            {eventLabels.map((label) => <span key={label}>{label}</span>)}
          </div>
        </section>
      ) : null}

      <section className="replay-evidence-section">
        <div className="replay-section-title">Transcript context</div>
        {transcriptContext.length === 0 ? (
          <p className="replay-muted">No adjacent transcript buckets are available for this window.</p>
        ) : (
          <div className="replay-transcript-context">
            {transcriptContext.map((context) => (
              <TranscriptContextRow context={context} key={`${context.role}-${context.bucket.bucket_start || ""}`} />
            ))}
          </div>
        )}
      </section>

      <section className="replay-evidence-section">
        <div className="replay-section-title">Representative chat scores</div>
        {messageScores.length === 0 ? (
          <p className="replay-muted">No per-message score evidence stored for this window.</p>
        ) : (
          <div className="replay-message-list">
            {messageScores.map((scoreItem, index) => (
              <div className="replay-message-row" key={scoreItem.message_id || `${scoreItem.timestamp}-${index}`}>
                <div>
                  <time>{formatCompactTime(scoreItem.timestamp)}</time>
                  <strong>{scoreItem.display_name || scoreItem.username || "unknown"}</strong>
                  <span className={`sentiment-read ${sentimentTone(scoreItem.sentiment_score)}`}>{formatSignedNumber(scoreItem.sentiment_score)} / {formatOptionalPercent(scoreItem.confidence)}</span>
                </div>
                <p>{scoreItem.text || "No message text stored."}</p>
              </div>
            ))}
          </div>
        )}
      </section>

      {window.events.length > 0 ? (
        <section className="replay-evidence-section">
          <div className="replay-section-title">Signal events</div>
          <div className="replay-event-list">
            {window.events.slice(0, 6).map((event, index) => {
              const context = eventContext(event, window);
              return (
                <div className="replay-event-row" key={`${event.timestamp}-${event.source}-${index}`}>
                  <span>{eventHeader(event)}</span>
                  {context ? <span>{context}</span> : null}
                  <p>{eventText(event)}</p>
                </div>
              );
            })}
          </div>
        </section>
      ) : null}
    </div>
  );
}

function TranscriptContextRow({ context }: { context: TranscriptContextBucket }) {
  const bucket = context.bucket;
  const snippet = transcriptSnippetForBucket(bucket);

  return (
    <div className={`replay-transcript-context-row ${context.role}`}>
      <div>
        <span>{context.role}</span>
        <strong>{bucketWindowLabel(bucket.bucket_start, bucket.bucket_end)}</strong>
        <em>{formatOptionalPercent(bucket.sentiment_confidence ?? bucket.transcript_confidence)}</em>
      </div>
      <p>{snippet || "No transcript text stored for this bucket."}</p>
    </div>
  );
}

function EvidenceMetric({ label, value, tone = "", wide = false }: { label: string; value: string; tone?: string; wide?: boolean }) {
  return (
    <div className={wide ? "wide" : ""}>
      <span>{label}</span>
      <strong className={tone}>{value}</strong>
    </div>
  );
}

function representativeScores(scores: MessageScore[]) {
  return scores
    .slice()
    .sort((first, second) => Math.abs(second.sentiment_score || 0) - Math.abs(first.sentiment_score || 0))
    .slice(0, 5);
}

function transcriptSnippetForBucket(bucket?: TranscriptBucket) {
  if (!bucket) return "";
  if (bucket.text?.trim()) return bucket.text.trim();
  return (bucket.segments || [])
    .map((segment) => segment.text)
    .filter(Boolean)
    .join(" ")
    .trim();
}

function eventLabelsForWindow(events: SignalEvent[]) {
  const labels = events
    .map((event) => event.reaction_type || event.event_hint || event.label || event.type || event.source)
    .filter((label): label is string => Boolean(label));
  return Array.from(new Set(labels)).slice(0, 8);
}

function eventHeader(event: SignalEvent) {
  return [
    event.source || event.type || "unknown",
    formatCompactTime(event.timestamp),
    event.reaction_type || event.label || "unknown",
    formatOptionalPercent(event.confidence)
  ].join(" / ");
}

function eventContext(event: SignalEvent, window?: ReplayWindowPoint) {
  const evidence = window ? formatEvidenceRefs(resolveReplayEvidenceRefs(window, event.evidence_ids || [])) : formatEvidenceIDs(event.evidence_ids || []);
  const parts = [
    event.event_hint ? `hint ${event.event_hint}` : "",
    event.target_type || event.target_text ? `possible target ${targetContextLabel(event.target_type, event.target_text)}` : "",
    event.evidence_ids?.length ? `evidence ${evidence}` : ""
  ].filter(Boolean);
  return parts.join(" / ");
}

function eventText(event: SignalEvent) {
  return event.text || event.message?.text || event.transcript_segment?.text || event.target_text || event.event_hint || event.label || "Stored signal event";
}

function windowReactionContext(window: ReplayWindowPoint) {
  const event = window.events.find((candidate) => (
    candidate.reaction_type
    || candidate.target_type
    || candidate.target_text
    || candidate.event_hint
    || candidate.confidence
    || candidate.evidence_ids?.length
  ));
  const reaction = window.reactionWindow;
  const targetType = window.signalWindow?.target_type || reaction?.target_type || event?.target_type;
  const targetText = window.signalWindow?.target_text || reaction?.target_text || event?.target_text;
  return {
    confidence: window.signalWindow?.confidence ?? reaction?.confidence ?? event?.confidence,
    reactionType: window.signalWindow?.reaction_type || reaction?.reaction_type || event?.reaction_type,
    eventHint: window.signalWindow?.event_hint || reaction?.event_hint || event?.event_hint,
    target: targetContextLabel(targetType, targetText),
    evidenceIDs: window.signalWindow?.evidence_ids?.length ? window.signalWindow.evidence_ids : reaction?.evidence_ids?.length ? reaction.evidence_ids : event?.evidence_ids || []
  };
}

function targetContextLabel(targetType?: string, targetText?: string) {
  const target = knownText(targetType) === "unknown" && targetText?.trim() ? "possible" : knownText(targetType);
  const text = targetText?.trim();
  return text ? `${target}: ${truncateText(text, 64)}` : target;
}

function knownText(value?: string | null) {
  const trimmed = value?.trim();
  return trimmed || "unknown";
}

function formatOptionalPercent(value?: number) {
  return typeof value === "number" ? formatPercent(value) : "unknown";
}

function formatEvidenceIDs(values: string[]) {
  const visible = values.filter(Boolean).slice(0, 3);
  const suffix = values.length > visible.length ? ` +${values.length - visible.length}` : "";
  return `${visible.join(", ")}${suffix}`;
}

function formatEvidenceRefs(refs: ReturnType<typeof resolveReplayEvidenceRefs>) {
  const visible = refs.slice(0, 3).map((ref) => {
    const text = ref.text ? `: ${truncateText(ref.text, 48)}` : "";
    return `${ref.label}${text}`;
  });
  const suffix = refs.length > visible.length ? ` +${refs.length - visible.length}` : "";
  return `${visible.join(", ")}${suffix}`;
}

function truncateText(value: string, maxLength: number) {
  return value.length > maxLength ? `${value.slice(0, Math.max(0, maxLength - 3))}...` : value;
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

function sentimentTone(value?: number) {
  if (typeof value !== "number") return "neutral";
  if (value > 0.05) return "positive";
  if (value < -0.05) return "negative";
  return "neutral";
}
