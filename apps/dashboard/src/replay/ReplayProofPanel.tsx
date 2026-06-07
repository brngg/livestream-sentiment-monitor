import { AlertTriangle, CheckCircle2, Gauge, Info } from "lucide-react";
import type { ReactNode } from "react";
import type {
  ReplayProof,
  ReplayProofLatencySummary,
  ReplayProofSpeed,
  ReplayProofTruncation,
  ReplayProofUnsupported,
  SessionReplay
} from "../types";
import { formatPercent } from "../utils";

const proofSpeeds = [1, 5, 10];

export function ReplayProofPanel({
  selectedSessionID,
  proof,
  replay,
  loading,
  error
}: {
  selectedSessionID: string;
  proof?: ReplayProof;
  replay?: SessionReplay;
  loading: boolean;
  error: string;
}) {
  if (!selectedSessionID) {
    return (
      <div className="replay-proof-block">
        <div className="metric-label-wrapper"><div className="metric-label">Replay Proof</div></div>
        <ProofState icon={<Info size={14} />} title="No replay selected" text="Select a stored session to load proof metrics." />
      </div>
    );
  }

  if (loading && !proof) {
    return (
      <div className="replay-proof-block">
        <div className="metric-label-wrapper"><div className="metric-label">Replay Proof</div></div>
        <ProofState icon={<Gauge size={14} />} title="Loading proof" text="Fetching backend replay proof metrics." />
      </div>
    );
  }

  if (error) {
    return (
      <div className="replay-proof-block">
        <div className="metric-label-wrapper"><div className="metric-label">Replay Proof</div></div>
        <ProofState icon={<AlertTriangle size={14} />} title="Proof unavailable" text={error} tone="error" />
      </div>
    );
  }

  if (!proof) {
    return (
      <div className="replay-proof-block">
        <div className="metric-label-wrapper"><div className="metric-label">Replay Proof</div></div>
        <ProofState icon={<Info size={14} />} title="No proof returned" text="The proof endpoint returned no metrics for this session." />
      </div>
    );
  }

  const coverage = proof.label_coverage?.coverage ?? undefined;
  const partial = Boolean(proof.partial || proof.truncated_sources?.length);
  const signalEventCount = replay?.signal_events?.length;
  const speedRows = proofSpeeds.map((speed) => proofSpeedFor(proof.speeds, speed) || { speed });

  return (
    <div className="replay-proof-block">
      <div className="metric-label-wrapper"><div className="metric-label">Replay Proof</div></div>
      <div className={`replay-proof-banner ${partial ? "warn" : "ok"}`}>
        {partial ? <AlertTriangle size={14} /> : <CheckCircle2 size={14} />}
        <div>
          <span>{partial ? "Partial proof" : "Proof ready"}</span>
          <p>{proofSummaryText(proof)}</p>
        </div>
      </div>

      <div className="replay-proof-speed-grid" aria-label="Replay proof readiness by speed">
        {speedRows.map((speed) => (
          <ProofSpeedCard proof={proof} speed={speed} key={speed.speed || "unknown"} />
        ))}
      </div>

      <div className="replay-proof-grid">
        <ProofMetric label="Label coverage" value={formatPercent(coverage)} />
        <ProofMetric label="Labels" value={`${compactNumber(proof.label_coverage?.labeled_windows)} / ${compactNumber(proof.label_coverage?.total_windows)}`} />
        <ProofMetric label="Stored labels" value={compactNumber(proof.label_coverage?.stored_label_count)} />
        <ProofMetric label="Unmatched labels" value={compactNumber(proof.label_coverage?.unmatched_labels)} />
        <ProofMetric label="Signal events" value={compactNumber(signalEventCount)} />
        <ProofMetric label="Signal windows" value={compactNumber(proof.signal_window_count)} />
        <ProofMetric label="Matched windows" value={compactNumber(proof.matched_windows)} />
        <ProofMetric label="Loaded buckets" value={`${compactNumber(proof.bucket_count)} / ${compactNumber(proof.session_totals?.bucket_count ?? proof.bucket_count)}`} />
        <ProofMetric label="Chat buckets" value={`${compactNumber(proof.chat_bucket_count)} / ${compactNumber(proof.session_totals?.chat_bucket_count ?? proof.chat_bucket_count)}`} />
        <ProofMetric label="Transcript buckets" value={`${compactNumber(proof.transcript_bucket_count)} / ${compactNumber(proof.session_totals?.transcript_bucket_count ?? proof.transcript_bucket_count)}`} />
        <ProofMetric label="Transcript audio" value={formatPercent(proof.transcript_coverage?.audio_coverage ?? undefined)} />
        <ProofMetric label="Transcript empty" value={formatPercent(proof.transcript_coverage?.empty_ratio ?? undefined)} />
        <ProofMetric label="Transcript words" value={compactNumber(proof.transcript_coverage?.word_count)} />
        <ProofMetric label="Repair words" value={compactNumber(proof.transcript_coverage?.repair_added_words)} />
        <ProofMetric label="Repair changed" value={formatPercent(proof.transcript_coverage?.average_repair_changed_ratio ?? undefined)} />
        <ProofMetric label="Repair improvement" value={formatPercent(proof.transcript_coverage?.repair_improvement ?? undefined)} />
        <ProofMetric label="Transcript status" value={formatStatusCounts(proof.transcript_coverage?.status_counts)} />
        <ProofMetric label="Alignments" value={`${compactNumber(proof.alignment_count)} / ${compactNumber(proof.session_totals?.alignment_count ?? proof.alignment_count)}`} />
        <ProofMetric label="Timeline" value={formatDurationMS(proof.timeline?.source_duration_ms)} />
      </div>

      <div className="replay-proof-latency-grid">
        <LatencyRow label="Chat latency" summary={proof.latency?.chat_analysis_latency_ms} statusCounts={proof.latency?.chat_analysis_status_counts} />
        <LatencyRow label="Transcript sentiment latency" summary={proof.latency?.transcript_sentiment_latency_ms} statusCounts={proof.latency?.transcript_sentiment_status_counts} />
        <LatencyRow label="Transcript ASR latency" summary={proof.latency?.transcript_asr_latency_ms} />
        <LatencyRow label="Transcript pipeline latency" summary={proof.latency?.transcript_pipeline_latency_ms} />
      </div>

      <ProofWarnings truncations={proof.truncated_sources} unsupported={proof.unsupported_metrics} />
    </div>
  );
}

function ProofState({
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
    <div className={`replay-proof-state ${tone}`}>
      {icon}
      <div>
        <span>{title}</span>
        <p>{text}</p>
      </div>
    </div>
  );
}

function ProofSpeedCard({ proof, speed }: { proof: ReplayProof; speed: ReplayProofSpeed }) {
  const status = proofReadinessStatus(proof, speed);
  return (
    <div className={`replay-proof-speed ${status.tone}`}>
      <span><Gauge size={12} /> {formatSpeed(speed.speed)} readiness</span>
      <strong>{status.label}</strong>
      <em>{formatDurationMS(speed.estimated_replay_duration_ms)}</em>
      <small>{formatThroughput(speed)}</small>
    </div>
  );
}

function ProofMetric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function LatencyRow({
  label,
  summary,
  statusCounts
}: {
  label: string;
  summary?: ReplayProofLatencySummary;
  statusCounts?: Record<string, number>;
}) {
  return (
    <div className="replay-proof-latency-row">
      <div>
        <span>{label}</span>
        <strong>{compactNumber(summary?.available_count)} measured</strong>
        <em>{compactNumber(summary?.missing_count)} missing</em>
      </div>
      <div>
        <span>P50</span>
        <strong>{formatLatencyMS(summary?.p50)}</strong>
      </div>
      <div>
        <span>P95</span>
        <strong>{formatLatencyMS(summary?.p95)}</strong>
      </div>
      <div>
        <span>Status</span>
        <strong>{formatStatusCounts(statusCounts)}</strong>
      </div>
    </div>
  );
}

function ProofWarnings({
  truncations = [],
  unsupported = []
}: {
  truncations?: ReplayProofTruncation[];
  unsupported?: ReplayProofUnsupported[];
}) {
  const hasWarnings = truncations.length > 0 || unsupported.length > 0;
  if (!hasWarnings) return null;

  return (
    <div className="replay-proof-warning-list">
      {truncations.map((item, index) => (
        <div className="replay-proof-warning" key={`${item.source}-${item.loaded_count}-${item.total_count}-${index}`}>
          <AlertTriangle size={12} />
          <span>{sourceLabel(item.source)} truncated</span>
          <strong>{compactNumber(item.loaded_count)} / {compactNumber(item.total_count)}</strong>
        </div>
      ))}
      {unsupported.map((item, index) => (
        <div className="replay-proof-warning muted" key={`${item.name || "metric"}-${index}`}>
          <Info size={12} />
          <span>{sourceLabel(item.name)} unsupported</span>
          <strong>{item.reason || "Metric unavailable"}</strong>
        </div>
      ))}
    </div>
  );
}

function proofReadinessStatus(proof: ReplayProof, speed: ReplayProofSpeed): { label: string; tone: "ok" | "warn" | "muted" } {
  if (!hasPositiveNumber(speed.estimated_replay_duration_ms)) return { label: "No timeline", tone: "muted" };
  if (proof.partial) return { label: "Partial", tone: "warn" };
  if (!hasPositiveNumber(proof.source_bucket_count) || !hasPositiveNumber(proof.signal_window_count)) {
    return { label: "Sparse", tone: "warn" };
  }
  if (!hasPositiveNumber(proof.matched_windows)) return { label: "Unmatched", tone: "warn" };
  return { label: "Ready", tone: "ok" };
}

function proofSpeedFor(speeds: ReplayProofSpeed[] | undefined, speed: number) {
  return speeds?.find((item) => item.speed === speed);
}

function proofSummaryText(proof: ReplayProof) {
  const loaded = compactNumber(proof.bucket_count);
  const total = compactNumber(proof.session_totals?.bucket_count ?? proof.bucket_count);
  const coverage = formatPercent(proof.label_coverage?.coverage ?? undefined);
  const generated = formatProofDate(proof.generated_at);
  const suffix = generated ? ` / generated ${generated}` : "";
  if (proof.partial) return `${loaded} of ${total} evidence buckets loaded / label coverage ${coverage}${suffix}`;
  return `${loaded} evidence buckets loaded / label coverage ${coverage}${suffix}`;
}

function formatSpeed(value?: number) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1)}x`;
}

function formatThroughput(speed: ReplayProofSpeed) {
  const windows = hasNumber(speed.windows_per_second) ? `${speed.windows_per_second.toFixed(2)} win/s` : "- win/s";
  const buckets = hasNumber(speed.buckets_per_second) ? `${speed.buckets_per_second.toFixed(2)} bucket/s` : "- bucket/s";
  return `${windows} / ${buckets}`;
}

function formatDurationMS(value?: number) {
  if (!hasNumber(value) || value <= 0) return "-";
  if (value < 1000) return `${Math.round(value)} ms`;
  const seconds = value / 1000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = Math.round(seconds % 60);
  return `${minutes}m ${remainder}s`;
}

function formatLatencyMS(value?: number | null) {
  if (!hasNumber(value)) return "-";
  return `${Math.round(value)} ms`;
}

function formatStatusCounts(counts?: Record<string, number>) {
  const entries = Object.entries(counts || {})
    .filter((entry): entry is [string, number] => typeof entry[1] === "number")
    .sort((first, second) => second[1] - first[1])
    .slice(0, 2);
  if (entries.length === 0) return "-";
  return entries.map(([status, count]) => `${status} ${count}`).join(" / ");
}

function formatProofDate(value?: string) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function compactNumber(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
  return Math.round(value).toLocaleString();
}

function sourceLabel(value?: string) {
  if (!value) return "Metric";
  return value.replace(/_/g, " ");
}

function hasNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function hasPositiveNumber(value: unknown): value is number {
  return hasNumber(value) && value > 0;
}
