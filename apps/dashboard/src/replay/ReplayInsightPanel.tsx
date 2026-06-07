import { AlertTriangle, Flame, TrendingUp } from "lucide-react";
import type { ReactNode } from "react";
import type { SessionInsight, SessionInsightSummary } from "../types";
import { formatPercent } from "../utils";

export function ReplayInsightPanel({
  insights = [],
  summary
}: {
  insights?: SessionInsight[];
  summary?: SessionInsightSummary;
}) {
  const compactSummary = buildCompactSummary(insights, summary);
  const hasInsights = compactSummary.topMoments.length > 0
    || compactSummary.biggestDivergence
    || compactSummary.highestHype
    || compactSummary.highestFrustration
    || compactSummary.lowConfidenceFlags.length > 0;
  const hasSpotlights = Boolean(
    compactSummary.biggestDivergence || compactSummary.highestHype || compactSummary.highestFrustration
  );

  if (!hasInsights) return null;

  return (
    <div className="replay-insight-block">
      <div className="metric-label-wrapper"><div className="metric-label">Session Intelligence</div></div>
      {hasSpotlights ? (
        <div className="replay-insight-grid">
          {compactSummary.biggestDivergence ? (
            <InsightSpotlight
              icon={<TrendingUp size={13} />}
              label="Biggest divergence"
              insight={compactSummary.biggestDivergence}
            />
          ) : null}
          {compactSummary.highestHype ? (
            <InsightSpotlight
              icon={<Flame size={13} />}
              label="Highest hype"
              insight={compactSummary.highestHype}
            />
          ) : null}
          {compactSummary.highestFrustration ? (
            <InsightSpotlight
              icon={<AlertTriangle size={13} />}
              label="Highest frustration"
              insight={compactSummary.highestFrustration}
            />
          ) : null}
        </div>
      ) : null}

      {compactSummary.topMoments.length > 0 ? (
        <div className="replay-insight-list">
          {compactSummary.topMoments.map((insight, index) => (
            <InsightRow
              insight={insight}
              key={`${insight.type || "moment"}-${insight.window_start || ""}-${index}`}
              prefix={`${index + 1}`}
            />
          ))}
        </div>
      ) : null}

      {compactSummary.lowConfidenceFlags.length > 0 ? (
        <div className="replay-insight-flags">
          {compactSummary.lowConfidenceFlags.map((insight, index) => (
            <InsightRow
              insight={insight}
              key={`${insight.type || "flag"}-${insight.window_start || ""}-${index}`}
              prefix="flag"
              muted
            />
          ))}
        </div>
      ) : null}
    </div>
  );
}

function InsightSpotlight({
  icon,
  label,
  insight
}: {
  icon: ReactNode;
  label: string;
  insight: SessionInsight;
}) {
  return (
    <div className={`replay-insight-card ${severityTone(insight)}`}>
      <span>{icon}{label}</span>
      <strong>{insight.title || insight.type || "Untitled insight"}</strong>
      <em>{insightMeta(insight)}</em>
    </div>
  );
}

function InsightRow({
  insight,
  prefix,
  muted = false
}: {
  insight: SessionInsight;
  prefix: string;
  muted?: boolean;
}) {
  const evidence = firstEvidenceText(insight);

  return (
    <div className={muted ? "replay-insight-row muted" : `replay-insight-row ${severityTone(insight)}`}>
      <span>{prefix}</span>
      <div>
        <strong>{insight.title || insight.type || "Untitled insight"}</strong>
        <p>{insight.explanation || insight.description || evidence || insight.uncertainty || "No explanation stored for this insight."}</p>
        <em>{insightMeta(insight)}</em>
      </div>
    </div>
  );
}

function buildCompactSummary(insights: SessionInsight[], summary?: SessionInsightSummary) {
  const cleanInsights = insights.filter(Boolean);
  const topMoments = (summary?.top_moments?.length ? summary.top_moments : topInsights(cleanInsights)).slice(0, 3);
  const biggestDivergence = summary?.biggest_divergence || findByText(cleanInsights, "diverg");
  const highestHype = summary?.highest_hype || findByText(cleanInsights, "hype");
  const highestFrustration = summary?.highest_frustration || findByText(cleanInsights, "frustrat");
  const lowConfidenceFlags = (summary?.low_confidence_flags?.length
    ? summary.low_confidence_flags
    : cleanInsights.filter((insight) =>
        (typeof insight.confidence === "number" && insight.confidence < 0.55)
        || insightText(insight).includes("transcript_gap")
        || insightText(insight).includes("evidence gap")
      )
  ).slice(0, 2);

  return { topMoments, biggestDivergence, highestHype, highestFrustration, lowConfidenceFlags };
}

function topInsights(insights: SessionInsight[]) {
  return insights
    .slice()
    .sort((first, second) => insightRank(second) - insightRank(first));
}

function findByText(insights: SessionInsight[], needle: string) {
  return topInsights(insights.filter((insight) => insightText(insight).includes(needle)))[0];
}

function insightRank(insight: SessionInsight) {
  const severity = typeof insight.severity === "number" ? insight.severity : severityWeight(insight.severity);
  const confidence = typeof insight.confidence === "number" ? insight.confidence : 0.5;
  return severity * 0.7 + confidence * 0.3;
}

function severityWeight(value?: string | number) {
  if (typeof value === "number") return value;
  if (value === "critical") return 1;
  if (value === "high") return 0.82;
  if (value === "medium") return 0.58;
  if (value === "low") return 0.34;
  return 0.42;
}

function severityTone(insight: SessionInsight) {
  const score = severityWeight(insight.severity);
  if (score >= 0.75) return "high";
  if (score >= 0.5) return "medium";
  return "low";
}

function insightMeta(insight: SessionInsight) {
  const parts = [
    windowLabel(insight.window_start, insight.window_end),
    typeof insight.confidence === "number" ? `${formatPercent(insight.confidence)} conf.` : "",
    insight.uncertainty ? "uncertain" : ""
  ].filter(Boolean);
  return parts.join(" / ") || "stored insight";
}

function windowLabel(start?: string, end?: string) {
  if (!start && !end) return "";
  return `${formatCompactTime(start)}-${formatCompactTime(end)}`;
}

function formatCompactTime(value?: string) {
  if (!value) return "--:--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--:--";
  return date.toLocaleTimeString([], { minute: "2-digit", second: "2-digit" });
}

function firstEvidenceText(insight: SessionInsight) {
  const evidence = Array.isArray(insight.evidence) ? insight.evidence[0] : insight.evidence;
  if (typeof evidence === "string") return evidence;
  return evidence?.text || evidence?.summary || evidence?.label || "";
}

function insightText(insight: SessionInsight) {
  return `${insight.type || ""} ${insight.kind || ""} ${insight.title || ""} ${insight.explanation || ""} ${insight.description || ""}`.toLowerCase();
}
