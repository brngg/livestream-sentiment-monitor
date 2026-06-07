export function formatTime(value?: string): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export function formatNumber(value?: number): string {
  return typeof value === "number" ? value.toFixed(2) : "0.00";
}

export function formatSignedNumber(value?: number): string {
  if (typeof value !== "number") return "-";
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}`;
}

export function formatRatio(value?: number): string {
  return typeof value === "number" ? `${Math.round(value * 100)}%` : "-";
}

export function formatPercent(value?: number): string {
  return typeof value === "number" ? `${Math.round(value * 100)}%` : "-";
}

export function formatLatency(value?: number): string {
  return typeof value === "number" && value > 0 ? `${value} ms` : "-";
}

export function formatList(value?: string[]): string {
  return Array.isArray(value) && value.length > 0 ? value.join(", ") : "-";
}

export function formatLanguageMix(value?: Record<string, number>): string {
  if (!value) return "-";
  const parts = Object.entries(value)
    .filter((entry): entry is [string, number] => typeof entry[1] === "number" && Number.isFinite(entry[1]))
    .map(([language, ratio]) => `${language} ${formatRatio(ratio)}`);
  return parts.length > 0 ? parts.join(", ") : "-";
}

export function extractChannel(value: string): string {
  const text = normalizeStreamInputURL(value.trim());
  if (!text) return "";
  try {
    const url = new URL(text);
    if (isYouTubeHost(url.hostname)) {
      const videoID = youtubeVideoID(url);
      const fallback = url.pathname.split("/").filter(Boolean).join("-");
      return `youtube-${sanitizeSourceID(videoID || fallback)}`;
    }
    const parts = url.pathname.split("/").filter(Boolean);
    return parts[0] || "";
  } catch {
    return text.replace(/^@/, "").split(/[/?#]/)[0];
  }
}

function sanitizeSourceID(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function streamPlatformFromURL(value?: string): "youtube" | "twitch" | "" {
  if (!value) return "";
  try {
    const url = new URL(normalizeStreamInputURL(value));
    if (isYouTubeHost(url.hostname)) return "youtube";
    if (url.hostname.toLowerCase().includes("twitch.tv")) return "twitch";
  } catch {
    return "";
  }
  return "";
}

function normalizeStreamInputURL(value: string) {
  const lower = value.toLowerCase();
  if (
    lower.startsWith("youtube.com/") ||
    lower.startsWith("www.youtube.com/") ||
    lower.startsWith("m.youtube.com/") ||
    lower.startsWith("youtu.be/") ||
    lower.startsWith("twitch.tv/") ||
    lower.startsWith("www.twitch.tv/")
  ) {
    return `https://${value}`;
  }
  return value;
}

function isYouTubeHost(hostname: string) {
  const host = hostname.toLowerCase().replace(/^www\./, "").replace(/^m\./, "");
  return host === "youtube.com" || host.endsWith(".youtube.com") || host === "youtu.be" || host === "youtube-nocookie.com";
}

function youtubeVideoID(url: URL) {
  const host = url.hostname.toLowerCase().replace(/^www\./, "").replace(/^m\./, "");
  if (host === "youtu.be") return url.pathname.split("/").filter(Boolean)[0] || "";
  const fromQuery = url.searchParams.get("v");
  if (fromQuery) return fromQuery;
  const parts = url.pathname.split("/").filter(Boolean);
  const markerIndex = parts.findIndex((part) => ["live", "embed", "shorts"].includes(part));
  return markerIndex >= 0 ? parts[markerIndex + 1] || "" : "";
}

export function relationshipLabel(value?: string): { label: string; key: string } {
  if (value === "converged") return { label: "converged", key: "converged" };
  if (value === "soft_split") return { label: "soft split", key: "soft-split" };
  if (value === "diverged") return { label: "diverged", key: "diverged" };
  return { label: "waiting", key: "muted" };
}
