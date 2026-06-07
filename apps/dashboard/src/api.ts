import type {
  DashboardSessionStartResponse,
  DashboardState,
  SessionEvaluationResponse,
  SessionHistoryResponse,
  SessionProofResponse,
  SessionReplay,
  SessionSummary,
  SignalWindowLabel,
  TranscriptHealth,
  TranscriptState
} from "./types";

export const TRANSCRIPT_API = "/transcript";
export const TRANSCRIPT_CHUNK_SECONDS_FALLBACK = 1;
export const TRANSCRIPT_LIVE_POLL_MS = 1000;

export async function fetchDashboardState(): Promise<DashboardState> {
  const response = await fetch("/state", { cache: "no-store" });
  if (!response.ok) throw new Error("Dashboard state unavailable");
  return response.json();
}

export async function fetchSessionHistory(limit = 20): Promise<SessionHistoryResponse> {
  const params = new URLSearchParams({ limit: String(limit) });
  const response = await fetch(`/sessions/history?${params.toString()}`, { cache: "no-store" });
  if (!response.ok) throw new Error("Session history unavailable");
  return response.json();
}

export async function fetchSessionSummary(sessionID: string): Promise<SessionSummary> {
  const response = await fetch(`/sessions/${encodeURIComponent(sessionID)}/summary`, { cache: "no-store" });
  if (!response.ok) throw new Error(response.status === 404 ? "Session summary not found" : "Session summary unavailable");
  return response.json();
}

export async function fetchSessionReplay(sessionID: string, limit = 200): Promise<SessionReplay> {
  const params = new URLSearchParams({ limit: String(limit) });
  const response = await fetch(`/sessions/${encodeURIComponent(sessionID)}/replay?${params.toString()}`, { cache: "no-store" });
  if (!response.ok) throw new Error(response.status === 404 ? "Session replay not found" : "Session replay unavailable");
  return response.json();
}

export async function fetchSessionProof(sessionID: string, limit = 500): Promise<SessionProofResponse> {
  const params = new URLSearchParams({ limit: String(limit) });
  const response = await fetch(`/sessions/${encodeURIComponent(sessionID)}/proof?${params.toString()}`, { cache: "no-store" });
  if (!response.ok) throw new Error(response.status === 404 ? "Session proof not found" : "Session proof unavailable");
  return response.json();
}

export async function fetchSessionEvaluation(sessionID: string): Promise<SessionEvaluationResponse> {
  const response = await fetch(`/sessions/${encodeURIComponent(sessionID)}/evaluation`, { cache: "no-store" });
  if (!response.ok) {
    const text = (await response.text()).trim();
    throw new Error(response.status === 404 ? text || "Session evaluation not found" : text || "Session evaluation unavailable");
  }
  return response.json();
}

async function fetchTranscriptMode(path: "live" | "buckets" | "state"): Promise<TranscriptState> {
  const response = await fetch(`${TRANSCRIPT_API}/${path}`, { cache: "no-store" });
  if (!response.ok) throw new Error("Transcript state unavailable");
  return response.json();
}

export async function fetchTranscriptState(): Promise<TranscriptState> {
  return fetchTranscriptMode("state");
}

export async function fetchTranscriptLive(): Promise<TranscriptState> {
  return fetchTranscriptMode("live");
}

export async function fetchTranscriptBuckets(): Promise<TranscriptState> {
  return fetchTranscriptMode("buckets");
}

export async function fetchTranscriptHealth(): Promise<TranscriptHealth> {
  const response = await fetch(`${TRANSCRIPT_API}/health`, { cache: "no-store" });
  if (!response.ok) throw new Error("Transcript health unavailable");
  return response.json();
}

export async function startDashboardSession(channel: string): Promise<DashboardSessionStartResponse> {
  const response = await fetch("/sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ channel })
  });
  if (!response.ok) {
    const text = await response.text();
    if (response.status === 401 || response.status === 403) {
      throw new Error(text.trim() ? `Live start is admin-only in this deployment: ${text.trim()}` : "Live start is admin-only in this deployment.");
    }
    throw new Error(text.trim() || response.statusText);
  }
  return response.json();
}

export async function saveHumanLabel(sessionID: string, messageID: string, label: string): Promise<void> {
  const response = await fetch("/labels", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionID, message_id: messageID, label })
  });
  if (!response.ok) throw new Error("Unable to save label");
}

export type SaveSignalWindowLabelRequest = {
  session_id: string;
  window_start: string;
  window_end: string;
  predicted_event?: string;
  predicted_relationship?: string;
  correctness: string;
  event_label: string;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  divergence_type?: string;
  event_start?: string;
  event_peak?: string;
  notes?: string;
};

export async function saveSignalWindowLabel(label: SaveSignalWindowLabelRequest): Promise<SignalWindowLabel> {
  const response = await fetch("/signal-window-labels", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(label)
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text.trim() || "Unable to save window label");
  }
  return response.json();
}
