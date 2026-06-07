import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { greatest, least } from "d3-array";
import { curveCatmullRom, line } from "d3-shape";
import { scaleLinear } from "d3-scale";
import { AnimatePresence, motion } from "motion/react";
import { ChevronLeft, ChevronRight, Play, RotateCcw, X } from "lucide-react";
import {
  fetchDashboardState,
  fetchSessionEvaluation,
  fetchSessionHistory,
  fetchSessionReplay,
  fetchTranscriptBuckets,
  fetchTranscriptHealth,
  fetchTranscriptLive,
  saveHumanLabel,
  saveSignalWindowLabel,
  startDashboardSession,
  TRANSCRIPT_API,
  TRANSCRIPT_CHUNK_SECONDS_FALLBACK,
  TRANSCRIPT_LIVE_POLL_MS
} from "./api";
import type {
  AlignmentBucket,
  ChatBucket,
  ChatMessage,
  DashboardEvent,
  DashboardState,
  MessageScore,
  ReactionWindow,
  SessionReplay,
  SignalWindow,
  StreamInfo,
  TranscriptBucket,
  TranscriptHealth,
  TranscriptSegment,
  TranscriptState
} from "./types";
import {
  extractChannel,
  formatLanguageMix,
  formatLatency,
  formatList,
  formatNumber,
  formatPercent,
  formatRatio,
  formatSignedNumber,
  formatTime,
  relationshipLabel,
  streamPlatformFromURL
} from "./utils";
import type { SaveReplayEvaluationLabel } from "./replay/ReplayEvaluationPanel";
import { EvaluationLabPage } from "./replay/EvaluationLabPage";
import { clampTranscriptOffset } from "./replay/replayModel";

type SentimentPoint = {
  key: string;
  time: string;
  source?: string;
  provisional?: boolean;
  chat?: number;
  transcript?: number;
  aggregate?: number;
  delta?: number;
  similarity?: number;
  relationship?: string;
  messages?: number;
  hype?: number;
  intensity?: number;
  confusion?: number;
  frustration?: number;
  reactionType?: string;
  targetType?: string;
  targetText?: string;
  eventHint?: string;
  confidence?: number;
  evidenceIDs?: string[];
  text?: string;
};

type RibbonPoint = {
  x: number;
  y: number;
  value: number;
};

type ChartSentimentPoint = SentimentPoint & {
  index: number;
  x: number;
  chatY: number;
  transcriptY: number;
  aggregateY: number;
};

type TimelineCanvasPoint = SentimentPoint & {
  index: number;
  time: string;
  activity: number;
};

type LiveChatRow = {
  key: string;
  time: string;
  user: string;
  text: string;
};

type TranscriptCaptionBlock = {
  key: string;
  start?: string;
  end?: string;
  text: string;
};

type HealthMetricTone = "ok" | "warn" | "error" | "muted";

type HealthMetric = {
  label: string;
  value: string;
  meta: string;
  tone: HealthMetricTone;
};

type DiagnosticMetric = {
  label: string;
  value: string;
  meta?: string;
};

type TranscriptQualityMetrics = {
  liveModel: string;
  finalModel: string;
  p50LatencyMS?: number;
  p95LatencyMS?: number;
  emptyRate?: number;
  medianConfidence?: number;
  qualityDropRate?: number;
  latestDelaySeconds?: number;
  segmentCount: number;
};

type BucketInspectorSelection =
  | { kind: "chat"; bucket: ChatBucket }
  | { kind: "transcript"; bucket: TranscriptBucket };

type TranscriptStreamEvent = TranscriptSegment & {
  type?: string;
  status?: string;
  error?: string;
  audio_end_seconds?: number;
  reaction_window?: ReactionWindow;
  reaction_type?: string;
  target_type?: string;
  target_text?: string;
  event_hint?: string;
  evidence_ids?: string[];
  sentiment_score?: number;
  sentiment_confidence?: number;
};

type TranscriptStreamState = "idle" | "connecting" | "connected" | "fallback";

type ReadTone = "positive" | "neutral" | "negative" | "muted";

type PrimaryInsight = {
  reaction: string;
  reactionTone: ReadTone;
  target: string;
  intensity: string;
  evidence: string;
  agreement: string;
  delay: string;
  delayTone: HealthMetricTone;
};

const emptyState: DashboardState = {
  status: "ready",
  messages: [],
  buckets: [],
  reaction_windows: [],
  transcript_buckets: [],
  alignments: [],
  message_count: 0,
  bucket_count: 0
};

const positiveTranscriptTerms = new Set([
  "amazing",
  "awesome",
  "best",
  "clutch",
  "great",
  "hype",
  "insane",
  "love",
  "nice",
  "perfect",
  "wow",
  "yes"
]);

const negativeTranscriptTerms = new Set([
  "awful",
  "bad",
  "broken",
  "hate",
  "lost",
  "no",
  "rough",
  "terrible",
  "ugh",
  "unlucky",
  "worse",
  "wrong"
]);

const confusionTranscriptTerms = new Set(["confused", "how", "huh", "wait", "what", "where", "why"]);
const frustrationTranscriptTerms = new Set(["angry", "annoying", "bad", "hate", "mad", "terrible", "ugh", "wrong"]);

type AppRoute = "/" | "/eval";

export const dashboardChartHeadings = {
  sentiment: "Sentiment Timeline",
  reaction: "Live Reaction Timeline"
} as const;

export function App() {
  const queryClient = useQueryClient();
  const [channelInput, setChannelInput] = useState(() => new URLSearchParams(window.location.search).get("channel") || "");
  const [startError, setStartError] = useState("");
  const [liveSegments, setLiveSegments] = useState<TranscriptSegment[]>([]);
  const [clock, setClock] = useState(() => new Date());
  const [selectedEvaluationSessionID, setSelectedEvaluationSessionID] = useState("");
  const [route, setRoute] = useState<AppRoute>(() => routeFromPath(window.location.pathname));
  const [transcriptOffsetSeconds, setTranscriptOffsetSeconds] = useState(0);
  const [transcriptStartWarning, setTranscriptStartWarning] = useState("");
  const [transcriptStreamState, setTranscriptStreamState] = useState<TranscriptStreamState>("idle");
  const [provisionalTranscriptWindows, setProvisionalTranscriptWindows] = useState<ReactionWindow[]>([]);
  const transcriptStreamClockRef = useRef<{ sessionID: string; startedAtMS: number } | null>(null);
  const selectedHistorySessionID = route === "/eval" ? selectedEvaluationSessionID : "";

  const dashboardQuery = useQuery({
    queryKey: ["dashboard-state"],
    queryFn: fetchDashboardState,
    refetchInterval: 1000
  });

  const transcriptBucketQuery = useQuery({
    queryKey: ["transcript-buckets"],
    queryFn: fetchTranscriptBuckets,
    refetchInterval: 1000,
    retry: 0
  });

  const transcriptLiveQuery = useQuery({
    queryKey: ["transcript-live"],
    queryFn: fetchTranscriptLive,
    refetchInterval: TRANSCRIPT_LIVE_POLL_MS,
    enabled: transcriptStreamState !== "connected",
    retry: 0
  });

  const transcriptHealthQuery = useQuery({
    queryKey: ["transcript-health"],
    queryFn: fetchTranscriptHealth,
    refetchInterval: 10000,
    retry: 0
  });

  const historyQuery = useQuery({
    queryKey: ["session-history"],
    queryFn: () => fetchSessionHistory(20),
    refetchInterval: 15000,
    retry: 0
  });

  const replayQuery = useQuery({
    queryKey: ["session-replay", selectedHistorySessionID],
    queryFn: () => fetchSessionReplay(selectedHistorySessionID, 200),
    enabled: Boolean(selectedHistorySessionID),
    retry: 0
  });

  const evaluationQuery = useQuery({
    queryKey: ["session-evaluation", selectedHistorySessionID],
    queryFn: () => fetchSessionEvaluation(selectedHistorySessionID),
    enabled: Boolean(selectedHistorySessionID && replayQuery.data?.session?.session_id === selectedHistorySessionID),
    retry: 0
  });

  const windowLabelMutation = useMutation({
    mutationFn: (label: SaveReplayEvaluationLabel) => saveSignalWindowLabel(label),
    onSuccess: (_label, variables) => {
      queryClient.invalidateQueries({ queryKey: ["session-replay", variables.session_id] });
      queryClient.invalidateQueries({ queryKey: ["session-evaluation", variables.session_id] });
      queryClient.invalidateQueries({ queryKey: ["session-history"] });
    }
  });

  const startMutation = useMutation({
    mutationFn: async (channel: string) => {
      const session = await startDashboardSession(channel);
      return session;
    },
    onMutate: (channel) => {
      setStartError("");
      setLiveSegments([]);
      setSelectedEvaluationSessionID("");
      setTranscriptOffsetSeconds(0);
      setTranscriptStartWarning("");
      setTranscriptStreamState("idle");
      setProvisionalTranscriptWindows([]);
      queryClient.setQueryData<DashboardState>(["dashboard-state"], {
        ...emptyState,
        status: "starting",
        channel: extractChannel(channel)
      });
    },
    onSuccess: (session) => {
      setTranscriptStartWarning(session.transcript_warning || "");
      queryClient.setQueryData<DashboardState>(["dashboard-state"], (current) => ({
        ...(current || emptyState),
        status: session.status,
        session_id: session.session_id,
        channel: extractChannel(session.channel)
      }));
      void queryClient.invalidateQueries({ queryKey: ["dashboard-state"] });
      void queryClient.invalidateQueries({ queryKey: ["session-history"] });
      void queryClient.invalidateQueries({ queryKey: ["transcript-live"] });
      void queryClient.invalidateQueries({ queryKey: ["transcript-buckets"] });
    },
    onError: (error) => {
      setTranscriptStartWarning("");
      setStartError(error instanceof Error ? error.message : "Unable to start ingestion");
    }
  });

  useEffect(() => {
    const timer = window.setInterval(() => setClock(new Date()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const onPopState = () => setRoute(routeFromPath(window.location.pathname));
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    const events = new EventSource("/events");
    events.onmessage = (event) => {
      const payload = parseEventPayload<DashboardEvent>(event.data);
      if (!payload) return;
      queryClient.setQueryData<DashboardState>(["dashboard-state"], (current) =>
        applyDashboardEvent(current || emptyState, payload)
      );
    };
    events.onerror = () => {
      queryClient.setQueryData<DashboardState>(["dashboard-state"], (current) => ({
        ...(current || emptyState),
        status: "event stream disconnected"
      }));
    };
    return () => events.close();
  }, [queryClient]);

  useEffect(() => {
    const initialChannel = new URLSearchParams(window.location.search).get("channel");
    if (initialChannel) {
      void handleStart(initialChannel);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const state = dashboardQuery.data || emptyState;
  const replayState = replayQuery.data ? sessionReplayToDashboardState(replayQuery.data) : undefined;
  const displayState = replayState || state;
  const replayMode = Boolean(replayState);
  const liveTranscriptMatches = Boolean(state.session_id && transcriptLiveQuery.data?.session_id === state.session_id);
  const bucketTranscriptMatches = Boolean(state.session_id && transcriptBucketQuery.data?.session_id === state.session_id);
  const latestSegment = liveTranscriptMatches ? transcriptLiveQuery.data?.latest_segment : undefined;
  const serviceTranscriptSegments = liveTranscriptMatches ? transcriptLiveQuery.data?.segments || [] : [];

  useEffect(() => {
    if (!latestSegment?.text) return;
    setLiveSegments((current) => upsertTranscriptSegment(current, latestSegment));
  }, [latestSegment]);

  useEffect(() => {
    if (!state.session_id) return;
    transcriptStreamClockRef.current = null;
    let closedByEffect = false;
    setTranscriptStreamState("connecting");
    const events = new EventSource(`${TRANSCRIPT_API}/events`);
    events.onopen = () => {
      if (!closedByEffect) setTranscriptStreamState("connected");
    };
    events.onmessage = (event) => {
      const payload = parseEventPayload<TranscriptStreamEvent>(event.data);
      if (!payload) return;
      applyTranscriptStreamPayload(resolveTranscriptStreamTiming(payload));
    };
    events.onerror = () => {
      if (!closedByEffect) setTranscriptStreamState("fallback");
      events.close();
    };
    return () => {
      closedByEffect = true;
      events.close();
    };
  }, [queryClient, state.session_id]);

  const latestTranscriptBucket = replayMode
    ? displayState.transcript_buckets?.[0]
    : bucketTranscriptMatches ? transcriptBucketQuery.data?.latest_bucket : undefined;
  const serviceTranscriptBuckets = bucketTranscriptMatches ? transcriptBucketQuery.data?.buckets || [] : [];
  const transcriptBuckets = displayState.transcript_buckets?.length
    ? displayState.transcript_buckets
    : serviceTranscriptBuckets.length
      ? serviceTranscriptBuckets
      : latestTranscriptBucket
      ? [latestTranscriptBucket]
      : [];
  const committedTranscriptBucketKey = useMemo(() => transcriptBuckets.map(transcriptBucketIdentity).join("|"), [transcriptBuckets]);
  const visibleProvisionalTranscriptWindows = useMemo(
    () => replayMode ? [] : provisionalTranscriptWindows.filter((window) => !committedTranscriptBucketCoversWindow(window, transcriptBuckets)),
    [provisionalTranscriptWindows, replayMode, transcriptBuckets]
  );
  const reactionWindows = useMemo(
    () => replayMode
      ? displayState.reaction_windows || []
      : mergeReactionWindows(displayState.reaction_windows || [], visibleProvisionalTranscriptWindows),
    [displayState.reaction_windows, replayMode, visibleProvisionalTranscriptWindows]
  );
  const replayTranscriptRows = useMemo(() => transcriptSegmentsFromBuckets(transcriptBuckets), [transcriptBuckets]);
  const transcriptRows = useMemo(
    () => replayMode ? replayTranscriptRows : transcriptPanelRows([...serviceTranscriptSegments, ...liveSegments]),
    [liveSegments, replayMode, replayTranscriptRows, serviceTranscriptSegments]
  );
  const transcriptMetricSegments = replayMode
    ? transcriptRows
    : serviceTranscriptSegments.length > 0 ? serviceTranscriptSegments : transcriptRows;
  const status = startError ? `error: ${startError}` : state.error ? `error: ${state.error}` : state.status || "ready";
  const chatAvailable = streamSupportsChat(displayState.stream || state.stream, displayState.channel || state.channel);
  const displayStatus = replayMode ? `replay: ${displayState.status || "stored"}` : transcriptStartWarning ? `${status} / transcript warning` : status;
  const transcriptStatus = replayMode
    ? transcriptBuckets.length > 0 ? "stored transcript" : "stored summary"
    : transcriptStreamState === "connected"
    ? `${asrRuntimeLabel(transcriptHealthQuery.data?.asr)} stream`
    : transcriptStreamState === "connecting"
    ? "connecting transcript stream"
    : liveTranscriptMatches || bucketTranscriptMatches
    ? transcriptLiveQuery.data?.error || transcriptBucketQuery.data?.error
      ? `error: ${transcriptLiveQuery.data?.error || transcriptBucketQuery.data?.error}`
      : transcriptLiveQuery.isError && transcriptBucketQuery.isError
        ? "transcript service offline"
      : transcriptLiveQuery.data?.status || transcriptBucketQuery.data?.status || "idle"
    : "idle";
  const activeTranscriptChunkSeconds = replayMode
    ? TRANSCRIPT_CHUNK_SECONDS_FALLBACK
    : positiveNumber(transcriptLiveQuery.data?.chunk_seconds)
      ?? positiveNumber(transcriptBucketQuery.data?.chunk_seconds)
      ?? TRANSCRIPT_CHUNK_SECONDS_FALLBACK;
  const healthMetrics = buildHealthMetrics(displayState, transcriptBuckets, transcriptStatus, replayMode, transcriptStartWarning, transcriptHealthQuery.data);
  const diagnosticMetrics = buildDashboardDiagnostics(displayState, transcriptBuckets, transcriptStatus, replayMode, transcriptHealthQuery.data);

  useEffect(() => {
    if (replayMode || !committedTranscriptBucketKey) return;
    setProvisionalTranscriptWindows((current) => {
      const filtered = current.filter((window) => !committedTranscriptBucketCoversWindow(window, transcriptBuckets));
      return filtered.length === current.length ? current : filtered;
    });
  }, [committedTranscriptBucketKey, replayMode, transcriptBuckets]);

  async function handleStart(channel: string) {
    const trimmed = channel.trim();
    if (!trimmed) return;
    setChannelInput(trimmed);
    await startMutation.mutateAsync(trimmed);
  }

  function applyTranscriptStreamPayload(payload: TranscriptStreamEvent) {
    if (!payload.type || payload.type === "keepalive") return;
    if (payload.session_id !== state.session_id) return;
    if (payload.type === "error") {
      queryClient.setQueryData<TranscriptState>(["transcript-live"], (current) => ({
        ...(current || {}),
        status: "error",
        session_id: state.session_id,
        error: payload.error || "Transcript stream error"
      }));
      return;
    }
    if (payload.type === "status") {
      queryClient.setQueryData<TranscriptState>(["transcript-live"], (current) => ({
        ...(current || {}),
        status: payload.status || current?.status || "ingesting",
        session_id: state.session_id
      }));
      return;
    }
    if (payload.type === "transcript_bucket") {
      const bucket = payload as unknown as TranscriptBucket;
      if (!bucket.text?.trim()) return;
      queryClient.setQueryData<TranscriptState>(["transcript-buckets"], (current) =>
        upsertTranscriptBucketState(current, bucket)
      );
      queryClient.setQueryData<DashboardState>(["dashboard-state"], (current) =>
        applyDashboardEvent(current || emptyState, {
          type: "transcript_bucket",
          session_id: bucket.session_id,
          channel: bucket.channel_id,
          transcript_bucket: bucket
        })
      );
      return;
    }
    if (!payload.text?.trim()) return;
    if (payload.type !== "transcript_partial" && payload.type !== "transcript_segment") return;
    setLiveSegments((current) => upsertTranscriptSegment(current, payload));
    queryClient.setQueryData<TranscriptState>(["transcript-live"], (current) =>
      upsertTranscriptLiveState(current, payload)
    );
    if (payload.type === "transcript_partial") {
      const window = provisionalTranscriptReactionWindow(payload);
      if (window) {
        setProvisionalTranscriptWindows((current) => upsertProvisionalTranscriptWindow(current, window));
      }
    }
  }

  function resolveTranscriptStreamTiming(payload: TranscriptStreamEvent): TranscriptStreamEvent {
    if (payload.type !== "transcript_partial") return payload;
    if (payload.transcript_start && payload.transcript_end) return payload;

    const nowMS = Date.now();
    const audioEndSeconds = positiveNumber(payload.audio_end_seconds);
    let endMS = nowMS;
    if (payload.session_id && audioEndSeconds) {
      const current = transcriptStreamClockRef.current;
      if (!current || current.sessionID !== payload.session_id) {
        transcriptStreamClockRef.current = {
          sessionID: payload.session_id,
          startedAtMS: nowMS - (audioEndSeconds * 1000)
        };
      }
      const timing = transcriptStreamClockRef.current;
      endMS = (timing?.startedAtMS ?? nowMS - (audioEndSeconds * 1000)) + (audioEndSeconds * 1000);
    }

    const estimatedDurationMS = partialTranscriptDurationMS(payload.text || "");
    const startMS = Math.max(endMS - estimatedDurationMS, endMS - 10000);
    const transcriptStart = payload.transcript_start || new Date(startMS).toISOString();
    const transcriptEnd = payload.transcript_end || new Date(endMS).toISOString();
    return {
      ...payload,
      transcript_start: transcriptStart,
      transcript_end: transcriptEnd,
      transcribed_at: payload.transcribed_at || new Date(nowMS).toISOString()
    };
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    void handleStart(channelInput);
  }

  function navigate(routePath: AppRoute) {
    if (routePath === route) return;
    window.history.pushState({}, "", routePath);
    setRoute(routePath);
  }

  if (route === "/eval") {
    return (
      <div className="analyst-app">
        <AnalystHeader state={state} status={status} clock={clock} route={route} onNavigate={navigate} />
        <EvaluationLabPage
          sessions={historyQuery.data?.sessions || []}
          selectedSessionID={selectedEvaluationSessionID}
          replay={replayQuery.data}
          loading={historyQuery.isLoading || replayQuery.isFetching}
          error={historyQuery.isError ? "History unavailable" : replayQuery.isError ? "Replay unavailable" : ""}
          labels={replayQuery.data?.window_labels || []}
          evaluation={evaluationQuery.data?.evaluation}
          evaluationLoading={evaluationQuery.isFetching}
          evaluationError={evaluationQuery.isError ? errorMessage(evaluationQuery.error, "Evaluation metrics unavailable.") : ""}
          evaluationPartial={Boolean(evaluationQuery.data?.partial)}
          evaluationTruncations={evaluationQuery.data?.truncated_sources}
          onSelect={setSelectedEvaluationSessionID}
          onBack={() => navigate("/")}
          onSaveEvaluation={(label) => windowLabelMutation.mutateAsync(label)}
        />
        <AnalystFooter state={displayState} status={displayStatus} clock={clock} replay={replayMode} />
      </div>
    );
  }

  return (
    <div className="analyst-app">
      <AnalystHeader state={state} status={status} clock={clock} route={route} onNavigate={navigate} />
      <main className="analyst-main">
        <HeroFeed
          channelInput={channelInput}
          onChannelInput={setChannelInput}
          onSubmit={handleSubmit}
          pending={startMutation.isPending}
          state={displayState}
          status={displayStatus}
          stream={displayState.stream}
          healthMetrics={healthMetrics}
          diagnosticMetrics={diagnosticMetrics}
          transcriptWarning={replayMode ? "" : transcriptStartWarning}
        />
        <section className="content-strip" aria-label="Live stream analysis">
          <ModernTranscriptPanel
            title={replayMode ? "Replay Transcript" : "Live Transcript"}
            status={transcriptStatus}
            chunkSeconds={activeTranscriptChunkSeconds}
            segments={transcriptRows}
            metricSegments={transcriptMetricSegments}
            health={transcriptHealthQuery.data}
            clock={clock}
            liveActive={!replayMode && Boolean(state.session_id)}
            replay={replayMode}
            offsetSeconds={transcriptOffsetSeconds}
            onOffsetChange={(value) => setTranscriptOffsetSeconds(clampTranscriptOffset(value))}
          />
          <ModernChatPanel
            buckets={displayState.buckets || []}
            messages={displayState.messages || []}
            liveActive={!replayMode && Boolean(state.session_id)}
            replay={replayMode}
            loading={dashboardQuery.isLoading}
            error={dashboardQuery.isError ? "Dashboard state unavailable." : ""}
            chatAvailable={chatAvailable}
          />
          <ModernAnalyticsPanel
            alignments={displayState.alignments || []}
            buckets={displayState.buckets || []}
            reactionWindows={reactionWindows}
            transcriptBuckets={transcriptBuckets}
            messages={displayState.messages || []}
            liveActive={!replayMode && Boolean(state.session_id)}
            replay={replayMode}
          />
          <LiveDemoEvidencePanel
            alignments={displayState.alignments || []}
            buckets={displayState.buckets || []}
            reactionWindows={reactionWindows}
            transcriptBuckets={transcriptBuckets}
            messages={displayState.messages || []}
            liveActive={!replayMode && Boolean(state.session_id)}
            replay={replayMode}
          />
        </section>
      </main>
      <AnalystFooter state={displayState} status={displayStatus} clock={clock} replay={replayMode} />
    </div>
  );
}

function AnalystHeader({
  state,
  status,
  clock,
  route,
  onNavigate
}: {
  state: DashboardState;
  status: string;
  clock: Date;
  route: AppRoute;
  onNavigate: (route: AppRoute) => void;
}) {
  const session = state.session_id ? state.session_id.slice(-4).toUpperCase() : "0492";
  const unique = uniqueChatters(state.buckets || [], state.messages || []);
  const viewers = typeof state.stream?.viewer_count === "number"
    ? compactNumber(state.stream.viewer_count, 0)
    : state.session_id && unique > 0 ? compactNumber(unique, 0) : "0";
  return (
    <header className="analyst-header">
      <div>SOURCE: {state.channel ? state.channel.toUpperCase() : "NO STREAM"}</div>
      <div className="header-center">
        <span>STREAM REACTION INTELLIGENCE - SESSION {session}</span>
        <nav className="header-nav" aria-label="Workspace">
          <button className={route === "/" ? "selected" : ""} type="button" onClick={() => onNavigate("/")}>Dashboard</button>
          <button className={route === "/eval" ? "selected" : ""} type="button" onClick={() => onNavigate("/eval")}>Eval Lab</button>
        </nav>
      </div>
      <div className="header-right">
        <span>{clock.toLocaleDateString([], { month: "short", day: "2-digit", year: "numeric" })}</span>
        <div className="viewer-badge" aria-label={`${viewers} viewers`}>
          <span className="viewer-count">{viewers}</span>
          <span className="viewer-label">VIEWERS</span>
        </div>
      </div>
    </header>
  );
}

function routeFromPath(pathname: string): AppRoute {
  return pathname === "/eval" ? "/eval" : "/";
}

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

function HeroFeed({
  channelInput,
  onChannelInput,
  onSubmit,
  pending,
  state,
  status,
  stream,
  healthMetrics,
  diagnosticMetrics,
  transcriptWarning
}: {
  channelInput: string;
  onChannelInput: (value: string) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  pending: boolean;
  state: DashboardState;
  status: string;
  stream?: StreamInfo;
  healthMetrics: HealthMetric[];
  diagnosticMetrics: DiagnosticMetric[];
  transcriptWarning?: string;
}) {
  const channel = state.channel || extractChannel(channelInput);
  const requestedChannel = extractChannel(channelInput);
  const switchingStream = Boolean(state.session_id && requestedChannel && requestedChannel !== state.channel);
  const startLabel = pending ? "Starting" : switchingStream ? "Switch" : "Start";
  const title = stream?.title || (channel ? `${channel} stream monitor` : "Stream Monitor");
  const caption = streamCaption(stream, channel, status);
  return (
    <section className="hero-feed">
      <StreamViewport channel={channel} stream={stream} status={status} pending={pending} />
      <div className="hero-meta">
        <h1 className="stream-title">{title}</h1>
        <p className="stream-caption">{caption}</p>
        <div className="hero-signal">
          STATUS: <span>{status.toUpperCase()}</span> / {channel ? `CAM_${channel.toUpperCase()}` : "CAM_01"}
        </div>
        <form className="hero-start-form" onSubmit={onSubmit}>
          <input
            value={channelInput}
            onChange={(event) => onChannelInput(event.target.value)}
            placeholder="twitch.tv/channel or youtube.com/watch?v=..."
            autoComplete="off"
          />
          <button type="submit" disabled={pending} aria-label={pending ? "Starting session" : switchingStream ? "Switch stream" : "Start session"}>
            <Play size={15} />
            {startLabel}
          </button>
        </form>
        {transcriptWarning ? (
          <p className="start-warning">Transcript warning: {transcriptWarning}</p>
        ) : null}
        <HealthStrip metrics={healthMetrics} />
        <DiagnosticsDetails title="Diagnostics" metrics={diagnosticMetrics} />
      </div>
    </section>
  );
}

function StreamViewport({
  channel,
  stream,
  status,
  pending
}: {
  channel?: string;
  stream?: StreamInfo;
  status: string;
  pending: boolean;
}) {
  const channelName = channel ? extractChannel(channel) : "";
  const sourceURL = stream?.url || "";
  const platform = stream?.platform || streamPlatformFromURL(sourceURL) || "twitch";
  const youtubeEmbedUrl = platform === "youtube" ? youtubePlayerUrl(sourceURL) : "";
  const twitchEmbedUrl = platform === "twitch" ? twitchPlayerUrl(channelName) : "";
  const externalUrl = sourceURL || (channelName ? `https://www.twitch.tv/${channelName}` : "");
  const thumbnailUrl = stream?.thumbnail_url || "";
  const message = streamViewportMessage(channelName, status, pending, platform);
  const hasVideoSource = Boolean(youtubeEmbedUrl || twitchEmbedUrl);

  return (
    <div className={`video-viewport ${hasVideoSource ? "has-feed" : ""}`}>
      {hasVideoSource ? (
        <>
          <div className="video-frame">
            {thumbnailUrl ? <img className="video-preview-image" src={thumbnailUrl} alt="" /> : null}
            <iframe
              key={`${platform}-${externalUrl || channelName}`}
              title={`${platform === "youtube" ? "YouTube" : "Twitch"} livestream preview`}
              src={youtubeEmbedUrl || twitchEmbedUrl}
              allow="autoplay; encrypted-media; fullscreen; picture-in-picture"
              allowFullScreen
              referrerPolicy="origin"
            />
          </div>
          {externalUrl ? (
            <a className="video-fallback-note" href={externalUrl} target="_blank" rel="noreferrer">
              Open {platform === "youtube" ? "YouTube" : "Twitch"}
            </a>
          ) : null}
          {message ? <div className={`video-state-note ${message.tone}`}>{message.text}</div> : null}
        </>
      ) : (
        <div className="video-empty-state">
          <strong>Stream preview</strong>
          <span>Enter a Twitch channel or YouTube live URL to load the player.</span>
        </div>
      )}
    </div>
  );
}

function HealthStrip({ metrics }: { metrics: HealthMetric[] }) {
  return (
    <div className="health-strip" aria-label="Live pipeline health">
      {metrics.map((metric) => (
        <div className={`health-chip ${metric.tone}`} key={metric.label}>
          <span>{metric.label}</span>
          <strong>{metric.value}</strong>
          <em>{metric.meta}</em>
        </div>
      ))}
    </div>
  );
}

function DiagnosticsDetails({ title, metrics }: { title: string; metrics: DiagnosticMetric[] }) {
  if (metrics.length === 0) return null;
  return (
    <details className="diagnostics-details">
      <summary>{title}</summary>
      <div className="diagnostics-grid">
        {metrics.map((metric) => (
          <div key={metric.label}>
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
            {metric.meta ? <em>{metric.meta}</em> : null}
          </div>
        ))}
      </div>
    </details>
  );
}

function ModernTranscriptPanel({
  title = "Live Transcript",
  status,
  chunkSeconds,
  segments,
  metricSegments,
  health,
  clock,
  liveActive,
  replay = false,
  offsetSeconds = 0,
  onOffsetChange
}: {
  title?: string;
  status: string;
  chunkSeconds: number;
  segments: TranscriptSegment[];
  metricSegments?: TranscriptSegment[];
  health?: TranscriptHealth;
  clock: Date;
  liveActive: boolean;
  replay?: boolean;
  offsetSeconds?: number;
  onOffsetChange?: (value: number) => void;
}) {
  const activeKeyRef = useRef("");
  const safeOffsetSeconds = clampTranscriptOffset(offsetSeconds);
  const captions = useMemo(() => liveClosedCaptionBlocks(segments, 80), [segments]);
  const activeCaptionIndex = selectedCaptionIndex(captions, clock, safeOffsetSeconds, replay);
  const activeCaption = activeCaptionIndex >= 0 ? captions[activeCaptionIndex] : undefined;
  const previousCaptions = activeCaptionIndex >= 0 ? captions.slice(Math.max(0, activeCaptionIndex - 2), activeCaptionIndex) : [];
  const feedCaptions = useMemo(() => captions.slice(-24).reverse(), [captions]);
  const activeWords = useMemo(() => captionWords(activeCaption?.text || ""), [activeCaption?.text]);
  const [visibleWordCount, setVisibleWordCount] = useState(0);
  const [transcriptView, setTranscriptView] = useState<"feed" | "caption">("feed");
  const qualityMetrics = useMemo(
    () => transcriptQualityMetrics(metricSegments || segments, health, clock),
    [clock, health, metricSegments, segments]
  );
  const sourceMeta = transcriptSourceMeta(status, chunkSeconds, replay);
  const delayMeta = !replay ? formatTranscriptDelay(transcriptDelaySeconds(activeCaption, clock)) : "";
  const meta = displayTranscriptFinalityLabel(status);
  const emptyMessage = transcriptEmptyMessage(status, liveActive, replay, chunkSeconds);
  const diagnostics = transcriptPanelDiagnostics(qualityMetrics, sourceMeta, delayMeta, safeOffsetSeconds, replay);

  useEffect(() => {
    if (!activeCaption) {
      activeKeyRef.current = "";
      setVisibleWordCount(0);
      return;
    }

    const isNewCaption = activeKeyRef.current !== activeCaption.key;
    const startingWords = isNewCaption ? Math.min(2, activeWords.length) : 0;
    activeKeyRef.current = activeCaption.key;
    setVisibleWordCount((current) => isNewCaption ? startingWords : Math.min(current, activeWords.length));

    const wordDelay = Math.max(70, Math.min(180, (chunkSeconds * 1000) / Math.max(activeWords.length, 1)));
    const timer = window.setInterval(() => {
      setVisibleWordCount((current) => current >= activeWords.length ? current : current + 1);
    }, wordDelay);
    return () => window.clearInterval(timer);
  }, [activeCaption, activeWords.length, chunkSeconds]);

  return (
    <section className="modern-panel transcript-modern">
      <div className="modern-panel-header">{title} <span>{meta}</span></div>
      <div className="transcript-view-tabs" role="tablist" aria-label="Transcript view">
        <button
          type="button"
          className={transcriptView === "feed" ? "active" : ""}
          onClick={() => setTranscriptView("feed")}
          role="tab"
          aria-selected={transcriptView === "feed"}
        >
          Feed
        </button>
        <button
          type="button"
          className={transcriptView === "caption" ? "active" : ""}
          onClick={() => setTranscriptView("caption")}
          role="tab"
          aria-selected={transcriptView === "caption"}
        >
          Captions
        </button>
      </div>
      <div className={transcriptView === "feed" ? "transcript-feed-stage" : "cc-stage"}>
        {captions.length === 0 || (transcriptView === "caption" && !activeCaption) ? (
          <p className="empty-modern cc-empty">
            {emptyMessage}
          </p>
        ) : transcriptView === "feed" ? (
          <div className="transcript-full-feed" aria-label="Recent transcript feed">
            {feedCaptions.map((caption, index) => (
              <article
                className={`transcript-feed-row ${caption.key === activeCaption?.key ? "is-current" : ""}`}
                key={caption.key}
              >
                <span className="cc-line-meta">
                  <span>{index === 0 ? "Latest" : "Recent"}</span>
                  <time>{formatCompactTime(caption.start)}</time>
                </span>
                <p>{caption.text}</p>
              </article>
            ))}
          </div>
        ) : (
          <div className="cc-stack">
            <div className="cc-history">
              <AnimatePresence initial={false}>
                {previousCaptions.map((caption) => (
                  <motion.div
                    className="cc-history-row"
                    key={caption.key}
                    initial={{ opacity: 0, y: 8 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -6 }}
                    transition={{ duration: 0.16 }}
                  >
                    <span className="cc-line-meta"><span>Prev</span><time>{formatCompactTime(caption.start)}</time></span>
                    <p className="cc-history-line">{caption.text}</p>
                  </motion.div>
                ))}
              </AnimatePresence>
              {activeCaption ? (
                <motion.div
                  className="cc-history-row is-live"
                  key={activeCaption.key}
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.16 }}
                  aria-live="polite"
                >
                  <span className="cc-line-meta"><span>Current</span><time>{formatCompactTime(activeCaption.start)}</time></span>
                  <p className="cc-history-line">
                    {activeWords.slice(0, visibleWordCount).map((word, index) => (
                      <motion.span
                        className="cc-word"
                        key={`${activeCaption.key}-${index}-${word}`}
                        initial={{ opacity: 0, y: 5 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.12 }}
                      >
                        {word}
                        {index < visibleWordCount - 1 ? " " : ""}
                      </motion.span>
                    ))}
                  </p>
                </motion.div>
              ) : null}
            </div>
          </div>
        )}
      </div>
      <TranscriptDiagnosticsDetails metrics={diagnostics} />
      {!replay && onOffsetChange ? (
        <TranscriptSyncControl
          value={safeOffsetSeconds}
          disabled={captions.length === 0}
          onChange={onOffsetChange}
        />
      ) : null}
    </section>
  );
}

function TranscriptDiagnosticsDetails({ metrics }: { metrics: DiagnosticMetric[] }) {
  return (
    <details className="transcript-diagnostics">
      <summary>Transcript diagnostics</summary>
      <div className="transcript-metrics-strip" aria-label="Transcript quality metrics">
        {metrics.map((metric) => (
          <div title={`${metric.label}: ${metric.value}`} key={metric.label}>
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
          </div>
        ))}
      </div>
    </details>
  );
}

function TranscriptSyncControl({
  value,
  disabled,
  onChange
}: {
  value: number;
  disabled: boolean;
  onChange: (value: number) => void;
}) {
  const clampedValue = clampTranscriptOffset(value);

  return (
    <div className="transcript-sync-control">
      <div>
        <span>Preview delay</span>
        <strong>{formatOffsetSeconds(clampedValue)}</strong>
      </div>
      <button
        type="button"
        onClick={() => onChange(clampTranscriptOffset(clampedValue - 5))}
        disabled={clampedValue <= -120}
        aria-label="Show newer transcript captions"
        title="Show newer transcript captions"
      >
        <ChevronLeft size={13} />
      </button>
      <input
        type="range"
        min={-120}
        max={120}
        step={5}
        value={clampedValue}
        disabled={disabled}
        onChange={(event) => onChange(clampTranscriptOffset(Number(event.target.value)))}
        aria-label="Transcript preview delay"
      />
      <button
        type="button"
        onClick={() => onChange(clampTranscriptOffset(clampedValue + 5))}
        disabled={clampedValue >= 120}
        aria-label="Show older transcript captions"
        title="Show older transcript captions"
      >
        <ChevronRight size={13} />
      </button>
      <button
        type="button"
        onClick={() => onChange(0)}
        disabled={clampedValue === 0}
        aria-label="Reset transcript preview delay"
        title="Reset transcript preview delay"
      >
        <RotateCcw size={12} />
      </button>
    </div>
  );
}

function ModernChatPanel({
  buckets,
  messages,
  liveActive,
  replay = false,
  loading = false,
  error = "",
  chatAvailable = true
}: {
  buckets: ChatBucket[];
  messages: ChatMessage[];
  liveActive: boolean;
  replay?: boolean;
  loading?: boolean;
  error?: string;
  chatAvailable?: boolean;
}) {
  const latestBucket = buckets[0];
  const rows = useMemo(() => liveChatRows(messages), [messages]);
  const hasChatData = Boolean(latestBucket || messages.length > 0);
  const distribution = chatSentimentDistribution(latestBucket, hasChatData);
  const positive = distribution.positive;
  const neutral = distribution.neutral;
  const negative = distribution.negative;
  const rate = latestBucket ? bucketRate(latestBucket) : messages.length;
  const previousRate = bucketRate(buckets[1]);
  const sentiment = sentimentDescriptor(latestBucket?.chat_sentiment);
  const peakRate = Math.max(rate, ...buckets.slice(0, 20).map(bucketRate));
  const analyzed = latestBucket?.analyzed_count ?? latestBucket?.message_count ?? messages.length ?? 0;
  const windowLabel = latestBucket ? `${formatCompactTime(latestBucket.bucket_start)}-${formatCompactTime(latestBucket.bucket_end)}` : "N/A";
  const trend = hasChatData ? trendLabel(rate, previousRate) : "N/A";
  const chatDelay = latestBucket ? formatLatency(latestBucket.analysis_latency_ms) : "N/A";
  const percentLabel = (value: number) => hasChatData ? `${value}%` : "N/A";
  const emptyMessage = chatEmptyMessage({ replay, liveActive, loading, error, chatAvailable });
  return (
    <section className="modern-panel chat-modern">
      <div className="modern-panel-header">{replay ? "Replay Chat" : "Live Chat"} <span>{error || (loading ? "Loading" : `${compactNumber(rate, 0)} msg/min`)}</span></div>
      <div className="chat-analytics analytical-chat compact-chat-analysis">
        <div className="chat-analysis-topline">
          <MetricLabel label="Audience Mood" />
          <strong className={`sentiment-read ${sentiment.tone}`}>{formatSignedNumber(latestBucket?.chat_sentiment)}</strong>
        </div>
        <div className="sentiment-track compact-track">
          <div className="s-pos" style={{ width: `${positive}%` }} />
          <div className="s-neu" style={{ width: `${neutral}%` }} />
          <div className="s-neg" style={{ width: `${negative}%` }} />
        </div>
        <div className="compact-analysis-grid">
          <div><span>POS</span><strong>{percentLabel(positive)}</strong></div>
          <div><span>NEU</span><strong>{percentLabel(neutral)}</strong></div>
          <div><span>NEG</span><strong>{percentLabel(negative)}</strong></div>
          <div><span>Label</span><strong>{hasChatData ? sentiment.label : "N/A"}</strong></div>
          <div><span>Delay</span><strong>{chatDelay}</strong></div>
          <div><span>Scored</span><strong>{compactNumber(analyzed, 0)}</strong></div>
        </div>
        <div className="compact-velocity-strip">
          <div><span>Rate</span><strong>{compactNumber(rate, 0)}/min</strong></div>
          <div><span>Trend</span><strong>{trend}</strong></div>
          <div><span>Peak</span><strong>{compactNumber(peakRate, 0)}/min</strong></div>
          <div><span>Window</span><strong>{windowLabel}</strong></div>
        </div>
      </div>
      <div className="scroll-area">
        {rows.length === 0 ? (
          <p className="empty-modern">{emptyMessage}</p>
        ) : (
          rows.map((row) => (
            <motion.div className="chat-entry" key={row.key} initial={{ opacity: 0, y: 5 }} animate={{ opacity: 1, y: 0 }}>
              <ChatRowTime time={row.time} />
              <span className="chat-entry-body">
                <span className="user-id">{row.user}</span>
                <span className="chat-text">{row.text}</span>
              </span>
            </motion.div>
          ))
        )}
      </div>
    </section>
  );
}

function ChatRowTime({ time }: { time: string }) {
  const displayTime = time || "--:--:--";
  const match = displayTime.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?/);
  const primary = match ? `${match[1]}:${match[2]}` : displayTime;
  const secondary = match?.[3] ? `:${match[3]}` : undefined;

  return (
    <time className="chat-time" aria-label={displayTime} title={displayTime}>
      <span>{primary}</span>
      {secondary ? <span>{secondary}</span> : null}
    </time>
  );
}

function ModernAnalyticsPanel({
  alignments,
  buckets,
  reactionWindows,
  transcriptBuckets,
  messages,
  liveActive,
  replay = false
}: {
  alignments: AlignmentBucket[];
  buckets: ChatBucket[];
  reactionWindows: ReactionWindow[];
  transcriptBuckets: TranscriptBucket[];
  messages: ChatMessage[];
  liveActive: boolean;
  replay?: boolean;
}) {
  const [inspectedBucket, setInspectedBucket] = useState<BucketInspectorSelection | null>(null);
  const latestBucket = buckets[0];
  const latestReactionWindow = reactionWindows[0];
  const latestAlignment = alignments[0];
  const sentimentPoints = useMemo(() => buildSentimentPoints(alignments, buckets, transcriptBuckets), [alignments, buckets, transcriptBuckets]);
  const reactionPoints = useMemo(() => buildReactionWindowPoints(reactionWindows), [reactionWindows]);
  const latestSentimentPoint = sentimentPoints[sentimentPoints.length - 1];
  const latestChat = latestAlignment?.chat_sentiment ?? latestBucket?.chat_sentiment ?? latestSentimentPoint?.chat;
  const latestTranscript = latestAlignment?.transcript_sentiment ?? latestSentimentPoint?.transcript;
  const latestAggregate = averageSignals(latestChat, latestTranscript) ?? latestSentimentPoint?.aggregate ?? latestChat ?? latestTranscript;
  const latestGap = typeof latestAlignment?.delta === "number"
    ? Math.abs(latestAlignment.delta)
      : typeof latestChat === "number" && typeof latestTranscript === "number"
      ? Math.abs(latestChat - latestTranscript)
      : 0;
  const keywords = keywordRows(latestBucket, messages);
  const topEmotes = latestBucket?.top_emotes?.filter(Boolean).slice(0, 3) || [];
  const agreementLabel = latestAlignment ? relationshipStatus(relationshipLabel(latestAlignment.relationship).key) : latestTranscript ? "matching" : "waiting";
  const processingDelay = processingDelayLabel(latestBucket, transcriptBuckets[0]);
  const hasProvisionalTranscriptWindow = reactionWindows.some((window) => isTranscriptReactionWindow(window) && window.provisional);
  const sentimentChartMode = typeof latestTranscript === "number"
    ? "Time x-axis, fixed sentiment y-axis, hover for window details"
    : latestBucket ? "Chat-only timeline until raw transcript buckets align" : replay ? "No stored alignment buckets returned for this replay" : liveActive ? "Waiting for live chat and transcript data" : "Start a live session to collect chat and transcript windows";
  const reactionChartMode = reactionChartDescription(hasProvisionalTranscriptWindow, replay, liveActive);
  const aggregateValues = sentimentPoints.map((point) => point.aggregate ?? point.chat ?? point.transcript).filter((value): value is number => typeof value === "number");
  const chartRange = valueRange(aggregateValues);
  const chartTrend = valueTrend(aggregateValues);
  const primaryInsight = primaryInsightForWindow(latestReactionWindow, transcriptBuckets[0], latestBucket, latestAlignment);
  return (
    <>
      <section className="modern-panel analytics-modern">
        <div className="modern-panel-header">{replay ? "Replay Analytics" : "Real-Time Analytics"} <span>{replay ? "Stored Session" : "Last 5 Min"}</span></div>
        <div className="scroll-area">
          <div className="analytics-subgrid">
            <PrimaryInsightCards insight={primaryInsight} />

            <div className="topic-analysis">
              <MetricLabel label={dashboardChartHeadings.sentiment} />
              <p className="analysis-copy">{sentimentChartMode}</p>
              <SentimentTimelineChart
                points={sentimentPoints}
                buckets={buckets}
                emptyMessage="Waiting for at least two scored sentiment windows."
              />
              <div className="topic-stat-grid">
                <div><span>Chat</span><strong className="chat-color">{formatSignedNumber(latestChat)}</strong></div>
                <div><span>Transcript</span><strong className="transcript-color">{formatSignedNumber(latestTranscript)}</strong></div>
                <div><span>Aggregate</span><strong>{formatSignedNumber(latestAggregate)}</strong></div>
                <div><span>Gap</span><strong className={latestGap >= 0.5 ? "risk-color" : "ok-color"}>{latestGap.toFixed(2)}</strong></div>
                <div><span>Range</span><strong>{chartRange.toFixed(2)}</strong></div>
                <div><span>Trend</span><strong className={chartTrend >= 0 ? "ok-color" : "risk-color"}>{formatSignedNumber(chartTrend)}</strong></div>
              </div>
            </div>

            <div className="reaction-chart-block">
              <MetricLabel label={dashboardChartHeadings.reaction} />
              <p className="analysis-copy">{reactionChartMode}</p>
              <LiveReactionTimelineChart
                points={reactionPoints}
                buckets={buckets}
                emptyMessage="Waiting for at least two live reaction windows."
              />
              <ReactionDiagnosticsDetails window={latestReactionWindow} />
            </div>

            <ThirtySecondBucketSections buckets={buckets} transcriptBuckets={transcriptBuckets} onInspect={setInspectedBucket} />

            <div className="analytics-two-up demo-metric-strip">
              <div>
                <MetricLabel label="Chat / Voice Agreement" />
                <div className="metric-value agreement-value">{agreementLabel}</div>
              </div>
              <div>
                <MetricLabel label="Processing Delay" />
                <div className={`metric-value ${processingDelay.tone === "warn" || processingDelay.tone === "error" ? "delay-value warn" : "delay-value"}`}>{processingDelay.label}</div>
              </div>
            </div>

            <div className="signal-context-grid">
              <div>
                <MetricLabel label="Top Topics" />
                {keywords.length === 0 ? (
                  <p className="empty-modern">No topics yet.</p>
                ) : (
                  keywords.slice(0, 3).map((keyword) => (
                    <div className="keyword-row" key={keyword.term}>
                      <span>{keyword.term}</span>
                      <span>{keyword.count}</span>
                    </div>
                  ))
                )}
              </div>
              <div>
                <MetricLabel label="Top Emotes" />
                {topEmotes.length === 0 ? (
                  <p className="empty-modern">No emotes yet.</p>
                ) : (
                  topEmotes.map((emote, index) => (
                    <div className="keyword-row" key={`${emote}-${index}`}>
                      <span>{emote}</span>
                      <span>{index + 1}</span>
                    </div>
                  ))
                )}
              </div>
            </div>

          </div>
        </div>
      </section>
      <AnimatePresence>
        {inspectedBucket ? <BucketInspectorModal selection={inspectedBucket} onClose={() => setInspectedBucket(null)} /> : null}
      </AnimatePresence>
    </>
  );
}

function LiveDemoEvidencePanel({
  alignments,
  buckets,
  reactionWindows,
  transcriptBuckets,
  messages,
  liveActive,
  replay = false
}: {
  alignments: AlignmentBucket[];
  buckets: ChatBucket[];
  reactionWindows: ReactionWindow[];
  transcriptBuckets: TranscriptBucket[];
  messages: ChatMessage[];
  liveActive: boolean;
  replay?: boolean;
}) {
  const latestWindow = reactionWindows[0];
  const latestBucket = buckets[0];
  const latestTranscriptBucket = transcriptBuckets[0];
  const latestAlignment = alignments[0];
  const audienceMood = sentimentDescriptor(latestBucket?.chat_sentiment ?? latestWindow?.valence);
  const relationship = latestAlignment ? relationshipStatus(relationshipLabel(latestAlignment.relationship).key) : latestTranscriptBucket ? "matching" : "waiting";
  const delay = processingDelayLabel(latestBucket, latestTranscriptBucket);
  const evidenceMessages = latestWindow?.evidence_messages?.filter((message) => message.text).slice(0, 3) || [];
  const transcriptEvidence = latestWindow?.transcript_text || latestTranscriptBucket?.text || "";
  const topics = keywordRows(latestBucket, messages).slice(0, 5);
  const emotes = latestBucket?.top_emotes?.filter(Boolean).slice(0, 5) || [];
  const status = latestWindow?.reaction_type && latestWindow.reaction_type !== "unknown"
    ? humanizeLabel(latestWindow.reaction_type)
    : liveActive || replay ? "collecting" : "standby";
  const headline = latestWindow
    ? reactionEvidenceLabel(latestWindow)
    : liveActive || replay ? "Waiting for the next detected audience moment." : "Start a stream to collect live evidence.";

  return (
    <section className="modern-panel live-demo-evidence">
      <div className="modern-panel-header">{replay ? "Replay Evidence" : "Signal Evidence"} <span>{status}</span></div>
      <div className="scroll-area evidence-scroll">
        <section className="signal-evidence-hero">
          <span>Why this signal?</span>
          <strong>{headline}</strong>
          <p>{transcriptEvidence ? truncateText(transcriptEvidence, 180) : "Transcript evidence appears here when speech windows are available."}</p>
        </section>

        <div className="evidence-metric-grid">
          <div>
            <span>Audience mood</span>
            <strong className={`sentiment-read ${audienceMood.tone}`}>{audienceMood.label}</strong>
          </div>
          <div>
            <span>Agreement</span>
            <strong>{relationship}</strong>
          </div>
          <div>
            <span>Delay</span>
            <strong className={delay.tone === "warn" || delay.tone === "error" ? "risk-color" : "ok-color"}>{delay.label}</strong>
          </div>
          <div>
            <span>Messages</span>
            <strong>{compactNumber(latestBucket?.message_count || latestAlignment?.chat_message_count || messages.length || 0, 0)}</strong>
          </div>
        </div>

        <section className="evidence-section">
          <MetricLabel label="Representative Chat" />
          {evidenceMessages.length === 0 ? (
            <p className="empty-modern">No reaction-specific chat evidence yet.</p>
          ) : (
            evidenceMessages.map((message) => (
              <blockquote key={message.message_id || `${message.timestamp}-${message.text}`}>
                <span>{message.display_name || message.username || "chat"}</span>
                <p>{message.text}</p>
              </blockquote>
            ))
          )}
        </section>

        <section className="signal-context-grid evidence-context-grid">
          <div>
            <MetricLabel label="Topics" />
            {topics.length === 0 ? (
              <p className="empty-modern">No topics yet.</p>
            ) : (
              topics.map((topic) => (
                <div className="keyword-row" key={topic.term}>
                  <span>{topic.term}</span>
                  <span>{topic.count}</span>
                </div>
              ))
            )}
          </div>
          <div>
            <MetricLabel label="Emotes" />
            {emotes.length === 0 ? (
              <p className="empty-modern">No emotes yet.</p>
            ) : (
              emotes.map((emote, index) => (
                <div className="keyword-row" key={`${emote}-${index}`}>
                  <span>{emote}</span>
                  <span>{index + 1}</span>
                </div>
              ))
            )}
          </div>
        </section>

        <details className="demo-advanced-details">
          <summary>Technical proof lives in Eval Lab</summary>
          <p>Replay review, label coverage, regression proof, and model diagnostics stay out of the main live demo but remain available from the Eval Lab route.</p>
        </details>
      </div>
    </section>
  );
}

function PrimaryInsightCards({ insight }: { insight: PrimaryInsight }) {
  return (
    <section className="primary-insight-grid" aria-label="Current stream insight">
      <InsightCard label="Current reaction" value={insight.reaction} tone={insight.reactionTone} />
      <InsightCard label="Target" value={insight.target} />
      <InsightCard label="Intensity" value={insight.intensity} />
      <InsightCard label="Chat / voice agreement" value={insight.agreement} />
      <InsightCard label="Processing delay" value={insight.delay} healthTone={insight.delayTone} />
      <InsightCard label="Evidence" value={insight.evidence} wide />
    </section>
  );
}

function InsightCard({
  label,
  value,
  tone,
  healthTone,
  wide = false
}: {
  label: string;
  value: string;
  tone?: ReadTone;
  healthTone?: HealthMetricTone;
  wide?: boolean;
}) {
  const toneClass = tone ? ` ${tone}` : healthTone ? ` ${healthTone}` : "";
  return (
    <div className={`primary-insight-card${wide ? " wide" : ""}`}>
      <span>{label}</span>
      <strong className={toneClass.trim() || undefined}>{value}</strong>
    </div>
  );
}

function SentimentTimelineChart({
  points,
  buckets,
  emptyMessage
}: {
  points: SentimentPoint[];
  buckets: ChatBucket[];
  emptyMessage: string;
}) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const chart = useMemo(() => buildTimelineCanvasData(points, buckets), [points, buckets]);
  const layout = useMemo(() => timelineCanvasLayout(chart.points, 360, 180), [chart.points]);
  const chatPath = miniTimelinePath(layout.points, "chatY");
  const transcriptPath = miniTimelinePath(layout.points, "transcriptY");
  const aggregatePath = miniTimelinePath(layout.points, "aggregateY");
  const activePoint = typeof activeIndex === "number" ? layout.points[activeIndex] : undefined;
  const activeTooltipX = activePoint ? `${(activePoint.x / layout.width) * 100}%` : undefined;
  const activeTooltipY = activePoint ? `${timelineTooltipTopPercent(activePoint, layout.height)}%` : undefined;
  const activeTooltipFlipped = activePoint ? activePoint.x > layout.width * 0.62 : false;

  if (chart.points.length < 2) {
    return (
      <div className="chart-box analytical-chart-box sentiment-axis-chart empty-axis-chart">
        <span>{emptyMessage}</span>
      </div>
    );
  }

  return (
    <div className="chart-box analytical-chart-box sentiment-axis-chart svg-sentiment-chart" onMouseLeave={() => setActiveIndex(null)}>
      <svg viewBox={`0 0 ${layout.width} ${layout.height}`} preserveAspectRatio="none" role="img" aria-label="Chat, transcript, and aggregate sentiment over time">
        <rect className="mini-chart-plot-bg" x={layout.padding.left} y={layout.padding.top} width={layout.plotWidth} height={layout.plotHeight} rx="5" />
        {layout.yTicks.map((tick) => (
          <g key={tick.value}>
            <line className={tick.value === 0 ? "mini-chart-zero" : "mini-chart-grid"} x1={layout.padding.left} x2={layout.width - layout.padding.right} y1={tick.y} y2={tick.y} />
            <text className="mini-chart-y-label" x={layout.padding.left - 7} y={tick.y + 3} textAnchor="end">
              {formatAxisTick(tick.value)}
            </text>
          </g>
        ))}
        {layout.points.map((point, index) => {
          const bounds = timelineCanvasCellBounds(layout.points, index, layout.padding.left, layout.width - layout.padding.right);
          const barHeight = Math.max(2, Math.min(16, (point.activity / chart.maxActivity) * 16));
          return (
            <rect
              className="mini-chart-activity"
              key={`${point.key}-activity`}
              x={roundSvg(bounds.x + 1)}
              y={roundSvg(layout.height - layout.padding.bottom - barHeight)}
              width={roundSvg(Math.max(2, bounds.width - 2))}
              height={roundSvg(barHeight)}
              rx="1.5"
            />
          );
        })}
        {layout.points.map((point, index) => {
          if (typeof point.chat !== "number" || typeof point.transcript !== "number" || typeof point.chatY !== "number" || typeof point.transcriptY !== "number") return null;
          const bounds = timelineCanvasCellBounds(layout.points, index, layout.padding.left, layout.width - layout.padding.right);
          const split = Math.abs(point.chat - point.transcript) >= 0.35;
          return (
            <rect
              className={`mini-chart-gap${split ? " split" : " close"}`}
              key={`${point.key}-gap`}
              x={roundSvg(bounds.x)}
              y={roundSvg(Math.min(point.chatY, point.transcriptY))}
              width={roundSvg(bounds.width)}
              height={roundSvg(Math.max(1, Math.abs(point.chatY - point.transcriptY)))}
              rx="1.5"
            />
          );
        })}
        {aggregatePath ? <path className="mini-chart-line aggregate" d={aggregatePath} /> : null}
        {transcriptPath ? <path className="mini-chart-line transcript" d={transcriptPath} /> : null}
        {chatPath ? <path className="mini-chart-line chat" d={chatPath} /> : null}
        {layout.xTicks.map((tick) => (
          <g key={`${tick.label}-${tick.x}`}>
            <line className="mini-chart-x-tick" x1={tick.x} x2={tick.x} y1={layout.height - layout.padding.bottom + 4} y2={layout.height - layout.padding.bottom + 10} />
            <text className="mini-chart-x-label" x={tick.x} y={layout.height - 8} textAnchor="middle">{tick.label}</text>
          </g>
        ))}
        {activePoint ? (
          <g className="mini-chart-active">
            <line className="mini-chart-hover-line" x1={activePoint.x} x2={activePoint.x} y1={layout.padding.top} y2={layout.height - layout.padding.bottom} />
            {typeof activePoint.chatY === "number" ? <circle className="mini-chart-dot chat" cx={activePoint.x} cy={activePoint.chatY} r="4" /> : null}
            {typeof activePoint.transcriptY === "number" ? <circle className="mini-chart-dot transcript" cx={activePoint.x} cy={activePoint.transcriptY} r="4" /> : null}
            {typeof activePoint.aggregateY === "number" ? <circle className="mini-chart-dot aggregate" cx={activePoint.x} cy={activePoint.aggregateY} r="3.5" /> : null}
          </g>
        ) : null}
        {layout.points.map((point, index) => {
          const bounds = timelineCanvasCellBounds(layout.points, index, layout.padding.left, layout.width - layout.padding.right);
          return (
            <rect
              aria-label={chartPointAriaLabel(point)}
              className="mini-chart-hover-zone"
              data-chart-index={index}
              key={`${point.key}-hover`}
              tabIndex={0}
              x={roundSvg(bounds.x)}
              y={layout.padding.top}
              width={roundSvg(bounds.width)}
              height={layout.plotHeight}
              onBlur={() => setActiveIndex(null)}
              onClick={() => setActiveIndex(index)}
              onFocus={() => setActiveIndex(index)}
              onMouseEnter={() => setActiveIndex(index)}
              onPointerEnter={() => setActiveIndex(index)}
            />
          );
        })}
      </svg>
      {activePoint ? (
        <div className={`mini-chart-tooltip${activeTooltipFlipped ? " flip" : ""}`} style={{ left: activeTooltipX, top: activeTooltipY }}>
          <strong>{activePoint.time}</strong>
          <span><em>Chat</em><b className="chat-color">{formatSignedNumber(activePoint.chat)}</b></span>
          <span><em>Voice</em><b className="transcript-color">{formatSignedNumber(activePoint.transcript)}</b></span>
          <span><em>Aggregate</em><b>{formatSignedNumber(activePoint.aggregate)}</b></span>
          <span><em>Messages</em><b>{compactNumber(activePoint.activity || activePoint.messages || 0, 0)}</b></span>
          {activePoint.reactionType ? <p>{humanizeLabel(activePoint.reactionType)}{activePoint.targetText ? ` - ${truncateText(activePoint.targetText, 54)}` : ""}</p> : null}
        </div>
      ) : null}
    </div>
  );
}

function LiveReactionTimelineChart({
  points,
  buckets,
  emptyMessage
}: {
  points: SentimentPoint[];
  buckets: ChatBucket[];
  emptyMessage: string;
}) {
  return (
    <SentimentTimelineChart
      points={points}
      buckets={buckets}
      emptyMessage={emptyMessage}
    />
  );
}

function ReactionDiagnosticsDetails({ window }: { window?: ReactionWindow }) {
  if (!window) return null;
  return (
    <details className="reaction-diagnostics">
      <summary>Reaction diagnostics</summary>
      <ReactionWindowSummary window={window} />
    </details>
  );
}

function ReactionWindowSummary({ window }: { window?: ReactionWindow }) {
  if (!window) {
    return null;
  }
  const confidence = windowConfidence(window);
  const targetLabel = targetContextLabel(window.target_type, window.target_text);
  const evidenceCount = windowEvidenceCount(window);
  if (isTranscriptReactionWindow(window)) {
    return (
      <div className={`reaction-window-summary transcript-reaction-summary ${window.provisional ? "provisional" : ""}`}>
        <div>
          <span>Transcript</span>
          <strong>{window.provisional ? "Live" : "Finalizing"}</strong>
        </div>
        <div>
          <span>Voice</span>
          <strong>{formatSignedNumber(window.valence)}</strong>
        </div>
        <div>
          <span>Intensity</span>
          <strong>{formatNumber(window.intensity_score)}</strong>
        </div>
        <div>
          <span>Conf.</span>
          <strong>{formatOptionalPercent(confidence)}</strong>
        </div>
        <div>
          <span>Reaction</span>
          <strong>{knownText(window.reaction_type)}</strong>
        </div>
        <div>
          <span>Target</span>
          <strong>{targetLabel}</strong>
        </div>
        {window.event_hint ? (
          <div>
            <span>Hint</span>
            <strong>{window.event_hint}</strong>
          </div>
        ) : null}
        {evidenceCount > 0 ? (
          <div>
            <span>Evidence</span>
            <strong>{compactNumber(evidenceCount, 0)} ids</strong>
          </div>
        ) : null}
        {window.target_text ? <p>{window.target_text}</p> : null}
      </div>
    );
  }
  const evidence = window.evidence_messages?.filter((message) => message.text).slice(0, 2) || [];
  return (
    <div className="reaction-window-summary">
      <div>
        <span>Rate</span>
        <strong>{compactNumber(window.messages_per_minute || 0, 0)} mpm</strong>
      </div>
      <div>
        <span>Type</span>
        <strong>{knownText(window.reaction_type)}</strong>
      </div>
      <div>
        <span>Conf.</span>
        <strong>{formatOptionalPercent(confidence)}</strong>
      </div>
      <div>
        <span>Target</span>
        <strong>{targetLabel}</strong>
      </div>
      <div>
        <span>Hype</span>
        <strong>{formatNumber(window.hype_score)}</strong>
      </div>
      <div>
        <span>Confuse</span>
        <strong>{formatNumber(window.confusion_score)}</strong>
      </div>
      <div>
        <span>Frustrate</span>
        <strong>{formatNumber(window.frustration_score)}</strong>
      </div>
      {window.event_hint ? (
        <div>
          <span>Hint</span>
          <strong>{window.event_hint}</strong>
        </div>
      ) : null}
      {evidenceCount > 0 ? (
        <div>
          <span>Evidence</span>
          <strong>{compactNumber(evidenceCount, 0)} ids</strong>
        </div>
      ) : null}
      {evidence.length > 0 ? (
        <p>{evidence.map((message) => message.text).join(" / ")}</p>
      ) : window.target_text ? (
        <p>{window.target_text}</p>
      ) : null}
    </div>
  );
}

function ThirtySecondBucketSections({
  buckets,
  transcriptBuckets,
  onInspect
}: {
  buckets: ChatBucket[];
  transcriptBuckets: TranscriptBucket[];
  onInspect: (selection: BucketInspectorSelection) => void;
}) {
  return (
    <div className="bucket-window-sections">
      <ChatThirtySecondBuckets buckets={buckets} onInspect={onInspect} />
      <TranscriptThirtySecondBuckets buckets={transcriptBuckets} onInspect={onInspect} />
    </div>
  );
}

function ChatThirtySecondBuckets({ buckets, onInspect }: { buckets: ChatBucket[]; onInspect: (selection: BucketInspectorSelection) => void }) {
  const visibleBuckets = buckets.slice(0, 5);
  return (
    <div className="bucket-window-block">
      <MetricLabel label="Chat Scored Buckets" />
      {visibleBuckets.length === 0 ? (
        <p className="bucket-empty">No scored chat buckets yet.</p>
      ) : (
        <div className="bucket-window-list">
          {visibleBuckets.map((bucket, index) => {
            const sentiment = sentimentDescriptor(bucket.chat_sentiment);
            const key = `${bucket.session_id}-${bucket.bucket_start}-${index}`;
            return (
              <button className="bucket-window-row" key={key} type="button" onClick={() => onInspect({ kind: "chat", bucket })}>
                <div>
                  <strong>{formatCompactTime(bucket.bucket_start)}-{formatCompactTime(bucket.bucket_end)}</strong>
                  <span>{compactNumber(bucket.message_count || 0, 0)} msgs / {compactNumber(bucket.unique_chatters || 0, 0)} unique</span>
                  {typeof bucket.peak_reaction_score === "number" ? (
                    <span className="bucket-window-text">Peak {knownText(bucket.peak_reaction_type)} / {formatOptionalPercent(bucket.peak_confidence)} / {formatWindowContext(bucket.peak_window_start, bucket.peak_window_end)}</span>
                  ) : null}
                </div>
                <div>
                  <strong className={`sentiment-read ${sentiment.tone}`}>{formatSignedNumber(bucket.chat_sentiment)}</strong>
                  <span>{sentiment.label}</span>
                </div>
                <div>
                  <strong>{formatOptionalPercent(bucket.sentiment_confidence)}</strong>
                  <span>confidence</span>
                </div>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

function TranscriptThirtySecondBuckets({ buckets, onInspect }: { buckets: TranscriptBucket[]; onInspect: (selection: BucketInspectorSelection) => void }) {
  const visibleBuckets = buckets.slice(0, 5);
  return (
    <div className="bucket-window-block">
      <MetricLabel label="Transcript Scored Buckets" />
      {visibleBuckets.length === 0 ? (
        <p className="bucket-empty">Waiting for the first finalized transcript bucket. Live transcript segments are shown before sentiment scoring.</p>
      ) : (
        <div className="bucket-window-list">
          {visibleBuckets.map((bucket, index) => {
            const sentiment = sentimentDescriptor(bucket.sentiment_score);
            const key = `${bucket.session_id}-${bucket.bucket_start}-${index}`;
            const score = transcriptScoreValue(bucket);
            return (
              <button className="bucket-window-row transcript-bucket-row" key={key} type="button" onClick={() => onInspect({ kind: "transcript", bucket })}>
                <div>
                  <strong>{formatCompactTime(bucket.bucket_start)}-{formatCompactTime(bucket.bucket_end)}</strong>
                  <span>{bucket.language || "lang -"} / {bucket.segments?.length || 0} segments</span>
                  <span className="bucket-window-text">{bucket.text || "No transcript text captured."}</span>
                </div>
                <div>
                  <strong className={`sentiment-read ${sentiment.tone}`}>{score}</strong>
                  <span>{transcriptScoreLabel(bucket, sentiment.label)}</span>
                </div>
                <div>
                  <strong>{formatOptionalPercent(bucket.sentiment_confidence)}</strong>
                  <span>confidence</span>
                </div>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

function BucketInspectorModal({ selection, onClose }: { selection: BucketInspectorSelection; onClose: () => void }) {
  const chatBucket = selection.kind === "chat" ? selection.bucket : undefined;
  const transcriptBucket = selection.kind === "transcript" ? selection.bucket : undefined;
  const bucket = selection.bucket;
  const score = chatBucket ? chatBucket.chat_sentiment : transcriptBucket?.sentiment_score;
  const confidence = chatBucket ? chatBucket.sentiment_confidence : transcriptBucket?.sentiment_confidence;
  const sentiment = sentimentDescriptor(score);
  const title = chatBucket ? "Chat bucket" : "Transcript bucket";
  const model = bucket.sentiment_model;
  const latency = chatBucket ? chatBucket.analysis_latency_ms : transcriptBucket?.sentiment_latency_ms;
  const source = chatBucket ? chatAnalysisSource(chatBucket) : transcriptBucket?.sentiment_status ? transcriptBucket.sentiment_status.toUpperCase() : model ? "MODEL" : "unknown";

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  return (
    <motion.div
      className="bucket-inspector-backdrop"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.16 }}
      onMouseDown={onClose}
    >
      <motion.aside
        className="bucket-inspector"
        role="dialog"
        aria-modal="true"
        aria-labelledby="bucket-inspector-title"
        initial={{ opacity: 0, y: 16, scale: 0.98 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        exit={{ opacity: 0, y: 12, scale: 0.98 }}
        transition={{ duration: 0.18 }}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header className="bucket-inspector-header">
          <div>
            <span>{source}</span>
            <h2 id="bucket-inspector-title">{title} / {bucketWindowLabel(bucket.bucket_start, bucket.bucket_end)}</h2>
          </div>
          <button className="icon-button bucket-inspector-close" type="button" onClick={onClose} aria-label="Close bucket inspector">
            <X size={18} />
          </button>
        </header>

        <section className="bucket-inspector-hero">
          <div>
            <span>Sentiment</span>
            <strong className={`sentiment-read ${sentiment.tone}`}>{formatSignedNumber(score)}</strong>
            <em>{chatBucket ? sentiment.label : transcriptBucket?.sentiment_label || sentiment.label}</em>
          </div>
          <div className="bucket-inspector-meter">
            {chatBucket ? (
              <>
                <div className="sentiment-track">
                  <div className="s-pos" style={{ width: `${ratioToPercent(chatBucket.positive)}%` }} />
                  <div className="s-neu" style={{ width: `${ratioToPercent(chatBucket.neutral)}%` }} />
                  <div className="s-neg" style={{ width: `${ratioToPercent(chatBucket.negative)}%` }} />
                </div>
                <div className="bucket-inspector-distribution">
                  <span>POS {ratioToPercent(chatBucket.positive)}%</span>
                  <span>NEU {ratioToPercent(chatBucket.neutral)}%</span>
                  <span>NEG {ratioToPercent(chatBucket.negative)}%</span>
                </div>
              </>
            ) : (
              <p>{transcriptBucket?.text || "No transcript text captured for this window."}</p>
            )}
          </div>
        </section>

        <div className="bucket-inspector-grid">
          {chatBucket ? (
            <>
              <InspectorMetric label="Messages" value={compactNumber(chatBucket.message_count || 0, 0)} />
              <InspectorMetric label="Unique chatters" value={compactNumber(chatBucket.unique_chatters || 0, 0)} />
              <InspectorMetric label="Scored" value={compactNumber(chatBucket.analyzed_count ?? 0, 0)} />
              <InspectorMetric label="Confidence" value={formatOptionalPercent(confidence)} />
              <InspectorMetric label="Latency" value={formatLatency(latency)} />
              <InspectorMetric label="Model" value={knownText(model || source)} wide />
              <InspectorMetric label="Peak reaction" value={knownText(chatBucket.peak_reaction_type)} />
              <InspectorMetric label="Peak source" value={knownText(chatBucket.peak_source)} />
              <InspectorMetric label="Peak conf." value={formatOptionalPercent(chatBucket.peak_confidence)} />
              <InspectorMetric label="Peak score" value={formatNumber(chatBucket.peak_reaction_score)} />
              <InspectorMetric label="Peak time" value={formatCompactTime(chatBucket.peak_time)} />
              <InspectorMetric label="Peak window" value={formatWindowContext(chatBucket.peak_window_start, chatBucket.peak_window_end)} wide />
              <InspectorMetric label="Peak target" value={targetContextLabel(chatBucket.peak_target_type, chatBucket.peak_target_text)} wide />
              <InspectorMetric label="Peak hint" value={knownText(chatBucket.peak_event_hint)} wide />
              <InspectorMetric label="Evidence IDs" value={formatEvidenceIDs(chatBucket.peak_evidence_ids)} wide />
              <InspectorMetric label="Evidence" value={evidenceCountLabel(chatBucket.peak_evidence_ids?.length ?? chatBucket.peak_evidence_messages?.length)} />
            </>
          ) : (
            <>
              <InspectorMetric label="Language" value={knownText(transcriptBucket?.language)} />
              <InspectorMetric label="Segments" value={compactNumber(transcriptBucket?.segments?.length || 0, 0)} />
              <InspectorMetric label="ASR conf." value={formatOptionalPercent(transcriptBucket?.transcript_confidence)} />
              <InspectorMetric label="Model conf." value={formatOptionalPercent(confidence)} />
              <InspectorMetric label="Latency" value={formatLatency(latency)} />
              <InspectorMetric label="Model" value={knownText(model || source)} wide />
              <InspectorMetric label="Context" value={targetContextLabel("transcript", transcriptBucket?.text)} wide />
            </>
          )}
        </div>

        {chatBucket ? <ChatBucketInspectorEvidence bucket={chatBucket} /> : transcriptBucket ? <TranscriptBucketInspectorEvidence bucket={transcriptBucket} /> : null}
      </motion.aside>
    </motion.div>
  );
}

function InspectorMetric({ label, value, wide = false }: { label: string; value: string; wide?: boolean }) {
  return (
    <div className={wide ? "wide" : ""}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function ChatBucketInspectorEvidence({ bucket }: { bucket: ChatBucket }) {
  const scores = (bucket.message_scores || []).slice(0, 16);
  const peakEvidence = bucket.peak_evidence_messages?.filter((message) => message.text).slice(0, 8) || [];
  return (
    <section className="bucket-inspector-section">
      {typeof bucket.peak_reaction_score === "number" ? (
        <>
          <div className="bucket-inspector-section-title">Peak reaction / {knownText(bucket.peak_reaction_type)} / {formatOptionalPercent(bucket.peak_confidence)} / {targetContextLabel(bucket.peak_target_type, bucket.peak_target_text)} / {formatWindowContext(bucket.peak_window_start, bucket.peak_window_end)} / {formatNumber(bucket.peak_reaction_score)}</div>
          <div className="bucket-inspector-list">
            {peakEvidence.length === 0 ? (
              <p className="bucket-inspector-empty">No peak evidence messages captured for this bucket.</p>
            ) : peakEvidence.map((message, index) => (
              <div className="bucket-inspector-message" key={message.message_id || `${message.timestamp}-${index}`}>
                <div>
                  <time>{formatCompactTime(message.timestamp)}</time>
                  <strong>{message.display_name || message.username || "unknown"}</strong>
                </div>
                <p>{message.text}</p>
              </div>
            ))}
          </div>
        </>
      ) : null}
      <div className="bucket-inspector-section-title">Scored messages</div>
      {scores.length === 0 ? (
        <p className="bucket-inspector-empty">No per-message trace available for this bucket.</p>
      ) : (
        <div className="bucket-inspector-list">
          {scores.map((score, index) => {
            const sentiment = sentimentDescriptor(score.sentiment_score);
            return (
              <div className="bucket-inspector-message" key={score.message_id || `${score.timestamp}-${index}`}>
                <div>
                  <time>{formatCompactTime(score.timestamp)}</time>
                  <strong>{score.display_name || score.username || "unknown"}</strong>
                </div>
                <p>{score.text}</p>
                <span className={`sentiment-read ${sentiment.tone}`}>{formatSignedNumber(score.sentiment_score)} / {knownText(score.label || sentiment.label)} / {formatOptionalPercent(score.confidence)}</span>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

function TranscriptBucketInspectorEvidence({ bucket }: { bucket: TranscriptBucket }) {
  const segments = bucket.segments || [];
  return (
    <section className="bucket-inspector-section">
      <div className="bucket-inspector-section-title">Transcript evidence</div>
      {segments.length === 0 ? (
        <p className="bucket-inspector-empty">{bucket.text || "No segment-level evidence available for this bucket."}</p>
      ) : (
        <div className="bucket-inspector-list">
          {segments.slice(0, 18).map((segment, index) => (
            <div className="bucket-inspector-message" key={`${segment.start}-${segment.end}-${index}`}>
              <div>
                <time>{formatSegmentRange(segment.start, segment.end)}</time>
                <strong>{formatOptionalPercent(segment.confidence)}</strong>
              </div>
              <p>{segment.text || "No text"}</p>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function MetricLabel({ label }: { label: string }) {
  return (
    <div className="metric-label-wrapper">
      <div className="metric-label">{label}</div>
    </div>
  );
}

function AnalystFooter({ state, status, clock, replay = false }: { state: DashboardState; status: string; clock: Date; replay?: boolean }) {
  const live = Boolean(state.session_id);
  return (
    <footer className="analyst-footer">
      <div className="live-indicator"><div className="dot" /> {replay ? "REPLAY" : live ? "LIVE" : "READY"} / {clock.toLocaleTimeString([], { hour12: false })}</div>
      <div className="footer-center">{compactNumber(state.message_count || state.messages?.length || 0, 0)} messages</div>
      <div className="footer-right">{status.startsWith("error") ? "ATTN_REQUIRED" : replay ? "HISTORY_VIEW" : live ? "REC_ACTIVE" : "STANDBY"}</div>
    </footer>
  );
}

function streamCaption(stream?: StreamInfo, channel?: string, status?: string) {
  const transcriptOnly = stream?.platform === "youtube";
  const details = [
    stream?.game || "live commentary",
    stream?.language ? `language ${stream.language}` : "",
    typeof stream?.viewer_count === "number" ? `${stream.viewer_count.toLocaleString()} viewers` : ""
  ].filter(Boolean);
  if (stream?.title) {
    return `${details.join(" / ") || "stream activity"} / ${transcriptOnly ? "speech only" : "chat + speech"}`;
  }
  if (channel) {
    return `Metadata pending / ${status || "ready"}`;
  }
  return "Start a stream for transcript and reaction analysis.";
}

function streamViewportMessage(channelName: string, status: string, pending: boolean, platform = "twitch") {
  if (!channelName) return undefined;
  const lowerStatus = status.toLowerCase();
  if (pending || lowerStatus.includes("starting")) {
    return { text: platform === "youtube" ? "Starting transcript-only YouTube stream." : "Verifying live Twitch stream.", tone: "muted" as const };
  }
  if (lowerStatus.includes("offline")) {
    return { text: "Channel is not live.", tone: "warn" as const };
  }
  if (lowerStatus.startsWith("error") || lowerStatus.includes("disconnected")) {
    return { text: status, tone: "error" as const };
  }
  return undefined;
}

function buildHealthMetrics(
  state: DashboardState,
  transcriptBuckets: TranscriptBucket[],
  transcriptStatus: string,
  replay: boolean,
  transcriptWarning = "",
  transcriptHealth?: TranscriptHealth
): HealthMetric[] {
  const latestChatBucket = state.buckets?.[0];
  const latestTranscriptBucket = transcriptBuckets[0];
  const latestAlignment = state.alignments?.[0];
  const relation = relationshipLabel(latestAlignment?.relationship);
  const delay = processingDelayLabel(latestChatBucket, latestTranscriptBucket);
  const transcriptBucketStatus = transcriptStatusLabel(latestTranscriptBucket);
  const transcriptHealthValue = transcriptWarning && !replay
    ? "Delayed"
    : displayTranscriptFinalityLabel(transcriptBucketStatus || transcriptStatus || transcriptHealth?.status);
  const transcriptHealthMeta = transcriptWarning && !replay
    ? transcriptWarning
    : latestTranscriptBucket
      ? transcriptFinalityMeta(latestTranscriptBucket)
      : replay ? "No stored transcript" : "Waiting for speech";

  return [
    {
      label: "Chat",
      value: latestChatBucket ? "Live" : replay ? "No bucket" : state.session_id ? "Collecting" : "Waiting",
      meta: latestChatBucket ? `${compactNumber(latestChatBucket.message_count || 0, 0)} messages` : "-",
      tone: healthTone(latestChatBucket?.analysis_status || state.status)
    },
    {
      label: "Transcript",
      value: transcriptHealthValue,
      meta: transcriptHealthMeta,
      tone: transcriptWarning && !replay ? "warn" : healthTone(transcriptBucketStatus || transcriptStatus)
    },
    {
      label: "Agreement",
      value: latestAlignment ? relation.label : replay ? "No match" : "Waiting",
      meta: latestAlignment ? `${formatPercent(latestAlignment.quality)} / ${Math.round(latestAlignment.overlap_seconds || 0)}s` : "-",
      tone: latestAlignment ? relationshipHealthTone(relation.key) : "muted"
    },
    {
      label: "Delay",
      value: delay.label,
      meta: "processing",
      tone: delay.tone
    }
  ];
}

function buildDashboardDiagnostics(
  state: DashboardState,
  transcriptBuckets: TranscriptBucket[],
  transcriptStatus: string,
  replay: boolean,
  transcriptHealth?: TranscriptHealth
): DiagnosticMetric[] {
  const latestChatBucket = state.buckets?.[0];
  const latestTranscriptBucket = transcriptBuckets[0];
  const repairQueue = transcriptHealth?.repair?.queue_size;
  return [
    {
      label: "Chat model",
      value: latestChatBucket?.sentiment_model || chatAnalysisSource(latestChatBucket),
      meta: `latency ${formatLatency(latestChatBucket?.analysis_latency_ms)}`
    },
    {
      label: "ASR model",
      value: asrModelLabel(transcriptHealth?.asr),
      meta: asrRuntimeLabel(transcriptHealth?.asr)
    },
    {
      label: "Final model",
      value: finalTranscriptModelLabel(transcriptHealth),
      meta: typeof repairQueue === "number" ? `queue ${compactNumber(repairQueue, 0)}` : "queue unknown"
    },
    {
      label: "Transport",
      value: replay ? "replay" : transcriptStatus || state.status || "idle",
      meta: state.session_id ? `session ${state.session_id.slice(-6)}` : "no active session"
    },
    {
      label: "Transcript latency",
      value: formatLatency(numericValue(latestTranscriptBucket?.pipeline_latency_ms) ?? numericValue(latestTranscriptBucket?.asr_latency_ms)),
      meta: `sentiment ${formatLatency(latestTranscriptBucket?.sentiment_latency_ms)}`
    }
  ];
}

function healthTone(value?: string): HealthMetricTone {
  const normalized = (value || "").toLowerCase();
  if (normalized.includes("error") || normalized.includes("offline") || normalized.includes("disconnected") || normalized.includes("degraded")) return "error";
  if (normalized.includes("fallback") || normalized.includes("waiting") || normalized.includes("idle") || normalized.includes("repairing")) return "warn";
  if (normalized.includes("final") || normalized.includes("python") || normalized.includes("model") || normalized.includes("nvidia") || normalized.includes("stream") || normalized.includes("ingest") || normalized.includes("running") || normalized.includes("live")) return "ok";
  return "muted";
}

function transcriptStatusLabel(bucket?: TranscriptBucket) {
  return bucket?.transcript_status || bucket?.quality?.status || bucket?.sentiment_status || "";
}

function transcriptStatusMeta(bucket?: TranscriptBucket) {
  if (!bucket) return "-";
  if (bucket.transcript_status === "repairing" || bucket.quality?.status === "repairing") {
    return bucket.quality?.repair_status || "repair pending";
  }
  if (typeof bucket.repair_added_words === "number" && bucket.repair_added_words > 0) {
    return `+${bucket.repair_added_words} words`;
  }
  if (typeof bucket.audio_seconds === "number" && bucket.audio_seconds > 0) {
    return `${Math.round(bucket.audio_seconds)}s audio`;
  }
  return bucket.sentiment_latency_ms ? formatLatency(bucket.sentiment_latency_ms) : "-";
}

function transcriptFinalityMeta(bucket?: TranscriptBucket) {
  if (!bucket) return "-";
  const wordCount = bucket.word_count ?? captionWords(bucket.text || "").length;
  if (typeof bucket.repair_added_words === "number" && bucket.repair_added_words > 0) {
    return `${compactNumber(bucket.repair_added_words, 0)} repaired words`;
  }
  if (wordCount > 0) return `${compactNumber(wordCount, 0)} words`;
  if (typeof bucket.audio_seconds === "number" && bucket.audio_seconds > 0) return `${Math.round(bucket.audio_seconds)}s audio`;
  return transcriptStatusMeta(bucket);
}

function transcriptPanelDiagnostics(
  metrics: TranscriptQualityMetrics,
  sourceMeta: string,
  delayMeta: string,
  offsetSeconds: number,
  replay: boolean
): DiagnosticMetric[] {
  return [
    { label: "Live model", value: metrics.liveModel },
    { label: "Final model", value: metrics.finalModel },
    { label: "p50 latency", value: formatMetricLatency(metrics.p50LatencyMS) },
    { label: "p95 latency", value: formatMetricLatency(metrics.p95LatencyMS) },
    { label: "Empty", value: formatPercent(metrics.emptyRate) },
    { label: "Confidence", value: formatPercent(metrics.medianConfidence) },
    { label: "Drop", value: formatPercent(metrics.qualityDropRate) },
    { label: "Delay", value: formatDelaySeconds(metrics.latestDelaySeconds) },
    { label: "Transport", value: sourceMeta },
    { label: "Preview sync", value: replay ? "replay" : formatOffsetSeconds(offsetSeconds) },
    { label: "Raw delay", value: delayMeta || "-" }
  ];
}

function relationshipHealthTone(value: string): HealthMetricTone {
  if (value === "diverged") return "warn";
  if (value === "converged" || value === "soft-split") return "ok";
  return "muted";
}

function transcriptSourceMeta(status: string, chunkSeconds: number, replay: boolean) {
  if (replay) return status || "stored";
  const normalized = status.toLowerCase();
  if (normalized === "ingesting" || normalized === "running") return `ASR ~${chunkSeconds}s`;
  return status || "Idle";
}

function displayTranscriptFinalityLabel(value?: string) {
  const normalized = (value || "").toLowerCase();
  if (!normalized) return "Delayed";
  if (normalized.includes("repair") && (normalized.includes("done") || normalized.includes("complete") || normalized.includes("ed"))) return "Repaired";
  if (normalized.includes("repaired")) return "Repaired";
  if (
    normalized.includes("degraded") ||
    normalized.includes("delayed") ||
    normalized.includes("error") ||
    normalized.includes("offline") ||
    normalized.includes("disconnect") ||
    normalized.includes("fallback") ||
    normalized.includes("warning") ||
    normalized.includes("unavailable")
  ) {
    return "Delayed";
  }
  if (
    normalized.includes("live") ||
    normalized.includes("partial") ||
    normalized.includes("stream") ||
    normalized.includes("ingest") ||
    normalized.includes("running") ||
    normalized.includes("connect")
  ) {
    return "Live";
  }
  return "Finalizing";
}

function transcriptFinalityForBucket(bucket?: TranscriptBucket) {
  if (bucket?.quality?.repaired || (typeof bucket?.repair_added_words === "number" && bucket.repair_added_words > 0)) {
    return "Repaired";
  }
  return displayTranscriptFinalityLabel(transcriptStatusLabel(bucket));
}

function transcriptFinalityForPrimaryInsight(window: ReactionWindow | undefined, bucket: TranscriptBucket | undefined) {
  if (!bucket && window?.provisional && isTranscriptReactionWindow(window)) {
    return "Live";
  }
  return transcriptFinalityForBucket(bucket);
}

function transcriptFinalityTone(value: string): HealthMetricTone {
  if (value === "Delayed") return "warn";
  if (value === "Live" || value === "Repaired") return "ok";
  return "muted";
}

function transcriptEmptyMessage(status: string, liveActive: boolean, replay: boolean, chunkSeconds: number) {
  const normalized = status.toLowerCase();
  if (replay) return "No stored transcript evidence returned for this replay.";
  if (normalized.startsWith("error") || normalized.includes("offline")) return status;
  if (!liveActive) return "Start a live stream session to show speech.";
  if (normalized.includes("starting") || normalized.includes("connect")) return "Connecting to the transcript service.";
  if (normalized === "ingesting" || normalized === "running") {
    return `Listening for speech. Captions appear after about ${chunkSeconds}-${chunkSeconds + 1}s.`;
  }
  return "Waiting for raw speech segments.";
}

function transcriptScoreValue(bucket?: TranscriptBucket) {
  if (typeof bucket?.sentiment_score === "number") return formatSignedNumber(bucket.sentiment_score);
  const status = (bucket?.sentiment_status || "").toLowerCase();
  if (status === "unavailable") return "unavailable";
  if (status === "skipped") return "not scored";
  return "pending";
}

function transcriptScoreLabel(bucket?: TranscriptBucket, fallback = "-") {
  if (bucket?.sentiment_label) return bucket.sentiment_label;
  if (typeof bucket?.sentiment_score === "number") return fallback;
  const status = (bucket?.sentiment_status || "").toLowerCase();
  if (status === "unavailable") return "analyzer unavailable";
  if (status === "skipped") return bucket?.text?.trim() ? "not scored yet" : "no speech";
  return "awaiting bucket score";
}

function sessionReplayToDashboardState(replay: SessionReplay): DashboardState {
  const session = replay.session;
  const chatBuckets = newestFirst(replay.chat_buckets, (bucket) => bucket.bucket_start);
  const transcriptBuckets = newestFirst(replay.transcript_buckets, (bucket) => bucket.bucket_start);
  const alignments = newestFirst(replay.alignments, (alignment) => alignment.window_start);
  const signalWindows = newestFirst(replaySignalWindows(replay), (window) => window.window_start);
  return {
    status: session?.status || "stored",
    session_id: session?.session_id,
    channel: session?.channel_id,
    stream: session
      ? {
          title: session.stream_title,
          game: session.stream_game,
          viewer_count: session.stream_viewer_count,
          started_at: session.started_at
        }
      : undefined,
    message_count: chatBuckets.reduce((total, bucket) => total + (bucket.message_count || 0), 0),
    bucket_count: session?.chat_bucket_count ?? chatBuckets.length,
    messages: replayMessagesFromBuckets(chatBuckets),
    buckets: chatBuckets,
    transcript_buckets: transcriptBuckets,
    alignments,
    signal_windows: signalWindows,
    signal_events: replay.signal_events || []
  };
}

function newestFirst<T>(items: T[] | undefined, timestampForItem: (item: T) => string | undefined): T[] {
  return (items || [])
    .map((item, index) => ({ item, index, time: timestampValue(timestampForItem(item)) }))
    .sort((first, second) => {
      if (first.time !== second.time) return second.time - first.time;
      return first.index - second.index;
    })
    .map(({ item }) => item);
}

function replayMessagesFromBuckets(buckets: ChatBucket[]): ChatMessage[] {
  return buckets
    .flatMap((bucket) => (bucket.message_scores || []).map((score) => ({
      session_id: bucket.session_id,
      channel_id: bucket.channel_id,
      message_id: score.message_id,
      timestamp: score.timestamp || bucket.bucket_start,
      username: score.username,
      display_name: score.display_name,
      text: score.text
    })))
    .slice(0, 120);
}

function transcriptSegmentsFromBuckets(buckets: TranscriptBucket[]): TranscriptSegment[] {
  return buckets
    .slice()
    .reverse()
    .flatMap((bucket) => {
      if (bucket.segments?.length) {
        return bucket.segments.map((segment) => ({
          session_id: bucket.session_id,
          channel_id: bucket.channel_id,
          transcript_start: bucket.bucket_start,
          transcript_end: bucket.bucket_end,
          text: segment.text,
          language: bucket.language,
          confidence: segment.confidence,
          transcript_confidence: bucket.transcript_confidence
        }));
      }
      return [{
        session_id: bucket.session_id,
        channel_id: bucket.channel_id,
        transcript_start: bucket.bucket_start,
        transcript_end: bucket.bucket_end,
        text: bucket.text,
        language: bucket.language,
        transcript_confidence: bucket.transcript_confidence
      }];
    });
}

function replaySignalWindows(replay?: SessionReplay): SignalWindow[] {
  return replay?.signal_windows || [];
}

function transcriptPanelRows(liveSegments: TranscriptSegment[]): TranscriptSegment[] {
  const unique = new Map<string, TranscriptSegment>();
  liveSegments
    .filter((segment) => shouldDisplayTranscriptSegment(segment))
    .forEach((segment, index) => {
      const key = segment.type === "transcript_partial"
        ? [segment.session_id, "partial"].join(":")
        : [segment.session_id, segment.transcript_start, segment.transcript_end, segment.text || index].join(":");
      unique.set(key, segment);
    });
  return Array.from(unique.values())
    .sort((first, second) => timestampValue(first.transcript_start) - timestampValue(second.transcript_start))
    .slice(-80);
}

function upsertTranscriptSegment(current: TranscriptSegment[], next: TranscriptSegment): TranscriptSegment[] {
  if (!next.text?.trim()) return current;
  if (next.type === "transcript_partial") {
    const withoutPriorPartial = current.filter((segment) => segment.type !== "transcript_partial");
    return [...withoutPriorPartial, next].slice(-80);
  }
  const key = transcriptSegmentKey(next);
  if (current.some((segment) => transcriptSegmentKey(segment) === key)) {
    return current.filter((segment) => segment.type !== "transcript_partial");
  }
  return [...current.filter((segment) => segment.type !== "transcript_partial"), next].slice(-80);
}

function upsertTranscriptLiveState(current: TranscriptState | undefined, next: TranscriptSegment): TranscriptState {
  const segments = upsertTranscriptSegment(current?.segments || [], next);
  const isPartial = next.type === "transcript_partial";
  return {
    ...(current || {}),
    mode: current?.mode || "live",
    status: current?.status || "ingesting",
    session_id: next.session_id || current?.session_id,
    channel_id: next.channel_id || current?.channel_id,
    partial_count: isPartial ? (current?.partial_count || 0) + 1 : current?.partial_count,
    segment_count: isPartial ? current?.segment_count || 0 : Math.max(current?.segment_count || 0, segments.filter((segment) => segment.type !== "transcript_partial").length),
    segments,
    latest_partial: isPartial ? next : current?.latest_partial,
    latest_segment: isPartial ? current?.latest_segment : next
  };
}

function upsertTranscriptBucketState(current: TranscriptState | undefined, next: TranscriptBucket): TranscriptState {
  const buckets = current?.buckets || [];
  const existingIndex = buckets.findIndex((bucket) => sameTranscriptBucket(bucket, next));
  const nextBuckets = existingIndex >= 0
    ? buckets.map((bucket, index) => index === existingIndex ? next : bucket)
    : [next, ...buckets].slice(0, 80);
  return {
    ...(current || {}),
    mode: current?.mode || "buckets",
    status: current?.status || "ingesting",
    session_id: next.session_id || current?.session_id,
    channel_id: next.channel_id || current?.channel_id,
    bucket_count: Math.max(current?.bucket_count || 0, nextBuckets.length),
    buckets: nextBuckets,
    latest_bucket: next
  };
}

function transcriptSegmentKey(segment: TranscriptSegment) {
  if (segment.type === "transcript_partial") {
    return [segment.session_id, "partial"].join(":");
  }
  return [segment.session_id, segment.transcript_start, segment.transcript_end, segment.text || ""].join(":");
}

function provisionalTranscriptReactionWindow(event: TranscriptStreamEvent): ReactionWindow | undefined {
  const text = normalizeTranscriptText(event.text);
  if (!text) return undefined;

  const supplied = event.reaction_window;
  const windowStart = supplied?.window_start || event.transcript_start || event.audio_started_at || event.transcribed_at;
  const windowEnd = supplied?.window_end || event.transcript_end || event.audio_ended_at || event.transcribed_at || windowStart;
  if (!windowStart) return undefined;
  const resolvedWindowEnd = windowEnd || windowStart;

  const valence = numericValue(supplied?.valence) ?? numericValue(event.sentiment_score) ?? estimateTranscriptValence(text);
  const intensity = numericValue(supplied?.intensity_score) ?? estimateTranscriptIntensity(text, valence);

  return {
    ...(supplied || {}),
    type: supplied?.type || "transcript_reaction_window",
    source: "transcript",
    provisional: true,
    session_id: supplied?.session_id || event.session_id,
    channel_id: supplied?.channel_id || event.channel_id,
    window_start: windowStart,
    window_end: resolvedWindowEnd,
    message_count: supplied?.message_count ?? captionWords(text).length,
    messages_per_minute: supplied?.messages_per_minute ?? transcriptWordsPerMinute(text, windowStart, resolvedWindowEnd),
    valence,
    intensity_score: intensity,
    hype_score: numericValue(supplied?.hype_score) ?? estimateTranscriptHype(text, intensity),
    confusion_score: numericValue(supplied?.confusion_score) ?? estimateTranscriptTermScore(text, confusionTranscriptTerms),
    frustration_score: numericValue(supplied?.frustration_score) ?? estimateTranscriptTermScore(text, frustrationTranscriptTerms),
    reaction_type: supplied?.reaction_type || transcriptPartialReactionType(valence, intensity),
    target_type: supplied?.target_type || event.target_type || "unknown",
    target_text: supplied?.target_text || event.target_text,
    event_hint: supplied?.event_hint || event.event_hint,
    confidence: supplied?.confidence ?? event.confidence,
    evidence_ids: supplied?.evidence_ids || event.evidence_ids,
    transcript_text: supplied?.transcript_text || text,
    transcript_confidence: supplied?.transcript_confidence ?? event.transcript_confidence ?? event.confidence,
    sentiment_confidence: supplied?.sentiment_confidence ?? event.sentiment_confidence ?? event.transcript_confidence
  };
}

function upsertProvisionalTranscriptWindow(current: ReactionWindow[], incoming: ReactionWindow): ReactionWindow[] {
  const withoutSameWindow = current.filter((window) => !sameReactionWindow(window, incoming));
  return [incoming, ...withoutSameWindow].slice(0, 120);
}

function mergeReactionWindows(committed: ReactionWindow[], provisional: ReactionWindow[]) {
  return newestFirst([...committed, ...provisional], (window) => window.window_start || window.window_end).slice(0, 300);
}

function sameReactionWindow(left: ReactionWindow, right: ReactionWindow) {
  return left.session_id === right.session_id &&
    left.channel_id === right.channel_id &&
    left.source === right.source &&
    left.window_start === right.window_start &&
    left.window_end === right.window_end;
}

function committedTranscriptBucketCoversWindow(window: ReactionWindow, buckets: TranscriptBucket[]) {
  if (!window.provisional || !isTranscriptReactionWindow(window)) return false;
  return buckets.some((bucket) => {
    if (bucket.session_id && window.session_id && bucket.session_id !== window.session_id) return false;
    if (bucket.channel_id && window.channel_id && bucket.channel_id !== window.channel_id) return false;
    return rangeContains(bucket.bucket_start, bucket.bucket_end, window.window_start, window.window_end);
  });
}

function rangeContains(containerStart?: string, containerEnd?: string, childStart?: string, childEnd?: string) {
  const containerStartMs = timestampValue(containerStart);
  const containerEndMs = timestampValue(containerEnd) || containerStartMs;
  const childStartMs = timestampValue(childStart);
  const childEndMs = timestampValue(childEnd) || childStartMs;
  if (!containerStartMs || !containerEndMs || !childStartMs || !childEndMs) return false;
  return containerStartMs <= childStartMs && childEndMs <= containerEndMs;
}

function transcriptBucketIdentity(bucket: TranscriptBucket) {
  return [bucket.session_id, bucket.channel_id, bucket.bucket_start, bucket.bucket_end].join(":");
}

function isTranscriptReactionWindow(window?: ReactionWindow) {
  return window?.source === "transcript" || window?.type === "transcript_reaction_window";
}

function estimateTranscriptValence(text: string) {
  const tokens = transcriptTokens(text);
  if (tokens.length === 0) return 0;
  const positive = countTokenMatches(tokens, positiveTranscriptTerms);
  const negative = countTokenMatches(tokens, negativeTranscriptTerms);
  return clamp((positive - negative) / Math.max(positive + negative, 2), -1, 1);
}

function estimateTranscriptIntensity(text: string, valence?: number) {
  const words = captionWords(text);
  const punctuationBoost = Math.min(0.24, ((text.match(/[!?]/g) || []).length * 0.06));
  const lengthBoost = Math.min(0.5, words.length / 34);
  const valenceBoost = Math.min(0.2, Math.abs(valence || 0) * 0.2);
  return clamp(0.08 + lengthBoost + punctuationBoost + valenceBoost, 0.05, 1);
}

function estimateTranscriptHype(text: string, intensity: number) {
  const positive = estimateTranscriptTermScore(text, positiveTranscriptTerms);
  const punctuationBoost = Math.min(0.24, ((text.match(/[!]/g) || []).length * 0.08));
  return clamp((positive * 0.7) + (intensity * 0.22) + punctuationBoost, 0, 1);
}

function estimateTranscriptTermScore(text: string, terms: Set<string>) {
  const tokens = transcriptTokens(text);
  if (tokens.length === 0) return 0;
  return clamp(countTokenMatches(tokens, terms) / Math.max(3, tokens.length), 0, 1);
}

function transcriptPartialReactionType(valence: number, intensity: number) {
  if (intensity >= 0.68 && valence >= 0.1) return "voice_hype";
  if (valence <= -0.25) return "voice_frustration";
  if (valence >= 0.25) return "voice_positive";
  return "voice_partial";
}

function transcriptWordsPerMinute(text: string, start?: string, end?: string) {
  const startMs = timestampValue(start);
  const endMs = timestampValue(end);
  const seconds = startMs && endMs ? Math.max(1, (endMs - startMs) / 1000) : 30;
  return Math.round((captionWords(text).length / seconds) * 60);
}

function partialTranscriptDurationMS(text: string) {
  const words = captionWords(text).length;
  return clamp(Math.round((Math.max(1, words) / 2.7) * 1000), 900, 10000);
}

function transcriptTokens(text: string) {
  return text.toLowerCase().match(/[\p{Letter}\p{Number}]+/gu) || [];
}

function countTokenMatches(tokens: string[], terms: Set<string>) {
  return tokens.reduce((count, token) => count + (terms.has(token) ? 1 : 0), 0);
}

function liveClosedCaptionBlocks(liveSegments: TranscriptSegment[], limit = 3): TranscriptCaptionBlock[] {
  const rows = transcriptPanelRows(liveSegments);
  const blocks: TranscriptCaptionBlock[] = [];

  rows.forEach((segment, index) => {
    const text = normalizeTranscriptText(segment.text);
    if (!text) return;

    const startMs = timestampValue(segment.transcript_start);
    const last = blocks[blocks.length - 1];
    const lastEndMs = timestampValue(last?.end || last?.start);
    const gapSeconds = last && startMs && lastEndMs ? (startMs - lastEndMs) / 1000 : 0;
    const mergedText = last ? mergeCaptionText(last.text, text) : text;
    const shouldMerge = Boolean(
      last &&
      gapSeconds <= 5 &&
      mergedText.length <= 145 &&
      !isCaptionBoundary(last.text)
    );

    if (shouldMerge && last) {
      last.text = mergedText;
      last.end = segment.transcript_end || segment.transcript_start || last.end;
      return;
    }

    blocks.push({
      key: [segment.session_id, segment.transcript_start, segment.transcript_end, index].join(":"),
      start: segment.transcript_start,
      end: segment.transcript_end || segment.transcript_start,
      text
    });
  });

  return blocks.slice(-limit);
}

function selectedCaptionIndex(captions: TranscriptCaptionBlock[], clock: Date, offsetSeconds: number, replay: boolean) {
  if (captions.length === 0) return -1;
  if (replay) return captions.length - 1;

  const targetMs = clock.getTime() - clampTranscriptOffset(offsetSeconds) * 1000;
  let selectedIndex = -1;
  captions.forEach((caption, index) => {
    const startMs = timestampValue(caption.start);
    if (!startMs) return;
    if (startMs <= targetMs) selectedIndex = index;
  });

  if (selectedIndex >= 0) return selectedIndex;
  return 0;
}

function normalizeTranscriptText(value?: string) {
  return (value || "").replace(/\s+/g, " ").trim();
}

function shouldDisplayTranscriptSegment(segment: TranscriptSegment) {
  const text = normalizeTranscriptText(segment.text);
  if (!text) return false;

  const confidence = transcriptSegmentConfidence(segment);
  if (typeof confidence === "number" && confidence < 0.25) return false;
  if (hasHighNonLatinShare(text)) return false;
  if (isRepetitiveCaptionNoise(text)) return false;
  return true;
}

function transcriptSegmentConfidence(segment: TranscriptSegment) {
  const value = segment.transcript_confidence ?? segment.confidence;
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function transcriptQualityMetrics(
  segments: TranscriptSegment[],
  health: TranscriptHealth | undefined,
  clock: Date
): TranscriptQualityMetrics {
  const recent = segments.slice(-80);
  const nonEmpty = recent.filter((segment) => normalizeTranscriptText(segment.text));
  const latencies = recent
    .map((segment) => numericValue(segment.pipeline_latency_ms ?? segment.asr_latency_ms))
    .filter((value): value is number => typeof value === "number");
  const confidences = nonEmpty
    .map(transcriptSegmentConfidence)
    .filter((value): value is number => typeof value === "number");
  const latestNonEmpty = nonEmpty[nonEmpty.length - 1];
  const latestEndMs = timestampValue(latestNonEmpty?.transcribed_at || latestNonEmpty?.transcript_end || latestNonEmpty?.transcript_start);
  const qualityTotals = recent.reduce(
    (total, segment) => {
      const quality = segment.quality;
      total.raw += numericValue(quality?.raw_segment_count) || 0;
      total.dropped += numericValue(quality?.dropped_low_confidence_count) || 0;
      total.dropped += numericValue(quality?.dropped_repeat_count) || 0;
      return total;
    },
    { raw: 0, dropped: 0 }
  );

  return {
    liveModel: asrModelLabel(health?.asr),
    finalModel: finalTranscriptModelLabel(health),
    p50LatencyMS: percentile(latencies, 0.5),
    p95LatencyMS: percentile(latencies, 0.95),
    emptyRate: recent.length > 0 ? 1 - nonEmpty.length / recent.length : undefined,
    medianConfidence: percentile(confidences, 0.5),
    qualityDropRate: qualityTotals.raw > 0 ? qualityTotals.dropped / qualityTotals.raw : undefined,
    latestDelaySeconds: latestEndMs ? Math.max(0, (clock.getTime() - latestEndMs) / 1000) : undefined,
    segmentCount: recent.length
  };
}

function finalTranscriptModelLabel(health: TranscriptHealth | undefined) {
  if (!health?.repair?.enabled) return "-";
  return asrModelLabel(health.repair.asr || undefined);
}

function asrRuntimeLabel(asr: TranscriptHealth["asr"] | undefined) {
  const profile = asr?.profile || "asr";
  return profile.toUpperCase();
}

function asrModelLabel(asr: TranscriptHealth["asr"] | undefined) {
  const model = asr?.model || "asr";
  const language = asr?.language;
  const compact = compactAsrModelName(model);
  if (language && compact.toLowerCase().endsWith(`.${language.toLowerCase()}`)) return compact;
  return language ? `${compact}/${language}` : compact;
}

function compactAsrModelName(model: string) {
  return model
    .replace(/\.en-q5_[01]$/i, "-q5")
    .replace(/\.en-q8_0$/i, "-q8")
    .replace(/\.en$/i, ".en");
}

function percentile(values: number[], ratio: number): number | undefined {
  const sorted = values.filter(Number.isFinite).sort((left, right) => left - right);
  if (sorted.length === 0) return undefined;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index];
}

function numericValue(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function formatMetricLatency(value?: number) {
  if (typeof value !== "number") return "-";
  if (value >= 1000) return `${(value / 1000).toFixed(value >= 10_000 ? 0 : 1)}s`;
  return `${Math.round(value)}ms`;
}

function formatDelaySeconds(value?: number) {
  if (typeof value !== "number") return "-";
  if (value >= 60) return `${Math.round(value / 60)}m`;
  return `${Math.round(value)}s`;
}

function hasHighNonLatinShare(text: string) {
  const letters = Array.from(text).filter((char) => /\p{Letter}/u.test(char));
  if (letters.length === 0) return false;
  const nonLatin = letters.filter((char) => !/\p{Script=Latin}/u.test(char)).length;
  return nonLatin / letters.length > 0.35;
}

function isRepetitiveCaptionNoise(text: string) {
  const tokens = text.toLowerCase().match(/[\p{Letter}\p{Number}]+/gu) || [];
  if (tokens.length < 4) return false;
  const unique = new Set(tokens);
  if (unique.size <= 2) return true;

  const bigrams = new Map<string, number>();
  for (let index = 0; index < tokens.length - 1; index += 1) {
    const bigram = `${tokens[index]} ${tokens[index + 1]}`;
    bigrams.set(bigram, (bigrams.get(bigram) || 0) + 1);
  }
  return Array.from(bigrams.values()).some((count) => count >= 2);
}

function captionWords(value: string) {
  return normalizeTranscriptText(value).split(" ").filter(Boolean);
}

function isCaptionBoundary(value: string) {
  return /[.!?]["')\]]?$/.test(value.trim());
}

function mergeCaptionText(previous: string, next: string) {
  if (!previous) return next;
  if (!next) return previous;
  if (previous.endsWith(next)) return previous;
  if (next.startsWith(previous)) return next;
  return `${previous} ${next}`;
}

function twitchPlayerUrl(channelName: string) {
  if (!channelName) return "";
  const params = new URLSearchParams({
    channel: channelName,
    muted: "true",
    autoplay: "true",
    controls: "true"
  });
  const parentCandidates = [window.location.hostname, "localhost", "127.0.0.1"].filter(Boolean);
  Array.from(new Set(parentCandidates)).forEach((parent) => params.append("parent", parent));
  return `https://player.twitch.tv/?${params.toString()}`;
}

function streamSupportsChat(stream?: StreamInfo, channel?: string) {
  const platform = stream?.platform || streamPlatformFromURL(stream?.url) || streamPlatformFromURL(channel);
  return platform !== "youtube";
}

function youtubePlayerUrl(sourceURL: string) {
  const videoID = youtubeVideoID(sourceURL);
  if (!videoID) return "";
  const params = new URLSearchParams({
    autoplay: "1",
    mute: "1",
    playsinline: "1"
  });
  return `https://www.youtube.com/embed/${encodeURIComponent(videoID)}?${params.toString()}`;
}

function youtubeVideoID(sourceURL: string) {
  try {
    const url = new URL(sourceURL.startsWith("http") ? sourceURL : `https://${sourceURL}`);
    const host = url.hostname.toLowerCase().replace(/^www\./, "").replace(/^m\./, "");
    if (host === "youtu.be") return url.pathname.split("/").filter(Boolean)[0] || "";
    const queryID = url.searchParams.get("v");
    if (queryID) return queryID;
    const parts = url.pathname.split("/").filter(Boolean);
    const markerIndex = parts.findIndex((part) => ["live", "embed", "shorts"].includes(part));
    return markerIndex >= 0 ? parts[markerIndex + 1] || "" : "";
  } catch {
    return "";
  }
}

function timestampValue(value?: string) {
  if (!value) return 0;
  const time = new Date(value).getTime();
  return Number.isFinite(time) ? time : 0;
}

function transcriptDelaySeconds(caption: TranscriptCaptionBlock | undefined, clock: Date) {
  const transcriptEndMs = timestampValue(caption?.end || caption?.start);
  if (!transcriptEndMs) return undefined;
  return Math.max(0, (clock.getTime() - transcriptEndMs) / 1000);
}

function formatTranscriptDelay(value?: number) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "delay --";
  if (value < 10) return `delay ${value.toFixed(1)}s`;
  return `delay ${Math.round(value)}s`;
}

function formatOffsetSeconds(value: number) {
  if (value === 0) return "0s";
  return `${value > 0 ? "+" : ""}${value}s`;
}

function chatEmptyMessage({
  replay,
  liveActive,
  loading,
  error,
  chatAvailable
}: {
  replay: boolean;
  liveActive: boolean;
  loading: boolean;
  error: string;
  chatAvailable: boolean;
}) {
  if (error) return error;
  if (loading) return "Loading dashboard state.";
  if (replay) return "No stored chat evidence returned for this replay.";
  if (!chatAvailable) return "This source is running transcript-only because live chat is unavailable.";
  if (!liveActive) return "Start a live stream session to show chat.";
  return "Waiting for accepted Twitch chat messages.";
}

function formatCompactTime(value?: string) {
  if (!value) return "--:--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--:--";
  return date.toLocaleTimeString([], { minute: "2-digit", second: "2-digit" });
}

function bucketWindowLabel(start?: string, end?: string) {
  return `${formatCompactTime(start)}-${formatCompactTime(end)}`;
}

function formatSegmentRange(start?: number, end?: number) {
  const formatValue = (value?: number) => typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(1)}s` : "--";
  return `${formatValue(start)}-${formatValue(end)}`;
}

function buildTimelineCanvasData(points: SentimentPoint[], buckets: ChatBucket[]) {
  const reactionSeries = points.some((point) => typeof point.intensity === "number" || point.reactionType);
  const recent = points.slice(reactionSeries ? -300 : -12);
  const recentBuckets = buckets.slice(0, Math.max(recent.length, 1)).reverse();
  const maxActivity = Math.max(
    ...recentBuckets.map((bucket) => bucket.message_count || 0),
    ...recent.map((point) => point.messages || 0),
    1
  );

  return {
    reactionSeries,
    maxActivity,
    points: recent.map((point, index) => {
      const bucket = recentBuckets[index];
      return {
        ...point,
        index,
        time: compactChartTime(point.time),
        activity: point.messages || bucket?.message_count || 0
      };
    })
  };
}

function measureTimelineCanvas(canvas: HTMLCanvasElement) {
  const parent = canvas.parentElement;
  const bounds = parent?.getBoundingClientRect();
  const width = parent?.clientWidth || bounds?.width || canvas.clientWidth || canvas.offsetWidth || 360;
  const height = parent?.clientHeight || bounds?.height || canvas.clientHeight || canvas.offsetHeight || 220;
  return { width: Math.max(240, width), height: Math.max(180, height) };
}

function timelineCanvasLayout(points: TimelineCanvasPoint[], width: number, height: number) {
  const padding = { top: 18, right: 18, bottom: 27, left: 42 };
  const plotWidth = Math.max(1, width - padding.left - padding.right);
  const plotHeight = Math.max(1, height - padding.top - padding.bottom);
  const xScale = scaleLinear()
    .domain([0, Math.max(1, points.length - 1)])
    .range([padding.left, width - padding.right]);
  const yScale = scaleLinear()
    .domain([-1, 1])
    .range([height - padding.bottom, padding.top]);
  const layoutPoints = points.map((point, index) => ({
    ...point,
    x: xScale(index),
    chatY: typeof point.chat === "number" ? yScale(clamp(point.chat, -1, 1)) : undefined,
    transcriptY: typeof point.transcript === "number" ? yScale(clamp(point.transcript, -1, 1)) : undefined,
    aggregateY: typeof point.aggregate === "number" ? yScale(clamp(point.aggregate, -1, 1)) : undefined
  }));

  return {
    width,
    height,
    padding,
    plotWidth,
    plotHeight,
    points: layoutPoints,
    yTicks: [-1, -0.5, 0, 0.5, 1].map((value) => ({ value, y: yScale(value) })),
    xTicks: timelineCanvasXTicks(layoutPoints)
  };
}

function timelineCanvasXTicks(points: Array<TimelineCanvasPoint & { x: number }>) {
  if (points.length === 0) return [];
  const indexes = Array.from(new Set([
    0,
    Math.floor((points.length - 1) * 0.33),
    Math.floor((points.length - 1) * 0.66),
    points.length - 1
  ]));
  return indexes.map((index) => ({
    x: points[index].x,
    label: points[index].time
  }));
}

function htmlLineSegments(
  points: Array<{ x: number; chatY?: number; transcriptY?: number; aggregateY?: number }>,
  key: "chatY" | "transcriptY" | "aggregateY"
) {
  const clean = points.filter((point) => typeof point[key] === "number");
  const smooth = smoothChartPoints(clean, key);
  return smooth.slice(1).map((point, index) => {
    const previous = smooth[index];
    const previousY = previous[key] || 0;
    const y = point[key] || 0;
    const deltaX = point.x - previous.x;
    const deltaY = y - previousY;
    return {
      left: previous.x,
      top: previousY,
      width: Math.max(1, Math.hypot(deltaX, deltaY)),
      transform: `rotate(${Math.atan2(deltaY, deltaX)}rad)`
    };
  });
}

function smoothChartPoints(
  points: Array<{ x: number; chatY?: number; transcriptY?: number; aggregateY?: number }>,
  key: "chatY" | "transcriptY" | "aggregateY"
) {
  if (points.length < 3) return points;
  const softened = softenChartYValues(points, key);
  const samplesPerSegment = 8;
  const smoothed: Array<{ x: number; chatY?: number; transcriptY?: number; aggregateY?: number }> = [];

  for (let index = 0; index < softened.length - 1; index += 1) {
    const p0 = softened[index - 1] || softened[index];
    const p1 = softened[index];
    const p2 = softened[index + 1];
    const p3 = softened[index + 2] || p2;
    const y0 = p0[key] || 0;
    const y1 = p1[key] || 0;
    const y2 = p2[key] || 0;
    const y3 = p3[key] || 0;

    for (let sample = 0; sample < samplesPerSegment; sample += 1) {
      const t = sample / samplesPerSegment;
      const x = catmullRom(p0.x, p1.x, p2.x, p3.x, t);
      const y = catmullRom(y0, y1, y2, y3, t);
      smoothed.push({ x, [key]: y });
    }
  }

  smoothed.push(softened[softened.length - 1]);
  return smoothed;
}

function softenChartYValues(
  points: Array<{ x: number; chatY?: number; transcriptY?: number; aggregateY?: number }>,
  key: "chatY" | "transcriptY" | "aggregateY"
) {
  return points.map((point, index) => {
    const previous = points[index - 1]?.[key];
    const current = point[key];
    const next = points[index + 1]?.[key];
    if (typeof current !== "number") return point;
    if (typeof previous !== "number" || typeof next !== "number") return point;
    return {
      ...point,
      [key]: previous * 0.22 + current * 0.56 + next * 0.22
    };
  });
}

function catmullRom(p0: number, p1: number, p2: number, p3: number, t: number) {
  const t2 = t * t;
  const t3 = t2 * t;
  return 0.5 * (
    (2 * p1) +
    (-p0 + p2) * t +
    (2 * p0 - 5 * p1 + 4 * p2 - p3) * t2 +
    (-p0 + 3 * p1 - 3 * p2 + p3) * t3
  );
}

function drawSentimentCanvas(
  canvas: HTMLCanvasElement,
  chart: ReturnType<typeof buildTimelineCanvasData>,
  size: { width: number; height: number },
  activeIndex: number
) {
  const dpr = window.devicePixelRatio || 1;
  const width = Math.max(1, size.width);
  const height = Math.max(1, size.height);
  canvas.width = Math.floor(width * dpr);
  canvas.height = Math.floor(height * dpr);

  const ctx = canvas.getContext("2d");
  if (!ctx) return;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#080808";
  ctx.fillRect(0, 0, width, height);

  const layout = timelineCanvasLayout(chart.points, width, height);
  const { padding, plotWidth, plotHeight } = layout;
  const plotRight = width - padding.right;
  const plotBottom = height - padding.bottom;

  ctx.fillStyle = "rgba(255,255,255,0.035)";
  roundedRect(ctx, padding.left, padding.top, plotWidth, plotHeight, 8);
  ctx.fill();

  ctx.font = "700 8px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";
  ctx.textBaseline = "middle";
  ctx.textAlign = "right";
  layout.yTicks.forEach((tick) => {
    ctx.beginPath();
    ctx.strokeStyle = tick.value === 0 ? "rgba(255,255,255,0.34)" : "rgba(255,255,255,0.16)";
    ctx.setLineDash(tick.value === 0 ? [4, 5] : []);
    ctx.lineWidth = 1;
    ctx.moveTo(padding.left, tick.y);
    ctx.lineTo(plotRight, tick.y);
    ctx.stroke();

    ctx.fillStyle = "rgba(255,255,255,0.68)";
    ctx.fillText(formatAxisTick(tick.value), padding.left - 8, tick.y);
  });
  ctx.setLineDash([]);

  const baseline = plotBottom - 2;
  ctx.fillStyle = "rgba(255,255,255,0.09)";
  layout.points.forEach((point, index) => {
    const bounds = timelineCanvasCellBounds(layout.points, index, padding.left, plotRight);
    const barHeight = Math.max(2, Math.min(20, (point.activity / chart.maxActivity) * 20));
    roundedRect(ctx, bounds.x + 2, baseline - barHeight, Math.max(3, bounds.width - 4), barHeight, 1.5);
    ctx.fill();
  });

  layout.points.forEach((point, index) => {
    if (typeof point.chat !== "number" || typeof point.transcript !== "number" || typeof point.chatY !== "number" || typeof point.transcriptY !== "number") return;
    const bounds = timelineCanvasCellBounds(layout.points, index, padding.left, plotRight);
    const delta = Math.abs(point.chat - point.transcript);
    ctx.fillStyle = delta < 0.25 ? "rgba(16,185,129,0.34)" : "rgba(255,16,122,0.34)";
    roundedRect(ctx, bounds.x, Math.min(point.chatY, point.transcriptY), Math.max(1, bounds.width), Math.max(1, Math.abs(point.chatY - point.transcriptY)), 1.5);
    ctx.fill();
  });

  drawCanvasLine(ctx, layout.points, "aggregateY", { color: "#f0a3c6", width: 1.8, dash: [4, 5] });
  drawCanvasLine(ctx, layout.points, "transcriptY", { color: "#f2bc2f", width: 2.8 });
  drawCanvasLine(ctx, layout.points, "chatY", { color: "#47b5ff", width: 2.8 });

  ctx.textAlign = "center";
  ctx.textBaseline = "alphabetic";
  ctx.fillStyle = "rgba(255,255,255,0.7)";
  layout.xTicks.forEach((tick) => {
    ctx.strokeStyle = "rgba(255,255,255,0.18)";
    ctx.beginPath();
    ctx.moveTo(tick.x, plotBottom);
    ctx.lineTo(tick.x, plotBottom + 5);
    ctx.stroke();
    ctx.fillText(tick.label, tick.x, height - 7);
  });

  const active = layout.points[activeIndex];
  if (active) {
    ctx.strokeStyle = "rgba(255,255,255,0.44)";
    ctx.setLineDash([3, 4]);
    ctx.beginPath();
    ctx.moveTo(active.x, padding.top);
    ctx.lineTo(active.x, plotBottom);
    ctx.stroke();
    ctx.setLineDash([]);
    drawCanvasDot(ctx, active.x, active.chatY, "#47b5ff", 4);
    drawCanvasDot(ctx, active.x, active.transcriptY, "#f2bc2f", 4);
    drawCanvasDot(ctx, active.x, active.aggregateY, "#f0a3c6", 3.5);
  }
}

function timelineCanvasCellBounds(points: Array<{ x: number }>, index: number, left: number, right: number) {
  const previousX = points[index - 1]?.x;
  const nextX = points[index + 1]?.x;
  const x = index === 0 ? left : ((previousX ?? left) + points[index].x) / 2;
  const endX = index === points.length - 1 ? right : (points[index].x + (nextX ?? right)) / 2;
  return { x, width: Math.max(1, endX - x) };
}

function miniTimelinePath(
  points: Array<TimelineCanvasPoint & { chatY?: number; transcriptY?: number; aggregateY?: number; x: number }>,
  key: "chatY" | "transcriptY" | "aggregateY"
) {
  const clean = points.filter((point) => typeof point[key] === "number");
  if (clean.length < 2) return "";
  return clean.map((point, index) => `${index === 0 ? "M" : "L"} ${roundSvg(point.x)} ${roundSvg(point[key] || 0)}`).join(" ");
}

function timelineTooltipTopPercent(
  point: TimelineCanvasPoint & { chatY?: number; transcriptY?: number; aggregateY?: number },
  chartHeight: number
) {
  const yValues = [point.chatY, point.transcriptY, point.aggregateY].filter((value): value is number => typeof value === "number");
  const y = yValues.length > 0 ? Math.min(...yValues) : chartHeight / 2;
  return clamp((y / chartHeight) * 100, 16, 78);
}

function chartPointAriaLabel(point: TimelineCanvasPoint) {
  const detail = [
    `time ${point.time}`,
    `chat ${formatSignedNumber(point.chat)}`,
    `voice ${formatSignedNumber(point.transcript)}`,
    `aggregate ${formatSignedNumber(point.aggregate)}`,
    `messages ${compactNumber(point.activity || point.messages || 0, 0)}`
  ];
  if (point.reactionType) detail.push(`reaction ${humanizeLabel(point.reactionType)}`);
  if (point.targetText) detail.push(`target ${truncateText(point.targetText, 64)}`);
  return detail.join(", ");
}

function drawCanvasLine(
  ctx: CanvasRenderingContext2D,
  points: Array<{ x: number; chatY?: number; transcriptY?: number; aggregateY?: number }>,
  key: "chatY" | "transcriptY" | "aggregateY",
  style: { color: string; width: number; dash?: number[] }
) {
  const clean = points.filter((point) => typeof point[key] === "number");
  if (clean.length < 2) return;
  ctx.beginPath();
  clean.forEach((point, index) => {
    const y = point[key] || 0;
    if (index === 0) {
      ctx.moveTo(point.x, y);
      return;
    }
    ctx.lineTo(point.x, y);
  });
  ctx.strokeStyle = style.color;
  ctx.lineWidth = style.width;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.setLineDash(style.dash || []);
  ctx.stroke();
  ctx.setLineDash([]);
}

function drawCanvasDot(ctx: CanvasRenderingContext2D, x: number, y: number | undefined, color: string, radius: number) {
  if (typeof y !== "number") return;
  ctx.beginPath();
  ctx.arc(x, y, radius, 0, Math.PI * 2);
  ctx.fillStyle = color;
  ctx.fill();
  ctx.strokeStyle = "#0a0a0a";
  ctx.lineWidth = 2.4;
  ctx.stroke();
}

function roundedRect(ctx: CanvasRenderingContext2D, x: number, y: number, width: number, height: number, radius: number) {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + width - r, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + r);
  ctx.lineTo(x + width, y + height - r);
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
  ctx.lineTo(x + r, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}

function compactChartTime(value: string) {
  const [clock] = value.split(" ");
  const parts = clock.split(":");
  if (parts.length >= 3) return `${parts[1]}:${parts[2]}`;
  return value || "--:--";
}

function formatAxisTick(value: number) {
  if (value === 0) return "0";
  return `${value > 0 ? "+" : ""}${value.toFixed(1)}`;
}

function buildTopicSeries(points: SentimentPoint[]) {
  const recent = points.slice(-24);
  return {
    chat: recent.map((point) => point.chat),
    transcript: recent.map((point) => point.transcript),
    aggregate: recent.map((point) => point.aggregate ?? point.chat)
  };
}

function topicInsightPath(values: Array<number | undefined>) {
  if (!values.some((value) => typeof value === "number")) return "";
  const width = 100;
  const height = 76;
  const clean = fillMissingValues(values).slice(-24);
  if (clean.length < 2) return "";
  const bounds = dynamicSentimentDomain(clean);
  const xScale = scaleLinear()
    .domain([0, Math.max(1, clean.length - 1)])
    .range([0, width]);
  const yScale = scaleLinear()
    .domain(bounds)
    .range([height - 10, 10]);
  const path = line<number>()
    .x((_, index) => xScale(index))
    .y((value) => yScale(clamp(value, -1, 1)))
    .curve(curveCatmullRom.alpha(0.5));
  return path(clean) || "";
}

function topicVarianceBars(chatValues: Array<number | undefined>, transcriptValues: Array<number | undefined>) {
  if (!transcriptValues.some((value) => typeof value === "number")) return [];
  const chat = fillMissingValues(chatValues).slice(-24);
  const transcript = fillMissingValues(transcriptValues).slice(-24);
  if (chat.length < 2 || transcript.length < 2) return [];
  const length = Math.min(chat.length, transcript.length);
  const width = 100;
  const height = 76;
  const values = [...chat.slice(-length), ...transcript.slice(-length)];
  const bounds = dynamicSentimentDomain(values);
  const xScale = scaleLinear()
    .domain([0, Math.max(1, length - 1)])
    .range([0, width]);
  const yScale = scaleLinear()
    .domain(bounds)
    .range([height - 10, 10]);
  return chat.slice(-length).map((chatValue, index) => {
    const transcriptValue = transcript.slice(-length)[index];
    const yA = yScale(clamp(chatValue, -1, 1));
    const yB = yScale(clamp(transcriptValue, -1, 1));
    const x = xScale(index);
    const nextX = xScale(Math.min(length - 1, index + 1));
    const delta = Math.abs(chatValue - transcriptValue);
    return {
      key: `${index}-${chatValue}-${transcriptValue}`,
      x: Math.max(0, x - 0.6),
      y: Math.min(yA, yB),
      width: Math.max(1.2, nextX - x - 0.7),
      height: Math.max(0.8, Math.abs(yA - yB)),
      close: delta < 0.25
    };
  });
}

function topicActivityBars(buckets: ChatBucket[], pointCount: number) {
  const recent = buckets.slice(0, Math.max(pointCount, 2)).reverse();
  if (recent.length < 2) return [];
  const maxMessages = Math.max(...recent.map((bucket) => bucket.message_count || 0), 1);
  const xScale = scaleLinear()
    .domain([0, Math.max(1, recent.length - 1)])
    .range([0, 100]);
  return recent.map((bucket, index) => {
    const x = xScale(index);
    const nextX = xScale(Math.min(recent.length - 1, index + 1));
    const height = Math.max(2, ((bucket.message_count || 0) / maxMessages) * 18);
    return {
      key: `${bucket.session_id}-${bucket.bucket_start}-${index}`,
      x: Math.max(0, x - 0.4),
      y: 66 - height,
      width: Math.max(1, nextX - x - 0.7),
      height
    };
  });
}

function fillMissingValues(values: Array<number | undefined>) {
  const filled: number[] = [];
  let last = 0;
  values.forEach((value) => {
    if (typeof value === "number" && Number.isFinite(value)) {
      last = clamp(value, -1, 1);
      filled.push(last);
      return;
    }
    if (filled.length > 0) {
      filled.push(last);
    }
  });
  return filled.length > 1 ? filled : [0, 0];
}

function dynamicSentimentDomain(values: number[]): [number, number] {
  const minimum = Math.min(...values, 0);
  const maximum = Math.max(...values, 0);
  const midpoint = (minimum + maximum) / 2;
  const span = Math.max(0.24, maximum - minimum);
  const lower = clamp(midpoint - span * 0.72, -1, 1);
  const upper = clamp(midpoint + span * 0.72, -1, 1);
  if (upper - lower < 0.2) {
    return [clamp(midpoint - 0.1, -1, 0.8), clamp(midpoint + 0.1, -0.8, 1)];
  }
  return [lower, upper];
}

function valueRange(values: Array<number | undefined>) {
  const clean = fillMissingValues(values);
  return Math.max(...clean) - Math.min(...clean);
}

function valueTrend(values: Array<number | undefined>) {
  const clean = fillMissingValues(values);
  return clean[clean.length - 1] - clean[0];
}

function heatCells(bucket?: ChatBucket, alignment?: AlignmentBucket) {
  if (!bucket && !alignment) {
    return Array.from({ length: 12 }, () => false);
  }
  const positive = ratioToPercent(bucket?.positive);
  const negative = ratioToPercent(bucket?.negative);
  const agreement = typeof alignment?.similarity === "number" ? Math.round(alignment.similarity * 100) : 0;
  const seed = positive + negative + agreement + (bucket?.message_count || 0);
  return Array.from({ length: 12 }, (_, index) => ((seed + index * 17) % 41) > 16);
}

function keywordRows(bucket?: ChatBucket, messages: ChatMessage[] = []) {
  const sourceTerms = bucket?.top_terms?.filter(Boolean).slice(0, 5) || [];
  const terms = sourceTerms.length > 0 ? sourceTerms : fallbackKeywords(messages);
  if (terms.length === 0) {
    return [];
  }
  return terms.slice(0, 5).map((term, index) => ({
    term,
    count: Math.max(1, (bucket?.message_count || messages.length || 1) - index * 3)
  }));
}

function fallbackKeywords(messages: ChatMessage[]) {
  const stop = new Set(["the", "and", "that", "this", "with", "for", "you", "are", "was", "have", "has", "not", "but", "just"]);
  const counts = new Map<string, number>();
  messages.slice(0, 80).forEach((message) => {
    (message.text || "").toLowerCase().match(/[a-z0-9_]{4,}/g)?.forEach((term) => {
      if (stop.has(term)) return;
      counts.set(term, (counts.get(term) || 0) + 1);
    });
  });
  return Array.from(counts.entries())
    .sort((first, second) => second[1] - first[1])
    .slice(0, 5)
    .map(([term]) => term);
}

function DashboardHeader({ state, status, clock }: { state: DashboardState; status: string; clock: Date }) {
  const statusLabel = status.startsWith("error") ? "ERROR" : state.session_id ? "LIVE" : state.status === "starting" ? "STARTING" : "READY";
  return (
    <header className="header-panel">
      <div>
        <p className="eyebrow">Stream Reaction Intelligence</p>
        <h1 className="hero-title">
          SRI.DASH<span>_</span>V2
        </h1>
      </div>
      <div className="meta-tags">
        <div className={`tag ${state.session_id ? "live" : ""}`}>{statusLabel}</div>
        <div className="tag">{clock.toLocaleTimeString([], { hour12: false })}</div>
      </div>
    </header>
  );
}

function Panel({ title, meta, className = "", children }: { title: ReactNode; meta?: ReactNode; className?: string; children: ReactNode }) {
  return (
    <section className={`panel ${className}`}>
      <div className="panel-header">
        <span>{title}</span>
        {meta ? <strong>{meta}</strong> : null}
      </div>
      <div className="panel-body">{children}</div>
    </section>
  );
}

function ControlPanel({
  channelInput,
  onChannelInput,
  onSubmit,
  pending,
  state,
  status,
  stream
}: {
  channelInput: string;
  onChannelInput: (value: string) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  pending: boolean;
  state: DashboardState;
  status: string;
  stream?: StreamInfo;
}) {
  const channel = state.channel || extractChannel(channelInput) || "NO CHANNEL";
  const parts = [
    stream?.game,
    stream?.language ? `LANG ${stream.language}` : "",
    typeof stream?.viewer_count === "number" ? `${stream.viewer_count.toLocaleString()} VIEWERS` : ""
  ].filter(Boolean);
  return (
    <Panel className="control-panel" title="Session Control" meta={channel}>
      <form className="start-form" onSubmit={onSubmit}>
        <input
          value={channelInput}
          onChange={(event) => onChannelInput(event.target.value)}
          placeholder="twitch.tv/channel or youtube.com/watch?v=..."
          autoComplete="off"
        />
        <button type="submit" disabled={pending} aria-label={pending ? "Starting session" : "Start session"}>
          <Play size={16} />
          {pending ? "Starting" : "Start"}
        </button>
      </form>
      <div className="control-stack">
        <span className={`color-block ${status.startsWith("error") ? "bg-pink" : state.session_id ? "bg-mint" : "bg-white"}`}>
          {status}
        </span>
        {stream?.title ? <p className="stream-title">{stream.title}</p> : null}
        <p>{parts.length > 0 ? parts.join(" / ") : "Backend state ready for the next live session."}</p>
      </div>
    </Panel>
  );
}

function ChatVelocityPanel({ buckets, messages }: { buckets: ChatBucket[]; messages: ChatMessage[] }) {
  const latest = buckets[0];
  const previous = buckets[1];
  const currentRate = bucketRate(latest) || messages.length;
  const peakRate = buckets.length > 0 ? Math.max(...buckets.map(bucketRate)) : currentRate;
  const trend = trendLabel(bucketRate(latest), bucketRate(previous));
  return (
    <Panel className="vel-panel" title="Chat Velocity" meta="MSGS/MIN">
      <div className="huge-metric" aria-label={`${currentRate} messages per minute`}>
        {metricCharacters(compactNumber(currentRate, 0)).map((char, index) => (
          <span key={`${char}-${index}`} className={["bg-mint", "bg-pink", "bg-yellow", "bg-blue", "bg-white"][index % 5]}>
            {char}
          </span>
        ))}
      </div>
      <div className="number-sub">
        <span className="color-block bg-teal">{trend}</span>
        <span className="color-block bg-peach">PEAK {compactNumber(peakRate, 0)}</span>
      </div>
    </Panel>
  );
}

function MiniMetricPanel({ title, value, color, meta }: { title: string; value: string; color: string; meta?: string }) {
  return (
    <Panel className="mini-panel" title={title} meta={meta}>
      <div className="mini-val">
        <span className={color}>{value}</span>
      </div>
    </Panel>
  );
}

function ReadPanel({ state, latestTranscriptBucket }: { state: DashboardState; latestTranscriptBucket?: TranscriptBucket }) {
  const latestAlignment = state.alignments?.[0];
  const latestChatBucket = state.buckets?.[0];
  const read = latestAlignment ? alignedRead(latestAlignment) : chatPreviewRead(latestChatBucket, latestTranscriptBucket);
  return (
    <Panel className="read-panel" title="Current Read" meta={read.status.toUpperCase()}>
      <motion.div className="read-card" initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}>
        <span className={`read-status ${read.tone}`}>{read.status}</span>
        <h2>{compactReadHeadline(read.status)}</h2>
        <p>{compactReadSummary(read.chatMood, read.transcriptMood)}</p>
      </motion.div>
      <div className="read-grid">
        <ReadMetric label="Chat" value={read.chatMood} tone={read.chatTone} />
        <ReadMetric label="Voice" value={read.transcriptMood} tone={read.transcriptTone} />
        <ReadMetric label="Agreement" value={read.agreement} />
        <ReadMetric label="Window" value={read.window} />
      </div>
    </Panel>
  );
}

function ReadMetric({ label, value, tone }: { label: string; value: string; tone?: ReadTone }) {
  return (
    <div>
      <span>{label}</span>
      <strong className={tone ? `metric-tone ${tone}` : undefined}>{value}</strong>
    </div>
  );
}

function compactReadHeadline(status: string) {
  if (status === "split") return "Split";
  if (status === "partly split") return "Partial split";
  if (status === "in sync") return "In sync";
  if (status === "aligning") return "Aligning";
  return "Collecting";
}

function compactReadSummary(chatMood: string, transcriptMood: string) {
  return `Chat ${chatMood}; voice ${transcriptMood}.`;
}

function SentimentFlowPanel({ alignments, buckets }: { alignments: AlignmentBucket[]; buckets: ChatBucket[] }) {
  const points = useMemo(() => buildSentimentPoints(alignments, buckets), [alignments, buckets]);
  const meta = alignments.length > 0 ? "10 MIN WINDOW" : buckets.length > 0 ? "CHAT PREVIEW" : "WAITING";
  const width = 1000;
  const height = 300;
  const padding = { top: 34, right: 28, bottom: 44, left: 62 };
  const xScale = scaleLinear()
    .domain([0, Math.max(1, points.length - 1)])
    .range([padding.left, width - padding.right]);
  const yScale = scaleLinear()
    .domain([-1, 1])
    .range([height - padding.bottom, padding.top]);
  const chartPoints: ChartSentimentPoint[] = points.map((point, index) => ({
    ...point,
    index,
    x: xScale(index),
    chatY: yScale(clamp(point.chat ?? 0, -1, 1)),
    transcriptY: yScale(clamp(point.transcript ?? 0, -1, 1)),
    aggregateY: yScale(clamp(point.aggregate ?? 0, -1, 1))
  }));
  const chatRibbon = signalPath(chartPoints.filter((point) => typeof point.chat === "number").map((point) => ({ x: point.x, y: point.chatY, value: point.chat ?? 0 })));
  const transcriptRibbon = signalPath(
    chartPoints.filter((point) => typeof point.transcript === "number").map((point) => ({ x: point.x, y: point.transcriptY, value: point.transcript ?? 0 }))
  );
  const aggregateRibbon = signalPath(
    chartPoints.filter((point) => typeof point.aggregate === "number").map((point) => ({ x: point.x, y: point.aggregateY, value: point.aggregate ?? 0 }))
  );
  const alignedPoints = chartPoints.filter((point) => typeof point.chat === "number" && typeof point.transcript === "number");
  const convergence = least(alignedPoints, (point) => Math.abs((point.chat ?? 0) - (point.transcript ?? 0)));
  const divergence = greatest(alignedPoints, (point) => Math.abs((point.chat ?? 0) - (point.transcript ?? 0)));
  const ribbonCrossings = findRibbonCrossings(alignedPoints);
  const yTicks = yScale.ticks(5).filter((value) => value >= -1 && value <= 1);
  const xTicks = ["0:00", "2:30", "5:00", "7:30", "10:00"].map((label, index) => ({
    label,
    x: xScale((Math.max(1, points.length - 1) * index) / 4)
  }));
  const now = chartPoints[chartPoints.length - 1];
  const maxDelta = divergence ? Math.abs((divergence.chat ?? 0) - (divergence.transcript ?? 0)) : undefined;
  const chatNow = now?.chat;
  const transcriptNow = now?.transcript;
  const chartMotion = { duration: 0.7, ease: "easeOut" as const };

  return (
    <Panel
      className="graph-panel sentiment-flow-panel"
      title={(
        <span className="flow-title">
          <span>SENTIMENT FLOW</span>
          <i className="legend-rect chat" />Chat
          <i className="legend-rect transcript" />Transcript
          <i className="legend-rect aggregate" />Aggregate
        </span>
      )}
      meta={meta}
    >
      <div className="sentiment-ribbon-card" role="img" aria-label="Chat, transcript, and aggregate sentiment ribbons over ten minutes">
        <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
          {yTicks.map((value) => {
            const y = yScale(value);
            return (
              <g key={value}>
                <line className={value === 0 ? "chart-zero" : "chart-grid"} x1={padding.left} x2={width - padding.right} y1={y} y2={y} />
                <text className="chart-axis-label" x={padding.left - 12} y={y + 4} textAnchor="end">
                  {value > 0 ? `+${value.toFixed(1)}` : value.toFixed(1)}
                </text>
              </g>
            );
          })}
          {xTicks.map(({ label, x }) => {
            return (
              <g key={label}>
                <line className="chart-tick" x1={x} x2={x} y1={height - padding.bottom + 5} y2={height - padding.bottom + 13} />
                <text className="chart-time-label" x={x} y={height - 12} textAnchor="middle">{label}</text>
              </g>
            );
          })}
          {chartPoints.slice(0, -1).map((point, index) => {
            const next = chartPoints[index + 1];
            if (typeof point.chat !== "number" || typeof point.transcript !== "number" || typeof next.chat !== "number" || typeof next.transcript !== "number") return null;
            const delta = Math.abs(point.chat - point.transcript);
            const nextDelta = Math.abs(next.chat - next.transcript);
            const close = (delta + nextDelta) / 2 < 0.28;
            const topLeft = Math.min(point.chatY, point.transcriptY);
            const bottomLeft = Math.max(point.chatY, point.transcriptY);
            const topRight = Math.min(next.chatY, next.transcriptY);
            const bottomRight = Math.max(next.chatY, next.transcriptY);
            return (
              <path
                key={`${point.key}-fill`}
                className={close ? "flow-close-fill" : "flow-diverge-fill"}
                d={`M ${point.x} ${topLeft} L ${next.x} ${topRight} L ${next.x} ${bottomRight} L ${point.x} ${bottomLeft} Z`}
              />
            );
          })}
          {chatRibbon ? <motion.path className="sentiment-ribbon chat" initial={false} animate={{ d: chatRibbon }} transition={chartMotion} /> : null}
          {transcriptRibbon ? <motion.path className="sentiment-ribbon transcript" initial={false} animate={{ d: transcriptRibbon }} transition={chartMotion} /> : null}
          {aggregateRibbon ? <motion.path className="sentiment-ribbon aggregate" initial={false} animate={{ d: aggregateRibbon }} transition={chartMotion} /> : null}
          {ribbonCrossings.map((crossing) => (
            <motion.path key={crossing.key} className="ribbon-overlap" initial={false} animate={{ d: crossing.d }} transition={chartMotion} />
          ))}
          {convergence ? (
            <g className="marker convergence-marker">
              <motion.line initial={false} animate={{ x1: convergence.x, x2: convergence.x }} transition={chartMotion} y1={padding.top - 8} y2={height - padding.bottom + 16} />
              <motion.circle initial={false} animate={{ cx: convergence.x, cy: (convergence.chatY + convergence.transcriptY) / 2 }} transition={chartMotion} r="7" />
              <motion.text initial={false} animate={{ x: convergence.x + 10 }} transition={chartMotion} y={padding.top + 12}>CONVERGE</motion.text>
            </g>
          ) : null}
          {divergence ? (
            <g className="marker divergence-marker">
              <motion.line initial={false} animate={{ x1: divergence.x, x2: divergence.x }} transition={chartMotion} y1={padding.top - 8} y2={height - padding.bottom + 16} />
              <motion.circle className="chat-dot" initial={false} animate={{ cx: divergence.x, cy: divergence.chatY }} transition={chartMotion} r="6" />
              <motion.circle className="transcript-dot" initial={false} animate={{ cx: divergence.x, cy: divergence.transcriptY }} transition={chartMotion} r="6" />
              <motion.text initial={false} animate={{ x: Math.max(padding.left + 8, divergence.x - 42), y: Math.min(divergence.chatY, divergence.transcriptY) - 14 }} transition={chartMotion}>
                {`Δ ${formatDelta(maxDelta)}`}
              </motion.text>
            </g>
          ) : null}
          {now ? (
            <>
              <motion.line className="now-line" initial={false} animate={{ x1: now.x, x2: now.x }} transition={chartMotion} y1={padding.top - 8} y2={height - padding.bottom + 18} />
              {typeof now.chat === "number" ? <motion.circle className="now-dot chat" initial={false} animate={{ cx: now.x, cy: now.chatY }} transition={chartMotion} r="6" /> : null}
              {typeof now.transcript === "number" ? <motion.circle className="now-dot transcript" initial={false} animate={{ cx: now.x, cy: now.transcriptY }} transition={chartMotion} r="6" /> : null}
              {typeof now.aggregate === "number" ? <motion.circle className="now-dot aggregate" initial={false} animate={{ cx: now.x, cy: now.aggregateY }} transition={chartMotion} r="6" /> : null}
              <motion.text className="now-label" initial={false} animate={{ x: now.x - 4 }} transition={chartMotion} y={padding.top - 14} textAnchor="end">NOW</motion.text>
            </>
          ) : null}
        </svg>
        {points.length === 0 ? <div className="chart-empty">Waiting for scored sentiment windows.</div> : null}
      </div>
      <div className="flow-stat-strip">
        <RibbonStat label="Converge At" value={convergence?.time || "-"} tone="green" />
        <RibbonStat label="Max Delta" value={typeof maxDelta === "number" ? `Δ ${formatDelta(maxDelta)}` : "-"} tone="pink" />
        <RibbonStat label="Chat Now" value={formatSignedNumber(chatNow)} tone="chat" />
        <RibbonStat label="Transcript Now" value={formatSignedNumber(transcriptNow)} tone="transcript" />
      </div>
    </Panel>
  );
}

function RibbonStat({ label, value, tone }: { label: string; value: string; tone: "green" | "pink" | "chat" | "transcript" }) {
  return (
    <div className="flow-stat-card">
      <span>{label}</span>
      <strong className={tone}>{value}</strong>
    </div>
  );
}

function TranscriptAnalysisPanel({ messages }: { messages: ChatMessage[] }) {
  const rows = useMemo(() => liveChatRows(messages), [messages]);
  return (
    <Panel className="transcript-panel" title="Streamer Chat" meta="DIRECT LIVE">
      <div className="chat-list">
        {rows.length === 0 ? (
          <div className="empty-state">No live chat messages yet.</div>
        ) : (
          rows.map((row) => (
            <motion.div className="chat-msg-row live-chat-row" key={row.key} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}>
              <span className="chat-time">{row.time}</span>
              <span className="chat-user">{row.user}</span>
              <span className="chat-text">{row.text}</span>
            </motion.div>
          ))
        )}
      </div>
    </Panel>
  );
}

function ReactionMixPanel({
  alignments,
  buckets,
  transcriptBucket
}: {
  alignments: AlignmentBucket[];
  buckets: ChatBucket[];
  transcriptBucket?: TranscriptBucket;
}) {
  const latestAlignment = alignments[0];
  const latestBucket = buckets[0];
  const chatScore = latestAlignment?.chat_sentiment ?? latestBucket?.chat_sentiment;
  const transcriptScore = latestAlignment?.transcript_sentiment ?? transcriptBucket?.sentiment_score;
  const positive = ratioToPercent(latestBucket?.positive);
  const neutral = ratioToPercent(latestBucket?.neutral);
  const negative = ratioToPercent(latestBucket?.negative);
  const gap = typeof latestAlignment?.delta === "number" ? Math.abs(latestAlignment.delta) : undefined;
  const relation = relationshipLabel(latestAlignment?.relationship);
  const transcriptPosition = scoreToPercent(transcriptScore);
  const chatPosition = scoreToPercent(chatScore);

  return (
    <Panel className="map-panel" title="Reaction Mix" meta={relation.label === "waiting" ? "LATEST" : relation.label.toUpperCase()}>
      <div className="reaction-mix">
        <div className="mix-row">
          <span>Chat distribution</span>
          <strong>{formatSignedNumber(chatScore)}</strong>
        </div>
        <div className="stacked-bar" aria-label="Latest chat positive neutral negative mix">
          <i className="mix-pos" style={{ width: `${positive}%` }} />
          <i className="mix-neu" style={{ width: `${neutral}%` }} />
          <i className="mix-neg" style={{ width: `${negative}%` }} />
        </div>
        <div className="mix-labels">
          <span>POS {positive}%</span>
          <span>NEU {neutral}%</span>
          <span>NEG {negative}%</span>
        </div>

        <div className="mix-row">
          <span>Transcript sentiment</span>
          <strong>{formatSignedNumber(transcriptScore)}</strong>
        </div>
        <div className="sentiment-meter">
          <span>NEG</span>
          <div>
            <i style={{ left: `${transcriptPosition}%` }} />
          </div>
          <span>POS</span>
        </div>

        <div className="mix-row">
          <span>Chat position</span>
          <strong>{formatSignedNumber(chatScore)}</strong>
        </div>
        <div className="sentiment-meter chat-meter">
          <span>NEG</span>
          <div>
            <i style={{ left: `${chatPosition}%` }} />
          </div>
          <span>POS</span>
        </div>

        <div className="mix-grid">
          <div>
            <span>Agreement</span>
            <strong>{formatPercent(latestAlignment?.similarity)}</strong>
          </div>
          <div>
            <span>Gap</span>
            <strong>{typeof gap === "number" ? gap.toFixed(2) : "-"}</strong>
          </div>
          <div>
            <span>Messages</span>
            <strong>{String(latestBucket?.message_count || latestAlignment?.chat_message_count || 0)}</strong>
          </div>
          <div>
            <span>State</span>
            <strong>{relationshipStatus(relation.key)}</strong>
          </div>
        </div>
      </div>
    </Panel>
  );
}

function LiveTranscriptPanel({ status, segments }: { status: string; segments: TranscriptSegment[] }) {
  return (
    <Panel className="speech-panel" title="Streamer Transcript" meta={status}>
      <div className="transcript-flow">
        <AnimatePresence initial={false}>
          {segments.length === 0 ? (
            <p className="empty-state">No transcript segments yet.</p>
          ) : (
            segments.map((segment) => (
              <motion.p
                key={[segment.session_id, segment.transcript_start, segment.transcript_end].join(":")}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0 }}
              >
                <span className="flow-time">{formatTime(segment.transcript_start)}</span>{" "}
                <span className="transcript-words">{segment.text || "(no speech detected)"}</span>
              </motion.p>
            ))
          )}
        </AnimatePresence>
      </div>
    </Panel>
  );
}

function LivePlayerPanel({ channel }: { channel?: string }) {
  const channelName = channel ? extractChannel(channel) : "";
  const parentCandidates = [window.location.hostname, "localhost", "127.0.0.1"].filter(Boolean);
  const parentParams = Array.from(new Set(parentCandidates))
    .map((parent) => `parent=${encodeURIComponent(parent)}`)
    .join("&");
  return (
    <Panel className="player-panel" title="Livestream" meta={channelName || "waiting"}>
      <div className="player-shell">
        {channelName ? (
          <iframe
            title="Twitch livestream preview"
            src={`https://player.twitch.tv/?channel=${encodeURIComponent(channelName)}&${parentParams}&muted=true&autoplay=false`}
            allow="autoplay; fullscreen; picture-in-picture"
            allowFullScreen
            referrerPolicy="origin"
          />
        ) : (
          <div className="empty-state">Preview waiting for channel.</div>
        )}
      </div>
    </Panel>
  );
}

function ChatBucketPanel({ buckets }: { buckets: ChatBucket[] }) {
  return (
    <Panel className="bucket-panel" title="Chat Scored Windows" meta={`${buckets.length} windows`}>
      <ol className="bucket-feed">
        {buckets.length === 0 ? (
          <li className="empty-state">No scored chat windows yet.</li>
        ) : (
          buckets.slice(0, 12).map((bucket, index) => (
            <BucketCard
              key={`${bucket.session_id}-${bucket.bucket_start}-${index}`}
              title={`${formatTime(bucket.bucket_start)} - ${formatTime(bucket.bucket_end)}`}
              score={formatSignedNumber(bucket.chat_sentiment)}
            >
              <Metric label="Messages" value={String(bucket.message_count || 0)} />
              <Metric label="Unique" value={String(bucket.unique_chatters || 0)} />
              <Metric label="Confidence" value={formatPercent(bucket.sentiment_confidence)} />
              <Metric label="Analyzed" value={String(bucket.analyzed_count || bucket.message_count || 0)} />
              <Metric label="Pos / Neu / Neg" value={`${formatRatio(bucket.positive)} / ${formatRatio(bucket.neutral)} / ${formatRatio(bucket.negative)}`} />
              <Metric label="Latency" value={formatLatency(bucket.analysis_latency_ms)} />
              <WideMetric label="Language mix" value={formatLanguageMix(bucket.language_mix)} />
              <WideMetric label="Top terms" value={formatList(bucket.top_terms)} />
              <WideMetric label="Top emotes" value={formatList(bucket.top_emotes)} />
            </BucketCard>
          ))
        )}
      </ol>
    </Panel>
  );
}

function TranscriptBucketPanel({ buckets, count }: { buckets: TranscriptBucket[]; count: number }) {
  return (
    <Panel className="bucket-panel transcript-bucket-panel" title="Transcript Scored Windows" meta={`${count} windows`}>
      <ol className="bucket-feed">
        {buckets.length === 0 ? (
          <li className="empty-state">No transcript buckets yet.</li>
        ) : (
          buckets.slice(0, 12).map((bucket, index) => (
            <BucketCard
              key={`${bucket.session_id}-${bucket.bucket_start}-${index}`}
              title={`${formatTime(bucket.bucket_start)} - ${formatTime(bucket.bucket_end)}`}
              score={transcriptScoreValue(bucket)}
            >
              <Metric label="Sentiment" value={transcriptScoreValue(bucket)} />
              <Metric label="Label" value={transcriptScoreLabel(bucket)} />
              <Metric label="Model conf." value={formatPercent(bucket.sentiment_confidence)} />
              <Metric label="Language" value={bucket.language || "unknown"} />
              <Metric label="Segments" value={String(bucket.segments?.length || 0)} />
              <Metric label="Latency" value={formatLatency(bucket.sentiment_latency_ms)} />
              <Metric label="Status" value={bucket.sentiment_status || "pending"} />
              <WideMetric label="Model" value={bucket.sentiment_model || "-"} />
              <WideMetric label="Transcript text" value={bucket.text || "(no speech detected)"} />
            </BucketCard>
          ))
        )}
      </ol>
    </Panel>
  );
}

function AlignmentPanel({ alignments }: { alignments: AlignmentBucket[] }) {
  return (
    <Panel className="alignment-panel" title="Chat / Transcript Alignment" meta={alignments.length > 0 ? `${alignments.length} matched` : "waiting"}>
      <ol className="alignment-feed">
        {alignments.length === 0 ? (
          <li className="alignment-empty">Backend alignment waits for one scored chat bucket and one scored transcript bucket with enough time overlap.</li>
        ) : (
          alignments.slice(0, 8).map((match, index) => <AlignmentItem key={`${match.session_id}-${match.window_start}-${index}`} match={match} />)
        )}
      </ol>
    </Panel>
  );
}

function AlignmentItem({ match }: { match: AlignmentBucket }) {
  const relation = relationshipLabel(match.relationship);
  return (
    <li>
      <div className="bucket-title">
        <strong>{formatTime(match.window_start)} - {formatTime(match.window_end)}</strong>
        <span className={`score-pill ${relation.key}`}>{relationshipStatus(relation.key)}</span>
      </div>
      <div className="bucket-grid">
        <Metric label="Chat" value={formatSignedNumber(match.chat_sentiment)} />
        <Metric label="Voice" value={formatSignedNumber(match.transcript_sentiment)} />
        <Metric label="Mood gap" value={moodGapLabel(match.delta)} />
        <Metric label="Agreement" value={formatPercent(match.similarity)} />
        <Metric label="Overlap" value={`${match.overlap_seconds || 0}s`} />
        <Metric label="Messages" value={String(match.chat_message_count || 0)} />
        <Metric label="Quality" value={formatPercent(match.quality)} />
        <WideMetric label="Quality flags" value={formatList(match.quality_flags)} />
      </div>
    </li>
  );
}

function BucketCard({ title, score, children }: { title: string; score: string; children: ReactNode }) {
  return (
    <li className="bucket-card">
      <div className="bucket-title">
        <strong>{title}</strong>
        <span className="score-pill">{score}</span>
      </div>
      <div className="bucket-grid">{children}</div>
    </li>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function WideMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-wide">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function HumanReview({ buckets }: { buckets: ChatBucket[] }) {
  const queryClient = useQueryClient();
  const labelMutation = useMutation({
    mutationFn: ({ sessionID, messageID, label }: { sessionID: string; messageID: string; label: string }) =>
      saveHumanLabel(sessionID, messageID, label),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ["dashboard-state"] })
  });
  const scores = buckets.flatMap((bucket) => (bucket.message_scores || []).map((score) => ({ bucket, score }))).slice(0, 18);

  return (
    <Panel className="review-panel" title="Human Review" meta={scores.length > 0 ? `${scores.length} model traces` : "waiting"}>
      <ol className="trace-feed">
        {scores.length === 0 ? (
          <li className="empty-state">No model trace samples yet.</li>
        ) : (
          scores.map(({ bucket, score }) => (
            <TraceItem
              key={`${bucket.session_id}-${score.message_id}`}
              bucket={bucket}
              score={score}
              onLabel={(label) => {
                if (!bucket.session_id || !score.message_id) return;
                labelMutation.mutate({ sessionID: bucket.session_id, messageID: score.message_id, label });
              }}
            />
          ))
        )}
      </ol>
    </Panel>
  );
}

function TraceItem({ bucket, score, onLabel }: { bucket: ChatBucket; score: MessageScore; onLabel: (label: string) => void }) {
  const humanLabel = score.human_label || "-";
  return (
    <li>
      <div className="bucket-title">
        <strong>{score.display_name || score.username || "unknown"}</strong>
        <span>{formatTime(score.timestamp || bucket.bucket_start)}</span>
      </div>
      <div className="trace-text">{score.text}</div>
      <div className="trace-score">
        <span>Model</span>
        <strong>{score.label || "neutral"} / {formatNumber(score.confidence)}</strong>
        <span>Human</span>
        <strong>{humanLabel}</strong>
      </div>
      <div className="label-controls">
        {["positive", "neutral", "negative"].map((label) => (
          <button key={label} type="button" className={humanLabel === label ? "selected" : ""} onClick={() => onLabel(label)}>
            {label}
          </button>
        ))}
      </div>
    </li>
  );
}

function buildSentimentPoints(alignments: AlignmentBucket[], buckets: ChatBucket[], transcriptBuckets: TranscriptBucket[] = []): SentimentPoint[] {
  if (alignments.length > 0) {
    return alignments
      .slice(0, 24)
      .reverse()
      .map((item, index) => ({
        key: `${item.session_id}-${item.window_start}-${index}`,
        time: formatTime(item.window_start),
        chat: item.chat_sentiment,
        transcript: item.transcript_sentiment,
        aggregate: averageSignals(item.chat_sentiment, item.transcript_sentiment),
        delta: item.delta,
        similarity: item.similarity,
        relationship: item.relationship,
        messages: item.chat_message_count
      }));
  }

  if (transcriptBuckets.length > 0 && buckets.length === 0) {
    return transcriptBuckets
      .filter((bucket) => typeof bucket.sentiment_score === "number")
      .slice(0, 24)
      .reverse()
      .map((bucket, index) => {
        const score = bucket.sentiment_score;
        return {
          key: `${bucket.session_id}-${bucket.bucket_start}-${index}`,
          time: formatTime(bucket.bucket_start),
          source: "transcript",
          transcript: score,
          aggregate: score,
          messages: bucket.word_count ?? captionWords(bucket.text || "").length,
          confidence: bucket.sentiment_confidence,
          text: bucket.text
        };
      });
  }

  return buckets
    .slice(0, 24)
    .reverse()
    .map((bucket, index) => ({
      key: `${bucket.session_id}-${bucket.bucket_start}-${index}`,
      time: formatTime(bucket.bucket_start),
      chat: bucket.chat_sentiment,
      aggregate: bucket.chat_sentiment,
      messages: bucket.message_count
    }));
}

function buildReactionWindowPoints(windows: ReactionWindow[]): SentimentPoint[] {
  return windows
    .slice(0, 300)
    .reverse()
    .map((window, index) => {
      const isTranscript = isTranscriptReactionWindow(window);
      return {
        key: `${window.session_id}-${window.source || "chat"}-${window.window_start}-${index}`,
        time: formatTime(window.window_start),
        source: window.source,
        provisional: window.provisional,
        chat: isTranscript ? undefined : window.valence,
        transcript: isTranscript ? window.valence : undefined,
        aggregate: window.valence,
        messages: window.message_count,
        hype: window.hype_score,
        intensity: window.intensity_score,
        confusion: window.confusion_score,
        frustration: window.frustration_score,
        reactionType: window.reaction_type,
        targetType: window.target_type,
        targetText: window.target_text,
        eventHint: window.event_hint,
        confidence: window.confidence ?? window.sentiment_confidence ?? window.transcript_confidence,
        evidenceIDs: window.evidence_ids,
        text: window.target_text
      };
    });
}

function primaryInsightForWindow(
  window: ReactionWindow | undefined,
  transcriptBucket: TranscriptBucket | undefined,
  chatBucket?: ChatBucket,
  alignment?: AlignmentBucket
): PrimaryInsight {
  const sentiment = sentimentDescriptor(window?.valence);
  const relation = alignment ? relationshipStatus(relationshipLabel(alignment.relationship).key) : transcriptBucket ? "matching" : "waiting";
  const delay = processingDelayLabel(chatBucket, transcriptBucket);
  const reactionLabel = window?.reaction_type && window.reaction_type !== "unknown"
    ? humanizeLabel(window.reaction_type)
    : sentiment.label;
  return {
    reaction: window ? reactionLabel : "waiting",
    reactionTone: window ? sentiment.tone : "muted",
    target: window ? targetContextLabel(window.target_type, window.target_text) : "unknown",
    intensity: window ? reactionIntensityLabel(window) : "waiting",
    evidence: window ? reactionEvidenceLabel(window) : "waiting for evidence",
    agreement: relation,
    delay: delay.label,
    delayTone: delay.tone
  };
}

function processingDelayLabel(chatBucket?: ChatBucket, transcriptBucket?: TranscriptBucket) {
  const delayMS = processingDelayMS(chatBucket, transcriptBucket);
  return {
    label: typeof delayMS === "number" ? formatLatency(delayMS) : "waiting",
    tone: latencyTone(delayMS)
  };
}

function processingDelayMS(chatBucket?: ChatBucket, transcriptBucket?: TranscriptBucket) {
  return numericValue(transcriptBucket?.pipeline_latency_ms)
    ?? numericValue(transcriptBucket?.asr_latency_ms)
    ?? numericValue(chatBucket?.analysis_latency_ms);
}

function latencyTone(value?: number): HealthMetricTone {
  if (typeof value !== "number") return "muted";
  if (value >= 10000) return "error";
  if (value >= 3000) return "warn";
  return "ok";
}

function reactionIntensityLabel(window: ReactionWindow) {
  const score = Math.max(
    numericValue(window.intensity_score) ?? 0,
    numericValue(window.hype_score) ?? 0,
    numericValue(window.confusion_score) ?? 0,
    numericValue(window.frustration_score) ?? 0,
    numericValue(window.velocity_score) ?? 0,
    Math.abs(numericValue(window.valence) ?? 0)
  );
  const label = score >= 0.72 ? "high" : score >= 0.38 ? "medium" : "low";
  return `${label} ${formatPercent(score)}`;
}

function reactionEvidenceLabel(window: ReactionWindow) {
  const messageText = window.evidence_messages?.find((message) => message.text)?.text;
  const transcriptText = window.transcript_text;
  const hint = window.event_hint;
  const text = messageText || transcriptText || hint || window.target_text;
  if (text) return truncateText(text, 74);
  const count = windowEvidenceCount(window);
  return count > 0 ? `${compactNumber(count, 0)} evidence ids` : "none captured";
}

function reactionChartDescription(hasLiveTranscriptReaction: boolean, replay: boolean, liveActive: boolean) {
  if (hasLiveTranscriptReaction) {
    return "5s live reaction windows; live transcript reactions update until committed buckets replace them";
  }
  if (replay) return "Stored 5s reaction windows from the selected replay";
  if (liveActive) return "5s live reaction windows update independently from slower model sentiment";
  return "Start a session to collect live reaction windows";
}

function signalPath(points: RibbonPoint[]) {
  if (points.length < 2) return "";
  const signalLine = line<RibbonPoint>()
    .x((point) => point.x)
    .y((point) => point.y)
    .curve(curveCatmullRom.alpha(0.5));
  return signalLine(points) || "";
}

function findRibbonCrossings(points: ChartSentimentPoint[]) {
  return points.slice(0, -1).flatMap((point, index) => {
    const next = points[index + 1];
    const delta = (point.chat ?? 0) - (point.transcript ?? 0);
    const nextDelta = (next.chat ?? 0) - (next.transcript ?? 0);
    if (delta === 0 && nextDelta === 0) return [];
    if (delta !== 0 && nextDelta !== 0 && Math.sign(delta) === Math.sign(nextDelta)) return [];

    const denominator = Math.abs(delta) + Math.abs(nextDelta);
    const ratio = denominator === 0 ? 0.5 : Math.abs(delta) / denominator;
    const x = point.x + (next.x - point.x) * ratio;
    const chatY = point.chatY + (next.chatY - point.chatY) * ratio;
    const transcriptY = point.transcriptY + (next.transcriptY - point.transcriptY) * ratio;
    const y = (chatY + transcriptY) / 2;
    const d = [
      `M ${roundSvg(x - 16)} ${roundSvg(y)}`,
      `C ${roundSvg(x - 10)} ${roundSvg(y - 9)}, ${roundSvg(x + 10)} ${roundSvg(y - 9)}, ${roundSvg(x + 16)} ${roundSvg(y)}`,
      `C ${roundSvg(x + 10)} ${roundSvg(y + 9)}, ${roundSvg(x - 10)} ${roundSvg(y + 9)}, ${roundSvg(x - 16)} ${roundSvg(y)}`,
      "Z"
    ].join(" ");

    return [{ key: `${point.key}-${next.key}-crossing`, d }];
  });
}

function roundSvg(value: number) {
  return Number.isFinite(value) ? value.toFixed(1) : "0";
}

function averageSignals(first?: number, second?: number) {
  const values = [first, second].filter((value): value is number => typeof value === "number");
  if (values.length === 0) return undefined;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function formatDelta(value?: number) {
  return typeof value === "number" ? value.toFixed(2) : "-";
}

function knownText(value?: string | null) {
  const trimmed = value?.trim();
  return trimmed || "unknown";
}

function humanizeLabel(value: string) {
  return value.replace(/[_-]+/g, " ");
}

function formatOptionalPercent(value?: number) {
  return typeof value === "number" ? formatPercent(value) : "unknown";
}

function formatWindowContext(start?: string, end?: string) {
  if (!start && !end) return "unknown";
  return bucketWindowLabel(start, end);
}

function targetContextLabel(targetType?: string, targetText?: string) {
  const knownTarget = knownText(targetType);
  const target = humanizeLabel(knownTarget === "unknown" && targetText?.trim() ? "possible" : knownTarget);
  const text = targetText?.trim();
  return text ? `${target}: ${truncateText(text, 64)}` : target;
}

function windowConfidence(window: ReactionWindow) {
  return window.confidence ?? window.sentiment_confidence ?? window.transcript_confidence;
}

function windowEvidenceCount(window: ReactionWindow) {
  return window.evidence_ids?.length ?? window.evidence_messages?.filter((message) => message.text || message.message_id).length ?? 0;
}

function evidenceCountLabel(value?: number) {
  return value && value > 0 ? compactNumber(value, 0) : "unknown";
}

function formatEvidenceIDs(values?: string[]) {
  if (!values?.length) return "unknown";
  const visible = values.filter(Boolean).slice(0, 3);
  const suffix = values.length > visible.length ? ` +${values.length - visible.length}` : "";
  return `${visible.join(", ")}${suffix}`;
}

function truncateText(value: string, maxLength: number) {
  return value.length > maxLength ? `${value.slice(0, Math.max(0, maxLength - 3))}...` : value;
}

function liveChatRows(messages: ChatMessage[]): LiveChatRow[] {
  return messages.filter((message) => message.text).slice(0, 12).map((message, index) => ({
    key: message.message_id || `${message.timestamp}-${index}`,
    time: formatTime(message.timestamp),
    user: message.display_name || message.username || "unknown",
    text: message.text || ""
  }));
}

function alignedRead(match: AlignmentBucket) {
  const relation = relationshipLabel(match.relationship);
  const chatMood = sentimentDescriptor(match.chat_sentiment);
  const transcriptMood = sentimentDescriptor(match.transcript_sentiment);
  const gap = Math.abs(match.delta ?? 0);
  const gapLabel = gap >= 0.65 ? "wide" : gap >= 0.3 ? "moderate" : "small";
  const overlap = match.overlap_seconds ? `${match.overlap_seconds}s overlap` : "matched window";
  const messageCount = match.chat_message_count ? `${match.chat_message_count} chat messages` : "chat sample";
  const status = relationshipStatus(relation.key);
  return {
    headline: relationshipHeadline(relation.key),
    summary: `Chat reads ${chatMood.label}; streamer speech reads ${transcriptMood.label} in the latest matched window.`,
    status,
    tone: relationTone(relation.key),
    chatMood: `${chatMood.label} ${formatSignedNumber(match.chat_sentiment)}`,
    chatTone: chatMood.tone,
    transcriptMood: `${transcriptMood.label} ${formatSignedNumber(match.transcript_sentiment)}`,
    transcriptTone: transcriptMood.tone,
    agreement: formatPercent(match.similarity),
    window: `${formatTime(match.window_start)} - ${formatTime(match.window_end)}`,
    reason: `${capitalize(gapLabel)} mood gap, ${overlap}, ${messageCount}, ${formatPercent(match.quality)} match quality.`
  };
}

function chatPreviewRead(chatBucket?: ChatBucket, transcriptBucket?: TranscriptBucket) {
  const chatMood = sentimentDescriptor(chatBucket?.chat_sentiment);
  const transcriptMood = sentimentDescriptor(transcriptBucket?.sentiment_score);
  const hasChat = typeof chatBucket?.chat_sentiment === "number";
  const hasTranscript = typeof transcriptBucket?.sentiment_score === "number";
  return {
    headline: hasChat ? `Chat is currently ${chatMood.label}` : "Waiting for the first scored window",
    summary: hasTranscript
      ? "Chat has a live mood score and transcript scoring is active; alignment appears once their windows overlap."
      : "Chat scoring is visible first; streamer transcript needs a scored window before agreement can be measured.",
    status: hasTranscript ? "aligning" : "collecting",
    tone: "muted" as ReadTone,
    chatMood: hasChat ? `${chatMood.label} ${formatSignedNumber(chatBucket?.chat_sentiment)}` : "-",
    chatTone: chatMood.tone,
    transcriptMood: hasTranscript ? `${transcriptMood.label} ${formatSignedNumber(transcriptBucket?.sentiment_score)}` : "waiting",
    transcriptTone: transcriptMood.tone,
    agreement: "waiting",
    window: chatBucket ? `${formatTime(chatBucket.bucket_start)} - ${formatTime(chatBucket.bucket_end)}` : "-",
    reason: hasChat
      ? `${chatBucket?.message_count || 0} chat messages scored; waiting for a transcript window with enough time overlap.`
      : "The dashboard needs at least one closed chat bucket before it can explain the stream mood."
  };
}

function relationTone(value: string): ReadTone {
  if (value === "converged") return "positive";
  if (value === "soft-split") return "neutral";
  if (value === "diverged") return "negative";
  return "muted";
}

function relationshipStatus(value: string) {
  if (value === "converged") return "in sync";
  if (value === "soft-split") return "partly split";
  if (value === "diverged") return "split";
  return "matching";
}

function relationshipHeadline(value: string) {
  if (value === "converged") return "Chat and streamer are in sync";
  if (value === "soft-split") return "Chat and streamer are partly split";
  if (value === "diverged") return "Chat and streamer are split";
  return "Waiting for a matched read";
}

function sentimentDescriptor(value?: number): { label: string; tone: "positive" | "neutral" | "negative" } {
  if (typeof value !== "number") return { label: "waiting", tone: "neutral" };
  if (value >= 0.6) return { label: "very positive", tone: "positive" };
  if (value >= 0.2) return { label: "positive", tone: "positive" };
  if (value > 0.05) return { label: "slightly positive", tone: "positive" };
  if (value <= -0.6) return { label: "very negative", tone: "negative" };
  if (value <= -0.2) return { label: "negative", tone: "negative" };
  if (value < -0.05) return { label: "slightly negative", tone: "negative" };
  return { label: "neutral", tone: "neutral" };
}

function chatAnalysisSource(bucket?: ChatBucket) {
  if (!bucket) return "N/A";
  if (bucket.analysis_status === "python") return "MODEL";
  if (bucket.analysis_status === "fallback_pending") return "MODEL LATE";
  if (bucket.analysis_status === "fallback") return "FALLBACK";
  if (bucket.analysis_status === "local") return "LOCAL";
  return bucket.analysis_status ? bucket.analysis_status.toUpperCase() : "COLLECTING";
}

function chatSentimentDistribution(bucket: ChatBucket | undefined, hasChatData: boolean) {
  const positive = ratioToPercent(bucket?.positive);
  const neutral = ratioToPercent(bucket?.neutral);
  const negative = ratioToPercent(bucket?.negative);
  const total = positive + neutral + negative;
  if (!hasChatData || !bucket) {
    return { positive: 0, neutral: 0, negative: 0 };
  }
  if (total > 0) {
    return { positive, neutral, negative };
  }

  const score = bucket.chat_sentiment ?? 0;
  if (score > 0.15) return { positive: 100, neutral: 0, negative: 0 };
  if (score < -0.15) return { positive: 0, neutral: 0, negative: 100 };
  return { positive: 0, neutral: 100, negative: 0 };
}

function moodGapLabel(value?: number) {
  if (typeof value !== "number") return "-";
  const absolute = Math.abs(value);
  const label = absolute >= 0.65 ? "wide" : absolute >= 0.3 ? "moderate" : "small";
  return `${label} ${formatSignedNumber(value)}`;
}

function bucketRate(bucket?: ChatBucket) {
  if (!bucket?.message_count) return 0;
  const start = bucket.bucket_start ? new Date(bucket.bucket_start).getTime() : Number.NaN;
  const end = bucket.bucket_end ? new Date(bucket.bucket_end).getTime() : Number.NaN;
  const seconds = Number.isFinite(start) && Number.isFinite(end) && end > start ? (end - start) / 1000 : 30;
  return Math.round((bucket.message_count / seconds) * 60);
}

function trendLabel(current: number, previous: number) {
  if (!current || !previous) return "COLLECTING";
  const delta = ((current - previous) / previous) * 100;
  return `${delta >= 0 ? "+" : ""}${Math.round(delta)}% TREND`;
}

function uniqueChatters(buckets: ChatBucket[], messages: ChatMessage[]) {
  if (buckets[0]?.unique_chatters) return buckets[0].unique_chatters;
  return new Set(messages.map((message) => message.display_name || message.username).filter(Boolean)).size;
}

function ratioToPercent(value?: number) {
  return typeof value === "number" ? Math.max(0, Math.min(100, Math.round(value * 100))) : 0;
}

function scoreToPercent(value?: number) {
  return Math.round(((clamp(value ?? 0, -1, 1) + 1) / 2) * 100);
}

function latestMatchQuality(alignments: AlignmentBucket[]) {
  const latest = alignments[0];
  return typeof latest?.quality === "number" ? formatPercent(latest.quality) : "--";
}

function compactNumber(value: number, fractionDigits = value >= 1000 ? 1 : 0) {
  if (!Number.isFinite(value)) return "0";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(fractionDigits)}M`;
  if (value >= 10_000) return `${Math.round(value / 1000)}K`;
  if (value >= 1000) return `${(value / 1000).toFixed(fractionDigits)}K`;
  return Math.round(value).toLocaleString();
}

function positiveNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : undefined;
}

function metricCharacters(value: string) {
  return value.length > 0 ? value.split("") : ["0"];
}

function clamp(value: number, minimum: number, maximum: number) {
  return Math.min(maximum, Math.max(minimum, value));
}

function capitalize(value: string) {
  return value.length > 0 ? value[0].toUpperCase() + value.slice(1) : value;
}

function applyDashboardEvent(current: DashboardState, event: DashboardEvent): DashboardState {
  const next: DashboardState = { ...current };
  if (event.session_id) next.session_id = event.session_id;
  if (event.status) next.status = event.status;
  if (event.channel) next.channel = event.channel;
  if (event.stream) next.stream = event.stream;
  if (event.error) next.error = event.error;
  if (event.message) {
    next.message_count = (next.message_count || 0) + 1;
    next.messages = [event.message, ...(next.messages || [])].slice(0, 120);
  }
  if (event.bucket) {
    const buckets = next.buckets || [];
    const incoming = event.bucket;
    const existingIndex = buckets.findIndex((bucket) => sameChatBucket(bucket, incoming));
    if (existingIndex >= 0) {
      next.buckets = buckets.map((bucket, index) => index === existingIndex ? incoming : bucket);
    } else {
      next.bucket_count = (next.bucket_count || 0) + 1;
      next.buckets = [incoming, ...buckets].slice(0, 80);
    }
  }
  if (event.transcript_bucket) {
    const buckets = next.transcript_buckets || [];
    const incoming = event.transcript_bucket;
    const existingIndex = buckets.findIndex((bucket) => sameTranscriptBucket(bucket, incoming));
    if (existingIndex >= 0) {
      next.transcript_buckets = buckets.map((bucket, index) => index === existingIndex ? incoming : bucket);
    } else {
      next.transcript_buckets = [incoming, ...buckets].slice(0, 80);
    }
  }
  if (event.alignments) {
    next.alignments = event.alignments;
  }
  if (event.signal_windows) {
    next.signal_windows = event.signal_windows;
  }
  if (event.signal_events) {
    next.signal_events = event.signal_events;
  }
  if (event.reaction_window) {
    const windows = next.reaction_windows || [];
    const incoming = event.reaction_window;
    const existingIndex = windows.findIndex((window) =>
      window.session_id === incoming.session_id &&
      window.channel_id === incoming.channel_id &&
      window.window_start === incoming.window_start &&
      window.window_end === incoming.window_end
    );
    if (existingIndex >= 0) {
      next.reaction_windows = windows.map((window, index) => index === existingIndex ? incoming : window);
    } else {
      next.reaction_windows = [incoming, ...windows].slice(0, 300);
    }
  }
  return next;
}

function sameChatBucket(left: ChatBucket, right: ChatBucket) {
  return left.session_id === right.session_id &&
    left.channel_id === right.channel_id &&
    left.bucket_start === right.bucket_start &&
    left.bucket_end === right.bucket_end;
}

function sameTranscriptBucket(left: TranscriptBucket, right: TranscriptBucket) {
  return left.session_id === right.session_id &&
    left.channel_id === right.channel_id &&
    left.bucket_start === right.bucket_start &&
    left.bucket_end === right.bucket_end;
}

function parseEventPayload<T>(data: string): T | undefined {
  try {
    return JSON.parse(data) as T;
  } catch {
    return undefined;
  }
}

export const dashboardUITestAccess = {
  primaryInsightForWindow,
  reactionChartDescription
};
