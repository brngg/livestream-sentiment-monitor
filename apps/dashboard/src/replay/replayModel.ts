import type {
  AlignmentBucket,
  ChatBucket,
  ChatBucketSubwindow,
  SessionReplay,
  SignalEvent,
  SignalWindow,
  TranscriptBucket
} from "../types";

export type ReplaySource = "signal" | "reaction" | "alignment" | "chat" | "transcript";

export type ReplayWindowPoint = {
  id: string;
  source: ReplaySource;
  start?: string;
  end?: string;
  signalWindow?: SignalWindow;
  reactionWindow?: ChatBucketSubwindow;
  alignment?: AlignmentBucket;
  chatBucket?: ChatBucket;
  transcriptBucket?: TranscriptBucket;
  transcriptContext: TranscriptContextBucket[];
  transcriptOffsetSeconds: number;
  events: SignalEvent[];
};

export type ReplayBuildOptions = {
  transcriptOffsetSeconds?: number;
};

export type TranscriptContextRole = "previous" | "current" | "next";

export type TranscriptContextBucket = {
  role: TranscriptContextRole;
  bucket: TranscriptBucket;
};

export type ReplayEvidenceRef = {
  id: string;
  source: "chat" | "transcript" | "unknown";
  label: string;
  text?: string;
};

type WindowRange = {
  start?: string;
  end?: string;
};

export function buildReplayWindows(replay?: SessionReplay, options: ReplayBuildOptions = {}): ReplayWindowPoint[] {
  if (!replay) return [];

  const transcriptOffsetSeconds = clampTranscriptOffset(options.transcriptOffsetSeconds);
  const chatBuckets = replay.chat_buckets || [];
  const transcriptBuckets = replay.transcript_buckets || [];
  const alignments = replay.alignments || [];
  const signalEvents = replay.signal_events || [];
  const signalWindows = replay.signal_windows || [];
  const reactionWindows = buildReactionReplayWindows(
    chatBuckets,
    transcriptBuckets,
    alignments,
    signalEvents,
    transcriptOffsetSeconds
  );

  if (signalWindows.length > 0) {
    const signalReplayWindows: ReplayWindowPoint[] = signalWindows.map((window, index) => {
      const range = { start: window.window_start, end: window.window_end };
      const alignment = findBestAlignment(range, alignments);
      const chatBucket = findBestChatBucket(range, chatBuckets, alignment);
      const transcriptBucket = findBestTranscriptBucket(range, transcriptBuckets, alignment, transcriptOffsetSeconds);
      return {
        id: replayPointID("signal", range, index),
        source: "signal",
        start: range.start,
        end: range.end,
        signalWindow: window,
        alignment,
        chatBucket,
        transcriptBucket,
        transcriptContext: transcriptContextForBucket(transcriptBucket, transcriptBuckets),
        transcriptOffsetSeconds,
        events: eventsForRange(range, [...(window.events || []), ...signalEvents])
      };
    });
    return sortReplayPoints([...signalReplayWindows, ...reactionWindows]);
  }

  if (alignments.length > 0) {
    const alignmentReplayWindows: ReplayWindowPoint[] = alignments.map((alignment, index) => {
      const range = { start: alignment.window_start, end: alignment.window_end };
      const transcriptBucket = findBestTranscriptBucket(range, transcriptBuckets, alignment, transcriptOffsetSeconds);
      return {
        id: replayPointID("alignment", range, index),
        source: "alignment",
        start: range.start,
        end: range.end,
        alignment,
        chatBucket: findBestChatBucket(range, chatBuckets, alignment),
        transcriptBucket,
        transcriptContext: transcriptContextForBucket(transcriptBucket, transcriptBuckets),
        transcriptOffsetSeconds,
        events: eventsForRange(range, signalEvents)
      };
    });
    return sortReplayPoints([...alignmentReplayWindows, ...reactionWindows]);
  }

  if (chatBuckets.length > 0) {
    const chatReplayWindows: ReplayWindowPoint[] = chatBuckets.map((bucket, index) => {
      const range = { start: bucket.bucket_start, end: bucket.bucket_end };
      const alignment = findBestAlignment(range, alignments);
      const transcriptBucket = findBestTranscriptBucket(range, transcriptBuckets, alignment, transcriptOffsetSeconds);
      return {
        id: replayPointID("chat", range, index),
        source: "chat",
        start: range.start,
        end: range.end,
        alignment,
        chatBucket: bucket,
        transcriptBucket,
        transcriptContext: transcriptContextForBucket(transcriptBucket, transcriptBuckets),
        transcriptOffsetSeconds,
        events: eventsForRange(range, signalEvents)
      };
    });
    return sortReplayPoints([...chatReplayWindows, ...reactionWindows]);
  }

  return sortReplayPoints(transcriptBuckets.map((bucket, index) => {
    const range = { start: bucket.bucket_start, end: bucket.bucket_end };
    return {
      id: replayPointID("transcript", range, index),
      source: "transcript",
      start: range.start,
      end: range.end,
      alignment: findBestAlignment(range, alignments),
      transcriptBucket: bucket,
      transcriptContext: transcriptContextForBucket(bucket, transcriptBuckets),
      transcriptOffsetSeconds,
      events: eventsForRange(range, signalEvents)
    };
  }));
}

export function findBestChatBucket(
  window: WindowRange,
  buckets: ChatBucket[],
  alignment?: AlignmentBucket
): ChatBucket | undefined {
  if (alignment?.chat_bucket_start) {
    const exact = buckets.find((bucket) => bucket.bucket_start === alignment.chat_bucket_start);
    if (exact) return exact;
  }
  return findBestOverlappingBucket(window, buckets, (bucket) => ({
    start: bucket.bucket_start,
    end: bucket.bucket_end
  }));
}

export function findBestTranscriptBucket(
  window: WindowRange,
  buckets: TranscriptBucket[],
  alignment?: AlignmentBucket,
  transcriptOffsetSeconds = 0
): TranscriptBucket | undefined {
  const offsetSeconds = clampTranscriptOffset(transcriptOffsetSeconds);
  if (offsetSeconds === 0 && alignment?.transcript_bucket_start) {
    const exact = buckets.find((bucket) => bucket.bucket_start === alignment.transcript_bucket_start);
    if (exact) return exact;
  }
  return findBestOverlappingBucket(offsetWindow(window, offsetSeconds), buckets, (bucket) => ({
    start: bucket.bucket_start,
    end: bucket.bucket_end
  }));
}

export function findBestAlignment(window: WindowRange, alignments: AlignmentBucket[]): AlignmentBucket | undefined {
  return findBestOverlappingBucket(window, alignments, (alignment) => ({
    start: alignment.window_start,
    end: alignment.window_end
  }));
}

export function replayWindowScore(point: ReplayWindowPoint): number | undefined {
  if (point.reactionWindow) {
    return point.reactionWindow.reaction_score
      ?? reactionScoreFromComponents(point.reactionWindow)
      ?? point.chatBucket?.peak_reaction_score;
  }
  if (point.transcriptOffsetSeconds !== 0) {
    return averageNumbers(point.chatBucket?.chat_sentiment, point.transcriptBucket?.sentiment_score)
      ?? point.chatBucket?.chat_sentiment
      ?? point.transcriptBucket?.sentiment_score;
  }
  const signal = point.signalWindow;
  return signal?.aggregate_sentiment
    ?? averageNumbers(signal?.chat_sentiment, signal?.transcript_sentiment)
    ?? averageNumbers(point.alignment?.chat_sentiment, point.alignment?.transcript_sentiment)
    ?? point.chatBucket?.chat_sentiment
    ?? point.transcriptBucket?.sentiment_score;
}

export function replayWindowDelta(point: ReplayWindowPoint): number | undefined {
  if (point.transcriptOffsetSeconds !== 0) {
    return deltaBetween(point.chatBucket?.chat_sentiment, point.transcriptBucket?.sentiment_score);
  }
  return point.signalWindow?.alignment_delta
    ?? point.signalWindow?.delta
    ?? point.alignment?.delta
    ?? deltaBetween(point.chatBucket?.chat_sentiment, point.transcriptBucket?.sentiment_score);
}

export function replayWindowQuality(point: ReplayWindowPoint): number | undefined {
  if (point.reactionWindow) return point.reactionWindow.confidence;
  if (point.transcriptOffsetSeconds !== 0) {
    return averageNumbers(point.chatBucket?.sentiment_confidence, point.transcriptBucket?.sentiment_confidence);
  }
  return point.signalWindow?.confidence
    ?? point.signalWindow?.quality
    ?? point.signalWindow?.alignment_quality
    ?? point.alignment?.quality
    ?? averageNumbers(point.chatBucket?.sentiment_confidence, point.transcriptBucket?.sentiment_confidence);
}

export function replayWindowRelationship(point: ReplayWindowPoint): string | undefined {
  if (point.transcriptOffsetSeconds !== 0) {
    const delta = replayWindowDelta(point);
    if (typeof delta !== "number") return undefined;
    if (Math.abs(delta) >= 0.45) return "diverged";
    if (Math.abs(delta) >= 0.2) return "soft_split";
    return "converged";
  }
  return point.signalWindow?.relationship ?? point.alignment?.relationship;
}

export function replayWindowMessageCount(point: ReplayWindowPoint): number {
  if (typeof point.reactionWindow?.message_count === "number") return point.reactionWindow.message_count;
  return point.signalWindow?.message_count
    ?? point.signalWindow?.chat_message_count
    ?? point.alignment?.chat_message_count
    ?? point.chatBucket?.message_count
    ?? 0;
}

export function replayWindowChatSentiment(point: ReplayWindowPoint): number | undefined {
  return point.signalWindow?.chat_sentiment ?? point.alignment?.chat_sentiment ?? point.chatBucket?.chat_sentiment;
}

export function replayWindowTranscriptSentiment(point: ReplayWindowPoint): number | undefined {
  if (point.transcriptOffsetSeconds !== 0) return point.transcriptBucket?.sentiment_score;
  return point.signalWindow?.transcript_sentiment
    ?? point.alignment?.transcript_sentiment
    ?? point.transcriptBucket?.sentiment_score;
}

export function clampTranscriptOffset(value?: number): number {
  if (typeof value !== "number" || !Number.isFinite(value)) return 0;
  return Math.max(-120, Math.min(120, Math.round(value)));
}

export function transcriptContextForBucket(
  bucket: TranscriptBucket | undefined,
  buckets: TranscriptBucket[]
): TranscriptContextBucket[] {
  if (!bucket) return [];
  const sorted = buckets.slice().sort((first, second) => timestampValue(first.bucket_start) - timestampValue(second.bucket_start));
  const index = sorted.findIndex((candidate) => candidate === bucket || sameBucketRange(candidate, bucket));
  if (index < 0) return [{ role: "current", bucket }];

  return [
    sorted[index - 1] ? { role: "previous", bucket: sorted[index - 1] } : undefined,
    { role: "current", bucket: sorted[index] },
    sorted[index + 1] ? { role: "next", bucket: sorted[index + 1] } : undefined
  ].filter((context): context is TranscriptContextBucket => Boolean(context));
}

export function resolveReplayEvidenceRefs(point: ReplayWindowPoint, ids: string[] = []): ReplayEvidenceRef[] {
  return ids
    .filter(Boolean)
    .map((id) => resolveReplayEvidenceRef(point, id));
}

function resolveReplayEvidenceRef(point: ReplayWindowPoint, id: string): ReplayEvidenceRef {
  const transcriptBucket = transcriptBucketForEvidenceID(point, id);
  if (transcriptBucket) {
    return {
      id,
      source: "transcript",
      label: `transcript ${formatEvidenceTime(transcriptBucket.bucket_start)}`,
      text: transcriptBucketText(transcriptBucket)
    };
  }
  return {
    id,
    source: id.startsWith("transcript:") ? "unknown" : "chat",
    label: id
  };
}

function transcriptBucketForEvidenceID(point: ReplayWindowPoint, id: string): TranscriptBucket | undefined {
  const timestamp = transcriptEvidenceTimestamp(id);
  if (!timestamp) return undefined;
  const buckets = [
    point.transcriptBucket,
    ...point.transcriptContext.map((context) => context.bucket)
  ].filter((bucket): bucket is TranscriptBucket => Boolean(bucket));
  return buckets.find((bucket) => timestampValue(bucket.bucket_start) === timestamp);
}

function transcriptEvidenceTimestamp(id: string) {
  if (!id.startsWith("transcript:")) return 0;
  return timestampValue(id.slice("transcript:".length));
}

function transcriptBucketText(bucket: TranscriptBucket) {
  if (bucket.text?.trim()) return bucket.text.trim();
  return (bucket.segments || [])
    .map((segment) => segment.text)
    .filter(Boolean)
    .join(" ")
    .trim();
}

function formatEvidenceTime(value?: string) {
  if (!value) return "--:--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--:--";
  return date.toLocaleTimeString([], { minute: "2-digit", second: "2-digit" });
}

function buildReactionReplayWindows(
  chatBuckets: ChatBucket[],
  transcriptBuckets: TranscriptBucket[],
  alignments: AlignmentBucket[],
  signalEvents: SignalEvent[],
  transcriptOffsetSeconds: number
): ReplayWindowPoint[] {
  let index = 0;
  return chatBuckets.flatMap((bucket) => (bucket.subwindows || []).map((subwindow) => {
    const range = reactionWindowRange(subwindow, bucket);
    const alignment = findBestAlignment(range, alignments);
    const transcriptBucket = findBestTranscriptBucket(range, transcriptBuckets, alignment, transcriptOffsetSeconds);
    const point: ReplayWindowPoint = {
      id: replayPointID("reaction", range, index),
      source: "reaction",
      start: range.start,
      end: range.end,
      reactionWindow: subwindow,
      alignment,
      chatBucket: bucket,
      transcriptBucket,
      transcriptContext: transcriptContextForBucket(transcriptBucket, transcriptBuckets),
      transcriptOffsetSeconds,
      events: eventsForRange(range, signalEvents)
    };
    index += 1;
    return point;
  }));
}

function reactionWindowRange(subwindow: ChatBucketSubwindow, bucket: ChatBucket): WindowRange {
  return {
    start: subwindow.window_start || bucket.bucket_start,
    end: subwindow.window_end || bucket.bucket_end
  };
}

function findBestOverlappingBucket<T>(
  window: WindowRange,
  buckets: T[],
  rangeForBucket: (bucket: T) => WindowRange
): T | undefined {
  const windowStart = timestampValue(window.start);
  const windowEnd = timestampValue(window.end);
  if (!windowStart && !windowEnd) return undefined;

  let bestBucket: T | undefined;
  let bestScore = Number.NEGATIVE_INFINITY;
  buckets.forEach((bucket) => {
    const range = rangeForBucket(bucket);
    const bucketStart = timestampValue(range.start);
    const bucketEnd = timestampValue(range.end);
    const overlap = overlapMs(windowStart, windowEnd, bucketStart, bucketEnd);
    const distance = midpointDistance(windowStart, windowEnd, bucketStart, bucketEnd);
    const score = overlap > 0 ? overlap : -distance;
    if (score > bestScore) {
      bestScore = score;
      bestBucket = bucket;
    }
  });
  return bestBucket;
}

function eventsForRange(window: WindowRange, events: SignalEvent[]): SignalEvent[] {
  const seen = new Set<string>();
  return events.filter((event, index) => {
    const key = `${event.timestamp}-${event.source}-${event.label}-${event.text}-${index}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return isTimestampInRange(event.timestamp, window);
  });
}

function isTimestampInRange(timestamp: string | undefined, window: WindowRange) {
  if (!timestamp) return true;
  const value = timestampValue(timestamp);
  const start = timestampValue(window.start);
  const end = timestampValue(window.end);
  if (!value || (!start && !end)) return true;
  if (start && value < start) return false;
  if (end && value > end) return false;
  return true;
}

function sortReplayPoints(points: ReplayWindowPoint[]) {
  return points.sort((first, second) => timestampValue(first.start) - timestampValue(second.start));
}

function offsetWindow(window: WindowRange, offsetSeconds: number): WindowRange {
  return {
    start: offsetTimestamp(window.start, offsetSeconds),
    end: offsetTimestamp(window.end, offsetSeconds)
  };
}

function offsetTimestamp(value: string | undefined, offsetSeconds: number) {
  if (!value || offsetSeconds === 0) return value;
  const time = timestampValue(value);
  if (!time) return value;
  return new Date(time + offsetSeconds * 1000).toISOString();
}

function sameBucketRange(first: TranscriptBucket, second: TranscriptBucket) {
  return first.bucket_start === second.bucket_start && first.bucket_end === second.bucket_end;
}

function replayPointID(source: ReplaySource, range: WindowRange, index: number) {
  return `${source}:${range.start || "open"}:${range.end || "open"}:${index}`;
}

function timestampValue(value?: string) {
  if (!value) return 0;
  const time = new Date(value).getTime();
  return Number.isFinite(time) ? time : 0;
}

function overlapMs(firstStart: number, firstEnd: number, secondStart: number, secondEnd: number) {
  if (!firstStart || !firstEnd || !secondStart || !secondEnd) return 0;
  return Math.max(0, Math.min(firstEnd, secondEnd) - Math.max(firstStart, secondStart));
}

function midpointDistance(firstStart: number, firstEnd: number, secondStart: number, secondEnd: number) {
  const firstMid = midpoint(firstStart, firstEnd);
  const secondMid = midpoint(secondStart, secondEnd);
  return Math.abs(firstMid - secondMid);
}

function midpoint(start: number, end: number) {
  if (start && end) return start + (end - start) / 2;
  return start || end || 0;
}

function averageNumbers(...values: Array<number | undefined>) {
  const clean = values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  if (clean.length === 0) return undefined;
  return clean.reduce((sum, value) => sum + value, 0) / clean.length;
}

function reactionScoreFromComponents(window: ChatBucketSubwindow) {
  return maxNumber(window.hype_score, window.intensity_score, window.confusion_score, window.frustration_score);
}

function maxNumber(...values: Array<number | undefined>) {
  const clean = values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  return clean.length > 0 ? Math.max(...clean) : undefined;
}

function deltaBetween(first?: number, second?: number) {
  if (typeof first !== "number" || typeof second !== "number") return undefined;
  return first - second;
}
