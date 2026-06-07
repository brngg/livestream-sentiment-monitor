package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProxyTranscriptPreservesLiveResponseShape(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/live" {
			t.Fatalf("path = %q, want /live", r.URL.Path)
		}
		if r.URL.RawQuery != "mode=raw" {
			t.Fatalf("query = %q, want mode=raw", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"mode":"live","session_id":"abc","segments":[{"text":"hello"}]}`))
	}))
	defer upstream.Close()

	s := &server{cfg: appConfig{TranscriptURL: upstream.URL, TranscriptChunkSeconds: 1}}
	request := httptest.NewRequest(http.MethodGet, "/transcript/live?mode=raw", nil)
	response := httptest.NewRecorder()

	s.handleTranscriptLive(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Body.String(); got != `{"mode":"live","session_id":"abc","segments":[{"text":"hello"}]}` {
		t.Fatalf("body = %s", got)
	}
}

func TestParseStreamSourceAcceptsYouTubeLiveURL(t *testing.T) {
	source, err := parseStreamSource("youtube.com/watch?v=abc123XYZ")
	if err != nil {
		t.Fatalf("parseStreamSource returned error: %v", err)
	}
	if source.Platform != "youtube" || source.ID != "youtube-abc123xyz" || source.StreamID != "abc123xyz" {
		t.Fatalf("source = %#v", source)
	}
	if source.ChatEnabled {
		t.Fatal("YouTube source should be transcript-only by default")
	}
}

func TestStartTranscriptSessionForwardsDashboardSession(t *testing.T) {
	var payload map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sessions" {
			t.Fatalf("path = %q, want /sessions", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"running","session_id":"speed-1","channel_id":"speed"}`))
	}))
	defer upstream.Close()

	s := &server{cfg: appConfig{TranscriptURL: upstream.URL}}
	if err := s.startTranscriptSession(context.Background(), testStreamSource("speed"), "speed-1"); err != nil {
		t.Fatalf("startTranscriptSession returned error: %v", err)
	}

	if payload["channel"] != "speed" {
		t.Fatalf("channel = %v, want speed", payload["channel"])
	}
	if payload["channel_id"] != "speed" {
		t.Fatalf("channel_id = %v, want speed", payload["channel_id"])
	}
	if payload["source_url"] != "https://www.twitch.tv/speed" {
		t.Fatalf("source_url = %v, want Twitch URL", payload["source_url"])
	}
	if payload["session_id"] != "speed-1" {
		t.Fatalf("session_id = %v, want speed-1", payload["session_id"])
	}
	if payload["bucket_seconds"] != float64(defaultTranscriptBucketSeconds) {
		t.Fatalf("bucket_seconds = %v, want %d", payload["bucket_seconds"], defaultTranscriptBucketSeconds)
	}
	if payload["chunk_seconds"] != float64(defaultTranscriptChunkSeconds) {
		t.Fatalf("chunk_seconds = %v, want %d", payload["chunk_seconds"], defaultTranscriptChunkSeconds)
	}
}

func TestStartTranscriptSessionUsesConfiguredBucketSeconds(t *testing.T) {
	var payload map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"running","session_id":"speed-1","channel_id":"speed"}`))
	}))
	defer upstream.Close()

	s := &server{cfg: appConfig{TranscriptURL: upstream.URL, TranscriptBucketSeconds: 10, TranscriptChunkSeconds: 1}}
	if err := s.startTranscriptSession(context.Background(), testStreamSource("speed"), "speed-1"); err != nil {
		t.Fatalf("startTranscriptSession returned error: %v", err)
	}

	if payload["bucket_seconds"] != float64(10) {
		t.Fatalf("bucket_seconds = %v, want 10", payload["bucket_seconds"])
	}
	if payload["chunk_seconds"] != float64(1) {
		t.Fatalf("chunk_seconds = %v, want 1", payload["chunk_seconds"])
	}
}

func testStreamSource(channel string) streamSource {
	return streamSource{
		Platform:    "twitch",
		ID:          channel,
		URL:         "https://www.twitch.tv/" + channel,
		Label:       channel,
		StreamID:    channel,
		ChatEnabled: true,
	}
}

func TestScanTranscriptSSEFiltersBySession(t *testing.T) {
	body := strings.NewReader(
		": connected\n\n" +
			`data: {"type":"transcript_partial","session_id":"other","text":"skip"}` + "\n\n" +
			`data: {"type":"transcript_segment","session_id":"session-1","text":"keep"}` + "\n\n",
	)
	events := make(chan map[string]any, 2)
	errCh := make(chan error, 1)

	scanTranscriptSSE(context.Background(), body, "session-1", events, errCh)

	event, ok := <-events
	if !ok {
		t.Fatal("expected filtered event")
	}
	if event["type"] != "transcript_segment" || event["text"] != "keep" {
		t.Fatalf("event = %#v", event)
	}
	if _, ok := <-events; ok {
		t.Fatal("expected event channel to close")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("scan error: %v", err)
	}
}
