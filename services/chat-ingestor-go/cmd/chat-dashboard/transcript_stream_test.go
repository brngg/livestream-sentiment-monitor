package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

type transcriptRecordingStore struct {
	storage.NoopStore

	mu      sync.Mutex
	buckets []storage.TranscriptBucket
}

func (s *transcriptRecordingStore) SaveTranscriptBucket(_ context.Context, bucket storage.TranscriptBucket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets = append(s.buckets, bucket)
	return nil
}

func (s *transcriptRecordingStore) transcriptBuckets() []storage.TranscriptBucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]storage.TranscriptBucket, len(s.buckets))
	copy(out, s.buckets)
	return out
}

func TestStreamTranscriptEventsAddsTranscriptBucketWithWords(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			t.Fatalf("path = %q, want /events", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"transcript_bucket","session_id":"other","channel_id":"channel","bucket_start":"2026-05-01T12:00:00Z","bucket_end":"2026-05-01T12:00:30Z","text":"skip","language":"en","transcript_confidence":0.9}` + "\n\n"))
		_, _ = w.Write([]byte(`data: {"type":"transcript_bucket","session_id":"session","channel_id":"channel","bucket_start":"2026-05-01T12:00:00Z","bucket_end":"2026-05-01T12:00:30Z","text":"hello world","language":"en","transcript_confidence":0.93,"segments":[{"start":0,"end":1.2,"text":"hello world","confidence":0.94,"words":[{"start":0,"end":0.5,"text":"hello","probability":0.91},{"start":0.5,"end":1.2,"text":"world","probability":0.92}]}]}` + "\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer upstream.Close()

	store := &transcriptRecordingStore{}
	server := testTranscriptServer(upstream.URL, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.streamTranscriptEvents(ctx, "session", "channel")

	bucket := waitForTranscriptBucket(t, server)
	cancel()

	if bucket.Text != "hello world" {
		t.Fatalf("text = %q, want hello world", bucket.Text)
	}
	if len(bucket.Segments) != 1 || len(bucket.Segments[0].Words) != 2 {
		t.Fatalf("word timestamps not kept in dashboard state: %#v", bucket.Segments)
	}
	if bucket.Segments[0].Words[0].Text != "hello" || bucket.Segments[0].Words[0].Start != 0 || bucket.Segments[0].Words[0].End != 0.5 {
		t.Fatalf("unexpected first word: %#v", bucket.Segments[0].Words[0])
	}

	stored := waitForStoredTranscriptBucket(t, store)
	if len(stored.Segments) != 1 || len(stored.Segments[0].Words) != 2 {
		t.Fatalf("word timestamps not kept in storage record: %#v", stored.Segments)
	}
	if stored.Segments[0].Words[1].Text != "world" || stored.Segments[0].Words[1].Probability == nil || *stored.Segments[0].Words[1].Probability != 0.92 {
		t.Fatalf("unexpected stored second word: %#v", stored.Segments[0].Words[1])
	}
}

func TestPollTranscriptBucketsRemainsFallback(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/buckets" {
			t.Fatalf("path = %q, want /buckets", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"session_id":"session","channel_id":"channel","latest_bucket":{"type":"transcript_bucket","session_id":"session","channel_id":"channel","bucket_start":"2026-05-01T12:00:30Z","bucket_end":"2026-05-01T12:01:00Z","text":"fallback bucket","language":"en","transcript_confidence":0.88}}`))
	}))
	defer upstream.Close()

	store := &transcriptRecordingStore{}
	server := testTranscriptServer(upstream.URL, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.pollTranscriptBuckets(ctx, "session", testStreamSource("channel"))

	bucket := waitForTranscriptBucket(t, server)
	cancel()

	if bucket.Text != "fallback bucket" {
		t.Fatalf("text = %q, want fallback bucket", bucket.Text)
	}
}

func TestPollTranscriptBucketsRecoversIdleTranscriptSession(t *testing.T) {
	started := make(chan map[string]any, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/buckets", "/state":
			_, _ = w.Write([]byte(`{"status":"idle"}`))
		case "/sessions":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			select {
			case started <- payload:
			default:
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"status":"starting","session_id":"session","channel_id":"channel"}`))
		default:
			t.Fatalf("path = %q, want /buckets, /state, or /sessions", r.URL.Path)
		}
	}))
	defer upstream.Close()

	server := testTranscriptServer(upstream.URL, &transcriptRecordingStore{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.pollTranscriptBuckets(ctx, "session", testStreamSource("channel"))

	select {
	case payload := <-started:
		cancel()
		if payload["channel"] != "channel" || payload["session_id"] != "session" {
			t.Fatalf("payload = %#v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for transcript recovery start")
	}
}

func testTranscriptServer(transcriptURL string, store storage.Store) *server {
	return &server{
		cfg: appConfig{
			TranscriptURL:        transcriptURL,
			TranscriptPoll:       5 * time.Millisecond,
			DatabaseWriteTimeout: time.Second,
			BucketEvery:          30 * time.Second,
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:  store,
		hub:    newEventHub(),
		state: dashboardState{
			Session: "session",
			Channel: "channel",
		},
		humanLabels: map[string]string{},
	}
}

func waitForTranscriptBucket(t *testing.T, server *server) transcriptBucket {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		server.mu.Lock()
		if len(server.state.Transcripts) > 0 {
			bucket := server.state.Transcripts[0]
			server.mu.Unlock()
			return bucket
		}
		server.mu.Unlock()

		select {
		case <-deadline:
			t.Fatal("timed out waiting for transcript bucket")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func waitForStoredTranscriptBucket(t *testing.T, store *transcriptRecordingStore) storage.TranscriptBucket {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		buckets := store.transcriptBuckets()
		if len(buckets) > 0 {
			return buckets[0]
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for stored transcript bucket")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
