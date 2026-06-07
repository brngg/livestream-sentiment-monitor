package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultTranscriptBucketSeconds = 30
	maxTranscriptBucketSeconds     = 120
	defaultTranscriptChunkSeconds  = 5
	transcriptProxyTimeout         = 10 * time.Second
)

func (s *server) handleTranscriptState(w http.ResponseWriter, r *http.Request) {
	s.proxyTranscript(w, r, "/state")
}

func (s *server) handleTranscriptLive(w http.ResponseWriter, r *http.Request) {
	s.proxyTranscript(w, r, "/live")
}

func (s *server) handleTranscriptBuckets(w http.ResponseWriter, r *http.Request) {
	s.proxyTranscript(w, r, "/buckets")
}

func (s *server) handleTranscriptHealth(w http.ResponseWriter, r *http.Request) {
	s.proxyTranscript(w, r, "/health")
}

func (s *server) handleTranscriptStartProxy(w http.ResponseWriter, r *http.Request) {
	s.proxyTranscript(w, r, "/sessions")
}

func (s *server) handleTranscriptStopProxy(w http.ResponseWriter, r *http.Request) {
	s.proxyTranscript(w, r, "/stop")
}

func (s *server) handleTranscriptEvents(w http.ResponseWriter, r *http.Request) {
	s.proxyTranscript(w, r, "/events")
}

func (s *server) proxyTranscript(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()
	if path != "/events" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, transcriptProxyTimeout)
		defer cancel()
	}

	target := s.transcriptTarget(path, r.URL.RawQuery)
	request, err := http.NewRequestWithContext(ctx, r.Method, target, r.Body)
	if err != nil {
		http.Error(w, "invalid transcript proxy request", http.StatusInternalServerError)
		return
	}
	if accept := r.Header.Get("Accept"); accept != "" {
		request.Header.Set("Accept", accept)
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		http.Error(w, fmt.Sprintf("transcript service unavailable: %v", err), http.StatusBadGateway)
		return
	}
	defer response.Body.Close()

	copyProxyHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	if path == "/events" {
		if flusher, ok := w.(http.Flusher); ok {
			_, _ = io.Copy(flushingWriter{writer: w, flusher: flusher}, response.Body)
			return
		}
	}
	_, _ = io.Copy(w, response.Body)
}

func scanTranscriptSSE(ctx context.Context, body io.Reader, sessionID string, out chan<- map[string]any, errCh chan<- error) {
	defer close(out)
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			errCh <- nil
			return
		default:
		}
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if raw == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			continue
		}
		if sessionID != "" && strings.TrimSpace(fmt.Sprint(event["session_id"])) != sessionID {
			continue
		}
		select {
		case <-ctx.Done():
			errCh <- nil
			return
		case out <- event:
		}
	}
	if err := scanner.Err(); err != nil {
		errCh <- err
		return
	}
	errCh <- nil
}

func (s *server) startTranscriptSession(ctx context.Context, source streamSource, sessionID string) error {
	payload, err := json.Marshal(map[string]any{
		"channel":        source.ID,
		"channel_id":     source.ID,
		"source_url":     source.URL,
		"session_id":     sessionID,
		"bucket_seconds": s.transcriptBucketSeconds(),
		"chunk_seconds":  s.transcriptChunkSeconds(),
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, transcriptProxyTimeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.transcriptTarget("/sessions", ""), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("transcript service unavailable: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = response.Status
		}
		return fmt.Errorf("transcript session start failed: %s", message)
	}
	return nil
}

func (s *server) ensureTranscriptSession(ctx context.Context, source streamSource, sessionID string) error {
	if strings.TrimSpace(s.cfg.TranscriptURL) == "" {
		return nil
	}
	channel := source.ID
	state, ok := s.fetchTranscriptServiceState(ctx)
	if ok && state.SessionID == sessionID && state.ChannelID == channel && isActiveTranscriptStatus(state.Status) {
		return nil
	}
	return s.startTranscriptSession(ctx, source, sessionID)
}

func (s *server) fetchTranscriptServiceState(ctx context.Context) (transcriptServiceState, bool) {
	requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, s.transcriptTarget("/state", ""), nil)
	if err != nil {
		return transcriptServiceState{}, false
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return transcriptServiceState{}, false
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return transcriptServiceState{}, false
	}
	var state transcriptServiceState
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		return transcriptServiceState{}, false
	}
	return state, true
}

func isActiveTranscriptStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "starting", "ingesting":
		return true
	default:
		return false
	}
}

func (s *server) transcriptChunkSeconds() int {
	if s.cfg.TranscriptChunkSeconds > 0 {
		return s.cfg.TranscriptChunkSeconds
	}
	return defaultTranscriptChunkSeconds
}

func (s *server) transcriptBucketSeconds() int {
	if s.cfg.TranscriptBucketSeconds > 0 {
		return s.cfg.TranscriptBucketSeconds
	}
	return defaultTranscriptBucketSeconds
}

func (s *server) transcriptTarget(path, rawQuery string) string {
	target := strings.TrimRight(s.cfg.TranscriptURL, "/") + "/" + strings.TrimLeft(path, "/")
	if rawQuery != "" {
		target += "?" + rawQuery
	}
	return target
}

func copyProxyHeaders(dst, src http.Header) {
	for key, values := range src {
		if strings.EqualFold(key, "Content-Length") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

type flushingWriter struct {
	writer  io.Writer
	flusher http.Flusher
}

func (w flushingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.flusher.Flush()
	return n, err
}
