package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/storage"
)

func TestMutatingRoutesRequireAdminTokenWhenConfigured(t *testing.T) {
	s := &server{cfg: appConfig{AdminToken: "secret"}, store: storage.NewNoopStore(), humanLabels: map[string]string{}}
	mux := controlsTestMux(t, s)
	paths := []string{
		"/sessions",
		"/transcript/sessions",
		"/transcript/stop",
		"/labels",
		"/signal-window-labels",
		"/sessions/session-1/proof",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			response := httptest.NewRecorder()

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
			}
			if response.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("expected WWW-Authenticate header")
			}
		})
	}
}

func TestMutatingRouteAllowsValidAdminBearer(t *testing.T) {
	upstreamHit := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit = true
		if r.URL.Path != "/stop" {
			t.Fatalf("upstream path = %q, want /stop", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"stopping"}`))
	}))
	defer upstream.Close()

	s := &server{cfg: appConfig{AdminToken: "secret", TranscriptURL: upstream.URL}, store: storage.NewNoopStore(), humanLabels: map[string]string{}}
	mux := controlsTestMux(t, s)
	request := httptest.NewRequest(http.MethodPost, "/transcript/stop", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body.String())
	}
	if !upstreamHit {
		t.Fatal("expected authenticated request to reach transcript upstream")
	}
}

func TestEmptyAdminTokenPreservesLocalMutationUnlessPublicStartExplicitlyDisabled(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"stopping"}`))
	}))
	defer upstream.Close()

	local := &server{cfg: appConfig{TranscriptURL: upstream.URL}, store: storage.NewNoopStore(), humanLabels: map[string]string{}}
	localMux := controlsTestMux(t, local)
	localRequest := httptest.NewRequest(http.MethodPost, "/transcript/stop", nil)
	localResponse := httptest.NewRecorder()
	localMux.ServeHTTP(localResponse, localRequest)
	if localResponse.Code != http.StatusAccepted {
		t.Fatalf("local status = %d, want %d", localResponse.Code, http.StatusAccepted)
	}

	locked := &server{cfg: appConfig{TranscriptURL: upstream.URL, PublicStartConfigured: true, PublicStartEnabled: false}, store: storage.NewNoopStore(), humanLabels: map[string]string{}}
	lockedMux := controlsTestMux(t, locked)
	lockedRequest := httptest.NewRequest(http.MethodPost, "/transcript/stop", nil)
	lockedResponse := httptest.NewRecorder()
	lockedMux.ServeHTTP(lockedResponse, lockedRequest)
	if lockedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("locked status = %d, want %d", lockedResponse.Code, http.StatusUnauthorized)
	}
}

func TestPublicReadRoutesStayOpenWithAdminToken(t *testing.T) {
	s := &server{cfg: appConfig{AdminToken: "secret"}, store: storage.NewNoopStore(), humanLabels: map[string]string{}}
	mux := controlsTestMux(t, s)
	request := httptest.NewRequest(http.MethodGet, "/state", nil)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
}

func TestStartSessionReplacesActiveSessionWhenMaxActiveSessionsIsOne(t *testing.T) {
	canceled := false
	s := &server{
		cfg:    appConfig{AdminToken: "secret", MaxActiveSessions: 1},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		state: dashboardState{
			Status:  "ingesting",
			Session: "active-1",
			Channel: "active",
		},
		cancelActive: func() { canceled = true },
		store:        storage.NewNoopStore(),
		hub:          newEventHub(),
		humanLabels:  map[string]string{},
	}
	mux := controlsTestMux(t, s)
	request := httptest.NewRequest(http.MethodPost, "/sessions", strings.NewReader(`{"channel":"https://www.youtube.com/watch?v=abc123"}`))
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body.String())
	}
	if !canceled {
		t.Fatal("previous active session was not cancelled")
	}
	s.mu.Lock()
	replaced := s.state.Session != "active-1" && s.state.Channel == "youtube-abc123" && s.activeRunID != "" && s.cancelActive != nil
	session, channel, runID, cancelNil := s.state.Session, s.state.Channel, s.activeRunID, s.cancelActive == nil
	s.mu.Unlock()
	if !replaced {
		t.Fatalf("active state was not replaced: session=%q channel=%q run=%q cancel_nil=%t", session, channel, runID, cancelNil)
	}
	s.stopActiveSession()
}

func TestSessionContextUsesConfiguredMaxDuration(t *testing.T) {
	s := &server{cfg: appConfig{SessionMaxDuration: time.Nanosecond}}
	ctx, cancel := s.newSessionContext()
	defer cancel()

	<-ctx.Done()

	if got := sessionStopReason(ctx); got != "session max duration reached" {
		t.Fatalf("stop reason = %q, want session max duration reached", got)
	}
}

func TestDashboardMetricsOutput(t *testing.T) {
	s := &server{
		cfg: appConfig{MaxActiveSessions: 1, DailyLiveStartLimit: 3},
		state: dashboardState{
			Status:  "ingesting",
			Session: "active-1",
			Channel: "active",
		},
		cancelActive:        func() {},
		dailyLiveStartsDate: "2026-05-11",
		dailyLiveStarts:     2,
	}
	s.metrics.modelFallbacks.Add(2)
	mux := controlsTestMux(t, s)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, want := range []string{
		"# TYPE stream_dashboard_active_sessions gauge",
		"stream_dashboard_active_sessions 1",
		"stream_dashboard_daily_live_starts 2",
		"stream_dashboard_model_fallbacks_total 2",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func controlsTestMux(t *testing.T, s *server) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	s.registerRoutes(mux, t.TempDir())
	return mux
}
