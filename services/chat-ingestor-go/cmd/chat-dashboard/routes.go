package main

import "net/http"

func (s *server) registerRoutes(mux *http.ServeMux, frontendDir string) {
	mux.HandleFunc("POST /sessions", s.requireAdmin(s.handleStartSession))
	mux.HandleFunc("GET /sessions/history", s.handleSessionHistory)
	mux.HandleFunc("GET /sessions/{session_id}/summary", s.handleSessionSummary)
	mux.HandleFunc("GET /sessions/{session_id}/replay", s.handleSessionReplay)
	mux.HandleFunc("GET /sessions/{session_id}/evaluation", s.handleSessionEvaluation)
	mux.HandleFunc("GET /sessions/{session_id}/proof", s.handleSessionProof)
	mux.HandleFunc("POST /sessions/{session_id}/proof", s.requireAdmin(s.handleSessionProofPersist))
	mux.HandleFunc("POST /labels", s.requireAdmin(s.handleLabel))
	mux.HandleFunc("POST /signal-window-labels", s.requireAdmin(s.handleSignalWindowLabel))
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.HandleFunc("GET /state", s.handleState)
	mux.HandleFunc("GET /transcript/state", s.handleTranscriptState)
	mux.HandleFunc("GET /transcript/live", s.handleTranscriptLive)
	mux.HandleFunc("GET /transcript/buckets", s.handleTranscriptBuckets)
	mux.HandleFunc("GET /transcript/health", s.handleTranscriptHealth)
	mux.HandleFunc("GET /transcript/events", s.handleTranscriptEvents)
	mux.HandleFunc("POST /transcript/sessions", s.requireAdmin(s.handleTranscriptStartProxy))
	mux.HandleFunc("POST /transcript/stop", s.requireAdmin(s.handleTranscriptStopProxy))
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.Handle("/", staticHandler(frontendDir))
}
