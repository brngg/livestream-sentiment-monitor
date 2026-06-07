package main

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

func (s *server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.adminAuthRequired() {
			next(w, r)
			return
		}
		if !s.validAdminBearer(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="dashboard-admin"`)
			http.Error(w, "admin token required", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *server) adminAuthRequired() bool {
	if strings.TrimSpace(s.cfg.AdminToken) != "" {
		return true
	}
	return s.cfg.PublicStartConfigured && !s.cfg.PublicStartEnabled
}

func (s *server) validAdminBearer(r *http.Request) bool {
	expected := strings.TrimSpace(s.cfg.AdminToken)
	if expected == "" {
		return false
	}
	const prefix = "bearer "
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}
	actual := strings.TrimSpace(header[len(prefix):])
	return subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

func (s *server) activeSessionLimitReached(channel string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeSessionLimitReachedLocked(channel)
}

func (s *server) activeSessionLimitReachedLocked(channel string) bool {
	if s.cancelActive == nil || !isActiveSessionStatus(s.state.Status) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(s.state.Channel), strings.TrimSpace(channel)) {
		return false
	}
	return 1 >= s.maxActiveSessions()
}

func (s *server) maxActiveSessions() int {
	if s.cfg.MaxActiveSessions <= 0 {
		return 1
	}
	return s.cfg.MaxActiveSessions
}

func (s *server) newSessionContext() (context.Context, context.CancelFunc) {
	if s.cfg.SessionMaxDuration <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), s.cfg.SessionMaxDuration)
}

func (s *server) validateLiveStartSource(source streamSource) error {
	allowlist := normalizedLiveSourceAllowlist(s.cfg.LiveSourceAllowlist)
	if len(allowlist) == 0 {
		return nil
	}
	for _, candidate := range liveSourceAllowlistCandidates(source) {
		if allowlist[candidate] {
			return nil
		}
	}
	return fmt.Errorf("live source is not allowlisted")
}

func normalizedLiveSourceAllowlist(raw string) map[string]bool {
	out := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		value := normalizeLiveSourceAllowlistValue(item)
		if value != "" {
			out[value] = true
		}
	}
	return out
}

func liveSourceAllowlistCandidates(source streamSource) []string {
	return []string{
		normalizeLiveSourceAllowlistValue(source.ID),
		normalizeLiveSourceAllowlistValue(source.StreamID),
		normalizeLiveSourceAllowlistValue(source.Platform + ":" + source.ID),
		normalizeLiveSourceAllowlistValue(source.URL),
	}
}

func normalizeLiveSourceAllowlistValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimRight(value, "/")
	return value
}

func (s *server) reserveDailyLiveStart(now time.Time) error {
	limit := s.cfg.DailyLiveStartLimit
	if limit <= 0 {
		return nil
	}
	day := now.UTC().Format("2006-01-02")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dailyLiveStartsDate != day {
		s.dailyLiveStartsDate = day
		s.dailyLiveStarts = 0
	}
	if s.dailyLiveStarts >= limit {
		return fmt.Errorf("daily live start limit reached")
	}
	s.dailyLiveStarts++
	return nil
}

func (s *server) activeSessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelActive != nil && isActiveSessionStatus(s.state.Status) {
		return 1
	}
	return 0
}

func (s *server) liveStartCounterSnapshot() (string, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dailyLiveStartsDate, s.dailyLiveStarts, s.cfg.DailyLiveStartLimit
}

func (s *server) stopTranscriptService(ctx context.Context, runID, sessionID string) {
	if strings.TrimSpace(s.cfg.TranscriptURL) == "" || !s.isCurrentRun(runID, sessionID) {
		return
	}
	s.stopTranscriptServiceSession(ctx, sessionID)
}

func (s *server) stopTranscriptServiceSession(ctx context.Context, sessionID string) {
	if strings.TrimSpace(s.cfg.TranscriptURL) == "" {
		return
	}
	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(stopCtx, http.MethodPost, s.transcriptTarget("/stop", ""), nil)
	if err != nil {
		return
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("stop transcript service", "session_id", sessionID, "error", err)
		}
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if s.logger != nil {
			s.logger.Warn("stop transcript service returned non-success", "session_id", sessionID, "status", response.Status)
		}
	}
}

func envPresent(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}
