package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStaticHandlerOnlyFallsBackForFrontendRoutes(t *testing.T) {
	frontendDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(frontendDir, "index.html"), []byte("SPA_INDEX"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(frontendDir, "assets"), 0o755); err != nil {
		t.Fatalf("mkdir assets: %v", err)
	}
	if err := os.WriteFile(filepath.Join(frontendDir, "assets", "app.js"), []byte("console.log('ok')"), 0o644); err != nil {
		t.Fatalf("write asset: %v", err)
	}

	handler := staticHandler(frontendDir)
	tests := []struct {
		name          string
		path          string
		wantStatus    int
		wantBody      string
		mustNotReturn string
	}{
		{name: "root route", path: "/", wantStatus: http.StatusOK, wantBody: "SPA_INDEX"},
		{name: "eval route", path: "/eval", wantStatus: http.StatusOK, wantBody: "SPA_INDEX"},
		{name: "asset", path: "/assets/app.js", wantStatus: http.StatusOK, wantBody: "console.log('ok')"},
		{name: "unknown session API", path: "/sessions/foo", wantStatus: http.StatusNotFound, mustNotReturn: "SPA_INDEX"},
		{name: "unknown transcript API", path: "/transcript/typo", wantStatus: http.StatusNotFound, mustNotReturn: "SPA_INDEX"},
		{name: "missing asset", path: "/assets/missing.js", wantStatus: http.StatusNotFound, mustNotReturn: "SPA_INDEX"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if test.wantBody != "" && !strings.Contains(response.Body.String(), test.wantBody) {
				t.Fatalf("body = %q, want to contain %q", response.Body.String(), test.wantBody)
			}
			if test.mustNotReturn != "" && strings.Contains(response.Body.String(), test.mustNotReturn) {
				t.Fatalf("body = %q, should not contain %q", response.Body.String(), test.mustNotReturn)
			}
		})
	}
}
