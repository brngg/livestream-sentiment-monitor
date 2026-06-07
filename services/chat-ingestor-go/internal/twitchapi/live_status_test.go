package twitchapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetStreamStatusLive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/streams" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("user_login"); got != "ishowspeed" {
			t.Fatalf("user_login = %q", got)
		}
		if got := r.URL.Query().Get("type"); got != "live" {
			t.Fatalf("type = %q", got)
		}
		if got := r.Header.Get("Client-Id"); got != "client-1" {
			t.Fatalf("client id header = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
			t.Fatalf("authorization header = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"stream-1","user_id":"user-1","user_login":"ishowspeed","user_name":"IShowSpeed","game_id":"509658","game_name":"Just Chatting","type":"live","title":"live now","viewer_count":12345,"started_at":"2026-04-29T18:00:00Z","language":"en"}]}`))
	}))
	defer server.Close()

	status, err := Client{
		ClientID:       "client-1",
		AppAccessToken: "token-1",
		HelixBaseURL:   server.URL,
	}.GetStreamStatus(context.Background(), "IShowSpeed")
	if err != nil {
		t.Fatal(err)
	}
	if !status.Live {
		t.Fatal("expected live stream")
	}
	if status.UserLogin != "ishowspeed" {
		t.Fatalf("user login = %q", status.UserLogin)
	}
	if status.ViewerCount != 12345 {
		t.Fatalf("viewer count = %d", status.ViewerCount)
	}
}

func TestGetStreamStatusOffline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()

	status, err := Client{
		ClientID:       "client-1",
		AppAccessToken: "token-1",
		HelixBaseURL:   server.URL,
	}.GetStreamStatus(context.Background(), "offline_channel")
	if err != nil {
		t.Fatal(err)
	}
	if status.Live {
		t.Fatal("expected offline status")
	}
	if status.UserLogin != "offline_channel" {
		t.Fatalf("user login = %q", status.UserLogin)
	}
}
