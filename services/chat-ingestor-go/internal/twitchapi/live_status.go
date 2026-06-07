package twitchapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultHelixBaseURL = "https://api.twitch.tv/helix"
	DefaultOAuthURL     = "https://id.twitch.tv/oauth2/token"
)

type Client struct {
	ClientID       string
	ClientSecret   string
	AppAccessToken string
	HelixBaseURL   string
	OAuthURL       string
	HTTPClient     *http.Client
}

type StreamStatus struct {
	Live         bool
	ID           string
	UserID       string
	UserLogin    string
	UserName     string
	GameID       string
	GameName     string
	Title        string
	ViewerCount  int
	StartedAt    time.Time
	Language     string
	ThumbnailURL string
}

func (c Client) GetStreamStatus(ctx context.Context, login string) (StreamStatus, error) {
	login = strings.TrimSpace(strings.ToLower(login))
	login = strings.TrimPrefix(login, "#")
	if login == "" {
		return StreamStatus{}, fmt.Errorf("stream login is required")
	}

	token := strings.TrimSpace(c.AppAccessToken)
	if token == "" {
		generated, err := c.fetchAppAccessToken(ctx)
		if err != nil {
			return StreamStatus{}, err
		}
		token = generated
	}

	baseURL := c.HelixBaseURL
	if baseURL == "" {
		baseURL = DefaultHelixBaseURL
	}

	endpoint, err := url.Parse(strings.TrimRight(baseURL, "/") + "/streams")
	if err != nil {
		return StreamStatus{}, fmt.Errorf("parse helix url: %w", err)
	}
	query := endpoint.Query()
	query.Set("user_login", login)
	query.Set("type", "live")
	query.Set("first", "1")
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return StreamStatus{}, fmt.Errorf("create live status request: %w", err)
	}
	req.Header.Set("Client-Id", c.ClientID)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.client().Do(req)
	if err != nil {
		return StreamStatus{}, fmt.Errorf("get live status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return StreamStatus{}, fmt.Errorf("get live status: twitch returned %s", resp.Status)
	}

	var payload streamsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return StreamStatus{}, fmt.Errorf("decode live status: %w", err)
	}
	if len(payload.Data) == 0 {
		return StreamStatus{Live: false, UserLogin: login}, nil
	}

	stream := payload.Data[0]
	startedAt, _ := time.Parse(time.RFC3339, stream.StartedAt)
	return StreamStatus{
		Live:         strings.EqualFold(stream.Type, "live"),
		ID:           stream.ID,
		UserID:       stream.UserID,
		UserLogin:    stream.UserLogin,
		UserName:     stream.UserName,
		GameID:       stream.GameID,
		GameName:     stream.GameName,
		Title:        stream.Title,
		ViewerCount:  stream.ViewerCount,
		StartedAt:    startedAt,
		Language:     stream.Language,
		ThumbnailURL: stream.ThumbnailURL,
	}, nil
}

func (c Client) fetchAppAccessToken(ctx context.Context) (string, error) {
	if strings.TrimSpace(c.ClientID) == "" {
		return "", fmt.Errorf("TWITCH_CLIENT_ID or -twitch-client-id is required for live status checks")
	}
	if strings.TrimSpace(c.ClientSecret) == "" {
		return "", fmt.Errorf("TWITCH_APP_ACCESS_TOKEN or TWITCH_CLIENT_SECRET/-twitch-client-secret is required for live status checks")
	}

	oauthURL := c.OAuthURL
	if oauthURL == "" {
		oauthURL = DefaultOAuthURL
	}

	form := url.Values{}
	form.Set("client_id", c.ClientID)
	form.Set("client_secret", c.ClientSecret)
	form.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, oauthURL, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.client().Do(req)
	if err != nil {
		return "", fmt.Errorf("get app access token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("get app access token: twitch returned %s", resp.Status)
	}

	var payload tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode app access token: %w", err)
	}
	if payload.AccessToken == "" {
		return "", fmt.Errorf("get app access token: empty access token")
	}
	return payload.AccessToken, nil
}

func (c Client) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

type streamsResponse struct {
	Data []streamPayload `json:"data"`
}

type streamPayload struct {
	ID           string `json:"id"`
	UserID       string `json:"user_id"`
	UserLogin    string `json:"user_login"`
	UserName     string `json:"user_name"`
	GameID       string `json:"game_id"`
	GameName     string `json:"game_name"`
	Type         string `json:"type"`
	Title        string `json:"title"`
	ViewerCount  int    `json:"viewer_count"`
	StartedAt    string `json:"started_at"`
	Language     string `json:"language"`
	ThumbnailURL string `json:"thumbnail_url"`
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
}
