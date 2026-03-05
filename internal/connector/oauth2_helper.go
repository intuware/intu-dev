package connector

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type oauth2CachedToken struct {
	token     string
	expiresAt time.Time
}

var (
	oauth2Cache   = make(map[string]*oauth2CachedToken)
	oauth2CacheMu sync.RWMutex
)

func ClearOAuth2Cache() {
	oauth2CacheMu.Lock()
	oauth2Cache = make(map[string]*oauth2CachedToken)
	oauth2CacheMu.Unlock()
}

func fetchOAuth2Token(tokenURL, clientID, clientSecret string, scopes []string) (string, error) {
	if tokenURL == "" || clientID == "" || clientSecret == "" {
		return "", fmt.Errorf("oauth2: token_url, client_id, and client_secret are required")
	}

	cacheKey := tokenURL + "|" + clientID

	oauth2CacheMu.RLock()
	if cached, ok := oauth2Cache[cacheKey]; ok {
		if time.Now().Before(cached.expiresAt) {
			oauth2CacheMu.RUnlock()
			return cached.token, nil
		}
	}
	oauth2CacheMu.RUnlock()

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	if len(scopes) > 0 {
		data.Set("scope", strings.Join(scopes, " "))
	}

	resp, err := http.Post(tokenURL, "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("oauth2 token request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("oauth2 read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oauth2 token endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("oauth2 parse response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("oauth2: empty access_token in response")
	}

	expiresIn := tokenResp.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}
	// Cache with a 60-second safety margin
	expiresAt := time.Now().Add(time.Duration(expiresIn-60) * time.Second)

	oauth2CacheMu.Lock()
	oauth2Cache[cacheKey] = &oauth2CachedToken{
		token:     tokenResp.AccessToken,
		expiresAt: expiresAt,
	}
	oauth2CacheMu.Unlock()

	return tokenResp.AccessToken, nil
}
