package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/intuware/intu/pkg/config"
)

type Authenticator interface {
	Authenticate(r *http.Request) (bool, string, error)
}

func NewAuthenticator(cfg *config.AuthConfig) (Authenticator, error) {
	if cfg == nil {
		return &NoopAuth{}, nil
	}

	switch cfg.Type {
	case "", "none":
		return &NoopAuth{}, nil
	case "bearer":
		return &BearerAuth{Token: cfg.Token}, nil
	case "basic":
		return &BasicAuth{Username: cfg.Username, Password: cfg.Password}, nil
	case "api_key":
		return &APIKeyAuth{Key: cfg.Key, Header: cfg.Header, QueryParam: cfg.QueryParam}, nil
	case "mtls":
		return &MTLSAuth{}, nil
	case "custom":
		return &NoopAuth{}, nil
	default:
		return nil, fmt.Errorf("unsupported auth type: %s", cfg.Type)
	}
}

type NoopAuth struct{}

func (n *NoopAuth) Authenticate(r *http.Request) (bool, string, error) {
	return true, "", nil
}

type BearerAuth struct {
	Token string
}

func (b *BearerAuth) Authenticate(r *http.Request) (bool, string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return false, "", nil
	}
	token := strings.TrimPrefix(header, "Bearer ")
	if token == b.Token {
		return true, "bearer-user", nil
	}
	return false, "", nil
}

type BasicAuth struct {
	Username string
	Password string
}

func (b *BasicAuth) Authenticate(r *http.Request) (bool, string, error) {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false, "", nil
	}
	if user == b.Username && pass == b.Password {
		return true, user, nil
	}
	return false, "", nil
}

type APIKeyAuth struct {
	Key        string
	Header     string
	QueryParam string
}

func (a *APIKeyAuth) Authenticate(r *http.Request) (bool, string, error) {
	if a.Header != "" {
		val := r.Header.Get(a.Header)
		if val == a.Key {
			return true, "api-key-user", nil
		}
		return false, "", nil
	}
	if a.QueryParam != "" {
		val := r.URL.Query().Get(a.QueryParam)
		if val == a.Key {
			return true, "api-key-user", nil
		}
		return false, "", nil
	}
	return false, "", nil
}

type MTLSAuth struct{}

func (m *MTLSAuth) Authenticate(r *http.Request) (bool, string, error) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return false, "", nil
	}
	cn := r.TLS.PeerCertificates[0].Subject.CommonName
	return true, cn, nil
}

func ApplyDestAuth(req *http.Request, cfg *config.HTTPAuthConfig) {
	if cfg == nil {
		return
	}
	switch cfg.Type {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	case "basic":
		encoded := base64.StdEncoding.EncodeToString([]byte(cfg.Username + ":" + cfg.Password))
		req.Header.Set("Authorization", "Basic "+encoded)
	case "api_key":
		if cfg.Header != "" {
			req.Header.Set(cfg.Header, cfg.Key)
		} else if cfg.QueryParam != "" {
			q := req.URL.Query()
			q.Set(cfg.QueryParam, cfg.Key)
			req.URL.RawQuery = q.Encode()
		}
	case "oauth2_client_credentials":
		if cfg.TokenURL != "" && cfg.ClientID != "" && cfg.ClientSecret != "" {
			token, err := FetchOAuth2ClientCredentials(cfg.TokenURL, cfg.ClientID, cfg.ClientSecret, cfg.Scopes)
			if err == nil && token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
		}
	}
}

// FetchOAuth2ClientCredentials obtains an access token from the token
// endpoint using the client_credentials grant type.
func FetchOAuth2ClientCredentials(tokenURL, clientID, clientSecret string, scopes []string) (string, error) {
	data := "grant_type=client_credentials&client_id=" + clientID + "&client_secret=" + clientSecret
	if len(scopes) > 0 {
		data += "&scope=" + strings.Join(scopes, " ")
	}

	resp, err := http.Post(tokenURL, "application/x-www-form-urlencoded", strings.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("oauth2 token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oauth2 token endpoint returned %d", resp.StatusCode)
	}

	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("oauth2 decode response: %w", err)
	}
	return result.AccessToken, nil
}
