package auth

import (
	"encoding/base64"
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
		}
	case "oauth2_client_credentials":
		// Token fetch would happen in a real implementation
		// For now, set a placeholder
	}
}
