package connector

import (
	"net/http"
	"strings"

	"github.com/intuware/intu-dev/pkg/config"
)

func authenticateHTTP(r *http.Request, cfg *config.AuthConfig) bool {
	if cfg == nil {
		return true
	}

	switch cfg.Type {
	case "bearer":
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		return token != "" && token == cfg.Token
	case "basic":
		user, pass, ok := r.BasicAuth()
		return ok && user == cfg.Username && pass == cfg.Password
	case "api_key":
		if cfg.Header != "" {
			return r.Header.Get(cfg.Header) == cfg.Key
		}
		if cfg.QueryParam != "" {
			return r.URL.Query().Get(cfg.QueryParam) == cfg.Key
		}
		return false
	case "mtls":
		return r.TLS != nil && len(r.TLS.PeerCertificates) > 0
	case "none", "":
		return true
	default:
		return true
	}
}
