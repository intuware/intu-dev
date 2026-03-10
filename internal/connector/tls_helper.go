package connector

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/pkg/config"
)

func applyTLSToListener(ln net.Listener, server *http.Server, cfg *config.TLSConfig) (net.Listener, error) {
	if cfg == nil || !cfg.Enabled {
		return ln, nil
	}

	tlsCfg, err := auth.BuildTLSConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build TLS config: %w", err)
	}
	server.TLSConfig = tlsCfg
	return tls.NewListener(ln, tlsCfg), nil
}
