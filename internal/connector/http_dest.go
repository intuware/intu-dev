package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type HTTPDest struct {
	name   string
	cfg    *config.HTTPDestConfig
	client *http.Client
	logger *slog.Logger
}

func NewHTTPDest(name string, cfg *config.HTTPDestConfig, logger *slog.Logger) *HTTPDest {
	timeout := 30 * time.Second
	if cfg.TimeoutMs > 0 {
		timeout = time.Duration(cfg.TimeoutMs) * time.Millisecond
	}

	transport := &http.Transport{}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsCfg, err := auth.BuildTLSConfigFromMap(cfg.TLS)
		if err == nil && tlsCfg != nil {
			transport.TLSClientConfig = tlsCfg
		}
	}

	return &HTTPDest{
		name:   name,
		cfg:    cfg,
		client: &http.Client{Timeout: timeout, Transport: transport},
		logger: logger,
	}
}

func (h *HTTPDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	method := h.cfg.Method
	if method == "" {
		method = "POST"
	}

	headers := mergeMaps(h.cfg.Headers, nil)
	queryParams := mergeMaps(h.cfg.QueryParams, nil)
	pathParams := mergeMaps(h.cfg.PathParams, nil)
	if msg.HTTP != nil {
		headers = mergeMaps(headers, msg.HTTP.Headers)
		queryParams = mergeMaps(queryParams, msg.HTTP.QueryParams)
		pathParams = mergeMaps(pathParams, msg.HTTP.PathParams)
	}

	targetURL := h.cfg.URL
	for k, v := range pathParams {
		targetURL = strings.ReplaceAll(targetURL, "{"+k+"}", url.PathEscape(v))
	}

	if len(queryParams) > 0 {
		u, err := url.Parse(targetURL)
		if err != nil {
			return nil, fmt.Errorf("parse URL: %w", err)
		}
		q := u.Query()
		for k, v := range queryParams {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
		targetURL = u.String()
	}

	req, err := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(msg.Raw))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	if h.cfg.Auth != nil {
		h.applyAuth(req)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return &message.Response{Error: err}, nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	respHeaders := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			respHeaders[k] = v[0]
		}
	}

	return &message.Response{
		StatusCode: resp.StatusCode,
		Body:       body,
		Headers:    respHeaders,
	}, nil
}

func mergeMaps(base, override map[string]string) map[string]string {
	merged := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range override {
		merged[k] = v
	}
	return merged
}

func (h *HTTPDest) applyAuth(req *http.Request) {
	if h.cfg.Auth == nil {
		return
	}
	switch h.cfg.Auth.Type {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+h.cfg.Auth.Token)
	case "basic":
		req.SetBasicAuth(h.cfg.Auth.Username, h.cfg.Auth.Password)
	case "api_key":
		if h.cfg.Auth.Header != "" {
			req.Header.Set(h.cfg.Auth.Header, h.cfg.Auth.Key)
		} else if h.cfg.Auth.QueryParam != "" {
			q := req.URL.Query()
			q.Set(h.cfg.Auth.QueryParam, h.cfg.Auth.Key)
			req.URL.RawQuery = q.Encode()
		}
	case "oauth2_client_credentials":
		token, err := fetchOAuth2Token(h.cfg.Auth.TokenURL, h.cfg.Auth.ClientID, h.cfg.Auth.ClientSecret, h.cfg.Auth.Scopes)
		if err != nil {
			h.logger.Error("http dest oauth2 token fetch failed", "destination", h.name, "error", err)
			return
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

func (h *HTTPDest) Stop(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return nil
}

func (h *HTTPDest) Type() string {
	return "http"
}
