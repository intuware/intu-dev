package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
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
	return &HTTPDest{
		name:   name,
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
		logger: logger,
	}
}

func (h *HTTPDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	method := h.cfg.Method
	if method == "" {
		method = "POST"
	}

	req, err := http.NewRequestWithContext(ctx, method, h.cfg.URL, bytes.NewReader(msg.Raw))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range h.cfg.Headers {
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
	headers := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	return &message.Response{
		StatusCode: resp.StatusCode,
		Body:       body,
		Headers:    headers,
	}, nil
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
		}
	}
}

func (h *HTTPDest) Stop(ctx context.Context) error {
	h.client.CloseIdleConnections()
	return nil
}

func (h *HTTPDest) Type() string {
	return "http"
}
