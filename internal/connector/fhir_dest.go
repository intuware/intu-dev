package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type FHIRDest struct {
	name   string
	cfg    *config.FHIRDestMapConfig
	client *http.Client
	logger *slog.Logger
}

func NewFHIRDest(name string, cfg *config.FHIRDestMapConfig, logger *slog.Logger) *FHIRDest {
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

	return &FHIRDest{
		name:   name,
		cfg:    cfg,
		client: &http.Client{Timeout: timeout, Transport: transport},
		logger: logger,
	}
}

func (f *FHIRDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	if f.cfg.BaseURL == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("fhir destination %s: base_url not configured", f.name)}, nil
	}

	resourceType := f.extractResourceType(msg)
	operation := f.determineOperation(msg)

	url := f.buildURL(resourceType, operation, msg)
	method := f.httpMethod(operation)

	var body io.Reader
	if method != "GET" && method != "DELETE" {
		body = bytes.NewReader(msg.Raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("fhir create request: %w", err)}, nil
	}

	req.Header.Set("Content-Type", "application/fhir+json")
	req.Header.Set("Accept", "application/fhir+json")

	if f.cfg.Auth != nil {
		f.applyAuth(req)
	}

	finalHeaders := make(map[string]string, len(req.Header))
	for k, v := range req.Header {
		if len(v) > 0 {
			finalHeaders[k] = v[0]
		}
	}
	msg.ClearTransportMeta()
	msg.Transport = "fhir"
	msg.HTTP = &message.HTTPMeta{
		Headers: finalHeaders,
		Method:  method,
	}

	resp, err := f.client.Do(req)
	if err != nil {
		f.logger.Error("fhir dest send failed", "destination", f.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("fhir send: %w", err)}, nil
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	headers := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	if resp.StatusCode >= 400 {
		f.logger.Error("fhir dest received error response",
			"destination", f.name,
			"status", resp.StatusCode,
			"resource_type", resourceType,
			"operation", operation,
		)
		return &message.Response{
			StatusCode: resp.StatusCode,
			Body:       respBody,
			Headers:    headers,
			Error:      fmt.Errorf("fhir server returned %d", resp.StatusCode),
		}, nil
	}

	f.logger.Debug("fhir dest message sent",
		"destination", f.name,
		"resource_type", resourceType,
		"operation", operation,
		"status", resp.StatusCode,
	)

	return &message.Response{StatusCode: resp.StatusCode, Body: respBody, Headers: headers}, nil
}

func (f *FHIRDest) extractResourceType(msg *message.Message) string {
	if rt, ok := msg.Metadata["resource_type"].(string); ok && rt != "" {
		return rt
	}

	var resource map[string]any
	if json.Unmarshal(msg.Raw, &resource) == nil {
		if rt, ok := resource["resourceType"].(string); ok {
			return rt
		}
	}

	return ""
}

func (f *FHIRDest) determineOperation(msg *message.Message) string {
	if op, ok := msg.Metadata["fhir_operation"].(string); ok && op != "" {
		return op
	}

	if len(f.cfg.Operations) > 0 {
		return f.cfg.Operations[0]
	}

	// Auto-detect: if the outbound body is a Bundle of type transaction or
	// batch, map Bundle.type to the corresponding FHIR interaction so the
	// engine POSTs to the base URL instead of {base}/Bundle (which HAPI and
	// most FHIR servers reject with 422 because a transaction/batch Bundle
	// is not meant to be stored as a Bundle resource).
	var env struct {
		ResourceType string `json:"resourceType"`
		Type         string `json:"type"`
	}
	if json.Unmarshal(msg.Raw, &env) == nil && env.ResourceType == "Bundle" {
		switch strings.ToLower(env.Type) {
		case "transaction":
			return "transaction"
		case "batch":
			return "batch"
		}
	}

	return "create"
}

func (f *FHIRDest) buildURL(resourceType, operation string, msg *message.Message) string {
	base := strings.TrimRight(f.cfg.BaseURL, "/")

	switch operation {
	case "create":
		if resourceType != "" {
			return fmt.Sprintf("%s/%s", base, resourceType)
		}
		return base
	case "update":
		resourceID := ""
		if id, ok := msg.Metadata["resource_id"].(string); ok {
			resourceID = id
		} else {
			var resource map[string]any
			if json.Unmarshal(msg.Raw, &resource) == nil {
				if id, ok := resource["id"].(string); ok {
					resourceID = id
				}
			}
		}
		if resourceType != "" && resourceID != "" {
			return fmt.Sprintf("%s/%s/%s", base, resourceType, resourceID)
		}
		if resourceType != "" {
			return fmt.Sprintf("%s/%s", base, resourceType)
		}
		return base
	case "transaction", "batch":
		return base
	default:
		if resourceType != "" {
			return fmt.Sprintf("%s/%s", base, resourceType)
		}
		return base
	}
}

func (f *FHIRDest) httpMethod(operation string) string {
	switch operation {
	case "create":
		return "POST"
	case "update":
		return "PUT"
	case "delete":
		return "DELETE"
	case "read":
		return "GET"
	case "transaction", "batch":
		return "POST"
	default:
		return "POST"
	}
}

func (f *FHIRDest) applyAuth(req *http.Request) {
	if f.cfg.Auth == nil {
		return
	}
	switch f.cfg.Auth.Type {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+f.cfg.Auth.Token)
	case "basic":
		req.SetBasicAuth(f.cfg.Auth.Username, f.cfg.Auth.Password)
	case "api_key":
		if f.cfg.Auth.Header != "" {
			req.Header.Set(f.cfg.Auth.Header, f.cfg.Auth.Key)
		} else if f.cfg.Auth.QueryParam != "" {
			q := req.URL.Query()
			q.Set(f.cfg.Auth.QueryParam, f.cfg.Auth.Key)
			req.URL.RawQuery = q.Encode()
		}
	case "oauth2_client_credentials":
		token, err := fetchOAuth2Token(f.cfg.Auth.TokenURL, f.cfg.Auth.ClientID, f.cfg.Auth.ClientSecret, f.cfg.Auth.Scopes)
		if err != nil {
			f.logger.Error("fhir dest oauth2 token fetch failed", "error", err)
			return
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

func (f *FHIRDest) Stop(ctx context.Context) error {
	f.client.CloseIdleConnections()
	return nil
}

func (f *FHIRDest) Type() string {
	return "fhir"
}
