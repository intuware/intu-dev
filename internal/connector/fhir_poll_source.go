package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type FHIRPollSource struct {
	cfg    *config.FHIRPollListener
	client *http.Client
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewFHIRPollSource(cfg *config.FHIRPollListener, logger *slog.Logger) *FHIRPollSource {
	timeout := 30 * time.Second
	transport := &http.Transport{}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsCfg, err := auth.BuildTLSConfig(cfg.TLS)
		if err == nil && tlsCfg != nil {
			transport.TLSClientConfig = tlsCfg
		}
	}
	return &FHIRPollSource{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout, Transport: transport},
		logger: logger,
	}
}

func (f *FHIRPollSource) Start(ctx context.Context, handler MessageHandler) error {
	if f.cfg.BaseURL == "" {
		return fmt.Errorf("fhir_poll: base_url is required")
	}
	if len(f.cfg.Resources) == 0 && len(f.cfg.SearchQueries) == 0 {
		return fmt.Errorf("fhir_poll: at least one of resources or search_queries is required")
	}

	interval := 1 * time.Minute
	if f.cfg.PollInterval != "" {
		if d, err := time.ParseDuration(f.cfg.PollInterval); err == nil {
			interval = d
		}
	}

	pollRange := f.cfg.PollRange
	if pollRange == "" {
		pollRange = f.cfg.Since
	}

	dateParam := f.cfg.DateParam
	if dateParam == "" {
		dateParam = "_lastUpdated"
	}

	ctx, f.cancel = context.WithCancel(ctx)
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := f.poll(ctx, handler, pollRange, dateParam); err != nil {
					f.logger.Error("fhir_poll poll error", "error", err)
				}
			}
		}
	}()

	f.logger.Info("fhir_poll source started",
		"base_url", f.cfg.BaseURL,
		"poll_interval", interval,
		"poll_range", pollRange,
	)
	return nil
}

func (f *FHIRPollSource) poll(ctx context.Context, handler MessageHandler, pollRange, dateParam string) error {
	baseURL := strings.TrimRight(f.cfg.BaseURL, "/")
	if baseURL == "" {
		return fmt.Errorf("fhir_poll: base_url not configured")
	}

	var fhirTime string
	if pollRange != "" {
		d, err := time.ParseDuration(pollRange)
		if err != nil {
			return fmt.Errorf("fhir_poll poll_range/since parse: %w", err)
		}
		sinceTime := time.Now().Add(-d)
		fhirTime = sinceTime.UTC().Format("2006-01-02T15:04:05Z07:00")
	}

	queries := f.buildQueries(baseURL, fhirTime, dateParam)
	for _, rawURL := range queries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			f.logger.Error("fhir_poll build request failed", "url", rawURL, "error", err)
			continue
		}
		req.Header.Set("Accept", "application/fhir+json")
		f.applyAuth(req)

		resp, err := f.client.Do(req)
		if err != nil {
			f.logger.Error("fhir_poll GET failed", "url", rawURL, "error", err)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			f.logger.Error("fhir_poll read body failed", "error", err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			f.logger.Error("fhir_poll GET non-200", "url", rawURL, "status", resp.StatusCode, "body", string(body))
			continue
		}

		if err := f.emitFromResponse(ctx, handler, body); err != nil {
			f.logger.Error("fhir_poll emit failed", "url", rawURL, "error", err)
		}
	}
	return nil
}

func (f *FHIRPollSource) buildQueries(baseURL, fhirTime, dateParam string) []string {
	var out []string
	var param string
	if fhirTime != "" && dateParam != "" {
		param = dateParam + "=ge" + url.QueryEscape(fhirTime)
	}
	_count := "_count=200"

	if len(f.cfg.Resources) > 0 {
		for _, res := range f.cfg.Resources {
			res = strings.TrimSpace(res)
			if res == "" {
				continue
			}
			var u string
			if param != "" {
				u = baseURL + "/" + res + "?" + param + "&" + _count
			} else {
				u = baseURL + "/" + res + "?" + _count
			}
			out = append(out, u)
		}
	}
	if len(f.cfg.SearchQueries) > 0 {
		for _, q := range f.cfg.SearchQueries {
			q = strings.TrimSpace(q)
			if q == "" {
				continue
			}
			path := q
			if !strings.HasPrefix(path, "/") {
				path = "/" + path
			}
			full := baseURL + path
			if param != "" {
				if strings.Contains(q, "?") {
					full += "&" + param
				} else {
					full += "?" + param
				}
			} else if !strings.Contains(q, "?") {
				full += "?" + _count
			}
			out = append(out, full)
		}
	}
	return out
}

func (f *FHIRPollSource) emitFromResponse(ctx context.Context, handler MessageHandler, body []byte) error {
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return err
	}
	rt, _ := root["resourceType"].(string)

	if rt == "Bundle" {
		entries, _ := root["entry"].([]any)
		for _, e := range entries {
			entry, _ := e.(map[string]any)
			resource, _ := entry["resource"]
			if resource == nil {
				continue
			}
			raw, err := json.Marshal(resource)
			if err != nil {
				continue
			}
			msg := message.New("", raw)
			msg.Transport = "fhir_poll"
			msg.ContentType = "fhir_r4"
			msg.Metadata["source"] = "fhir_poll"
			if f.cfg.Version != "" {
				msg.Metadata["fhir_version"] = f.cfg.Version
			}
			if res, ok := resource.(map[string]any); ok {
				if rtype, ok := res["resourceType"].(string); ok {
					msg.Metadata["resource_type"] = rtype
				}
				if id, ok := res["id"].(string); ok {
					msg.Metadata["resource_id"] = id
				}
			}
			if err := handler(ctx, msg); err != nil {
				return err
			}
		}
		return nil
	}

	msg := message.New("", body)
	msg.Transport = "fhir_poll"
	msg.ContentType = "fhir_r4"
	msg.Metadata["source"] = "fhir_poll"
	if f.cfg.Version != "" {
		msg.Metadata["fhir_version"] = f.cfg.Version
	}
	if rt != "" {
		msg.Metadata["resource_type"] = rt
	}
	if id, ok := root["id"].(string); ok {
		msg.Metadata["resource_id"] = id
	}
	return handler(ctx, msg)
}

func (f *FHIRPollSource) applyAuth(req *http.Request) {
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
			f.logger.Error("fhir_poll oauth2 token fetch failed", "error", err)
			return
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

func (f *FHIRPollSource) Stop(ctx context.Context) error {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
	f.client.CloseIdleConnections()
	return nil
}

func (f *FHIRPollSource) Type() string {
	return "fhir_poll"
}
