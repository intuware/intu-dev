package logging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/intuware/intu/pkg/config"
)

const (
	ddFlushInterval = 2 * time.Second
	ddMaxBatchCount = 100
	ddMaxBatchBytes = 5242880 // 5 MB
)

type DatadogTransport struct {
	apiKey  string
	url     string
	service string
	source  string
	tags    string
	client  *http.Client
	batch   *batchBuffer
}

func NewDatadogTransport(cfg *config.DatadogLogConfig) (*DatadogTransport, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("datadog api_key is required")
	}

	site := cfg.Site
	if site == "" {
		site = "datadoghq.com"
	}
	service := cfg.Service
	if service == "" {
		service = "intu"
	}
	source := cfg.Source
	if source == "" {
		source = "intu"
	}
	tags := ""
	if len(cfg.Tags) > 0 {
		tags = strings.Join(cfg.Tags, ",")
	}

	url := fmt.Sprintf("https://http-intake.logs.%s/api/v2/logs", site)

	t := &DatadogTransport{
		apiKey:  cfg.APIKey,
		url:     url,
		service: service,
		source:  source,
		tags:    tags,
		client:  &http.Client{Timeout: 10 * time.Second},
	}

	t.batch = newBatchBuffer(ddMaxBatchCount, ddMaxBatchBytes, ddFlushInterval, t.flushBatch)
	return t, nil
}

func (t *DatadogTransport) Write(p []byte) (int, error) {
	t.batch.Add(p)
	return len(p), nil
}

func (t *DatadogTransport) Close() error {
	return t.batch.Close()
}

func (t *DatadogTransport) flushBatch(batch [][]byte) error {
	if len(batch) == 0 {
		return nil
	}

	entries := make([]map[string]any, 0, len(batch))
	for _, raw := range batch {
		var parsed map[string]any
		if err := json.Unmarshal(bytes.TrimRight(raw, "\n"), &parsed); err != nil {
			parsed = map[string]any{"message": string(raw)}
		}
		parsed["ddsource"] = t.source
		parsed["service"] = t.service
		if t.tags != "" {
			parsed["ddtags"] = t.tags
		}
		entries = append(entries, parsed)
	}

	body, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", t.apiKey)

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("send to datadog: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("datadog returned status %d", resp.StatusCode)
	}
	return nil
}
