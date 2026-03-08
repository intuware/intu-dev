package logging

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/intuware/intu/pkg/config"
)

const (
	esFlushInterval = 2 * time.Second
	esMaxBatchCount = 100
	esMaxBatchBytes = 5242880 // 5 MB
)

type ElasticsearchTransport struct {
	urls      []string
	index     string
	authHeader string
	client    *http.Client
	batch     *batchBuffer
	urlIdx    int
}

func NewElasticsearchTransport(cfg *config.ElasticsearchLogConfig) (*ElasticsearchTransport, error) {
	if len(cfg.URLs) == 0 {
		return nil, fmt.Errorf("elasticsearch urls list is required")
	}
	index := cfg.Index
	if index == "" {
		index = "intu-logs"
	}

	var authHeader string
	if cfg.APIKey != "" {
		authHeader = "ApiKey " + cfg.APIKey
	} else if cfg.Username != "" {
		creds := base64.StdEncoding.EncodeToString([]byte(cfg.Username + ":" + cfg.Password))
		authHeader = "Basic " + creds
	}

	t := &ElasticsearchTransport{
		urls:       cfg.URLs,
		index:      index,
		authHeader: authHeader,
		client:     &http.Client{Timeout: 10 * time.Second},
	}

	t.batch = newBatchBuffer(esMaxBatchCount, esMaxBatchBytes, esFlushInterval, t.flushBatch)
	return t, nil
}

func (t *ElasticsearchTransport) Write(p []byte) (int, error) {
	t.batch.Add(p)
	return len(p), nil
}

func (t *ElasticsearchTransport) Close() error {
	return t.batch.Close()
}

func (t *ElasticsearchTransport) resolvedIndex() string {
	now := time.Now()
	idx := t.index
	idx = strings.ReplaceAll(idx, "{year}", fmt.Sprintf("%d", now.Year()))
	idx = strings.ReplaceAll(idx, "{month}", fmt.Sprintf("%02d", now.Month()))
	idx = strings.ReplaceAll(idx, "{day}", fmt.Sprintf("%02d", now.Day()))
	return idx
}

func (t *ElasticsearchTransport) flushBatch(batch [][]byte) error {
	if len(batch) == 0 {
		return nil
	}

	idx := t.resolvedIndex()
	actionLine := []byte(fmt.Sprintf(`{"index":{"_index":"%s"}}`, idx) + "\n")

	var buf bytes.Buffer
	for _, entry := range batch {
		buf.Write(actionLine)
		buf.Write(bytes.TrimRight(entry, "\n"))
		buf.WriteByte('\n')
	}

	baseURL := t.urls[t.urlIdx%len(t.urls)]
	t.urlIdx++
	url := strings.TrimRight(baseURL, "/") + "/_bulk"

	req, err := http.NewRequest(http.MethodPost, url, &buf)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if t.authHeader != "" {
		req.Header.Set("Authorization", t.authHeader)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("send to elasticsearch: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("elasticsearch returned status %d", resp.StatusCode)
	}
	return nil
}
