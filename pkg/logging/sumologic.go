package logging

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/intuware/intu/pkg/config"
)

const (
	sumoFlushInterval = 2 * time.Second
	sumoMaxBatchCount = 100
	sumoMaxBatchBytes = 1048576 // 1 MB
)

type SumoLogicTransport struct {
	endpoint       string
	sourceCategory string
	sourceName     string
	client         *http.Client
	batch          *batchBuffer
}

func NewSumoLogicTransport(cfg *config.SumoLogicLogConfig) (*SumoLogicTransport, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("sumologic endpoint is required")
	}

	t := &SumoLogicTransport{
		endpoint:       cfg.Endpoint,
		sourceCategory: cfg.SourceCategory,
		sourceName:     cfg.SourceName,
		client:         &http.Client{Timeout: 10 * time.Second},
	}

	t.batch = newBatchBuffer(sumoMaxBatchCount, sumoMaxBatchBytes, sumoFlushInterval, t.flushBatch)
	return t, nil
}

func (t *SumoLogicTransport) Write(p []byte) (int, error) {
	t.batch.Add(p)
	return len(p), nil
}

func (t *SumoLogicTransport) Close() error {
	return t.batch.Close()
}

func (t *SumoLogicTransport) flushBatch(batch [][]byte) error {
	if len(batch) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, entry := range batch {
		buf.Write(bytes.TrimRight(entry, "\n"))
		buf.WriteByte('\n')
	}

	req, err := http.NewRequest(http.MethodPost, t.endpoint, &buf)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if t.sourceCategory != "" {
		req.Header.Set("X-Sumo-Category", t.sourceCategory)
	}
	if t.sourceName != "" {
		req.Header.Set("X-Sumo-Name", t.sourceName)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("send to sumo logic: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("sumo logic returned status %d", resp.StatusCode)
	}
	return nil
}
