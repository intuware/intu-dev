package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// PlatformStore ships message and stage records to a managed control plane
// instead of writing them locally.
//
// It exists because a cloud deployment's operator surface is the platform's
// Messages and Monitor views, not the engine's own dashboard, and because a
// runner task must not hold database credentials: it executes customer
// TypeScript, so anything it can reach is reachable by that code. Posting to an
// authenticated ingest endpoint keeps the blast radius of a hostile channel
// script down to its own deployment's records.
//
// Writes are batched and asynchronous. Message throughput must not be coupled
// to the control plane's latency or availability -- a channel that stops
// accepting HL7 because a reporting endpoint is slow has failed at its actual
// job. The trade-off is explicit: records are best-effort and a hard kill loses
// whatever is still buffered. Delivery guarantees for clinical data are the
// destination's business, not the reporting path's.
//
// Only Save is supported. The read methods exist to satisfy MessageStore and
// return ErrPlatformReadUnsupported; their callers are the engine dashboard and
// the reprocess CLI, neither of which runs in a cloud task.
type PlatformStore struct {
	endpoint     string
	token        string
	deploymentID string
	client       *http.Client
	logger       *slog.Logger

	batchSize     int
	flushInterval time.Duration
	maxQueue      int

	mu      sync.Mutex
	pending []*MessageRecord
	dropped int

	flushNow chan struct{}
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

// ErrPlatformReadUnsupported is returned by the read side of PlatformStore.
// The control plane owns queries over stored messages; an engine that could
// read them back would need query credentials it deliberately does not have.
var ErrPlatformReadUnsupported = errors.New("platform message store is write-only; query the control plane instead")

const (
	defaultPlatformBatchSize     = 25
	defaultPlatformFlushInterval = 2 * time.Second

	// maxQueueMultiple bounds the buffer at a multiple of the batch size. When
	// the control plane is unreachable the queue must not grow until the task
	// is OOM-killed -- losing reporting records is recoverable, losing the
	// channel is not. Overflow drops oldest-first and is counted, so the gap is
	// visible rather than silent.
	maxQueueMultiple = 40
)

// NewPlatformStore constructs the store and starts its flush loop.
func NewPlatformStore(cfg *config.StoragePlatformConfig, logger *slog.Logger) (*PlatformStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("platform config is required when driver is platform")
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("platform.endpoint is required")
	}
	if cfg.DeploymentID == "" {
		return nil, fmt.Errorf("platform.deployment_id is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	batch := cfg.BatchSize
	if batch <= 0 {
		batch = defaultPlatformBatchSize
	}
	interval := defaultPlatformFlushInterval
	if cfg.FlushInterval != "" {
		d, err := time.ParseDuration(cfg.FlushInterval)
		if err != nil {
			return nil, fmt.Errorf("platform.flush_interval %q: %w", cfg.FlushInterval, err)
		}
		if d > 0 {
			interval = d
		}
	}

	s := &PlatformStore{
		endpoint:      cfg.Endpoint,
		token:         cfg.Token,
		deploymentID:  cfg.DeploymentID,
		client:        &http.Client{Timeout: 10 * time.Second},
		logger:        logger,
		batchSize:     batch,
		flushInterval: interval,
		maxQueue:      batch * maxQueueMultiple,
		flushNow:      make(chan struct{}, 1),
		stop:          make(chan struct{}),
		stopped:       make(chan struct{}),
	}
	go s.loop()
	return s, nil
}

// Save queues a record. It never blocks on the network.
func (s *PlatformStore) Save(record *MessageRecord) error {
	if record == nil {
		return nil
	}
	s.mu.Lock()
	s.pending = append(s.pending, record)
	if len(s.pending) > s.maxQueue {
		// Drop oldest: the newest records describe what is happening now, which
		// is what an operator watching a live feed is looking at.
		overflow := len(s.pending) - s.maxQueue
		s.pending = s.pending[overflow:]
		s.dropped += overflow
	}
	full := len(s.pending) >= s.batchSize
	s.mu.Unlock()

	if full {
		select {
		case s.flushNow <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *PlatformStore) Get(string) (*MessageRecord, error) {
	return nil, ErrPlatformReadUnsupported
}

func (s *PlatformStore) GetStage(string, string) (*MessageRecord, error) {
	return nil, ErrPlatformReadUnsupported
}

func (s *PlatformStore) Query(QueryOpts) ([]*MessageRecord, error) {
	return nil, ErrPlatformReadUnsupported
}

func (s *PlatformStore) Count(QueryOpts) (int64, error) {
	return 0, ErrPlatformReadUnsupported
}

func (s *PlatformStore) Delete(string) error {
	return ErrPlatformReadUnsupported
}

// Prune is a no-op rather than an error: retention on the managed store is the
// control plane's policy (it owns the payloads and the legal-hold rules), and
// an engine that returned an error here would make a scheduled prune look like
// a failure every time it ran.
func (s *PlatformStore) Prune(time.Time, string) (int, error) { return 0, nil }

// Close flushes what is buffered and stops the loop. The runner calls it after
// the engine has drained, so the last messages of a deployment's life are not
// the ones that go missing.
func (s *PlatformStore) Close() error {
	s.stopOnce.Do(func() {
		close(s.stop)
		<-s.stopped
	})
	return nil
}

func (s *PlatformStore) loop() {
	defer close(s.stopped)
	t := time.NewTicker(s.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			s.flush(context.Background())
			s.reportDropped()
			return
		case <-t.C:
			s.flush(context.Background())
		case <-s.flushNow:
			s.flush(context.Background())
		}
	}
}

func (s *PlatformStore) reportDropped() {
	s.mu.Lock()
	n := s.dropped
	s.dropped = 0
	s.mu.Unlock()
	if n > 0 {
		s.logger.Warn("platform store dropped records under backpressure", "count", n)
	}
}

type platformBatch struct {
	DeploymentID string           `json:"deployment_id"`
	Records      []platformRecord `json:"records"`
}

// platformRecord is the wire form. Field names are snake_case and stable: the
// control plane's ingest maps them onto its own messages/message_stages
// columns, and renaming one here silently empties a column there.
type platformRecord struct {
	ID            string         `json:"id"`
	CorrelationID string         `json:"correlation_id"`
	ChannelID     string         `json:"channel_id"`
	Stage         string         `json:"stage"`
	Status        string         `json:"status"`
	Content       []byte         `json:"content,omitempty"`
	ContentSize   int            `json:"content_size"`
	DurationMs    int64          `json:"duration_ms"`
	Timestamp     time.Time      `json:"timestamp"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

func toPlatformRecord(r *MessageRecord) platformRecord {
	size := r.ContentSize
	if size == 0 {
		size = len(r.Content)
	}
	return platformRecord{
		ID:            r.ID,
		CorrelationID: r.CorrelationID,
		ChannelID:     r.ChannelID,
		Stage:         r.Stage,
		Status:        r.Status,
		Content:       r.Content,
		ContentSize:   size,
		DurationMs:    r.DurationMs,
		Timestamp:     r.Timestamp,
		Metadata:      r.Metadata,
	}
}

// flush sends one batch. On failure the records go back to the front of the
// queue so ordering survives a transient outage; the queue bound in Save is
// what stops that retry from growing without limit.
func (s *PlatformStore) flush(ctx context.Context) {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return
	}
	n := len(s.pending)
	if n > s.batchSize {
		n = s.batchSize
	}
	batch := s.pending[:n]
	s.pending = s.pending[n:]
	s.mu.Unlock()

	if err := s.post(ctx, batch); err != nil {
		s.logger.Warn("platform message ingest failed; requeueing", "count", len(batch), "error", err)
		s.mu.Lock()
		s.pending = append(batch, s.pending...)
		if len(s.pending) > s.maxQueue {
			overflow := len(s.pending) - s.maxQueue
			s.pending = s.pending[overflow:]
			s.dropped += overflow
		}
		s.mu.Unlock()
	}
}

func (s *PlatformStore) post(ctx context.Context, records []*MessageRecord) error {
	payload := platformBatch{
		DeploymentID: s.deploymentID,
		Records:      make([]platformRecord, 0, len(records)),
	}
	for _, r := range records {
		payload.Records = append(payload.Records, toPlatformRecord(r))
	}
	body, err := json.Marshal(payload)
	if err != nil {
		// Unmarshalable records would poison the queue forever if requeued.
		s.logger.Error("platform record could not be encoded; dropping batch", "count", len(records), "error", err)
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		// Retrying a rejected credential forever accomplishes nothing and hides
		// the real problem behind an endless warning loop.
		s.logger.Error("platform message ingest rejected credentials; dropping batch",
			"status", resp.StatusCode, "count", len(records))
		return nil
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity:
		s.logger.Error("platform message ingest rejected batch as invalid; dropping",
			"status", resp.StatusCode, "count", len(records))
		return nil
	default:
		return fmt.Errorf("platform ingest returned %d", resp.StatusCode)
	}
}
