package storage

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

type capture struct {
	mu       sync.Mutex
	batches  []platformBatch
	authSeen []string
	status   int
	failNext int
}

func (c *capture) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var b platformBatch
		_ = json.Unmarshal(body, &b)

		c.mu.Lock()
		defer c.mu.Unlock()
		c.authSeen = append(c.authSeen, r.Header.Get("Authorization"))
		if c.failNext > 0 {
			c.failNext--
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		c.batches = append(c.batches, b)
		if c.status != 0 {
			w.WriteHeader(c.status)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func (c *capture) records() []platformRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []platformRecord
	for _, b := range c.batches {
		out = append(out, b.Records...)
	}
	return out
}

func newTestStore(t *testing.T, srvURL string, batchSize int) *PlatformStore {
	t.Helper()
	s, err := NewPlatformStore(&config.StoragePlatformConfig{
		Endpoint:      srvURL,
		Token:         "tok-123",
		DeploymentID:  "dep-abc",
		BatchSize:     batchSize,
		FlushInterval: "20ms",
	}, nil)
	if err != nil {
		t.Fatalf("NewPlatformStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func rec(id, stage, status string) *MessageRecord {
	return &MessageRecord{
		ID:            id,
		CorrelationID: "corr-" + id,
		ChannelID:     "adt-feed",
		Stage:         stage,
		Status:        status,
		Content:       []byte(`{"raw":"MSH|^~\\&|"}`),
		Timestamp:     time.Unix(1700000000, 0).UTC(),
		Metadata:      map[string]any{"destination": "ehr"},
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

func TestPlatformStorePostsRecordsWithAuth(t *testing.T) {
	c := &capture{}
	srv := httptest.NewServer(c.handler())
	defer srv.Close()

	s := newTestStore(t, srv.URL, 2)
	if err := s.Save(rec("m1", "received", "RECEIVED")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save(rec("m1", "transformed", "TRANSFORMED")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	waitFor(t, func() bool { return len(c.records()) == 2 })

	got := c.records()
	if got[0].ID != "m1" || got[0].Stage != "received" {
		t.Errorf("first record = %+v", got[0])
	}
	if got[1].Stage != "transformed" {
		t.Errorf("second record stage = %q, want transformed", got[1].Stage)
	}
	if got[0].ContentSize == 0 {
		t.Error("ContentSize was not derived from Content")
	}
	if got[0].Metadata["destination"] != "ehr" {
		t.Errorf("metadata not carried: %+v", got[0].Metadata)
	}

	c.mu.Lock()
	auth := c.authSeen[0]
	c.mu.Unlock()
	if auth != "Bearer tok-123" {
		t.Errorf("Authorization = %q, want Bearer tok-123", auth)
	}
}

// Every Save must reach the wire. intu-dev writes a `sent` record per
// destination under the same (id, stage) key, so a store that deduplicated on
// that key would keep only the last destination -- which is the exact problem
// the control plane's schema was built to avoid.
func TestPlatformStoreDoesNotDedupePerDestinationSends(t *testing.T) {
	c := &capture{}
	srv := httptest.NewServer(c.handler())
	defer srv.Close()

	s := newTestStore(t, srv.URL, 10)

	first := rec("m1", "sent", "SENT")
	first.Metadata = map[string]any{"destination": "ehr"}
	second := rec("m1", "sent", "SENT")
	second.Metadata = map[string]any{"destination": "archive"}

	_ = s.Save(first)
	_ = s.Save(second)

	waitFor(t, func() bool { return len(c.records()) == 2 })

	dests := map[string]bool{}
	for _, r := range c.records() {
		dests[r.Metadata["destination"].(string)] = true
	}
	if !dests["ehr"] || !dests["archive"] {
		t.Errorf("both destinations must survive, got %v", dests)
	}
}

// A slow or broken control plane must not lose ordering on recovery, and must
// not stop the channel.
func TestPlatformStoreRequeuesOnTransientFailure(t *testing.T) {
	c := &capture{failNext: 1}
	srv := httptest.NewServer(c.handler())
	defer srv.Close()

	s := newTestStore(t, srv.URL, 1)
	_ = s.Save(rec("m1", "received", "RECEIVED"))

	waitFor(t, func() bool { return len(c.records()) == 1 })
	if got := c.records()[0].ID; got != "m1" {
		t.Errorf("record id = %q, want m1 after retry", got)
	}
}

// A rejected credential is a permanent condition. Retrying it forever would
// spin the flush loop and bury the real cause under a repeating warning.
func TestPlatformStoreDropsBatchOnAuthRejection(t *testing.T) {
	var calls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	s := newTestStore(t, srv.URL, 1)
	_ = s.Save(rec("m1", "received", "RECEIVED"))

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 1
	})
	time.Sleep(120 * time.Millisecond)

	mu.Lock()
	got := calls
	mu.Unlock()
	if got > 2 {
		t.Errorf("batch was retried %d times after a 401; it should be dropped", got)
	}
}

// The queue is bounded so an unreachable control plane cannot OOM the task.
// Losing reporting records is recoverable; losing the channel is not.
func TestPlatformStoreBoundsQueueUnderBackpressure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	s := newTestStore(t, srv.URL, 2)
	for i := 0; i < 5000; i++ {
		_ = s.Save(rec("m", "received", "RECEIVED"))
	}

	s.mu.Lock()
	n := len(s.pending)
	dropped := s.dropped
	s.mu.Unlock()

	if n > s.maxQueue {
		t.Errorf("pending = %d, exceeds bound %d", n, s.maxQueue)
	}
	if dropped == 0 {
		t.Error("overflow was not counted; a silent gap in the record stream")
	}
}

// Close must land what is buffered: the last records of a deployment's life
// describe how it ended, which is exactly when someone is looking.
func TestPlatformStoreFlushesOnClose(t *testing.T) {
	c := &capture{}
	srv := httptest.NewServer(c.handler())
	defer srv.Close()

	s, err := NewPlatformStore(&config.StoragePlatformConfig{
		Endpoint:      srv.URL,
		DeploymentID:  "dep-abc",
		BatchSize:     100,
		FlushInterval: "1h",
	}, nil)
	if err != nil {
		t.Fatalf("NewPlatformStore: %v", err)
	}
	_ = s.Save(rec("last", "sent", "SENT"))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := c.records(); len(got) != 1 || got[0].ID != "last" {
		t.Fatalf("Close did not flush the buffer, got %+v", got)
	}
}

func TestPlatformStoreReadsAreUnsupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	s := newTestStore(t, srv.URL, 10)

	if _, err := s.Get("m1"); !errors.Is(err, ErrPlatformReadUnsupported) {
		t.Errorf("Get err = %v", err)
	}
	if _, err := s.Query(QueryOpts{}); !errors.Is(err, ErrPlatformReadUnsupported) {
		t.Errorf("Query err = %v", err)
	}
	// Prune is deliberately a no-op, not an error: retention is control-plane
	// policy and a scheduled prune should not report failure every run.
	if n, err := s.Prune(time.Now(), ""); err != nil || n != 0 {
		t.Errorf("Prune = (%d, %v), want (0, nil)", n, err)
	}
}

func TestNewMessageStoreWiresPlatformDriver(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	st, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "platform",
		Platform: &config.StoragePlatformConfig{
			Endpoint:     srv.URL,
			DeploymentID: "dep-abc",
		},
	})
	if err != nil {
		t.Fatalf("NewMessageStore: %v", err)
	}
	ps, ok := st.(*PlatformStore)
	if !ok {
		t.Fatalf("store = %T, want *PlatformStore", st)
	}
	_ = ps.Close()
}

func TestNewMessageStorePlatformRequiresConfig(t *testing.T) {
	if _, err := NewMessageStore(&config.MessageStorageConfig{Driver: "platform"}); err == nil {
		t.Fatal("expected an error when platform config is missing")
	}
	if _, err := NewMessageStore(&config.MessageStorageConfig{
		Driver:   "platform",
		Platform: &config.StoragePlatformConfig{DeploymentID: "d"},
	}); err == nil {
		t.Fatal("expected an error when endpoint is missing")
	}
}
