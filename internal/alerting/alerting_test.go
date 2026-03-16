package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/observability"
	"github.com/intuware/intu-dev/pkg/config"
)

func TestNewAlertManager(t *testing.T) {
	m := observability.NewMetrics()
	logger := slog.Default()
	am := NewAlertManager(nil, m, nil, logger)
	if am == nil {
		t.Fatal("NewAlertManager returned nil")
	}
	if am.alerts != nil {
		t.Error("alerts should be nil")
	}
	if am.metrics != m {
		t.Error("metrics should match")
	}
	if am.send != nil {
		t.Error("send should be nil")
	}
	if am.logger != logger {
		t.Error("logger should match")
	}
}

func TestAlertManager_StartStop(t *testing.T) {
	m := observability.NewMetrics()
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	am := NewAlertManager([]config.AlertConfig{}, m, nil, logger)
	ctx := context.Background()
	am.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	am.Stop()
	if !strings.Contains(buf.String(), "alert manager started") {
		t.Errorf("expected start log, got %s", buf.String())
	}
}

func TestAlertManager_StartStopWithCancel(t *testing.T) {
	m := observability.NewMetrics()
	am := NewAlertManager(nil, m, nil, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	am.Start(ctx)
	cancel()
	am.Stop()
}

func TestAlertManager_ErrorCountTrigger(t *testing.T) {
	m := observability.NewMetrics()
	var sent [][]byte
	var mu sync.Mutex
	send := func(ctx context.Context, dest string, payload []byte) error {
		mu.Lock()
		sent = append(sent, payload)
		mu.Unlock()
		return nil
	}
	logger := slog.Default()
	alerts := []config.AlertConfig{
		{
			Name: "high-errors",
			Trigger: config.AlertTrigger{
				Type:      "error_count",
				Channel:   "ch1",
				Threshold: 5,
			},
			Destinations: []string{"webhook"},
		},
	}
	am := NewAlertManager(alerts, m, send, logger)
	ctx := context.Background()

	// Set up counter that alerting checks: messages_errored_total.ch1
	// (alerting checks per-channel aggregate, not per-destination)
	m.Counter("messages_errored_total.ch1").Add(10)

	am.evaluate(ctx)

	mu.Lock()
	count := len(sent)
	payloads := make([][]byte, len(sent))
	copy(payloads, sent)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 alert sent, got %d", count)
	}
	var event AlertEvent
	if err := json.Unmarshal(payloads[0], &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if event.Name != "high-errors" {
		t.Errorf("expected name high-errors, got %s", event.Name)
	}
	if event.Trigger != "error_count" {
		t.Errorf("expected trigger error_count, got %s", event.Trigger)
	}
	if event.Channel != "ch1" {
		t.Errorf("expected channel ch1, got %s", event.Channel)
	}
	if event.Value != 10 {
		t.Errorf("expected value 10, got %d", event.Value)
	}
	if event.Threshold != 5 {
		t.Errorf("expected threshold 5, got %d", event.Threshold)
	}
	if event.Message == "" {
		t.Error("expected non-empty message")
	}
}

func TestAlertManager_ErrorCountNotTriggered(t *testing.T) {
	m := observability.NewMetrics()
	var sendCount atomic.Int32
	send := func(ctx context.Context, dest string, payload []byte) error {
		sendCount.Add(1)
		return nil
	}
	alerts := []config.AlertConfig{
		{
			Name: "high-errors",
			Trigger: config.AlertTrigger{
				Type:      "error_count",
				Channel:   "ch1",
				Threshold: 100,
			},
			Destinations: []string{"webhook"},
		},
	}
	am := NewAlertManager(alerts, m, send, slog.Default())
	m.Counter("messages_errored_total.ch1").Add(5)
	am.evaluate(context.Background())
	if sendCount.Load() != 0 {
		t.Errorf("expected 0 sends (below threshold), got %d", sendCount.Load())
	}
}

func TestAlertManager_ErrorCountExactThreshold(t *testing.T) {
	m := observability.NewMetrics()
	var sendCount atomic.Int32
	send := func(ctx context.Context, dest string, payload []byte) error {
		sendCount.Add(1)
		return nil
	}
	alerts := []config.AlertConfig{
		{
			Name: "exact",
			Trigger: config.AlertTrigger{
				Type:      "error_count",
				Channel:   "ch1",
				Threshold: 3,
			},
			Destinations: []string{"webhook"},
		},
	}
	am := NewAlertManager(alerts, m, send, slog.Default())
	m.Counter("messages_errored_total.ch1").Add(3)
	am.evaluate(context.Background())
	if sendCount.Load() != 1 {
		t.Errorf("expected 1 send at exact threshold, got %d", sendCount.Load())
	}
}

func TestAlertManager_QueueDepthTrigger(t *testing.T) {
	m := observability.NewMetrics()
	var sent [][]byte
	var mu sync.Mutex
	send := func(ctx context.Context, dest string, payload []byte) error {
		mu.Lock()
		sent = append(sent, payload)
		mu.Unlock()
		return nil
	}
	alerts := []config.AlertConfig{
		{
			Name: "queue-backlog",
			Trigger: config.AlertTrigger{
				Type:      "queue_depth",
				Channel:   "ch1",
				Threshold: 50,
			},
			Destinations: []string{"slack"},
		},
	}
	am := NewAlertManager(alerts, m, send, slog.Default())
	m.Gauge("queue_depth.ch1").Store(100)
	am.evaluate(context.Background())

	mu.Lock()
	count := len(sent)
	payloads := make([][]byte, len(sent))
	copy(payloads, sent)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 alert sent, got %d", count)
	}
	var event AlertEvent
	if err := json.Unmarshal(payloads[0], &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if event.Name != "queue-backlog" {
		t.Errorf("expected name queue-backlog, got %s", event.Name)
	}
	if event.Trigger != "queue_depth" {
		t.Errorf("expected trigger queue_depth, got %s", event.Trigger)
	}
	if event.Value != 100 {
		t.Errorf("expected value 100, got %d", event.Value)
	}
}

func TestAlertManager_QueueDepthNotTriggered(t *testing.T) {
	m := observability.NewMetrics()
	var sendCount atomic.Int32
	send := func(ctx context.Context, dest string, payload []byte) error {
		sendCount.Add(1)
		return nil
	}
	alerts := []config.AlertConfig{
		{
			Name: "queue-backlog",
			Trigger: config.AlertTrigger{
				Type:      "queue_depth",
				Channel:   "ch1",
				Threshold: 100,
			},
			Destinations: []string{"slack"},
		},
	}
	am := NewAlertManager(alerts, m, send, slog.Default())
	m.Gauge("queue_depth.ch1").Store(10)
	am.evaluate(context.Background())
	if sendCount.Load() != 0 {
		t.Errorf("expected 0 sends, got %d", sendCount.Load())
	}
}

func TestAlertManager_UnknownTriggerType(t *testing.T) {
	m := observability.NewMetrics()
	var sendCount atomic.Int32
	send := func(ctx context.Context, dest string, payload []byte) error {
		sendCount.Add(1)
		return nil
	}
	alerts := []config.AlertConfig{
		{
			Name: "unknown",
			Trigger: config.AlertTrigger{
				Type:      "unknown_type",
				Channel:   "ch1",
				Threshold: 1,
			},
			Destinations: []string{"webhook"},
		},
	}
	am := NewAlertManager(alerts, m, send, slog.Default())
	am.evaluate(context.Background())
	if sendCount.Load() != 0 {
		t.Errorf("unknown trigger type should not send, got %d", sendCount.Load())
	}
}

func TestAlertManager_ErrorCountWildcardChannel(t *testing.T) {
	m := observability.NewMetrics()
	var sent [][]byte
	var mu sync.Mutex
	send := func(ctx context.Context, dest string, payload []byte) error {
		mu.Lock()
		sent = append(sent, payload)
		mu.Unlock()
		return nil
	}
	alerts := []config.AlertConfig{
		{
			Name: "all-errors",
			Trigger: config.AlertTrigger{
				Type:      "error_count",
				Channel:   "*",
				Threshold: 1,
			},
			Destinations: []string{"webhook"},
		},
	}
	am := NewAlertManager(alerts, m, send, slog.Default())
	m.Counter("messages_errored_total").Add(5)
	am.evaluate(context.Background())

	mu.Lock()
	count := len(sent)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 alert for wildcard channel, got %d", count)
	}
}

func TestAlertManager_SendToMultipleDestinations(t *testing.T) {
	m := observability.NewMetrics()
	var dests []string
	var mu sync.Mutex
	send := func(ctx context.Context, dest string, payload []byte) error {
		mu.Lock()
		dests = append(dests, dest)
		mu.Unlock()
		return nil
	}
	alerts := []config.AlertConfig{
		{
			Name: "multi-dest",
			Trigger: config.AlertTrigger{
				Type:      "error_count",
				Channel:   "ch1",
				Threshold: 1,
			},
			Destinations: []string{"webhook1", "webhook2", "slack"},
		},
	}
	am := NewAlertManager(alerts, m, send, slog.Default())
	m.Counter("messages_errored_total.ch1").Add(10)
	am.evaluate(context.Background())

	mu.Lock()
	got := make([]string, len(dests))
	copy(got, dests)
	mu.Unlock()

	if len(got) != 3 {
		t.Fatalf("expected 3 sends, got %d: %v", len(got), got)
	}
	want := map[string]bool{"webhook1": true, "webhook2": true, "slack": true}
	for _, d := range got {
		if !want[d] {
			t.Errorf("unexpected destination %s", d)
		}
	}
}

func TestAlertManager_SendFuncNil(t *testing.T) {
	m := observability.NewMetrics()
	alerts := []config.AlertConfig{
		{
			Name: "no-send",
			Trigger: config.AlertTrigger{
				Type:      "error_count",
				Channel:   "ch1",
				Threshold: 1,
			},
			Destinations: []string{"webhook"},
		},
	}
	am := NewAlertManager(alerts, m, nil, slog.Default())
	m.Counter("messages_errored_total.ch1").Add(10)
	am.evaluate(context.Background())
	// Should not panic when send is nil
}

func TestAlertManager_SendFuncReturnsError(t *testing.T) {
	m := observability.NewMetrics()
	send := func(ctx context.Context, dest string, payload []byte) error {
		return context.DeadlineExceeded
	}
	alerts := []config.AlertConfig{
		{
			Name: "send-err",
			Trigger: config.AlertTrigger{
				Type:      "error_count",
				Channel:   "ch1",
				Threshold: 1,
			},
			Destinations: []string{"webhook"},
		},
	}
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelError}))
	am := NewAlertManager(alerts, m, send, logger)
	m.Counter("messages_errored_total.ch1").Add(10)
	am.evaluate(context.Background())
	if !strings.Contains(buf.String(), "send alert failed") {
		t.Errorf("expected error log when send fails, got %s", buf.String())
	}
}

func TestAlertManager_EmptyDestinations(t *testing.T) {
	m := observability.NewMetrics()
	var sendCount atomic.Int32
	send := func(ctx context.Context, dest string, payload []byte) error {
		sendCount.Add(1)
		return nil
	}
	alerts := []config.AlertConfig{
		{
			Name: "no-dests",
			Trigger: config.AlertTrigger{
				Type:      "error_count",
				Channel:   "ch1",
				Threshold: 1,
			},
			Destinations: []string{},
		},
	}
	am := NewAlertManager(alerts, m, send, slog.Default())
	m.Counter("messages_errored_total.ch1").Add(10)
	am.evaluate(context.Background())
	if sendCount.Load() != 0 {
		t.Errorf("expected 0 sends with empty destinations, got %d", sendCount.Load())
	}
}

func TestAlertEvent_JSONRoundtrip(t *testing.T) {
	ev := AlertEvent{
		Name:      "test",
		Trigger:   "error_count",
		Channel:   "ch1",
		Value:     42,
		Threshold: 10,
		Timestamp: time.Now(),
		Message:   "test message",
	}
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded AlertEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Name != ev.Name || decoded.Trigger != ev.Trigger || decoded.Channel != ev.Channel ||
		decoded.Value != ev.Value || decoded.Threshold != ev.Threshold || decoded.Message != ev.Message {
		t.Errorf("roundtrip mismatch: got %+v", decoded)
	}
}
