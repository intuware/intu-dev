package retry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

func testMsg() *message.Message {
	return &message.Message{
		ID:            "test-msg-1",
		CorrelationID: "corr-1",
		ChannelID:     "ch-1",
		Raw:           []byte("hello"),
		Timestamp:     time.Now(),
		Metadata:      map[string]any{},
	}
}

func TestNewRetryerNilConfig(t *testing.T) {
	r := NewRetryer(nil, slog.Default())
	if r == nil {
		t.Fatal("expected non-nil retryer")
	}
	if r.cfg.MaxAttempts != 1 {
		t.Fatalf("expected MaxAttempts=1 for nil config, got %d", r.cfg.MaxAttempts)
	}
}

func TestNewRetryerCustomConfig(t *testing.T) {
	cfg := &config.RetryConfig{
		MaxAttempts:    5,
		Backoff:        "exponential",
		InitialDelayMs: 100,
		MaxDelayMs:     5000,
		Jitter:         true,
		RetryOn:        []string{"timeout"},
		NoRetryOn:      []string{"status_4xx"},
	}
	r := NewRetryer(cfg, slog.Default())
	if r.cfg.MaxAttempts != 5 {
		t.Fatalf("expected MaxAttempts=5, got %d", r.cfg.MaxAttempts)
	}
	if r.cfg.Backoff != "exponential" {
		t.Fatalf("expected backoff=exponential, got %s", r.cfg.Backoff)
	}
}

func TestExecuteSuccess(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{MaxAttempts: 3}, slog.Default())
	calls := 0
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		calls++
		return &message.Response{StatusCode: 200}, nil
	}

	resp, err := r.Execute(context.Background(), testMsg(), send)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestExecuteRetryOnError(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{MaxAttempts: 3, InitialDelayMs: 1}, slog.Default())
	calls := 0
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		calls++
		if calls < 3 {
			return nil, fmt.Errorf("connection refused")
		}
		return &message.Response{StatusCode: 200}, nil
	}

	resp, err := r.Execute(context.Background(), testMsg(), send)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestExecuteRetryOnHTTP5xx(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{MaxAttempts: 3, InitialDelayMs: 1}, slog.Default())
	calls := 0
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		calls++
		if calls < 3 {
			return &message.Response{StatusCode: 503}, nil
		}
		return &message.Response{StatusCode: 200}, nil
	}

	resp, err := r.Execute(context.Background(), testMsg(), send)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestExecuteContextCancellation(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{MaxAttempts: 5, InitialDelayMs: 500}, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		calls++
		if calls == 1 {
			cancel()
		}
		return nil, fmt.Errorf("connection refused")
	}

	_, err := r.Execute(ctx, testMsg(), send)
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestExecuteMaxRetriesExhausted(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{MaxAttempts: 3, InitialDelayMs: 1}, slog.Default())
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return nil, fmt.Errorf("connection refused")
	}

	_, err := r.Execute(context.Background(), testMsg(), send)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	expected := "max retries exhausted after 3 attempts"
	if !containsStr(err.Error(), expected) {
		t.Fatalf("expected error containing %q, got %q", expected, err.Error())
	}
}

func TestExecuteNoRetryOnStatus4xx(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		MaxAttempts:    3,
		InitialDelayMs: 1,
		NoRetryOn:      []string{"status_4xx"},
	}, slog.Default())
	calls := 0
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		calls++
		return &message.Response{StatusCode: 404}, nil
	}

	resp, err := r.Execute(context.Background(), testMsg(), send)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 404 {
		t.Fatalf("expected status 404, got %d", resp.StatusCode)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call (no retry on 4xx), got %d", calls)
	}
}

func TestExecuteRetryOnStatus5xx(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		MaxAttempts:    3,
		InitialDelayMs: 1,
		RetryOn:        []string{"status_5xx"},
	}, slog.Default())
	calls := 0
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		calls++
		if calls < 3 {
			return &message.Response{StatusCode: 500}, nil
		}
		return &message.Response{StatusCode: 200}, nil
	}

	resp, err := r.Execute(context.Background(), testMsg(), send)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestExecuteRetryOnSpecificStatusCode(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		MaxAttempts:    3,
		InitialDelayMs: 1,
		RetryOn:        []string{"status_502"},
	}, slog.Default())
	calls := 0
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		calls++
		if calls < 2 {
			return &message.Response{StatusCode: 502}, nil
		}
		return &message.Response{StatusCode: 200}, nil
	}

	resp, err := r.Execute(context.Background(), testMsg(), send)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestExecuteNoRetryOnSpecificStatusCode(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		MaxAttempts:    3,
		InitialDelayMs: 1,
		NoRetryOn:      []string{"status_503"},
	}, slog.Default())
	calls := 0
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		calls++
		return &message.Response{StatusCode: 503}, nil
	}

	resp, err := r.Execute(context.Background(), testMsg(), send)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 503 {
		t.Fatalf("expected status 503, got %d", resp.StatusCode)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call (no retry on 503), got %d", calls)
	}
}

// --- calculateDelay tests ---

func TestCalculateDelayLinear(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		Backoff:        "linear",
		InitialDelayMs: 100,
		MaxDelayMs:     10000,
	}, slog.Default())

	d1 := r.calculateDelay(1)
	d2 := r.calculateDelay(2)
	d3 := r.calculateDelay(3)

	if d1 != 100*time.Millisecond {
		t.Fatalf("expected 100ms for attempt 1, got %v", d1)
	}
	if d2 != 200*time.Millisecond {
		t.Fatalf("expected 200ms for attempt 2, got %v", d2)
	}
	if d3 != 300*time.Millisecond {
		t.Fatalf("expected 300ms for attempt 3, got %v", d3)
	}
}

func TestCalculateDelayExponential(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		Backoff:        "exponential",
		InitialDelayMs: 100,
		MaxDelayMs:     100000,
	}, slog.Default())

	d1 := r.calculateDelay(1)
	d2 := r.calculateDelay(2)
	d3 := r.calculateDelay(3)

	if d1 != 100*time.Millisecond {
		t.Fatalf("expected 100ms for attempt 1, got %v", d1)
	}
	if d2 != 200*time.Millisecond {
		t.Fatalf("expected 200ms for attempt 2, got %v", d2)
	}
	if d3 != 400*time.Millisecond {
		t.Fatalf("expected 400ms for attempt 3, got %v", d3)
	}
}

func TestCalculateDelayDefault(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		Backoff:        "",
		InitialDelayMs: 100,
		MaxDelayMs:     10000,
	}, slog.Default())

	d1 := r.calculateDelay(1)
	d2 := r.calculateDelay(2)

	if d1 != 100*time.Millisecond {
		t.Fatalf("expected 100ms for attempt 1, got %v", d1)
	}
	if d2 != 100*time.Millisecond {
		t.Fatalf("expected 100ms for attempt 2 (default=fixed), got %v", d2)
	}
}

func TestCalculateDelayJitter(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		Backoff:        "linear",
		InitialDelayMs: 1000,
		MaxDelayMs:     100000,
		Jitter:         true,
	}, slog.Default())

	delay := r.calculateDelay(1)
	if delay < 1000*time.Millisecond {
		t.Fatalf("delay with jitter should be >= base delay, got %v", delay)
	}
	if delay > 1250*time.Millisecond {
		t.Fatalf("delay with jitter should be <= base + 25%%, got %v", delay)
	}
}

func TestCalculateDelayMaxDelayCap(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		Backoff:        "exponential",
		InitialDelayMs: 1000,
		MaxDelayMs:     2000,
	}, slog.Default())

	delay := r.calculateDelay(10)
	if delay > 2000*time.Millisecond {
		t.Fatalf("delay should be capped at max_delay, got %v", delay)
	}
}

func TestCalculateDelayDefaultInitialAndMax(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		Backoff:        "linear",
		InitialDelayMs: 0,
		MaxDelayMs:     0,
	}, slog.Default())

	delay := r.calculateDelay(1)
	if delay != 500*time.Millisecond {
		t.Fatalf("expected default initial delay 500ms, got %v", delay)
	}
}

// --- shouldRetry tests ---

func TestShouldRetryWithError(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{}, slog.Default())
	if !r.shouldRetry(nil, fmt.Errorf("connection refused")) {
		t.Fatal("shouldRetry should return true for error")
	}
}

func TestShouldRetryNilResp(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{}, slog.Default())
	if r.shouldRetry(nil, nil) {
		t.Fatal("shouldRetry should return false for nil resp and nil err")
	}
}

func TestShouldRetryNoRetryOn(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		NoRetryOn: []string{"status_4xx"},
	}, slog.Default())
	resp := &message.Response{StatusCode: 404}
	if r.shouldRetry(resp, nil) {
		t.Fatal("shouldRetry should return false when status matches NoRetryOn")
	}
}

func TestShouldRetryRetryOn(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{
		RetryOn: []string{"status_5xx"},
	}, slog.Default())
	resp := &message.Response{StatusCode: 502}
	if !r.shouldRetry(resp, nil) {
		t.Fatal("shouldRetry should return true when status matches RetryOn")
	}
}

func TestShouldRetryDefault5xx(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{}, slog.Default())
	resp := &message.Response{StatusCode: 500}
	if !r.shouldRetry(resp, nil) {
		t.Fatal("shouldRetry should return true for 5xx by default")
	}
}

func TestShouldRetry4xxDefault(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{}, slog.Default())
	resp := &message.Response{StatusCode: 400}
	if r.shouldRetry(resp, nil) {
		t.Fatal("shouldRetry should return false for 4xx by default")
	}
}

// --- shouldRetryError tests ---

func TestShouldRetryErrorNilError(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{}, slog.Default())
	if r.shouldRetryError(nil) {
		t.Fatal("shouldRetryError should return false for nil error")
	}
}

type mockTimeoutErr struct{ timeout bool }

func (e *mockTimeoutErr) Error() string   { return "mock timeout error" }
func (e *mockTimeoutErr) Timeout() bool   { return e.timeout }
func (e *mockTimeoutErr) Temporary() bool { return false }

func TestShouldRetryErrorTimeout(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{RetryOn: []string{"timeout"}}, slog.Default())
	err := &mockTimeoutErr{timeout: true}
	if !r.shouldRetryError(err) {
		t.Fatal("shouldRetryError should return true for timeout error")
	}
}

func TestShouldRetryErrorConnectionRefused(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{RetryOn: []string{"connection_refused"}}, slog.Default())
	err := fmt.Errorf("dial tcp: connection refused")
	if !r.shouldRetryError(err) {
		t.Fatal("shouldRetryError should return true for connection refused")
	}
}

func TestShouldRetryErrorConnectionReset(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{}, slog.Default())
	err := fmt.Errorf("read: connection reset by peer")
	if !r.shouldRetryError(err) {
		t.Fatal("shouldRetryError should return true for connection reset")
	}
}

func TestShouldRetryErrorNoRetryOnMatch(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{NoRetryOn: []string{"auth_failed"}}, slog.Default())
	err := fmt.Errorf("auth_failed: invalid credentials")
	if r.shouldRetryError(err) {
		t.Fatal("shouldRetryError should return false when NoRetryOn matches error string")
	}
}

func TestShouldRetryErrorRetryOnMatch(t *testing.T) {
	r := NewRetryer(&config.RetryConfig{RetryOn: []string{"temporary_failure"}}, slog.Default())
	err := fmt.Errorf("temporary_failure: try again")
	if !r.shouldRetryError(err) {
		t.Fatal("shouldRetryError should return true when RetryOn matches error string")
	}
}

// --- isTimeout tests ---

func TestIsTimeoutNetError(t *testing.T) {
	err := &mockTimeoutErr{timeout: true}
	var _ net.Error = err
	if !isTimeout(err) {
		t.Fatal("isTimeout should return true for net.Error with Timeout()=true")
	}
}

func TestIsTimeoutNetErrorFalse(t *testing.T) {
	err := &mockTimeoutErr{timeout: false}
	if isTimeout(err) {
		t.Fatal("isTimeout should return false for net.Error with Timeout()=false")
	}
}

func TestIsTimeoutStringContains(t *testing.T) {
	err := fmt.Errorf("request timeout exceeded")
	if !isTimeout(err) {
		t.Fatal("isTimeout should return true when error string contains 'timeout'")
	}
}

func TestIsTimeoutNotTimeout(t *testing.T) {
	err := fmt.Errorf("connection refused")
	if isTimeout(err) {
		t.Fatal("isTimeout should return false for non-timeout error")
	}
}

// --- DLQ tests ---

func TestNewDeadLetterQueue(t *testing.T) {
	cfg := &config.DeadLetterConfig{Enabled: true}
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return &message.Response{StatusCode: 200}, nil
	}
	dlq := NewDeadLetterQueue(cfg, send, slog.Default())
	if dlq == nil {
		t.Fatal("expected non-nil DLQ")
	}
}

func TestDLQSendDisabled(t *testing.T) {
	cfg := &config.DeadLetterConfig{Enabled: false}
	called := false
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		called = true
		return &message.Response{StatusCode: 200}, nil
	}
	dlq := NewDeadLetterQueue(cfg, send, slog.Default())
	dlq.Send(context.Background(), testMsg(), "dest-1", fmt.Errorf("failed"))
	if called {
		t.Fatal("send func should not be called when DLQ is disabled")
	}
}

func TestDLQSendNilConfig(t *testing.T) {
	called := false
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		called = true
		return &message.Response{StatusCode: 200}, nil
	}
	dlq := NewDeadLetterQueue(nil, send, slog.Default())
	dlq.Send(context.Background(), testMsg(), "dest-1", fmt.Errorf("failed"))
	if called {
		t.Fatal("send func should not be called when config is nil")
	}
}

func TestDLQSendEnabled(t *testing.T) {
	cfg := &config.DeadLetterConfig{Enabled: true}
	var sentMsg *message.Message
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		sentMsg = msg
		return &message.Response{StatusCode: 200}, nil
	}
	dlq := NewDeadLetterQueue(cfg, send, slog.Default())
	dlq.Send(context.Background(), testMsg(), "dest-1", fmt.Errorf("some error"))

	if sentMsg == nil {
		t.Fatal("expected DLQ message to be sent")
	}
	if sentMsg.ID != "test-msg-1-dlq" {
		t.Fatalf("expected DLQ message ID 'test-msg-1-dlq', got %q", sentMsg.ID)
	}
	if sentMsg.HTTP == nil || sentMsg.HTTP.Headers["X-DLQ"] != "true" {
		t.Fatal("expected X-DLQ header")
	}
}

func TestDLQSendIncludeOriginal(t *testing.T) {
	cfg := &config.DeadLetterConfig{Enabled: true, IncludeOriginal: true}
	var sentMsg *message.Message
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		sentMsg = msg
		return &message.Response{StatusCode: 200}, nil
	}
	dlq := NewDeadLetterQueue(cfg, send, slog.Default())
	dlq.Send(context.Background(), testMsg(), "dest-1", fmt.Errorf("err"))

	if sentMsg == nil {
		t.Fatal("expected DLQ message to be sent")
	}
	if !containsStr(string(sentMsg.Raw), "hello") {
		t.Fatal("expected original payload in DLQ entry when IncludeOriginal is true")
	}
}

func TestDLQSendFuncError(t *testing.T) {
	cfg := &config.DeadLetterConfig{Enabled: true}
	send := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return nil, fmt.Errorf("send failed")
	}
	dlq := NewDeadLetterQueue(cfg, send, slog.Default())
	dlq.Send(context.Background(), testMsg(), "dest-1", fmt.Errorf("original err"))
}

func TestDLQSendNilSendFunc(t *testing.T) {
	cfg := &config.DeadLetterConfig{Enabled: true}
	dlq := NewDeadLetterQueue(cfg, nil, slog.Default())
	dlq.Send(context.Background(), testMsg(), "dest-1", fmt.Errorf("original err"))
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
