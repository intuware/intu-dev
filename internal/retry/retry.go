package retry

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type SendFunc func(ctx context.Context, msg *message.Message) (*message.Response, error)

type Retryer struct {
	cfg    *config.RetryConfig
	logger *slog.Logger
}

func NewRetryer(cfg *config.RetryConfig, logger *slog.Logger) *Retryer {
	if cfg == nil {
		cfg = &config.RetryConfig{MaxAttempts: 1}
	}
	return &Retryer{cfg: cfg, logger: logger}
}

func (r *Retryer) Execute(ctx context.Context, msg *message.Message, send SendFunc) (*message.Response, error) {
	maxAttempts := r.cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	var lastResp *message.Response

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err := send(ctx, msg)

		if err == nil && resp != nil && resp.Error == nil {
			if resp.StatusCode > 0 && resp.StatusCode < 400 {
				return resp, nil
			}
			if r.shouldRetry(resp, err) {
				lastResp = resp
				lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			} else {
				return resp, nil
			}
		} else {
			lastErr = err
			if resp != nil {
				lastResp = resp
			}

			if err != nil && !r.shouldRetryError(err) {
				return resp, err
			}
			if resp != nil && resp.Error != nil && !r.shouldRetryError(resp.Error) {
				return resp, nil
			}
		}

		if attempt < maxAttempts {
			delay := r.calculateDelay(attempt)
			r.logger.Debug("retrying", "attempt", attempt, "maxAttempts", maxAttempts, "delay", delay)

			select {
			case <-ctx.Done():
				return lastResp, ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return lastResp, fmt.Errorf("max retries exhausted after %d attempts: %w", maxAttempts, lastErr)
}

func (r *Retryer) calculateDelay(attempt int) time.Duration {
	initial := time.Duration(r.cfg.InitialDelayMs) * time.Millisecond
	if initial == 0 {
		initial = 500 * time.Millisecond
	}

	maxDelay := time.Duration(r.cfg.MaxDelayMs) * time.Millisecond
	if maxDelay == 0 {
		maxDelay = 30 * time.Second
	}

	var delay time.Duration
	switch r.cfg.Backoff {
	case "linear":
		delay = initial * time.Duration(attempt)
	case "exponential":
		delay = initial * time.Duration(math.Pow(2, float64(attempt-1)))
	default:
		delay = initial
	}

	if delay > maxDelay {
		delay = maxDelay
	}

	if r.cfg.Jitter {
		jitter := time.Duration(rand.Int63n(int64(delay / 4)))
		delay += jitter
	}

	return delay
}

func (r *Retryer) shouldRetry(resp *message.Response, err error) bool {
	if err != nil {
		return r.shouldRetryError(err)
	}

	if resp == nil {
		return false
	}

	for _, noRetry := range r.cfg.NoRetryOn {
		if noRetry == "status_4xx" && resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return false
		}
		if noRetry == fmt.Sprintf("status_%d", resp.StatusCode) {
			return false
		}
	}

	for _, retryOn := range r.cfg.RetryOn {
		if retryOn == "status_5xx" && resp.StatusCode >= 500 {
			return true
		}
		if retryOn == fmt.Sprintf("status_%d", resp.StatusCode) {
			return true
		}
	}

	if resp.StatusCode >= 500 {
		return true
	}

	return false
}

func (r *Retryer) shouldRetryError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	for _, noRetry := range r.cfg.NoRetryOn {
		if strings.Contains(errStr, noRetry) {
			return false
		}
	}

	for _, retryOn := range r.cfg.RetryOn {
		switch retryOn {
		case "timeout":
			if isTimeout(err) {
				return true
			}
		case "connection_refused":
			if strings.Contains(errStr, "connection refused") {
				return true
			}
		default:
			if strings.Contains(errStr, retryOn) {
				return true
			}
		}
	}

	if isTimeout(err) || strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "connection reset") {
		return true
	}

	return true
}

func isTimeout(err error) bool {
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}
	return strings.Contains(err.Error(), "timeout")
}
