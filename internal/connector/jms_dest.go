package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// JMSDest sends messages to a JMS provider via its HTTP/REST bridge API.
// JMS providers like ActiveMQ, IBM MQ, and others expose REST endpoints
// for message enqueue operations.
type JMSDest struct {
	name   string
	cfg    *config.JMSDestMapConfig
	client *http.Client
	logger *slog.Logger
}

func NewJMSDest(name string, cfg *config.JMSDestMapConfig, logger *slog.Logger) *JMSDest {
	timeout := 30 * time.Second
	if cfg.TimeoutMs > 0 {
		timeout = time.Duration(cfg.TimeoutMs) * time.Millisecond
	}
	return &JMSDest{
		name:   name,
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
		logger: logger,
	}
}

func (j *JMSDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	if j.cfg.URL == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("jms destination %s: URL not configured", j.name)}, nil
	}
	if j.cfg.Queue == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("jms destination %s: queue not configured", j.name)}, nil
	}

	endpoint := j.buildEndpoint()

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(msg.Raw))
	if err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("jms create request: %w", err)}, nil
	}

	contentType := "application/octet-stream"
	switch msg.ContentType {
	case message.ContentTypeJSON:
		contentType = "application/json"
	case message.ContentTypeXML:
		contentType = "text/xml"
	case message.ContentTypeHL7v2:
		contentType = "x-application/hl7-v2+er7"
	}
	req.Header.Set("Content-Type", contentType)

	if j.cfg.Auth != nil {
		j.applyAuth(req)
	}

	finalHeaders := make(map[string]string, len(req.Header))
	for k, v := range req.Header {
		if len(v) > 0 {
			finalHeaders[k] = v[0]
		}
	}
	msg.ClearTransportMeta()
	msg.Transport = "jms"
	msg.HTTP = &message.HTTPMeta{
		Headers: finalHeaders,
		Method:  "POST",
	}

	resp, err := j.client.Do(req)
	if err != nil {
		j.logger.Error("jms dest send failed", "destination", j.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("jms send: %w", err)}, nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	headers := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	if resp.StatusCode >= 400 {
		j.logger.Error("jms dest received error response",
			"destination", j.name,
			"status", resp.StatusCode,
			"body", string(body),
		)
		return &message.Response{
			StatusCode: resp.StatusCode,
			Body:       body,
			Headers:    headers,
			Error:      fmt.Errorf("jms server returned %d", resp.StatusCode),
		}, nil
	}

	j.logger.Debug("jms dest message sent",
		"destination", j.name,
		"queue", j.cfg.Queue,
		"messageId", msg.ID,
	)

	return &message.Response{StatusCode: resp.StatusCode, Body: body, Headers: headers}, nil
}

func (j *JMSDest) buildEndpoint() string {
	switch j.cfg.Provider {
	case "activemq":
		return fmt.Sprintf("%s/api/message/%s?type=queue", j.cfg.URL, j.cfg.Queue)
	default:
		return fmt.Sprintf("%s/api/message/%s?type=queue", j.cfg.URL, j.cfg.Queue)
	}
}

func (j *JMSDest) applyAuth(req *http.Request) {
	if j.cfg.Auth == nil {
		return
	}
	switch j.cfg.Auth.Type {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+j.cfg.Auth.Token)
	case "basic":
		req.SetBasicAuth(j.cfg.Auth.Username, j.cfg.Auth.Password)
	case "api_key":
		if j.cfg.Auth.Header != "" {
			req.Header.Set(j.cfg.Auth.Header, j.cfg.Auth.Key)
		}
	}
}

func (j *JMSDest) Stop(ctx context.Context) error {
	j.client.CloseIdleConnections()
	return nil
}

func (j *JMSDest) Type() string {
	return "jms"
}
