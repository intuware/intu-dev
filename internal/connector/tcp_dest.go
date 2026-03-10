package connector

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type TCPDest struct {
	name   string
	cfg    *config.TCPDestMapConfig
	logger *slog.Logger

	mu   sync.Mutex
	conn net.Conn
}

func NewTCPDest(name string, cfg *config.TCPDestMapConfig, logger *slog.Logger) *TCPDest {
	return &TCPDest{name: name, cfg: cfg, logger: logger}
}

func (t *TCPDest) dial() (net.Conn, error) {
	addr := net.JoinHostPort(t.cfg.Host, fmt.Sprintf("%d", t.cfg.Port))
	timeout := 30 * time.Second
	if t.cfg.TimeoutMs > 0 {
		timeout = time.Duration(t.cfg.TimeoutMs) * time.Millisecond
	}

	if t.cfg.TLS != nil && t.cfg.TLS.Enabled {
		tlsCfg, err := auth.BuildTLSConfigFromMap(t.cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("tcp dest TLS config: %w", err)
		}
		return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, tlsCfg)
	}
	return net.DialTimeout("tcp", addr, timeout)
}

func (t *TCPDest) getConn() (net.Conn, error) {
	if !t.cfg.KeepAlive {
		return t.dial()
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.conn != nil {
		t.conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		if _, err := t.conn.Write(nil); err == nil {
			return t.conn, nil
		}
		t.conn.Close()
		t.conn = nil
	}

	conn, err := t.dial()
	if err != nil {
		return nil, err
	}
	t.conn = conn
	return conn, nil
}

func (t *TCPDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	conn, err := t.getConn()
	if err != nil {
		t.logger.Error("tcp dest connect failed", "destination", t.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("tcp connect to %s:%d: %w", t.cfg.Host, t.cfg.Port, err)}, nil
	}
	if !t.cfg.KeepAlive {
		defer conn.Close()
	}

	writeTimeout := 30 * time.Second
	if t.cfg.TimeoutMs > 0 {
		writeTimeout = time.Duration(t.cfg.TimeoutMs) * time.Millisecond
	}
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))

	var payload []byte
	if t.cfg.Mode == "mllp" {
		var buf bytes.Buffer
		buf.WriteByte(0x0B)
		buf.Write(msg.Raw)
		buf.WriteByte(0x1C)
		buf.WriteByte(0x0D)
		payload = buf.Bytes()
	} else {
		payload = append(msg.Raw, '\n')
	}

	if _, err := conn.Write(payload); err != nil {
		if t.cfg.KeepAlive {
			t.mu.Lock()
			t.conn = nil
			t.mu.Unlock()
		}
		t.logger.Error("tcp dest write failed", "destination", t.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("tcp write: %w", err)}, nil
	}

	if t.cfg.Mode == "mllp" {
		conn.SetReadDeadline(time.Now().Add(writeTimeout))
		ackData, err := t.readMLLPResponse(conn)
		if err != nil {
			t.logger.Warn("tcp dest MLLP ack read failed", "destination", t.name, "error", err)
			return &message.Response{StatusCode: 200, Body: []byte(`{"status":"sent_no_ack"}`)}, nil
		}

		ackCode := extractHL7AckCode(ackData)
		if ackCode == "AE" || ackCode == "AR" || ackCode == "CR" || ackCode == "CE" {
			t.logger.Warn("tcp dest received NACK", "destination", t.name, "ack_code", ackCode)
			return &message.Response{
				StatusCode: 422,
				Body:       ackData,
				Error:      fmt.Errorf("MLLP NACK received: %s", ackCode),
			}, nil
		}

		return &message.Response{StatusCode: 200, Body: ackData}, nil
	}

	t.logger.Debug("tcp dest message sent", "destination", t.name, "bytes", len(payload))
	return &message.Response{StatusCode: 200, Body: []byte(`{"status":"sent"}`)}, nil
}

func (t *TCPDest) readMLLPResponse(conn net.Conn) ([]byte, error) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 1)
	started := false

	for {
		if _, err := io.ReadFull(conn, tmp); err != nil {
			return nil, err
		}
		b := tmp[0]

		if !started {
			if b == 0x0B {
				started = true
			}
			continue
		}

		if b == 0x1C {
			if _, err := io.ReadFull(conn, tmp); err == nil && tmp[0] == 0x0D {
				return buf, nil
			}
			return buf, nil
		}
		buf = append(buf, b)
	}
}

func extractHL7AckCode(data []byte) string {
	lines := bytes.Split(data, []byte("\r"))
	for _, line := range lines {
		if bytes.HasPrefix(line, []byte("MSA")) {
			fields := bytes.Split(line, []byte("|"))
			if len(fields) > 1 {
				return string(fields[1])
			}
		}
	}
	return ""
}

func (t *TCPDest) Stop(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
	return nil
}

func (t *TCPDest) Type() string {
	return "tcp"
}
