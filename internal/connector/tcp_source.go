package connector

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

const (
	mllpStartBlock     = 0x0B
	mllpEndBlock       = 0x1C
	mllpCarriageReturn = 0x0D
)

type TCPSource struct {
	cfg      *config.TCPListener
	listener net.Listener
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewTCPSource(cfg *config.TCPListener, logger *slog.Logger) *TCPSource {
	return &TCPSource{cfg: cfg, logger: logger}
}

func (t *TCPSource) Start(ctx context.Context, handler MessageHandler) error {
	addr := ":" + strconv.Itoa(t.cfg.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	tlsEnabled := false
	if t.cfg.TLS != nil && t.cfg.TLS.Enabled {
		tlsCfg, tlsErr := auth.BuildTLSConfig(t.cfg.TLS)
		if tlsErr != nil {
			ln.Close()
			return fmt.Errorf("TCP TLS config: %w", tlsErr)
		}
		ln = tls.NewListener(ln, tlsCfg)
		tlsEnabled = true
	}

	t.listener = ln
	ctx, t.cancel = context.WithCancel(ctx)

	t.logger.Info("TCP listener started", "addr", addr, "mode", t.cfg.Mode, "tls", tlsEnabled)

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					t.logger.Error("accept error", "error", err)
					continue
				}
			}
			t.wg.Add(1)
			go func(c net.Conn) {
				defer t.wg.Done()
				defer c.Close()
				t.handleConn(ctx, c, handler)
			}(conn)
		}
	}()

	return nil
}

func (t *TCPSource) handleConn(ctx context.Context, conn net.Conn, handler MessageHandler) {
	timeout := 30 * time.Second
	if t.cfg.TimeoutMs > 0 {
		timeout = time.Duration(t.cfg.TimeoutMs) * time.Millisecond
	}
	conn.SetDeadline(time.Now().Add(timeout))

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var data []byte
		var err error

		if t.cfg.Mode == "mllp" {
			data, err = readMLLP(reader)
		} else {
			data, err = readRawTCP(reader)
		}

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return
			}
			if err.Error() == "EOF" {
				return
			}
			t.logger.Error("read error", "error", err)
			return
		}

		if len(data) == 0 {
			continue
		}

		msg := message.New("", data)
		msg.Transport = "tcp"
		msg.TCP = &message.TCPMeta{
			RemoteAddr: conn.RemoteAddr().String(),
		}
		handlerErr := handler(ctx, msg)
		if handlerErr != nil {
			t.logger.Error("handler error", "error", handlerErr)
		}

		if t.cfg.Mode == "mllp" && t.cfg.ACK != nil && t.cfg.ACK.Auto {
			ack := t.buildMLLPACK(data, handlerErr)
			if ack != nil {
				conn.Write(ack)
			}
		}

		conn.SetDeadline(time.Now().Add(timeout))
	}
}

func readMLLP(reader *bufio.Reader) ([]byte, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != mllpStartBlock {
		return nil, fmt.Errorf("expected MLLP start block (0x0B), got 0x%02X", b)
	}

	var buf bytes.Buffer
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == mllpEndBlock {
			next, err := reader.ReadByte()
			if err != nil {
				return buf.Bytes(), nil
			}
			if next == mllpCarriageReturn {
				return buf.Bytes(), nil
			}
			buf.WriteByte(b)
			buf.WriteByte(next)
			continue
		}
		buf.WriteByte(b)
	}
}

func readRawTCP(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(line), nil
}

func (t *TCPSource) buildMLLPACK(msgData []byte, handlerErr error) []byte {
	ackCode := "AA"
	if t.cfg.ACK.SuccessCode != "" {
		ackCode = t.cfg.ACK.SuccessCode
	}
	if handlerErr != nil {
		ackCode = "AE"
		if t.cfg.ACK.ErrorCode != "" {
			ackCode = t.cfg.ACK.ErrorCode
		}
	}

	controlID := extractHL7ControlID(msgData)

	ack := fmt.Sprintf("MSH|^~\\&|INTU|INTU|||||ACK||P|2.5\rMSA|%s|%s\r", ackCode, controlID)

	var buf bytes.Buffer
	buf.WriteByte(mllpStartBlock)
	buf.WriteString(ack)
	buf.WriteByte(mllpEndBlock)
	buf.WriteByte(mllpCarriageReturn)
	return buf.Bytes()
}

func extractHL7ControlID(data []byte) string {
	lines := bytes.Split(data, []byte("\r"))
	for _, line := range lines {
		if bytes.HasPrefix(line, []byte("MSH")) {
			fields := bytes.Split(line, []byte("|"))
			if len(fields) > 9 {
				return string(fields[9])
			}
		}
	}
	return "0"
}

func (t *TCPSource) Addr() string {
	if t.listener != nil {
		return t.listener.Addr().String()
	}
	return ""
}

func (t *TCPSource) Stop(ctx context.Context) error {
	if t.cancel != nil {
		t.cancel()
	}
	if t.listener != nil {
		t.listener.Close()
	}
	t.wg.Wait()
	return nil
}

func (t *TCPSource) Type() string {
	return "tcp"
}
