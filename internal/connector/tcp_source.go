package connector

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

const (
	mllpStartBlock = 0x0B
	mllpEndBlock   = 0x1C
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
	t.listener = ln

	ctx, t.cancel = context.WithCancel(ctx)

	t.logger.Info("TCP listener started", "addr", addr, "mode", t.cfg.Mode)

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
		if err := handler(ctx, msg); err != nil {
			t.logger.Error("handler error", "error", err)
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
