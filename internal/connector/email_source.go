package connector

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type EmailSource struct {
	cfg    *config.EmailListener
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewEmailSource(cfg *config.EmailListener, logger *slog.Logger) *EmailSource {
	return &EmailSource{cfg: cfg, logger: logger}
}

func (e *EmailSource) Start(ctx context.Context, handler MessageHandler) error {
	interval := 1 * time.Second
	if e.cfg.PollInterval != "" {
		if d, err := time.ParseDuration(e.cfg.PollInterval); err == nil {
			interval = d
		}
	}

	ctx, e.cancel = context.WithCancel(ctx)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := e.poll(ctx, handler); err != nil {
					e.logger.Error("email poll error", "error", err)
				}
			}
		}
	}()

	protocol := e.cfg.Protocol
	if protocol == "" {
		protocol = "imap"
	}

	e.logger.Info("email source started",
		"host", e.cfg.Host,
		"port", e.cfg.Port,
		"protocol", protocol,
		"folder", e.cfg.Folder,
		"poll_interval", interval.String(),
	)
	return nil
}

func (e *EmailSource) poll(ctx context.Context, handler MessageHandler) error {
	protocol := strings.ToLower(e.cfg.Protocol)
	if protocol == "" {
		protocol = "imap"
	}

	switch protocol {
	case "imap":
		return e.pollIMAP(ctx, handler)
	case "pop3":
		return e.pollPOP3(ctx, handler)
	default:
		return fmt.Errorf("unsupported email protocol: %s", protocol)
	}
}

func (e *EmailSource) pollIMAP(ctx context.Context, handler MessageHandler) error {
	conn, reader, err := e.dialIMAP()
	if err != nil {
		return err
	}
	defer conn.Close()

	greeting, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read IMAP greeting: %w", err)
	}
	if !strings.Contains(greeting, "OK") {
		return fmt.Errorf("unexpected IMAP greeting: %s", strings.TrimSpace(greeting))
	}

	if e.cfg.Auth != nil && e.cfg.Auth.Username != "" {
		loginCmd := fmt.Sprintf("A001 LOGIN %s %s\r\n", e.cfg.Auth.Username, e.cfg.Auth.Password)
		if _, err := conn.Write([]byte(loginCmd)); err != nil {
			return fmt.Errorf("IMAP login write: %w", err)
		}
		loginResp, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("IMAP login read: %w", err)
		}
		if !strings.Contains(loginResp, "OK") {
			return fmt.Errorf("IMAP login failed: %s", strings.TrimSpace(loginResp))
		}
	}

	folder := e.cfg.Folder
	if folder == "" {
		folder = "INBOX"
	}
	selectCmd := fmt.Sprintf("A002 SELECT %s\r\n", folder)
	if _, err := conn.Write([]byte(selectCmd)); err != nil {
		return fmt.Errorf("IMAP SELECT write: %w", err)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("IMAP SELECT read: %w", err)
		}
		if strings.HasPrefix(line, "A002 ") {
			if !strings.Contains(line, "OK") {
				return fmt.Errorf("IMAP SELECT failed: %s", strings.TrimSpace(line))
			}
			break
		}
	}

	searchCriteria := "UNSEEN"
	if e.cfg.Filter != "" {
		searchCriteria = e.cfg.Filter
	}
	searchCmd := fmt.Sprintf("A003 SEARCH %s\r\n", searchCriteria)
	if _, err := conn.Write([]byte(searchCmd)); err != nil {
		return fmt.Errorf("IMAP SEARCH write: %w", err)
	}

	var messageNums []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("IMAP SEARCH read: %w", err)
		}
		if strings.HasPrefix(line, "* SEARCH") {
			parts := strings.Fields(strings.TrimPrefix(line, "* SEARCH"))
			messageNums = append(messageNums, parts...)
		}
		if strings.HasPrefix(line, "A003 ") {
			break
		}
	}

	for _, num := range messageNums {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetchCmd := fmt.Sprintf("A004 FETCH %s BODY[]\r\n", num)
		if _, err := conn.Write([]byte(fetchCmd)); err != nil {
			e.logger.Error("IMAP FETCH write error", "num", num, "error", err)
			continue
		}

		var body strings.Builder
		inBody := false
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			if strings.HasPrefix(line, "A004 ") {
				break
			}
			if strings.Contains(line, "BODY[]") {
				inBody = true
				continue
			}
			if inBody {
				if strings.TrimSpace(line) == ")" {
					break
				}
				body.WriteString(line)
			}
		}

		if body.Len() == 0 {
			continue
		}

		msg := message.New("", []byte(body.String()))
		msg.Metadata["source"] = "email"
		msg.Metadata["protocol"] = "imap"
		msg.Metadata["folder"] = folder
		msg.Metadata["message_num"] = num

		if err := handler(ctx, msg); err != nil {
			e.logger.Error("email handler error", "num", num, "error", err)
			continue
		}

		if e.cfg.DeleteAfterRead {
			storeCmd := fmt.Sprintf("A005 STORE %s +FLAGS (\\Deleted)\r\n", num)
			conn.Write([]byte(storeCmd))
			reader.ReadString('\n')
		}
	}

	logoutCmd := "A099 LOGOUT\r\n"
	conn.Write([]byte(logoutCmd))

	return nil
}

func (e *EmailSource) pollPOP3(ctx context.Context, handler MessageHandler) error {
	conn, reader, err := e.dialPOP3()
	if err != nil {
		return err
	}
	defer conn.Close()

	greeting, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("POP3 greeting: %w", err)
	}
	if !strings.HasPrefix(greeting, "+OK") {
		return fmt.Errorf("unexpected POP3 greeting: %s", strings.TrimSpace(greeting))
	}

	if e.cfg.Auth != nil && e.cfg.Auth.Username != "" {
		if _, err := fmt.Fprintf(conn, "USER %s\r\n", e.cfg.Auth.Username); err != nil {
			return fmt.Errorf("POP3 USER: %w", err)
		}
		resp, _ := reader.ReadString('\n')
		if !strings.HasPrefix(resp, "+OK") {
			return fmt.Errorf("POP3 USER failed: %s", strings.TrimSpace(resp))
		}

		if _, err := fmt.Fprintf(conn, "PASS %s\r\n", e.cfg.Auth.Password); err != nil {
			return fmt.Errorf("POP3 PASS: %w", err)
		}
		resp, _ = reader.ReadString('\n')
		if !strings.HasPrefix(resp, "+OK") {
			return fmt.Errorf("POP3 PASS failed: %s", strings.TrimSpace(resp))
		}
	}

	if _, err := fmt.Fprintf(conn, "LIST\r\n"); err != nil {
		return fmt.Errorf("POP3 LIST: %w", err)
	}

	var msgNums []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if line == "." {
			break
		}
		if strings.HasPrefix(line, "+OK") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 1 {
			msgNums = append(msgNums, parts[0])
		}
	}

	for _, num := range msgNums {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if _, err := fmt.Fprintf(conn, "RETR %s\r\n", num); err != nil {
			continue
		}

		resp, _ := reader.ReadString('\n')
		if !strings.HasPrefix(resp, "+OK") {
			continue
		}

		var body strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			if strings.TrimSpace(line) == "." {
				break
			}
			body.WriteString(line)
		}

		if body.Len() == 0 {
			continue
		}

		msg := message.New("", []byte(body.String()))
		msg.Metadata["source"] = "email"
		msg.Metadata["protocol"] = "pop3"
		msg.Metadata["message_num"] = num

		if err := handler(ctx, msg); err != nil {
			e.logger.Error("email handler error", "num", num, "error", err)
			continue
		}

		if e.cfg.DeleteAfterRead {
			fmt.Fprintf(conn, "DELE %s\r\n", num)
			reader.ReadString('\n')
		}
	}

	fmt.Fprintf(conn, "QUIT\r\n")
	return nil
}

func (e *EmailSource) dialWithTLS(addr string) (net.Conn, error) {
	if e.cfg.TLS != nil && e.cfg.TLS.Enabled {
		tlsCfg, err := auth.BuildTLSConfig(e.cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("email TLS config: %w", err)
		}
		return auth.DialTLS(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, tlsCfg)
	}
	return net.DialTimeout("tcp", addr, 10*time.Second)
}

func (e *EmailSource) dialIMAP() (net.Conn, *bufio.Reader, error) {
	port := e.cfg.Port
	if port == 0 {
		if e.cfg.TLS != nil && e.cfg.TLS.Enabled {
			port = 993
		} else {
			port = 143
		}
	}
	addr := fmt.Sprintf("%s:%d", e.cfg.Host, port)

	conn, err := e.dialWithTLS(addr)
	if err != nil {
		return nil, nil, fmt.Errorf("dial IMAP %s: %w", addr, err)
	}

	return conn, bufio.NewReader(conn), nil
}

func (e *EmailSource) dialPOP3() (net.Conn, *bufio.Reader, error) {
	port := e.cfg.Port
	if port == 0 {
		if e.cfg.TLS != nil && e.cfg.TLS.Enabled {
			port = 995
		} else {
			port = 110
		}
	}
	addr := fmt.Sprintf("%s:%d", e.cfg.Host, port)

	conn, err := e.dialWithTLS(addr)
	if err != nil {
		return nil, nil, fmt.Errorf("dial POP3 %s: %w", addr, err)
	}

	return conn, bufio.NewReader(conn), nil
}

func (e *EmailSource) Stop(ctx context.Context) error {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	return nil
}

func (e *EmailSource) Type() string {
	protocol := e.cfg.Protocol
	if protocol == "" {
		protocol = "imap"
	}
	return "email/" + protocol
}
