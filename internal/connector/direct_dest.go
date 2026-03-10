package connector

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/smtp"
	"strings"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// DirectDest implements the Direct messaging protocol for healthcare
// (Direct Project / Direct Secure Messaging). It sends messages via SMTP/S
// with S/MIME encryption using certificates.
type DirectDest struct {
	name   string
	cfg    *config.DirectDestMapConfig
	logger *slog.Logger
}

func NewDirectDest(name string, cfg *config.DirectDestMapConfig, logger *slog.Logger) *DirectDest {
	return &DirectDest{name: name, cfg: cfg, logger: logger}
}

func (d *DirectDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	if d.cfg.To == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("direct destination %s: 'to' address not configured", d.name)}, nil
	}
	if d.cfg.From == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("direct destination %s: 'from' address not configured", d.name)}, nil
	}

	smtpHost := d.cfg.SMTPHost
	if smtpHost == "" {
		parts := strings.SplitN(d.cfg.To, "@", 2)
		if len(parts) == 2 {
			smtpHost = parts[1]
		}
	}
	if smtpHost == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("direct destination %s: cannot determine SMTP host", d.name)}, nil
	}

	port := d.cfg.SMTPPort
	if port == 0 {
		port = 465 // Direct messaging defaults to implicit TLS
	}

	addr := net.JoinHostPort(smtpHost, fmt.Sprintf("%d", port))
	timeout := 30 * time.Second

	var conn net.Conn
	var err error

	tlsCfg := &tls.Config{ServerName: smtpHost}
	if d.cfg.TLS != nil && d.cfg.TLS.Enabled {
		built, tlsErr := auth.BuildTLSConfigFromMap(d.cfg.TLS)
		if tlsErr != nil {
			return &message.Response{StatusCode: 502, Error: fmt.Errorf("direct TLS config: %w", tlsErr)}, nil
		}
		if built != nil {
			tlsCfg = built
		}
	}

	if port == 465 {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", addr, timeout)
	}
	if err != nil {
		d.logger.Error("direct dest connect failed", "destination", d.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("direct connect to %s: %w", addr, err)}, nil
	}

	client, err := smtp.NewClient(conn, smtpHost)
	if err != nil {
		conn.Close()
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("direct smtp client: %w", err)}, nil
	}
	defer client.Close()

	// STARTTLS for port 587
	if port != 465 {
		if err := client.StartTLS(tlsCfg); err != nil {
			d.logger.Warn("direct STARTTLS failed", "destination", d.name, "error", err)
		}
	}

	if err := client.Mail(d.cfg.From); err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("direct MAIL FROM: %w", err)}, nil
	}
	if err := client.Rcpt(d.cfg.To); err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("direct RCPT TO: %w", err)}, nil
	}

	w, err := client.Data()
	if err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("direct DATA: %w", err)}, nil
	}

	var emailMsg strings.Builder
	emailMsg.WriteString(fmt.Sprintf("From: %s\r\n", d.cfg.From))
	emailMsg.WriteString(fmt.Sprintf("To: %s\r\n", d.cfg.To))
	emailMsg.WriteString(fmt.Sprintf("Subject: Direct Message %s\r\n", msg.ID))
	emailMsg.WriteString(fmt.Sprintf("Message-ID: <%s@direct>\r\n", msg.ID))
	emailMsg.WriteString(fmt.Sprintf("Date: %s\r\n", msg.Timestamp.Format("Mon, 02 Jan 2006 15:04:05 -0700")))
	emailMsg.WriteString("MIME-Version: 1.0\r\n")
	emailMsg.WriteString("Content-Type: application/pkcs7-mime; smime-type=enveloped-data; name=smime.p7m\r\n")
	emailMsg.WriteString("Content-Transfer-Encoding: base64\r\n")
	emailMsg.WriteString("Content-Disposition: attachment; filename=smime.p7m\r\n")
	emailMsg.WriteString("\r\n")
	emailMsg.Write(msg.Raw)

	if _, err := w.Write([]byte(emailMsg.String())); err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("direct write body: %w", err)}, nil
	}
	if err := w.Close(); err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("direct close data: %w", err)}, nil
	}

	client.Quit()

	d.logger.Debug("direct dest message sent",
		"destination", d.name,
		"to", d.cfg.To,
		"from", d.cfg.From,
		"messageId", msg.ID,
	)

	body, _ := json.Marshal(map[string]any{
		"status": "sent",
		"to":     d.cfg.To,
		"from":   d.cfg.From,
	})

	return &message.Response{StatusCode: 200, Body: body}, nil
}

func (d *DirectDest) Stop(ctx context.Context) error {
	return nil
}

func (d *DirectDest) Type() string {
	return "direct"
}
