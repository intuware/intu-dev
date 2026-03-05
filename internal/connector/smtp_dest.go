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

	"github.com/intuware/intu/internal/auth"
	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

type SMTPDest struct {
	name   string
	cfg    *config.SMTPDestMapConfig
	logger *slog.Logger
}

func NewSMTPDest(name string, cfg *config.SMTPDestMapConfig, logger *slog.Logger) *SMTPDest {
	return &SMTPDest{name: name, cfg: cfg, logger: logger}
}

func (s *SMTPDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	if s.cfg.Host == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("smtp destination %s: host not configured", s.name)}, nil
	}
	if s.cfg.From == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("smtp destination %s: from address not configured", s.name)}, nil
	}
	if len(s.cfg.To) == 0 {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("smtp destination %s: no recipients configured", s.name)}, nil
	}

	port := s.cfg.Port
	if port == 0 {
		if s.cfg.TLS != nil && s.cfg.TLS.Enabled {
			port = 465
		} else {
			port = 25
		}
	}
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, port)

	var conn net.Conn
	var err error

	if s.cfg.TLS != nil && s.cfg.TLS.Enabled && port == 465 {
		var tlsCfg *tls.Config
		tlsCfg, err = auth.BuildTLSConfigFromMap(s.cfg.TLS)
		if err != nil {
			return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp TLS config: %w", err)}, nil
		}
		if tlsCfg == nil {
			tlsCfg = &tls.Config{ServerName: s.cfg.Host}
		}
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 30 * time.Second}, "tcp", addr, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 30*time.Second)
	}
	if err != nil {
		s.logger.Error("smtp dest connect failed", "destination", s.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp connect to %s: %w", addr, err)}, nil
	}

	client, err := smtp.NewClient(conn, s.cfg.Host)
	if err != nil {
		conn.Close()
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp client create: %w", err)}, nil
	}
	defer client.Close()

	// STARTTLS for non-implicit TLS connections
	if s.cfg.TLS != nil && s.cfg.TLS.Enabled && port != 465 {
		tlsCfg, tlsErr := auth.BuildTLSConfigFromMap(s.cfg.TLS)
		if tlsErr != nil {
			return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp STARTTLS config: %w", tlsErr)}, nil
		}
		if tlsCfg == nil {
			tlsCfg = &tls.Config{ServerName: s.cfg.Host}
		}
		if err := client.StartTLS(tlsCfg); err != nil {
			s.logger.Warn("smtp STARTTLS failed", "destination", s.name, "error", err)
		}
	}

	if s.cfg.Auth != nil && s.cfg.Auth.Username != "" {
		smtpAuth := smtp.PlainAuth("", s.cfg.Auth.Username, s.cfg.Auth.Password, s.cfg.Host)
		if err := client.Auth(smtpAuth); err != nil {
			s.logger.Error("smtp auth failed", "destination", s.name, "error", err)
			return &message.Response{StatusCode: 401, Error: fmt.Errorf("smtp auth: %w", err)}, nil
		}
	}

	if err := client.Mail(s.cfg.From); err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp MAIL FROM: %w", err)}, nil
	}
	for _, to := range s.cfg.To {
		if err := client.Rcpt(to); err != nil {
			return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp RCPT TO %s: %w", to, err)}, nil
		}
	}

	w, err := client.Data()
	if err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp DATA: %w", err)}, nil
	}

	subject := s.cfg.Subject
	if subject == "" {
		subject = fmt.Sprintf("intu message %s", msg.ID)
	}
	subject = strings.ReplaceAll(subject, "{{messageId}}", msg.ID)
	subject = strings.ReplaceAll(subject, "{{channelId}}", msg.ChannelID)

	var emailMsg strings.Builder
	emailMsg.WriteString(fmt.Sprintf("From: %s\r\n", s.cfg.From))
	emailMsg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(s.cfg.To, ", ")))
	emailMsg.WriteString(fmt.Sprintf("Subject: %s\r\n", subject))
	emailMsg.WriteString(fmt.Sprintf("Message-ID: <%s@intu>\r\n", msg.ID))
	emailMsg.WriteString(fmt.Sprintf("Date: %s\r\n", msg.Timestamp.Format("Mon, 02 Jan 2006 15:04:05 -0700")))

	contentType := "text/plain"
	if msg.ContentType == message.ContentTypeJSON {
		contentType = "application/json"
	} else if msg.ContentType == message.ContentTypeXML {
		contentType = "text/xml"
	} else if msg.ContentType == message.ContentTypeHL7v2 {
		contentType = "x-application/hl7-v2+er7"
	}
	emailMsg.WriteString(fmt.Sprintf("Content-Type: %s; charset=UTF-8\r\n", contentType))
	emailMsg.WriteString("MIME-Version: 1.0\r\n")
	emailMsg.WriteString("\r\n")
	emailMsg.Write(msg.Raw)

	if _, err := w.Write([]byte(emailMsg.String())); err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp write body: %w", err)}, nil
	}

	if err := w.Close(); err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("smtp close data: %w", err)}, nil
	}

	client.Quit()

	s.logger.Debug("smtp dest message sent",
		"destination", s.name,
		"to", s.cfg.To,
		"messageId", msg.ID,
	)

	body, _ := json.Marshal(map[string]any{
		"status":     "sent",
		"recipients": s.cfg.To,
	})

	return &message.Response{StatusCode: 200, Body: body}, nil
}

func (s *SMTPDest) Stop(ctx context.Context) error {
	return nil
}

func (s *SMTPDest) Type() string {
	return "smtp"
}
