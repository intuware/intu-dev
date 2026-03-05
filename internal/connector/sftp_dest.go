package connector

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPDest struct {
	name   string
	cfg    *config.SFTPDestMapConfig
	logger *slog.Logger
}

func NewSFTPDest(name string, cfg *config.SFTPDestMapConfig, logger *slog.Logger) *SFTPDest {
	return &SFTPDest{name: name, cfg: cfg, logger: logger}
}

func (s *SFTPDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	if s.cfg.Host == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("SFTP host is required")}, nil
	}

	sshClient, err := s.dial()
	if err != nil {
		return &message.Response{StatusCode: 502, Error: err}, nil
	}
	defer sshClient.Close()

	client, err := sftp.NewClient(sshClient)
	if err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("SFTP client: %w", err)}, nil
	}
	defer client.Close()

	dir := s.cfg.Directory
	if dir == "" {
		dir = "."
	}
	client.MkdirAll(dir)

	filename := s.cfg.FilenamePattern
	if filename == "" {
		filename = fmt.Sprintf("%s_%d.dat", msg.ChannelID, time.Now().UnixNano())
	} else {
		filename = strings.ReplaceAll(filename, "{{channelId}}", msg.ChannelID)
		filename = strings.ReplaceAll(filename, "{{correlationId}}", msg.CorrelationID)
		filename = strings.ReplaceAll(filename, "{{messageId}}", msg.ID)
		filename = strings.ReplaceAll(filename, "{{timestamp}}", time.Now().Format("20060102T150405"))
	}

	remotePath := filepath.Join(dir, filename)
	f, err := client.Create(remotePath)
	if err != nil {
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("SFTP create %s: %w", remotePath, err)}, nil
	}

	if _, err := f.Write(msg.Raw); err != nil {
		f.Close()
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("SFTP write %s: %w", remotePath, err)}, nil
	}
	f.Close()

	s.logger.Debug("SFTP file written", "path", remotePath, "bytes", len(msg.Raw))
	return &message.Response{StatusCode: 200, Body: []byte(remotePath)}, nil
}

func (s *SFTPDest) dial() (*ssh.Client, error) {
	port := s.cfg.Port
	if port == 0 {
		port = 22
	}
	addr := net.JoinHostPort(s.cfg.Host, fmt.Sprintf("%d", port))

	sshCfg := &ssh.ClientConfig{
		User:            s.authUsername(),
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	authMethods := s.buildAuthMethods()
	if len(authMethods) > 0 {
		sshCfg.Auth = authMethods
	}

	client, err := ssh.Dial("tcp", addr, sshCfg)
	if err != nil {
		return nil, fmt.Errorf("SSH dial %s: %w", addr, err)
	}
	return client, nil
}

func (s *SFTPDest) authUsername() string {
	if s.cfg.Auth != nil && s.cfg.Auth.Username != "" {
		return s.cfg.Auth.Username
	}
	return "anonymous"
}

func (s *SFTPDest) buildAuthMethods() []ssh.AuthMethod {
	if s.cfg.Auth == nil {
		return nil
	}

	var methods []ssh.AuthMethod

	switch s.cfg.Auth.Type {
	case "password":
		if s.cfg.Auth.Password != "" {
			methods = append(methods, ssh.Password(s.cfg.Auth.Password))
		}
	case "key":
		if s.cfg.Auth.PrivateKeyFile != "" {
			if signer := s.loadSigner(); signer != nil {
				methods = append(methods, ssh.PublicKeys(signer))
			}
		}
	default:
		if s.cfg.Auth.Password != "" {
			methods = append(methods, ssh.Password(s.cfg.Auth.Password))
		}
	}

	return methods
}

func (s *SFTPDest) loadSigner() ssh.Signer {
	keyData, err := os.ReadFile(s.cfg.Auth.PrivateKeyFile)
	if err != nil {
		s.logger.Error("read SSH key failed", "file", s.cfg.Auth.PrivateKeyFile, "error", err)
		return nil
	}

	var signer ssh.Signer
	if s.cfg.Auth.Passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(keyData, []byte(s.cfg.Auth.Passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(keyData)
	}
	if err != nil {
		s.logger.Error("parse SSH key failed", "error", err)
		return nil
	}
	return signer
}

func (s *SFTPDest) Stop(ctx context.Context) error {
	return nil
}

func (s *SFTPDest) Type() string {
	return "sftp"
}
