package connector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type SFTPSource struct {
	cfg    *config.SFTPListener
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewSFTPSource(cfg *config.SFTPListener, logger *slog.Logger) *SFTPSource {
	return &SFTPSource{cfg: cfg, logger: logger}
}

func (s *SFTPSource) Start(ctx context.Context, handler MessageHandler) error {
	interval := 1 * time.Second
	if s.cfg.PollInterval != "" {
		if d, err := time.ParseDuration(s.cfg.PollInterval); err == nil {
			interval = d
		}
	}

	ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.poll(ctx, handler); err != nil {
					s.logger.Error("SFTP poll error", "error", err)
				}
			}
		}
	}()

	s.logger.Info("SFTP source started",
		"host", s.cfg.Host,
		"port", s.cfg.Port,
		"directory", s.cfg.Directory,
		"pattern", s.cfg.FilePattern,
		"poll_interval", interval.String(),
	)
	return nil
}

func (s *SFTPSource) dial() (*ssh.Client, error) {
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

func (s *SFTPSource) authUsername() string {
	if s.cfg.Auth != nil && s.cfg.Auth.Username != "" {
		return s.cfg.Auth.Username
	}
	return "anonymous"
}

func (s *SFTPSource) buildAuthMethods() []ssh.AuthMethod {
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
		if s.cfg.Auth.PrivateKeyFile != "" {
			if signer := s.loadSigner(); signer != nil {
				methods = append(methods, ssh.PublicKeys(signer))
			}
		}
	}

	return methods
}

func (s *SFTPSource) loadSigner() ssh.Signer {
	keyData, err := os.ReadFile(s.cfg.Auth.PrivateKeyFile)
	if err != nil {
		s.logger.Error("read SSH private key failed", "file", s.cfg.Auth.PrivateKeyFile, "error", err)
		return nil
	}

	var signer ssh.Signer
	if s.cfg.Auth.Passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(keyData, []byte(s.cfg.Auth.Passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(keyData)
	}
	if err != nil {
		s.logger.Error("parse SSH private key failed", "error", err)
		return nil
	}
	return signer
}

func (s *SFTPSource) poll(ctx context.Context, handler MessageHandler) error {
	sshClient, err := s.dial()
	if err != nil {
		return err
	}
	defer sshClient.Close()

	client, err := sftp.NewClient(sshClient)
	if err != nil {
		return fmt.Errorf("SFTP client: %w", err)
	}
	defer client.Close()

	// SFTP remote paths must use forward slashes (Unix-style) regardless of OS.
	dir := filepath.ToSlash(s.cfg.Directory)
	if dir == "" {
		dir = "."
	}

	entries, err := client.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("SFTP readdir %s: %w", dir, err)
	}

	var matched []os.FileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if s.cfg.FilePattern != "" {
			ok, _ := filepath.Match(s.cfg.FilePattern, entry.Name())
			if !ok {
				continue
			}
		}
		matched = append(matched, entry)
	}

	s.sortEntries(matched)

	for _, entry := range matched {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		remotePath := path.Join(dir, entry.Name())

		data, err := s.readRemoteFile(client, remotePath)
		if err != nil {
			s.logger.Error("SFTP read file failed", "path", remotePath, "error", err)
			if s.cfg.ErrorDir != "" {
				s.moveRemoteFile(client, remotePath, path.Join(filepath.ToSlash(s.cfg.ErrorDir), entry.Name()))
			}
			continue
		}

		msg := message.New("", data)
		msg.Transport = "sftp"
		msg.FTP = &message.FTPMeta{
			Filename:  entry.Name(),
			Directory: dir,
		}
		msg.Metadata["filename"] = entry.Name()
		msg.Metadata["filepath"] = remotePath
		msg.Metadata["sftp_host"] = s.cfg.Host
		msg.Metadata["file_size"] = entry.Size()
		msg.Metadata["file_modified"] = entry.ModTime().Format(time.RFC3339)

		if err := handler(ctx, msg); err != nil {
			s.logger.Error("SFTP handler error", "file", entry.Name(), "error", err)
			if s.cfg.ErrorDir != "" {
				s.moveRemoteFile(client, remotePath, path.Join(filepath.ToSlash(s.cfg.ErrorDir), entry.Name()))
			}
			continue
		}

		if s.cfg.MoveTo != "" {
			s.moveRemoteFile(client, remotePath, path.Join(filepath.ToSlash(s.cfg.MoveTo), entry.Name()))
		} else {
			if err := client.Remove(remotePath); err != nil {
				s.logger.Error("SFTP remove failed", "path", remotePath, "error", err)
			}
		}
	}

	return nil
}

func (s *SFTPSource) readRemoteFile(client *sftp.Client, path string) ([]byte, error) {
	f, err := client.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return data, nil
}

func (s *SFTPSource) moveRemoteFile(client *sftp.Client, src, dst string) {
	dstDir := path.Dir(dst)
	client.MkdirAll(dstDir)

	if err := client.Rename(src, dst); err != nil {
		s.logger.Error("SFTP rename failed, attempting copy+delete",
			"src", src, "dst", dst, "error", err)
		data, readErr := s.readRemoteFile(client, src)
		if readErr != nil {
			s.logger.Error("SFTP copy-read failed", "error", readErr)
			return
		}
		f, createErr := client.Create(dst)
		if createErr != nil {
			s.logger.Error("SFTP copy-create failed", "error", createErr)
			return
		}
		if _, writeErr := f.Write(data); writeErr != nil {
			f.Close()
			s.logger.Error("SFTP copy-write failed", "error", writeErr)
			return
		}
		f.Close()
		client.Remove(src)
	}
}

func (s *SFTPSource) sortEntries(entries []os.FileInfo) {
	switch s.cfg.SortBy {
	case "name":
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
	case "modified":
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].ModTime().Before(entries[j].ModTime())
		})
	case "size":
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Size() < entries[j].Size()
		})
	}
}

func (s *SFTPSource) Stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return nil
}

func (s *SFTPSource) Type() string {
	return "sftp"
}
