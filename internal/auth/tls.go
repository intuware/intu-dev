package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"github.com/intuware/intu/pkg/config"
)

func DialTLS(dialer *net.Dialer, network, addr string, cfg *tls.Config) (net.Conn, error) {
	return tls.DialWithDialer(dialer, network, addr, cfg)
}

func BuildTLSConfig(cfg *config.TLSConfig) (*tls.Config, error) {
	if cfg == nil {
		return nil, nil
	}

	tlsCfg := &tls.Config{}

	switch cfg.MinVersion {
	case "1.0":
		tlsCfg.MinVersion = tls.VersionTLS10
	case "1.1":
		tlsCfg.MinVersion = tls.VersionTLS11
	case "1.3":
		tlsCfg.MinVersion = tls.VersionTLS13
	default:
		tlsCfg.MinVersion = tls.VersionTLS12
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load server cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		tlsCfg.RootCAs = pool
		tlsCfg.ClientCAs = pool
	}

	switch cfg.ClientAuth {
	case "require":
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	case "request":
		tlsCfg.ClientAuth = tls.RequestClientCert
	default:
		tlsCfg.ClientAuth = tls.NoClientCert
	}

	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = append(tlsCfg.Certificates, cert)
	}

	tlsCfg.InsecureSkipVerify = cfg.InsecureSkipVerify

	return tlsCfg, nil
}

func BuildTLSConfigFromMap(cfg *config.TLSMapConfig) (*tls.Config, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	yamlCfg := &config.TLSConfig{
		Enabled:            cfg.Enabled,
		CertFile:           cfg.CertFile,
		KeyFile:            cfg.KeyFile,
		CAFile:             cfg.CAFile,
		ClientCertFile:     cfg.ClientCertFile,
		ClientKeyFile:      cfg.ClientKeyFile,
		MinVersion:         cfg.MinVersion,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}
	return BuildTLSConfig(yamlCfg)
}
