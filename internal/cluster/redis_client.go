package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/intuware/intu/pkg/config"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client    *redis.Client
	keyPrefix string
}

func NewRedisClient(cfg *config.RedisConfig) (*RedisClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("redis config is nil")
	}
	if cfg.Address == "" {
		return nil, fmt.Errorf("redis address is required")
	}

	opts := &redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	}
	if cfg.MinIdleConns > 0 {
		opts.MinIdleConns = cfg.MinIdleConns
	}

	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
		}

		if cfg.TLS.MinVersion == "1.3" {
			tlsCfg.MinVersion = tls.VersionTLS13
		} else {
			tlsCfg.MinVersion = tls.VersionTLS12
		}

		if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("load redis TLS key pair: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}

		if cfg.TLS.CAFile != "" {
			caCert, err := os.ReadFile(cfg.TLS.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read redis TLS CA file: %w", err)
			}
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caCert)
			tlsCfg.RootCAs = pool
		}

		opts.TLSConfig = tlsCfg
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = "intu"
	}

	return &RedisClient{
		client:    client,
		keyPrefix: prefix,
	}, nil
}

func (rc *RedisClient) Key(parts ...string) string {
	key := rc.keyPrefix
	for _, p := range parts {
		key += ":" + p
	}
	return key
}

func (rc *RedisClient) Client() *redis.Client {
	return rc.client
}

func (rc *RedisClient) Close() error {
	return rc.client.Close()
}
