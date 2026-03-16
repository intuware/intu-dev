package connector

import (
	"context"
	"crypto/tls"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type KafkaDest struct {
	name   string
	cfg    *config.KafkaDestConfig
	logger *slog.Logger

	mu   sync.Mutex
	conn net.Conn
}

func NewKafkaDest(name string, cfg *config.KafkaDestConfig, logger *slog.Logger) *KafkaDest {
	return &KafkaDest{name: name, cfg: cfg, logger: logger}
}

func (k *KafkaDest) dialBroker(broker string) (net.Conn, error) {
	if !strings.Contains(broker, ":") {
		broker = broker + ":9092"
	}

	if k.cfg.TLS != nil && k.cfg.TLS.Enabled {
		tlsCfg, err := auth.BuildTLSConfigFromMap(k.cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("kafka dest TLS config: %w", err)
		}
		return tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", broker, tlsCfg)
	}
	return net.DialTimeout("tcp", broker, 10*time.Second)
}

func (k *KafkaDest) performSASL(conn net.Conn) error {
	if k.cfg.Auth == nil || k.cfg.Auth.Type == "" || k.cfg.Auth.Type == "none" {
		return nil
	}

	switch k.cfg.Auth.Type {
	case "sasl_plain":
		return k.saslPlainHandshake(conn, k.cfg.Auth.Username, k.cfg.Auth.Password)
	case "sasl_scram":
		k.logger.Warn("SASL SCRAM requires full Kafka client library; falling back to SASL PLAIN")
		return k.saslPlainHandshake(conn, k.cfg.Auth.Username, k.cfg.Auth.Password)
	default:
		k.logger.Warn("unsupported kafka dest auth type, skipping", "type", k.cfg.Auth.Type)
		return nil
	}
}

func (k *KafkaDest) saslPlainHandshake(conn net.Conn, user, pass string) error {
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	apiKey := int16(17) // SaslHandshake
	apiVersion := int16(0)
	correlationID := int32(200)
	clientID := k.clientID()
	mechanism := "PLAIN"

	var buf []byte
	buf = appendInt16(buf, apiKey)
	buf = appendInt16(buf, apiVersion)
	buf = appendInt32(buf, correlationID)
	buf = appendKafkaString(buf, clientID)
	buf = appendKafkaString(buf, mechanism)

	sizeBuf := make([]byte, 4)
	sizeBuf[0] = byte(len(buf) >> 24)
	sizeBuf[1] = byte(len(buf) >> 16)
	sizeBuf[2] = byte(len(buf) >> 8)
	sizeBuf[3] = byte(len(buf))

	if _, err := conn.Write(append(sizeBuf, buf...)); err != nil {
		return fmt.Errorf("SASL handshake write: %w", err)
	}

	respSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, respSizeBuf); err != nil {
		return fmt.Errorf("SASL handshake read size: %w", err)
	}
	respSize := int(respSizeBuf[0])<<24 | int(respSizeBuf[1])<<16 | int(respSizeBuf[2])<<8 | int(respSizeBuf[3])
	if respSize > 0 && respSize < 1024*1024 {
		respBody := make([]byte, respSize)
		io.ReadFull(conn, respBody)
	}

	saslPayload := []byte("\x00" + user + "\x00" + pass)
	authBuf := make([]byte, 4+len(saslPayload))
	authBuf[0] = byte(len(saslPayload) >> 24)
	authBuf[1] = byte(len(saslPayload) >> 16)
	authBuf[2] = byte(len(saslPayload) >> 8)
	authBuf[3] = byte(len(saslPayload))
	copy(authBuf[4:], saslPayload)

	if _, err := conn.Write(authBuf); err != nil {
		return fmt.Errorf("SASL authenticate write: %w", err)
	}

	authRespSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, authRespSizeBuf); err != nil {
		return fmt.Errorf("SASL authenticate read: %w", err)
	}
	authRespSize := int(authRespSizeBuf[0])<<24 | int(authRespSizeBuf[1])<<16 | int(authRespSizeBuf[2])<<8 | int(authRespSizeBuf[3])
	if authRespSize > 0 && authRespSize < 1024*1024 {
		authRespBody := make([]byte, authRespSize)
		io.ReadFull(conn, authRespBody)
	}

	k.logger.Debug("kafka dest SASL PLAIN authentication completed", "user", user)
	return nil
}

func (k *KafkaDest) clientID() string {
	if k.cfg.ClientID != "" {
		return k.cfg.ClientID
	}
	return "intu-kafka-dest"
}

func (k *KafkaDest) getConn() (net.Conn, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.conn != nil {
		return k.conn, nil
	}

	if len(k.cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka dest requires at least one broker")
	}

	var lastErr error
	for _, broker := range k.cfg.Brokers {
		conn, err := k.dialBroker(broker)
		if err != nil {
			lastErr = err
			continue
		}

		if err := k.performSASL(conn); err != nil {
			conn.Close()
			lastErr = err
			continue
		}

		k.conn = conn
		return conn, nil
	}
	return nil, fmt.Errorf("kafka dest failed to connect to any broker: %w", lastErr)
}

func (k *KafkaDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	conn, err := k.getConn()
	if err != nil {
		k.logger.Error("kafka dest connect failed", "destination", k.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("kafka connect: %w", err)}, nil
	}

	msg.ClearTransportMeta()
	msg.Transport = "kafka"
	msg.Kafka = &message.KafkaMeta{Topic: k.cfg.Topic}

	if err := k.produce(conn, msg.Raw); err != nil {
		k.mu.Lock()
		if k.conn != nil {
			k.conn.Close()
			k.conn = nil
		}
		k.mu.Unlock()

		k.logger.Error("kafka dest produce failed", "destination", k.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("kafka produce: %w", err)}, nil
	}

	k.logger.Debug("kafka dest message produced",
		"destination", k.name,
		"topic", k.cfg.Topic,
		"bytes", len(msg.Raw),
	)

	return &message.Response{StatusCode: 200, Body: []byte(`{"status":"produced"}`)}, nil
}

func (k *KafkaDest) produce(conn net.Conn, value []byte) error {
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	apiKey := int16(0) // Produce
	apiVersion := int16(0)
	correlationID := int32(201)
	clientID := k.clientID()
	requiredAcks := int16(1)
	timeout := int32(5000)

	var msgBuf []byte
	// CRC placeholder (4) + magic (1) + attributes (1) + key length (4) + value length (4) + value
	msgBuf = append(msgBuf, 0, 0, 0, 0) // CRC placeholder — filled below
	msgBuf = append(msgBuf, 0x00)        // magic
	msgBuf = append(msgBuf, 0x00)        // attributes
	msgBuf = appendInt32(msgBuf, -1)
	msgBuf = appendInt32(msgBuf, int32(len(value)))
	msgBuf = append(msgBuf, value...)

	// Kafka v0 message CRC covers everything after the CRC field itself.
	crc := crc32.ChecksumIEEE(msgBuf[4:])
	msgBuf[0] = byte(crc >> 24)
	msgBuf[1] = byte(crc >> 16)
	msgBuf[2] = byte(crc >> 8)
	msgBuf[3] = byte(crc)

	// MessageSet entry: offset(8) + message_size(4) + message
	var msgSet []byte
	msgSet = appendInt64(msgSet, 0)
	msgSet = appendInt32(msgSet, int32(len(msgBuf)))
	msgSet = append(msgSet, msgBuf...)

	// Produce request body
	var buf []byte
	buf = appendInt16(buf, apiKey)
	buf = appendInt16(buf, apiVersion)
	buf = appendInt32(buf, correlationID)
	buf = appendKafkaString(buf, clientID)
	buf = appendInt16(buf, requiredAcks)
	buf = appendInt32(buf, timeout)
	buf = appendInt32(buf, 1) // topic count
	buf = appendKafkaString(buf, k.cfg.Topic)
	buf = appendInt32(buf, 1) // partition count
	buf = appendInt32(buf, 0) // partition
	buf = appendInt32(buf, int32(len(msgSet)))
	buf = append(buf, msgSet...)

	sizeBuf := make([]byte, 4)
	sizeBuf[0] = byte(len(buf) >> 24)
	sizeBuf[1] = byte(len(buf) >> 16)
	sizeBuf[2] = byte(len(buf) >> 8)
	sizeBuf[3] = byte(len(buf))

	if _, err := conn.Write(append(sizeBuf, buf...)); err != nil {
		return fmt.Errorf("produce write: %w", err)
	}

	// Read produce response
	respSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, respSizeBuf); err != nil {
		return fmt.Errorf("produce read response size: %w", err)
	}

	respSize := int(respSizeBuf[0])<<24 | int(respSizeBuf[1])<<16 | int(respSizeBuf[2])<<8 | int(respSizeBuf[3])
	if respSize > 0 && respSize < 1024*1024 {
		respBody := make([]byte, respSize)
		io.ReadFull(conn, respBody)
	}

	return nil
}

func (k *KafkaDest) Stop(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.conn != nil {
		k.conn.Close()
		k.conn = nil
	}
	return nil
}

func (k *KafkaDest) Type() string {
	return "kafka"
}
