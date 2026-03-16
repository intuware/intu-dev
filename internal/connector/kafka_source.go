package connector

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
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

type KafkaSource struct {
	cfg    *config.KafkaListener
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewKafkaSource(cfg *config.KafkaListener, logger *slog.Logger) *KafkaSource {
	return &KafkaSource{cfg: cfg, logger: logger}
}

func (k *KafkaSource) Start(ctx context.Context, handler MessageHandler) error {
	if len(k.cfg.Brokers) == 0 {
		return fmt.Errorf("kafka source requires at least one broker")
	}
	if k.cfg.Topic == "" {
		return fmt.Errorf("kafka source requires a topic")
	}

	ctx, k.cancel = context.WithCancel(ctx)
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		k.consumeLoop(ctx, handler)
	}()

	k.logger.Info("kafka source started",
		"brokers", k.cfg.Brokers,
		"topic", k.cfg.Topic,
		"group_id", k.cfg.GroupID,
	)
	return nil
}

func (k *KafkaSource) consumeLoop(ctx context.Context, handler MessageHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := k.consume(ctx, handler)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			k.logger.Error("kafka consume error, reconnecting in 5s", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (k *KafkaSource) dialBroker(broker string) (net.Conn, error) {
	if k.cfg.TLS != nil && k.cfg.TLS.Enabled {
		tlsCfg, err := auth.BuildTLSConfig(k.cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("kafka TLS config: %w", err)
		}
		return tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", broker, tlsCfg)
	}
	return net.DialTimeout("tcp", broker, 10*time.Second)
}

func (k *KafkaSource) performSASL(conn net.Conn) error {
	if k.cfg.Auth == nil {
		return nil
	}

	switch k.cfg.Auth.Type {
	case "sasl_plain":
		return k.saslPlainHandshake(conn, k.cfg.Auth.Username, k.cfg.Auth.Password)
	case "sasl_scram":
		k.logger.Warn("SASL SCRAM auth requires a full Kafka client library; falling back to SASL PLAIN")
		return k.saslPlainHandshake(conn, k.cfg.Auth.Username, k.cfg.Auth.Password)
	case "", "none":
		return nil
	default:
		k.logger.Warn("unsupported kafka auth type, skipping", "type", k.cfg.Auth.Type)
		return nil
	}
}

func (k *KafkaSource) saslPlainHandshake(conn net.Conn, user, pass string) error {
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	apiKey := int16(17) // SaslHandshake
	apiVersion := int16(0)
	correlationID := int32(100)
	clientID := "intu-kafka-source"
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

	// SASL PLAIN: \x00user\x00password
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

	k.logger.Debug("SASL PLAIN authentication completed", "user", user)
	return nil
}

func (k *KafkaSource) consume(ctx context.Context, handler MessageHandler) error {
	broker := k.cfg.Brokers[0]
	if !strings.Contains(broker, ":") {
		broker = broker + ":9092"
	}

	conn, err := k.dialBroker(broker)
	if err != nil {
		return fmt.Errorf("connect to kafka broker %s: %w", broker, err)
	}
	defer conn.Close()

	if err := k.performSASL(conn); err != nil {
		return fmt.Errorf("kafka SASL auth: %w", err)
	}

	if err := k.sendFetchMetadata(conn); err != nil {
		return fmt.Errorf("kafka metadata request: %w", err)
	}

	k.logger.Debug("connected to kafka broker", "broker", broker, "topic", k.cfg.Topic)

	pollInterval := 1 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			msgs, err := k.fetchMessages(conn)
			if err != nil {
				return err
			}
			for _, data := range msgs {
				msg := message.New("", data)
				msg.Transport = "kafka"
				msg.Kafka = &message.KafkaMeta{
					Topic: k.cfg.Topic,
				}
				msg.Metadata["source"] = "kafka"
				msg.Metadata["topic"] = k.cfg.Topic
				msg.Metadata["broker"] = broker
				if k.cfg.GroupID != "" {
					msg.Metadata["group_id"] = k.cfg.GroupID
				}

				if err := handler(ctx, msg); err != nil {
					k.logger.Error("kafka handler error", "error", err)
				}
			}
		}
	}
}

func (k *KafkaSource) sendFetchMetadata(conn net.Conn) error {
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	apiKey := int16(3)
	apiVersion := int16(0)
	correlationID := int32(1)
	clientID := "intu-kafka-source"

	topicName := k.cfg.Topic

	var buf []byte
	buf = appendInt16(buf, apiKey)
	buf = appendInt16(buf, apiVersion)
	buf = appendInt32(buf, correlationID)
	buf = appendKafkaString(buf, clientID)
	buf = appendInt32(buf, 1)
	buf = appendKafkaString(buf, topicName)

	sizeBuf := make([]byte, 4)
	sizeBuf[0] = byte(len(buf) >> 24)
	sizeBuf[1] = byte(len(buf) >> 16)
	sizeBuf[2] = byte(len(buf) >> 8)
	sizeBuf[3] = byte(len(buf))

	if _, err := conn.Write(append(sizeBuf, buf...)); err != nil {
		return err
	}

	respSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, respSizeBuf); err != nil {
		return fmt.Errorf("read metadata response size: %w", err)
	}

	respSize := int(respSizeBuf[0])<<24 | int(respSizeBuf[1])<<16 | int(respSizeBuf[2])<<8 | int(respSizeBuf[3])
	if respSize > 0 && respSize < 1024*1024 {
		respBody := make([]byte, respSize)
		io.ReadFull(conn, respBody)
	}

	return nil
}

func (k *KafkaSource) fetchMessages(conn net.Conn) ([][]byte, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	apiKey := int16(1)
	apiVersion := int16(0)
	correlationID := int32(2)
	clientID := "intu-kafka-source"
	replicaID := int32(-1)
	maxWaitMs := int32(1000)
	minBytes := int32(1)

	topicName := k.cfg.Topic
	partition := int32(0)
	fetchOffset := int64(0)
	maxBytes := int32(65536)

	var buf []byte
	buf = appendInt16(buf, apiKey)
	buf = appendInt16(buf, apiVersion)
	buf = appendInt32(buf, correlationID)
	buf = appendKafkaString(buf, clientID)
	buf = appendInt32(buf, replicaID)
	buf = appendInt32(buf, maxWaitMs)
	buf = appendInt32(buf, minBytes)
	buf = appendInt32(buf, 1)
	buf = appendKafkaString(buf, topicName)
	buf = appendInt32(buf, 1)
	buf = appendInt32(buf, partition)
	buf = appendInt64(buf, fetchOffset)
	buf = appendInt32(buf, maxBytes)

	sizeBuf := make([]byte, 4)
	sizeBuf[0] = byte(len(buf) >> 24)
	sizeBuf[1] = byte(len(buf) >> 16)
	sizeBuf[2] = byte(len(buf) >> 8)
	sizeBuf[3] = byte(len(buf))

	if _, err := conn.Write(append(sizeBuf, buf...)); err != nil {
		return nil, err
	}

	respSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, respSizeBuf); err != nil {
		return nil, fmt.Errorf("read fetch response size: %w", err)
	}

	respSize := int(respSizeBuf[0])<<24 | int(respSizeBuf[1])<<16 | int(respSizeBuf[2])<<8 | int(respSizeBuf[3])
	if respSize <= 0 || respSize > 10*1024*1024 {
		return nil, nil
	}

	respBody := make([]byte, respSize)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		return nil, nil
	}

	return k.parseMessageSet(respBody), nil
}

func (k *KafkaSource) parseMessageSet(data []byte) [][]byte {
	var messages [][]byte

	offset := 0
	offset += 4 // skip correlationId

	if offset+4 > len(data) {
		return messages
	}
	topicCount := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
	offset += 4

	for t := 0; t < topicCount && offset < len(data); t++ {
		if offset+2 > len(data) {
			break
		}
		topicLen := int(data[offset])<<8 | int(data[offset+1])
		offset += 2 + topicLen

		if offset+4 > len(data) {
			break
		}
		partCount := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		for p := 0; p < partCount && offset < len(data); p++ {
			if offset+16 > len(data) {
				break
			}
			offset += 4
			offset += 2
			offset += 8

			if offset+4 > len(data) {
				break
			}
			msgSetSize := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
			offset += 4

			endOfMsgSet := offset + msgSetSize
			if endOfMsgSet > len(data) {
				endOfMsgSet = len(data)
			}

			for offset+12 < endOfMsgSet {
				offset += 8

				if offset+4 > endOfMsgSet {
					break
				}
				msgSize := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
				offset += 4

				if msgSize <= 0 || offset+msgSize > endOfMsgSet {
					offset = endOfMsgSet
					break
				}

				if offset+10 > endOfMsgSet {
					offset = endOfMsgSet
					break
				}

				offset += 4
				offset += 1
				offset += 1

				if offset+4 > endOfMsgSet {
					offset = endOfMsgSet
					break
				}
				keyLen := int32(data[offset])<<24 | int32(data[offset+1])<<16 | int32(data[offset+2])<<8 | int32(data[offset+3])
				offset += 4
				if keyLen > 0 {
					offset += int(keyLen)
				}

				if offset+4 > endOfMsgSet {
					offset = endOfMsgSet
					break
				}
				valueLen := int32(data[offset])<<24 | int32(data[offset+1])<<16 | int32(data[offset+2])<<8 | int32(data[offset+3])
				offset += 4

				if valueLen > 0 && offset+int(valueLen) <= endOfMsgSet {
					msgData := make([]byte, valueLen)
					copy(msgData, data[offset:offset+int(valueLen)])
					messages = append(messages, msgData)
					offset += int(valueLen)
				} else if valueLen > 0 {
					offset = endOfMsgSet
					break
				}
			}

			offset = endOfMsgSet
		}
	}

	return messages
}

func appendInt16(buf []byte, v int16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

func appendInt32(buf []byte, v int32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendInt64(buf []byte, v int64) []byte {
	return append(buf, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendKafkaString(buf []byte, s string) []byte {
	buf = appendInt16(buf, int16(len(s)))
	return append(buf, []byte(s)...)
}

func (k *KafkaSource) buildDebugInfo() map[string]any {
	info := map[string]any{
		"type":    "kafka",
		"brokers": k.cfg.Brokers,
		"topic":   k.cfg.Topic,
	}
	if k.cfg.GroupID != "" {
		info["group_id"] = k.cfg.GroupID
	}
	if k.cfg.Offset != "" {
		info["offset"] = k.cfg.Offset
	}
	return info
}

func (k *KafkaSource) debugJSON() string {
	data, _ := json.Marshal(k.buildDebugInfo())
	return string(data)
}

func (k *KafkaSource) Stop(ctx context.Context) error {
	if k.cancel != nil {
		k.cancel()
	}
	k.wg.Wait()
	return nil
}

func (k *KafkaSource) Type() string {
	return "kafka"
}
