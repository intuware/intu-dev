package retry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/redis/go-redis/v9"
)

type RedisDestinationQueue struct {
	client   *redis.Client
	queueKey string
	name     string
	maxSize  int
	overflow string
	send     SendFunc
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

type redisQueueItem struct {
	ID            string                 `json:"id"`
	CorrelationID string                 `json:"correlation_id"`
	ChannelID     string                 `json:"channel_id"`
	Raw           []byte                 `json:"raw"`
	Transport     string                 `json:"transport,omitempty"`
	ContentType   string                 `json:"content_type"`
	HTTP          *message.HTTPMeta      `json:"http,omitempty"`
	File          *message.FileMeta      `json:"file,omitempty"`
	FTP           *message.FTPMeta       `json:"ftp,omitempty"`
	Kafka         *message.KafkaMeta     `json:"kafka,omitempty"`
	TCP           *message.TCPMeta       `json:"tcp,omitempty"`
	SMTP          *message.SMTPMeta      `json:"smtp,omitempty"`
	DICOM         *message.DICOMMeta     `json:"dicom,omitempty"`
	Database      *message.DatabaseMeta  `json:"database,omitempty"`
	Metadata      map[string]any         `json:"metadata"`
	Timestamp     int64                  `json:"timestamp"`
}

func NewRedisDestinationQueue(
	client *redis.Client,
	keyPrefix string,
	channelID string,
	name string,
	maxSize int,
	overflow string,
	threads int,
	send SendFunc,
	logger *slog.Logger,
) *RedisDestinationQueue {
	if maxSize <= 0 {
		maxSize = 1000
	}
	if threads <= 0 {
		threads = 1
	}

	queueKey := fmt.Sprintf("%s:queue:%s:%s", keyPrefix, channelID, name)

	q := &RedisDestinationQueue{
		client:   client,
		queueKey: queueKey,
		name:     name,
		maxSize:  maxSize,
		overflow: overflow,
		send:     send,
		logger:   logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	q.cancel = cancel

	for i := 0; i < threads; i++ {
		q.wg.Add(1)
		go q.worker(ctx)
	}

	return q
}

func (q *RedisDestinationQueue) Enqueue(ctx context.Context, msg *message.Message) error {
	item := redisQueueItem{
		ID:            msg.ID,
		CorrelationID: msg.CorrelationID,
		ChannelID:     msg.ChannelID,
		Raw:           msg.Raw,
		Transport:     msg.Transport,
		ContentType:   string(msg.ContentType),
		HTTP:          msg.HTTP,
		File:          msg.File,
		FTP:           msg.FTP,
		Kafka:         msg.Kafka,
		TCP:           msg.TCP,
		SMTP:          msg.SMTP,
		DICOM:         msg.DICOM,
		Database:      msg.Database,
		Metadata:      msg.Metadata,
		Timestamp:     msg.Timestamp.UnixMilli(),
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal queue item: %w", err)
	}

	length, err := q.client.LLen(ctx, q.queueKey).Result()
	if err != nil {
		return fmt.Errorf("check queue depth: %w", err)
	}

	if int(length) >= q.maxSize {
		switch q.overflow {
		case "drop_oldest":
			q.client.LPop(ctx, q.queueKey)
		case "reject":
			return &QueueFullError{Name: q.name}
		default:
			// block behavior: allow push anyway for Redis (bounded by maxSize check on next enqueue)
		}
	}

	return q.client.RPush(ctx, q.queueKey, data).Err()
}

func (q *RedisDestinationQueue) worker(ctx context.Context) {
	defer q.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err := q.client.BLPop(ctx, 5*time.Second, q.queueKey).Result()
		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			q.logger.Error("redis queue pop error",
				"queue", q.name,
				"error", err,
			)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if len(result) < 2 {
			continue
		}

		var item redisQueueItem
		if err := json.Unmarshal([]byte(result[1]), &item); err != nil {
			q.logger.Error("failed to unmarshal queue item",
				"queue", q.name,
				"error", err,
			)
			continue
		}

		msg := &message.Message{
			ID:            item.ID,
			CorrelationID: item.CorrelationID,
			ChannelID:     item.ChannelID,
			Raw:           item.Raw,
			Transport:     item.Transport,
			ContentType:   message.ContentType(item.ContentType),
			HTTP:          item.HTTP,
			File:          item.File,
			FTP:           item.FTP,
			Kafka:         item.Kafka,
			TCP:           item.TCP,
			SMTP:          item.SMTP,
			DICOM:         item.DICOM,
			Database:      item.Database,
			Metadata:      item.Metadata,
			Timestamp:     time.UnixMilli(item.Timestamp),
		}

		resp, err := q.send(ctx, msg)
		if err != nil {
			q.logger.Error("queued send failed",
				"destination", q.name,
				"messageId", msg.ID,
				"error", err,
			)
		}
		if resp != nil && resp.Error != nil {
			q.logger.Warn("queued send error response",
				"destination", q.name,
				"messageId", msg.ID,
				"statusCode", resp.StatusCode,
			)
		}
	}
}

func (q *RedisDestinationQueue) Stop() {
	if q.cancel != nil {
		q.cancel()
	}
	q.wg.Wait()
}

func (q *RedisDestinationQueue) Depth() int {
	length, err := q.client.LLen(context.Background(), q.queueKey).Result()
	if err != nil {
		return 0
	}
	return int(length)
}
