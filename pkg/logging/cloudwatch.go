package logging

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/intuware/intu/pkg/config"
)

const (
	cwFlushInterval = 500 * time.Millisecond
	cwMaxBatchCount = 10000
	cwMaxBatchBytes = 1048576 // 1 MB
)

type CloudWatchTransport struct {
	client    *cloudwatchlogs.Client
	logGroup  string
	logStream string
	batch     *batchBuffer

	mu            sync.Mutex
	sequenceToken *string
}

func NewCloudWatchTransport(cfg *config.CloudWatchLogConfig) (*CloudWatchTransport, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := cloudwatchlogs.NewFromConfig(awsCfg)

	t := &CloudWatchTransport{
		client:    client,
		logGroup:  cfg.LogGroup,
		logStream: cfg.LogStream,
	}

	if err := t.ensureGroupAndStream(ctx); err != nil {
		return nil, err
	}

	t.batch = newBatchBuffer(cwMaxBatchCount, cwMaxBatchBytes, cwFlushInterval, t.flushBatch)
	return t, nil
}

func (t *CloudWatchTransport) ensureGroupAndStream(ctx context.Context) error {
	_, err := t.client.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: &t.logGroup,
	})
	if err != nil && !isAlreadyExistsError(err) {
		return fmt.Errorf("create log group %q: %w", t.logGroup, err)
	}

	_, err = t.client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  &t.logGroup,
		LogStreamName: &t.logStream,
	})
	if err != nil && !isAlreadyExistsError(err) {
		return fmt.Errorf("create log stream %q: %w", t.logStream, err)
	}
	return nil
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ResourceAlreadyExistsException") ||
		strings.Contains(msg, "already exists")
}

func (t *CloudWatchTransport) Write(p []byte) (int, error) {
	t.batch.Add(p)
	return len(p), nil
}

func (t *CloudWatchTransport) Close() error {
	return t.batch.Close()
}

func (t *CloudWatchTransport) flushBatch(batch [][]byte) error {
	if len(batch) == 0 {
		return nil
	}

	events := make([]cwTypes.InputLogEvent, 0, len(batch))
	now := time.Now().UnixMilli()
	for _, entry := range batch {
		msg := strings.TrimRight(string(entry), "\n")
		events = append(events, cwTypes.InputLogEvent{
			Message:   &msg,
			Timestamp: &now,
		})
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	input := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  &t.logGroup,
		LogStreamName: &t.logStream,
		LogEvents:     events,
	}
	if t.sequenceToken != nil {
		input.SequenceToken = t.sequenceToken
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := t.client.PutLogEvents(ctx, input)
	if err != nil {
		return fmt.Errorf("put log events: %w", err)
	}
	if resp != nil {
		t.sequenceToken = resp.NextSequenceToken
	}
	return nil
}
