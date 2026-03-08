package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/intuware/intu/pkg/config"
)

type S3Store struct {
	client *s3.Client
	bucket string
	prefix string
}

type s3MessageEnvelope struct {
	ID            string         `json:"id"`
	CorrelationID string         `json:"correlation_id"`
	ChannelID     string         `json:"channel_id"`
	Stage         string         `json:"stage"`
	Content       []byte         `json:"content,omitempty"`
	Status        string         `json:"status"`
	Timestamp     time.Time      `json:"timestamp"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

func NewS3Store(cfg *config.StorageS3Config) (*S3Store, error) {
	if cfg == nil || cfg.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}

	ctx := context.Background()

	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	prefix := cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &S3Store{
		client: client,
		bucket: cfg.Bucket,
		prefix: prefix,
	}, nil
}

func (s *S3Store) objectKey(channelID, stage, id string) string {
	return fmt.Sprintf("%s%s/%s/%s.json", s.prefix, channelID, stage, id)
}

func (s *S3Store) Save(record *MessageRecord) error {
	envelope := s3MessageEnvelope{
		ID:            record.ID,
		CorrelationID: record.CorrelationID,
		ChannelID:     record.ChannelID,
		Stage:         record.Stage,
		Content:       record.Content,
		Status:        record.Status,
		Timestamp:     record.Timestamp,
		Metadata:      record.Metadata,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal message record: %w", err)
	}

	key := s.objectKey(record.ChannelID, record.Stage, record.ID)
	_, err = s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
		Metadata: map[string]string{
			"message-id":     record.ID,
			"channel-id":     record.ChannelID,
			"stage":          record.Stage,
			"status":         record.Status,
			"timestamp":      record.Timestamp.UTC().Format(time.RFC3339Nano),
			"correlation-id": record.CorrelationID,
		},
	})
	if err != nil {
		return fmt.Errorf("put S3 object: %w", err)
	}

	return nil
}

func (s *S3Store) Get(id string) (*MessageRecord, error) {
	ctx := context.Background()
	prefix := s.prefix
	records, err := s.listByPrefix(ctx, prefix, func(env *s3MessageEnvelope) bool {
		return env.ID == id
	}, 1)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("message %s not found", id)
	}
	return records[0], nil
}

func (s *S3Store) GetStage(id, stage string) (*MessageRecord, error) {
	ctx := context.Background()
	records, err := s.listByPrefix(ctx, s.prefix, func(env *s3MessageEnvelope) bool {
		return env.ID == id && env.Stage == stage
	}, 1)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("message %s stage %s not found", id, stage)
	}
	return records[0], nil
}

func (s *S3Store) Query(opts QueryOpts) ([]*MessageRecord, error) {
	ctx := context.Background()

	searchPrefix := s.prefix
	if opts.ChannelID != "" {
		searchPrefix = fmt.Sprintf("%s%s/", s.prefix, opts.ChannelID)
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}

	if opts.Stage != "" && opts.ChannelID != "" {
		searchPrefix = fmt.Sprintf("%s%s/%s/", s.prefix, opts.ChannelID, opts.Stage)
	}

	records, err := s.listByPrefix(ctx, searchPrefix, func(env *s3MessageEnvelope) bool {
		if opts.Status != "" && env.Status != opts.Status {
			return false
		}
		if opts.Stage != "" && env.Stage != opts.Stage {
			return false
		}
		if !opts.Since.IsZero() && env.Timestamp.Before(opts.Since) {
			return false
		}
		if !opts.Before.IsZero() && env.Timestamp.After(opts.Before) {
			return false
		}
		return true
	}, limit+opts.Offset)
	if err != nil {
		return nil, err
	}

	if opts.Offset > 0 && opts.Offset < len(records) {
		records = records[opts.Offset:]
	} else if opts.Offset >= len(records) {
		return nil, nil
	}

	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}

	if opts.ExcludeContent {
		for _, rec := range records {
			rec.Content = nil
		}
	}

	return records, nil
}

func (s *S3Store) Delete(id string) error {
	ctx := context.Background()

	var toDelete []types.ObjectIdentifier
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil && strings.Contains(*obj.Key, "/"+id+".json") {
				toDelete = append(toDelete, types.ObjectIdentifier{
					Key: obj.Key,
				})
			}
		}
	}

	if len(toDelete) == 0 {
		return nil
	}

	_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{
			Objects: toDelete,
			Quiet:   aws.Bool(true),
		},
	})
	if err != nil {
		return fmt.Errorf("delete objects: %w", err)
	}

	return nil
}

func (s *S3Store) Prune(before time.Time, channel string) (int, error) {
	ctx := context.Background()
	searchPrefix := s.prefix
	if channel != "" {
		searchPrefix = fmt.Sprintf("%s%s/", s.prefix, channel)
	}

	var toDelete []types.ObjectIdentifier

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(searchPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			env, err := s.getObject(ctx, *obj.Key)
			if err != nil {
				continue
			}
			if env.Timestamp.Before(before) {
				toDelete = append(toDelete, types.ObjectIdentifier{
					Key: obj.Key,
				})
			}
		}
	}

	if len(toDelete) == 0 {
		return 0, nil
	}

	const batchSize = 1000
	pruned := 0
	for i := 0; i < len(toDelete); i += batchSize {
		end := i + batchSize
		if end > len(toDelete) {
			end = len(toDelete)
		}
		batch := toDelete[i:end]

		_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{
				Objects: batch,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return pruned, fmt.Errorf("delete objects batch: %w", err)
		}
		pruned += len(batch)
	}

	return pruned, nil
}

func (s *S3Store) Close() error {
	return nil
}

func (s *S3Store) getObject(ctx context.Context, key string) (*s3MessageEnvelope, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get S3 object %s: %w", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read S3 object body: %w", err)
	}

	var env s3MessageEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshal S3 object: %w", err)
	}
	return &env, nil
}

func (s *S3Store) listByPrefix(ctx context.Context, prefix string, filter func(*s3MessageEnvelope) bool, maxResults int) ([]*MessageRecord, error) {
	var records []*MessageRecord

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil || !strings.HasSuffix(*obj.Key, ".json") {
				continue
			}

			env, err := s.getObject(ctx, *obj.Key)
			if err != nil {
				continue
			}

			if filter != nil && !filter(env) {
				continue
			}

			records = append(records, &MessageRecord{
				ID:            env.ID,
				CorrelationID: env.CorrelationID,
				ChannelID:     env.ChannelID,
				Stage:         env.Stage,
				Content:       env.Content,
				Status:        env.Status,
				Timestamp:     env.Timestamp,
				Metadata:      env.Metadata,
			})

			if maxResults > 0 && len(records) >= maxResults {
				return records, nil
			}
		}
	}

	return records, nil
}
