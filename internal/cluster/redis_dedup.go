package cluster

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisDeduplicator struct {
	client *RedisClient
	window time.Duration
}

func NewRedisDeduplicator(client *RedisClient, window time.Duration) *RedisDeduplicator {
	return &RedisDeduplicator{
		client: client,
		window: window,
	}
}

func (rd *RedisDeduplicator) IsDuplicate(key string) bool {
	ctx := context.Background()
	redisKey := rd.client.Key("dedup", key)

	ok, err := rd.client.Client().SetNX(ctx, redisKey, 1, rd.window).Result()
	if err != nil {
		return false
	}

	// SetNX returns true if the key was set (new key = not a duplicate)
	return !ok
}

func (rd *RedisDeduplicator) IsDuplicateCtx(ctx context.Context, key string) (bool, error) {
	redisKey := rd.client.Key("dedup", key)

	ok, err := rd.client.Client().SetNX(ctx, redisKey, 1, rd.window).Result()
	if err != nil {
		if err == redis.Nil {
			return true, nil
		}
		return false, err
	}

	return !ok, nil
}
