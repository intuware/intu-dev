package retry

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/redis/go-redis/v9"
)

func setupRedisQueue(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, client
}

func TestNewRedisDestinationQueue(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer mr.Close()
	defer client.Close()

	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewRedisDestinationQueue(client, "test", "ch-1", "dest-1", 100, "", 1, sendFn, slog.Default())
	defer q.Stop()

	if q.queueKey != "test:queue:ch-1:dest-1" {
		t.Fatalf("expected queue key 'test:queue:ch-1:dest-1', got %q", q.queueKey)
	}
	if q.maxSize != 100 {
		t.Fatalf("expected maxSize 100, got %d", q.maxSize)
	}
}

func TestNewRedisDestinationQueueDefaults(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer mr.Close()
	defer client.Close()

	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return nil, nil
	}

	q := NewRedisDestinationQueue(client, "pfx", "ch-1", "dest", 0, "", 0, sendFn, slog.Default())
	defer q.Stop()

	if q.maxSize != 1000 {
		t.Fatalf("expected default maxSize 1000, got %d", q.maxSize)
	}
}

func TestRedisDestinationQueueEnqueueAndProcess(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer mr.Close()
	defer client.Close()

	processed := make(chan string, 10)
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		processed <- msg.ID
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewRedisDestinationQueue(client, "test", "ch-1", "dest-1", 100, "", 1, sendFn, slog.Default())
	defer q.Stop()

	msg := message.New("ch-1", []byte("hello"))

	if err := q.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	select {
	case id := <-processed:
		if id != msg.ID {
			t.Fatalf("expected %s, got %s", msg.ID, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message to be processed")
	}
}

func TestRedisDestinationQueueDepth(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer mr.Close()
	defer client.Close()

	blockCh := make(chan struct{})
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		<-blockCh
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewRedisDestinationQueue(client, "test", "ch-1", "depth-dest", 100, "", 1, sendFn, slog.Default())
	defer func() {
		close(blockCh)
		q.Stop()
	}()

	if q.Depth() != 0 {
		t.Fatalf("expected initial depth 0, got %d", q.Depth())
	}

	for i := 0; i < 5; i++ {
		msg := message.New("ch-1", []byte("data"))
		if err := q.Enqueue(context.Background(), msg); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	depth := q.Depth()
	if depth < 3 {
		t.Fatalf("expected depth >= 3 (some may be in processing), got %d", depth)
	}
}

func TestRedisDestinationQueueRejectOverflow(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer mr.Close()
	defer client.Close()

	workerStarted := make(chan struct{}, 1)
	blockForever := make(chan struct{})
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		select {
		case workerStarted <- struct{}{}:
		default:
		}
		<-blockForever
		return nil, nil
	}

	q := NewRedisDestinationQueue(client, "test", "ch-1", "reject-dest", 3, "reject", 1, sendFn, slog.Default())
	defer func() {
		close(blockForever)
		q.Stop()
	}()

	msg0 := message.New("ch-1", []byte("first"))
	if err := q.Enqueue(context.Background(), msg0); err != nil {
		t.Fatalf("enqueue first failed: %v", err)
	}

	select {
	case <-workerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for worker to pick up first message")
	}

	for i := 0; i < 3; i++ {
		msg := message.New("ch-1", []byte("fill"))
		if err := q.Enqueue(context.Background(), msg); err != nil {
			t.Fatalf("enqueue fill %d failed: %v", i, err)
		}
	}

	msg := message.New("ch-1", []byte("overflow"))
	err := q.Enqueue(context.Background(), msg)
	if err == nil {
		t.Fatal("expected QueueFullError")
	}
	if _, ok := err.(*QueueFullError); !ok {
		t.Fatalf("expected *QueueFullError, got %T", err)
	}
}

func TestRedisDestinationQueueDropOldestOverflow(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer mr.Close()
	defer client.Close()

	workerStarted := make(chan struct{}, 1)
	blockForever := make(chan struct{})
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		select {
		case workerStarted <- struct{}{}:
		default:
		}
		<-blockForever
		return nil, nil
	}

	q := NewRedisDestinationQueue(client, "test", "ch-1", "drop-dest", 3, "drop_oldest", 1, sendFn, slog.Default())
	defer func() {
		close(blockForever)
		q.Stop()
	}()

	msg0 := message.New("ch-1", []byte("first"))
	if err := q.Enqueue(context.Background(), msg0); err != nil {
		t.Fatalf("enqueue first failed: %v", err)
	}

	select {
	case <-workerStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for worker to start")
	}

	for i := 0; i < 3; i++ {
		msg := message.New("ch-1", []byte("fill"))
		if err := q.Enqueue(context.Background(), msg); err != nil {
			t.Fatalf("enqueue fill %d failed: %v", i, err)
		}
	}

	msg := message.New("ch-1", []byte("newest"))
	err := q.Enqueue(context.Background(), msg)
	if err != nil {
		t.Fatalf("drop_oldest should not error: %v", err)
	}
}

func TestRedisDestinationQueueStop(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer mr.Close()
	defer client.Close()

	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return nil, nil
	}

	q := NewRedisDestinationQueue(client, "test", "ch-1", "stop-dest", 100, "", 2, sendFn, slog.Default())

	done := make(chan struct{})
	go func() {
		q.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not complete in time")
	}
}

func TestRedisDestinationQueueMultipleWorkers(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer mr.Close()
	defer client.Close()

	processed := make(chan string, 20)
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		processed <- msg.ID
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewRedisDestinationQueue(client, "test", "ch-1", "multi-dest", 100, "", 3, sendFn, slog.Default())
	defer q.Stop()

	for i := 0; i < 5; i++ {
		msg := message.New("ch-1", []byte("data"))
		if err := q.Enqueue(context.Background(), msg); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	count := 0
	timeout := time.After(5 * time.Second)
	for count < 5 {
		select {
		case <-processed:
			count++
		case <-timeout:
			t.Fatalf("timed out, only %d/5 messages processed", count)
		}
	}
}

func TestRedisDestinationQueueEnqueueErrorOnLLen(t *testing.T) {
	mr, client := setupRedisQueue(t)
	defer client.Close()

	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return nil, nil
	}

	q := NewRedisDestinationQueue(client, "test", "ch-1", "err-dest", 100, "", 1, sendFn, slog.Default())
	defer q.Stop()

	mr.Close()

	msg := message.New("ch-1", []byte("data"))
	err := q.Enqueue(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error when Redis is down")
	}
}
