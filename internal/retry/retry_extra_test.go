package retry

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
)

// ---------------------------------------------------------------------------
// DestinationQueue: drop_oldest overflow mode
// ---------------------------------------------------------------------------

func TestDestinationQueue_DropOldest(t *testing.T) {
	var received sync.Map
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		received.Store(msg.ID, true)
		return &message.Response{StatusCode: 200}, nil
	}

	// Capacity of 2, no workers yet (0 threads → defaults to 1, but with slow send)
	q := NewDestinationQueue("drop-test", 2, "drop_oldest", 0, sendFn, slog.Default())
	defer q.Stop()

	// Give the worker time to start
	time.Sleep(50 * time.Millisecond)

	// Pause the worker by making send slow
	slowSend := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		time.Sleep(2 * time.Second)
		received.Store(msg.ID, true)
		return &message.Response{StatusCode: 200}, nil
	}

	q2 := NewDestinationQueue("drop-test-2", 2, "drop_oldest", 1, slowSend, slog.Default())
	defer q2.Stop()

	// Fill the queue — worker is busy sleeping on first message
	for i := 0; i < 5; i++ {
		msg := &message.Message{
			ID:        "msg-" + string(rune('A'+i)),
			Raw:       []byte("payload"),
			Timestamp: time.Now(),
		}
		if err := q2.Enqueue(context.Background(), msg); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	// drop_oldest should not return an error
}

// ---------------------------------------------------------------------------
// DestinationQueue: multiple workers
// ---------------------------------------------------------------------------

func TestDestinationQueue_MultipleWorkers(t *testing.T) {
	var count atomic.Int64
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		count.Add(1)
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewDestinationQueue("multi-worker", 100, "", 4, sendFn, slog.Default())
	defer q.Stop()

	numMsgs := 20
	for i := 0; i < numMsgs; i++ {
		msg := &message.Message{
			ID:        "mw-msg-" + string(rune('A'+i%26)),
			Raw:       []byte("hello"),
			Timestamp: time.Now(),
		}
		if err := q.Enqueue(context.Background(), msg); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	// Wait for all messages to be processed
	deadline := time.After(3 * time.Second)
	for {
		if count.Load() >= int64(numMsgs) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: processed %d of %d", count.Load(), numMsgs)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// ---------------------------------------------------------------------------
// DestinationQueue: Stop while processing
// ---------------------------------------------------------------------------

func TestDestinationQueue_StopWhileProcessing(t *testing.T) {
	started := make(chan struct{})
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		close(started)
		time.Sleep(500 * time.Millisecond)
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewDestinationQueue("stop-test", 100, "", 1, sendFn, slog.Default())

	msg := &message.Message{
		ID:        "stop-msg",
		Raw:       []byte("x"),
		Timestamp: time.Now(),
	}
	if err := q.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	<-started
	// Now stop while the worker is still processing
	done := make(chan struct{})
	go func() {
		q.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() took too long; possible hang")
	}
}

// ---------------------------------------------------------------------------
// DestinationQueue: Depth
// ---------------------------------------------------------------------------

func TestDestinationQueue_Depth_Empty(t *testing.T) {
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return &message.Response{StatusCode: 200}, nil
	}
	q := NewDestinationQueue("depth-test", 100, "", 1, sendFn, slog.Default())
	defer q.Stop()

	if q.Depth() != 0 {
		t.Fatalf("expected depth 0, got %d", q.Depth())
	}
}

// ---------------------------------------------------------------------------
// DestinationQueue: default maxSize and threads
// ---------------------------------------------------------------------------

func TestDestinationQueue_DefaultMaxSize(t *testing.T) {
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return &message.Response{StatusCode: 200}, nil
	}
	q := NewDestinationQueue("default-size", 0, "", 0, sendFn, slog.Default())
	defer q.Stop()

	if q.maxSize != 1000 {
		t.Fatalf("expected default maxSize 1000, got %d", q.maxSize)
	}
}

func TestDestinationQueue_NegativeMaxSize(t *testing.T) {
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return &message.Response{StatusCode: 200}, nil
	}
	q := NewDestinationQueue("neg-size", -5, "", 0, sendFn, slog.Default())
	defer q.Stop()

	if q.maxSize != 1000 {
		t.Fatalf("expected default maxSize 1000, got %d", q.maxSize)
	}
}

// ---------------------------------------------------------------------------
// DestinationQueue: send error logging (does not crash)
// ---------------------------------------------------------------------------

func TestDestinationQueue_SendError(t *testing.T) {
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return nil, context.DeadlineExceeded
	}

	q := NewDestinationQueue("err-test", 100, "", 1, sendFn, slog.Default())
	defer q.Stop()

	msg := &message.Message{
		ID:        "err-msg",
		Raw:       []byte("x"),
		Timestamp: time.Now(),
	}
	if err := q.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// RedisDestinationQueue: constructor
// ---------------------------------------------------------------------------

func TestNewRedisDestinationQueue_NilClient(t *testing.T) {
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewRedisDestinationQueue(nil, "intu", "ch-1", "dest-1", 0, "", 0, sendFn, slog.Default())
	if q == nil {
		t.Fatal("expected non-nil RedisDestinationQueue")
	}
	if q.maxSize != 1000 {
		t.Fatalf("expected default maxSize 1000, got %d", q.maxSize)
	}
	if q.queueKey != "intu:queue:ch-1:dest-1" {
		t.Fatalf("unexpected queue key: %q", q.queueKey)
	}
	q.Stop()
}

func TestNewRedisDestinationQueue_CustomValues(t *testing.T) {
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewRedisDestinationQueue(nil, "myprefix", "channel-a", "dest-b", 500, "reject", 3, sendFn, slog.Default())
	if q.maxSize != 500 {
		t.Fatalf("expected maxSize 500, got %d", q.maxSize)
	}
	if q.overflow != "reject" {
		t.Fatalf("expected overflow 'reject', got %q", q.overflow)
	}
	if q.queueKey != "myprefix:queue:channel-a:dest-b" {
		t.Fatalf("unexpected queue key: %q", q.queueKey)
	}
	q.Stop()
}
