package retry

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
)

func TestDestinationQueueEnqueueAndProcess(t *testing.T) {
	processed := make(chan string, 10)

	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		processed <- msg.ID
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewDestinationQueue("test-dest", 100, "", 1, sendFn, slog.Default())
	defer q.Stop()

	msg := &message.Message{
		ID:        "msg-1",
		Raw:       []byte("hello"),
		Timestamp: time.Now(),
	}

	if err := q.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	select {
	case id := <-processed:
		if id != "msg-1" {
			t.Fatalf("expected msg-1, got %s", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message to be processed")
	}
}

func TestDestinationQueueDepth(t *testing.T) {
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		time.Sleep(100 * time.Millisecond)
		return &message.Response{StatusCode: 200}, nil
	}

	q := NewDestinationQueue("test-dest", 100, "", 0, sendFn, slog.Default())
	defer q.Stop()

	depth := q.Depth()
	if depth != 0 {
		t.Fatalf("expected depth 0, got %d", depth)
	}
}

func TestDestinationQueueRejectOverflow(t *testing.T) {
	sendFn := func(ctx context.Context, msg *message.Message) (*message.Response, error) {
		time.Sleep(5 * time.Second)
		return nil, nil
	}

	q := NewDestinationQueue("test-dest", 2, "reject", 0, sendFn, slog.Default())
	defer q.Stop()

	for i := 0; i < 2; i++ {
		msg := &message.Message{ID: "msg", Raw: []byte("x"), Timestamp: time.Now()}
		if err := q.Enqueue(context.Background(), msg); err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}

	msg := &message.Message{ID: "msg-overflow", Raw: []byte("x"), Timestamp: time.Now()}
	err := q.Enqueue(context.Background(), msg)
	if err == nil {
		t.Fatal("expected QueueFullError")
	}
	if _, ok := err.(*QueueFullError); !ok {
		t.Fatalf("expected *QueueFullError, got %T", err)
	}
}

func TestQueueFullErrorMessage(t *testing.T) {
	err := &QueueFullError{Name: "my-queue"}
	expected := "destination queue full: my-queue"
	if err.Error() != expected {
		t.Fatalf("expected %q, got %q", expected, err.Error())
	}
}
