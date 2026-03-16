package retry

import (
	"context"
	"log/slog"
	"sync"

	"github.com/intuware/intu-dev/internal/message"
)

type DestinationQueue struct {
	name     string
	maxSize  int
	overflow string
	messages chan *queuedMessage
	send     SendFunc
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

type queuedMessage struct {
	msg *message.Message
	ctx context.Context
}

func NewDestinationQueue(name string, maxSize int, overflow string, threads int, send SendFunc, logger *slog.Logger) *DestinationQueue {
	if maxSize <= 0 {
		maxSize = 1000
	}
	if threads <= 0 {
		threads = 1
	}

	q := &DestinationQueue{
		name:     name,
		maxSize:  maxSize,
		overflow: overflow,
		messages: make(chan *queuedMessage, maxSize),
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

func (q *DestinationQueue) Enqueue(ctx context.Context, msg *message.Message) error {
	qm := &queuedMessage{msg: msg, ctx: ctx}

	select {
	case q.messages <- qm:
		return nil
	default:
		switch q.overflow {
		case "drop_oldest":
			select {
			case <-q.messages:
			default:
			}
			q.messages <- qm
			return nil
		case "reject":
			return &QueueFullError{Name: q.name}
		default: // "block"
			q.messages <- qm
			return nil
		}
	}
}

func (q *DestinationQueue) worker(ctx context.Context) {
	defer q.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case qm := <-q.messages:
			resp, err := q.send(qm.ctx, qm.msg)
			if err != nil {
				q.logger.Error("queued send failed",
					"destination", q.name,
					"messageId", qm.msg.ID,
					"error", err,
				)
			}
			if resp != nil && resp.Error != nil {
				q.logger.Warn("queued send error response",
					"destination", q.name,
					"messageId", qm.msg.ID,
					"statusCode", resp.StatusCode,
				)
			}
		}
	}
}

func (q *DestinationQueue) Stop() {
	if q.cancel != nil {
		q.cancel()
	}
	q.wg.Wait()
}

func (q *DestinationQueue) Depth() int {
	return len(q.messages)
}

type QueueFullError struct {
	Name string
}

func (e *QueueFullError) Error() string {
	return "destination queue full: " + e.Name
}
