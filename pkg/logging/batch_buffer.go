package logging

import (
	"sync"
	"time"
)

type flushFunc func(batch [][]byte) error

type batchBuffer struct {
	mu            sync.Mutex
	buf           [][]byte
	bufBytes      int
	maxCount      int
	maxBytes      int
	flushInterval time.Duration
	flush         flushFunc
	done          chan struct{}
	closed        bool
}

func newBatchBuffer(maxCount, maxBytes int, flushInterval time.Duration, flush flushFunc) *batchBuffer {
	b := &batchBuffer{
		maxCount:      maxCount,
		maxBytes:      maxBytes,
		flushInterval: flushInterval,
		flush:         flush,
		done:          make(chan struct{}),
	}
	go b.ticker()
	return b
}

func (b *batchBuffer) ticker() {
	t := time.NewTicker(b.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			b.mu.Lock()
			if len(b.buf) > 0 {
				batch := b.buf
				b.buf = nil
				b.bufBytes = 0
				b.mu.Unlock()
				b.flush(batch)
			} else {
				b.mu.Unlock()
			}
		case <-b.done:
			return
		}
	}
}

func (b *batchBuffer) Add(p []byte) {
	entry := make([]byte, len(p))
	copy(entry, p)

	b.mu.Lock()
	b.buf = append(b.buf, entry)
	b.bufBytes += len(entry)

	if len(b.buf) >= b.maxCount || b.bufBytes >= b.maxBytes {
		batch := b.buf
		b.buf = nil
		b.bufBytes = 0
		b.mu.Unlock()
		b.flush(batch)
		return
	}
	b.mu.Unlock()
}

func (b *batchBuffer) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.done)
	remaining := b.buf
	b.buf = nil
	b.bufBytes = 0
	b.mu.Unlock()

	if len(remaining) > 0 {
		return b.flush(remaining)
	}
	return nil
}
