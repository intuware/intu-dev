package connector

import (
	"context"
	"log/slog"
	"sync"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

var channelBus = &ChannelBus{
	subscribers: make(map[string][]chan *message.Message),
}

type ChannelBus struct {
	mu          sync.RWMutex
	subscribers map[string][]chan *message.Message
}

func (cb *ChannelBus) Subscribe(channelID string) chan *message.Message {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	ch := make(chan *message.Message, 100)
	cb.subscribers[channelID] = append(cb.subscribers[channelID], ch)
	return ch
}

func (cb *ChannelBus) Publish(channelID string, msg *message.Message) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	for _, ch := range cb.subscribers[channelID] {
		select {
		case ch <- msg:
		default:
		}
	}
}

func GetChannelBus() *ChannelBus {
	return channelBus
}

type ChannelSource struct {
	cfg    *config.ChannelListener
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
	ch     chan *message.Message
}

func NewChannelSource(cfg *config.ChannelListener, logger *slog.Logger) *ChannelSource {
	return &ChannelSource{cfg: cfg, logger: logger}
}

func (c *ChannelSource) Start(ctx context.Context, handler MessageHandler) error {
	c.ch = channelBus.Subscribe(c.cfg.SourceChannelID)
	ctx, c.cancel = context.WithCancel(ctx)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-c.ch:
				if err := handler(ctx, msg); err != nil {
					c.logger.Error("channel source handler error", "error", err)
				}
			}
		}
	}()

	c.logger.Info("channel source started", "source_channel", c.cfg.SourceChannelID)
	return nil
}

func (c *ChannelSource) Stop(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return nil
}

func (c *ChannelSource) Type() string {
	return "channel"
}
