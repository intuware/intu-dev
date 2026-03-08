package storage

import (
	"time"
)

type CompositeStore struct {
	inner  MessageStore
	mode   string
	stages map[string]bool
}

func NewCompositeStore(inner MessageStore, mode string, stages []string) *CompositeStore {
	if mode == "" {
		mode = "full"
	}

	stageSet := make(map[string]bool)
	for _, s := range stages {
		stageSet[s] = true
	}

	return &CompositeStore{
		inner:  inner,
		mode:   mode,
		stages: stageSet,
	}
}

func (c *CompositeStore) Mode() string {
	return c.mode
}

func (c *CompositeStore) ShouldStore(stage string) bool {
	switch c.mode {
	case "none":
		return false
	case "status":
		return true
	case "full":
		if len(c.stages) == 0 {
			return true
		}
		return c.stages[stage]
	default:
		return true
	}
}

func (c *CompositeStore) Save(record *MessageRecord) error {
	if c.mode == "none" {
		return nil
	}

	if c.mode == "full" && len(c.stages) > 0 && !c.stages[record.Stage] {
		return nil
	}

	if c.mode == "status" {
		record = &MessageRecord{
			ID:            record.ID,
			CorrelationID: record.CorrelationID,
			ChannelID:     record.ChannelID,
			Stage:         record.Stage,
			Content:       nil,
			Status:        record.Status,
			Timestamp:     record.Timestamp,
			Metadata:      record.Metadata,
		}
	}

	return c.inner.Save(record)
}

func (c *CompositeStore) Get(id string) (*MessageRecord, error) {
	if c.mode == "none" {
		return nil, nil
	}
	return c.inner.Get(id)
}

func (c *CompositeStore) GetStage(id, stage string) (*MessageRecord, error) {
	if c.mode == "none" {
		return nil, nil
	}
	return c.inner.GetStage(id, stage)
}

func (c *CompositeStore) Query(opts QueryOpts) ([]*MessageRecord, error) {
	if c.mode == "none" {
		return nil, nil
	}
	return c.inner.Query(opts)
}

func (c *CompositeStore) Delete(id string) error {
	if c.mode == "none" {
		return nil
	}
	return c.inner.Delete(id)
}

func (c *CompositeStore) Prune(before time.Time, channel string) (int, error) {
	if c.mode == "none" {
		return 0, nil
	}
	return c.inner.Prune(before, channel)
}
