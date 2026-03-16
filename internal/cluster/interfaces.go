package cluster

import "context"

type ChannelCoordinator interface {
	Start(ctx context.Context) error
	Stop()
	GetPeers() []PeerInfo
	InstanceID() string
	IsLeader() bool
	AcquireChannel(ctx context.Context, channelID string) (bool, error)
	RenewChannelLease(ctx context.Context, channelID string) error
	ReleaseChannel(ctx context.Context, channelID string) error
	OwnedChannels() []string
	ShouldAcquireChannel(channelID string, tags []string) bool
}

type MessageDeduplicator interface {
	IsDuplicate(key string) bool
}
