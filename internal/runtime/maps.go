package runtime

import "sync"

type MapVariables struct {
	globalMap    *SyncMap
	channelMaps  map[string]*SyncMap
	responseMaps map[string]*SyncMap
	mu           sync.RWMutex
}

type SyncMap struct {
	mu   sync.RWMutex
	data map[string]any
}

func NewSyncMap() *SyncMap {
	return &SyncMap{data: make(map[string]any)}
}

func (m *SyncMap) Get(key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return v, ok
}

func (m *SyncMap) Put(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

func (m *SyncMap) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

func (m *SyncMap) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]any)
}

func (m *SyncMap) Snapshot() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap := make(map[string]any, len(m.data))
	for k, v := range m.data {
		snap[k] = v
	}
	return snap
}

func NewMapVariables() *MapVariables {
	return &MapVariables{
		globalMap:    NewSyncMap(),
		channelMaps:  make(map[string]*SyncMap),
		responseMaps: make(map[string]*SyncMap),
	}
}

func (mv *MapVariables) GlobalMap() *SyncMap {
	return mv.globalMap
}

func (mv *MapVariables) ChannelMap(channelID string) *SyncMap {
	mv.mu.RLock()
	if m, ok := mv.channelMaps[channelID]; ok {
		mv.mu.RUnlock()
		return m
	}
	mv.mu.RUnlock()

	mv.mu.Lock()
	defer mv.mu.Unlock()
	if m, ok := mv.channelMaps[channelID]; ok {
		return m
	}
	m := NewSyncMap()
	mv.channelMaps[channelID] = m
	return m
}

func (mv *MapVariables) ResponseMap(channelID string) *SyncMap {
	mv.mu.RLock()
	if m, ok := mv.responseMaps[channelID]; ok {
		mv.mu.RUnlock()
		return m
	}
	mv.mu.RUnlock()

	mv.mu.Lock()
	defer mv.mu.Unlock()
	if m, ok := mv.responseMaps[channelID]; ok {
		return m
	}
	m := NewSyncMap()
	mv.responseMaps[channelID] = m
	return m
}

// ConnectorMap creates a per-message connector map (not shared across messages).
func NewConnectorMap() *SyncMap {
	return NewSyncMap()
}

// Export returns all map data as a plain map structure for JS context injection.
func (mv *MapVariables) ExportForChannel(channelID string) map[string]any {
	return map[string]any{
		"globalMap":   mv.globalMap.Snapshot(),
		"channelMap":  mv.ChannelMap(channelID).Snapshot(),
		"responseMap": mv.ResponseMap(channelID).Snapshot(),
	}
}
