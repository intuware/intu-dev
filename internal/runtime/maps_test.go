package runtime

import (
	"sync"
	"testing"
)

func TestNewSyncMap(t *testing.T) {
	m := NewSyncMap()
	if m == nil {
		t.Fatal("expected non-nil SyncMap")
	}
	snap := m.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(snap))
	}
}

func TestSyncMapPutAndGet(t *testing.T) {
	m := NewSyncMap()
	m.Put("key1", "value1")
	m.Put("key2", 42)

	v, ok := m.Get("key1")
	if !ok || v != "value1" {
		t.Fatalf("expected key1=value1, got %v (ok=%v)", v, ok)
	}

	v, ok = m.Get("key2")
	if !ok || v != 42 {
		t.Fatalf("expected key2=42, got %v (ok=%v)", v, ok)
	}
}

func TestSyncMapGetMissing(t *testing.T) {
	m := NewSyncMap()
	v, ok := m.Get("nonexistent")
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
	if v != nil {
		t.Fatalf("expected nil value for missing key, got %v", v)
	}
}

func TestSyncMapRemove(t *testing.T) {
	m := NewSyncMap()
	m.Put("key1", "value1")
	m.Remove("key1")

	_, ok := m.Get("key1")
	if ok {
		t.Fatal("expected key to be removed")
	}
}

func TestSyncMapRemoveMissing(t *testing.T) {
	m := NewSyncMap()
	m.Remove("nonexistent")
}

func TestSyncMapClear(t *testing.T) {
	m := NewSyncMap()
	m.Put("k1", "v1")
	m.Put("k2", "v2")
	m.Put("k3", "v3")
	m.Clear()

	snap := m.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected empty map after clear, got %d entries", len(snap))
	}
}

func TestSyncMapSnapshot(t *testing.T) {
	m := NewSyncMap()
	m.Put("a", 1)
	m.Put("b", 2)

	snap := m.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(snap))
	}
	if snap["a"] != 1 || snap["b"] != 2 {
		t.Fatalf("unexpected snapshot contents: %v", snap)
	}

	snap["c"] = 3
	_, ok := m.Get("c")
	if ok {
		t.Fatal("modifying snapshot should not affect original map")
	}
}

func TestSyncMapOverwriteKey(t *testing.T) {
	m := NewSyncMap()
	m.Put("key", "first")
	m.Put("key", "second")

	v, ok := m.Get("key")
	if !ok || v != "second" {
		t.Fatalf("expected overwritten value 'second', got %v", v)
	}
}

func TestSyncMapConcurrency(t *testing.T) {
	m := NewSyncMap()
	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key"
			m.Put(key, i)
			m.Get(key)
			m.Snapshot()
		}(i)
	}
	wg.Wait()
}

// --- MapVariables tests ---

func TestNewMapVariables(t *testing.T) {
	mv := NewMapVariables()
	if mv == nil {
		t.Fatal("expected non-nil MapVariables")
	}
	if mv.GlobalMap() == nil {
		t.Fatal("expected non-nil global map")
	}
}

func TestMapVariablesGlobalMap(t *testing.T) {
	mv := NewMapVariables()
	g := mv.GlobalMap()
	g.Put("global_key", "global_val")

	v, ok := mv.GlobalMap().Get("global_key")
	if !ok || v != "global_val" {
		t.Fatal("expected to read value from global map")
	}
}

func TestMapVariablesChannelMapLazyCreation(t *testing.T) {
	mv := NewMapVariables()
	cm := mv.ChannelMap("ch-1")
	if cm == nil {
		t.Fatal("expected non-nil channel map")
	}
	cm.Put("ch_key", "ch_val")

	v, ok := mv.ChannelMap("ch-1").Get("ch_key")
	if !ok || v != "ch_val" {
		t.Fatal("expected to read value from lazily created channel map")
	}
}

func TestMapVariablesChannelMapReuse(t *testing.T) {
	mv := NewMapVariables()
	cm1 := mv.ChannelMap("ch-1")
	cm2 := mv.ChannelMap("ch-1")
	if cm1 != cm2 {
		t.Fatal("expected same channel map instance for same channel ID")
	}
}

func TestMapVariablesChannelMapIsolation(t *testing.T) {
	mv := NewMapVariables()
	mv.ChannelMap("ch-1").Put("key", "val1")
	mv.ChannelMap("ch-2").Put("key", "val2")

	v1, _ := mv.ChannelMap("ch-1").Get("key")
	v2, _ := mv.ChannelMap("ch-2").Get("key")
	if v1 == v2 {
		t.Fatal("different channels should have isolated maps")
	}
}

func TestMapVariablesResponseMapLazyCreation(t *testing.T) {
	mv := NewMapVariables()
	rm := mv.ResponseMap("ch-1")
	if rm == nil {
		t.Fatal("expected non-nil response map")
	}
	rm.Put("resp_key", "resp_val")

	v, ok := mv.ResponseMap("ch-1").Get("resp_key")
	if !ok || v != "resp_val" {
		t.Fatal("expected to read value from lazily created response map")
	}
}

func TestMapVariablesResponseMapReuse(t *testing.T) {
	mv := NewMapVariables()
	rm1 := mv.ResponseMap("ch-1")
	rm2 := mv.ResponseMap("ch-1")
	if rm1 != rm2 {
		t.Fatal("expected same response map instance for same channel ID")
	}
}

func TestMapVariablesResponseMapIsolation(t *testing.T) {
	mv := NewMapVariables()
	mv.ResponseMap("ch-1").Put("key", "a")
	mv.ResponseMap("ch-2").Put("key", "b")

	v1, _ := mv.ResponseMap("ch-1").Get("key")
	v2, _ := mv.ResponseMap("ch-2").Get("key")
	if v1 == v2 {
		t.Fatal("different channels should have isolated response maps")
	}
}

func TestMapVariablesChannelMapConcurrentLazyCreation(t *testing.T) {
	mv := NewMapVariables()
	var wg sync.WaitGroup
	results := make([]*SyncMap, 50)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = mv.ChannelMap("ch-concurrent")
		}(i)
	}
	wg.Wait()

	for i := 1; i < 50; i++ {
		if results[i] != results[0] {
			t.Fatal("concurrent ChannelMap calls should return the same instance")
		}
	}
}

// --- NewConnectorMap ---

func TestNewConnectorMap(t *testing.T) {
	cm := NewConnectorMap()
	if cm == nil {
		t.Fatal("expected non-nil connector map")
	}
	cm.Put("connector_key", "connector_val")
	v, ok := cm.Get("connector_key")
	if !ok || v != "connector_val" {
		t.Fatal("expected to read value from connector map")
	}
}

func TestNewConnectorMapIndependence(t *testing.T) {
	cm1 := NewConnectorMap()
	cm2 := NewConnectorMap()
	cm1.Put("key", "val1")

	_, ok := cm2.Get("key")
	if ok {
		t.Fatal("separate connector maps should be independent")
	}
}

// --- ExportForChannel ---

func TestExportForChannel(t *testing.T) {
	mv := NewMapVariables()
	mv.GlobalMap().Put("gKey", "gVal")
	mv.ChannelMap("ch-1").Put("cKey", "cVal")
	mv.ResponseMap("ch-1").Put("rKey", "rVal")

	export := mv.ExportForChannel("ch-1")
	if export == nil {
		t.Fatal("expected non-nil export")
	}

	gm, ok := export["globalMap"].(map[string]any)
	if !ok {
		t.Fatal("expected globalMap in export")
	}
	if gm["gKey"] != "gVal" {
		t.Fatalf("expected gKey=gVal in globalMap, got %v", gm["gKey"])
	}

	cm, ok := export["channelMap"].(map[string]any)
	if !ok {
		t.Fatal("expected channelMap in export")
	}
	if cm["cKey"] != "cVal" {
		t.Fatalf("expected cKey=cVal in channelMap, got %v", cm["cKey"])
	}

	rm, ok := export["responseMap"].(map[string]any)
	if !ok {
		t.Fatal("expected responseMap in export")
	}
	if rm["rKey"] != "rVal" {
		t.Fatalf("expected rKey=rVal in responseMap, got %v", rm["rKey"])
	}
}

func TestExportForChannelEmpty(t *testing.T) {
	mv := NewMapVariables()
	export := mv.ExportForChannel("ch-new")
	if export == nil {
		t.Fatal("expected non-nil export even for new channel")
	}
	gm := export["globalMap"].(map[string]any)
	if len(gm) != 0 {
		t.Fatalf("expected empty globalMap, got %v", gm)
	}
	cm := export["channelMap"].(map[string]any)
	if len(cm) != 0 {
		t.Fatalf("expected empty channelMap, got %v", cm)
	}
}
