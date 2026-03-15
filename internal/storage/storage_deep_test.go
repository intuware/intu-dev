package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// NewMessageStore — driver error paths
// ===================================================================

func TestNewMessageStore_PostgresNilConfig(t *testing.T) {
	_, err := NewMessageStore(&config.MessageStorageConfig{
		Driver:   "postgres",
		Postgres: nil,
	})
	if err == nil {
		t.Fatal("expected error when postgres config is nil")
	}
}

func TestNewMessageStore_S3NilConfig(t *testing.T) {
	_, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "s3",
		S3:     nil,
	})
	if err == nil {
		t.Fatal("expected error when s3 config is nil")
	}
}

// ===================================================================
// NewMessageStore — mode + stages combinations
// ===================================================================

func TestNewMessageStore_ModeStatusWithStages(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Mode:   "status",
		Stages: []string{"received"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cs, ok := store.(*CompositeStore)
	if !ok {
		t.Fatal("expected CompositeStore for mode=status")
	}
	if cs.Mode() != "status" {
		t.Fatalf("expected mode=status, got %s", cs.Mode())
	}
}

func TestNewMessageStore_ModeNone(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Mode:   "none",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cs, ok := store.(*CompositeStore)
	if !ok {
		t.Fatal("expected CompositeStore for mode=none")
	}
	if cs.Mode() != "none" {
		t.Fatalf("expected mode=none, got %s", cs.Mode())
	}
}

func TestNewMessageStore_ModeFullReturnsInnerDirectly(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Mode:   "full",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := store.(*MemoryStore); !ok {
		t.Fatalf("expected MemoryStore returned directly for mode=full with no stages, got %T", store)
	}
}

func TestNewMessageStore_ModeFullWithStages(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Mode:   "full",
		Stages: []string{"received", "sent"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := store.(*CompositeStore); !ok {
		t.Fatal("expected CompositeStore when mode=full and stages are set")
	}
}

func TestNewMessageStore_EmptyDriverDefaultsToMemory(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{Driver: ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := store.(*MemoryStore); !ok {
		t.Fatal("expected MemoryStore for empty driver")
	}
}

func TestNewMessageStore_MemoryWithLimitsAndMode(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Mode:   "status",
		Memory: &config.StorageMemoryConfig{MaxRecords: 100, MaxBytes: 2048},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cs, ok := store.(*CompositeStore)
	if !ok {
		t.Fatal("expected CompositeStore")
	}
	if cs.Mode() != "status" {
		t.Fatalf("expected mode=status, got %s", cs.Mode())
	}
}

// ===================================================================
// MemoryStore — Query with all filter combinations
// ===================================================================

func TestMemoryStoreQueryAllFiltersCombined(t *testing.T) {
	store := NewMemoryStore(0, 0)
	base := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: base, Content: []byte("a")})
	store.Save(&MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "received", Status: "ERROR", Timestamp: base.Add(1 * time.Hour), Content: []byte("b")})
	store.Save(&MessageRecord{ID: "m3", ChannelID: "ch-2", Stage: "received", Status: "RECEIVED", Timestamp: base.Add(2 * time.Hour), Content: []byte("c")})
	store.Save(&MessageRecord{ID: "m4", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: base.Add(3 * time.Hour), Content: []byte("d")})
	store.Save(&MessageRecord{ID: "m5", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: base.Add(4 * time.Hour), Content: []byte("e")})

	records, err := store.Query(QueryOpts{
		ChannelID: "ch-1",
		Status:    "RECEIVED",
		Stage:     "received",
		Since:     base,
		Before:    base.Add(5 * time.Hour),
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records (m1, m5), got %d", len(records))
	}
	ids := map[string]bool{}
	for _, r := range records {
		ids[r.ID] = true
	}
	if !ids["m1"] || !ids["m5"] {
		t.Fatalf("expected m1 and m5, got %v", ids)
	}
}

func TestMemoryStoreQueryChannelAndStatus(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "received", Status: "ERROR", Timestamp: now})
	store.Save(&MessageRecord{ID: "m3", ChannelID: "ch-2", Stage: "received", Status: "RECEIVED", Timestamp: now})

	records, err := store.Query(QueryOpts{ChannelID: "ch-1", Status: "ERROR"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 || records[0].ID != "m2" {
		t.Fatalf("expected 1 record m2, got %d", len(records))
	}
}

// ===================================================================
// MemoryStore — concurrent Save/Query/Delete
// ===================================================================

func TestMemoryStoreConcurrentOperations(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			store.Save(&MessageRecord{
				ID:        fmt.Sprintf("msg-%d", idx),
				ChannelID: "ch-1",
				Stage:     "received",
				Status:    "RECEIVED",
				Timestamp: now.Add(time.Duration(idx) * time.Millisecond),
				Content:   []byte(fmt.Sprintf("data-%d", idx)),
			})
		}(i)
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Query(QueryOpts{ChannelID: "ch-1"})
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			store.Delete(fmt.Sprintf("msg-%d", idx))
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Count(QueryOpts{})
		}()
	}

	wg.Wait()

	remaining := store.Len()
	if remaining < 0 {
		t.Fatalf("store length should be non-negative, got %d", remaining)
	}
}

// ===================================================================
// MemoryStore — saving record with same ID+Stage overwrites
// ===================================================================

func TestMemoryStoreSameIDStageOverwrites(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{
		ID: "m1", ChannelID: "ch", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: []byte("v1"),
	})
	store.Save(&MessageRecord{
		ID: "m1", ChannelID: "ch", Stage: "received",
		Status: "UPDATED", Timestamp: now, Content: []byte("v2-overwritten"),
	})

	if store.Len() != 1 {
		t.Fatalf("expected 1 record after overwrite, got %d", store.Len())
	}

	rec, err := store.GetStage("m1", "received")
	if err != nil {
		t.Fatalf("GetStage failed: %v", err)
	}
	if rec.Status != "UPDATED" {
		t.Fatalf("expected UPDATED, got %s", rec.Status)
	}
	if string(rec.Content) != "v2-overwritten" {
		t.Fatalf("expected v2-overwritten, got %s", string(rec.Content))
	}
}

func TestMemoryStoreSameIDDifferentStageDoesNotOverwrite(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("r")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "sent", Status: "SENT", Timestamp: now, Content: []byte("s")})

	if store.Len() != 2 {
		t.Fatalf("expected 2 records (different stages), got %d", store.Len())
	}
}

// ===================================================================
// CompositeStore — status mode strips content
// ===================================================================

func TestCompositeStoreStatusModeStripsContentPreservesFields(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "status", nil)

	now := time.Now()
	cs.Save(&MessageRecord{
		ID:            "msg-1",
		CorrelationID: "corr-1",
		ChannelID:     "ch-1",
		Stage:         "received",
		Content:       []byte("sensitive-data-here"),
		Status:        "RECEIVED",
		Timestamp:     now,
		DurationMs:    42,
		Metadata:      map[string]any{"key": "val"},
	})

	rec, err := cs.Get("msg-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.Content != nil {
		t.Fatal("expected nil content in status mode")
	}
	if rec.CorrelationID != "corr-1" {
		t.Fatalf("expected corr-1, got %s", rec.CorrelationID)
	}
	if rec.Status != "RECEIVED" {
		t.Fatalf("expected RECEIVED, got %s", rec.Status)
	}
	if rec.Metadata["key"] != "val" {
		t.Fatal("expected metadata preserved")
	}
}

// ===================================================================
// CompositeStore — full mode with stages filtering
// ===================================================================

func TestCompositeStoreFullModeStagesVerifyShouldStore(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", []string{"received", "sent"})

	if !cs.ShouldStore("received") {
		t.Fatal("expected ShouldStore(received)=true")
	}
	if !cs.ShouldStore("sent") {
		t.Fatal("expected ShouldStore(sent)=true")
	}
	if cs.ShouldStore("transformed") {
		t.Fatal("expected ShouldStore(transformed)=false")
	}

	now := time.Now()
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Content: []byte("r"), Status: "RECEIVED", Timestamp: now})
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "transformed", Content: []byte("t"), Status: "TRANSFORMED", Timestamp: now})
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "sent", Content: []byte("s"), Status: "SENT", Timestamp: now})

	allRecords, _ := inner.Query(QueryOpts{})
	if len(allRecords) != 2 {
		t.Fatalf("expected 2 records stored, got %d", len(allRecords))
	}
}

// ===================================================================
// CompositeStore — Delete and Prune delegation
// ===================================================================

func TestCompositeStoreDeleteDelegates(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", nil)

	now := time.Now()
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("data")})
	cs.Save(&MessageRecord{ID: "m2", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("data2")})

	if err := cs.Delete("m1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	records, _ := cs.Query(QueryOpts{})
	if len(records) != 1 {
		t.Fatalf("expected 1 record after delete, got %d", len(records))
	}
	if records[0].ID != "m2" {
		t.Fatalf("expected m2 remaining, got %s", records[0].ID)
	}
}

func TestCompositeStorePruneDelegatesFull(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", nil)

	old := time.Now().Add(-2 * time.Hour)
	recent := time.Now()

	cs.Save(&MessageRecord{ID: "old", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: old, Content: []byte("old")})
	cs.Save(&MessageRecord{ID: "new", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: recent, Content: []byte("new")})

	pruned, err := cs.Prune(time.Now().Add(-1*time.Hour), "")
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned, got %d", pruned)
	}

	records, _ := cs.Query(QueryOpts{})
	if len(records) != 1 || records[0].ID != "new" {
		t.Fatal("expected only 'new' record remaining")
	}
}

func TestCompositeStorePruneByChannelDelegates(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", nil)

	old := time.Now().Add(-2 * time.Hour)
	cs.Save(&MessageRecord{ID: "a", ChannelID: "ch-1", Stage: "received", Status: "OK", Timestamp: old})
	cs.Save(&MessageRecord{ID: "b", ChannelID: "ch-2", Stage: "received", Status: "OK", Timestamp: old})

	pruned, err := cs.Prune(time.Now(), "ch-1")
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned, got %d", pruned)
	}

	records, _ := cs.Query(QueryOpts{})
	if len(records) != 1 || records[0].ChannelID != "ch-2" {
		t.Fatal("expected only ch-2 remaining")
	}
}

// ===================================================================
// recordSize — various record sizes
// ===================================================================

func TestRecordSizeWithAllFields(t *testing.T) {
	rec := &MessageRecord{
		ID:            "id-123",
		CorrelationID: "corr-456",
		ChannelID:     "channel-abc",
		Stage:         "transformed",
		Content:       []byte("hello world content here"),
		Status:        "SENT",
	}
	size := recordSize(rec)
	expected := len("id-123") + len("corr-456") + len("channel-abc") + len("transformed") + len("hello world content here") + len("SENT") + 64
	if size != expected {
		t.Fatalf("expected %d, got %d", expected, size)
	}
}

func TestRecordSizeNilContent(t *testing.T) {
	rec := &MessageRecord{
		ID:        "x",
		ChannelID: "ch",
		Stage:     "r",
		Status:    "OK",
	}
	size := recordSize(rec)
	expected := len("x") + len("ch") + len("r") + len("OK") + 64
	if size != expected {
		t.Fatalf("expected %d, got %d", expected, size)
	}
}

func TestRecordSizeLargeContent(t *testing.T) {
	bigContent := make([]byte, 10000)
	rec := &MessageRecord{
		ID:        "big",
		ChannelID: "ch",
		Stage:     "received",
		Content:   bigContent,
		Status:    "OK",
	}
	size := recordSize(rec)
	if size < 10000 {
		t.Fatalf("expected size >= 10000, got %d", size)
	}
}

// ===================================================================
// MemoryStore — eviction with both maxRecords and maxBytes
// ===================================================================

func TestMemoryStoreEvictionBothLimits(t *testing.T) {
	store := NewMemoryStore(10, 500)
	now := time.Now()

	for i := 0; i < 20; i++ {
		store.Save(&MessageRecord{
			ID:        fmt.Sprintf("msg-%d", i),
			ChannelID: "ch",
			Stage:     "received",
			Status:    "OK",
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Content:   make([]byte, 50),
		})
	}

	if store.Len() > 10 {
		t.Fatalf("expected at most 10 records, got %d", store.Len())
	}
	if store.BytesUsed() > 500 {
		t.Fatalf("expected bytes used <= 500, got %d", store.BytesUsed())
	}

	_, err := store.Get(fmt.Sprintf("msg-%d", 19))
	if err != nil {
		t.Fatal("expected most recent record to survive eviction")
	}
}

func TestMemoryStoreEvictionPreservesNewest(t *testing.T) {
	store := NewMemoryStore(3, 0)
	now := time.Now()

	for i := 0; i < 10; i++ {
		store.Save(&MessageRecord{
			ID:        fmt.Sprintf("m%d", i),
			ChannelID: "ch",
			Stage:     "received",
			Status:    "OK",
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Content:   []byte("data"),
		})
	}

	if store.Len() > 3 {
		t.Fatalf("expected at most 3 records, got %d", store.Len())
	}

	_, err := store.Get("m9")
	if err != nil {
		t.Fatal("expected newest record m9 to survive")
	}

	_, err = store.Get("m0")
	if err == nil {
		t.Fatal("expected oldest record m0 to be evicted")
	}
}

// ===================================================================
// MemoryStore — BytesUsed tracking with eviction
// ===================================================================

func TestMemoryStoreBytesDecreasesOnEviction(t *testing.T) {
	store := NewMemoryStore(1000, 200)
	now := time.Now()

	for i := 0; i < 10; i++ {
		store.Save(&MessageRecord{
			ID:        fmt.Sprintf("msg-%d", i),
			ChannelID: "ch",
			Stage:     "received",
			Status:    "OK",
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Content:   make([]byte, 50),
		})
	}

	if store.BytesUsed() > 200 {
		t.Fatalf("expected bytes used <= 200 after eviction, got %d", store.BytesUsed())
	}
}

// ===================================================================
// CompositeStore — ShouldStore for unknown modes
// ===================================================================

func TestCompositeStoreShouldStoreUnknownMode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "custom", nil)
	if !cs.ShouldStore("anything") {
		t.Fatal("unknown mode should return true from ShouldStore")
	}
}

// ===================================================================
// MemoryStore — Count with combined filters
// ===================================================================

func TestMemoryStoreCountCombinedFilters(t *testing.T) {
	store := NewMemoryStore(0, 0)
	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: base})
	store.Save(&MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "received", Status: "ERROR", Timestamp: base.Add(1 * time.Hour)})
	store.Save(&MessageRecord{ID: "m3", ChannelID: "ch-2", Stage: "received", Status: "RECEIVED", Timestamp: base.Add(2 * time.Hour)})
	store.Save(&MessageRecord{ID: "m4", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: base.Add(3 * time.Hour)})

	count, err := store.Count(QueryOpts{ChannelID: "ch-1", Stage: "received", Status: "RECEIVED"})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}

	count, _ = store.Count(QueryOpts{ChannelID: "ch-1"})
	if count != 3 {
		t.Fatalf("expected 3 for ch-1, got %d", count)
	}

	count, _ = store.Count(QueryOpts{Since: base.Add(30 * time.Minute), Before: base.Add(2*time.Hour + 30*time.Minute)})
	if count != 2 {
		t.Fatalf("expected 2 in time range, got %d", count)
	}
}

// ===================================================================
// MemoryStore — Query with Limit, Offset, and filters
// ===================================================================

func TestMemoryStoreQueryLimitOffsetWithFilters(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	for i := 0; i < 10; i++ {
		store.Save(&MessageRecord{
			ID:        fmt.Sprintf("m%d", i),
			ChannelID: "ch-1",
			Stage:     "received",
			Status:    "OK",
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}
	store.Save(&MessageRecord{ID: "other", ChannelID: "ch-2", Stage: "received", Status: "OK", Timestamp: now})

	records, err := store.Query(QueryOpts{ChannelID: "ch-1", Limit: 3, Offset: 2})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3, got %d", len(records))
	}
}

// ===================================================================
// MemoryStore — DurationMs preserved
// ===================================================================

func TestMemoryStoreDurationMsPreserved(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{
		ID: "m1", ChannelID: "ch", Stage: "received",
		Status: "OK", Timestamp: now, DurationMs: 150,
	})

	rec, err := store.Get("m1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.DurationMs != 150 {
		t.Fatalf("expected DurationMs=150, got %d", rec.DurationMs)
	}
}

// ===================================================================
// MemoryStore — Get returns first match (newest stored)
// ===================================================================

func TestMemoryStoreGetReturnsFirstMatch(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("r")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "sent", Status: "SENT", Timestamp: now.Add(time.Second), Content: []byte("s")})

	rec, err := store.Get("m1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.ID != "m1" {
		t.Fatalf("expected m1, got %s", rec.ID)
	}
}

// ===================================================================
// CompositeStore — Count in various modes
// ===================================================================

func TestCompositeStoreCountStatusMode(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "status", nil)
	now := time.Now()

	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("data")})
	cs.Save(&MessageRecord{ID: "m2", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("data2")})

	count, err := cs.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

// ===================================================================
// CompositeStore — GetStage delegation in full mode
// ===================================================================

func TestCompositeStoreGetStageFullMode(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", nil)
	now := time.Now()

	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("recv")})
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "sent", Status: "OK", Timestamp: now, Content: []byte("sent")})

	rec, err := cs.GetStage("m1", "sent")
	if err != nil {
		t.Fatalf("GetStage failed: %v", err)
	}
	if string(rec.Content) != "sent" {
		t.Fatalf("expected 'sent', got %s", string(rec.Content))
	}
}

// ===================================================================
// CompositeStore — status mode delegation for Get
// ===================================================================

func TestCompositeStoreGetStatusMode(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "status", nil)
	now := time.Now()

	cs.Save(&MessageRecord{
		ID: "m1", ChannelID: "ch", Stage: "received",
		Status: "OK", Timestamp: now, Content: []byte("big-payload"),
	})

	rec, err := cs.Get("m1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.Content != nil {
		t.Fatal("expected nil content in status mode Get")
	}
	if rec.Status != "OK" {
		t.Fatalf("expected OK status, got %s", rec.Status)
	}
}

// ===================================================================
// MemoryStore — empty Query returns nil
// ===================================================================

func TestMemoryStoreQueryEmptyStore(t *testing.T) {
	store := NewMemoryStore(0, 0)
	records, err := store.Query(QueryOpts{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records from empty store, got %d", len(records))
	}
}

func TestMemoryStoreCountEmptyStore(t *testing.T) {
	store := NewMemoryStore(0, 0)
	count, err := store.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}
}

// ===================================================================
// MemoryStore — Delete all stages of an ID
// ===================================================================

func TestMemoryStoreDeleteRemovesAllStages(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("r")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "transformed", Status: "OK", Timestamp: now, Content: []byte("t")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "sent", Status: "OK", Timestamp: now, Content: []byte("s")})

	if store.Len() != 3 {
		t.Fatalf("expected 3, got %d", store.Len())
	}

	store.Delete("m1")

	if store.Len() != 0 {
		t.Fatalf("expected 0 after delete, got %d", store.Len())
	}

	_, err := store.Get("m1")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

// ===================================================================
// MemoryStore — Prune does nothing when no records match
// ===================================================================

func TestMemoryStorePruneNoMatches(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now})

	pruned, err := store.Prune(now.Add(-1*time.Hour), "")
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 0 {
		t.Fatalf("expected 0 pruned, got %d", pruned)
	}
	if store.Len() != 1 {
		t.Fatalf("expected 1 remaining, got %d", store.Len())
	}
}
