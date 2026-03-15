package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ---------------------------------------------------------------------------
// MemoryStore Query with Since and Before
// ---------------------------------------------------------------------------

func TestMemoryStore_Query_Since(t *testing.T) {
	store := NewMemoryStore(0, 0)

	now := time.Now()
	old := now.Add(-2 * time.Hour)
	recent := now.Add(-30 * time.Minute)

	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Timestamp: old, Content: []byte("old")})
	store.Save(&MessageRecord{ID: "m2", Stage: "received", ChannelID: "ch1", Timestamp: recent, Content: []byte("recent")})
	store.Save(&MessageRecord{ID: "m3", Stage: "received", ChannelID: "ch1", Timestamp: now, Content: []byte("now")})

	results, err := store.Query(QueryOpts{Since: now.Add(-1 * time.Hour)})
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results (recent+now), got %d", len(results))
	}
}

func TestMemoryStore_Query_Before(t *testing.T) {
	store := NewMemoryStore(0, 0)

	now := time.Now()
	old := now.Add(-2 * time.Hour)
	recent := now.Add(-30 * time.Minute)

	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Timestamp: old, Content: []byte("old")})
	store.Save(&MessageRecord{ID: "m2", Stage: "received", ChannelID: "ch1", Timestamp: recent, Content: []byte("recent")})
	store.Save(&MessageRecord{ID: "m3", Stage: "received", ChannelID: "ch1", Timestamp: now, Content: []byte("now")})

	results, err := store.Query(QueryOpts{Before: now.Add(-1 * time.Hour)})
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result (old), got %d", len(results))
	}
}

func TestMemoryStore_Query_SinceAndBefore(t *testing.T) {
	store := NewMemoryStore(0, 0)

	now := time.Now()
	t1 := now.Add(-3 * time.Hour)
	t2 := now.Add(-2 * time.Hour)
	t3 := now.Add(-1 * time.Hour)

	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Timestamp: t1, Content: []byte("1")})
	store.Save(&MessageRecord{ID: "m2", Stage: "received", ChannelID: "ch1", Timestamp: t2, Content: []byte("2")})
	store.Save(&MessageRecord{ID: "m3", Stage: "received", ChannelID: "ch1", Timestamp: t3, Content: []byte("3")})
	store.Save(&MessageRecord{ID: "m4", Stage: "received", ChannelID: "ch1", Timestamp: now, Content: []byte("4")})

	results, err := store.Query(QueryOpts{
		Since:  t1.Add(30 * time.Minute),
		Before: now.Add(-30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results (t2+t3), got %d", len(results))
	}
}

func TestMemoryStore_Query_WithOffset(t *testing.T) {
	store := NewMemoryStore(0, 0)

	for i := 0; i < 10; i++ {
		store.Save(&MessageRecord{
			ID:        "m" + string(rune('0'+i)),
			Stage:     "received",
			ChannelID: "ch1",
			Timestamp: time.Now(),
			Content:   []byte("data"),
		})
	}

	results, err := store.Query(QueryOpts{Offset: 5, Limit: 3})
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results with offset, got %d", len(results))
	}
}

func TestMemoryStore_Count_WithSince(t *testing.T) {
	store := NewMemoryStore(0, 0)

	now := time.Now()
	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Timestamp: now.Add(-2 * time.Hour), Content: []byte("1")})
	store.Save(&MessageRecord{ID: "m2", Stage: "received", ChannelID: "ch1", Timestamp: now, Content: []byte("2")})

	count, err := store.Count(QueryOpts{Since: now.Add(-1 * time.Hour)})
	if err != nil {
		t.Fatalf("count error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}
}

func TestMemoryStore_Count_WithBefore(t *testing.T) {
	store := NewMemoryStore(0, 0)

	now := time.Now()
	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Timestamp: now.Add(-2 * time.Hour), Content: []byte("1")})
	store.Save(&MessageRecord{ID: "m2", Stage: "received", ChannelID: "ch1", Timestamp: now, Content: []byte("2")})

	count, err := store.Count(QueryOpts{Before: now.Add(-1 * time.Hour)})
	if err != nil {
		t.Fatalf("count error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// MemoryStore Len and BytesUsed tracking
// ---------------------------------------------------------------------------

func TestMemoryStore_LenTracking(t *testing.T) {
	store := NewMemoryStore(0, 0)

	if store.Len() != 0 {
		t.Errorf("expected 0, got %d", store.Len())
	}

	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Content: []byte("data1"), Timestamp: time.Now()})
	if store.Len() != 1 {
		t.Errorf("expected 1, got %d", store.Len())
	}

	store.Save(&MessageRecord{ID: "m2", Stage: "received", ChannelID: "ch1", Content: []byte("data2"), Timestamp: time.Now()})
	if store.Len() != 2 {
		t.Errorf("expected 2, got %d", store.Len())
	}

	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Content: []byte("updated"), Timestamp: time.Now()})
	if store.Len() != 2 {
		t.Errorf("expected 2 after update, got %d", store.Len())
	}

	store.Delete("m1")
	if store.Len() != 1 {
		t.Errorf("expected 1 after delete, got %d", store.Len())
	}
}

func TestMemoryStore_BytesUsedTracking(t *testing.T) {
	store := NewMemoryStore(0, 0)

	if store.BytesUsed() != 0 {
		t.Errorf("expected 0, got %d", store.BytesUsed())
	}

	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Content: []byte("data"), Timestamp: time.Now()})
	bytesAfterFirst := store.BytesUsed()
	if bytesAfterFirst <= 0 {
		t.Error("expected positive bytes after save")
	}

	store.Save(&MessageRecord{ID: "m2", Stage: "received", ChannelID: "ch1", Content: []byte("more data"), Timestamp: time.Now()})
	bytesAfterSecond := store.BytesUsed()
	if bytesAfterSecond <= bytesAfterFirst {
		t.Error("expected more bytes after second save")
	}

	store.Delete("m1")
	bytesAfterDelete := store.BytesUsed()
	if bytesAfterDelete >= bytesAfterSecond {
		t.Error("expected fewer bytes after delete")
	}
}

func TestMemoryStore_BytesUsed_UpdateInPlace(t *testing.T) {
	store := NewMemoryStore(0, 0)

	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Content: []byte("short"), Timestamp: time.Now()})
	bytesBefore := store.BytesUsed()

	store.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Content: []byte("much longer content here"), Timestamp: time.Now()})
	bytesAfter := store.BytesUsed()

	if bytesAfter <= bytesBefore {
		t.Error("expected more bytes after update with larger content")
	}
}

// ---------------------------------------------------------------------------
// CompositeStore delegation
// ---------------------------------------------------------------------------

func TestCompositeStore_GetStage_FullMode(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	inner.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Content: []byte("data"), Timestamp: time.Now()})

	cs := NewCompositeStore(inner, "full", nil)
	rec, err := cs.GetStage("m1", "received")
	if err != nil {
		t.Fatalf("GetStage error: %v", err)
	}
	if rec == nil {
		t.Fatal("expected record")
	}
	if rec.ID != "m1" {
		t.Errorf("expected m1, got %s", rec.ID)
	}
}

func TestCompositeStore_GetStage_NoneMode(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "none", nil)

	rec, err := cs.GetStage("m1", "received")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec != nil {
		t.Error("expected nil for none mode")
	}
}

func TestCompositeStore_Count_Delegation(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	inner.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Content: []byte("d"), Timestamp: time.Now()})
	inner.Save(&MessageRecord{ID: "m2", Stage: "received", ChannelID: "ch1", Content: []byte("d"), Timestamp: time.Now()})

	cs := NewCompositeStore(inner, "full", nil)
	count, err := cs.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("count error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2, got %d", count)
	}
}

func TestCompositeStore_Count_NoneMode(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "none", nil)
	count, err := cs.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("count error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 for none mode, got %d", count)
	}
}

func TestCompositeStore_Save_StatusMode(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "status", nil)

	err := cs.Save(&MessageRecord{
		ID:        "m1",
		Stage:     "received",
		ChannelID: "ch1",
		Content:   []byte("should be stripped"),
		Status:    "RECEIVED",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("save error: %v", err)
	}

	rec, _ := inner.GetStage("m1", "received")
	if rec == nil {
		t.Fatal("expected record in inner store")
	}
	if len(rec.Content) != 0 {
		t.Error("expected content to be stripped in status mode")
	}
}

func TestCompositeStore_Save_FullModeWithStages(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", []string{"received", "sent"})

	cs.Save(&MessageRecord{ID: "m1", Stage: "received", ChannelID: "ch1", Content: []byte("d"), Timestamp: time.Now()})
	cs.Save(&MessageRecord{ID: "m2", Stage: "transformed", ChannelID: "ch1", Content: []byte("d"), Timestamp: time.Now()})

	if inner.Len() != 1 {
		t.Errorf("expected 1 record (only 'received'), got %d", inner.Len())
	}
}

func TestCompositeStore_ShouldStore(t *testing.T) {
	tests := []struct {
		mode   string
		stages []string
		stage  string
		want   bool
	}{
		{"none", nil, "received", false},
		{"status", nil, "received", true},
		{"status", nil, "anything", true},
		{"full", nil, "received", true},
		{"full", []string{"received", "sent"}, "received", true},
		{"full", []string{"received", "sent"}, "transformed", false},
		{"unknown", nil, "received", true},
	}

	for _, tt := range tests {
		cs := NewCompositeStore(NewMemoryStore(0, 0), tt.mode, tt.stages)
		got := cs.ShouldStore(tt.stage)
		if got != tt.want {
			t.Errorf("ShouldStore(mode=%q, stages=%v, stage=%q) = %v, want %v",
				tt.mode, tt.stages, tt.stage, got, tt.want)
		}
	}
}

func TestCompositeStore_Mode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "status", nil)
	if cs.Mode() != "status" {
		t.Errorf("expected 'status', got %q", cs.Mode())
	}
}

func TestCompositeStore_DefaultMode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "", nil)
	if cs.Mode() != "full" {
		t.Errorf("expected default 'full', got %q", cs.Mode())
	}
}

func TestCompositeStore_Delete_NoneMode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "none", nil)
	err := cs.Delete("m1")
	if err != nil {
		t.Errorf("expected nil for none mode delete, got %v", err)
	}
}

func TestCompositeStore_Prune_NoneMode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "none", nil)
	pruned, err := cs.Prune(time.Now(), "")
	if err != nil {
		t.Fatalf("prune error: %v", err)
	}
	if pruned != 0 {
		t.Errorf("expected 0, got %d", pruned)
	}
}

func TestCompositeStore_Query_NoneMode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "none", nil)
	results, err := cs.Query(QueryOpts{})
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if results != nil {
		t.Error("expected nil results for none mode")
	}
}

func TestCompositeStore_Get_NoneMode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "none", nil)
	rec, err := cs.Get("m1")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if rec != nil {
		t.Error("expected nil for none mode")
	}
}

// ---------------------------------------------------------------------------
// NewMessageStore edge cases
// ---------------------------------------------------------------------------

func TestNewMessageStore_Nil(t *testing.T) {
	store, err := NewMessageStore(nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestNewMessageStore_EmptyDriver(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{Driver: ""})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestNewMessageStore_MemoryExplicit(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Memory: &config.StorageMemoryConfig{MaxRecords: 500, MaxBytes: 1024},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestNewMessageStore_UnsupportedDriver(t *testing.T) {
	_, err := NewMessageStore(&config.MessageStorageConfig{Driver: "mongodb"})
	if err == nil {
		t.Fatal("expected error for unsupported driver")
	}
}

func TestNewMessageStore_NoneMode(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Mode:   "none",
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}

	cs, ok := store.(*CompositeStore)
	if !ok {
		t.Fatal("expected CompositeStore for mode=none")
	}
	if cs.Mode() != "none" {
		t.Errorf("expected mode 'none', got %q", cs.Mode())
	}
}

func TestNewMessageStore_StatusMode(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Mode:   "status",
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	cs, ok := store.(*CompositeStore)
	if !ok {
		t.Fatal("expected CompositeStore for mode=status")
	}
	if cs.Mode() != "status" {
		t.Errorf("expected mode 'status', got %q", cs.Mode())
	}
}

func TestNewMessageStore_WithStagesNoMode(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Stages: []string{"received", "sent"},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	cs, ok := store.(*CompositeStore)
	if !ok {
		t.Fatal("expected CompositeStore when stages are set")
	}
	if cs.Mode() != "full" {
		t.Errorf("expected mode 'full' when stages set without mode, got %q", cs.Mode())
	}
}

func TestNewMessageStoreMax_PostgresNilConfig(t *testing.T) {
	_, err := NewMessageStore(&config.MessageStorageConfig{Driver: "postgres"})
	if err == nil {
		t.Fatal("expected error when postgres config is nil")
	}
}

func TestNewMessageStoreMax_S3NilConfig(t *testing.T) {
	_, err := NewMessageStore(&config.MessageStorageConfig{Driver: "s3"})
	if err == nil {
		t.Fatal("expected error when s3 config is nil")
	}
}

// ---------------------------------------------------------------------------
// MemoryStore: Eviction
// ---------------------------------------------------------------------------

func TestMemoryStore_EvictionByRecordCount(t *testing.T) {
	store := NewMemoryStore(3, 0)

	for i := 0; i < 5; i++ {
		store.Save(&MessageRecord{
			ID:        fmt.Sprintf("m%d", i),
			Stage:     "received",
			ChannelID: "ch1",
			Content:   []byte("data"),
			Timestamp: time.Now(),
		})
	}

	if store.Len() > 3 {
		t.Errorf("expected at most 3 records, got %d", store.Len())
	}
}

func TestMemoryStore_EvictionByBytes(t *testing.T) {
	store := NewMemoryStore(100000, 200)

	for i := 0; i < 20; i++ {
		store.Save(&MessageRecord{
			ID:        fmt.Sprintf("m%d", i),
			Stage:     "received",
			ChannelID: "ch1",
			Content:   []byte("some content that uses bytes"),
			Timestamp: time.Now(),
		})
	}

	if store.BytesUsed() > 200 {
		// Eviction should keep bytes under limit (allow some overhead)
		if store.BytesUsed() > 400 {
			t.Errorf("expected bytes under limit, got %d", store.BytesUsed())
		}
	}
}

