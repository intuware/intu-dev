package storage

import (
	"testing"
	"time"

	"github.com/intuware/intu/pkg/config"
)

func TestNewMessageStoreMemoryDefault(t *testing.T) {
	store, err := NewMessageStore(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := store.(*MemoryStore); !ok {
		t.Fatal("expected MemoryStore when config is nil")
	}
}

func TestNewMessageStoreMemoryExplicit(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{Driver: "memory"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := store.(*MemoryStore); !ok {
		t.Fatal("expected MemoryStore with driver=memory")
	}
}

func TestNewMessageStorePostgresRequiresConfig(t *testing.T) {
	_, err := NewMessageStore(&config.MessageStorageConfig{Driver: "postgres"})
	if err == nil {
		t.Fatal("expected error when postgres config is nil")
	}
}

func TestNewMessageStoreS3RequiresConfig(t *testing.T) {
	_, err := NewMessageStore(&config.MessageStorageConfig{Driver: "s3"})
	if err == nil {
		t.Fatal("expected error when s3 config is nil")
	}
}

func TestNewMessageStoreUnsupportedDriver(t *testing.T) {
	_, err := NewMessageStore(&config.MessageStorageConfig{Driver: "redis"})
	if err == nil {
		t.Fatal("expected error for unsupported driver")
	}
}

func TestNewMessageStoreWithModeReturnsCompositeStore(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Mode:   "status",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cs, ok := store.(*CompositeStore)
	if !ok {
		t.Fatal("expected CompositeStore when mode is set")
	}
	if cs.Mode() != "status" {
		t.Fatalf("expected mode=status, got %s", cs.Mode())
	}
}

func TestNewMessageStoreWithStagesReturnsCompositeStore(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Stages: []string{"received", "sent"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := store.(*CompositeStore); !ok {
		t.Fatal("expected CompositeStore when stages are set")
	}
}

func TestCompositeStoreNoneModeSkipsAllOperations(t *testing.T) {
	inner := NewMemoryStore()
	cs := NewCompositeStore(inner, "none", nil)

	record := &MessageRecord{
		ID:        "msg-1",
		ChannelID: "ch-1",
		Stage:     "received",
		Status:    "RECEIVED",
		Timestamp: time.Now(),
	}

	if err := cs.Save(record); err != nil {
		t.Fatalf("save should succeed (no-op): %v", err)
	}

	if !cs.ShouldStore("received") == true {
		// none mode should return false
	}

	rec, err := cs.Get("msg-1")
	if err != nil {
		t.Fatalf("get should not error in none mode: %v", err)
	}
	if rec != nil {
		t.Fatal("expected nil record in none mode")
	}

	records, err := cs.Query(QueryOpts{})
	if err != nil {
		t.Fatalf("query should not error in none mode: %v", err)
	}
	if records != nil {
		t.Fatal("expected nil records in none mode")
	}

	if err := cs.Delete("msg-1"); err != nil {
		t.Fatalf("delete should succeed in none mode: %v", err)
	}

	pruned, err := cs.Prune(time.Now(), "")
	if err != nil {
		t.Fatalf("prune should succeed in none mode: %v", err)
	}
	if pruned != 0 {
		t.Fatalf("expected 0 pruned in none mode, got %d", pruned)
	}
}

func TestCompositeStoreStatusModeStripsContent(t *testing.T) {
	inner := NewMemoryStore()
	cs := NewCompositeStore(inner, "status", nil)

	record := &MessageRecord{
		ID:        "msg-1",
		ChannelID: "ch-1",
		Stage:     "received",
		Content:   []byte("sensitive data"),
		Status:    "RECEIVED",
		Timestamp: time.Now(),
		Metadata:  map[string]any{"key": "val"},
	}

	if err := cs.Save(record); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	rec, err := cs.Get("msg-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec == nil {
		t.Fatal("expected record, got nil")
	}
	if rec.Content != nil {
		t.Fatalf("expected nil content in status mode, got %s", string(rec.Content))
	}
	if rec.Status != "RECEIVED" {
		t.Fatalf("expected RECEIVED status, got %s", rec.Status)
	}
	if rec.Metadata["key"] != "val" {
		t.Fatal("expected metadata to be preserved")
	}
}

func TestCompositeStoreFullModeWithStageFiltering(t *testing.T) {
	inner := NewMemoryStore()
	cs := NewCompositeStore(inner, "full", []string{"received", "sent"})

	now := time.Now()

	records := []*MessageRecord{
		{ID: "msg-1", ChannelID: "ch-1", Stage: "received", Content: []byte("data"), Status: "RECEIVED", Timestamp: now},
		{ID: "msg-1", ChannelID: "ch-1", Stage: "transformed", Content: []byte("data2"), Status: "TRANSFORMED", Timestamp: now},
		{ID: "msg-1", ChannelID: "ch-1", Stage: "sent", Content: []byte("data3"), Status: "SENT", Timestamp: now},
		{ID: "msg-1", ChannelID: "ch-1", Stage: "error", Content: []byte("data4"), Status: "ERROR", Timestamp: now},
	}

	for _, r := range records {
		if err := cs.Save(r); err != nil {
			t.Fatalf("save failed: %v", err)
		}
	}

	allRecords, err := inner.Query(QueryOpts{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(allRecords) != 2 {
		t.Fatalf("expected 2 records (received + sent), got %d", len(allRecords))
	}

	stages := make(map[string]bool)
	for _, r := range allRecords {
		stages[r.Stage] = true
	}
	if !stages["received"] {
		t.Fatal("expected 'received' stage to be stored")
	}
	if !stages["sent"] {
		t.Fatal("expected 'sent' stage to be stored")
	}
	if stages["transformed"] {
		t.Fatal("did not expect 'transformed' stage to be stored")
	}
	if stages["error"] {
		t.Fatal("did not expect 'error' stage to be stored")
	}
}

func TestCompositeStoreFullModeNoStagesStoresAll(t *testing.T) {
	inner := NewMemoryStore()
	cs := NewCompositeStore(inner, "full", nil)

	now := time.Now()
	stages := []string{"received", "transformed", "sent", "error"}

	for _, stage := range stages {
		if err := cs.Save(&MessageRecord{
			ID:        "msg-1",
			ChannelID: "ch-1",
			Stage:     stage,
			Content:   []byte("data"),
			Status:    "OK",
			Timestamp: now,
		}); err != nil {
			t.Fatalf("save failed: %v", err)
		}
	}

	allRecords, err := inner.Query(QueryOpts{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(allRecords) != 4 {
		t.Fatalf("expected 4 records, got %d", len(allRecords))
	}
}

func TestCompositeStoreShouldStore(t *testing.T) {
	tests := []struct {
		mode   string
		stages []string
		stage  string
		want   bool
	}{
		{"none", nil, "received", false},
		{"none", nil, "sent", false},
		{"status", nil, "received", true},
		{"status", nil, "anything", true},
		{"full", nil, "received", true},
		{"full", nil, "anything", true},
		{"full", []string{"received", "sent"}, "received", true},
		{"full", []string{"received", "sent"}, "sent", true},
		{"full", []string{"received", "sent"}, "transformed", false},
		{"full", []string{"received", "sent"}, "error", false},
	}

	for _, tt := range tests {
		cs := NewCompositeStore(NewMemoryStore(), tt.mode, tt.stages)
		got := cs.ShouldStore(tt.stage)
		if got != tt.want {
			t.Errorf("ShouldStore(mode=%s, stages=%v, stage=%s) = %v, want %v",
				tt.mode, tt.stages, tt.stage, got, tt.want)
		}
	}
}

func TestCompositeStoreDefaultsToFull(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(), "", nil)
	if cs.Mode() != "full" {
		t.Fatalf("expected default mode=full, got %s", cs.Mode())
	}
}

func TestCompositeStoreStatusModeDelegatesReadOps(t *testing.T) {
	inner := NewMemoryStore()
	cs := NewCompositeStore(inner, "status", nil)

	now := time.Now()
	record := &MessageRecord{
		ID:        "msg-1",
		ChannelID: "ch-1",
		Stage:     "received",
		Content:   []byte("test data"),
		Status:    "RECEIVED",
		Timestamp: now,
	}

	if err := cs.Save(record); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	records, err := cs.Query(QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	if err := cs.Delete("msg-1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	records, err = cs.Query(QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query after delete failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records after delete, got %d", len(records))
	}
}

func TestCompositeStorePruneDelegates(t *testing.T) {
	inner := NewMemoryStore()
	cs := NewCompositeStore(inner, "full", nil)

	past := time.Now().Add(-1 * time.Hour)
	record := &MessageRecord{
		ID:        "msg-1",
		ChannelID: "ch-1",
		Stage:     "received",
		Content:   []byte("data"),
		Status:    "RECEIVED",
		Timestamp: past,
	}
	if err := cs.Save(record); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	pruned, err := cs.Prune(time.Now(), "")
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned, got %d", pruned)
	}
}

func TestMemoryStoreBasicOperations(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()

	if err := store.Save(&MessageRecord{
		ID:        "msg-1",
		ChannelID: "ch-1",
		Stage:     "received",
		Content:   []byte("hello"),
		Status:    "RECEIVED",
		Timestamp: now,
	}); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	if err := store.Save(&MessageRecord{
		ID:        "msg-1",
		ChannelID: "ch-1",
		Stage:     "sent",
		Content:   []byte("hello transformed"),
		Status:    "SENT",
		Timestamp: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	rec, err := store.Get("msg-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.ID != "msg-1" {
		t.Fatalf("expected msg-1, got %s", rec.ID)
	}

	records, err := store.Query(QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	records, err = store.Query(QueryOpts{Status: "SENT"})
	if err != nil {
		t.Fatalf("query by status failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 sent record, got %d", len(records))
	}

	if err := store.Delete("msg-1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = store.Get("msg-1")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestMemoryStorePrune(t *testing.T) {
	store := NewMemoryStore()
	past := time.Now().Add(-2 * time.Hour)
	recent := time.Now()

	store.Save(&MessageRecord{
		ID: "old", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: past,
	})
	store.Save(&MessageRecord{
		ID: "new", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: recent,
	})

	pruned, err := store.Prune(time.Now().Add(-1*time.Hour), "")
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned, got %d", pruned)
	}

	records, _ := store.Query(QueryOpts{})
	if len(records) != 1 {
		t.Fatalf("expected 1 record remaining, got %d", len(records))
	}
	if records[0].ID != "new" {
		t.Fatalf("expected 'new' record remaining, got %s", records[0].ID)
	}
}

func TestMemoryStorePruneByChannel(t *testing.T) {
	store := NewMemoryStore()
	past := time.Now().Add(-2 * time.Hour)

	store.Save(&MessageRecord{
		ID: "msg-a", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: past,
	})
	store.Save(&MessageRecord{
		ID: "msg-b", ChannelID: "ch-2", Stage: "received",
		Status: "RECEIVED", Timestamp: past,
	})

	pruned, err := store.Prune(time.Now(), "ch-1")
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned, got %d", pruned)
	}

	records, _ := store.Query(QueryOpts{})
	if len(records) != 1 {
		t.Fatalf("expected 1 record remaining, got %d", len(records))
	}
	if records[0].ChannelID != "ch-2" {
		t.Fatalf("expected ch-2 remaining, got %s", records[0].ChannelID)
	}
}
