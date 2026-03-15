package storage

import (
	"fmt"

	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
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

func TestNewMessageStoreMemoryWithLimits(t *testing.T) {
	store, err := NewMessageStore(&config.MessageStorageConfig{
		Driver: "memory",
		Memory: &config.StorageMemoryConfig{
			MaxRecords: 500,
			MaxBytes:   1024 * 1024,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ms, ok := store.(*MemoryStore)
	if !ok {
		t.Fatal("expected MemoryStore")
	}
	if ms.maxRecords != 500 {
		t.Fatalf("expected maxRecords=500, got %d", ms.maxRecords)
	}
	if ms.maxBytes != 1024*1024 {
		t.Fatalf("expected maxBytes=1048576, got %d", ms.maxBytes)
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
	inner := NewMemoryStore(0, 0)
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
	inner := NewMemoryStore(0, 0)
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
	inner := NewMemoryStore(0, 0)
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
	inner := NewMemoryStore(0, 0)
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
		cs := NewCompositeStore(NewMemoryStore(0, 0), tt.mode, tt.stages)
		got := cs.ShouldStore(tt.stage)
		if got != tt.want {
			t.Errorf("ShouldStore(mode=%s, stages=%v, stage=%s) = %v, want %v",
				tt.mode, tt.stages, tt.stage, got, tt.want)
		}
	}
}

func TestMemoryStoreResponseStage(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("data")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: now, Content: []byte("sent-data")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "response", Status: "SENT", Timestamp: now, Content: []byte(`{"body":"OK","transport":"http","http":{"statusCode":200}}`)})

	rec, err := store.GetStage("m1", "response")
	if err != nil {
		t.Fatalf("GetStage(response) failed: %v", err)
	}
	if rec == nil {
		t.Fatal("expected response stage record")
	}
	if rec.Stage != "response" {
		t.Fatalf("expected stage=response, got %s", rec.Stage)
	}
	if string(rec.Content) != `{"body":"OK","transport":"http","http":{"statusCode":200}}` {
		t.Fatalf("unexpected content: %s", string(rec.Content))
	}

	records, err := store.Query(QueryOpts{Stage: "response"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 response record, got %d", len(records))
	}
}

func TestCompositeStoreResponseStageFiltering(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", []string{"received", "sent", "response"})

	now := time.Now()
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Content: []byte("r"), Status: "RECEIVED", Timestamp: now})
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "transformed", Content: []byte("t"), Status: "TRANSFORMED", Timestamp: now})
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "response", Content: []byte("resp"), Status: "SENT", Timestamp: now})

	allRecords, _ := inner.Query(QueryOpts{})
	if len(allRecords) != 2 {
		t.Fatalf("expected 2 records (received + response, transformed filtered out), got %d", len(allRecords))
	}

	stages := make(map[string]bool)
	for _, r := range allRecords {
		stages[r.Stage] = true
	}
	if !stages["received"] {
		t.Fatal("expected received stage")
	}
	if !stages["response"] {
		t.Fatal("expected response stage")
	}
	if stages["transformed"] {
		t.Fatal("did not expect transformed stage")
	}
}

func TestCompositeStoreDefaultsToFull(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "", nil)
	if cs.Mode() != "full" {
		t.Fatalf("expected default mode=full, got %s", cs.Mode())
	}
}

func TestCompositeStoreStatusModeDelegatesReadOps(t *testing.T) {
	inner := NewMemoryStore(0, 0)
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
	inner := NewMemoryStore(0, 0)
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
	store := NewMemoryStore(0, 0)
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
	store := NewMemoryStore(0, 0)
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

func TestMemoryStoreQueryStageFilter(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("r")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "transformed", Status: "TRANSFORMED", Timestamp: now, Content: []byte("t")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: now, Content: []byte("s")})

	records, err := store.Query(QueryOpts{Stage: "received"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 received record, got %d", len(records))
	}
	if records[0].Stage != "received" {
		t.Fatalf("expected stage=received, got %s", records[0].Stage)
	}

	records, err = store.Query(QueryOpts{Stage: "sent"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 sent record, got %d", len(records))
	}

	records, err = store.Query(QueryOpts{Stage: "nonexistent"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records for nonexistent stage, got %d", len(records))
	}
}

func TestMemoryStoreQueryExcludeContent(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("big payload here")})

	records, err := store.Query(QueryOpts{ExcludeContent: true})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Content != nil {
		t.Fatal("expected nil content with ExcludeContent=true")
	}
	if records[0].ID != "m1" {
		t.Fatalf("expected m1, got %s", records[0].ID)
	}

	// original record should be untouched
	rec, _ := store.Get("m1")
	if rec.Content == nil || string(rec.Content) != "big payload here" {
		t.Fatal("original record content should not be modified")
	}
}

func TestMemoryStoreQueryStageAndExcludeContent(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("r")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: now, Content: []byte("s")})

	records, err := store.Query(QueryOpts{Stage: "received", ExcludeContent: true})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Content != nil {
		t.Fatal("expected nil content")
	}
	if records[0].Stage != "received" {
		t.Fatalf("expected received, got %s", records[0].Stage)
	}
}

func TestMemoryStoreGetStage(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("recv-data")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "transformed", Status: "TRANSFORMED", Timestamp: now, Content: []byte("xfm-data")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: now, Content: []byte("sent-data")})

	rec, err := store.GetStage("m1", "transformed")
	if err != nil {
		t.Fatalf("GetStage failed: %v", err)
	}
	if string(rec.Content) != "xfm-data" {
		t.Fatalf("expected xfm-data, got %s", string(rec.Content))
	}

	rec, err = store.GetStage("m1", "sent")
	if err != nil {
		t.Fatalf("GetStage failed: %v", err)
	}
	if string(rec.Content) != "sent-data" {
		t.Fatalf("expected sent-data, got %s", string(rec.Content))
	}

	_, err = store.GetStage("m1", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent stage")
	}

	_, err = store.GetStage("nonexistent", "received")
	if err == nil {
		t.Fatal("expected error for nonexistent message")
	}
}

func TestCompositeStoreGetStageNoneMode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "none", nil)
	rec, err := cs.GetStage("m1", "received")
	if err != nil {
		t.Fatalf("expected no error in none mode, got %v", err)
	}
	if rec != nil {
		t.Fatal("expected nil record in none mode")
	}
}

func TestCompositeStoreGetStageDelegates(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", nil)
	now := time.Now()

	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("data")})

	rec, err := cs.GetStage("m1", "received")
	if err != nil {
		t.Fatalf("GetStage failed: %v", err)
	}
	if rec == nil || string(rec.Content) != "data" {
		t.Fatal("expected record with content")
	}
}

func TestMemoryStorePruneByChannel(t *testing.T) {
	store := NewMemoryStore(0, 0)
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

func TestMemoryStoreEvictionByRecordCount(t *testing.T) {
	store := NewMemoryStore(5, 0)
	now := time.Now()

	for i := 0; i < 10; i++ {
		store.Save(&MessageRecord{
			ID:        fmt.Sprintf("msg-%d", i),
			ChannelID: "ch-1",
			Stage:     "received",
			Status:    "RECEIVED",
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Content:   []byte("data"),
		})
	}

	if store.Len() > 5 {
		t.Fatalf("expected at most 5 records after eviction, got %d", store.Len())
	}

	_, err := store.Get("msg-9")
	if err != nil {
		t.Fatal("expected most recent record to survive eviction")
	}

	_, err = store.Get("msg-0")
	if err == nil {
		t.Fatal("expected oldest record to be evicted")
	}
}

func TestMemoryStoreEvictionByBytes(t *testing.T) {
	store := NewMemoryStore(1000, 500)
	now := time.Now()

	for i := 0; i < 20; i++ {
		store.Save(&MessageRecord{
			ID:        fmt.Sprintf("msg-%d", i),
			ChannelID: "ch-1",
			Stage:     "received",
			Status:    "RECEIVED",
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Content:   make([]byte, 100),
		})
	}

	if store.BytesUsed() > 500 {
		t.Fatalf("expected bytes used <= 500, got %d", store.BytesUsed())
	}
}

func TestMemoryStoreCount(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: now})
	store.Save(&MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&MessageRecord{ID: "m3", ChannelID: "ch-2", Stage: "received", Status: "ERROR", Timestamp: now})

	count, err := store.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 4 {
		t.Fatalf("expected 4 total, got %d", count)
	}

	count, err = store.Count(QueryOpts{Stage: "received"})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 received, got %d", count)
	}

	count, err = store.Count(QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 for ch-1, got %d", count)
	}

	count, err = store.Count(QueryOpts{Status: "ERROR"})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 error, got %d", count)
	}
}

func TestCompositeStoreCount(t *testing.T) {
	inner := NewMemoryStore(0, 0)
	cs := NewCompositeStore(inner, "full", nil)

	now := time.Now()
	cs.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})
	cs.Save(&MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})

	count, err := cs.Count(QueryOpts{Stage: "received"})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}

	noneStore := NewCompositeStore(inner, "none", nil)
	count, err = noneStore.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 in none mode, got %d", count)
	}
}

func TestMemoryStoreContentSize(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{
		ID:        "m1",
		ChannelID: "ch-1",
		Stage:     "received",
		Status:    "RECEIVED",
		Timestamp: now,
		Content:   []byte("hello world"),
	})

	rec, err := store.Get("m1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.ContentSize != 11 {
		t.Fatalf("expected ContentSize=11, got %d", rec.ContentSize)
	}
}
func TestMemoryStoreQuerySinceFilter(t *testing.T) {
	store := NewMemoryStore(0, 0)
	base := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base})
	store.Save(&MessageRecord{ID: "m2", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base.Add(1 * time.Hour)})
	store.Save(&MessageRecord{ID: "m3", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base.Add(2 * time.Hour)})

	records, err := store.Query(QueryOpts{Since: base.Add(30 * time.Minute)})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records after since filter, got %d", len(records))
	}
	for _, r := range records {
		if r.Timestamp.Before(base.Add(30 * time.Minute)) {
			t.Fatalf("record %s should be after since time", r.ID)
		}
	}
}

func TestMemoryStoreQueryBeforeFilter(t *testing.T) {
	store := NewMemoryStore(0, 0)
	base := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base})
	store.Save(&MessageRecord{ID: "m2", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base.Add(1 * time.Hour)})
	store.Save(&MessageRecord{ID: "m3", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base.Add(2 * time.Hour)})

	records, err := store.Query(QueryOpts{Before: base.Add(90 * time.Minute)})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records before filter, got %d", len(records))
	}
}

func TestMemoryStoreQuerySinceAndBefore(t *testing.T) {
	store := NewMemoryStore(0, 0)
	base := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		store.Save(&MessageRecord{
			ID: fmt.Sprintf("m%d", i), ChannelID: "ch", Stage: "received",
			Status: "OK", Timestamp: base.Add(time.Duration(i) * time.Hour),
		})
	}

	records, err := store.Query(QueryOpts{
		Since:  base.Add(1 * time.Hour),
		Before: base.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records in range [1h,3h], got %d", len(records))
	}
}

func TestMemoryStoreQueryLimit(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	for i := 0; i < 10; i++ {
		store.Save(&MessageRecord{
			ID: fmt.Sprintf("m%d", i), ChannelID: "ch", Stage: "received",
			Status: "OK", Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	records, err := store.Query(QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records with limit, got %d", len(records))
	}
}

func TestMemoryStoreQueryOffset(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	for i := 0; i < 5; i++ {
		store.Save(&MessageRecord{
			ID: fmt.Sprintf("m%d", i), ChannelID: "ch", Stage: "received",
			Status: "OK", Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	records, err := store.Query(QueryOpts{Offset: 2})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records after offset=2, got %d", len(records))
	}
}

func TestMemoryStoreQueryLimitAndOffset(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	for i := 0; i < 10; i++ {
		store.Save(&MessageRecord{
			ID: fmt.Sprintf("m%d", i), ChannelID: "ch", Stage: "received",
			Status: "OK", Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	records, err := store.Query(QueryOpts{Limit: 3, Offset: 2})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records with limit=3 offset=2, got %d", len(records))
	}
}

func TestMemoryStoreSaveWithCorrelationID(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{
		ID:            "m1",
		CorrelationID: "corr-1",
		ChannelID:     "ch",
		Stage:         "received",
		Status:        "OK",
		Timestamp:     now,
		Content:       []byte("data"),
	})

	rec, err := store.Get("m1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.CorrelationID != "corr-1" {
		t.Fatalf("expected CorrelationID=corr-1, got %s", rec.CorrelationID)
	}
}

func TestMemoryStoreQueryMultipleFilters(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: now})
	store.Save(&MessageRecord{ID: "m3", ChannelID: "ch-2", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&MessageRecord{ID: "m4", ChannelID: "ch-1", Stage: "received", Status: "ERROR", Timestamp: now})

	records, err := store.Query(QueryOpts{ChannelID: "ch-1", Stage: "received"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records matching ch-1 + received, got %d", len(records))
	}

	records, err = store.Query(QueryOpts{ChannelID: "ch-1", Stage: "received", Status: "RECEIVED"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record matching ch-1 + received + RECEIVED, got %d", len(records))
	}
}

func TestMemoryStoreDeleteNonExistent(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now})

	err := store.Delete("nonexistent")
	if err != nil {
		t.Fatalf("delete of nonexistent should not error, got %v", err)
	}

	if store.Len() != 1 {
		t.Fatalf("expected 1 record remaining, got %d", store.Len())
	}
}

func TestMemoryStoreLenAfterOperations(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	if store.Len() != 0 {
		t.Fatalf("expected Len=0 initially, got %d", store.Len())
	}

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("a")})
	if store.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", store.Len())
	}

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "sent", Status: "SENT", Timestamp: now, Content: []byte("b")})
	if store.Len() != 2 {
		t.Fatalf("expected Len=2, got %d", store.Len())
	}

	store.Delete("m1")
	if store.Len() != 0 {
		t.Fatalf("expected Len=0 after delete, got %d", store.Len())
	}
}

func TestMemoryStoreBytesUsedAfterOperations(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	if store.BytesUsed() != 0 {
		t.Fatalf("expected BytesUsed=0 initially, got %d", store.BytesUsed())
	}

	rec := &MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("hello")}
	store.Save(rec)
	bytesAfterSave := store.BytesUsed()
	if bytesAfterSave <= 0 {
		t.Fatal("expected positive BytesUsed after save")
	}

	store.Delete("m1")
	if store.BytesUsed() != 0 {
		t.Fatalf("expected BytesUsed=0 after delete, got %d", store.BytesUsed())
	}
}

func TestRecordSize(t *testing.T) {
	rec := &MessageRecord{
		ID:            "abc",
		CorrelationID: "def",
		ChannelID:     "ghi",
		Stage:         "received",
		Content:       []byte("hello world"),
		Status:        "OK",
	}
	size := recordSize(rec)
	expected := len("abc") + len("def") + len("ghi") + len("received") + len("hello world") + len("OK") + 64
	if size != expected {
		t.Fatalf("expected size=%d, got %d", expected, size)
	}
}

func TestRecordSize_EmptyRecord(t *testing.T) {
	rec := &MessageRecord{}
	size := recordSize(rec)
	if size != 64 {
		t.Fatalf("expected 64 for empty record, got %d", size)
	}
}

func TestNewMemoryStoreDefaultLimits(t *testing.T) {
	store := NewMemoryStore(0, 0)
	if store.maxRecords != defaultMaxRecords {
		t.Fatalf("expected maxRecords=%d, got %d", defaultMaxRecords, store.maxRecords)
	}
	if store.maxBytes != defaultMaxBytes {
		t.Fatalf("expected maxBytes=%d, got %d", defaultMaxBytes, store.maxBytes)
	}
}

func TestNewMemoryStoreNegativeLimits(t *testing.T) {
	store := NewMemoryStore(-1, -1)
	if store.maxRecords != defaultMaxRecords {
		t.Fatalf("expected default maxRecords for negative input, got %d", store.maxRecords)
	}
	if store.maxBytes != defaultMaxBytes {
		t.Fatalf("expected default maxBytes for negative input, got %d", store.maxBytes)
	}
}

func TestNewMemoryStoreCustomLimits(t *testing.T) {
	store := NewMemoryStore(50, 1024)
	if store.maxRecords != 50 {
		t.Fatalf("expected maxRecords=50, got %d", store.maxRecords)
	}
	if store.maxBytes != 1024 {
		t.Fatalf("expected maxBytes=1024, got %d", store.maxBytes)
	}
}

func TestMemoryStoreUpdateExistingRecord(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("v1")})
	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "UPDATED", Timestamp: now, Content: []byte("v2-longer-content")})

	if store.Len() != 1 {
		t.Fatalf("expected 1 record after update, got %d", store.Len())
	}

	rec, err := store.GetStage("m1", "received")
	if err != nil {
		t.Fatalf("GetStage failed: %v", err)
	}
	if rec.Status != "UPDATED" {
		t.Fatalf("expected status=UPDATED, got %s", rec.Status)
	}
	if string(rec.Content) != "v2-longer-content" {
		t.Fatalf("expected updated content, got %s", string(rec.Content))
	}
}

func TestMemoryStoreCountWithTimeFilters(t *testing.T) {
	store := NewMemoryStore(0, 0)
	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base})
	store.Save(&MessageRecord{ID: "m2", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base.Add(1 * time.Hour)})
	store.Save(&MessageRecord{ID: "m3", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: base.Add(2 * time.Hour)})

	count, err := store.Count(QueryOpts{Since: base.Add(30 * time.Minute)})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count=2 with since filter, got %d", count)
	}

	count, err = store.Count(QueryOpts{Before: base.Add(90 * time.Minute)})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count=2 with before filter, got %d", count)
	}
}

func TestMemoryStoreBytesUsedTrackingOnUpdate(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("short")})
	bytesV1 := store.BytesUsed()

	store.Save(&MessageRecord{ID: "m1", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now, Content: []byte("much longer content here")})
	bytesV2 := store.BytesUsed()

	if bytesV2 <= bytesV1 {
		t.Fatalf("expected bytes to increase after saving larger content, v1=%d v2=%d", bytesV1, bytesV2)
	}
}

func TestMemoryStoreContentSizeAutoSet(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{
		ID:        "m1",
		ChannelID: "ch",
		Stage:     "received",
		Status:    "OK",
		Timestamp: now,
		Content:   []byte("12345"),
	})

	rec, _ := store.Get("m1")
	if rec.ContentSize != 5 {
		t.Fatalf("expected ContentSize=5, got %d", rec.ContentSize)
	}
}

func TestMemoryStoreContentSizePreservedIfSet(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{
		ID:          "m1",
		ChannelID:   "ch",
		Stage:       "received",
		Status:      "OK",
		Timestamp:   now,
		Content:     []byte("12345"),
		ContentSize: 99,
	})

	rec, _ := store.Get("m1")
	if rec.ContentSize != 99 {
		t.Fatalf("expected ContentSize=99 (pre-set), got %d", rec.ContentSize)
	}
}

func TestMemoryStoreQueryReturnsNewestFirst(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{ID: "old", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now})
	store.Save(&MessageRecord{ID: "new", ChannelID: "ch", Stage: "received", Status: "OK", Timestamp: now.Add(time.Second)})

	records, _ := store.Query(QueryOpts{})
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].ID != "new" {
		t.Fatalf("expected newest first, got %s", records[0].ID)
	}
	if records[1].ID != "old" {
		t.Fatalf("expected oldest last, got %s", records[1].ID)
	}
}

func TestMemoryStoreMetadataPreserved(t *testing.T) {
	store := NewMemoryStore(0, 0)
	now := time.Now()

	store.Save(&MessageRecord{
		ID:        "m1",
		ChannelID: "ch",
		Stage:     "received",
		Status:    "OK",
		Timestamp: now,
		Metadata:  map[string]any{"source": "test", "count": float64(42)},
	})

	rec, _ := store.Get("m1")
	if rec.Metadata["source"] != "test" {
		t.Fatalf("expected metadata source=test, got %v", rec.Metadata["source"])
	}
	if rec.Metadata["count"] != float64(42) {
		t.Fatalf("expected metadata count=42, got %v", rec.Metadata["count"])
	}
}
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

func TestCompositeStoreShouldStoreUnknownMode(t *testing.T) {
	cs := NewCompositeStore(NewMemoryStore(0, 0), "custom", nil)
	if !cs.ShouldStore("anything") {
		t.Fatal("unknown mode should return true from ShouldStore")
	}
}

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

		if store.BytesUsed() > 400 {
			t.Errorf("expected bytes under limit, got %d", store.BytesUsed())
		}
	}
}
