package storage

import (
	"fmt"
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
