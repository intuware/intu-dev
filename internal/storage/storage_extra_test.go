package storage

import (
	"fmt"
	"testing"
	"time"
)

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
	// Since is inclusive (>=), Before is inclusive (<=): hours 1, 2, 3 match
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
