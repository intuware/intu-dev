package storage

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/intuware/intu-dev/internal/dbutil"
	"github.com/intuware/intu-dev/pkg/config"
)

func newMockPostgresStore(t *testing.T) (*PostgresStore, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	return &PostgresStore{db: db, tablePrefix: "intu_", dialect: dbutil.PostgresDialect{}}, mock
}

func TestNewPostgresStoreNilConfig(t *testing.T) {
	_, err := NewPostgresStore(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestNewPostgresStoreEmptyDSN(t *testing.T) {
	_, err := NewPostgresStore(&config.StoragePostgresConfig{DSN: ""})
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestPostgresStoreTableName(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"intu_", "intu_messages"},
		{"custom_", "custom_messages"},
		{"", "messages"},
	}
	for _, tt := range tests {
		store := &PostgresStore{tablePrefix: tt.prefix, dialect: dbutil.PostgresDialect{}}
		got := store.tableName()
		if got != tt.want {
			t.Errorf("tableName(prefix=%q) = %q, want %q", tt.prefix, got, tt.want)
		}
	}
}

func TestPostgresStoreSave(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("INSERT INTO intu_messages").
		WillReturnResult(sqlmock.NewResult(0, 1))

	record := &MessageRecord{
		ID:            "msg-1",
		CorrelationID: "corr-1",
		ChannelID:     "ch-1",
		Stage:         "received",
		Content:       []byte("hello"),
		Status:        "RECEIVED",
		Timestamp:     time.Now(),
		Metadata:      map[string]any{"key": "val"},
	}

	if err := store.Save(record); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresStoreSaveError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("INSERT INTO intu_messages").
		WillReturnError(fmt.Errorf("connection lost"))

	record := &MessageRecord{
		ID:        "msg-1",
		ChannelID: "ch-1",
		Stage:     "received",
		Content:   []byte("hello"),
		Status:    "RECEIVED",
		Timestamp: time.Now(),
	}

	err := store.Save(record)
	if err == nil {
		t.Fatal("expected error on save")
	}
}

func TestPostgresStoreSaveContentSizeFromContent(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("INSERT INTO intu_messages").
		WillReturnResult(sqlmock.NewResult(0, 1))

	record := &MessageRecord{
		ID:          "msg-1",
		ChannelID:   "ch-1",
		Stage:       "received",
		Content:     []byte("twelve chars"),
		ContentSize: 0,
		Status:      "RECEIVED",
		Timestamp:   time.Now(),
	}

	if err := store.Save(record); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresStoreSaveNilMetadata(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("INSERT INTO intu_messages").
		WillReturnResult(sqlmock.NewResult(0, 1))

	record := &MessageRecord{
		ID:        "msg-1",
		ChannelID: "ch-1",
		Stage:     "received",
		Content:   []byte("data"),
		Status:    "RECEIVED",
		Timestamp: time.Now(),
		Metadata:  nil,
	}

	if err := store.Save(record); err != nil {
		t.Fatalf("save with nil metadata failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresStoreGet(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow(
		"msg-1", "corr-1", "ch-1", "received",
		[]byte("hello"), 5, "RECEIVED", now, int64(42), []byte(`{"key":"val"}`),
	)

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE id").
		WithArgs("msg-1").
		WillReturnRows(rows)

	rec, err := store.Get("msg-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.ID != "msg-1" {
		t.Fatalf("expected msg-1, got %s", rec.ID)
	}
	if rec.CorrelationID != "corr-1" {
		t.Fatalf("expected corr-1, got %s", rec.CorrelationID)
	}
	if rec.Status != "RECEIVED" {
		t.Fatalf("expected RECEIVED, got %s", rec.Status)
	}
	if rec.DurationMs != 42 {
		t.Fatalf("expected duration 42, got %d", rec.DurationMs)
	}
	if rec.Metadata["key"] != "val" {
		t.Fatal("expected metadata key=val")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresStoreGetNotFound(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE id").
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := store.Get("missing")
	if err == nil {
		t.Fatal("expected error for missing record")
	}
}

func TestPostgresStoreGetNullCorrelationID(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow(
		"msg-1", nil, "ch-1", "received",
		[]byte("hello"), 5, "RECEIVED", now, int64(0), []byte("{}"),
	)

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE id").
		WithArgs("msg-1").
		WillReturnRows(rows)

	rec, err := store.Get("msg-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.CorrelationID != "" {
		t.Fatalf("expected empty correlation ID, got %q", rec.CorrelationID)
	}
}

func TestPostgresStoreGetStage(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow(
		"msg-1", nil, "ch-1", "sent",
		[]byte("output"), 6, "SENT", now, int64(10), []byte("{}"),
	)

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE id .+ AND stage").
		WithArgs("msg-1", "sent").
		WillReturnRows(rows)

	rec, err := store.GetStage("msg-1", "sent")
	if err != nil {
		t.Fatalf("GetStage failed: %v", err)
	}
	if rec.Stage != "sent" {
		t.Fatalf("expected stage=sent, got %s", rec.Stage)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresStoreGetStageError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE id .+ AND stage").
		WithArgs("msg-1", "sent").
		WillReturnError(sql.ErrNoRows)

	_, err := store.GetStage("msg-1", "sent")
	if err == nil {
		t.Fatal("expected error for missing stage")
	}
}

func TestPostgresStoreQueryNoFilters(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).
		AddRow("msg-1", nil, "ch-1", "received", []byte("a"), 1, "RECEIVED", now, int64(0), []byte("{}")).
		AddRow("msg-2", nil, "ch-1", "received", []byte("b"), 1, "RECEIVED", now, int64(0), []byte("{}"))

	mock.ExpectQuery("SELECT .+ FROM intu_messages ORDER BY timestamp DESC").
		WillReturnRows(rows)

	records, err := store.Query(QueryOpts{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestPostgresStoreQueryWithChannelFilter(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow("msg-1", nil, "ch-1", "received", []byte("a"), 1, "RECEIVED", now, int64(0), []byte("{}"))

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE channel_id").
		WithArgs("ch-1").
		WillReturnRows(rows)

	records, err := store.Query(QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
}

func TestPostgresStoreQueryWithAllFilters(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	since := now.Add(-1 * time.Hour)
	before := now.Add(1 * time.Hour)

	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow("msg-1", nil, "ch-1", "received", []byte("a"), 1, "RECEIVED", now, int64(0), []byte("{}"))

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE .+").
		WithArgs("ch-1", "RECEIVED", "received", since, before).
		WillReturnRows(rows)

	records, err := store.Query(QueryOpts{
		ChannelID: "ch-1",
		Status:    "RECEIVED",
		Stage:     "received",
		Since:     since,
		Before:    before,
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
}

func TestPostgresStoreQueryWithLimitOffset(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow("msg-1", nil, "ch-1", "received", []byte("a"), 1, "RECEIVED", now, int64(0), []byte("{}"))

	mock.ExpectQuery("SELECT .+ FROM intu_messages ORDER BY timestamp DESC LIMIT 10 OFFSET 5").
		WillReturnRows(rows)

	records, err := store.Query(QueryOpts{Limit: 10, Offset: 5})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
}

func TestPostgresStoreQueryExcludeContent(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow("msg-1", nil, "ch-1", "received", nil, 100, "RECEIVED", now, int64(0), []byte("{}"))

	mock.ExpectQuery("SELECT .+ FROM intu_messages").
		WillReturnRows(rows)

	records, err := store.Query(QueryOpts{ExcludeContent: true})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Content != nil {
		t.Fatal("expected nil content with ExcludeContent")
	}
}

func TestPostgresStoreQueryError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectQuery("SELECT .+ FROM intu_messages").
		WillReturnError(fmt.Errorf("db error"))

	_, err := store.Query(QueryOpts{})
	if err == nil {
		t.Fatal("expected error on query")
	}
}

func TestPostgresStoreQueryScanError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow("msg-1", nil, "ch-1", "received", []byte("a"), "not-an-int", "RECEIVED", time.Now(), int64(0), []byte("{}"))

	mock.ExpectQuery("SELECT .+ FROM intu_messages").
		WillReturnRows(rows)

	_, err := store.Query(QueryOpts{})
	if err == nil {
		t.Fatal("expected scan error")
	}
}

func TestPostgresStoreCount(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	rows := mock.NewRows([]string{"count"}).AddRow(int64(42))

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(rows)

	count, err := store.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 42 {
		t.Fatalf("expected 42, got %d", count)
	}
}

func TestPostgresStoreCountWithFilters(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	rows := mock.NewRows([]string{"count"}).AddRow(int64(5))

	mock.ExpectQuery("SELECT COUNT").
		WithArgs("ch-1", "RECEIVED").
		WillReturnRows(rows)

	count, err := store.Count(QueryOpts{ChannelID: "ch-1", Status: "RECEIVED"})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5, got %d", count)
	}
}

func TestPostgresStoreCountError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectQuery("SELECT COUNT").
		WillReturnError(fmt.Errorf("count error"))

	_, err := store.Count(QueryOpts{})
	if err == nil {
		t.Fatal("expected error on count")
	}
}

func TestPostgresStoreDelete(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("DELETE FROM intu_messages WHERE id").
		WithArgs("msg-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.Delete("msg-1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresStoreDeleteError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("DELETE FROM intu_messages WHERE id").
		WithArgs("msg-1").
		WillReturnError(fmt.Errorf("delete error"))

	err := store.Delete("msg-1")
	if err == nil {
		t.Fatal("expected error on delete")
	}
}

func TestPostgresStorePrune(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	before := time.Now()
	mock.ExpectExec("DELETE FROM intu_messages WHERE timestamp").
		WithArgs(before).
		WillReturnResult(sqlmock.NewResult(0, 3))

	count, err := store.Prune(before, "")
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 pruned, got %d", count)
	}
}

func TestPostgresStorePruneWithChannel(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	before := time.Now()
	mock.ExpectExec("DELETE FROM intu_messages WHERE timestamp .+ AND channel_id").
		WithArgs(before, "ch-1").
		WillReturnResult(sqlmock.NewResult(0, 2))

	count, err := store.Prune(before, "ch-1")
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 pruned, got %d", count)
	}
}

func TestPostgresStorePruneError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("DELETE FROM intu_messages WHERE timestamp").
		WillReturnError(fmt.Errorf("prune error"))

	_, err := store.Prune(time.Now(), "")
	if err == nil {
		t.Fatal("expected error on prune")
	}
}

func TestPostgresStoreClose(t *testing.T) {
	store, mock := newMockPostgresStore(t)

	mock.ExpectClose()

	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresStoreEnsureTable(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS intu_messages").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// migrations (best-effort)
	mock.ExpectExec("ALTER TABLE intu_messages ADD COLUMN IF NOT EXISTS content_size").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE intu_messages ADD COLUMN IF NOT EXISTS duration_ms").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// 7 indexes
	for i := 0; i < 7; i++ {
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS").
			WillReturnResult(sqlmock.NewResult(0, 0))
	}

	if err := store.ensureTable(); err != nil {
		t.Fatalf("ensureTable failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresStoreEnsureTableCreateError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS intu_messages").
		WillReturnError(fmt.Errorf("create table error"))

	err := store.ensureTable()
	if err == nil {
		t.Fatal("expected error on ensureTable")
	}
}

func TestPostgresStoreEnsureTableIndexError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS intu_messages").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec("ALTER TABLE").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec("CREATE INDEX IF NOT EXISTS").
		WillReturnError(fmt.Errorf("index error"))

	err := store.ensureTable()
	if err == nil {
		t.Fatal("expected error on index creation")
	}
}

func TestPostgresStoreQueryWithStatusFilter(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow("msg-1", nil, "ch-1", "received", []byte("a"), 1, "ERROR", now, int64(0), []byte("{}"))

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE status").
		WithArgs("ERROR").
		WillReturnRows(rows)

	records, err := store.Query(QueryOpts{Status: "ERROR"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 || records[0].Status != "ERROR" {
		t.Fatalf("expected 1 ERROR record, got %d", len(records))
	}
}

func TestPostgresStoreQueryWithStageFilter(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow("msg-1", nil, "ch-1", "sent", []byte("a"), 1, "SENT", now, int64(0), []byte("{}"))

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE stage").
		WithArgs("sent").
		WillReturnRows(rows)

	records, err := store.Query(QueryOpts{Stage: "sent"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(records) != 1 || records[0].Stage != "sent" {
		t.Fatalf("expected 1 sent record, got %d", len(records))
	}
}

func TestPostgresStoreGetScanError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE id").
		WithArgs("msg-1").
		WillReturnError(fmt.Errorf("scan error"))

	_, err := store.Get("msg-1")
	if err == nil {
		t.Fatal("expected error on get scan failure")
	}
}

func TestPostgresStoreCountWithTimeFilters(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	since := time.Now().Add(-1 * time.Hour)
	before := time.Now()

	rows := mock.NewRows([]string{"count"}).AddRow(int64(10))

	mock.ExpectQuery("SELECT COUNT").
		WithArgs(since, before).
		WillReturnRows(rows)

	count, err := store.Count(QueryOpts{Since: since, Before: before})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 10 {
		t.Fatalf("expected 10, got %d", count)
	}
}

func TestPostgresStoreGetEmptyMetadata(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{
		"id", "correlation_id", "channel_id", "stage",
		"content", "content_size", "status", "timestamp", "duration_ms", "metadata",
	}).AddRow("msg-1", nil, "ch-1", "received", []byte("data"), 4, "RECEIVED", now, int64(0), []byte(nil))

	mock.ExpectQuery("SELECT .+ FROM intu_messages WHERE id").
		WithArgs("msg-1").
		WillReturnRows(rows)

	rec, err := store.Get("msg-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if rec.Metadata != nil {
		t.Fatal("expected nil metadata for empty JSON")
	}
}

func TestPostgresStorePruneRowsAffectedError(t *testing.T) {
	store, mock := newMockPostgresStore(t)
	defer store.db.Close()

	mock.ExpectExec("DELETE FROM intu_messages WHERE timestamp").
		WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("rows affected error")))

	_, err := store.Prune(time.Now(), "")
	if err == nil {
		t.Fatal("expected error from RowsAffected")
	}
}
