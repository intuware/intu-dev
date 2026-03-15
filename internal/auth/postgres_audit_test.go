package auth

import (
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func newMockAuditStore(t *testing.T) (*PostgresAuditStore, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	return &PostgresAuditStore{db: db, tablePrefix: "intu_"}, mock
}

func TestNewPostgresAuditStoreEmptyDSN(t *testing.T) {
	_, err := NewPostgresAuditStore("", "")
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestPostgresAuditStoreTableName(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"intu_", "intu_audit_log"},
		{"custom_", "custom_audit_log"},
		{"", "audit_log"},
	}
	for _, tt := range tests {
		store := &PostgresAuditStore{tablePrefix: tt.prefix}
		got := store.tableName()
		if got != tt.want {
			t.Errorf("tableName(prefix=%q) = %q, want %q", tt.prefix, got, tt.want)
		}
	}
}

func TestPostgresAuditStoreSave(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	mock.ExpectExec("INSERT INTO intu_audit_log").
		WillReturnResult(sqlmock.NewResult(1, 1))

	entry := &AuditEntry{
		Timestamp: time.Now(),
		Event:     "login",
		User:      "admin",
		Details:   map[string]any{"ip": "127.0.0.1"},
		SourceIP:  "127.0.0.1",
	}

	if err := store.Save(entry); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresAuditStoreSaveError(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	mock.ExpectExec("INSERT INTO intu_audit_log").
		WillReturnError(fmt.Errorf("insert error"))

	entry := &AuditEntry{
		Timestamp: time.Now(),
		Event:     "login",
		User:      "admin",
	}

	err := store.Save(entry)
	if err == nil {
		t.Fatal("expected error on save")
	}
}

func TestPostgresAuditStoreSaveNilDetails(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	mock.ExpectExec("INSERT INTO intu_audit_log").
		WillReturnResult(sqlmock.NewResult(1, 1))

	entry := &AuditEntry{
		Timestamp: time.Now(),
		Event:     "logout",
		User:      "user1",
		Details:   nil,
	}

	if err := store.Save(entry); err != nil {
		t.Fatalf("save with nil details failed: %v", err)
	}
}

func TestPostgresAuditStoreQueryNoFilters(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"}).
		AddRow(now, "login", "admin", []byte(`{"ip":"127.0.0.1"}`), "127.0.0.1").
		AddRow(now, "logout", "admin", []byte("{}"), nil)

	mock.ExpectQuery("SELECT .+ FROM intu_audit_log ORDER BY timestamp DESC LIMIT 100").
		WillReturnRows(rows)

	entries, err := store.Query(AuditQueryOpts{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Event != "login" {
		t.Fatalf("expected 'login', got %q", entries[0].Event)
	}
	if entries[0].Details["ip"] != "127.0.0.1" {
		t.Fatal("expected details ip=127.0.0.1")
	}
	if entries[1].SourceIP != "" {
		t.Fatalf("expected empty source_ip for null, got %q", entries[1].SourceIP)
	}
}

func TestPostgresAuditStoreQueryWithEventFilter(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"}).
		AddRow(now, "login", "admin", []byte("{}"), nil)

	mock.ExpectQuery("SELECT .+ FROM intu_audit_log WHERE event").
		WithArgs("login").
		WillReturnRows(rows)

	entries, err := store.Query(AuditQueryOpts{Event: "login"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestPostgresAuditStoreQueryWithUserFilter(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"}).
		AddRow(now, "login", "bob", []byte("{}"), nil)

	mock.ExpectQuery("SELECT .+ FROM intu_audit_log WHERE username").
		WithArgs("bob").
		WillReturnRows(rows)

	entries, err := store.Query(AuditQueryOpts{User: "bob"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(entries) != 1 || entries[0].User != "bob" {
		t.Fatalf("expected 1 entry for bob, got %d", len(entries))
	}
}

func TestPostgresAuditStoreQueryWithAllFilters(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	since := now.Add(-1 * time.Hour)
	before := now.Add(1 * time.Hour)

	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"}).
		AddRow(now, "login", "admin", []byte("{}"), "10.0.0.1")

	mock.ExpectQuery("SELECT .+ FROM intu_audit_log WHERE .+").
		WithArgs("login", "admin", since, before).
		WillReturnRows(rows)

	entries, err := store.Query(AuditQueryOpts{
		Event:  "login",
		User:   "admin",
		Since:  since,
		Before: before,
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].SourceIP != "10.0.0.1" {
		t.Fatalf("expected source_ip '10.0.0.1', got %q", entries[0].SourceIP)
	}
}

func TestPostgresAuditStoreQueryWithLimitAndOffset(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"}).
		AddRow(now, "login", "admin", []byte("{}"), nil)

	mock.ExpectQuery("SELECT .+ FROM intu_audit_log .+ LIMIT 5 OFFSET 10").
		WillReturnRows(rows)

	entries, err := store.Query(AuditQueryOpts{Limit: 5, Offset: 10})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestPostgresAuditStoreQueryDefaultLimit(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"})
	mock.ExpectQuery("SELECT .+ FROM intu_audit_log .+ LIMIT 100").
		WillReturnRows(rows)

	_, err := store.Query(AuditQueryOpts{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
}

func TestPostgresAuditStoreQueryError(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	mock.ExpectQuery("SELECT .+ FROM intu_audit_log").
		WillReturnError(fmt.Errorf("query error"))

	_, err := store.Query(AuditQueryOpts{})
	if err == nil {
		t.Fatal("expected error on query")
	}
}

func TestPostgresAuditStoreQueryScanError(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"}).
		AddRow("not-a-timestamp", "login", "admin", []byte("{}"), nil)

	mock.ExpectQuery("SELECT .+ FROM intu_audit_log").
		WillReturnRows(rows)

	_, err := store.Query(AuditQueryOpts{})
	if err == nil {
		t.Fatal("expected scan error")
	}
}

func TestPostgresAuditStoreClose(t *testing.T) {
	store, mock := newMockAuditStore(t)

	mock.ExpectClose()

	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations not met: %v", err)
	}
}

func TestPostgresAuditStoreEnsureTable(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS intu_audit_log").
		WillReturnResult(sqlmock.NewResult(0, 0))

	for i := 0; i < 3; i++ {
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

func TestPostgresAuditStoreEnsureTableCreateError(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS intu_audit_log").
		WillReturnError(fmt.Errorf("create table error"))

	err := store.ensureTable()
	if err == nil {
		t.Fatal("expected error on ensureTable")
	}
}

func TestPostgresAuditStoreEnsureTableIndexError(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS intu_audit_log").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec("CREATE INDEX IF NOT EXISTS").
		WillReturnError(fmt.Errorf("index error"))

	err := store.ensureTable()
	if err == nil {
		t.Fatal("expected error on index creation")
	}
}

func TestPostgresAuditStoreQueryEmptyDetails(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	now := time.Now().Truncate(time.Microsecond)
	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"}).
		AddRow(now, "login", "admin", []byte(nil), nil)

	mock.ExpectQuery("SELECT .+ FROM intu_audit_log").
		WillReturnRows(rows)

	entries, err := store.Query(AuditQueryOpts{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Details != nil {
		t.Fatal("expected nil details for empty JSON")
	}
}

func TestPostgresAuditStoreQueryWithTimeFilters(t *testing.T) {
	store, mock := newMockAuditStore(t)
	defer store.db.Close()

	since := time.Now().Add(-1 * time.Hour)
	before := time.Now()

	rows := mock.NewRows([]string{"timestamp", "event", "username", "details", "source_ip"})
	mock.ExpectQuery("SELECT .+ FROM intu_audit_log WHERE timestamp .+ AND timestamp").
		WithArgs(since, before).
		WillReturnRows(rows)

	_, err := store.Query(AuditQueryOpts{Since: since, Before: before})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
}
