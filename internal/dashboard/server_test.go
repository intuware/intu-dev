package dashboard

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/intuware/intu/internal/observability"
	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
)

func newTestServer(store storage.MessageStore) *Server {
	return NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: "/tmp/nonexistent",
		Store:       store,
		Metrics:     observability.Global(),
		Logger:      slog.Default(),
	})
}

func TestHandleStorageInfoFull(t *testing.T) {
	store := storage.NewMemoryStore()
	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/storage-info", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]string
	json.NewDecoder(w.Body).Decode(&result)
	if result["mode"] != "full" {
		t.Fatalf("expected mode=full for raw MemoryStore, got %s", result["mode"])
	}
}

func TestHandleStorageInfoStatus(t *testing.T) {
	inner := storage.NewMemoryStore()
	cs := storage.NewCompositeStore(inner, "status", nil)
	s := newTestServer(cs)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/storage-info", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]string
	json.NewDecoder(w.Body).Decode(&result)
	if result["mode"] != "status" {
		t.Fatalf("expected mode=status, got %s", result["mode"])
	}
}

func TestHandleStorageInfoNone(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/storage-info", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]string
	json.NewDecoder(w.Body).Decode(&result)
	if result["mode"] != "none" {
		t.Fatalf("expected mode=none, got %s", result["mode"])
	}
}

func TestHandlePayloadPreview(t *testing.T) {
	store := storage.NewMemoryStore()
	now := time.Now()
	store.Save(&storage.MessageRecord{
		ID: "m1", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: []byte("Hello, this is the received payload content."),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m1/payload?stage=received", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)

	preview, _ := result["preview"].(string)
	if preview != "Hello, this is the received payload content." {
		t.Fatalf("unexpected preview: %s", preview)
	}
	size, _ := result["size"].(float64)
	if int(size) != 44 {
		t.Fatalf("expected size=44, got %v", size)
	}
	if result["unavailable"] != nil {
		t.Fatal("did not expect unavailable flag")
	}
}

func TestHandlePayloadPreviewTruncation(t *testing.T) {
	store := storage.NewMemoryStore()
	now := time.Now()

	longContent := make([]byte, 1000)
	for i := range longContent {
		longContent[i] = 'A'
	}
	store.Save(&storage.MessageRecord{
		ID: "m2", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: longContent,
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m2/payload?stage=received", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)

	preview, _ := result["preview"].(string)
	if len(preview) != 500 {
		t.Fatalf("expected 500-char preview, got %d chars", len(preview))
	}
	size, _ := result["size"].(float64)
	if int(size) != 1000 {
		t.Fatalf("expected size=1000, got %v", size)
	}
}

func TestHandlePayloadUnavailable(t *testing.T) {
	store := storage.NewMemoryStore()
	now := time.Now()
	store.Save(&storage.MessageRecord{
		ID: "m3", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: nil,
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m3/payload?stage=received", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)

	if result["unavailable"] != true {
		t.Fatal("expected unavailable=true when content is nil")
	}
}

func TestHandlePayloadDownload(t *testing.T) {
	store := storage.NewMemoryStore()
	now := time.Now()
	store.Save(&storage.MessageRecord{
		ID: "m4", ChannelID: "ch-1", Stage: "sent",
		Status: "SENT", Timestamp: now, Content: []byte("raw-payload-bytes"),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m4/payload?stage=sent&download=true", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("expected application/octet-stream, got %s", ct)
	}
	if cd := w.Header().Get("Content-Disposition"); cd == "" {
		t.Fatal("expected Content-Disposition header")
	}
	if w.Body.String() != "raw-payload-bytes" {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}

func TestHandlePayloadMissingStage(t *testing.T) {
	store := storage.NewMemoryStore()
	now := time.Now()
	store.Save(&storage.MessageRecord{
		ID: "m5", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: []byte("data"),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m5/payload?stage=transformed", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["unavailable"] != true {
		t.Fatal("expected unavailable=true for non-existent stage")
	}
}

func TestHandleMessagesStageFilter(t *testing.T) {
	store := storage.NewMemoryStore()
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("r")})
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: now, Content: []byte("s")})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?stage=received&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var records []map[string]any
	json.NewDecoder(w.Body).Decode(&records)

	if len(records) != 1 {
		t.Fatalf("expected 1 received record, got %d", len(records))
	}
	if records[0]["Stage"] != "received" {
		t.Fatalf("expected stage=received, got %v", records[0]["Stage"])
	}
	if records[0]["Content"] != nil {
		t.Fatal("expected nil content with exclude_content=1")
	}
}

func TestHandleMessagesDedupe(t *testing.T) {
	store := storage.NewMemoryStore()
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "transformed", Status: "TRANSFORMED", Timestamp: now})
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "sent", Status: "SENT", Timestamp: now})
	store.Save(&storage.MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&storage.MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "transformed", Status: "ERROR", Timestamp: now})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?dedupe=1&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var records []map[string]any
	json.NewDecoder(w.Body).Decode(&records)

	if len(records) != 2 {
		t.Fatalf("expected 2 deduplicated records, got %d", len(records))
	}

	statusByID := map[string]string{}
	for _, r := range records {
		id, _ := r["ID"].(string)
		st, _ := r["Status"].(string)
		statusByID[id] = st
	}

	if statusByID["m1"] != "SENT" {
		t.Fatalf("expected m1 status=SENT (highest priority), got %s", statusByID["m1"])
	}
	if statusByID["m2"] != "ERROR" {
		t.Fatalf("expected m2 status=ERROR (higher than RECEIVED), got %s", statusByID["m2"])
	}
}
