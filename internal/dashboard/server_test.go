package dashboard

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/observability"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
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
	store := storage.NewMemoryStore(0, 0)
	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/storage-info", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["mode"] != "full" {
		t.Fatalf("expected mode=full for raw MemoryStore, got %v", result["mode"])
	}
	if result["driver"] != "memory" {
		t.Fatalf("expected driver=memory, got %v", result["driver"])
	}
	if _, ok := result["records"]; !ok {
		t.Fatal("expected records field in storage info")
	}
	if _, ok := result["bytes_used"]; !ok {
		t.Fatal("expected bytes_used field in storage info")
	}
}

func TestHandleStorageInfoStatus(t *testing.T) {
	inner := storage.NewMemoryStore(0, 0)
	cs := storage.NewCompositeStore(inner, "status", nil)
	s := newTestServer(cs)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/storage-info", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["mode"] != "status" {
		t.Fatalf("expected mode=status, got %v", result["mode"])
	}
}

func TestHandleStorageInfoNone(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/storage-info", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["mode"] != "none" {
		t.Fatalf("expected mode=none, got %v", result["mode"])
	}
}

func TestHandlePayloadPreview(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
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
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()

	longContent := make([]byte, 3000)
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
	if len(preview) != 2000 {
		t.Fatalf("expected 2000-char preview, got %d chars", len(preview))
	}
	size, _ := result["size"].(float64)
	if int(size) != 3000 {
		t.Fatalf("expected size=3000, got %v", size)
	}
}

func TestHandlePayloadUnavailable(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
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
	store := storage.NewMemoryStore(0, 0)
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
	store := storage.NewMemoryStore(0, 0)
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
	store := storage.NewMemoryStore(0, 0)
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
	store := storage.NewMemoryStore(0, 0)
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

func TestHandlePayloadIntuMessageJSON(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()

	intuJSON := `{"body":"test payload","transport":"http","contentType":"json","http":{"headers":{"Content-Type":"application/json"},"statusCode":0}}`
	store.Save(&storage.MessageRecord{
		ID: "m-json", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: []byte(intuJSON),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m-json/payload?stage=received", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)

	preview, _ := result["preview"].(string)
	if len(preview) == 0 {
		t.Fatal("expected non-empty preview")
	}
	// Pretty-printed JSON should contain indentation
	if !json.Valid([]byte(preview)) {
		t.Fatal("expected valid JSON in preview")
	}
}

func TestHandlePayloadResponseStage(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()

	respJSON := `{"body":"OK","transport":"http","contentType":"raw","http":{"headers":{},"statusCode":200}}`
	store.Save(&storage.MessageRecord{
		ID: "m-resp", ChannelID: "ch-1", Stage: "response",
		Status: "SENT", Timestamp: now, Content: []byte(respJSON),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m-resp/payload?stage=response", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["unavailable"] != nil {
		t.Fatal("did not expect unavailable for response stage")
	}
	preview, _ := result["preview"].(string)
	if len(preview) == 0 {
		t.Fatal("expected non-empty preview for response stage")
	}
}

func TestHandlePayloadDownloadJSON(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()

	intuJSON := `{"body":"test","transport":"http","contentType":"raw"}`
	store.Save(&storage.MessageRecord{
		ID: "m-dl", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: []byte(intuJSON),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m-dl/payload?stage=received&download=true", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	cd := w.Header().Get("Content-Disposition")
	if cd == "" {
		t.Fatal("expected Content-Disposition header")
	}
	if !json.Valid(w.Body.Bytes()) {
		t.Fatal("expected valid JSON in download body")
	}
}

func TestListChannelsReturnsDescriptionAndProfiles(t *testing.T) {
	dir := t.TempDir()
	chDir := filepath.Join(dir, "test-ch")
	if err := os.MkdirAll(chDir, 0o755); err != nil {
		t.Fatal(err)
	}
	yaml := "id: test-ch\nenabled: true\ndescription: \"My test channel\"\nprofiles:\n  - dev\n  - staging\nlistener:\n  type: http\n  http:\n    port: 9090\n"
	if err := os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{}
	cfg.Runtime.Profile = "dev"
	s := NewServer(&ServerConfig{
		Config:      cfg,
		ChannelsDir: dir,
		Store:       storage.NewMemoryStore(0, 0),
		Metrics:     observability.Global(),
		Logger:      slog.Default(),
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var channels []map[string]any
	json.NewDecoder(w.Body).Decode(&channels)
	if len(channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(channels))
	}
	ch := channels[0]
	if ch["description"] != "My test channel" {
		t.Fatalf("expected description 'My test channel', got %v", ch["description"])
	}
	profiles, ok := ch["profiles"].([]any)
	if !ok || len(profiles) != 2 {
		t.Fatalf("expected 2 profiles, got %v", ch["profiles"])
	}
	if profiles[0] != "dev" || profiles[1] != "staging" {
		t.Fatalf("unexpected profiles: %v", profiles)
	}
}
