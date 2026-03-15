package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

func newTestServerWithDir(store storage.MessageStore, dir string) *Server {
	return NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: dir,
		Store:       store,
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
	})
}

func createChannelDir(t *testing.T, base, id string, enabled bool) string {
	t.Helper()
	chDir := filepath.Join(base, id)
	if err := os.MkdirAll(chDir, 0o755); err != nil {
		t.Fatal(err)
	}
	yaml := fmt.Sprintf("id: %s\nenabled: %v\nlistener:\n  type: http\n  http:\n    port: 8080\n", id, enabled)
	if err := os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	return chDir
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

// --- handleIndex ---

func TestHandleIndex_Root(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("expected text/html content type, got %s", ct)
	}
	if !strings.Contains(w.Body.String(), "<!DOCTYPE html>") {
		t.Fatal("expected SPA HTML in response body")
	}
}

func TestHandleIndex_IndexHTML(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/index.html", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleIndex_404ForOtherPaths(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/some/random/path", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// --- handleStats ---

func TestHandleStats_GET(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "ch-active", true)
	createChannelDir(t, dir, "ch-disabled", false)

	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "s1", ChannelID: "ch-active", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("hi")})

	metrics := observability.NewMetrics()
	metrics.Counter("messages_received_total.ch-active").Add(5)
	metrics.Counter("messages_processed_total.ch-active").Add(3)
	metrics.Counter("messages_errored_total.ch-active.timeout").Add(1)

	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: dir,
		Store:       store,
		Metrics:     metrics,
		Logger:      slog.Default(),
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)

	if result["total_channels"] == nil {
		t.Fatal("expected total_channels field")
	}
	if result["active_channels"] == nil {
		t.Fatal("expected active_channels field")
	}
	if result["message_counts"] == nil {
		t.Fatal("expected message_counts field")
	}
	if result["channel_volume"] == nil {
		t.Fatal("expected channel_volume field")
	}

	volume, ok := result["channel_volume"].([]any)
	if !ok || len(volume) == 0 {
		t.Fatal("expected non-empty channel_volume")
	}
}

func TestHandleStats_MethodNotAllowed(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleStats_NoStore(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	mc, ok := result["message_counts"].(map[string]any)
	if !ok {
		t.Fatal("expected message_counts map")
	}
	if len(mc) != 0 {
		t.Fatalf("expected empty message_counts when no store, got %v", mc)
	}
}

// --- handleChannels ---

func TestHandleChannels_MethodNotAllowed(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleChannels_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	s := newTestServerWithDir(nil, dir)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleChannels_TagsAndGroup(t *testing.T) {
	dir := t.TempDir()
	chDir := filepath.Join(dir, "tagged-ch")
	os.MkdirAll(chDir, 0o755)
	yaml := "id: tagged-ch\nenabled: true\ntags:\n  - hl7\n  - fhir\ngroup: integration\nlistener:\n  type: tcp\n  tcp:\n    port: 4000\ndestinations:\n  - name: d1\n    type: http\n    http:\n      url: http://localhost\n"
	os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte(yaml), 0o644)

	s := newTestServerWithDir(nil, dir)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var channels []map[string]any
	json.NewDecoder(w.Body).Decode(&channels)
	if len(channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(channels))
	}
	ch := channels[0]
	if ch["group"] != "integration" {
		t.Fatalf("expected group=integration, got %v", ch["group"])
	}
	tags, ok := ch["tags"].([]any)
	if !ok || len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %v", ch["tags"])
	}
}

// --- handleMetrics ---

func TestHandleMetrics_GET(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if _, ok := result["counters"]; !ok {
		t.Fatal("expected counters in metrics snapshot")
	}
}

func TestHandleMetrics_MethodNotAllowed(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPut, "/api/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// --- handleMessages ---

func TestHandleMessages_MethodNotAllowed(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/messages", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleMessages_NilStore(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []any
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 0 {
		t.Fatalf("expected empty array, got %d items", len(result))
	}
}

func TestHandleMessages_ChannelFilter(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-a", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&storage.MessageRecord{ID: "m2", ChannelID: "ch-b", Stage: "received", Status: "RECEIVED", Timestamp: now})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?channel=ch-a&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var records []map[string]any
	json.NewDecoder(w.Body).Decode(&records)
	for _, r := range records {
		if r["ChannelID"] != "ch-a" {
			t.Fatalf("expected only ch-a records, got %v", r["ChannelID"])
		}
	}
}

func TestHandleMessages_StatusFilter(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})
	store.Save(&storage.MessageRecord{ID: "m2", ChannelID: "ch-1", Stage: "received", Status: "ERROR", Timestamp: now})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?status=ERROR&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var records []map[string]any
	json.NewDecoder(w.Body).Decode(&records)
	for _, r := range records {
		if r["Status"] != "ERROR" {
			t.Fatalf("expected only ERROR records, got %v", r["Status"])
		}
	}
}

func TestHandleMessages_LimitAndOffset(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	for i := 0; i < 10; i++ {
		store.Save(&storage.MessageRecord{
			ID: fmt.Sprintf("m%d", i), ChannelID: "ch-1", Stage: "received",
			Status: "RECEIVED", Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=3&offset=2&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var records []map[string]any
	json.NewDecoder(w.Body).Decode(&records)
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
}

func TestHandleMessages_SinceAndBefore(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	store.Save(&storage.MessageRecord{ID: "old", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: t1})
	store.Save(&storage.MessageRecord{ID: "mid", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: t2})
	store.Save(&storage.MessageRecord{ID: "new", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: t3})

	s := newTestServer(store)
	handler := s.BuildHandler()

	since := t1.Add(time.Hour).Format(time.RFC3339)
	before := t3.Add(-time.Hour).Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodGet, "/api/messages?since="+since+"&before="+before+"&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var records []map[string]any
	json.NewDecoder(w.Body).Decode(&records)
	if len(records) != 1 {
		t.Fatalf("expected 1 record between since/before, got %d", len(records))
	}
}

func TestHandleMessages_SinceShortDateFormat(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	store.Save(&storage.MessageRecord{
		ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED",
		Timestamp: time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?since=2025-06-01&before=2025-07-01&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var records []map[string]any
	json.NewDecoder(w.Body).Decode(&records)
	if len(records) != 1 {
		t.Fatalf("expected 1 record with short date format, got %d", len(records))
	}
}

func TestHandleMessages_DedupeWithOffset(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	for i := 0; i < 5; i++ {
		store.Save(&storage.MessageRecord{
			ID: fmt.Sprintf("m%d", i), ChannelID: "ch-1", Stage: "received",
			Status: "RECEIVED", Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?dedupe=1&offset=2&limit=2&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var records []map[string]any
	json.NewDecoder(w.Body).Decode(&records)
	if len(records) != 2 {
		t.Fatalf("expected 2 records with offset, got %d", len(records))
	}
}

func TestHandleMessages_DedupeOffsetBeyondLength(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?dedupe=1&offset=999&exclude_content=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var records []any
	json.NewDecoder(w.Body).Decode(&records)
	if records != nil && len(records) != 0 {
		t.Fatalf("expected empty or nil result for large offset, got %d", len(records))
	}
}

// --- handleMessageByID ---

func TestHandleMessageByID_EmptyID(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleMessageByID_NoStore(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/some-id", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleMessageByID_NotFound(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleMessageByID_Found(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{
		ID: "msg-1", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: []byte("body"),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/msg-1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["message"] == nil {
		t.Fatal("expected message field in response")
	}
	if result["stages"] == nil {
		t.Fatal("expected stages field in response")
	}
}

func TestHandleMessageByID_MethodNotAllowed(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPut, "/api/messages/some-id", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// --- handleReprocess ---

func TestHandleReprocess_MethodNotAllowed(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m1/reprocess", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleReprocess_NoStore(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/messages/m1/reprocess", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReprocess_MessageNotFound(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/messages/nonexistent/reprocess", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleReprocess_NoReprocessFunc(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("x")})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/messages/m1/reprocess", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestHandleReprocess_Success(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("x")})

	called := false
	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: "/tmp/nonexistent",
		Store:       store,
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
		ReprocessFunc: func(_ context.Context, rec *storage.MessageRecord) error {
			called = true
			return nil
		},
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/messages/m1/reprocess", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !called {
		t.Fatal("expected reprocessFn to be called")
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["reprocessed"] != true {
		t.Fatal("expected reprocessed=true")
	}
}

func TestHandleReprocess_FuncError(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{ID: "m1", ChannelID: "ch-1", Stage: "received", Status: "RECEIVED", Timestamp: now, Content: []byte("x")})

	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: "/tmp/nonexistent",
		Store:       store,
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
		ReprocessFunc: func(_ context.Context, _ *storage.MessageRecord) error {
			return errors.New("pipeline error")
		},
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/messages/m1/reprocess", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// --- handlePayload ---

func TestHandlePayload_MethodNotAllowed(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/messages/m1/payload", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandlePayload_NilStore(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m1/payload?stage=received", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["unavailable"] != true {
		t.Fatal("expected unavailable=true for nil store")
	}
}

func TestHandlePayload_DefaultStage(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	now := time.Now()
	store.Save(&storage.MessageRecord{
		ID: "m1", ChannelID: "ch-1", Stage: "received",
		Status: "RECEIVED", Timestamp: now, Content: []byte("default stage data"),
	})

	s := newTestServer(store)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/m1/payload", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["unavailable"] != nil {
		t.Fatal("expected content to be available for default (received) stage")
	}
}

// --- handleStorageInfo ---

func TestHandleStorageInfo_MethodNotAllowed(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/storage-info", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleStorageInfo_WithDriverConfig(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	cfg := &config.Config{
		MessageStorage: &config.MessageStorageConfig{Driver: "postgres"},
	}
	s := NewServer(&ServerConfig{
		Config:      cfg,
		ChannelsDir: "/tmp/nonexistent",
		Store:       store,
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/storage-info", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["driver"] != "postgres" {
		t.Fatalf("expected driver=postgres from config, got %v", result["driver"])
	}
}

// --- handleChannelAction ---

func TestHandleChannelAction_Deploy(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "ch-deploy", false)

	called := false
	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: dir,
		Store:       storage.NewMemoryStore(0, 0),
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
		DeployFunc: func(_ context.Context, channelID string) error {
			called = true
			return nil
		},
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/ch-deploy/deploy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !called {
		t.Fatal("expected deployFn to be called")
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["action"] != "deployed" {
		t.Fatalf("expected action=deployed, got %v", result["action"])
	}

	data, _ := os.ReadFile(filepath.Join(dir, "ch-deploy", "channel.yaml"))
	if !strings.Contains(string(data), "enabled: true") {
		t.Fatal("expected channel.yaml to have enabled: true after deploy")
	}
}

func TestHandleChannelAction_DeployFuncError(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "ch-err", false)

	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: dir,
		Store:       storage.NewMemoryStore(0, 0),
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
		DeployFunc: func(_ context.Context, _ string) error {
			return errors.New("deploy failed")
		},
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/ch-err/deploy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandleChannelAction_Undeploy(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "ch-undeploy", true)

	called := false
	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: dir,
		Store:       storage.NewMemoryStore(0, 0),
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
		UndeployFunc: func(_ context.Context, channelID string) error {
			called = true
			return nil
		},
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/ch-undeploy/undeploy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !called {
		t.Fatal("expected undeployFn to be called")
	}

	data, _ := os.ReadFile(filepath.Join(dir, "ch-undeploy", "channel.yaml"))
	if !strings.Contains(string(data), "enabled: false") {
		t.Fatal("expected channel.yaml to have enabled: false after undeploy")
	}
}

func TestHandleChannelAction_UndeployFuncError(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "ch-fail", true)

	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: dir,
		Store:       storage.NewMemoryStore(0, 0),
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
		UndeployFunc: func(_ context.Context, _ string) error {
			return errors.New("undeploy failed")
		},
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/ch-fail/undeploy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandleChannelAction_Restart(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "ch-restart", true)

	called := false
	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: dir,
		Store:       storage.NewMemoryStore(0, 0),
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
		RestartFunc: func(_ context.Context, channelID string) error {
			called = true
			return nil
		},
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/ch-restart/restart", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !called {
		t.Fatal("expected restartFn to be called")
	}
	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["action"] != "restarted" {
		t.Fatalf("expected action=restarted, got %v", result["action"])
	}
}

func TestHandleChannelAction_RestartFuncError(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "ch-resterr", true)

	s := NewServer(&ServerConfig{
		Config:      &config.Config{},
		ChannelsDir: dir,
		Store:       storage.NewMemoryStore(0, 0),
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
		RestartFunc: func(_ context.Context, _ string) error {
			return errors.New("restart failed")
		},
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/ch-resterr/restart", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandleChannelAction_UnknownAction(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/ch-1/bogus", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleChannelAction_PostMethodRequired(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/channels/ch-1/deploy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleChannelAction_NoActionPOSTBadRequest(t *testing.T) {
	s := newTestServer(nil)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/ch-1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleChannelAction_DeployMissingChannel(t *testing.T) {
	dir := t.TempDir()
	s := newTestServerWithDir(nil, dir)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodPost, "/api/channels/nonexistent/deploy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// --- handleChannelDetail ---

func TestHandleChannelDetail_Found(t *testing.T) {
	dir := t.TempDir()
	chDir := filepath.Join(dir, "detail-ch")
	os.MkdirAll(chDir, 0o755)
	yaml := `id: detail-ch
enabled: true
description: A detailed channel
tags:
  - hl7
group: test-group
priority: high
listener:
  type: http
  http:
    port: 8080
    path: /inbound
    methods:
      - POST
pipeline:
  preprocessor: preprocess.ts
  validator: validate.ts
  source_filter: filter.ts
  transformer: transform.ts
  postprocessor: postprocess.ts
data_types:
  inbound: hl7v2
  outbound: fhir
destinations:
  - name: d1
    type: http
    http:
      url: http://example.com
      method: POST
      timeout_ms: 5000
`
	os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte(yaml), 0o644)

	s := newTestServerWithDir(storage.NewMemoryStore(0, 0), dir)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/channels/detail-ch", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result map[string]any
	json.NewDecoder(w.Body).Decode(&result)
	if result["id"] != "detail-ch" {
		t.Fatalf("expected id=detail-ch, got %v", result["id"])
	}
	if result["description"] != "A detailed channel" {
		t.Fatalf("expected description, got %v", result["description"])
	}
	if result["group"] != "test-group" {
		t.Fatalf("expected group, got %v", result["group"])
	}
	if result["priority"] != "high" {
		t.Fatalf("expected priority, got %v", result["priority"])
	}
	if result["pipeline"] == nil {
		t.Fatal("expected pipeline field")
	}
	if result["data_types"] == nil {
		t.Fatal("expected data_types field")
	}
	if result["listener"] == nil {
		t.Fatal("expected listener field")
	}
}

func TestHandleChannelDetail_NotFound(t *testing.T) {
	dir := t.TempDir()
	s := newTestServerWithDir(nil, dir)
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/channels/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleChannelDetail_ProfileMismatch(t *testing.T) {
	dir := t.TempDir()
	chDir := filepath.Join(dir, "profile-ch")
	os.MkdirAll(chDir, 0o755)
	yaml := "id: profile-ch\nenabled: true\nprofiles:\n  - prod\nlistener:\n  type: http\n  http:\n    port: 8080\n"
	os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte(yaml), 0o644)

	cfg := &config.Config{}
	cfg.Runtime.Profile = "dev"
	s := NewServer(&ServerConfig{
		Config:      cfg,
		ChannelsDir: dir,
		Store:       nil,
		Metrics:     observability.NewMetrics(),
		Logger:      slog.Default(),
	})
	handler := s.BuildHandler()

	req := httptest.NewRequest(http.MethodGet, "/api/channels/profile-ch", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for profile mismatch, got %d", w.Code)
	}
}

// --- setChannelEnabledDashboard ---

func TestSetChannelEnabledDashboard_Enable(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "enable-ch", false)

	err := setChannelEnabledDashboard(dir, "enable-ch", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "enable-ch", "channel.yaml"))
	if !strings.Contains(string(data), "enabled: true") {
		t.Fatal("expected enabled: true in channel.yaml")
	}
}

func TestSetChannelEnabledDashboard_Disable(t *testing.T) {
	dir := t.TempDir()
	createChannelDir(t, dir, "disable-ch", true)

	err := setChannelEnabledDashboard(dir, "disable-ch", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "disable-ch", "channel.yaml"))
	if !strings.Contains(string(data), "enabled: false") {
		t.Fatal("expected enabled: false in channel.yaml")
	}
}

func TestSetChannelEnabledDashboard_ChannelNotFound(t *testing.T) {
	dir := t.TempDir()
	err := setChannelEnabledDashboard(dir, "nonexistent", true)
	if err == nil {
		t.Fatal("expected error for nonexistent channel")
	}
}

// --- FormAuth / BasicAuthMiddleware ---

func TestFormAuth_LoginFlow(t *testing.T) {
	fa := NewFormAuth("admin", "secret")
	mw := fa.Middleware()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})
	handler := mw(inner)

	// GET /login -> serves login page
	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /login, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "<form") {
		t.Fatal("expected login form HTML")
	}

	// POST /login with correct creds -> redirect
	form := url.Values{}
	form.Set("username", "admin")
	form.Set("password", "secret")
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect after login, got %d", w.Code)
	}
	cookies := w.Result().Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "intu_session" {
			sessionCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("expected intu_session cookie after login")
	}

	// Access / with session cookie -> should pass through to inner handler
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with session, got %d", w.Code)
	}

	// Access /api/stats with session cookie -> passes
	req = httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for API with session, got %d", w.Code)
	}
}

func TestFormAuth_LoginBadCredentials(t *testing.T) {
	fa := NewFormAuth("admin", "secret")
	mw := fa.Middleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	form := url.Values{}
	form.Set("username", "admin")
	form.Set("password", "wrong")
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with error page, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Invalid username or password") {
		t.Fatal("expected error message in login page")
	}
}

func TestFormAuth_LoginMethodNotAllowed(t *testing.T) {
	fa := NewFormAuth("admin", "secret")
	mw := fa.Middleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	req := httptest.NewRequest(http.MethodPut, "/login", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestFormAuth_APIBasicAuth(t *testing.T) {
	fa := NewFormAuth("admin", "secret")
	mw := fa.Middleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := r.Header.Get("X-Auth-User")
		fmt.Fprint(w, user)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.SetBasicAuth("admin", "secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with basic auth, got %d", w.Code)
	}
	if w.Body.String() != "admin" {
		t.Fatalf("expected X-Auth-User=admin, got %s", w.Body.String())
	}
}

func TestFormAuth_APINoAuth(t *testing.T) {
	fa := NewFormAuth("admin", "secret")
	mw := fa.Middleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/data", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated API, got %d", w.Code)
	}
}

func TestFormAuth_NonAPIRedirectToLogin(t *testing.T) {
	fa := NewFormAuth("admin", "secret")
	mw := fa.Middleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect to /login, got %d", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/login" {
		t.Fatalf("expected redirect to /login, got %s", loc)
	}
}

func TestFormAuth_Logout(t *testing.T) {
	fa := NewFormAuth("admin", "secret")
	mw := fa.Middleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Login first
	form := url.Values{}
	form.Set("username", "admin")
	form.Set("password", "secret")
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var sessionCookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "intu_session" {
			sessionCookie = c
		}
	}

	// Logout
	req = httptest.NewRequest(http.MethodGet, "/logout", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect after logout, got %d", w.Code)
	}

	// Using old session should now fail
	req = httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 after logout, got %d", w.Code)
	}
}

func TestBasicAuthMiddleware(t *testing.T) {
	mw := BasicAuthMiddleware("user", "pass")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.SetBasicAuth("user", "pass")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- BuildHandler with auth middleware ---

func TestBuildHandler_WithAuthMiddleware(t *testing.T) {
	s := NewServer(&ServerConfig{
		Config:         &config.Config{},
		ChannelsDir:    "/tmp/nonexistent",
		Store:          nil,
		Metrics:        observability.NewMetrics(),
		Logger:         slog.Default(),
		AuthMiddleware: BasicAuthMiddleware("admin", "pass"),
	})
	handler := s.BuildHandler()

	// Without auth -> redirected
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", w.Code)
	}

	// API without auth -> 401
	req = httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}

	// API with auth -> 200
	req = httptest.NewRequest(http.MethodGet, "/api/metrics", nil)
	req.SetBasicAuth("admin", "pass")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- writeJSON ---

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusCreated, map[string]string{"hello": "world"})

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %s", ct)
	}
	var result map[string]string
	json.NewDecoder(w.Body).Decode(&result)
	if result["hello"] != "world" {
		t.Fatalf("expected hello=world, got %v", result)
	}
}

// --- generateSessionToken ---

func TestGenerateSessionToken(t *testing.T) {
	t1 := generateSessionToken()
	t2 := generateSessionToken()
	if len(t1) != 64 {
		t.Fatalf("expected 64 hex chars, got %d", len(t1))
	}
	if t1 == t2 {
		t.Fatal("expected unique tokens")
	}
}

// --- deduplicateMessages ---

func TestDeduplicateMessages_Empty(t *testing.T) {
	result := deduplicateMessages(nil)
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %d", len(result))
	}
}

func TestDeduplicateMessages_NoDuplicates(t *testing.T) {
	records := []*storage.MessageRecord{
		{ID: "a", Status: "RECEIVED"},
		{ID: "b", Status: "SENT"},
	}
	result := deduplicateMessages(records)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestDeduplicateMessages_KeepsHigherPriority(t *testing.T) {
	records := []*storage.MessageRecord{
		{ID: "a", Status: "RECEIVED"},
		{ID: "a", Status: "TRANSFORMED"},
		{ID: "a", Status: "SENT"},
		{ID: "a", Status: "ERROR"},
	}
	result := deduplicateMessages(records)
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	if result[0].Status != "ERROR" {
		t.Fatalf("expected ERROR (highest priority), got %s", result[0].Status)
	}
}

// --- listenerConfigMap ---

func TestListenerConfigMap_HTTP(t *testing.T) {
	l := config.ListenerConfig{
		Type: "http",
		HTTP: &config.HTTPListener{Port: 8080, Path: "/api", Methods: []string{"POST"}},
	}
	m := listenerConfigMap(l)
	if m["type"] != "http" {
		t.Fatalf("expected type=http, got %v", m["type"])
	}
	cfg := m["config"].(map[string]any)
	if cfg["port"] != 8080 {
		t.Fatalf("expected port=8080, got %v", cfg["port"])
	}
	if cfg["path"] != "/api" {
		t.Fatalf("expected path=/api, got %v", cfg["path"])
	}
}

func TestListenerConfigMap_TCP(t *testing.T) {
	l := config.ListenerConfig{
		Type: "tcp",
		TCP:  &config.TCPListener{Port: 2575, Mode: "mllp", MaxConnections: 10, TimeoutMs: 5000},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["port"] != 2575 {
		t.Fatalf("expected port=2575, got %v", cfg["port"])
	}
	if cfg["mode"] != "mllp" {
		t.Fatalf("expected mode=mllp, got %v", cfg["mode"])
	}
}

func TestListenerConfigMap_File(t *testing.T) {
	l := config.ListenerConfig{
		Type: "file",
		File: &config.FileListener{Directory: "/tmp/in", PollInterval: "5s", FilePattern: "*.hl7", Scheme: "local", MoveTo: "/tmp/done"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["directory"] != "/tmp/in" {
		t.Fatalf("expected directory=/tmp/in, got %v", cfg["directory"])
	}
	if cfg["scheme"] != "local" {
		t.Fatalf("expected scheme=local, got %v", cfg["scheme"])
	}
}

func TestListenerConfigMap_Kafka(t *testing.T) {
	l := config.ListenerConfig{
		Type:  "kafka",
		Kafka: &config.KafkaListener{Brokers: []string{"b1"}, Topic: "t1", GroupID: "g1"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["topic"] != "t1" {
		t.Fatalf("expected topic=t1, got %v", cfg["topic"])
	}
	if cfg["group_id"] != "g1" {
		t.Fatalf("expected group_id=g1, got %v", cfg["group_id"])
	}
}

func TestListenerConfigMap_Database(t *testing.T) {
	l := config.ListenerConfig{
		Type:     "database",
		Database: &config.DBListener{Driver: "postgres", PollInterval: "30s"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["driver"] != "postgres" {
		t.Fatalf("expected driver=postgres, got %v", cfg["driver"])
	}
}

func TestListenerConfigMap_Channel(t *testing.T) {
	l := config.ListenerConfig{
		Type:    "channel",
		Channel: &config.ChannelListener{SourceChannelID: "src-ch"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["source_channel_id"] != "src-ch" {
		t.Fatalf("expected source_channel_id=src-ch, got %v", cfg["source_channel_id"])
	}
}

func TestListenerConfigMap_DICOM(t *testing.T) {
	l := config.ListenerConfig{
		Type:  "dicom",
		DICOM: &config.DICOMListener{Port: 11112, AETitle: "MY_AE"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["port"] != 11112 {
		t.Fatalf("expected port=11112, got %v", cfg["port"])
	}
	if cfg["ae_title"] != "MY_AE" {
		t.Fatalf("expected ae_title=MY_AE, got %v", cfg["ae_title"])
	}
}

func TestListenerConfigMap_FHIR(t *testing.T) {
	l := config.ListenerConfig{
		Type: "fhir",
		FHIR: &config.FHIRListener{Port: 9090, BasePath: "/fhir", Version: "R4"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["base_path"] != "/fhir" {
		t.Fatalf("expected base_path=/fhir, got %v", cfg["base_path"])
	}
	if cfg["version"] != "R4" {
		t.Fatalf("expected version=R4, got %v", cfg["version"])
	}
}

func TestListenerConfigMap_Email(t *testing.T) {
	l := config.ListenerConfig{
		Type:  "email",
		Email: &config.EmailListener{Host: "imap.example.com", Port: 993, Protocol: "imaps", PollInterval: "60s", Folder: "INBOX"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["host"] != "imap.example.com" {
		t.Fatalf("expected host=imap.example.com, got %v", cfg["host"])
	}
	if cfg["protocol"] != "imaps" {
		t.Fatalf("expected protocol=imaps, got %v", cfg["protocol"])
	}
}

func TestListenerConfigMap_SOAP(t *testing.T) {
	l := config.ListenerConfig{
		Type: "soap",
		SOAP: &config.SOAPListener{Port: 8443, ServiceName: "PatientService"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["service_name"] != "PatientService" {
		t.Fatalf("expected service_name=PatientService, got %v", cfg["service_name"])
	}
}

func TestListenerConfigMap_IHE(t *testing.T) {
	l := config.ListenerConfig{
		Type: "ihe",
		IHE:  &config.IHEListener{Port: 3600, Profile: "XDS.b"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["profile"] != "XDS.b" {
		t.Fatalf("expected profile=XDS.b, got %v", cfg["profile"])
	}
}

func TestListenerConfigMap_SFTP(t *testing.T) {
	l := config.ListenerConfig{
		Type: "sftp",
		SFTP: &config.SFTPListener{Host: "sftp.example.com", Port: 22, Directory: "/uploads", PollInterval: "10s", FilePattern: "*.csv", MoveTo: "/done"},
	}
	m := listenerConfigMap(l)
	cfg := m["config"].(map[string]any)
	if cfg["host"] != "sftp.example.com" {
		t.Fatalf("expected host=sftp.example.com, got %v", cfg["host"])
	}
	if cfg["directory"] != "/uploads" {
		t.Fatalf("expected directory=/uploads, got %v", cfg["directory"])
	}
}

func TestListenerConfigMap_UnknownType(t *testing.T) {
	l := config.ListenerConfig{Type: "custom"}
	m := listenerConfigMap(l)
	if m["type"] != "custom" {
		t.Fatalf("expected type=custom, got %v", m["type"])
	}
	if m["config"] != nil {
		t.Fatal("expected nil config for unknown type")
	}
}

// --- destinationConfigMap ---

func TestDestinationConfigMap_HTTP(t *testing.T) {
	d := config.ChannelDestination{
		HTTP: &config.HTTPDestConfig{URL: "http://dest.com/api", Method: "POST", TimeoutMs: 3000},
	}
	m := destinationConfigMap(d)
	if m["url"] != "http://dest.com/api" {
		t.Fatalf("expected url, got %v", m["url"])
	}
	if m["method"] != "POST" {
		t.Fatalf("expected method=POST, got %v", m["method"])
	}
	if m["timeout_ms"] != 3000 {
		t.Fatalf("expected timeout_ms=3000, got %v", m["timeout_ms"])
	}
}

func TestDestinationConfigMap_TCP(t *testing.T) {
	d := config.ChannelDestination{
		TCP: &config.TCPDestConfig{Host: "10.0.0.1", Port: 2575},
	}
	m := destinationConfigMap(d)
	if m["host"] != "10.0.0.1" {
		t.Fatalf("expected host, got %v", m["host"])
	}
	if m["port"] != 2575 {
		t.Fatalf("expected port=2575, got %v", m["port"])
	}
}

func TestDestinationConfigMap_File(t *testing.T) {
	d := config.ChannelDestination{
		File: &config.FileDestConfig{Directory: "/out", FilenamePattern: "${id}.hl7"},
	}
	m := destinationConfigMap(d)
	if m["directory"] != "/out" {
		t.Fatalf("expected directory=/out, got %v", m["directory"])
	}
}

func TestDestinationConfigMap_Kafka(t *testing.T) {
	d := config.ChannelDestination{
		Kafka: &config.KafkaDestConfig{Topic: "output-topic"},
	}
	m := destinationConfigMap(d)
	if m["topic"] != "output-topic" {
		t.Fatalf("expected topic, got %v", m["topic"])
	}
}

func TestDestinationConfigMap_Database(t *testing.T) {
	d := config.ChannelDestination{
		Database: &config.DBDestConfig{Driver: "mysql"},
	}
	m := destinationConfigMap(d)
	if m["driver"] != "mysql" {
		t.Fatalf("expected driver=mysql, got %v", m["driver"])
	}
}

func TestDestinationConfigMap_Channel(t *testing.T) {
	d := config.ChannelDestination{
		ChannelDest: &config.ChannelDestRef{TargetChannelID: "target-ch"},
	}
	m := destinationConfigMap(d)
	if m["target_channel_id"] != "target-ch" {
		t.Fatalf("expected target_channel_id, got %v", m["target_channel_id"])
	}
}

func TestDestinationConfigMap_FHIR(t *testing.T) {
	d := config.ChannelDestination{
		FHIR: &config.FHIRDestConfig{BaseURL: "http://fhir.server", Version: "R4"},
	}
	m := destinationConfigMap(d)
	if m["base_url"] != "http://fhir.server" {
		t.Fatalf("expected base_url, got %v", m["base_url"])
	}
}

func TestDestinationConfigMap_SMTP(t *testing.T) {
	d := config.ChannelDestination{
		SMTP: &config.SMTPDestConfig{Host: "smtp.example.com", To: []string{"a@b.com"}},
	}
	m := destinationConfigMap(d)
	if m["smtp_host"] != "smtp.example.com" {
		t.Fatalf("expected smtp_host, got %v", m["smtp_host"])
	}
}

func TestDestinationConfigMap_FilterAndTransformer(t *testing.T) {
	d := config.ChannelDestination{
		Filter:      "filterScript.ts",
		Transformer: &config.ScriptRef{Entrypoint: "transform.ts"},
	}
	m := destinationConfigMap(d)
	if m["filter"] != "filterScript.ts" {
		t.Fatalf("expected filter, got %v", m["filter"])
	}
	if m["transformer"] != "transform.ts" {
		t.Fatalf("expected transformer, got %v", m["transformer"])
	}
}

func TestDestinationConfigMap_Empty(t *testing.T) {
	d := config.ChannelDestination{}
	m := destinationConfigMap(d)
	if len(m) != 0 {
		t.Fatalf("expected empty map, got %v", m)
	}
}
