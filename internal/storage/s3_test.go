package storage

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/intuware/intu-dev/pkg/config"
)

// mockS3Server simulates S3 API responses using an in-memory object store.
type mockS3Server struct {
	mu      sync.RWMutex
	objects map[string][]byte // key -> body
	srv     *httptest.Server
}

type listBucketResult struct {
	XMLName     xml.Name       `xml:"ListBucketResult"`
	XMLNS       string         `xml:"xmlns,attr"`
	Contents    []s3Object     `xml:"Contents"`
	IsTruncated bool           `xml:"IsTruncated"`
	KeyCount    int            `xml:"KeyCount"`
}

type s3Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int    `xml:"Size"`
	ETag         string `xml:"ETag"`
}

type deleteRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	Objects []deleteObject `xml:"Object"`
	Quiet   bool           `xml:"Quiet"`
}

type deleteObject struct {
	Key string `xml:"Key"`
}

type deleteResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	XMLNS   string   `xml:"xmlns,attr"`
}

func newMockS3Server() *mockS3Server {
	m := &mockS3Server{objects: make(map[string][]byte)}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *mockS3Server) URL() string { return m.srv.URL }
func (m *mockS3Server) Close()      { m.srv.Close() }

func (m *mockS3Server) handler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	// Strip leading /bucket-name
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)
	if len(parts) < 1 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	switch r.Method {
	case http.MethodPut:
		m.handlePut(w, r, key)
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			m.handleList(w, r)
			return
		}
		m.handleGet(w, r, key)
	case http.MethodDelete:
		m.handleDelete(w, r, key)
	case http.MethodPost:
		if _, ok := r.URL.Query()["delete"]; ok {
			m.handleDeleteObjects(w, r)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *mockS3Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}
	m.mu.Lock()
	m.objects[key] = body
	m.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3Server) handleGet(w http.ResponseWriter, _ *http.Request, key string) {
	m.mu.RLock()
	body, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>not found</Message></Error>`)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (m *mockS3Server) handleList(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")

	m.mu.RLock()
	var contents []s3Object
	for k, v := range m.objects {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			contents = append(contents, s3Object{
				Key:          k,
				LastModified: time.Now().UTC().Format(time.RFC3339),
				Size:         len(v),
				ETag:         `"mock-etag"`,
			})
		}
	}
	m.mu.RUnlock()

	result := listBucketResult{
		XMLNS:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Contents:    contents,
		IsTruncated: false,
		KeyCount:    len(contents),
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(result)
}

func (m *mockS3Server) handleDelete(w http.ResponseWriter, _ *http.Request, key string) {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockS3Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}
	var req deleteRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad xml", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	for _, obj := range req.Objects {
		delete(m.objects, obj.Key)
	}
	m.mu.Unlock()

	result := deleteResult{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/"}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(result)
}

func (m *mockS3Server) objectCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.objects)
}

// newTestS3Store creates an S3Store backed by the mock server.
func newTestS3Store(t *testing.T, mock *mockS3Server, prefix string) *S3Store {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(mock.URL())
		o.UsePathStyle = true
	})
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &S3Store{
		client: client,
		bucket: "test-bucket",
		prefix: prefix,
	}
}

func makeRecord(id, channelID, stage, status string, content []byte, ts time.Time) *MessageRecord {
	return &MessageRecord{
		ID:            id,
		CorrelationID: "corr-" + id,
		ChannelID:     channelID,
		Stage:         stage,
		Content:       content,
		Status:        status,
		Timestamp:     ts,
		DurationMs:    100,
		Metadata:      map[string]any{"src": "test"},
	}
}

// --- Constructor tests ---

func TestNewS3Store_NilConfig(t *testing.T) {
	_, err := NewS3Store(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	if err.Error() != "S3 bucket is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewS3Store_EmptyBucket(t *testing.T) {
	_, err := NewS3Store(&config.StorageS3Config{Bucket: "", Region: "us-east-1"})
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
}

func TestNewS3Store_ValidConfig(t *testing.T) {
	store, err := NewS3Store(&config.StorageS3Config{
		Bucket: "test-bucket",
		Region: "us-west-2",
		Prefix: "messages",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.bucket != "test-bucket" {
		t.Errorf("expected bucket 'test-bucket', got %q", store.bucket)
	}
	if store.prefix != "messages/" {
		t.Errorf("expected prefix 'messages/', got %q", store.prefix)
	}
}

func TestNewS3Store_PrefixWithTrailingSlash(t *testing.T) {
	store, err := NewS3Store(&config.StorageS3Config{Bucket: "b", Prefix: "data/"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.prefix != "data/" {
		t.Errorf("expected 'data/', got %q", store.prefix)
	}
}

func TestNewS3Store_EmptyPrefix(t *testing.T) {
	store, err := NewS3Store(&config.StorageS3Config{Bucket: "b"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.prefix != "" {
		t.Errorf("expected empty prefix, got %q", store.prefix)
	}
}

func TestNewS3Store_WithEndpoint(t *testing.T) {
	store, err := NewS3Store(&config.StorageS3Config{
		Bucket:   "b",
		Region:   "us-east-1",
		Endpoint: "http://localhost:9000",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

// --- objectKey tests ---

func TestS3Store_ObjectKey_NoPrefix(t *testing.T) {
	store := &S3Store{prefix: ""}
	got := store.objectKey("ch-1", "received", "msg-abc")
	want := "ch-1/received/msg-abc.json"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestS3Store_ObjectKey_WithPrefix(t *testing.T) {
	store := &S3Store{prefix: "intu/"}
	got := store.objectKey("ch-1", "received", "msg-abc")
	want := "intu/ch-1/received/msg-abc.json"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestS3Store_ObjectKey_VariousStages(t *testing.T) {
	store := &S3Store{prefix: "prod/"}
	cases := []struct {
		channelID, stage, id, want string
	}{
		{"ch-1", "received", "id-1", "prod/ch-1/received/id-1.json"},
		{"ch-2", "transformed", "id-2", "prod/ch-2/transformed/id-2.json"},
		{"ch-3", "sent", "id-3", "prod/ch-3/sent/id-3.json"},
		{"ch-4", "errored", "id-4", "prod/ch-4/errored/id-4.json"},
	}
	for _, tc := range cases {
		got := store.objectKey(tc.channelID, tc.stage, tc.id)
		if got != tc.want {
			t.Errorf("objectKey(%q,%q,%q)=%q, want %q", tc.channelID, tc.stage, tc.id, got, tc.want)
		}
	}
}

// --- Close ---

func TestS3Store_Close(t *testing.T) {
	store := &S3Store{}
	if err := store.Close(); err != nil {
		t.Fatalf("Close should return nil: %v", err)
	}
}

// --- Save ---

func TestS3Store_Save(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "pfx")

	rec := makeRecord("msg-1", "ch-1", "received", "success", []byte("hello"), time.Now().UTC())
	if err := store.Save(rec); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if mock.objectCount() != 1 {
		t.Fatalf("expected 1 object, got %d", mock.objectCount())
	}

	mock.mu.RLock()
	raw, ok := mock.objects["pfx/ch-1/received/msg-1.json"]
	mock.mu.RUnlock()
	if !ok {
		t.Fatal("expected object at pfx/ch-1/received/msg-1.json")
	}

	var env s3MessageEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal stored object: %v", err)
	}
	if env.ID != "msg-1" {
		t.Errorf("ID=%q, want msg-1", env.ID)
	}
	if env.CorrelationID != "corr-msg-1" {
		t.Errorf("CorrelationID=%q, want corr-msg-1", env.CorrelationID)
	}
	if string(env.Content) != "hello" {
		t.Errorf("Content=%q, want hello", env.Content)
	}
	if env.Status != "success" {
		t.Errorf("Status=%q, want success", env.Status)
	}
}

func TestS3Store_Save_ContentSizeAutoFilled(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	rec := makeRecord("m1", "ch-1", "received", "success", []byte("hello world"), time.Now().UTC())
	rec.ContentSize = 0
	if err := store.Save(rec); err != nil {
		t.Fatalf("Save: %v", err)
	}

	mock.mu.RLock()
	raw := mock.objects["ch-1/received/m1.json"]
	mock.mu.RUnlock()

	var env s3MessageEnvelope
	json.Unmarshal(raw, &env)
	if env.ContentSize != 11 {
		t.Errorf("ContentSize=%d, want 11", env.ContentSize)
	}
}

func TestS3Store_Save_ContentSizePreserved(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	rec := makeRecord("m1", "ch-1", "received", "success", []byte("hi"), time.Now().UTC())
	rec.ContentSize = 999
	if err := store.Save(rec); err != nil {
		t.Fatalf("Save: %v", err)
	}

	mock.mu.RLock()
	raw := mock.objects["ch-1/received/m1.json"]
	mock.mu.RUnlock()

	var env s3MessageEnvelope
	json.Unmarshal(raw, &env)
	if env.ContentSize != 999 {
		t.Errorf("ContentSize=%d, want 999", env.ContentSize)
	}
}

func TestS3Store_Save_MultipleRecords(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		rec := makeRecord(fmt.Sprintf("m-%d", i), "ch-1", "received", "success", []byte("data"), now)
		if err := store.Save(rec); err != nil {
			t.Fatalf("Save record %d: %v", i, err)
		}
	}
	if mock.objectCount() != 5 {
		t.Errorf("expected 5 objects, got %d", mock.objectCount())
	}
}

// --- Get ---

func TestS3Store_Get(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "data")

	now := time.Now().UTC().Truncate(time.Millisecond)
	rec := makeRecord("msg-42", "ch-A", "received", "success", []byte("payload"), now)
	if err := store.Save(rec); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := store.Get("msg-42")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != "msg-42" {
		t.Errorf("ID=%q, want msg-42", got.ID)
	}
	if got.ChannelID != "ch-A" {
		t.Errorf("ChannelID=%q, want ch-A", got.ChannelID)
	}
	if got.Stage != "received" {
		t.Errorf("Stage=%q, want received", got.Stage)
	}
	if string(got.Content) != "payload" {
		t.Errorf("Content=%q, want payload", got.Content)
	}
	if got.CorrelationID != "corr-msg-42" {
		t.Errorf("CorrelationID=%q, want corr-msg-42", got.CorrelationID)
	}
	if got.DurationMs != 100 {
		t.Errorf("DurationMs=%d, want 100", got.DurationMs)
	}
}

func TestS3Store_Get_NotFound(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	_, err := store.Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent record")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should contain 'not found': %v", err)
	}
}

// --- GetStage ---

func TestS3Store_GetStage(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("msg-1", "ch-1", "received", "success", []byte("raw"), now))
	store.Save(makeRecord("msg-1", "ch-1", "transformed", "success", []byte("xformed"), now))

	got, err := store.GetStage("msg-1", "transformed")
	if err != nil {
		t.Fatalf("GetStage: %v", err)
	}
	if got.Stage != "transformed" {
		t.Errorf("Stage=%q, want transformed", got.Stage)
	}
	if string(got.Content) != "xformed" {
		t.Errorf("Content=%q, want xformed", got.Content)
	}
}

func TestS3Store_GetStage_NotFound(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	store.Save(makeRecord("msg-1", "ch-1", "received", "success", []byte("x"), time.Now().UTC()))

	_, err := store.GetStage("msg-1", "sent")
	if err == nil {
		t.Fatal("expected error for nonexistent stage")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should contain 'not found': %v", err)
	}
}

func TestS3Store_GetStage_WrongID(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	store.Save(makeRecord("msg-1", "ch-1", "received", "success", []byte("x"), time.Now().UTC()))

	_, err := store.GetStage("msg-999", "received")
	if err == nil {
		t.Fatal("expected error for wrong id")
	}
}

// --- Query ---

func TestS3Store_Query_All(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("m2", "ch-1", "received", "error", []byte("b"), now))
	store.Save(makeRecord("m3", "ch-2", "sent", "success", []byte("c"), now))

	results, err := store.Query(QueryOpts{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestS3Store_Query_ByChannelID(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("m2", "ch-2", "received", "success", []byte("b"), now))
	store.Save(makeRecord("m3", "ch-1", "sent", "success", []byte("c"), now))

	results, err := store.Query(QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for ch-1, got %d", len(results))
	}
	for _, r := range results {
		if r.ChannelID != "ch-1" {
			t.Errorf("expected channelID ch-1, got %q", r.ChannelID)
		}
	}
}

func TestS3Store_Query_ByStatus(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("m2", "ch-1", "received", "error", []byte("b"), now))
	store.Save(makeRecord("m3", "ch-1", "received", "success", []byte("c"), now))

	results, err := store.Query(QueryOpts{Status: "error"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 error result, got %d", len(results))
	}
	if len(results) > 0 && results[0].ID != "m2" {
		t.Errorf("expected m2, got %q", results[0].ID)
	}
}

func TestS3Store_Query_ByStage(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("m2", "ch-1", "transformed", "success", []byte("b"), now))
	store.Save(makeRecord("m3", "ch-1", "sent", "success", []byte("c"), now))

	results, err := store.Query(QueryOpts{ChannelID: "ch-1", Stage: "transformed"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for stage transformed, got %d", len(results))
	}
}

func TestS3Store_Query_BySinceAndBefore(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1 * time.Hour)
	t2 := t0.Add(2 * time.Hour)
	t3 := t0.Add(3 * time.Hour)

	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), t0))
	store.Save(makeRecord("m2", "ch-1", "received", "success", []byte("b"), t1))
	store.Save(makeRecord("m3", "ch-1", "received", "success", []byte("c"), t2))
	store.Save(makeRecord("m4", "ch-1", "received", "success", []byte("d"), t3))

	results, err := store.Query(QueryOpts{Since: t1, Before: t2.Add(30 * time.Minute)})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results (t1 and t2), got %d", len(results))
	}
}

func TestS3Store_Query_WithLimit(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	for i := 0; i < 10; i++ {
		store.Save(makeRecord(fmt.Sprintf("m%d", i), "ch-1", "received", "success", []byte("x"), now))
	}

	results, err := store.Query(QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results with limit, got %d", len(results))
	}
}

func TestS3Store_Query_WithOffset(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		store.Save(makeRecord(fmt.Sprintf("m%d", i), "ch-1", "received", "success", []byte("x"), now))
	}

	results, err := store.Query(QueryOpts{Offset: 3, Limit: 10})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results with offset 3, got %d", len(results))
	}
}

func TestS3Store_Query_OffsetBeyondResults(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("x"), now))

	results, err := store.Query(QueryOpts{Offset: 100})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for large offset, got %d", len(results))
	}
}

func TestS3Store_Query_ExcludeContent(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("big payload"), now))

	results, err := store.Query(QueryOpts{ExcludeContent: true})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Content != nil {
		t.Errorf("expected nil content with ExcludeContent, got %q", results[0].Content)
	}
}

func TestS3Store_Query_EmptyStore(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	results, err := store.Query(QueryOpts{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results from empty store, got %d", len(results))
	}
}

func TestS3Store_Query_CombinedFilters(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	t0 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1 * time.Hour)
	t2 := t0.Add(2 * time.Hour)

	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), t0))
	store.Save(makeRecord("m2", "ch-1", "received", "error", []byte("b"), t1))
	store.Save(makeRecord("m3", "ch-1", "sent", "success", []byte("c"), t1))
	store.Save(makeRecord("m4", "ch-2", "received", "success", []byte("d"), t1))
	store.Save(makeRecord("m5", "ch-1", "received", "success", []byte("e"), t2))

	results, err := store.Query(QueryOpts{
		ChannelID: "ch-1",
		Stage:     "received",
		Status:    "success",
		Since:     t0.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result matching all filters, got %d", len(results))
	}
	if len(results) > 0 && results[0].ID != "m5" {
		t.Errorf("expected m5, got %q", results[0].ID)
	}
}

// --- Count ---

func TestS3Store_Count_All(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("m2", "ch-1", "sent", "success", []byte("b"), now))
	store.Save(makeRecord("m3", "ch-2", "received", "error", []byte("c"), now))

	count, err := store.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestS3Store_Count_ByChannel(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("m2", "ch-2", "received", "success", []byte("b"), now))
	store.Save(makeRecord("m3", "ch-1", "sent", "success", []byte("c"), now))

	count, err := store.Count(QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}
}

func TestS3Store_Count_ByStatus(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("m2", "ch-1", "received", "error", []byte("b"), now))
	store.Save(makeRecord("m3", "ch-1", "received", "success", []byte("c"), now))

	count, err := store.Count(QueryOpts{Status: "error"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
}

func TestS3Store_Count_ByChannelAndStage(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("m2", "ch-1", "sent", "success", []byte("b"), now))
	store.Save(makeRecord("m3", "ch-2", "received", "success", []byte("c"), now))

	count, err := store.Count(QueryOpts{ChannelID: "ch-1", Stage: "received"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
}

func TestS3Store_Count_EmptyStore(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	count, err := store.Count(QueryOpts{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}
}

func TestS3Store_Count_WithTimeFilters(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1 * time.Hour)
	t2 := t0.Add(2 * time.Hour)

	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), t0))
	store.Save(makeRecord("m2", "ch-1", "received", "success", []byte("b"), t1))
	store.Save(makeRecord("m3", "ch-1", "received", "success", []byte("c"), t2))

	count, err := store.Count(QueryOpts{Since: t1})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected count 2 (at or after t1), got %d", count)
	}
}

// --- Delete ---

func TestS3Store_Delete(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	store.Save(makeRecord("msg-del", "ch-1", "received", "success", []byte("a"), now))
	store.Save(makeRecord("msg-del", "ch-1", "sent", "success", []byte("b"), now))
	store.Save(makeRecord("msg-keep", "ch-1", "received", "success", []byte("c"), now))

	if err := store.Delete("msg-del"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if mock.objectCount() != 1 {
		t.Errorf("expected 1 remaining object, got %d", mock.objectCount())
	}

	_, err := store.Get("msg-del")
	if err == nil {
		t.Fatal("expected error after delete")
	}

	got, err := store.Get("msg-keep")
	if err != nil {
		t.Fatalf("Get msg-keep: %v", err)
	}
	if got.ID != "msg-keep" {
		t.Errorf("expected msg-keep, got %q", got.ID)
	}
}

func TestS3Store_Delete_Nonexistent(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	if err := store.Delete("no-such-id"); err != nil {
		t.Fatalf("Delete of nonexistent should not error: %v", err)
	}
}

func TestS3Store_Delete_AllStages(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	now := time.Now().UTC()
	stages := []string{"received", "transformed", "sent"}
	for _, s := range stages {
		store.Save(makeRecord("multi-stage", "ch-1", s, "success", []byte(s), now))
	}

	if mock.objectCount() != 3 {
		t.Fatalf("expected 3 objects, got %d", mock.objectCount())
	}

	if err := store.Delete("multi-stage"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if mock.objectCount() != 0 {
		t.Errorf("expected 0 objects after delete, got %d", mock.objectCount())
	}
}

// --- Prune ---

func TestS3Store_Prune(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	old := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	store.Save(makeRecord("old-1", "ch-1", "received", "success", []byte("a"), old))
	store.Save(makeRecord("old-2", "ch-1", "received", "error", []byte("b"), old))
	store.Save(makeRecord("new-1", "ch-1", "received", "success", []byte("c"), recent))

	pruned, err := store.Prune(cutoff, "")
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 2 {
		t.Errorf("expected 2 pruned, got %d", pruned)
	}
	if mock.objectCount() != 1 {
		t.Errorf("expected 1 remaining, got %d", mock.objectCount())
	}
}

func TestS3Store_Prune_WithChannelFilter(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	old := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), old))
	store.Save(makeRecord("m2", "ch-2", "received", "success", []byte("b"), old))

	pruned, err := store.Prune(cutoff, "ch-1")
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 1 {
		t.Errorf("expected 1 pruned, got %d", pruned)
	}
	if mock.objectCount() != 1 {
		t.Errorf("expected 1 remaining, got %d", mock.objectCount())
	}
}

func TestS3Store_Prune_NothingToPrune(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	recent := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), recent))

	pruned, err := store.Prune(cutoff, "")
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 0 {
		t.Errorf("expected 0 pruned, got %d", pruned)
	}
	if mock.objectCount() != 1 {
		t.Errorf("expected 1 remaining, got %d", mock.objectCount())
	}
}

func TestS3Store_Prune_EmptyStore(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "")

	pruned, err := store.Prune(time.Now(), "")
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if pruned != 0 {
		t.Errorf("expected 0 pruned, got %d", pruned)
	}
}

// --- Error paths ---

func TestS3Store_Save_ServerError(t *testing.T) {
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>InternalError</Code><Message>fail</Message></Error>`)
	}))
	defer errServer.Close()

	cfg, _ := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("t", "t", "")),
		awsconfig.WithRegion("us-east-1"),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(errServer.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3Store{client: client, bucket: "b", prefix: ""}

	err := store.Save(makeRecord("m1", "ch-1", "received", "success", []byte("x"), time.Now()))
	if err == nil {
		t.Fatal("expected error from failing server")
	}
	if !strings.Contains(err.Error(), "put S3 object") {
		t.Errorf("expected 'put S3 object' in error, got: %v", err)
	}
}

func TestS3Store_Get_ServerError(t *testing.T) {
	callCount := 0
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if r.URL.Query().Get("list-type") == "2" {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>InternalError</Code><Message>fail</Message></Error>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer errServer.Close()

	cfg, _ := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("t", "t", "")),
		awsconfig.WithRegion("us-east-1"),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(errServer.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3Store{client: client, bucket: "b", prefix: ""}

	_, err := store.Get("any-id")
	if err == nil {
		t.Fatal("expected error from failing list")
	}
}

func TestS3Store_Delete_ListError(t *testing.T) {
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>InternalError</Code><Message>fail</Message></Error>`)
	}))
	defer errServer.Close()

	cfg, _ := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("t", "t", "")),
		awsconfig.WithRegion("us-east-1"),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(errServer.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3Store{client: client, bucket: "b", prefix: ""}

	err := store.Delete("some-id")
	if err == nil {
		t.Fatal("expected error from failing list in Delete")
	}
	if !strings.Contains(err.Error(), "list objects") {
		t.Errorf("expected 'list objects' in error, got: %v", err)
	}
}

func TestS3Store_Count_ListError(t *testing.T) {
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>InternalError</Code><Message>fail</Message></Error>`)
	}))
	defer errServer.Close()

	cfg, _ := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("t", "t", "")),
		awsconfig.WithRegion("us-east-1"),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(errServer.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3Store{client: client, bucket: "b", prefix: ""}

	_, err := store.Count(QueryOpts{})
	if err == nil {
		t.Fatal("expected error from failing list in Count")
	}
}

func TestS3Store_Prune_ListError(t *testing.T) {
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>InternalError</Code><Message>fail</Message></Error>`)
	}))
	defer errServer.Close()

	cfg, _ := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("t", "t", "")),
		awsconfig.WithRegion("us-east-1"),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(errServer.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3Store{client: client, bucket: "b", prefix: ""}

	_, err := store.Prune(time.Now(), "")
	if err == nil {
		t.Fatal("expected error from failing list in Prune")
	}
}

// --- Integration-style round-trip tests ---

func TestS3Store_SaveGetRoundTrip(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "rt")

	now := time.Now().UTC().Truncate(time.Millisecond)
	rec := &MessageRecord{
		ID:            "rt-1",
		CorrelationID: "corr-rt-1",
		ChannelID:     "ch-rt",
		Stage:         "received",
		Content:       []byte(`{"hl7":"ADT^A01"}`),
		ContentSize:   18,
		Status:        "success",
		Timestamp:     now,
		DurationMs:    55,
		Metadata:      map[string]any{"patient": "john"},
	}

	if err := store.Save(rec); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := store.Get("rt-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ID != rec.ID {
		t.Errorf("ID mismatch: %q vs %q", got.ID, rec.ID)
	}
	if got.CorrelationID != rec.CorrelationID {
		t.Errorf("CorrelationID mismatch: %q vs %q", got.CorrelationID, rec.CorrelationID)
	}
	if got.ChannelID != rec.ChannelID {
		t.Errorf("ChannelID mismatch")
	}
	if got.Stage != rec.Stage {
		t.Errorf("Stage mismatch")
	}
	if string(got.Content) != string(rec.Content) {
		t.Errorf("Content mismatch: %q vs %q", got.Content, rec.Content)
	}
	if got.ContentSize != rec.ContentSize {
		t.Errorf("ContentSize mismatch: %d vs %d", got.ContentSize, rec.ContentSize)
	}
	if got.Status != rec.Status {
		t.Errorf("Status mismatch")
	}
	if got.DurationMs != rec.DurationMs {
		t.Errorf("DurationMs mismatch: %d vs %d", got.DurationMs, rec.DurationMs)
	}
}

func TestS3Store_SaveQueryDeleteRoundTrip(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()
	store := newTestS3Store(t, mock, "trip")

	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		store.Save(makeRecord(fmt.Sprintf("id-%d", i), "ch-trip", "received", "success", []byte("d"), now))
	}

	results, _ := store.Query(QueryOpts{ChannelID: "ch-trip"})
	if len(results) != 3 {
		t.Fatalf("expected 3, got %d", len(results))
	}

	count, _ := store.Count(QueryOpts{ChannelID: "ch-trip"})
	if count != 3 {
		t.Fatalf("expected count 3, got %d", count)
	}

	store.Delete("id-1")

	results, _ = store.Query(QueryOpts{ChannelID: "ch-trip"})
	if len(results) != 2 {
		t.Errorf("expected 2 after delete, got %d", len(results))
	}

	count, _ = store.Count(QueryOpts{ChannelID: "ch-trip"})
	if count != 2 {
		t.Errorf("expected count 2 after delete, got %d", count)
	}
}

func TestS3Store_WithPrefix_Isolation(t *testing.T) {
	mock := newMockS3Server()
	defer mock.Close()

	storeA := newTestS3Store(t, mock, "env-a")
	storeB := newTestS3Store(t, mock, "env-b")

	now := time.Now().UTC()
	storeA.Save(makeRecord("m1", "ch-1", "received", "success", []byte("a"), now))
	storeB.Save(makeRecord("m2", "ch-1", "received", "success", []byte("b"), now))

	resultsA, _ := storeA.Query(QueryOpts{})
	resultsB, _ := storeB.Query(QueryOpts{})

	if len(resultsA) != 1 || resultsA[0].ID != "m1" {
		t.Errorf("storeA should see only m1, got %d results", len(resultsA))
	}
	if len(resultsB) != 1 || resultsB[0].ID != "m2" {
		t.Errorf("storeB should see only m2, got %d results", len(resultsB))
	}
}
