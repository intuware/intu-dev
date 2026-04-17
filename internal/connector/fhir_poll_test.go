package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// FHIRPollSource — poll with mock FHIR server returning Bundle
// ===================================================================

func TestFHIRPollSource_Poll_MockFHIRServer_Bundle(t *testing.T) {
	bundle := map[string]any{
		"resourceType": "Bundle",
		"type":         "searchset",
		"entry": []any{
			map[string]any{
				"resource": map[string]any{
					"resourceType": "Patient",
					"id":           "pat-1",
					"name":         []any{map[string]any{"family": "Smith"}},
				},
			},
			map[string]any{
				"resource": map[string]any{
					"resourceType": "Patient",
					"id":           "pat-2",
					"name":         []any{map[string]any{"family": "Doe"}},
				},
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/fhir+json" {
			t.Errorf("expected Accept: application/fhir+json, got %q", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(bundle)
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
	}, testLogger())

	cap := &msgCapture{}
	err := src.poll(context.Background(), cap.handler(), "", "")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(cap.msgs))
	}

	if cap.msgs[0].Metadata["resource_type"] != "Patient" {
		t.Errorf("expected resource_type Patient, got %v", cap.msgs[0].Metadata["resource_type"])
	}
	if cap.msgs[0].Metadata["resource_id"] != "pat-1" {
		t.Errorf("expected resource_id pat-1, got %v", cap.msgs[0].Metadata["resource_id"])
	}
	if cap.msgs[1].Metadata["resource_id"] != "pat-2" {
		t.Errorf("expected resource_id pat-2, got %v", cap.msgs[1].Metadata["resource_id"])
	}
}

func TestFHIRPollSource_Poll_MockFHIRServer_SingleResource(t *testing.T) {
	resource := map[string]any{
		"resourceType": "Observation",
		"id":           "obs-1",
		"status":       "final",
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(resource)
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Observation"},
		Version:   "R4",
	}, testLogger())

	cap := &msgCapture{}
	err := src.poll(context.Background(), cap.handler(), "", "")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(cap.msgs))
	}
	if cap.msgs[0].Metadata["fhir_version"] != "R4" {
		t.Errorf("expected fhir_version R4, got %v", cap.msgs[0].Metadata["fhir_version"])
	}
	if cap.msgs[0].Transport != "fhir_poll" {
		t.Errorf("expected transport fhir_poll, got %q", cap.msgs[0].Transport)
	}
}

func TestFHIRPollSource_Poll_TimeBasedQueries(t *testing.T) {
	var receivedURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURL = r.URL.String()
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{
			"resourceType": "Bundle",
			"type":         "searchset",
			"entry":        []any{},
		})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "1h", "_lastUpdated")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if !strings.Contains(receivedURL, "_lastUpdated=ge") {
		t.Fatalf("expected _lastUpdated filter in URL, got %q", receivedURL)
	}
}

func TestFHIRPollSource_Poll_CustomDateParam(t *testing.T) {
	var receivedURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURL = r.URL.String()
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{
			"resourceType": "Bundle",
			"type":         "searchset",
			"entry":        []any{},
		})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Encounter"},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "30m", "date")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if !strings.Contains(receivedURL, "date=ge") {
		t.Fatalf("expected date=ge in URL, got %q", receivedURL)
	}
}

func TestFHIRPollSource_Poll_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
	}, testLogger())

	cap := &msgCapture{}
	err := src.poll(context.Background(), cap.handler(), "", "")
	if err != nil {
		t.Fatalf("poll should not return error for non-200 (logged instead): %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.msgs) != 0 {
		t.Fatalf("expected 0 messages on server error, got %d", len(cap.msgs))
	}
}

func TestFHIRPollSource_Start_MissingBaseURL(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "",
		Resources: []string{"Patient"},
	}, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err == nil {
		t.Fatal("expected error for missing base_url")
	}
}

func TestFHIRPollSource_Start_MissingResourcesAndQueries(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL: "http://fhir.test",
	}, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err == nil {
		t.Fatal("expected error for missing resources and search_queries")
	}
}

func TestFHIRPollSource_StartStop(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{
			"resourceType": "Bundle",
			"type":         "searchset",
			"entry":        []any{},
		})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:      ts.URL,
		Resources:    []string{"Patient"},
		PollInterval: "100ms",
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestFHIRPollSource_Type(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{}, testLogger())
	if src.Type() != "fhir_poll" {
		t.Fatalf("expected fhir_poll, got %q", src.Type())
	}
}

func TestFHIRPollSource_StopBeforeStart(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{}, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop before start: %v", err)
	}
}

func TestFHIRPollSource_Poll_ContextCancelled(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{
			"resourceType": "Bundle",
			"type":         "searchset",
			"entry":        []any{},
		})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
	}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := src.poll(ctx, noopHandler, "", "")
	if err != nil {
		// poll returns ctx.Err() when cancelled during query loop
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled or nil, got %v", err)
		}
	}
}

func TestFHIRPollSource_Poll_InvalidPollRange(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "invalid-duration", "_lastUpdated")
	if err == nil {
		t.Fatal("expected error for invalid poll range")
	}
}

func TestFHIRPollSource_Poll_EmptyBaseURL(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "",
		Resources: []string{"Patient"},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "", "")
	if err == nil {
		t.Fatal("expected error for empty base_url")
	}
}

func TestFHIRPollSource_Poll_AuthBearer(t *testing.T) {
	var authHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
		Auth:      &config.AuthConfig{Type: "bearer", Token: "my-fhir-token"},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "", "")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if authHeader != "Bearer my-fhir-token" {
		t.Fatalf("expected Bearer auth, got %q", authHeader)
	}
}

func TestFHIRPollSource_Poll_AuthBasic(t *testing.T) {
	var gotUser, gotPass string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
		Auth:      &config.AuthConfig{Type: "basic", Username: "fhir-user", Password: "fhir-pass"},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "", "")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if gotUser != "fhir-user" || gotPass != "fhir-pass" {
		t.Fatalf("expected fhir-user/fhir-pass, got %q/%q", gotUser, gotPass)
	}
}

// ===================================================================
// FHIRSubscriptionSource — rest-hook mode with httptest POST
// ===================================================================

func TestFHIRSubscriptionSource_RestHook_PostNotification(t *testing.T) {
	cap := &msgCapture{}
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType:    "rest-hook",
		Port:           0,
		Path:           "/fhir/notify",
		Version:        "R4",
		SubscriptionID: "sub-test-123",
	}, testLogger())

	err := src.Start(context.Background(), cap.handler())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	addr := src.Addr()
	if addr == "" {
		t.Fatal("expected non-empty addr")
	}

	notification := map[string]any{
		"resourceType": "Bundle",
		"type":         "subscription-notification",
		"subscription": "Subscription/sub-test-123",
		"eventNumber":  float64(42),
	}
	body, _ := json.Marshal(notification)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/fhir/notify", addr),
		"application/fhir+json",
		strings.NewReader(string(body)),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	time.Sleep(50 * time.Millisecond)
	cap.mu.Lock()
	defer cap.mu.Unlock()

	if len(cap.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(cap.msgs))
	}

	msg := cap.msgs[0]
	if msg.Transport != "fhir_subscription" {
		t.Errorf("expected transport fhir_subscription, got %q", msg.Transport)
	}
	if msg.ContentType != "fhir_r4" {
		t.Errorf("expected content type fhir_r4, got %q", msg.ContentType)
	}
	if msg.Metadata["channel_type"] != "rest-hook" {
		t.Errorf("expected channel_type rest-hook, got %v", msg.Metadata["channel_type"])
	}
	if msg.Metadata["fhir_version"] != "R4" {
		t.Errorf("expected fhir_version R4, got %v", msg.Metadata["fhir_version"])
	}
	if msg.Metadata["subscription_id"] != "Subscription/sub-test-123" {
		t.Errorf("expected subscription_id from payload, got %v", msg.Metadata["subscription_id"])
	}
	eventNum, ok := msg.Metadata["event_number"].(int)
	if !ok || eventNum != 42 {
		t.Errorf("expected event_number 42, got %v", msg.Metadata["event_number"])
	}
}

func TestFHIRSubscriptionSource_RestHook_MethodNotAllowed(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	addr := src.Addr()
	resp, err := http.Get(fmt.Sprintf("http://%s/fhir/subscription-notification", addr))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestFHIRSubscriptionSource_RestHook_HandlerError(t *testing.T) {
	errHandler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("handler error")
	}

	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
	}, testLogger())

	err := src.Start(context.Background(), errHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	addr := src.Addr()
	resp, err := http.Post(
		fmt.Sprintf("http://%s/fhir/subscription-notification", addr),
		"application/fhir+json",
		strings.NewReader(`{"resourceType":"Bundle"}`),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 on handler error, got %d", resp.StatusCode)
	}
}

func TestFHIRSubscriptionSource_RestHook_DefaultPath(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
		Path:        "",
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	addr := src.Addr()
	resp, err := http.Post(
		fmt.Sprintf("http://%s/fhir/subscription-notification", addr),
		"application/fhir+json",
		strings.NewReader(`{}`),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on default path, got %d", resp.StatusCode)
	}
}

func TestFHIRSubscriptionSource_RestHook_CustomPath(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
		Path:        "/custom/notify",
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	addr := src.Addr()
	resp, err := http.Post(
		fmt.Sprintf("http://%s/custom/notify", addr),
		"application/fhir+json",
		strings.NewReader(`{}`),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on custom path, got %d", resp.StatusCode)
	}
}

// ===================================================================
// DatabaseDest — driverName for all driver aliases
// ===================================================================

func TestDatabaseDest_DriverName_AllAliases(t *testing.T) {
	cases := []struct {
		driver string
		want   string
	}{
		{"postgres", "pgx"},
		{"postgresql", "pgx"},
		{"Postgres", "pgx"},
		{"PostgreSQL", "pgx"},
		{"mysql", "mysql"},
		{"MySQL", "mysql"},
		{"mssql", "sqlserver"},
		{"MSSQL", "sqlserver"},
		{"sqlserver", "sqlserver"},
		{"SqlServer", "sqlserver"},
		{"sqlite", "sqlite"},
		{"sqlite3", "sqlite"},
		{"SQLite", "sqlite"},
		{"SQLite3", "sqlite"},
		{"oracle", "oracle"},
		{"custom-driver", "custom-driver"},
		{"", ""},
	}

	for _, tc := range cases {
		dest := NewDatabaseDest("test", &config.DBDestMapConfig{Driver: tc.driver}, testLogger())
		got := dest.driverName()
		if got != tc.want {
			t.Errorf("driverName(%q) = %q, want %q", tc.driver, got, tc.want)
		}
	}
}

func TestDatabaseDest_Constructor(t *testing.T) {
	dest := NewDatabaseDest("dest-1", &config.DBDestMapConfig{
		Driver:    "postgres",
		DSN:       "host=localhost",
		Statement: "INSERT INTO t VALUES ($1)",
		MaxConns:  10,
	}, testLogger())

	if dest == nil {
		t.Fatal("expected non-nil")
	}
	if dest.name != "dest-1" {
		t.Fatalf("expected name dest-1, got %q", dest.name)
	}
	if dest.Type() != "database" {
		t.Fatalf("expected type database, got %q", dest.Type())
	}
}

func TestDatabaseDest_StopWithoutDB(t *testing.T) {
	dest := NewDatabaseDest("t", &config.DBDestMapConfig{Driver: "postgres"}, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// ===================================================================
// DatabaseSource — driverName for all aliases
// ===================================================================

func TestDatabaseSource_DriverName_AllAliases(t *testing.T) {
	cases := []struct {
		driver string
		want   string
	}{
		{"postgres", "pgx"},
		{"postgresql", "pgx"},
		{"POSTGRES", "pgx"},
		{"mysql", "mysql"},
		{"MYSQL", "mysql"},
		{"mssql", "sqlserver"},
		{"MSSQL", "sqlserver"},
		{"sqlserver", "sqlserver"},
		{"SQLSERVER", "sqlserver"},
		{"sqlite", "sqlite"},
		{"sqlite3", "sqlite"},
		{"SQLITE", "sqlite"},
		{"SQLITE3", "sqlite"},
		{"cockroachdb", "cockroachdb"},
		{"", ""},
	}

	for _, tc := range cases {
		src := NewDatabaseSource(&config.DBListener{Driver: tc.driver}, testLogger())
		got := src.driverName()
		if got != tc.want {
			t.Errorf("driverName(%q) = %q, want %q", tc.driver, got, tc.want)
		}
	}
}

func TestDatabaseSource_Constructor(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{
		Driver:       "mysql",
		DSN:          "user:pass@/db",
		PollInterval: "5s",
		Query:        "SELECT * FROM queue WHERE processed = 0",
	}, testLogger())

	if src == nil {
		t.Fatal("expected non-nil")
	}
	if src.Type() != "database" {
		t.Fatalf("expected type database, got %q", src.Type())
	}
}

func TestDatabaseSource_Start_InvalidDSN(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{
		Driver:       "postgres",
		DSN:          "invalid-dsn-that-will-fail",
		PollInterval: "50ms",
		Query:        "SELECT 1",
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start should not fail (polling is async): %v", err)
	}

	// Give it a tick to attempt a poll (which will fail due to invalid DSN)
	time.Sleep(100 * time.Millisecond)

	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestDatabaseSource_StopBeforeStart_Safe(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{
		Driver: "postgres",
		DSN:    "x",
	}, testLogger())

	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop before start: %v", err)
	}
}

// ===================================================================
// FHIRPollSource — Start with PollRange/Since config
// ===================================================================

func TestFHIRPollSource_Start_WithSinceAlias(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:      ts.URL,
		Resources:    []string{"Patient"},
		Since:        "30m",
		PollInterval: "100ms",
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestFHIRPollSource_Start_WithPollRange(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:      ts.URL,
		Resources:    []string{"Patient"},
		PollRange:    "2h",
		DateParam:    "onset-date",
		PollInterval: "100ms",
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestFHIRPollSource_Start_WithSearchQueries(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:       ts.URL,
		SearchQueries: []string{"Patient?name=Smith"},
		PollInterval:  "100ms",
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// ===================================================================
// FHIRPollSource — auth variants
// ===================================================================

func TestFHIRPollSource_ApplyAuth_APIKey_Header(t *testing.T) {
	var headerVal string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerVal = r.Header.Get("X-FHIR-Key")
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
		Auth: &config.AuthConfig{
			Type:   "api_key",
			Header: "X-FHIR-Key",
			Key:    "fhir-api-key-123",
		},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "", "")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if headerVal != "fhir-api-key-123" {
		t.Fatalf("expected X-FHIR-Key header, got %q", headerVal)
	}
}

func TestFHIRPollSource_ApplyAuth_APIKey_QueryParam(t *testing.T) {
	var gotKey string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.URL.Query().Get("api_key")
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
		Auth: &config.AuthConfig{
			Type:       "api_key",
			QueryParam: "api_key",
			Key:        "key-xyz",
		},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "", "")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if gotKey != "key-xyz" {
		t.Fatalf("expected api_key=key-xyz in query, got %q", gotKey)
	}
}

func TestFHIRPollSource_ApplyAuth_NoAuth(t *testing.T) {
	var authHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "Bundle", "entry": []any{}})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
	}, testLogger())

	err := src.poll(context.Background(), noopHandler, "", "")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if authHeader != "" {
		t.Fatalf("expected no auth header, got %q", authHeader)
	}
}
