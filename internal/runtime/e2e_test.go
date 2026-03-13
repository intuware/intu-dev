package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

func TestMain(m *testing.M) {
	code := m.Run()
	connector.ResetSharedHTTPListeners()
	os.Exit(code)
}

func e2eLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func writeJS(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write JS %s: %v", name, err)
	}
}

type destCapture struct {
	mu       sync.Mutex
	messages []*message.Message
	bodies   [][]byte
	headers  []http.Header
}

func (d *destCapture) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		d.mu.Lock()
		d.bodies = append(d.bodies, body)
		d.headers = append(d.headers, r.Header.Clone())
		d.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
}

func (d *destCapture) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.bodies)
}

func (d *destCapture) body(i int) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i < len(d.bodies) {
		return d.bodies[i]
	}
	return nil
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func buildChannelRuntime(
	t *testing.T,
	id string,
	chCfg *config.ChannelConfig,
	source connector.SourceConnector,
	destinations map[string]connector.DestinationConnector,
	channelDir string,
) *ChannelRuntime {
	t.Helper()
	logger := e2eLogger()
	runner, err := NewNodeRunner(2, logger)
	if err != nil {
		t.Fatalf("init node runner: %v", err)
	}
	t.Cleanup(func() { runner.Close() })
	pipeline := NewPipeline(channelDir, channelDir, id, chCfg, runner, logger)

	return &ChannelRuntime{
		ID:           id,
		Config:       chCfg,
		Source:       source,
		Destinations: destinations,
		DestConfigs:  chCfg.Destinations,
		Pipeline:     pipeline,
		Logger:       logger,
	}
}

// ===================================================================
// Test 1: HTTP Source -> Validator -> Transformer -> HTTP Destination
// ===================================================================

func TestE2E_HTTPSourceToHTTPDest(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	return msg.body !== null && msg.body !== undefined;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { transformed: true, original: msg.body, channelId: ctx.channelId } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-http-to-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Validator:   "validator.js",
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest-http"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest-http", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest-http": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("hello world"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	if err := json.Unmarshal(capture.body(0), &result); err != nil {
		t.Fatalf("unmarshal destination body: %v", err)
	}
	if result["transformed"] != true {
		t.Fatalf("expected transformed=true, got %v", result["transformed"])
	}
	if result["channelId"] != "e2e-http-to-http" {
		t.Fatalf("expected channelId, got %v", result["channelId"])
	}
}

// ===================================================================
// Test 2: File Source -> Validator -> Transformer -> HTTP Dest
// (Simulates SFTP -> HTTP)
// ===================================================================

func TestE2E_FileSourceToHTTPDest_SFTPToHTTP(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	inputDir := filepath.Join(t.TempDir(), "input")
	processedDir := filepath.Join(t.TempDir(), "processed")
	os.MkdirAll(inputDir, 0o755)
	os.MkdirAll(processedDir, 0o755)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	return typeof msg.body === "string" && msg.body.length > 0;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { source: "sftp", payload: msg.body, channelId: ctx.channelId } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-sftp-to-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Validator:   "validator.js",
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "file",
			File: &config.FileListener{
				Directory:    inputDir,
				FilePattern:  "*.dat",
				PollInterval: "100ms",
				MoveTo:       processedDir,
				SortBy:       "name",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "http-dest"},
		},
	}

	fileSrc := connector.NewFileSource(chCfg.Listener.File, e2eLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, fileSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
	}, channelDir)

	os.WriteFile(filepath.Join(inputDir, "message1.dat"), []byte("SFTP file content A"), 0o644)
	os.WriteFile(filepath.Join(inputDir, "message2.dat"), []byte("SFTP file content B"), 0o644)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 2 })

	for i := 0; i < 2; i++ {
		var result map[string]any
		if err := json.Unmarshal(capture.body(i), &result); err != nil {
			t.Fatalf("unmarshal body %d: %v", i, err)
		}
		if result["source"] != "sftp" {
			t.Fatalf("expected source=sftp, got %v", result["source"])
		}
	}

	processedEntries, _ := os.ReadDir(processedDir)
	if len(processedEntries) != 2 {
		t.Fatalf("expected 2 processed files, got %d", len(processedEntries))
	}
}

// ===================================================================
// Test 3: HTTP Source -> Transformer -> File Dest
// (Simulates HTTP -> SFTP)
// ===================================================================

func TestE2E_HTTPSourceToFileDest_HTTPToSFTP(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "output")
	os.MkdirAll(outputDir, 0o755)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { destination: "sftp", data: msg.body, messageId: ctx.messageId } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-http-to-sftp",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "sftp-dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	fileDest := connector.NewFileDest("sftp-dest", &config.FileDestMapConfig{
		Directory:       outputDir,
		FilenamePattern: "{{channelId}}_{{messageId}}.json",
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"sftp-dest": fileDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	for i := 0; i < 3; i++ {
		resp, err := http.Post("http://"+httpSrc.Addr()+"/", "text/plain",
			strings.NewReader(fmt.Sprintf("HTTP payload %d", i)))
		if err != nil {
			t.Fatalf("POST %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST %d: expected 200, got %d", i, resp.StatusCode)
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		entries, _ := os.ReadDir(outputDir)
		return len(entries) >= 3
	})

	entries, _ := os.ReadDir(outputDir)
	if len(entries) < 3 {
		t.Fatalf("expected 3 output files, got %d", len(entries))
	}

	data, _ := os.ReadFile(filepath.Join(outputDir, entries[0].Name()))
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal output file: %v", err)
	}
	if result["destination"] != "sftp" {
		t.Fatalf("expected destination=sftp, got %v", result["destination"])
	}
}

// ===================================================================
// Test 4: HL7 via SFTP -> FHIR to HTTPS with OAuth2
// (File Source + HL7v2 parser + Validator + Transformer -> FHIR HTTP Dest with OAuth2)
// ===================================================================

func TestE2E_HL7viaSFTP_ToFHIR_HTTPS_OAuth2(t *testing.T) {
	var destMu sync.Mutex
	var destBodies [][]byte
	var destAuthHeaders []string

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.Form.Get("grant_type") != "client_credentials" {
			w.WriteHeader(400)
			return
		}
		if r.Form.Get("client_id") != "fhir-client-id" || r.Form.Get("client_secret") != "fhir-client-secret" {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "oauth2-fhir-token-xyz",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	defer tokenServer.Close()

	fhirServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		destMu.Lock()
		destBodies = append(destBodies, body)
		destAuthHeaders = append(destAuthHeaders, r.Header.Get("Authorization"))
		destMu.Unlock()

		w.Header().Set("Content-Type", "application/fhir+json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"resourceType":"Bundle","id":"resp-123"}`))
	}))
	defer fhirServer.Close()

	// Clear OAuth2 cache for this test
	connector.ClearOAuth2Cache()

	inputDir := filepath.Join(t.TempDir(), "hl7-input")
	processedDir := filepath.Join(t.TempDir(), "hl7-processed")
	os.MkdirAll(inputDir, 0o755)
	os.MkdirAll(processedDir, 0o755)

	channelDir := t.TempDir()

	writeJS(t, channelDir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	var b = msg.body;
	if (!b || !b.MSH) return false;
	var msgType = b.MSH["8"];
	if (!msgType) return false;
	return true;
};`)

	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var pid = msg.body.PID || {};
	var nameField = pid["5"] || {};
	var family = nameField["1"] || "Unknown";
	var given = nameField["2"] || "Unknown";
	var mrn = pid["3"] || "000";

	return {
		body: {
			resourceType: "Bundle",
			type: "transaction",
			entry: [{
				resource: {
					resourceType: "Patient",
					identifier: [{ system: "urn:oid:2.16.840.1.113883.2.1", value: mrn }],
					name: [{ family: family, given: [given] }],
					active: true
				},
				request: {
					method: "POST",
					url: "Patient"
				}
			}]
		},
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-hl7-sftp-to-fhir",
		Enabled: true,
		DataTypes: &config.DataTypesConfig{
			Inbound:  "hl7v2",
			Outbound: "fhir_r4",
		},
		Pipeline: &config.PipelineConfig{
			Validator:   "validator.js",
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "file",
			File: &config.FileListener{
				Directory:    inputDir,
				FilePattern:  "*.hl7",
				PollInterval: "100ms",
				MoveTo:       processedDir,
				SortBy:       "name",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "fhir-dest"},
		},
	}

	fileSrc := connector.NewFileSource(chCfg.Listener.File, e2eLogger())
	httpDest := connector.NewHTTPDest("fhir-dest", &config.HTTPDestConfig{
		URL:    fhirServer.URL + "/fhir/",
		Method: "POST",
		Headers: map[string]string{
			"Content-Type": "application/fhir+json",
		},
		Auth: &config.HTTPAuthConfig{
			Type:         "oauth2_client_credentials",
			TokenURL:     tokenServer.URL,
			ClientID:     "fhir-client-id",
			ClientSecret: "fhir-client-secret",
			Scopes:       []string{"system/*.write"},
		},
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, fileSrc, map[string]connector.DestinationConnector{
		"fhir-dest": httpDest,
	}, channelDir)

	hl7Message := "MSH|^~\\&|LABSYS|HOSPITAL|FHIRSYS|CLOUD|20230615120000||ADT^A01|MSG001|P|2.5\r" +
		"PID|1||MRN12345||Smith^Jane^M||19850301|F|||123 Main St^^Springfield^IL^62704\r" +
		"PV1|1|I|ICU^101^A|E|||1234^Jones^Robert^^^Dr||||||||||||||V001\r"

	hl7Message2 := "MSH|^~\\&|LABSYS|HOSPITAL|FHIRSYS|CLOUD|20230615120100||ADT^A01|MSG002|P|2.5\r" +
		"PID|1||MRN67890||Doe^John^Q||19900715|M|||456 Oak Ave^^Chicago^IL^60601\r" +
		"PV1|1|O|ER^201^B|U|||5678^Brown^Alice^^^Dr||||||||||||||V002\r"

	os.WriteFile(filepath.Join(inputDir, "adt_001.hl7"), []byte(hl7Message), 0o644)
	os.WriteFile(filepath.Join(inputDir, "adt_002.hl7"), []byte(hl7Message2), 0o644)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	waitFor(t, 5*time.Second, func() bool {
		destMu.Lock()
		defer destMu.Unlock()
		return len(destBodies) >= 2
	})

	destMu.Lock()
	defer destMu.Unlock()

	if len(destBodies) < 2 {
		t.Fatalf("expected 2 FHIR bundles, got %d", len(destBodies))
	}

	for i, body := range destBodies {
		var bundle map[string]any
		if err := json.Unmarshal(body, &bundle); err != nil {
			t.Fatalf("unmarshal FHIR bundle %d: %v", i, err)
		}
		if bundle["resourceType"] != "Bundle" {
			t.Fatalf("bundle %d: expected resourceType=Bundle, got %v", i, bundle["resourceType"])
		}
		if bundle["type"] != "transaction" {
			t.Fatalf("bundle %d: expected type=transaction, got %v", i, bundle["type"])
		}
		entries, ok := bundle["entry"].([]any)
		if !ok || len(entries) == 0 {
			t.Fatalf("bundle %d: expected at least 1 entry", i)
		}
		entry := entries[0].(map[string]any)
		resource := entry["resource"].(map[string]any)
		if resource["resourceType"] != "Patient" {
			t.Fatalf("bundle %d: expected Patient resource, got %v", i, resource["resourceType"])
		}
	}

	for i, auth := range destAuthHeaders {
		if auth != "Bearer oauth2-fhir-token-xyz" {
			t.Fatalf("message %d: expected OAuth2 bearer token, got %q", i, auth)
		}
	}

	var bundle0 map[string]any
	json.Unmarshal(destBodies[0], &bundle0)
	entry0 := bundle0["entry"].([]any)[0].(map[string]any)
	resource0 := entry0["resource"].(map[string]any)
	names0 := resource0["name"].([]any)
	name0 := names0[0].(map[string]any)
	if name0["family"] != "Smith" {
		t.Fatalf("expected family=Smith, got %v", name0["family"])
	}

	processedEntries, _ := os.ReadDir(processedDir)
	if len(processedEntries) != 2 {
		t.Fatalf("expected 2 processed HL7 files, got %d", len(processedEntries))
	}
}

// ===================================================================
// Test 5: Validator Rejects Invalid Messages
// ===================================================================

func TestE2E_ValidatorRejects(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	if (typeof msg.body !== "string") return false;
	return msg.body.indexOf("GOOD") >= 0;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { validated: true, content: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-validator-reject",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Validator:   "validator.js",
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	addr := httpSrc.Addr()

	resp, _ := http.Post("http://"+addr+"/", "text/plain", strings.NewReader("BAD message"))
	resp.Body.Close()

	resp, _ = http.Post("http://"+addr+"/", "text/plain", strings.NewReader("GOOD message here"))
	resp.Body.Close()

	resp, _ = http.Post("http://"+addr+"/", "text/plain", strings.NewReader("another BAD one"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })
	time.Sleep(200 * time.Millisecond)

	if capture.count() != 1 {
		t.Fatalf("expected exactly 1 message to pass validator, got %d", capture.count())
	}

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["validated"] != true {
		t.Fatalf("expected validated=true")
	}
	content, ok := result["content"].(string)
	if !ok || !strings.Contains(content, "GOOD") {
		t.Fatalf("expected content with GOOD, got %v", result["content"])
	}
}

// ===================================================================
// Test 6: Source Filter Drops Messages
// ===================================================================

func TestE2E_SourceFilter(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "source-filter.js", `
exports.filter = function filter(msg, ctx) {
	if (typeof msg.body !== "string") return true;
	return msg.body.indexOf("ACCEPT") >= 0;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { filtered: false, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-source-filter",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			SourceFilter: "source-filter.js",
			Transformer:  "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	addr := httpSrc.Addr()
	http.Post("http://"+addr+"/", "text/plain", strings.NewReader("DROP this"))
	http.Post("http://"+addr+"/", "text/plain", strings.NewReader("ACCEPT this"))
	http.Post("http://"+addr+"/", "text/plain", strings.NewReader("DROP this too"))

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })
	time.Sleep(200 * time.Millisecond)

	if capture.count() != 1 {
		t.Fatalf("expected 1 accepted message, got %d", capture.count())
	}
}

// ===================================================================
// Test 7: Multi-Destination Routing
// ===================================================================

func TestE2E_MultiDestinationRouting(t *testing.T) {
	captureA := &destCapture{}
	captureB := &destCapture{}
	serverA := httptest.NewServer(captureA.handler())
	defer serverA.Close()
	serverB := httptest.NewServer(captureB.handler())
	defer serverB.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { routed: true, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-multi-dest",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest-a"},
			{Name: "dest-b"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	destA := connector.NewHTTPDest("dest-a", &config.HTTPDestConfig{URL: serverA.URL}, e2eLogger())
	destB := connector.NewHTTPDest("dest-b", &config.HTTPDestConfig{URL: serverB.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest-a": destA,
		"dest-b": destB,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("multi-dest payload"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return captureA.count() >= 1 && captureB.count() >= 1 })

	if captureA.count() != 1 {
		t.Fatalf("dest-a: expected 1 message, got %d", captureA.count())
	}
	if captureB.count() != 1 {
		t.Fatalf("dest-b: expected 1 message, got %d", captureB.count())
	}

	var resultA, resultB map[string]any
	json.Unmarshal(captureA.body(0), &resultA)
	json.Unmarshal(captureB.body(0), &resultB)
	if resultA["routed"] != true || resultB["routed"] != true {
		t.Fatal("expected routed=true on both destinations")
	}
}

// ===================================================================
// Test 8: Destination Filter
// ===================================================================

func TestE2E_DestinationFilter(t *testing.T) {
	captureAllow := &destCapture{}
	captureBlock := &destCapture{}
	serverAllow := httptest.NewServer(captureAllow.handler())
	defer serverAllow.Close()
	serverBlock := httptest.NewServer(captureBlock.handler())
	defer serverBlock.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { data: msg.body } };
};`)
	writeJS(t, channelDir, "dest-filter-block.js", `
exports.filter = function filter(msg, ctx) {
	return false;
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-dest-filter",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "allow-dest"},
			{Name: "block-dest", Filter: "dest-filter-block.js"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	allowDest := connector.NewHTTPDest("allow-dest", &config.HTTPDestConfig{URL: serverAllow.URL}, e2eLogger())
	blockDest := connector.NewHTTPDest("block-dest", &config.HTTPDestConfig{URL: serverBlock.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"allow-dest": allowDest,
		"block-dest": blockDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("test msg"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return captureAllow.count() >= 1 })
	time.Sleep(200 * time.Millisecond)

	if captureAllow.count() != 1 {
		t.Fatalf("allow-dest: expected 1, got %d", captureAllow.count())
	}
	if captureBlock.count() != 0 {
		t.Fatalf("block-dest: expected 0, got %d", captureBlock.count())
	}
}

// ===================================================================
// Test 9: Destination Transformer
// ===================================================================

func TestE2E_DestinationTransformer(t *testing.T) {
	captureRaw := &destCapture{}
	captureXform := &destCapture{}
	serverRaw := httptest.NewServer(captureRaw.handler())
	defer serverRaw.Close()
	serverXform := httptest.NewServer(captureXform.handler())
	defer serverXform.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { source_transformed: true, data: msg.body } };
};`)
	writeJS(t, channelDir, "dest-transform.js", `
exports.transform = function transform(msg, ctx) {
	var result = msg.body;
	result.dest_transformed = true;
	result.destination = ctx.destinationName;
	return { body: result };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-dest-transformer",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "raw-dest"},
			{Name: "xform-dest", Transformer: &config.ScriptRef{Entrypoint: "dest-transform.js"}},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	rawDest := connector.NewHTTPDest("raw-dest", &config.HTTPDestConfig{URL: serverRaw.URL}, e2eLogger())
	xformDest := connector.NewHTTPDest("xform-dest", &config.HTTPDestConfig{URL: serverXform.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"raw-dest":   rawDest,
		"xform-dest": xformDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("test data"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool {
		return captureRaw.count() >= 1 && captureXform.count() >= 1
	})

	var rawResult map[string]any
	json.Unmarshal(captureRaw.body(0), &rawResult)
	if rawResult["source_transformed"] != true {
		t.Fatal("raw-dest: expected source_transformed=true")
	}
	if _, exists := rawResult["dest_transformed"]; exists {
		t.Fatal("raw-dest: should NOT have dest_transformed")
	}

	var xformResult map[string]any
	json.Unmarshal(captureXform.body(0), &xformResult)
	if xformResult["source_transformed"] != true {
		t.Fatal("xform-dest: expected source_transformed=true")
	}
	if xformResult["dest_transformed"] != true {
		t.Fatal("xform-dest: expected dest_transformed=true")
	}
	if xformResult["destination"] != "xform-dest" {
		t.Fatalf("xform-dest: expected destination name, got %v", xformResult["destination"])
	}
}

// ===================================================================
// Test 10: Preprocessor modifies raw bytes
// ===================================================================

func TestE2E_Preprocessor(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "preprocessor.js", `
exports.preprocess = function preprocess(raw) {
	var s = typeof raw === "string" ? raw : String(raw);
	return "PREPROCESSED:" + s;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { preprocessed: typeof msg.body === "string" && msg.body.indexOf("PREPROCESSED:") === 0, content: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-preprocessor",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Preprocessor: "preprocessor.js",
			Transformer:  "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("original data"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["preprocessed"] != true {
		t.Fatalf("expected preprocessed=true, got %v", result["preprocessed"])
	}
}

// ===================================================================
// Test 11: Full Pipeline - All Stages Active
// ===================================================================

func TestE2E_FullPipelineAllStages(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()

	writeJS(t, channelDir, "preprocessor.js", `
exports.preprocess = function preprocess(raw) {
	var s = typeof raw === "string" ? raw : String(raw);
	return "PRE:" + s;
};`)
	writeJS(t, channelDir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	return typeof msg.body === "string" && msg.body.indexOf("PRE:") === 0;
};`)
	writeJS(t, channelDir, "source-filter.js", `
exports.filter = function filter(msg, ctx) {
	return typeof msg.body === "string" && msg.body.indexOf("KEEP") >= 0;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { stage: "source_transformed", content: msg.body } };
};`)
	writeJS(t, channelDir, "dest-transform.js", `
exports.transform = function transform(msg, ctx) {
	var result = msg.body;
	result.stage = "dest_transformed";
	return { body: result };
};`)
	writeJS(t, channelDir, "postprocessor.js", `
exports.postprocess = function postprocess(msg, results, ctx) {
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-full-pipeline",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Preprocessor:  "preprocessor.js",
			Validator:     "validator.js",
			SourceFilter:  "source-filter.js",
			Transformer:   "transformer.js",
			Postprocessor: "postprocessor.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest", Transformer: &config.ScriptRef{Entrypoint: "dest-transform.js"}},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	addr := httpSrc.Addr()

	http.Post("http://"+addr+"/", "text/plain", strings.NewReader("DROP this"))
	http.Post("http://"+addr+"/", "text/plain", strings.NewReader("KEEP this"))
	http.Post("http://"+addr+"/", "text/plain", strings.NewReader("DROP too"))

	waitFor(t, 5*time.Second, func() bool { return capture.count() >= 1 })
	time.Sleep(300 * time.Millisecond)

	if capture.count() != 1 {
		t.Fatalf("expected 1 message through full pipeline, got %d", capture.count())
	}

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["stage"] != "dest_transformed" {
		t.Fatalf("expected stage=dest_transformed, got %v", result["stage"])
	}
	content, _ := result["content"].(string)
	if !strings.HasPrefix(content, "PRE:") {
		t.Fatalf("expected content to start with PRE:, got %q", content)
	}
	if !strings.Contains(content, "KEEP") {
		t.Fatalf("expected content to contain KEEP, got %q", content)
	}
}

// ===================================================================
// Test 12: HL7v2 Data Type Parsing Through Pipeline
// ===================================================================

func TestE2E_HL7v2DataTypeParsing(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var msh = msg.body.MSH || {};
	var pid = msg.body.PID || {};
	return {
		body: {
			messageType: msh["8"],
			controlId: msh["9"],
			patientMRN: pid["3"],
			patientName: pid["5"],
			dataType: ctx.inboundDataType
		},
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-hl7v2-parse",
		Enabled: true,
		DataTypes: &config.DataTypesConfig{
			Inbound:  "hl7v2",
			Outbound: "json",
		},
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	hl7 := "MSH|^~\\&|SEND|FAC|RECV|FAC|20230101||ADT^A01|CTRL001|P|2.5\rPID|1||MRN999||Johnson^Michael\r"
	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader(hl7))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)

	if result["controlId"] != "CTRL001" {
		t.Fatalf("expected controlId=CTRL001, got %v", result["controlId"])
	}
	if result["patientMRN"] != "MRN999" {
		t.Fatalf("expected patientMRN=MRN999, got %v", result["patientMRN"])
	}
	if result["dataType"] != "hl7v2" {
		t.Fatalf("expected dataType=hl7v2, got %v", result["dataType"])
	}

	nameMap, ok := result["patientName"].(map[string]any)
	if !ok {
		t.Fatalf("expected patientName to be a map, got %T", result["patientName"])
	}
	if nameMap["1"] != "Johnson" {
		t.Fatalf("expected family=Johnson, got %v", nameMap["1"])
	}
	if nameMap["2"] != "Michael" {
		t.Fatalf("expected given=Michael, got %v", nameMap["2"])
	}
}

// ===================================================================
// Test 13: JSON Data Type Through Pipeline
// ===================================================================

func TestE2E_JSONDataTypePipeline(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	return msg.body && msg.body.patientId !== undefined;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return {
		body: {
			resourceType: "Patient",
			id: msg.body.patientId,
			name: [{ family: msg.body.lastName, given: [msg.body.firstName] }],
			birthDate: msg.body.dob
		},
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-json-pipeline",
		Enabled: true,
		DataTypes: &config.DataTypesConfig{
			Inbound:  "json",
			Outbound: "json",
		},
		Pipeline: &config.PipelineConfig{
			Validator:   "validator.js",
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	payload := `{"patientId":"P001","firstName":"Alice","lastName":"Brown","dob":"1990-05-15"}`
	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "application/json", strings.NewReader(payload))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["resourceType"] != "Patient" {
		t.Fatalf("expected resourceType=Patient, got %v", result["resourceType"])
	}
	if result["id"] != "P001" {
		t.Fatalf("expected id=P001, got %v", result["id"])
	}
	if result["birthDate"] != "1990-05-15" {
		t.Fatalf("expected birthDate, got %v", result["birthDate"])
	}
}

// ===================================================================
// Test 14: Legacy Validator Config (ScriptRef)
// ===================================================================

func TestE2E_LegacyValidatorConfig(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	return typeof msg.body === "string" && msg.body.length > 5;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { legacy_validated: true, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-legacy-validator",
		Enabled: true,
		Validator: &config.ScriptRef{
			Entrypoint: "validator.js",
		},
		Transformer: &config.ScriptRef{
			Entrypoint: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	addr := httpSrc.Addr()

	http.Post("http://"+addr+"/", "text/plain", strings.NewReader("short"))
	http.Post("http://"+addr+"/", "text/plain", strings.NewReader("long enough message"))

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })
	time.Sleep(200 * time.Millisecond)

	if capture.count() != 1 {
		t.Fatalf("expected 1 message (short rejected by validator), got %d", capture.count())
	}

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["legacy_validated"] != true {
		t.Fatal("expected legacy_validated=true")
	}
}

// ===================================================================
// Test 15: HTTP Source with Auth -> Transformer -> HTTP Dest with Auth
// ===================================================================

func TestE2E_AuthenticatedHTTPToHTTP(t *testing.T) {
	var destAuthHeader string
	capture := &destCapture{}
	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destAuthHeader = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		capture.mu.Lock()
		capture.bodies = append(capture.bodies, body)
		capture.mu.Unlock()
		w.WriteHeader(200)
	}))
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { secured: true, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-auth-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{
				Port: 0,
				Auth: &config.AuthConfig{Type: "bearer", Token: "source-secret"},
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{
		URL:  destServer.URL,
		Auth: &config.HTTPAuthConfig{Type: "bearer", Token: "dest-bearer-token"},
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	addr := httpSrc.Addr()

	resp, _ := http.Post("http://"+addr+"/", "text/plain", strings.NewReader("no auth"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", resp.StatusCode)
	}

	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("with auth"))
	req.Header.Set("Authorization", "Bearer source-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST with auth: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with auth, got %d", resp.StatusCode)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	if destAuthHeader != "Bearer dest-bearer-token" {
		t.Fatalf("expected dest bearer token, got %q", destAuthHeader)
	}
}

// ===================================================================
// Test 16: File Source + HL7 + Validator + Multiple Destinations
// (HL7 -> FHIR HTTP + File Archive)
// ===================================================================

func TestE2E_HL7ToMultiDest_FHIRAndArchive(t *testing.T) {
	fhirCapture := &destCapture{}
	fhirServer := httptest.NewServer(fhirCapture.handler())
	defer fhirServer.Close()

	archiveDir := filepath.Join(t.TempDir(), "archive")
	os.MkdirAll(archiveDir, 0o755)

	inputDir := filepath.Join(t.TempDir(), "input")
	os.MkdirAll(inputDir, 0o755)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	return msg.body && msg.body.MSH && msg.body.PID;
};`)
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var pid = msg.body.PID || {};
	var name = pid["5"] || {};
	return {
		body: {
			resourceType: "Bundle",
			type: "transaction",
			entry: [{
				resource: {
					resourceType: "Patient",
					identifier: [{ value: pid["3"] || "unknown" }],
					name: [{ family: name["1"] || "Unknown", given: [name["2"] || "Unknown"] }]
				},
				request: { method: "POST", url: "Patient" }
			}]
		},
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-hl7-multi-dest",
		Enabled: true,
		DataTypes: &config.DataTypesConfig{
			Inbound:  "hl7v2",
			Outbound: "fhir_r4",
		},
		Pipeline: &config.PipelineConfig{
			Validator:   "validator.js",
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "file",
			File: &config.FileListener{
				Directory:    inputDir,
				FilePattern:  "*.hl7",
				PollInterval: "100ms",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "fhir-server"},
			{Name: "archive"},
		},
	}

	fileSrc := connector.NewFileSource(chCfg.Listener.File, e2eLogger())
	fhirDest := connector.NewHTTPDest("fhir-server", &config.HTTPDestConfig{
		URL:     fhirServer.URL,
		Headers: map[string]string{"Content-Type": "application/fhir+json"},
	}, e2eLogger())
	archiveDest := connector.NewFileDest("archive", &config.FileDestMapConfig{
		Directory:       archiveDir,
		FilenamePattern: "{{channelId}}_{{timestamp}}.json",
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, fileSrc, map[string]connector.DestinationConnector{
		"fhir-server": fhirDest,
		"archive":     archiveDest,
	}, channelDir)

	hl7 := "MSH|^~\\&|LAB|HOSP|EHR|CLOUD|20230101||ADT^A01|M001|P|2.5\rPID|1||MRN555||Williams^Sarah\r"
	os.WriteFile(filepath.Join(inputDir, "patient.hl7"), []byte(hl7), 0o644)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	waitFor(t, 3*time.Second, func() bool { return fhirCapture.count() >= 1 })

	var bundle map[string]any
	json.Unmarshal(fhirCapture.body(0), &bundle)
	if bundle["resourceType"] != "Bundle" {
		t.Fatalf("expected Bundle, got %v", bundle["resourceType"])
	}
	entries := bundle["entry"].([]any)
	resource := entries[0].(map[string]any)["resource"].(map[string]any)
	names := resource["name"].([]any)
	name := names[0].(map[string]any)
	if name["family"] != "Williams" {
		t.Fatalf("expected Williams, got %v", name["family"])
	}

	waitFor(t, 2*time.Second, func() bool {
		archiveEntries, _ := os.ReadDir(archiveDir)
		return len(archiveEntries) >= 1
	})
}

// ===================================================================
// Test 17: Channel Destination (Inter-Channel)
// ===================================================================

func TestE2E_InterChannelRouting(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDirA := t.TempDir()
	channelDirB := t.TempDir()

	writeJS(t, channelDirA, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { from_channel_a: true, original: msg.body } };
};`)
	writeJS(t, channelDirB, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { from_channel_b: true, forwarded: msg.body } };
};`)

	busChannelID := fmt.Sprintf("inter-ch-bus-%d", time.Now().UnixNano())

	chCfgA := &config.ChannelConfig{
		ID:      "channel-a",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "to-channel-b"},
		},
	}

	chCfgB := &config.ChannelConfig{
		ID:      "channel-b",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type:    "channel",
			Channel: &config.ChannelListener{SourceChannelID: busChannelID},
		},
		Destinations: []config.ChannelDestination{
			{Name: "final-dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfgA.Listener.HTTP, e2eLogger())
	channelDest := connector.NewChannelDest("to-channel-b", busChannelID, e2eLogger())
	channelSrc := connector.NewChannelSource(chCfgB.Listener.Channel, e2eLogger())
	httpDest := connector.NewHTTPDest("final-dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	crA := buildChannelRuntime(t, chCfgA.ID, chCfgA, httpSrc, map[string]connector.DestinationConnector{
		"to-channel-b": channelDest,
	}, channelDirA)

	crB := buildChannelRuntime(t, chCfgB.ID, chCfgB, channelSrc, map[string]connector.DestinationConnector{
		"final-dest": httpDest,
	}, channelDirB)

	ctx := context.Background()
	if err := crB.Start(ctx); err != nil {
		t.Fatalf("start channel B: %v", err)
	}
	defer crB.Stop(ctx)

	if err := crA.Start(ctx); err != nil {
		t.Fatalf("start channel A: %v", err)
	}
	defer crA.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("inter-channel data"))
	resp.Body.Close()

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["from_channel_b"] != true {
		t.Fatalf("expected from_channel_b=true, got %v", result)
	}
}

// ===================================================================
// Test 18: TCP/MLLP Source -> Transformer -> HTTP Dest
// ===================================================================

func TestE2E_TCPMLLPSourceToHTTPDest(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { source_type: msg.transport, hl7_data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-mllp-to-http",
		Enabled: true,
		DataTypes: &config.DataTypesConfig{
			Inbound: "hl7v2",
		},
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "tcp",
			TCP: &config.TCPListener{
				Port:      0,
				Mode:      "mllp",
				TimeoutMs: 5000,
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	tcpSrc := connector.NewTCPSource(chCfg.Listener.TCP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, tcpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	time.Sleep(100 * time.Millisecond)

	addr := tcpSrc.Addr()
	if addr == "" {
		t.Fatal("TCP source has no address")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	hl7 := "MSH|^~\\&|SRC|FAC|DST|FAC|20230101||ADT^A01|CTL999|P|2.5\rPID|1||MRN777||Taylor^Robert\r"
	var buf strings.Builder
	buf.WriteByte(0x0B)
	buf.WriteString(hl7)
	buf.WriteByte(0x1C)
	buf.WriteByte(0x0D)
	conn.Write([]byte(buf.String()))
	conn.Close()

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["source_type"] != "tcp" {
		t.Fatalf("expected source_type=tcp, got %v", result["source_type"])
	}
	hl7Data, ok := result["hl7_data"].(map[string]any)
	if !ok {
		t.Fatalf("expected hl7_data map, got %T", result["hl7_data"])
	}
	pid, ok := hl7Data["PID"].(map[string]any)
	if !ok {
		t.Fatalf("expected PID segment map")
	}
	nameField, ok := pid["5"].(map[string]any)
	if !ok {
		t.Fatalf("expected PID.5 name map")
	}
	if nameField["1"] != "Taylor" {
		t.Fatalf("expected family=Taylor, got %v", nameField["1"])
	}
}

// ===================================================================
// Test 19: Engine-level Integration Test
// ===================================================================

func TestE2E_EngineIntegration(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "src", "channels")
	channelDir := filepath.Join(channelsDir, "test-channel")
	os.MkdirAll(channelDir, 0o755)

	intuYAML := fmt.Sprintf(`runtime:
  name: e2e-test
  log_level: error
channels_dir: src/channels
destinations:
  test-dest:
    type: http
    http:
      url: %s
`, destServer.URL)
	os.WriteFile(filepath.Join(projectDir, "intu.yaml"), []byte(intuYAML), 0o644)

	channelYAML := `id: test-channel
enabled: true
listener:
  type: http
  http:
    port: 0
pipeline:
  transformer: transformer.js
destinations:
  - name: test-dest
    ref: test-dest
`
	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte(channelYAML), 0o644)

	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { engine_test: true, data: msg.body } };
};`)

	loader := config.NewLoader(projectDir)
	cfg, err := loader.Load("dev")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	factory := connector.NewFactory(e2eLogger())
	engine := NewDefaultEngine(projectDir, cfg, factory, e2eLogger())

	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("engine start: %v", err)
	}
	defer engine.Stop(ctx)

	if len(engine.channels) != 1 {
		t.Fatalf("expected 1 channel running, got %d", len(engine.channels))
	}

	cr := engine.channels["test-channel"]
	httpSrc, ok := cr.Source.(*connector.HTTPSource)
	if !ok {
		t.Fatal("expected HTTP source")
	}

	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("engine test"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["engine_test"] != true {
		t.Fatalf("expected engine_test=true, got %v", result)
	}
}

// ===================================================================
// Test 20: Destination Transformer Receives Dest-Native IntuMessage
// Verifies that dest transformer sees destination HTTP config (headers,
// method) instead of source transport metadata, and has access to the
// original source message via ctx.sourceMessage.
// ===================================================================

func TestE2E_DestTransformerReceivesDestNativeIntuMessage(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { source_transformed: true, data: msg.body } };
};`)
	writeJS(t, channelDir, "dest-transform.js", `
exports.transform = function transform(msg, ctx) {
	return {
		body: {
			original_body: msg.body,
			dest_transport: msg.transport,
			dest_method: msg.http ? msg.http.method : null,
			dest_headers: msg.http ? msg.http.headers : null,
			source_transport: ctx.sourceMessage ? ctx.sourceMessage.transport : null,
			source_http_method: ctx.sourceMessage && ctx.sourceMessage.http ? ctx.sourceMessage.http.method : null,
			destination_name: ctx.destinationName,
		},
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-dest-native-msg",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{
				Name: "api-dest",
				Type: "http",
				HTTP: &config.HTTPDestConfig{
					URL:    destServer.URL,
					Method: "PUT",
					Headers: map[string]string{
						"X-Dest-Header": "dest-value",
						"Content-Type":  "application/fhir+json",
					},
				},
				Transformer: &config.ScriptRef{Entrypoint: "dest-transform.js"},
			},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("api-dest", &config.HTTPDestConfig{
		URL:    destServer.URL,
		Method: "PUT",
		Headers: map[string]string{
			"X-Dest-Header": "dest-value",
			"Content-Type":  "application/fhir+json",
		},
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"api-dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("hello"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)

	if result["dest_transport"] != "http" {
		t.Fatalf("expected dest_transport=http, got %v", result["dest_transport"])
	}
	if result["dest_method"] != "PUT" {
		t.Fatalf("expected dest_method=PUT (from dest config), got %v", result["dest_method"])
	}
	destHeaders, ok := result["dest_headers"].(map[string]any)
	if !ok {
		t.Fatalf("expected dest_headers to be a map, got %T", result["dest_headers"])
	}
	if destHeaders["X-Dest-Header"] != "dest-value" {
		t.Fatalf("expected X-Dest-Header=dest-value, got %v", destHeaders["X-Dest-Header"])
	}

	if result["source_transport"] != "http" {
		t.Fatalf("expected source_transport=http, got %v", result["source_transport"])
	}
	if result["source_http_method"] != "POST" {
		t.Fatalf("expected source_http_method=POST, got %v", result["source_http_method"])
	}
	if result["destination_name"] != "api-dest" {
		t.Fatalf("expected destination_name=api-dest, got %v", result["destination_name"])
	}
}

// ===================================================================
// Test 21: Cross-Transport Dest IntuMessage (HTTP Source -> File Dest)
// Verifies the dest transformer receives file transport metadata from
// the destination config, not the source's HTTP metadata.
// ===================================================================

func TestE2E_CrossTransportDestIntuMessage(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "output")
	os.MkdirAll(outputDir, 0o755)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { transformed: true, data: msg.body } };
};`)
	writeJS(t, channelDir, "dest-transform.js", `
exports.transform = function transform(msg, ctx) {
	return {
		body: {
			dest_transport: msg.transport,
			has_file_meta: !!msg.file,
			file_directory: msg.file ? msg.file.directory : null,
			has_http_meta: !!msg.http,
			source_transport: ctx.sourceMessage ? ctx.sourceMessage.transport : null,
			source_has_http: ctx.sourceMessage ? !!ctx.sourceMessage.http : false,
		},
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-cross-transport",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{
				Name: "file-dest",
				Type: "file",
				File: &config.FileDestConfig{
					Directory:       outputDir,
					FilenamePattern: "{{channelId}}_{{messageId}}.json",
				},
				Transformer: &config.ScriptRef{Entrypoint: "dest-transform.js"},
			},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	fileDest := connector.NewFileDest("file-dest", &config.FileDestMapConfig{
		Directory:       outputDir,
		FilenamePattern: "{{channelId}}_{{messageId}}.json",
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"file-dest": fileDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("file payload"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool {
		entries, _ := os.ReadDir(outputDir)
		return len(entries) >= 1
	})

	entries, _ := os.ReadDir(outputDir)
	if len(entries) < 1 {
		t.Fatal("expected at least 1 output file")
	}

	data, _ := os.ReadFile(filepath.Join(outputDir, entries[0].Name()))
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}

	if result["dest_transport"] != "file" {
		t.Fatalf("expected dest_transport=file, got %v", result["dest_transport"])
	}
	if result["has_file_meta"] != true {
		t.Fatalf("expected has_file_meta=true, got %v", result["has_file_meta"])
	}
	if result["file_directory"] != outputDir {
		t.Fatalf("expected file_directory=%s, got %v", outputDir, result["file_directory"])
	}
	if result["has_http_meta"] != false {
		t.Fatalf("expected has_http_meta=false (dest is file, not http), got %v", result["has_http_meta"])
	}
	if result["source_transport"] != "http" {
		t.Fatalf("expected source_transport=http (from ctx.sourceMessage), got %v", result["source_transport"])
	}
	if result["source_has_http"] != true {
		t.Fatalf("expected source_has_http=true, got %v", result["source_has_http"])
	}
}

// ===================================================================
// Test 22: SOAP Source -> Transformer -> HTTP Dest
// ===================================================================

func TestE2E_SOAPSourceToHTTPDest(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var soapAction = (msg.http && msg.http.headers) ? (msg.http.headers["Soapaction"] || msg.http.headers["SOAPAction"] || "") : "";
	return { body: { source: msg.transport, soap_action: soapAction, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-soap-to-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "soap",
			SOAP: &config.SOAPListener{Port: 0, ServiceName: "PatientService"},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	soapSrc := connector.NewSOAPSource(chCfg.Listener.SOAP, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, soapSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	time.Sleep(100 * time.Millisecond)
	addr := soapSrc.Addr()
	if addr == "" {
		t.Fatal("SOAP source has no address")
	}

	soapEnvelope := `<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body><RegisterPatient><mrn>MRN001</mrn></RegisterPatient></soap:Body>
</soap:Envelope>`

	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader(soapEnvelope))
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("SOAPAction", "RegisterPatient")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["source"] != "soap" {
		t.Fatalf("expected source=soap, got %v", result["source"])
	}
	soapAction, _ := result["soap_action"].(string)
	if soapAction != "RegisterPatient" {
		t.Fatalf("expected soap_action=RegisterPatient, got %v", result["soap_action"])
	}
}

// ===================================================================
// Test 23: FHIR Source -> Transformer -> HTTP Dest
// ===================================================================

func TestE2E_FHIRSourceToHTTPDest(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var rt = (typeof msg.body === "object" && msg.body) ? msg.body.resourceType : null;
	return { body: { source: msg.transport, resource_type: rt, patient: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-fhir-src-to-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "fhir",
			FHIR: &config.FHIRListener{Port: 0, BasePath: "/fhir", Resources: []string{"Patient"}},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	fhirSrc := connector.NewFHIRSource(chCfg.Listener.FHIR, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, fhirSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	time.Sleep(100 * time.Millisecond)
	addr := fhirSrc.Addr()
	if addr == "" {
		t.Fatal("FHIR source has no address")
	}

	patient := `{"resourceType":"Patient","id":"P-123","name":[{"family":"Smith","given":["Jane"]}]}`
	req, _ := http.NewRequest("POST", "http://"+addr+"/fhir/Patient", strings.NewReader(patient))
	req.Header.Set("Content-Type", "application/fhir+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["source"] != "fhir" {
		t.Fatalf("expected source=fhir, got %v", result["source"])
	}
	if result["resource_type"] != "Patient" {
		t.Fatalf("expected resource_type=Patient, got %v", result["resource_type"])
	}
}

// ===================================================================
// Test 24: IHE Source (XDS Repository) -> Transformer -> HTTP Dest
// ===================================================================

func TestE2E_IHESourceToHTTPDest(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { source: msg.transport, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-ihe-to-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "ihe",
			IHE:  &config.IHEListener{Profile: "xds_repository", Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	iheSrc := connector.NewIHESource(chCfg.Listener.IHE, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, iheSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	time.Sleep(100 * time.Millisecond)
	addr := iheSrc.Addr()
	if addr == "" {
		t.Fatal("IHE source has no address")
	}

	xdsDoc := `<?xml version="1.0"?><ProvideAndRegisterDocumentSet><document id="doc1"/></ProvideAndRegisterDocumentSet>`
	req, _ := http.NewRequest("POST", "http://"+addr+"/xds/repository/provide", strings.NewReader(xdsDoc))
	req.Header.Set("Content-Type", "text/xml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["source"] != "ihe" {
		t.Fatalf("expected source=ihe, got %v", result["source"])
	}
}

// ===================================================================
// Test 25: DICOM Source -> Transformer -> HTTP Dest
// ===================================================================

func TestE2E_DICOMSourceToHTTPDest(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var ae = (msg.dicom && msg.dicom.calledAE) ? msg.dicom.calledAE : "unknown";
	return { body: { source: msg.transport, ae_title: ae, data_length: typeof msg.body === "string" ? msg.body.length : 0 } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-dicom-to-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type:  "dicom",
			DICOM: &config.DICOMListener{Port: 0, AETitle: "TEST_SCP"},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	dicomSrc := connector.NewDICOMSource(chCfg.Listener.DICOM, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, dicomSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	time.Sleep(100 * time.Millisecond)
	addr := dicomSrc.Addr()
	if addr == "" {
		t.Fatal("DICOM source has no address")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	calledAE := fmt.Sprintf("%-16s", "TEST_SCP")
	callingAE := fmt.Sprintf("%-16s", "TEST_SCU")
	var assocData []byte
	assocData = append(assocData, 0x00, 0x01) // protocol version
	assocData = append(assocData, 0x00, 0x00) // reserved
	assocData = append(assocData, []byte(calledAE)...)
	assocData = append(assocData, []byte(callingAE)...)
	assocData = append(assocData, make([]byte, 32)...)

	assocPDU := make([]byte, 6+len(assocData))
	assocPDU[0] = 0x01 // A-ASSOCIATE-RQ
	assocPDU[1] = 0x00
	assocPDU[2] = byte(len(assocData) >> 24)
	assocPDU[3] = byte(len(assocData) >> 16)
	assocPDU[4] = byte(len(assocData) >> 8)
	assocPDU[5] = byte(len(assocData))
	copy(assocPDU[6:], assocData)
	conn.Write(assocPDU)

	time.Sleep(100 * time.Millisecond)

	pDataPayload := []byte("DICOM-STUDY-DATA-PAYLOAD")
	pDataPDU := make([]byte, 6+len(pDataPayload))
	pDataPDU[0] = 0x04 // P-DATA-TF
	pDataPDU[1] = 0x00
	pDataPDU[2] = byte(len(pDataPayload) >> 24)
	pDataPDU[3] = byte(len(pDataPayload) >> 16)
	pDataPDU[4] = byte(len(pDataPayload) >> 8)
	pDataPDU[5] = byte(len(pDataPayload))
	copy(pDataPDU[6:], pDataPayload)
	conn.Write(pDataPDU)

	releasePDU := make([]byte, 10)
	releasePDU[0] = 0x05 // A-RELEASE-RQ
	releasePDU[1] = 0x00
	releasePDU[2] = 0x00
	releasePDU[3] = 0x00
	releasePDU[4] = 0x00
	releasePDU[5] = 0x04
	conn.Write(releasePDU)
	conn.Close()

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["source"] != "dicom" {
		t.Fatalf("expected source=dicom, got %v", result["source"])
	}
	if result["ae_title"] != "TEST_SCP" {
		t.Fatalf("expected ae_title=TEST_SCP, got %v", result["ae_title"])
	}
}

// ===================================================================
// Test 26: HTTP Source -> TCP Destination (MLLP)
// ===================================================================

func TestE2E_HTTPSourceToTCPDest(t *testing.T) {
	var tcpMu sync.Mutex
	var tcpReceived [][]byte

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				n, _ := c.Read(buf)
				if n > 0 {
					tcpMu.Lock()
					tcpReceived = append(tcpReceived, buf[:n])
					tcpMu.Unlock()
				}
			}(conn)
		}
	}()

	tcpAddr := ln.Addr().(*net.TCPAddr)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: "TRANSFORMED:" + msg.body };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-http-to-tcp",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "tcp-dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	tcpDest := connector.NewTCPDest("tcp-dest", &config.TCPDestMapConfig{
		Host: "127.0.0.1",
		Port: tcpAddr.Port,
		Mode: "raw",
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"tcp-dest": tcpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("hello tcp"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool {
		tcpMu.Lock()
		defer tcpMu.Unlock()
		return len(tcpReceived) >= 1
	})

	tcpMu.Lock()
	defer tcpMu.Unlock()
	received := string(tcpReceived[0])
	if !strings.Contains(received, "TRANSFORMED:") {
		t.Fatalf("expected transformed content on TCP, got %q", received)
	}
}

// ===================================================================
// Test 27: HTTP Source -> FHIR Destination
// ===================================================================

func TestE2E_HTTPSourceToFHIRDest(t *testing.T) {
	var fhirMu sync.Mutex
	var fhirPaths []string
	var fhirBodies [][]byte

	fhirServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fhirMu.Lock()
		fhirPaths = append(fhirPaths, r.URL.Path)
		fhirBodies = append(fhirBodies, body)
		fhirMu.Unlock()
		w.Header().Set("Content-Type", "application/fhir+json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"resourceType":"OperationOutcome","issue":[]}`))
	}))
	defer fhirServer.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { resourceType: "Patient", id: "P-999", name: [{ family: "TestFamily" }] } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-http-to-fhir-dest",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "fhir-out"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	fhirDest := connector.NewFHIRDest("fhir-out", &config.FHIRDestMapConfig{
		BaseURL:    fhirServer.URL + "/fhir",
		Operations: []string{"create"},
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"fhir-out": fhirDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "application/json", strings.NewReader(`{"trigger":"admission"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool {
		fhirMu.Lock()
		defer fhirMu.Unlock()
		return len(fhirBodies) >= 1
	})

	fhirMu.Lock()
	defer fhirMu.Unlock()
	if !strings.Contains(fhirPaths[0], "/Patient") {
		t.Fatalf("expected FHIR path to contain /Patient, got %s", fhirPaths[0])
	}
	var patient map[string]any
	json.Unmarshal(fhirBodies[0], &patient)
	if patient["resourceType"] != "Patient" {
		t.Fatalf("expected resourceType=Patient, got %v", patient["resourceType"])
	}
}

// ===================================================================
// Test 28: HTTP Source -> Log Destination
// ===================================================================

func TestE2E_HTTPSourceToLogDest(t *testing.T) {
	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { logged: true, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-http-to-log",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "log-dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	logDest := connector.NewLogDest("log-dest", e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"log-dest": logDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("log this message"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	time.Sleep(500 * time.Millisecond)
}

// ===================================================================
// Test 29: 3-Destination Fan-Out (HTTP + File + TCP)
// ===================================================================

func TestE2E_ThreeDestFanOut(t *testing.T) {
	captureHTTP := &destCapture{}
	serverHTTP := httptest.NewServer(captureHTTP.handler())
	defer serverHTTP.Close()

	outputDir := filepath.Join(t.TempDir(), "output")
	os.MkdirAll(outputDir, 0o755)

	var tcpMu sync.Mutex
	var tcpReceived [][]byte
	tcpLn, _ := net.Listen("tcp", "127.0.0.1:0")
	defer tcpLn.Close()
	go func() {
		for {
			conn, err := tcpLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 8192)
				n, _ := c.Read(buf)
				if n > 0 {
					tcpMu.Lock()
					tcpReceived = append(tcpReceived, buf[:n])
					tcpMu.Unlock()
				}
			}(conn)
		}
	}()
	tcpAddr := tcpLn.Addr().(*net.TCPAddr)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { fanout: true, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-3-dest-fanout",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "http-dest"},
			{Name: "file-dest"},
			{Name: "tcp-dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: serverHTTP.URL}, e2eLogger())
	fileDest := connector.NewFileDest("file-dest", &config.FileDestMapConfig{
		Directory:       outputDir,
		FilenamePattern: "{{channelId}}_{{messageId}}.json",
	}, e2eLogger())
	tcpDest := connector.NewTCPDest("tcp-dest", &config.TCPDestMapConfig{
		Host: "127.0.0.1",
		Port: tcpAddr.Port,
		Mode: "raw",
	}, e2eLogger())

	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
		"file-dest": fileDest,
		"tcp-dest":  tcpDest,
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("fanout payload"))
	resp.Body.Close()

	waitFor(t, 3*time.Second, func() bool {
		tcpMu.Lock()
		tcpCount := len(tcpReceived)
		tcpMu.Unlock()
		entries, _ := os.ReadDir(outputDir)
		return captureHTTP.count() >= 1 && len(entries) >= 1 && tcpCount >= 1
	})

	if captureHTTP.count() < 1 {
		t.Fatal("HTTP dest did not receive message")
	}
	entries, _ := os.ReadDir(outputDir)
	if len(entries) < 1 {
		t.Fatal("File dest did not write file")
	}
	tcpMu.Lock()
	if len(tcpReceived) < 1 {
		t.Fatal("TCP dest did not receive message")
	}
	tcpMu.Unlock()

	var httpResult map[string]any
	json.Unmarshal(captureHTTP.body(0), &httpResult)
	if httpResult["fanout"] != true {
		t.Fatal("expected fanout=true on HTTP dest")
	}
}

// ===================================================================
// Test 30: Conditional Routing via _routeTo
// ===================================================================

func TestE2E_ConditionalRouteTo(t *testing.T) {
	captureA := &destCapture{}
	captureB := &destCapture{}
	captureC := &destCapture{}
	serverA := httptest.NewServer(captureA.handler())
	defer serverA.Close()
	serverB := httptest.NewServer(captureB.handler())
	defer serverB.Close()
	serverC := httptest.NewServer(captureC.handler())
	defer serverC.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var body = typeof msg.body === "string" ? msg.body : JSON.stringify(msg.body);
	if (body.indexOf("CRITICAL") >= 0) {
		return { body: { priority: "critical", data: body }, _routeTo: ["dest-a", "dest-b"] };
	}
	return { body: { priority: "normal", data: body }, _routeTo: ["dest-c"] };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-route-to",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest-a"},
			{Name: "dest-b"},
			{Name: "dest-c"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest-a": connector.NewHTTPDest("dest-a", &config.HTTPDestConfig{URL: serverA.URL}, e2eLogger()),
		"dest-b": connector.NewHTTPDest("dest-b", &config.HTTPDestConfig{URL: serverB.URL}, e2eLogger()),
		"dest-c": connector.NewHTTPDest("dest-c", &config.HTTPDestConfig{URL: serverC.URL}, e2eLogger()),
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	addr := httpSrc.Addr()

	r1, _ := http.Post("http://"+addr+"/", "text/plain", strings.NewReader("CRITICAL lab result"))
	r1.Body.Close()

	r2, _ := http.Post("http://"+addr+"/", "text/plain", strings.NewReader("normal update"))
	r2.Body.Close()

	waitFor(t, 3*time.Second, func() bool {
		return captureA.count() >= 1 && captureB.count() >= 1 && captureC.count() >= 1
	})

	time.Sleep(300 * time.Millisecond)

	if captureA.count() != 1 {
		t.Fatalf("dest-a: expected 1 (critical only), got %d", captureA.count())
	}
	if captureB.count() != 1 {
		t.Fatalf("dest-b: expected 1 (critical only), got %d", captureB.count())
	}
	if captureC.count() != 1 {
		t.Fatalf("dest-c: expected 1 (normal only), got %d", captureC.count())
	}

	var resultA map[string]any
	json.Unmarshal(captureA.body(0), &resultA)
	if resultA["priority"] != "critical" {
		t.Fatalf("dest-a: expected priority=critical, got %v", resultA["priority"])
	}

	var resultC map[string]any
	json.Unmarshal(captureC.body(0), &resultC)
	if resultC["priority"] != "normal" {
		t.Fatalf("dest-c: expected priority=normal, got %v", resultC["priority"])
	}
}

// ===================================================================
// Test 31: Multi-Dest with Mixed Filters and Transformers
// ===================================================================

func TestE2E_MultiDestMixedFilterAndTransform(t *testing.T) {
	captureBlocked := &destCapture{}
	captureEnriched := &destCapture{}
	serverBlocked := httptest.NewServer(captureBlocked.handler())
	defer serverBlocked.Close()
	serverEnriched := httptest.NewServer(captureEnriched.handler())
	defer serverEnriched.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { source_transformed: true, data: msg.body } };
};`)
	writeJS(t, channelDir, "block-filter.js", `
exports.filter = function filter(msg, ctx) {
	return false;
};`)
	writeJS(t, channelDir, "enrich-transform.js", `
exports.transform = function transform(msg, ctx) {
	var result = msg.body;
	result.enriched = true;
	result.destination = ctx.destinationName;
	return { body: result };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-mixed-filter-xform",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "blocked-dest", Filter: "block-filter.js"},
			{Name: "enriched-dest", Transformer: &config.ScriptRef{Entrypoint: "enrich-transform.js"}},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"blocked-dest":  connector.NewHTTPDest("blocked-dest", &config.HTTPDestConfig{URL: serverBlocked.URL}, e2eLogger()),
		"enriched-dest": connector.NewHTTPDest("enriched-dest", &config.HTTPDestConfig{URL: serverEnriched.URL}, e2eLogger()),
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("test data"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return captureEnriched.count() >= 1 })
	time.Sleep(300 * time.Millisecond)

	if captureBlocked.count() != 0 {
		t.Fatalf("blocked-dest: expected 0, got %d", captureBlocked.count())
	}
	if captureEnriched.count() != 1 {
		t.Fatalf("enriched-dest: expected 1, got %d", captureEnriched.count())
	}

	var result map[string]any
	json.Unmarshal(captureEnriched.body(0), &result)
	if result["enriched"] != true {
		t.Fatal("expected enriched=true")
	}
	if result["destination"] != "enriched-dest" {
		t.Fatalf("expected destination=enriched-dest, got %v", result["destination"])
	}
}

// ===================================================================
// Test 32: Multi-Dest Failure Isolation
// ===================================================================

func TestE2E_MultiDestFailureIsolation(t *testing.T) {
	captureHealthy := &destCapture{}
	serverHealthy := httptest.NewServer(captureHealthy.handler())
	defer serverHealthy.Close()

	serverFailing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"server error"}`))
	}))
	defer serverFailing.Close()

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { resilient: true, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "e2e-failure-isolation",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "healthy-dest"},
			{Name: "failing-dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, e2eLogger())
	cr := buildChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"healthy-dest": connector.NewHTTPDest("healthy-dest", &config.HTTPDestConfig{URL: serverHealthy.URL}, e2eLogger()),
		"failing-dest": connector.NewHTTPDest("failing-dest", &config.HTTPDestConfig{URL: serverFailing.URL}, e2eLogger()),
	}, channelDir)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("test payload"))
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return captureHealthy.count() >= 1 })

	if captureHealthy.count() != 1 {
		t.Fatalf("healthy-dest: expected 1, got %d", captureHealthy.count())
	}

	var result map[string]any
	json.Unmarshal(captureHealthy.body(0), &result)
	if result["resilient"] != true {
		t.Fatal("expected resilient=true on healthy dest")
	}
}
