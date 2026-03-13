package runtime

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
)

func buildChannelRuntimeWithStore(
	t *testing.T,
	id string,
	chCfg *config.ChannelConfig,
	source connector.SourceConnector,
	destinations map[string]connector.DestinationConnector,
	channelDir string,
	store storage.MessageStore,
) *ChannelRuntime {
	t.Helper()
	cr := buildChannelRuntime(t, id, chCfg, source, destinations, channelDir)
	cr.Store = store
	return cr
}

// ===================================================================
// Test 33: Replay on HTTP Source
// ===================================================================

func TestReplay_HTTPSource(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	store := storage.NewMemoryStore(0, 0)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { transformed: true, data: msg.body, reprocessed: msg.body && typeof msg.body === "object" ? !!msg.body.reprocessed : false } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "replay-http",
		Enabled: true,
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

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir, store)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("original payload"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["transformed"] != true {
		t.Fatal("expected transformed=true on first send")
	}

	records, err := store.Query(storage.QueryOpts{ChannelID: "replay-http", Stage: "received"})
	if err != nil {
		t.Fatalf("query store: %v", err)
	}
	if len(records) < 1 {
		t.Fatal("expected at least 1 received record in store")
	}

	record := records[0]
	replayMsg := message.New("replay-http", record.Content)
	replayMsg.Metadata["reprocessed"] = true
	replayMsg.Metadata["original_message_id"] = record.ID

	if err := cr.handleMessage(ctx, replayMsg); err != nil {
		t.Fatalf("replay: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 2 })

	if capture.count() < 2 {
		t.Fatalf("expected 2 messages (original + replay), got %d", capture.count())
	}
}

// ===================================================================
// Test 34: Replay on File Source
// ===================================================================

func TestReplay_FileSource(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	store := storage.NewMemoryStore(0, 0)

	inputDir := filepath.Join(t.TempDir(), "input")
	os.MkdirAll(inputDir, 0o755)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { transformed: true, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "replay-file",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "file",
			File: &config.FileListener{
				Directory:    inputDir,
				FilePattern:  "*.dat",
				PollInterval: "100ms",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest"},
		},
	}

	fileSrc := connector.NewFileSource(chCfg.Listener.File, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, fileSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir, store)

	os.WriteFile(filepath.Join(inputDir, "msg1.dat"), []byte("file content for replay"), 0o644)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 1 })

	records, err := store.Query(storage.QueryOpts{ChannelID: "replay-file", Stage: "received"})
	if err != nil {
		t.Fatalf("query store: %v", err)
	}
	if len(records) < 1 {
		t.Fatal("expected at least 1 received record in store")
	}

	record := records[0]
	replayMsg := message.New("replay-file", record.Content)
	replayMsg.Metadata["reprocessed"] = true

	if err := cr.handleMessage(ctx, replayMsg); err != nil {
		t.Fatalf("replay: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 2 })

	if capture.count() < 2 {
		t.Fatalf("expected 2 messages, got %d", capture.count())
	}
}

// ===================================================================
// Test 35: Replay on TCP/MLLP Source
// ===================================================================

func TestReplay_TCPMLLPSource(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	store := storage.NewMemoryStore(0, 0)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { source_type: msg.transport, hl7: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "replay-tcp",
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

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, tcpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir, store)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	time.Sleep(100 * time.Millisecond)

	hl7 := "MSH|^~\\&|SRC|FAC|DST|FAC|20230101||ADT^A01|REPLAY001|P|2.5\rPID|1||MRN888||Replay^Test\r"
	conn, err := net.Dial("tcp", tcpSrc.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	var buf strings.Builder
	buf.WriteByte(0x0B)
	buf.WriteString(hl7)
	buf.WriteByte(0x1C)
	buf.WriteByte(0x0D)
	conn.Write([]byte(buf.String()))
	conn.Close()

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 1 })

	records, err := store.Query(storage.QueryOpts{ChannelID: "replay-tcp", Stage: "received"})
	if err != nil {
		t.Fatalf("query store: %v", err)
	}
	if len(records) < 1 {
		t.Fatal("expected at least 1 received record")
	}

	record := records[0]
	replayMsg := message.New("replay-tcp", record.Content)
	replayMsg.Metadata["reprocessed"] = true

	if err := cr.handleMessage(ctx, replayMsg); err != nil {
		t.Fatalf("replay: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 2 })
	if capture.count() < 2 {
		t.Fatalf("expected 2 messages (original + replay), got %d", capture.count())
	}
}

// ===================================================================
// Test 36: Replay on Channel Source (Inter-Channel)
// ===================================================================

func TestReplay_ChannelSource(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	storeB := storage.NewMemoryStore(0, 0)

	channelDirA := t.TempDir()
	channelDirB := t.TempDir()

	writeJS(t, channelDirA, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { from_a: true, data: msg.body } };
};`)
	writeJS(t, channelDirB, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { from_b: true, forwarded: msg.body } };
};`)

	busID := fmt.Sprintf("replay-bus-%d", time.Now().UnixNano())

	chCfgA := &config.ChannelConfig{
		ID:      "replay-ch-a",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "to-b"},
		},
	}

	chCfgB := &config.ChannelConfig{
		ID:      "replay-ch-b",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type:    "channel",
			Channel: &config.ChannelListener{SourceChannelID: busID},
		},
		Destinations: []config.ChannelDestination{
			{Name: "final"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfgA.Listener.HTTP, e2eLogger())
	channelDest := connector.NewChannelDest("to-b", busID, e2eLogger())
	channelSrc := connector.NewChannelSource(chCfgB.Listener.Channel, e2eLogger())
	httpDest := connector.NewHTTPDest("final", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	crA := buildChannelRuntime(t, chCfgA.ID, chCfgA, httpSrc, map[string]connector.DestinationConnector{
		"to-b": channelDest,
	}, channelDirA)

	crB := buildChannelRuntimeWithStore(t, chCfgB.ID, chCfgB, channelSrc, map[string]connector.DestinationConnector{
		"final": httpDest,
	}, channelDirB, storeB)

	ctx := context.Background()
	if err := crB.Start(ctx); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer crB.Stop(ctx)
	if err := crA.Start(ctx); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer crA.Stop(ctx)

	resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader("channel replay data"))
	resp.Body.Close()

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 1 })

	records, err := storeB.Query(storage.QueryOpts{ChannelID: "replay-ch-b", Stage: "received"})
	if err != nil {
		t.Fatalf("query store: %v", err)
	}
	if len(records) < 1 {
		t.Fatal("expected at least 1 received record in channel B store")
	}

	record := records[0]
	replayMsg := message.New("replay-ch-b", record.Content)
	replayMsg.Metadata["reprocessed"] = true

	if err := crB.handleMessage(ctx, replayMsg); err != nil {
		t.Fatalf("replay on channel B: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 2 })
	if capture.count() < 2 {
		t.Fatalf("expected 2 messages, got %d", capture.count())
	}
}

// ===================================================================
// Test 40: Batch Replay
// ===================================================================

func TestReplay_Batch(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	store := storage.NewMemoryStore(0, 0)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { batch_replay: true, data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "replay-batch",
		Enabled: true,
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

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir, store)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	for i := 0; i < 5; i++ {
		resp, _ := http.Post("http://"+httpSrc.Addr()+"/", "text/plain",
			strings.NewReader(fmt.Sprintf("batch message %d", i)))
		resp.Body.Close()
	}

	waitFor(t, 5*time.Second, func() bool { return capture.count() >= 5 })

	var errorMu sync.Mutex
	var errorIDs []string
	records, _ := store.Query(storage.QueryOpts{ChannelID: "replay-batch", Stage: "received"})
	for i, rec := range records {
		if i%2 == 0 {
			errRec := &storage.MessageRecord{
				ID:        rec.ID,
				ChannelID: "replay-batch",
				Stage:     "error",
				Status:    "ERROR",
				Content:   rec.Content,
				Timestamp: time.Now(),
			}
			store.Save(errRec)
			errorMu.Lock()
			errorIDs = append(errorIDs, rec.ID)
			errorMu.Unlock()
		}
	}

	errorRecords, _ := store.Query(storage.QueryOpts{ChannelID: "replay-batch", Stage: "error", Status: "ERROR"})
	if len(errorRecords) < 1 {
		t.Fatalf("expected error records in store, got %d", len(errorRecords))
	}

	for _, errRec := range errorRecords {
		replayMsg := message.New("replay-batch", errRec.Content)
		replayMsg.Metadata["reprocessed"] = true
		replayMsg.Metadata["original_message_id"] = errRec.ID
		if err := cr.handleMessage(ctx, replayMsg); err != nil {
			t.Fatalf("batch replay message %s: %v", errRec.ID, err)
		}
	}

	waitFor(t, 3*time.Second, func() bool {
		return capture.count() >= 5+len(errorRecords)
	})

	if capture.count() < 5+len(errorRecords) {
		t.Fatalf("expected %d total messages (5 original + %d replayed), got %d",
			5+len(errorRecords), len(errorRecords), capture.count())
	}
}

// ===================================================================
// Test 41: Full Storage Roundtrip -- Received vs Sent
// ===================================================================

func TestStorageRoundtrip_ReceivedVsSent(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	store := storage.NewMemoryStore(0, 0)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { transformed: true, original_data: msg.body } };
};`)

	chCfg := &config.ChannelConfig{
		ID:      "storage-roundtrip",
		Enabled: true,
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

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir, store)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	inputPayload := `exact input bytes to verify`
	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "text/plain", strings.NewReader(inputPayload))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	waitFor(t, 2*time.Second, func() bool { return capture.count() >= 1 })

	receivedRecords, _ := store.Query(storage.QueryOpts{ChannelID: "storage-roundtrip", Stage: "received"})
	if len(receivedRecords) < 1 {
		t.Fatal("expected received stage in store")
	}
	receivedContent := receivedRecords[0].Content
	if receivedContent == nil {
		t.Fatal("received stage has nil content")
	}

	receivedMsg, err := message.FromIntuJSON(receivedContent, "storage-roundtrip")
	if err == nil {
		if !strings.Contains(string(receivedMsg.Raw), inputPayload) {
			t.Fatalf("received content should contain input payload, got: %s", string(receivedMsg.Raw))
		}
	} else {
		if !strings.Contains(string(receivedContent), inputPayload) {
			t.Fatalf("received content should contain input payload, got: %s", string(receivedContent))
		}
	}

	sentRecords, _ := store.Query(storage.QueryOpts{ChannelID: "storage-roundtrip", Stage: "sent"})
	if len(sentRecords) < 1 {
		t.Fatal("expected sent stage in store")
	}
	sentContent := sentRecords[0].Content
	if sentContent == nil {
		t.Fatal("sent stage has nil content")
	}
	if !strings.Contains(string(sentContent), "transformed") {
		t.Fatalf("sent content should contain transformer output, got: %s", string(sentContent))
	}

	transformedRecords, _ := store.Query(storage.QueryOpts{ChannelID: "storage-roundtrip", Stage: "transformed"})
	if len(transformedRecords) < 1 {
		t.Fatal("expected transformed stage in store")
	}
}

// ===================================================================
// Test 42: IntuMessage Serialization Roundtrip
// ===================================================================

func TestIntuMessageSerializationRoundtrip(t *testing.T) {
	original := message.New("test-ch", []byte(`{"patient":"John"}`))
	original.Transport = "http"
	original.ContentType = "json"
	original.HTTP = &message.HTTPMeta{
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Authorization": "Bearer token123",
		},
		Method: "POST",
	}

	data, err := original.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON: %v", err)
	}

	reconstructed, err := message.FromIntuJSON(data, "test-ch")
	if err != nil {
		t.Fatalf("FromIntuJSON: %v", err)
	}

	if reconstructed.Transport != "http" {
		t.Fatalf("expected transport=http, got %s", reconstructed.Transport)
	}
	if string(reconstructed.ContentType) != "json" {
		t.Fatalf("expected contentType=json, got %s", reconstructed.ContentType)
	}
	if reconstructed.ChannelID != "test-ch" {
		t.Fatalf("expected channelID=test-ch, got %s", reconstructed.ChannelID)
	}
	if string(reconstructed.Raw) != `{"patient":"John"}` {
		t.Fatalf("expected raw to match, got %s", string(reconstructed.Raw))
	}
	if reconstructed.HTTP == nil {
		t.Fatal("expected HTTP metadata")
	}
	if reconstructed.HTTP.Method != "POST" {
		t.Fatalf("expected method=POST, got %s", reconstructed.HTTP.Method)
	}
	if reconstructed.HTTP.Headers["Content-Type"] != "application/json" {
		t.Fatalf("expected Content-Type header, got %v", reconstructed.HTTP.Headers)
	}
}

// ===================================================================
// Test 43: HL7v2 Input Fidelity
// ===================================================================

func TestHL7v2InputFidelity(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	store := storage.NewMemoryStore(0, 0)

	inputDir := filepath.Join(t.TempDir(), "hl7-in")
	os.MkdirAll(inputDir, 0o755)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var msh = msg.body.MSH || {};
	var pid = msg.body.PID || {};
	return {
		body: {
			controlId: msh["9"],
			messageType: msh["8"],
			patientMRN: pid["3"],
			patientName: pid["5"],
		},
	};
};`)

	hl7Raw := "MSH|^~\\&|LABSYS|HOSPITAL|FHIRSYS|CLOUD|20230615120000||ADT^A01|FIDELITY001|P|2.5\rPID|1||MRN_FIDELITY||Fidelity^Test^M||19850301|F\r"

	chCfg := &config.ChannelConfig{
		ID:      "hl7-fidelity",
		Enabled: true,
		DataTypes: &config.DataTypesConfig{
			Inbound: "hl7v2",
		},
		Pipeline: &config.PipelineConfig{
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
			{Name: "dest"},
		},
	}

	fileSrc := connector.NewFileSource(chCfg.Listener.File, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, fileSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir, store)

	os.WriteFile(filepath.Join(inputDir, "fidelity.hl7"), []byte(hl7Raw), 0o644)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 1 })

	receivedRecords, _ := store.Query(storage.QueryOpts{ChannelID: "hl7-fidelity", Stage: "received"})
	if len(receivedRecords) < 1 {
		t.Fatal("expected received stage")
	}

	receivedContent := string(receivedRecords[0].Content)
	if !strings.Contains(receivedContent, "FIDELITY001") {
		t.Fatalf("received content should preserve HL7 control ID, got: %s", receivedContent[:min(200, len(receivedContent))])
	}

	var result map[string]any
	json.Unmarshal(capture.body(0), &result)
	if result["controlId"] != "FIDELITY001" {
		t.Fatalf("expected controlId=FIDELITY001, got %v", result["controlId"])
	}
	if result["patientMRN"] != "MRN_FIDELITY" {
		t.Fatalf("expected patientMRN=MRN_FIDELITY, got %v", result["patientMRN"])
	}
	nameMap, ok := result["patientName"].(map[string]any)
	if !ok {
		t.Fatalf("expected patientName to be map, got %T", result["patientName"])
	}
	if nameMap["1"] != "Fidelity" {
		t.Fatalf("expected family=Fidelity, got %v", nameMap["1"])
	}
}

// ===================================================================
// Test 44: Cross-Format Input/Output (HL7 in -> FHIR out)
// ===================================================================

func TestCrossFormat_HL7InFHIROut(t *testing.T) {
	capture := &destCapture{}
	destServer := httptest.NewServer(capture.handler())
	defer destServer.Close()

	store := storage.NewMemoryStore(0, 0)

	inputDir := filepath.Join(t.TempDir(), "hl7-in")
	os.MkdirAll(inputDir, 0o755)

	channelDir := t.TempDir()
	writeJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	var pid = msg.body.PID || {};
	var nameField = pid["5"] || {};
	return {
		body: {
			resourceType: "Bundle",
			type: "transaction",
			entry: [{
				resource: {
					resourceType: "Patient",
					identifier: [{ value: pid["3"] || "unknown" }],
					name: [{ family: nameField["1"] || "Unknown", given: [nameField["2"] || "Unknown"] }]
				},
				request: { method: "POST", url: "Patient" }
			}]
		},
	};
};`)

	hl7Msg := "MSH|^~\\&|LAB|HOSP|EHR|CLOUD|20230101||ADT^A01|CROSS001|P|2.5\rPID|1||MRN_CROSS||CrossTest^Jane^M||19900101|F\r"

	chCfg := &config.ChannelConfig{
		ID:      "cross-format",
		Enabled: true,
		DataTypes: &config.DataTypesConfig{
			Inbound:  "hl7v2",
			Outbound: "fhir_r4",
		},
		Pipeline: &config.PipelineConfig{
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
			{Name: "dest"},
		},
	}

	fileSrc := connector.NewFileSource(chCfg.Listener.File, e2eLogger())
	httpDest := connector.NewHTTPDest("dest", &config.HTTPDestConfig{URL: destServer.URL}, e2eLogger())

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, fileSrc, map[string]connector.DestinationConnector{
		"dest": httpDest,
	}, channelDir, store)

	os.WriteFile(filepath.Join(inputDir, "cross.hl7"), []byte(hl7Msg), 0o644)

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer cr.Stop(ctx)

	waitFor(t, 3*time.Second, func() bool { return capture.count() >= 1 })

	receivedRecords, _ := store.Query(storage.QueryOpts{ChannelID: "cross-format", Stage: "received"})
	if len(receivedRecords) < 1 {
		t.Fatal("expected received stage")
	}
	receivedContent := string(receivedRecords[0].Content)
	if !strings.Contains(receivedContent, "CROSS001") {
		t.Fatalf("received stage should contain raw HL7 control ID")
	}

	sentRecords, _ := store.Query(storage.QueryOpts{ChannelID: "cross-format", Stage: "sent"})
	if len(sentRecords) < 1 {
		t.Fatal("expected sent stage")
	}
	sentContent := string(sentRecords[0].Content)
	if !strings.Contains(sentContent, "Bundle") {
		t.Fatalf("sent stage should contain FHIR Bundle, got: %s", sentContent[:min(200, len(sentContent))])
	}

	var bundle map[string]any
	json.Unmarshal(capture.body(0), &bundle)
	if bundle["resourceType"] != "Bundle" {
		t.Fatalf("expected resourceType=Bundle, got %v", bundle["resourceType"])
	}
	if bundle["type"] != "transaction" {
		t.Fatalf("expected type=transaction, got %v", bundle["type"])
	}
	entries := bundle["entry"].([]any)
	entry := entries[0].(map[string]any)
	resource := entry["resource"].(map[string]any)
	if resource["resourceType"] != "Patient" {
		t.Fatalf("expected Patient, got %v", resource["resourceType"])
	}
	names := resource["name"].([]any)
	name := names[0].(map[string]any)
	if name["family"] != "CrossTest" {
		t.Fatalf("expected family=CrossTest, got %v", name["family"])
	}
}
