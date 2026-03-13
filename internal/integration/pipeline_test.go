//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/integration/testutil"
	"github.com/intuware/intu-dev/internal/runtime"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPipeline_HTTPToMultiDest_KafkaAndFile tests an HTTP source routing to
// both a Kafka destination and a file destination simultaneously.
func TestPipeline_HTTPToMultiDest_KafkaAndFile(t *testing.T) {
	if kafkaC == nil {
		t.Skip("Kafka container not available")
	}

	outputDir := filepath.Join(t.TempDir(), "output")
	os.MkdirAll(outputDir, 0o755)

	channelDir := t.TempDir()
	testutil.WriteJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return {
		body: {
			event: "patient_registered",
			data: msg.body,
			channelId: ctx.channelId,
			timestamp: new Date().toISOString()
		}
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "http-to-multi",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "kafka-dest"},
			{Name: "file-archive"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, testutil.DiscardLogger())
	kafkaDest := connector.NewKafkaDest("kafka-dest", &config.KafkaDestConfig{
		Brokers:  kafkaC.Brokers,
		Topic:    "multi-dest-output",
		ClientID: "multi-test",
	}, testutil.DiscardLogger())
	fileDest := connector.NewFileDest("file-archive", &config.FileDestMapConfig{
		Directory:       outputDir,
		FilenamePattern: "{{channelId}}_{{messageId}}.json",
	}, testutil.DiscardLogger())

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"kafka-dest":   kafkaDest,
		"file-archive": fileDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	payload := `{"mrn":"MRN001","name":"John Doe"}`
	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "application/json",
		strings.NewReader(payload))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	testutil.WaitFor(t, 5*time.Second, func() bool {
		entries, _ := os.ReadDir(outputDir)
		return len(entries) >= 1
	})

	entries, _ := os.ReadDir(outputDir)
	require.GreaterOrEqual(t, len(entries), 1, "file destination should have written at least 1 file")

	data, _ := os.ReadFile(filepath.Join(outputDir, entries[0].Name()))
	var result map[string]any
	require.NoError(t, json.Unmarshal(data, &result))
	assert.Equal(t, "patient_registered", result["event"])
	assert.Equal(t, "http-to-multi", result["channelId"])
}

// TestPipeline_HL7viaSFTP_ToFHIR exercises the healthcare-specific flow:
// HL7 files on SFTP → parse → transform to FHIR → send to FHIR server.
func TestPipeline_HL7viaSFTP_ToFHIR(t *testing.T) {
	if sftpC == nil {
		t.Skip("SFTP container not available")
	}

	sftpClient := dialSFTP(t)
	defer sftpClient.Close()

	sftpClient.MkdirAll("/upload/hl7-in")
	sftpClient.MkdirAll("/upload/hl7-done")

	var mu sync.Mutex
	var fhirBundles [][]byte
	var authHeaders []string
	fhirServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		fhirBundles = append(fhirBundles, body)
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/fhir+json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"resourceType":"OperationOutcome","issue":[]}`))
	}))
	defer fhirServer.Close()

	channelDir := t.TempDir()
	testutil.WriteJS(t, channelDir, "validator.js", testutil.ValidatorHL7)
	testutil.WriteJS(t, channelDir, "transformer.js", testutil.TransformerHL7ToFHIR)

	chCfg := &config.ChannelConfig{
		ID:      "hl7-sftp-to-fhir",
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
			Type: "sftp",
			SFTP: &config.SFTPListener{
				Host:         sftpC.Host,
				Port:         sftpC.Port,
				Directory:    "/upload/hl7-in",
				FilePattern:  "*.hl7",
				PollInterval: "500ms",
				MoveTo:       "/upload/hl7-done",
				Auth: &config.AuthConfig{
					Type:     "password",
					Username: sftpC.User,
					Password: sftpC.Password,
				},
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "fhir-dest"},
		},
	}

	sftpSrc := connector.NewSFTPSource(chCfg.Listener.SFTP, testutil.DiscardLogger())
	httpDest := connector.NewHTTPDest("fhir-dest", &config.HTTPDestConfig{
		URL:    fhirServer.URL + "/fhir/",
		Method: "POST",
		Headers: map[string]string{
			"Content-Type": "application/fhir+json",
		},
		Auth: &config.HTTPAuthConfig{
			Type:  "bearer",
			Token: "test-fhir-token",
		},
	}, testutil.DiscardLogger())

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, sftpSrc, map[string]connector.DestinationConnector{
		"fhir-dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	f, err := sftpClient.Create("/upload/hl7-in/adt_001.hl7")
	require.NoError(t, err)
	f.Write([]byte(testutil.HL7v2_ADT_A01))
	f.Close()

	f2, err := sftpClient.Create("/upload/hl7-in/adt_002.hl7")
	require.NoError(t, err)
	f2.Write([]byte(testutil.HL7v2_ADT_A01_2))
	f2.Close()

	testutil.WaitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(fhirBundles) >= 2
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(fhirBundles), 2)

	for i, body := range fhirBundles {
		var bundle map[string]any
		require.NoError(t, json.Unmarshal(body, &bundle))
		assert.Equal(t, "Bundle", bundle["resourceType"], "bundle %d", i)
		assert.Equal(t, "transaction", bundle["type"], "bundle %d", i)

		entries, ok := bundle["entry"].([]any)
		require.True(t, ok, "bundle %d entries", i)
		require.GreaterOrEqual(t, len(entries), 1, "bundle %d", i)

		entry := entries[0].(map[string]any)
		resource := entry["resource"].(map[string]any)
		assert.Equal(t, "Patient", resource["resourceType"])
	}

	for _, auth := range authHeaders {
		assert.Equal(t, "Bearer test-fhir-token", auth)
	}

	processed, err := sftpClient.ReadDir("/upload/hl7-done")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(processed), 2, "both HL7 files should be moved to done")
}

// TestPipeline_EngineLevel_WithRealConfig tests the full engine lifecycle using
// a realistic project directory layout with intu.yaml config.
func TestPipeline_EngineLevel_WithRealConfig(t *testing.T) {
	var mu sync.Mutex
	var capturedBodies [][]byte
	destServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		capturedBodies = append(capturedBodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer destServer.Close()

	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "src", "channels")
	channelDir := filepath.Join(channelsDir, "intake-channel")
	os.MkdirAll(channelDir, 0o755)

	intuYAML := fmt.Sprintf(`runtime:
  name: integration-test
  log_level: error
channels_dir: src/channels
destinations:
  api-gateway:
    type: http
    http:
      url: %s
`, destServer.URL)
	os.WriteFile(filepath.Join(projectDir, "intu.yaml"), []byte(intuYAML), 0o644)

	channelYAML := `id: intake-channel
enabled: true
listener:
  type: http
  http:
    port: 0
pipeline:
  validator: validator.js
  transformer: transformer.js
destinations:
  - name: api-gateway
    ref: api-gateway
`
	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte(channelYAML), 0o644)

	testutil.WriteJS(t, channelDir, "validator.js", testutil.ValidatorNonEmpty)
	testutil.WriteJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { body: { engine_integration: true, data: msg.body, channel: ctx.channelId } };
};`)

	loader := config.NewLoader(projectDir)
	cfg, err := loader.Load("dev")
	require.NoError(t, err)

	factory := connector.NewFactory(testutil.DiscardLogger())
	engine := runtime.NewDefaultEngine(projectDir, cfg, factory, testutil.DiscardLogger())

	ctx := context.Background()
	require.NoError(t, engine.Start(ctx))
	defer engine.Stop(ctx)

	channelIDs := engine.ListChannelIDs()
	require.Len(t, channelIDs, 1, "engine should have 1 running channel")

	cr, ok := engine.GetChannelRuntime("intake-channel")
	require.True(t, ok, "intake-channel should be running")
	require.NotNil(t, cr)

	httpSrc, ok := cr.Source.(*connector.HTTPSource)
	require.True(t, ok, "source should be HTTPSource")

	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "application/json",
		strings.NewReader(`{"event":"admission","mrn":"MRN-999"}`))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	testutil.WaitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(capturedBodies), 1)

	var result map[string]any
	require.NoError(t, json.Unmarshal(capturedBodies[0], &result))
	assert.Equal(t, true, result["engine_integration"])
	assert.Equal(t, "intake-channel", result["channel"])
}
