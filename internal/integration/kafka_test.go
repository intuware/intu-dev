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
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/integration/testutil"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/runtime"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// produceViaConnector publishes a message to Kafka using the real KafkaDest
// connector — the same code path used in production. This avoids re-implementing
// the Kafka wire protocol (CRC, framing, etc.) in test helpers.
func produceViaConnector(t *testing.T, brokers []string, topic string, payload []byte) {
	t.Helper()
	dest := connector.NewKafkaDest("test-producer", &config.KafkaDestConfig{
		Brokers:  brokers,
		Topic:    topic,
		ClientID: "integration-test-producer",
	}, testutil.DiscardLogger())
	t.Cleanup(func() { dest.Stop(context.Background()) })

	msg := message.New("", payload)
	resp, err := dest.Send(context.Background(), msg)
	require.NoError(t, err, "produce to kafka")
	require.NotNil(t, resp)
	require.Equal(t, 200, resp.StatusCode, "produce should succeed (status 200)")
}

func TestKafkaSource_ReceivesMessages(t *testing.T) {
	if kafkaC == nil {
		t.Skip("Kafka container not available")
	}

	topic := "test-source-recv"

	var mu sync.Mutex
	var received [][]byte

	src := connector.NewKafkaSource(&config.KafkaListener{
		Brokers: kafkaC.Brokers,
		Topic:   topic,
		GroupID: "test-group",
	}, testutil.DiscardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		mu.Lock()
		received = append(received, msg.Raw)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	defer src.Stop(context.Background())

	// Let the source connect and begin its poll loop before producing.
	time.Sleep(3 * time.Second)

	produceViaConnector(t, kafkaC.Brokers, topic, []byte(`{"patient":"John Doe","mrn":"MRN001"}`))

	testutil.WaitFor(t, 12*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, len(received), 1)
}

func TestKafkaDest_SendsMessages(t *testing.T) {
	if kafkaC == nil {
		t.Skip("Kafka container not available")
	}

	dest := connector.NewKafkaDest("kafka-dest", &config.KafkaDestConfig{
		Brokers:  kafkaC.Brokers,
		Topic:    "test-dest-send",
		ClientID: "test-producer",
	}, testutil.DiscardLogger())

	msg := message.New("", []byte(`{"event":"test","value":42}`))
	msg.Transport = "kafka"

	resp, err := dest.Send(context.Background(), msg)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 200, resp.StatusCode)

	assert.Equal(t, "kafka", dest.Type())
}

func TestKafkaSourceToHTTPDest_Pipeline(t *testing.T) {
	if kafkaC == nil {
		t.Skip("Kafka container not available")
	}

	topic := "test-pipeline-kafka-http"

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

	channelDir := t.TempDir()
	testutil.WriteJS(t, channelDir, "transformer.js", testutil.TransformerJSONEnrich)
	testutil.WriteJS(t, channelDir, "validator.js", testutil.ValidatorNonEmpty)

	chCfg := &config.ChannelConfig{
		ID:      "kafka-to-http-test",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Validator:   "validator.js",
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "kafka",
			Kafka: &config.KafkaListener{
				Brokers: kafkaC.Brokers,
				Topic:   topic,
				GroupID: "pipeline-test",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "http-dest"},
		},
	}

	kafkaSrc := connector.NewKafkaSource(chCfg.Listener.Kafka, testutil.DiscardLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: destServer.URL}, testutil.DiscardLogger())

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, kafkaSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	// Let the pipeline's source connect and start polling before producing.
	time.Sleep(3 * time.Second)

	produceViaConnector(t, kafkaC.Brokers, topic, []byte(`{"patient":"Jane Smith"}`))

	testutil.WaitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(capturedBodies), 1)

	var result map[string]any
	require.NoError(t, json.Unmarshal(capturedBodies[0], &result))
	assert.Equal(t, "kafka-to-http-test", result["channelId"])
	assert.Equal(t, "kafka", result["transport"])
}

func buildIntegrationChannelRuntime(
	t *testing.T,
	id string,
	chCfg *config.ChannelConfig,
	source connector.SourceConnector,
	destinations map[string]connector.DestinationConnector,
	channelDir string,
) *runtime.ChannelRuntime {
	t.Helper()
	logger := testutil.DiscardLogger()
	runner, err := runtime.NewNodeRunner(2, logger)
	require.NoError(t, err)
	t.Cleanup(func() { runner.Close() })
	pipeline := runtime.NewPipeline(channelDir, channelDir, id, chCfg, runner, logger)

	return &runtime.ChannelRuntime{
		ID:           id,
		Config:       chCfg,
		Source:       source,
		Destinations: destinations,
		DestConfigs:  chCfg.Destinations,
		Pipeline:     pipeline,
		Logger:       logger,
	}
}

// TestMain starts containers once and shares them across all tests in this package.
// When Docker is not available, all containers are nil and individual tests skip.
func TestMain(m *testing.M) {
	if !testutil.DockerAvailable() {
		fmt.Fprintf(os.Stderr, "SKIP: %s — integration tests require Docker\n", testutil.DockerReason())
		os.Exit(0)
	}

	ctx := context.Background()
	var err error

	kafkaC, err = testutil.StartKafkaContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: Kafka container failed: %v\n", err)
		kafkaC = nil
	}

	pgC, err = testutil.StartPostgresContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: Postgres container failed: %v\n", err)
		pgC = nil
	}

	sftpC, err = testutil.StartSFTPContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: SFTP container failed: %v\n", err)
		sftpC = nil
	}

	mailhogC, err = testutil.StartMailHogContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: MailHog container failed: %v\n", err)
		mailhogC = nil
	}

	greenmailC, err = testutil.StartGreenMailContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: GreenMail container failed: %v\n", err)
		greenmailC = nil
	}

	code := m.Run()

	if kafkaC != nil {
		kafkaC.Terminate(ctx)
	}
	if pgC != nil {
		pgC.Terminate(ctx)
	}
	if sftpC != nil {
		sftpC.Terminate(ctx)
	}
	if mailhogC != nil {
		mailhogC.Terminate(ctx)
	}
	if greenmailC != nil {
		greenmailC.Terminate(ctx)
	}

	os.Exit(code)
}

var (
	kafkaC     *testutil.KafkaContainer
	pgC        *testutil.PostgresContainer
	sftpC      *testutil.SFTPContainer
	mailhogC   *testutil.MailHogContainer
	greenmailC *testutil.GreenMailContainer
)
