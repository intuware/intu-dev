//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
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

func TestKafkaSource_ReceivesMessages(t *testing.T) {
	if kafkaC == nil {
		t.Skip("Kafka container not available")
	}

	topic := "test-source-recv"
	produceToKafka(t, kafkaC.Brokers[0], topic, []byte(`{"patient":"John Doe","mrn":"MRN001"}`))

	var mu sync.Mutex
	var received [][]byte

	src := connector.NewKafkaSource(&config.KafkaListener{
		Brokers: kafkaC.Brokers,
		Topic:   topic,
		GroupID: "test-group",
	}, testutil.DiscardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		mu.Lock()
		received = append(received, msg.Raw)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	defer src.Stop(context.Background())

	testutil.WaitFor(t, 10*time.Second, func() bool {
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

	_, err := dest.Send(context.Background(), msg)
	require.NoError(t, err)

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

	produceToKafka(t, kafkaC.Brokers[0], topic, []byte(`{"patient":"Jane Smith"}`))

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

func produceToKafka(t *testing.T, broker, topic string, value []byte) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", broker, 5*time.Second)
	require.NoError(t, err, "connect to kafka broker")
	defer conn.Close()

	sendMetadataRequest(t, conn, topic)
	time.Sleep(500 * time.Millisecond)
	sendProduceRequest(t, conn, topic, value)
}

func sendMetadataRequest(t *testing.T, conn net.Conn, topic string) {
	t.Helper()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	var buf []byte
	buf = appendInt16(buf, 3)  // Metadata API
	buf = appendInt16(buf, 0)  // v0
	buf = appendInt32(buf, 99) // correlationID
	buf = appendKafkaString(buf, "test-producer")
	buf = appendInt32(buf, 1)
	buf = appendKafkaString(buf, topic)

	frame := make([]byte, 4+len(buf))
	frame[0] = byte(len(buf) >> 24)
	frame[1] = byte(len(buf) >> 16)
	frame[2] = byte(len(buf) >> 8)
	frame[3] = byte(len(buf))
	copy(frame[4:], buf)
	conn.Write(frame)

	respSizeBuf := make([]byte, 4)
	io.ReadFull(conn, respSizeBuf)
	respSize := int(respSizeBuf[0])<<24 | int(respSizeBuf[1])<<16 | int(respSizeBuf[2])<<8 | int(respSizeBuf[3])
	if respSize > 0 && respSize < 1024*1024 {
		resp := make([]byte, respSize)
		io.ReadFull(conn, resp)
	}
}

func sendProduceRequest(t *testing.T, conn net.Conn, topic string, value []byte) {
	t.Helper()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	var msgBuf []byte
	msgBuf = append(msgBuf, 0, 0, 0, 0) // CRC placeholder
	msgBuf = append(msgBuf, 0)           // magic
	msgBuf = append(msgBuf, 0)           // attributes
	msgBuf = appendInt32(msgBuf, -1)     // key: null
	msgBuf = appendInt32(msgBuf, int32(len(value)))
	msgBuf = append(msgBuf, value...)

	var msgSet []byte
	msgSet = append(msgSet, 0, 0, 0, 0, 0, 0, 0, 0) // offset
	msgSet = appendInt32(msgSet, int32(len(msgBuf)))
	msgSet = append(msgSet, msgBuf...)

	var buf []byte
	buf = appendInt16(buf, 0) // Produce API
	buf = appendInt16(buf, 0) // v0
	buf = appendInt32(buf, 1) // correlationID
	buf = appendKafkaString(buf, "test-producer")
	buf = appendInt16(buf, 1)    // acks
	buf = appendInt32(buf, 5000) // timeout
	buf = appendInt32(buf, 1)    // 1 topic
	buf = appendKafkaString(buf, topic)
	buf = appendInt32(buf, 1) // 1 partition
	buf = appendInt32(buf, 0) // partition 0
	buf = appendInt32(buf, int32(len(msgSet)))
	buf = append(buf, msgSet...)

	frame := make([]byte, 4+len(buf))
	frame[0] = byte(len(buf) >> 24)
	frame[1] = byte(len(buf) >> 16)
	frame[2] = byte(len(buf) >> 8)
	frame[3] = byte(len(buf))
	copy(frame[4:], buf)
	conn.Write(frame)

	respSizeBuf := make([]byte, 4)
	io.ReadFull(conn, respSizeBuf)
	respSize := int(respSizeBuf[0])<<24 | int(respSizeBuf[1])<<16 | int(respSizeBuf[2])<<8 | int(respSizeBuf[3])
	if respSize > 0 && respSize < 1024*1024 {
		resp := make([]byte, respSize)
		io.ReadFull(conn, resp)
	}
}

func appendInt16(buf []byte, v int16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

func appendInt32(buf []byte, v int32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendKafkaString(buf []byte, s string) []byte {
	buf = appendInt16(buf, int16(len(s)))
	return append(buf, []byte(s)...)
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

	os.Exit(code)
}

var (
	kafkaC   *testutil.KafkaContainer
	pgC      *testutil.PostgresContainer
	sftpC    *testutil.SFTPContainer
	mailhogC *testutil.MailHogContainer
)
