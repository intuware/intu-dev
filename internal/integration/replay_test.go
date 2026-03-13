//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/integration/testutil"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/runtime"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildChannelRuntimeWithStore(
	t *testing.T,
	id string,
	chCfg *config.ChannelConfig,
	source connector.SourceConnector,
	destinations map[string]connector.DestinationConnector,
	channelDir string,
	store storage.MessageStore,
) *runtime.ChannelRuntime {
	t.Helper()
	cr := buildIntegrationChannelRuntime(t, id, chCfg, source, destinations, channelDir)
	cr.Store = store
	return cr
}

// TestReplay_KafkaSource produces a message to Kafka, processes it through
// the pipeline with storage, then replays from the stored content.
func TestReplay_KafkaSource(t *testing.T) {
	if kafkaC == nil {
		t.Skip("Kafka container not available")
	}

	topic := "test-replay-kafka"
	store := storage.NewMemoryStore(0, 0)

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

	chCfg := &config.ChannelConfig{
		ID:      "replay-kafka",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "kafka",
			Kafka: &config.KafkaListener{
				Brokers: kafkaC.Brokers,
				Topic:   topic,
				GroupID: "replay-test",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "http-dest"},
		},
	}

	kafkaSrc := connector.NewKafkaSource(chCfg.Listener.Kafka, testutil.DiscardLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: destServer.URL}, testutil.DiscardLogger())

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, kafkaSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
	}, channelDir, store)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	time.Sleep(3 * time.Second)

	produceViaConnector(t, kafkaC.Brokers, topic, []byte(`{"patient":"Replay Kafka"}`))

	testutil.WaitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 1
	})

	records, err := store.Query(storage.QueryOpts{ChannelID: "replay-kafka", Stage: "received"})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(records), 1)

	replayMsg := message.New("replay-kafka", records[0].Content)
	replayMsg.Metadata["reprocessed"] = true

	require.NoError(t, cr.HandleMessage(ctx, replayMsg))

	testutil.WaitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 2
	})

	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, len(capturedBodies), 2, "expected original + replay")
}

// TestReplay_SFTPSource writes a file to SFTP, processes it, then replays.
func TestReplay_SFTPSource(t *testing.T) {
	if sftpC == nil {
		t.Skip("SFTP container not available")
	}

	sftpClient := dialSFTP(t)
	defer sftpClient.Close()

	sftpClient.MkdirAll("/upload/replay-in")
	sftpClient.MkdirAll("/upload/replay-done")

	store := storage.NewMemoryStore(0, 0)

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

	chCfg := &config.ChannelConfig{
		ID:      "replay-sftp",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "sftp",
			SFTP: &config.SFTPListener{
				Host:         sftpC.Host,
				Port:         sftpC.Port,
				Directory:    "/upload/replay-in",
				FilePattern:  "*.json",
				PollInterval: "500ms",
				MoveTo:       "/upload/replay-done",
				Auth: &config.AuthConfig{
					Type:     "password",
					Username: sftpC.User,
					Password: sftpC.Password,
				},
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "http-dest"},
		},
	}

	sftpSrc := connector.NewSFTPSource(chCfg.Listener.SFTP, testutil.DiscardLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: destServer.URL}, testutil.DiscardLogger())

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, sftpSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
	}, channelDir, store)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	f, err := sftpClient.Create("/upload/replay-in/replay.json")
	require.NoError(t, err)
	f.Write([]byte(`{"patient":"Replay SFTP"}`))
	f.Close()

	testutil.WaitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 1
	})

	records, err := store.Query(storage.QueryOpts{ChannelID: "replay-sftp", Stage: "received"})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(records), 1)

	replayMsg := message.New("replay-sftp", records[0].Content)
	replayMsg.Metadata["reprocessed"] = true

	require.NoError(t, cr.HandleMessage(ctx, replayMsg))

	testutil.WaitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 2
	})

	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, len(capturedBodies), 2)
}

// TestReplay_DatabaseSource polls a row from Postgres, stores it, then replays.
func TestReplay_DatabaseSource(t *testing.T) {
	if pgC == nil {
		t.Skip("PostgreSQL container not available")
	}

	db := openReplayPG(t)
	setupReplayTable(t, db)

	_, err := db.Exec(`INSERT INTO replay_patients (mrn, first_name, last_name) VALUES ($1, $2, $3)`,
		"MRN-REPLAY", "Replay", "Patient")
	require.NoError(t, err)

	store := storage.NewMemoryStore(0, 0)

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

	chCfg := &config.ChannelConfig{
		ID:      "replay-db",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "database",
			Database: &config.DBListener{
				Driver:               "pgx",
				DSN:                  pgC.DSN,
				PollInterval:         "500ms",
				Query:                "SELECT id, mrn, first_name, last_name FROM replay_patients WHERE processed = false",
				PostProcessStatement: "UPDATE replay_patients SET processed = true WHERE id = :id",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "http-dest"},
		},
	}

	dbSrc := connector.NewDatabaseSource(chCfg.Listener.Database, testutil.DiscardLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: destServer.URL}, testutil.DiscardLogger())

	cr := buildChannelRuntimeWithStore(t, chCfg.ID, chCfg, dbSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
	}, channelDir, store)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	testutil.WaitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 1
	})

	records, err := store.Query(storage.QueryOpts{ChannelID: "replay-db", Stage: "received"})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(records), 1)

	replayMsg := message.New("replay-db", records[0].Content)
	replayMsg.Metadata["reprocessed"] = true

	require.NoError(t, cr.HandleMessage(ctx, replayMsg))

	testutil.WaitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 2
	})

	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, len(capturedBodies), 2)

	var result map[string]any
	require.NoError(t, json.Unmarshal(capturedBodies[1], &result))
	assert.Equal(t, "replay-db", result["channelId"])
}

func openReplayPG(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", pgC.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func setupReplayTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS replay_patients (
		id SERIAL PRIMARY KEY,
		mrn VARCHAR(50) NOT NULL,
		first_name VARCHAR(100),
		last_name VARCHAR(100),
		processed BOOLEAN DEFAULT false,
		created_at TIMESTAMP DEFAULT NOW()
	)`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM replay_patients`)
	require.NoError(t, err)
}
