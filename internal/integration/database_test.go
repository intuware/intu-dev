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
	"github.com/intuware/intu-dev/pkg/config"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseSource_PollsRows(t *testing.T) {
	if pgC == nil {
		t.Skip("PostgreSQL container not available")
	}

	db := openPG(t)
	setupPatientsTable(t, db)

	_, err := db.Exec(`INSERT INTO patients (mrn, first_name, last_name) VALUES ($1, $2, $3)`,
		"MRN001", "Alice", "Smith")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO patients (mrn, first_name, last_name) VALUES ($1, $2, $3)`,
		"MRN002", "Bob", "Jones")
	require.NoError(t, err)

	var mu sync.Mutex
	var received [][]byte

	src := connector.NewDatabaseSource(&config.DBListener{
		Driver:       "pgx",
		DSN:          pgC.DSN,
		PollInterval: "500ms",
		Query:        "SELECT id, mrn, first_name, last_name FROM patients WHERE processed = false",
		PostProcessStatement: "UPDATE patients SET processed = true WHERE id = :id",
	}, testutil.DiscardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		mu.Lock()
		received = append(received, msg.Raw)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	defer src.Stop(context.Background())

	testutil.WaitFor(t, 8*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= 2
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(received), 2)

	var row0 map[string]any
	require.NoError(t, json.Unmarshal(received[0], &row0))
	assert.Equal(t, "MRN001", row0["mrn"])
}

func TestDatabaseDest_InsertsRows(t *testing.T) {
	if pgC == nil {
		t.Skip("PostgreSQL container not available")
	}

	db := openPG(t)
	setupAuditTable(t, db)

	dest := connector.NewDatabaseDest("db-dest", &config.DBDestMapConfig{
		Driver:    "pgx",
		DSN:       pgC.DSN,
		Statement: "INSERT INTO audit_log (channel_id, message_body) VALUES ('${channelId}', '${raw}')",
	}, testutil.DiscardLogger())

	msg := message.New("test-channel", []byte(`{"event":"patient_registered","mrn":"MRN001"}`))
	_, err := dest.Send(context.Background(), msg)
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM audit_log").Scan(&count)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, 1)
}

func TestDatabaseSourceToHTTPDest_Pipeline(t *testing.T) {
	if pgC == nil {
		t.Skip("PostgreSQL container not available")
	}

	db := openPG(t)
	setupPatientsTable(t, db)

	_, err := db.Exec(`INSERT INTO patients (mrn, first_name, last_name) VALUES ($1, $2, $3)`,
		"MRN-PIPE-001", "Carol", "Williams")
	require.NoError(t, err)

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
		ID:      "db-to-http-test",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "database",
			Database: &config.DBListener{
				Driver:       "pgx",
				DSN:          pgC.DSN,
				PollInterval: "500ms",
				Query:        "SELECT id, mrn, first_name, last_name FROM patients WHERE processed = false",
				PostProcessStatement: "UPDATE patients SET processed = true WHERE id = :id",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "http-dest"},
		},
	}

	dbSrc := connector.NewDatabaseSource(chCfg.Listener.Database, testutil.DiscardLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: destServer.URL}, testutil.DiscardLogger())

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, dbSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	testutil.WaitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(capturedBodies), 1)

	var result map[string]any
	require.NoError(t, json.Unmarshal(capturedBodies[0], &result))
	assert.Equal(t, "db-to-http-test", result["channelId"])
	assert.Equal(t, "database", result["transport"])
}

func openPG(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", pgC.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func setupPatientsTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS patients (
		id SERIAL PRIMARY KEY,
		mrn VARCHAR(50) NOT NULL,
		first_name VARCHAR(100),
		last_name VARCHAR(100),
		processed BOOLEAN DEFAULT false,
		created_at TIMESTAMP DEFAULT NOW()
	)`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM patients`)
	require.NoError(t, err)
}

func setupAuditTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS audit_log (
		id SERIAL PRIMARY KEY,
		channel_id VARCHAR(100),
		message_body TEXT,
		created_at TIMESTAMP DEFAULT NOW()
	)`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM audit_log`)
	require.NoError(t, err)
}
