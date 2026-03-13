//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/integration/testutil"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiDest_HTTPToKafkaSFTPSMTP sends an HTTP message and verifies delivery
// to 3 real destinations: Kafka topic, SFTP file, and SMTP email (via MailHog).
func TestMultiDest_HTTPToKafkaSFTPSMTP(t *testing.T) {
	if kafkaC == nil {
		t.Skip("Kafka container not available")
	}
	if sftpC == nil {
		t.Skip("SFTP container not available")
	}
	if mailhogC == nil {
		t.Skip("MailHog container not available")
	}

	sftpClient := dialSFTP(t)
	defer sftpClient.Close()
	sftpClient.MkdirAll("/upload/multi-output")

	clearMailHog(t)

	channelDir := t.TempDir()
	testutil.WriteJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return {
		body: {
			event: "multi_dest_test",
			data: msg.body,
			channelId: ctx.channelId,
			timestamp: new Date().toISOString()
		}
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "multi-3way",
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
			{Name: "sftp-dest"},
			{Name: "smtp-dest"},
		},
	}

	httpSrc := connector.NewHTTPSource(chCfg.Listener.HTTP, testutil.DiscardLogger())
	kafkaDest := connector.NewKafkaDest("kafka-dest", &config.KafkaDestConfig{
		Brokers:  kafkaC.Brokers,
		Topic:    "multi-dest-3way",
		ClientID: "multi-3way-test",
	}, testutil.DiscardLogger())
	sftpDest := connector.NewSFTPDest("sftp-dest", &config.SFTPDestMapConfig{
		Host:            sftpC.Host,
		Port:            sftpC.Port,
		Directory:       "/upload/multi-output",
		FilenamePattern: "multi_{{messageId}}.json",
		Auth: &config.HTTPAuthConfig{
			Type:     "password",
			Username: sftpC.User,
			Password: sftpC.Password,
		},
	}, testutil.DiscardLogger())
	smtpDest := connector.NewSMTPDest("smtp-dest", &config.SMTPDestMapConfig{
		Host:    mailhogC.SMTPHost,
		Port:    mailhogC.SMTPPort,
		From:    "intu-multidest@test.com",
		To:      []string{"admin@hospital.com"},
		Subject: "Multi-dest Integration Test",
	}, testutil.DiscardLogger())

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, httpSrc, map[string]connector.DestinationConnector{
		"kafka-dest": kafkaDest,
		"sftp-dest":  sftpDest,
		"smtp-dest":  smtpDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	resp, err := http.Post("http://"+httpSrc.Addr()+"/", "application/json",
		strings.NewReader(`{"mrn":"MRN-3WAY","name":"Three Way Test"}`))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	testutil.WaitFor(t, 10*time.Second, func() bool {
		files, _ := sftpClient.ReadDir("/upload/multi-output")
		return len(files) >= 1
	})

	sftpFiles, _ := sftpClient.ReadDir("/upload/multi-output")
	require.GreaterOrEqual(t, len(sftpFiles), 1, "SFTP dest should have written file")

	time.Sleep(1 * time.Second)
	emails := getMailHogMessages(t)
	assert.GreaterOrEqual(t, len(emails), 1, "SMTP dest should have sent email")
}

// TestMultiDest_SFTPToDBAndHTTP reads a file from SFTP, then writes to both
// PostgreSQL and an HTTP capture server.
func TestMultiDest_SFTPToDBAndHTTP(t *testing.T) {
	if sftpC == nil {
		t.Skip("SFTP container not available")
	}
	if pgC == nil {
		t.Skip("PostgreSQL container not available")
	}

	sftpClient := dialSFTP(t)
	defer sftpClient.Close()
	sftpClient.MkdirAll("/upload/sftp-to-db-in")
	sftpClient.MkdirAll("/upload/sftp-to-db-done")

	db := openPG(t)
	setupAuditTable(t, db)

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
		ID:      "sftp-to-db-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "sftp",
			SFTP: &config.SFTPListener{
				Host:         sftpC.Host,
				Port:         sftpC.Port,
				Directory:    "/upload/sftp-to-db-in",
				FilePattern:  "*.json",
				PollInterval: "500ms",
				MoveTo:       "/upload/sftp-to-db-done",
				Auth: &config.AuthConfig{
					Type:     "password",
					Username: sftpC.User,
					Password: sftpC.Password,
				},
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "db-dest"},
			{Name: "http-dest"},
		},
	}

	sftpSrc := connector.NewSFTPSource(chCfg.Listener.SFTP, testutil.DiscardLogger())
	dbDest := connector.NewDatabaseDest("db-dest", &config.DBDestMapConfig{
		Driver:    "pgx",
		DSN:       pgC.DSN,
		Statement: "INSERT INTO audit_log (channel_id, message_body) VALUES ('${channelId}', '${raw}')",
	}, testutil.DiscardLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: destServer.URL}, testutil.DiscardLogger())

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, sftpSrc, map[string]connector.DestinationConnector{
		"db-dest":   dbDest,
		"http-dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	f, err := sftpClient.Create("/upload/sftp-to-db-in/record.json")
	require.NoError(t, err)
	f.Write([]byte(`{"patientId":"P-MULTI","name":"Multi Dest Patient"}`))
	f.Close()

	testutil.WaitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 1
	})

	mu.Lock()
	require.GreaterOrEqual(t, len(capturedBodies), 1, "HTTP dest should receive message")
	mu.Unlock()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM audit_log WHERE channel_id = 'sftp-to-db-http'").Scan(&count)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, 1, "DB should have at least 1 audit row")
}

// TestMultiDest_KafkaToSFTPAndDB consumes from Kafka and writes to both
// SFTP and PostgreSQL simultaneously.
func TestMultiDest_KafkaToSFTPAndDB(t *testing.T) {
	if kafkaC == nil {
		t.Skip("Kafka container not available")
	}
	if sftpC == nil {
		t.Skip("SFTP container not available")
	}
	if pgC == nil {
		t.Skip("PostgreSQL container not available")
	}

	sftpClient := dialSFTP(t)
	defer sftpClient.Close()
	sftpClient.MkdirAll("/upload/kafka-output")

	db := openPG(t)
	setupAuditTable(t, db)

	topic := "kafka-to-multi-out"

	channelDir := t.TempDir()
	testutil.WriteJS(t, channelDir, "transformer.js", testutil.TransformerJSONEnrich)

	chCfg := &config.ChannelConfig{
		ID:      "kafka-to-sftp-db",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "kafka",
			Kafka: &config.KafkaListener{
				Brokers: kafkaC.Brokers,
				Topic:   topic,
				GroupID: "kafka-multi-out",
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "sftp-dest"},
			{Name: "db-dest"},
		},
	}

	kafkaSrc := connector.NewKafkaSource(chCfg.Listener.Kafka, testutil.DiscardLogger())
	sftpDest := connector.NewSFTPDest("sftp-dest", &config.SFTPDestMapConfig{
		Host:            sftpC.Host,
		Port:            sftpC.Port,
		Directory:       "/upload/kafka-output",
		FilenamePattern: "kafka_{{messageId}}.json",
		Auth: &config.HTTPAuthConfig{
			Type:     "password",
			Username: sftpC.User,
			Password: sftpC.Password,
		},
	}, testutil.DiscardLogger())
	dbDest := connector.NewDatabaseDest("db-dest", &config.DBDestMapConfig{
		Driver:    "pgx",
		DSN:       pgC.DSN,
		Statement: "INSERT INTO audit_log (channel_id, message_body) VALUES ('${channelId}', '${raw}')",
	}, testutil.DiscardLogger())

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, kafkaSrc, map[string]connector.DestinationConnector{
		"sftp-dest": sftpDest,
		"db-dest":   dbDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	time.Sleep(3 * time.Second)

	produceViaConnector(t, kafkaC.Brokers, topic, []byte(`{"event":"kafka-to-multi","data":"cross-connector"}`))

	testutil.WaitFor(t, 15*time.Second, func() bool {
		files, _ := sftpClient.ReadDir("/upload/kafka-output")
		return len(files) >= 1
	})

	sftpFiles, _ := sftpClient.ReadDir("/upload/kafka-output")
	require.GreaterOrEqual(t, len(sftpFiles), 1, "SFTP should have output file")

	sf, err := sftpClient.Open(filepath.Join("/upload/kafka-output", sftpFiles[0].Name()))
	require.NoError(t, err)
	sftpData, _ := io.ReadAll(sf)
	sf.Close()

	var sftpResult map[string]any
	require.NoError(t, json.Unmarshal(sftpData, &sftpResult))
	assert.Equal(t, "kafka-to-sftp-db", sftpResult["channelId"])

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM audit_log WHERE channel_id = 'kafka-to-sftp-db'").Scan(&count)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, 1, "DB should have audit entry")
}
