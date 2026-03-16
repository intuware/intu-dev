//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

func TestSFTPSource_PollsAndReceives(t *testing.T) {
	if sftpC == nil {
		t.Skip("SFTP container not available")
	}

	sftpClient := dialSFTP(t)
	defer sftpClient.Close()

	sftpClient.MkdirAll("/upload/inbound")
	sftpClient.MkdirAll("/upload/processed")

	f, err := sftpClient.Create("/upload/inbound/test.dat")
	require.NoError(t, err)
	f.Write([]byte(`{"patient":"John Doe"}`))
	f.Close()

	var mu sync.Mutex
	var received [][]byte

	src := connector.NewSFTPSource(&config.SFTPListener{
		Host:         sftpC.Host,
		Port:         sftpC.Port,
		Directory:    "/upload/inbound",
		FilePattern:  "*.dat",
		PollInterval: "500ms",
		MoveTo:       "/upload/processed",
		Auth: &config.AuthConfig{
			Type:     "password",
			Username: sftpC.User,
			Password: sftpC.Password,
		},
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
		return len(received) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(received), 1)
	assert.Contains(t, string(received[0]), "John Doe")

	entries, err := sftpClient.ReadDir("/upload/processed")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 1)
}

func TestSFTPDest_WritesFiles(t *testing.T) {
	if sftpC == nil {
		t.Skip("SFTP container not available")
	}

	sftpClient := dialSFTP(t)
	defer sftpClient.Close()

	sftpClient.MkdirAll("/upload/outbound")

	dest := connector.NewSFTPDest("sftp-dest", &config.SFTPDestMapConfig{
		Host:            sftpC.Host,
		Port:            sftpC.Port,
		Directory:       "/upload/outbound",
		FilenamePattern: "msg_{{messageId}}.json",
		Auth: &config.HTTPAuthConfig{
			Username: sftpC.User,
			Password: sftpC.Password,
		},
	}, testutil.DiscardLogger())

	msg := message.New("test-channel", []byte(`{"result":"success"}`))
	_, err := dest.Send(context.Background(), msg)
	require.NoError(t, err)

	entries, err := sftpClient.ReadDir("/upload/outbound")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 1, "expected at least 1 file in outbound directory")

	if len(entries) > 0 {
		f, err := sftpClient.Open(fmt.Sprintf("/upload/outbound/%s", entries[0].Name()))
		require.NoError(t, err)
		data, _ := io.ReadAll(f)
		f.Close()
		assert.Contains(t, string(data), "success")
	}
}

func TestSFTPSourceToHTTPDest_Pipeline(t *testing.T) {
	if sftpC == nil {
		t.Skip("SFTP container not available")
	}

	sftpClient := dialSFTP(t)
	defer sftpClient.Close()

	sftpClient.MkdirAll("/upload/pipeline-in")
	sftpClient.MkdirAll("/upload/pipeline-done")

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
		ID:      "sftp-to-http-test",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "sftp",
			SFTP: &config.SFTPListener{
				Host:         sftpC.Host,
				Port:         sftpC.Port,
				Directory:    "/upload/pipeline-in",
				FilePattern:  "*.json",
				PollInterval: "500ms",
				MoveTo:       "/upload/pipeline-done",
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

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, sftpSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	f, err := sftpClient.Create("/upload/pipeline-in/patient.json")
	require.NoError(t, err)
	f.Write([]byte(`{"patient":"Jane Smith","mrn":"MRN002"}`))
	f.Close()

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
	assert.Equal(t, "sftp-to-http-test", result["channelId"])
	assert.Equal(t, "sftp", result["transport"])
}

func dialSFTP(t *testing.T) *sftp.Client {
	t.Helper()
	sshCfg := &ssh.ClientConfig{
		User:            sftpC.User,
		Auth:            []ssh.AuthMethod{ssh.Password(sftpC.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", sftpC.Host, sftpC.Port)
	sshClient, err := ssh.Dial("tcp", addr, sshCfg)
	require.NoError(t, err)
	t.Cleanup(func() { sshClient.Close() })

	client, err := sftp.NewClient(sshClient)
	require.NoError(t, err)
	return client
}
