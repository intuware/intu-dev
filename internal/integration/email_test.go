//go:build integration

package integration

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/integration/testutil"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sendTestEmail delivers an email to the mail server via SMTP so the EmailSource
// can pick it up via IMAP on its next poll. Retries on EOF/connection errors in case SMTP is still starting.
func sendTestEmail(t *testing.T, subject, body string) {
	t.Helper()
	addr := greenmailC.SMTPAddr()
	msg := "Subject: " + subject + "\r\n" +
		"From: lab@hospital.com\r\n" +
		"To: testuser@localhost\r\n" +
		"\r\n" + body
	var err error
	for i := 0; i < 12; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * 500 * time.Millisecond)
		}
		err = smtp.SendMail(addr, nil, "lab@hospital.com", []string{"testuser@localhost"}, []byte(msg))
		if err == nil {
			return
		}
		retryable := err == io.EOF || strings.Contains(err.Error(), "EOF") ||
			strings.Contains(err.Error(), "connection reset") || strings.Contains(err.Error(), "connection refused")
		if i == 11 || !retryable {
			break
		}
	}
	require.NoError(t, err, "send test email to mail server")
}

// TestEmailSource_ReceivesViaIMAP sends an email into GreenMail via SMTP,
// then verifies the EmailSource polls it via IMAP.
func TestEmailSource_ReceivesViaIMAP(t *testing.T) {
	if greenmailC == nil {
		t.Skip("GreenMail container not available")
	}

	sendTestEmail(t, "HL7 Alert", "Patient John Doe: Critical potassium level 6.8 mEq/L")

	var mu sync.Mutex
	var received [][]byte

	src := connector.NewEmailSource(&config.EmailListener{
		Protocol:     "pop3",
		Host:         greenmailC.Host,
		Port:         greenmailC.IMAPPort,
		PollInterval: "500ms",
		Auth: &config.AuthConfig{
			Type:     "password",
			Username: "testuser",
			Password: "testpass",
		},
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
	require.GreaterOrEqual(t, len(received), 1)
	assert.Contains(t, string(received[0]), "Critical potassium level")
}

// TestEmailSource_ToHTTPDest tests the full pipeline: GreenMail IMAP ->
// transformer -> HTTP destination.
func TestEmailSource_ToHTTPDest(t *testing.T) {
	if greenmailC == nil {
		t.Skip("GreenMail container not available")
	}

	sendTestEmail(t, "ADT Notification", "ADT^A01 - Patient Smith admitted to ICU")

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
	testutil.WriteJS(t, channelDir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return {
		body: {
			source: msg.transport,
			email_content: msg.body,
			channelId: ctx.channelId
		}
	};
};`)

	chCfg := &config.ChannelConfig{
		ID:      "email-to-http",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Transformer: "transformer.js",
		},
		Listener: config.ListenerConfig{
			Type: "email",
			Email: &config.EmailListener{
				Protocol:     "pop3",
				Host:         greenmailC.Host,
				Port:         greenmailC.IMAPPort,
				PollInterval: "500ms",
				Auth: &config.AuthConfig{
					Type:     "password",
					Username: "testuser",
					Password: "testpass",
				},
			},
		},
		Destinations: []config.ChannelDestination{
			{Name: "http-dest"},
		},
	}

	emailSrc := connector.NewEmailSource(chCfg.Listener.Email, testutil.DiscardLogger())
	httpDest := connector.NewHTTPDest("http-dest", &config.HTTPDestConfig{URL: destServer.URL}, testutil.DiscardLogger())

	cr := buildIntegrationChannelRuntime(t, chCfg.ID, chCfg, emailSrc, map[string]connector.DestinationConnector{
		"http-dest": httpDest,
	}, channelDir)

	ctx := context.Background()
	require.NoError(t, cr.Start(ctx))
	defer cr.Stop(ctx)

	testutil.WaitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(capturedBodies) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(capturedBodies), 1)
	assert.Contains(t, string(capturedBodies[0]), "email")
	assert.Contains(t, string(capturedBodies[0]), "email-to-http")
}
