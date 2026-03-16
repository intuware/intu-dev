//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/integration/testutil"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSMTPDest_SendsEmail(t *testing.T) {
	if mailhogC == nil {
		t.Skip("MailHog container not available")
	}

	clearMailHog(t)

	dest := connector.NewSMTPDest("smtp-dest", &config.SMTPDestMapConfig{
		Host:    mailhogC.SMTPHost,
		Port:    mailhogC.SMTPPort,
		From:    "intu@example.com",
		To:      []string{"doctor@hospital.com"},
		Subject: "HL7 Alert: ADT^A01",
	}, testutil.DiscardLogger())

	msg := message.New("test-channel", []byte("Patient John Doe admitted to ICU"))
	_, err := dest.Send(context.Background(), msg)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	emails := getMailHogMessages(t)
	require.GreaterOrEqual(t, len(emails), 1, "expected at least 1 email in MailHog")

	lastEmail := emails[len(emails)-1]
	headers, _ := lastEmail["Content"].(map[string]any)["Headers"].(map[string]any)
	subject, _ := headers["Subject"].([]any)
	if len(subject) > 0 {
		assert.Contains(t, subject[0], "HL7 Alert")
	}
}

func TestSMTPDest_MultipleRecipients(t *testing.T) {
	if mailhogC == nil {
		t.Skip("MailHog container not available")
	}

	clearMailHog(t)

	dest := connector.NewSMTPDest("smtp-multi", &config.SMTPDestMapConfig{
		Host:    mailhogC.SMTPHost,
		Port:    mailhogC.SMTPPort,
		From:    "alerts@intu.io",
		To:      []string{"nurse@hospital.com", "admin@hospital.com"},
		Subject: "Critical Lab Result",
	}, testutil.DiscardLogger())

	msg := message.New("lab-channel", []byte("Critical potassium level: 6.8 mEq/L"))
	_, err := dest.Send(context.Background(), msg)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	emails := getMailHogMessages(t)
	assert.GreaterOrEqual(t, len(emails), 1)
}

func getMailHogMessages(t *testing.T) []map[string]any {
	t.Helper()
	apiURL := fmt.Sprintf("http://%s:%d/api/v2/messages", mailhogC.APIHost, mailhogC.APIPort)
	resp, err := http.Get(apiURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(body, &result))

	items, ok := result["items"].([]any)
	if !ok {
		return nil
	}

	var emails []map[string]any
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			emails = append(emails, m)
		}
	}
	return emails
}

func clearMailHog(t *testing.T) {
	t.Helper()
	apiURL := fmt.Sprintf("http://%s:%d/api/v1/messages", mailhogC.APIHost, mailhogC.APIPort)
	req, _ := http.NewRequest("DELETE", apiURL, nil)
	http.DefaultClient.Do(req)
}
