//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
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

// postPatientToFHIR creates a Patient on the FHIR server and returns the response body.
func postPatientToFHIR(t *testing.T, baseURL string, patient map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(patient)
	require.NoError(t, err)
	url := strings.TrimSuffix(baseURL, "/") + "/Patient"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/fhir+json")
	req.Header.Set("Accept", "application/fhir+json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "POST Patient should return 201")
	out, _ := json.Marshal(patient)
	return out
}

// TestFHIRPoll_AgainstGoServer uses the in-process Go FHIR server: post a Patient, then poll and verify we receive it.
func TestFHIRPoll_AgainstGoServer(t *testing.T) {
	fhirServer := testutil.NewFHIRTestServer()
	defer fhirServer.Close()
	baseURL := fhirServer.BaseURL()

	patient := map[string]any{
		"resourceType": "Patient",
		"id":           "poll-test-1",
		"name":         []any{map[string]any{"family": "Poll", "given": []any{"Test"}}},
	}
	postPatientToFHIR(t, baseURL, patient)

	var mu sync.Mutex
	var received [][]byte

	src := connector.NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:      baseURL,
		Resources:    []string{"Patient"},
		PollInterval: "500ms",
		PollRange:    "1h",
	}, testutil.DiscardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		mu.Lock()
		received = append(received, msg.Raw)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	defer src.Stop(context.Background())

	testutil.WaitFor(t, 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(received), 1)
	var got map[string]any
	require.NoError(t, json.Unmarshal(received[0], &got))
	assert.Equal(t, "Patient", got["resourceType"])
}

// TestFHIRPostThenPoll_AgainstGoServer uses FHIR dest to POST to the Go server, then fhir_poll to verify (same server: post and poll).
func TestFHIRPostThenPoll_AgainstGoServer(t *testing.T) {
	fhirServer := testutil.NewFHIRTestServer()
	defer fhirServer.Close()
	baseURL := fhirServer.BaseURL()

	// Post via our FHIR destination
	dest := connector.NewFHIRDest("hapi", &config.FHIRDestMapConfig{
		BaseURL:    baseURL,
		Version:    "R4",
		Operations: []string{"Patient:create"},
	}, testutil.DiscardLogger())
	defer dest.Stop(context.Background())

	patientJSON := []byte(`{"resourceType":"Patient","id":"postpoll-1","name":[{"family":"PostPoll","given":["Test"]}]}`)
	msg := message.New("", patientJSON)
	msg.Metadata["resource_type"] = "Patient"
	resp, err := dest.Send(context.Background(), msg)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "FHIR dest POST should succeed: %d", resp.StatusCode)

	// Now poll and verify we get it
	var mu sync.Mutex
	var received [][]byte

	src := connector.NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:      baseURL,
		Resources:    []string{"Patient"},
		PollInterval: "500ms",
		PollRange:    "1h",
	}, testutil.DiscardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	err = src.Start(ctx, func(ctx context.Context, m *message.Message) error {
		mu.Lock()
		received = append(received, m.Raw)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	defer src.Stop(context.Background())

	testutil.WaitFor(t, 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(received), 1)
	var got map[string]any
	require.NoError(t, json.Unmarshal(received[0], &got))
	assert.Equal(t, "Patient", got["resourceType"])
}

// TestFHIRSubscription_RestHook_AgainstGoServer creates a rest-hook subscription on the Go server, posts a resource, and verifies we receive the notification.
func TestFHIRSubscription_RestHook_AgainstGoServer(t *testing.T) {
	fhirServer := testutil.NewFHIRTestServer()
	defer fhirServer.Close()
	baseURL := fhirServer.BaseURL()

	var mu sync.Mutex
	var notifications [][]byte

	subSrc := connector.NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
		Path:        "/fhir/subscription-notification",
	}, testutil.DiscardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	err := subSrc.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		mu.Lock()
		notifications = append(notifications, msg.Raw)
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	defer subSrc.Stop(context.Background())

	time.Sleep(200 * time.Millisecond)
	addr := subSrc.Addr()
	require.NotEmpty(t, addr, "rest-hook should have listener address")
	port := addr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		port = addr[idx+1:]
	}
	// Same host (in-process Go server calling our listener)
	callbackURL := "http://127.0.0.1:" + port + "/fhir/subscription-notification"

	sub := map[string]any{
		"resourceType": "Subscription",
		"status":       "active",
		"criteria":     "Patient?name=SubTest",
		"channel": map[string]any{
			"type":     "rest-hook",
			"endpoint": callbackURL,
			"payload":  "application/fhir+json",
		},
	}
	subBody, err := json.Marshal(sub)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/Subscription", bytes.NewReader(subBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/fhir+json")
	req.Header.Set("Accept", "application/fhir+json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.True(t, resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK, "create Subscription: %d", resp.StatusCode)

	postPatientToFHIR(t, baseURL, map[string]any{
		"resourceType": "Patient",
		"name":         []any{map[string]any{"family": "SubTest", "given": []any{"Integration"}}},
	})

	testutil.WaitFor(t, 25*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(notifications) >= 1
	})

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(notifications), 1)
	var notif map[string]any
	require.NoError(t, json.Unmarshal(notifications[0], &notif))
	// Subscription notification payload may be SubscriptionStatus or a Bundle
	assert.True(t, notif["resourceType"] != nil, "notification should have resourceType")
}
