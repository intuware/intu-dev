package config

import (
	"strings"
	"testing"
)

func httpChannel(id string, port int, path string) *ChannelConfig {
	return &ChannelConfig{
		ID:      id,
		Enabled: true,
		Listener: ListenerConfig{
			Type: "http",
			HTTP: &HTTPListener{Port: port, Path: path},
		},
	}
}

func TestValidateListenerEndpoints_NoDuplicates(t *testing.T) {
	channels := []*ChannelConfig{
		httpChannel("ch-a", 8081, "/patients"),
		httpChannel("ch-b", 8081, "/orders"),
		httpChannel("ch-c", 8082, "/patients"),
	}

	errs := ValidateListenerEndpoints(channels)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got: %v", errs)
	}
}

func TestValidateListenerEndpoints_DuplicateHTTP(t *testing.T) {
	channels := []*ChannelConfig{
		httpChannel("fhir-to-hl7", 8081, "/fhir/r4/Patient"),
		httpChannel("sample-channel", 8081, "/fhir/r4/Patient"),
	}

	errs := ValidateListenerEndpoints(channels)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
	if !strings.Contains(errs[0], "fhir-to-hl7") || !strings.Contains(errs[0], "sample-channel") {
		t.Fatalf("error should name both channels, got: %s", errs[0])
	}
	if !strings.Contains(errs[0], "8081") || !strings.Contains(errs[0], "/fhir/r4/Patient") {
		t.Fatalf("error should mention port and path, got: %s", errs[0])
	}
}

func TestValidateListenerEndpoints_DefaultPathCollision(t *testing.T) {
	ch1 := httpChannel("ch-1", 8080, "/")
	ch2 := httpChannel("ch-2", 8080, "")

	errs := ValidateListenerEndpoints([]*ChannelConfig{ch1, ch2})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error for default-path collision, got %d: %v", len(errs), errs)
	}
}

func TestValidateListenerEndpoints_DisabledChannelSkipped(t *testing.T) {
	ch1 := httpChannel("active", 8081, "/same")
	ch2 := httpChannel("disabled", 8081, "/same")
	ch2.Enabled = false

	errs := ValidateListenerEndpoints([]*ChannelConfig{ch1, ch2})
	if len(errs) != 0 {
		t.Fatalf("disabled channel should be skipped, got: %v", errs)
	}
}

func TestValidateListenerEndpoints_NonHTTPSkipped(t *testing.T) {
	ch1 := httpChannel("http-ch", 8081, "/data")
	ch2 := &ChannelConfig{
		ID:      "file-ch",
		Enabled: true,
		Listener: ListenerConfig{
			Type: "file",
			File: &FileListener{Directory: "/tmp/input"},
		},
	}

	errs := ValidateListenerEndpoints([]*ChannelConfig{ch1, ch2})
	if len(errs) != 0 {
		t.Fatalf("file listener should be skipped, got: %v", errs)
	}
}

func TestValidateListenerEndpoints_CrossTypeDuplicate(t *testing.T) {
	ch1 := httpChannel("http-ch", 9090, "/api")
	ch2 := &ChannelConfig{
		ID:      "fhir-ch",
		Enabled: true,
		Listener: ListenerConfig{
			Type: "fhir",
			FHIR: &FHIRListener{Port: 9090, BasePath: "/api"},
		},
	}

	errs := ValidateListenerEndpoints([]*ChannelConfig{ch1, ch2})
	if len(errs) != 1 {
		t.Fatalf("expected 1 cross-type duplicate error, got %d: %v", len(errs), errs)
	}
}

func TestValidateListenerEndpoints_ThreeWayDuplicate(t *testing.T) {
	channels := []*ChannelConfig{
		httpChannel("ch-1", 8081, "/dup"),
		httpChannel("ch-2", 8081, "/dup"),
		httpChannel("ch-3", 8081, "/dup"),
	}

	errs := ValidateListenerEndpoints(channels)
	if len(errs) != 2 {
		t.Fatalf("expected 2 errors for 3-way collision, got %d: %v", len(errs), errs)
	}
}

func TestListenerEndpoint_EmptyPathDefaultsToSlash(t *testing.T) {
	ch := httpChannel("test", 8080, "")
	port, path := ListenerEndpoint(ch)
	if port != 8080 || path != "/" {
		t.Fatalf("expected 8080 /, got %d %s", port, path)
	}
}
