package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// --- ListenerEndpoint ---

func TestListenerEndpoint_HTTPWithPath(t *testing.T) {
	ch := httpChannel("ch-http", 8080, "/patients")
	port, path := ListenerEndpoint(ch)
	if port != 8080 || path != "/patients" {
		t.Fatalf("expected 8080 /patients, got %d %s", port, path)
	}
}

func TestListenerEndpoint_SOAPListener(t *testing.T) {
	ch := &ChannelConfig{
		ID: "soap-ch",
		Listener: ListenerConfig{
			Type: "soap",
			SOAP: &SOAPListener{Port: 8443, WSDLPath: "/service"},
		},
	}
	port, path := ListenerEndpoint(ch)
	if port != 8443 || path != "/service" {
		t.Fatalf("expected 8443 /service, got %d %q", port, path)
	}
}

func TestListenerEndpoint_SOAPListenerEmptyPath(t *testing.T) {
	ch := &ChannelConfig{
		ID: "soap-ch2",
		Listener: ListenerConfig{
			Type: "soap",
			SOAP: &SOAPListener{Port: 8443},
		},
	}
	port, path := ListenerEndpoint(ch)
	if port != 8443 || path != "/" {
		t.Fatalf("expected 8443 /, got %d %q", port, path)
	}
}

func TestListenerEndpoint_FHIRListener(t *testing.T) {
	ch := &ChannelConfig{
		ID: "fhir-ch",
		Listener: ListenerConfig{
			Type: "fhir",
			FHIR: &FHIRListener{Port: 9090, BasePath: "/fhir/r4"},
		},
	}
	port, path := ListenerEndpoint(ch)
	if port != 9090 || path != "/fhir/r4" {
		t.Fatalf("expected 9090 /fhir/r4, got %d %q", port, path)
	}
}

func TestListenerEndpoint_FHIRListenerEmptyPath(t *testing.T) {
	ch := &ChannelConfig{
		ID: "fhir-ch2",
		Listener: ListenerConfig{
			Type: "fhir",
			FHIR: &FHIRListener{Port: 9090},
		},
	}
	port, path := ListenerEndpoint(ch)
	if port != 9090 || path != "/" {
		t.Fatalf("expected 9090 /, got %d %q", port, path)
	}
}

func TestListenerEndpoint_IHEListener(t *testing.T) {
	ch := &ChannelConfig{
		ID: "ihe-ch",
		Listener: ListenerConfig{
			Type: "ihe",
			IHE:  &IHEListener{Port: 3600, Profile: "XDS.b"},
		},
	}
	port, path := ListenerEndpoint(ch)
	if port != 3600 || path != "/XDS.b" {
		t.Fatalf("expected 3600 /XDS.b, got %d %q", port, path)
	}
}

func TestListenerEndpoint_DefaultTypes(t *testing.T) {
	for _, ltype := range []string{"tcp", "file", "database", "kafka", "channel", "email", "dicom", "sftp"} {
		ch := &ChannelConfig{
			ID:       ltype + "-ch",
			Listener: ListenerConfig{Type: ltype},
		}
		port, path := ListenerEndpoint(ch)
		if port != 0 || path != "" {
			t.Fatalf("type %s: expected 0,\"\", got %d %q", ltype, port, path)
		}
	}
}

func TestListenerEndpoint_NilListenerConfig(t *testing.T) {
	ch := &ChannelConfig{
		ID:       "nil-http",
		Listener: ListenerConfig{Type: "http"},
	}
	port, path := ListenerEndpoint(ch)
	if port != 0 || path != "/" {
		t.Fatalf("expected 0 / for nil HTTP config, got %d %q", port, path)
	}
}

func TestListenerEndpoint_NilSOAP(t *testing.T) {
	ch := &ChannelConfig{
		ID:       "nil-soap",
		Listener: ListenerConfig{Type: "soap"},
	}
	port, path := ListenerEndpoint(ch)
	if port != 0 || path != "/" {
		t.Fatalf("expected 0 / for nil SOAP config, got %d %q", port, path)
	}
}

func TestListenerEndpoint_NilFHIR(t *testing.T) {
	ch := &ChannelConfig{
		ID:       "nil-fhir",
		Listener: ListenerConfig{Type: "fhir"},
	}
	port, path := ListenerEndpoint(ch)
	if port != 0 || path != "/" {
		t.Fatalf("expected 0 / for nil FHIR config, got %d %q", port, path)
	}
}

func TestListenerEndpoint_NilIHE(t *testing.T) {
	ch := &ChannelConfig{
		ID:       "nil-ihe",
		Listener: ListenerConfig{Type: "ihe"},
	}
	port, path := ListenerEndpoint(ch)
	if port != 0 || path != "/" {
		t.Fatalf("expected 0 / for nil IHE config, got %d %q", port, path)
	}
}

func TestListenerEndpoint_FHIRSubscriptionNilConfig(t *testing.T) {
	ch := &ChannelConfig{
		ID:       "nil-fhir-sub",
		Listener: ListenerConfig{Type: "fhir_subscription"},
	}
	port, path := ListenerEndpoint(ch)
	if port != 0 || path != "" {
		t.Fatalf("expected 0,\"\" for nil fhir_subscription, got %d %q", port, path)
	}
}

// --- ValidateListenerEndpoints ---

func TestValidateListenerEndpoints_SOAPDuplicate(t *testing.T) {
	ch1 := &ChannelConfig{
		ID: "soap-1", Enabled: true,
		Listener: ListenerConfig{Type: "soap", SOAP: &SOAPListener{Port: 8443, WSDLPath: "/svc"}},
	}
	ch2 := &ChannelConfig{
		ID: "soap-2", Enabled: true,
		Listener: ListenerConfig{Type: "soap", SOAP: &SOAPListener{Port: 8443, WSDLPath: "/svc"}},
	}
	errs := ValidateListenerEndpoints([]*ChannelConfig{ch1, ch2})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error for SOAP duplicate, got %d: %v", len(errs), errs)
	}
}

func TestValidateListenerEndpoints_EmptyList(t *testing.T) {
	errs := ValidateListenerEndpoints(nil)
	if len(errs) != 0 {
		t.Fatalf("expected no errors for nil, got %v", errs)
	}
}

func TestValidateListenerEndpoints_IHEDuplicate(t *testing.T) {
	ch1 := &ChannelConfig{
		ID: "ihe-1", Enabled: true,
		Listener: ListenerConfig{Type: "ihe", IHE: &IHEListener{Port: 3600, Profile: "XDS.b"}},
	}
	ch2 := &ChannelConfig{
		ID: "ihe-2", Enabled: true,
		Listener: ListenerConfig{Type: "ihe", IHE: &IHEListener{Port: 3600, Profile: "XDS.b"}},
	}
	errs := ValidateListenerEndpoints([]*ChannelConfig{ch1, ch2})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error for IHE duplicate, got %d: %v", len(errs), errs)
	}
}

// --- LoadChannelConfig ---

func TestLoadChannelConfig_Basic(t *testing.T) {
	dir := t.TempDir()
	yamlContent := `id: test-channel
enabled: true
description: A test
listener:
  type: http
  http:
    port: 8080
    path: /test
destinations:
  - name: dest-1
    type: http
    http:
      url: http://localhost:9090
`
	os.WriteFile(filepath.Join(dir, "channel.yaml"), []byte(yamlContent), 0o644)

	cfg, err := LoadChannelConfig(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ID != "test-channel" {
		t.Fatalf("expected id=test-channel, got %s", cfg.ID)
	}
	if !cfg.Enabled {
		t.Fatal("expected enabled=true")
	}
	if cfg.Listener.Type != "http" {
		t.Fatalf("expected listener type=http, got %s", cfg.Listener.Type)
	}
	if cfg.Listener.HTTP == nil || cfg.Listener.HTTP.Port != 8080 {
		t.Fatal("expected HTTP listener with port 8080")
	}
}

func TestLoadChannelConfig_EnvVarExpansion(t *testing.T) {
	dir := t.TempDir()
	os.Setenv("TEST_PORT_VAL", "9999")
	defer os.Unsetenv("TEST_PORT_VAL")

	yamlContent := `id: env-channel
enabled: true
listener:
  type: http
  http:
    port: $TEST_PORT_VAL
`
	os.WriteFile(filepath.Join(dir, "channel.yaml"), []byte(yamlContent), 0o644)

	cfg, err := LoadChannelConfig(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Listener.HTTP == nil || cfg.Listener.HTTP.Port != 9999 {
		t.Fatalf("expected port 9999 from env var, got %v", cfg.Listener.HTTP)
	}
}

func TestLoadChannelConfig_MissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadChannelConfig(dir)
	if err == nil {
		t.Fatal("expected error for missing channel.yaml")
	}
}

func TestLoadChannelConfig_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "channel.yaml"), []byte("id: test\nenabled: [invalid\n"), 0o644)

	_, err := LoadChannelConfig(dir)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

// --- DiscoverChannelDirs ---

func TestDiscoverChannelDirs_Basic(t *testing.T) {
	dir := t.TempDir()

	ch1 := filepath.Join(dir, "ch-a")
	ch2 := filepath.Join(dir, "ch-b")
	os.MkdirAll(ch1, 0o755)
	os.MkdirAll(ch2, 0o755)
	os.WriteFile(filepath.Join(ch1, "channel.yaml"), []byte("id: ch-a"), 0o644)
	os.WriteFile(filepath.Join(ch2, "channel.yaml"), []byte("id: ch-b"), 0o644)

	dirs, err := DiscoverChannelDirs(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dirs) != 2 {
		t.Fatalf("expected 2 dirs, got %d", len(dirs))
	}
}

func TestDiscoverChannelDirs_Nested(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "group", "subgroup", "ch-nested")
	os.MkdirAll(nested, 0o755)
	os.WriteFile(filepath.Join(nested, "channel.yaml"), []byte("id: nested"), 0o644)

	dirs, err := DiscoverChannelDirs(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dirs) != 1 {
		t.Fatalf("expected 1 nested dir, got %d", len(dirs))
	}
	if dirs[0] != nested {
		t.Fatalf("expected %s, got %s", nested, dirs[0])
	}
}

func TestDiscoverChannelDirs_Empty(t *testing.T) {
	dir := t.TempDir()
	dirs, err := DiscoverChannelDirs(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dirs) != 0 {
		t.Fatalf("expected 0 dirs, got %d", len(dirs))
	}
}

func TestDiscoverChannelDirs_NonexistentDir(t *testing.T) {
	dirs, err := DiscoverChannelDirs("/tmp/nonexistent-dir-for-test-xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dirs) != 0 {
		t.Fatalf("expected 0 dirs, got %d", len(dirs))
	}
}

// --- FindChannelDir ---

func TestFindChannelDir_Found(t *testing.T) {
	dir := t.TempDir()
	chDir := filepath.Join(dir, "my-channel")
	os.MkdirAll(chDir, 0o755)
	os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte("id: my-channel\nenabled: true\nlistener:\n  type: http\n"), 0o644)

	result, err := FindChannelDir(dir, "my-channel")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != chDir {
		t.Fatalf("expected %s, got %s", chDir, result)
	}
}

func TestFindChannelDir_NotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := FindChannelDir(dir, "nonexistent")
	if err == nil {
		t.Fatal("expected error for not-found channel")
	}
}

func TestFindChannelDir_SkipsBadYAML(t *testing.T) {
	dir := t.TempDir()
	badDir := filepath.Join(dir, "bad")
	goodDir := filepath.Join(dir, "good")
	os.MkdirAll(badDir, 0o755)
	os.MkdirAll(goodDir, 0o755)
	os.WriteFile(filepath.Join(badDir, "channel.yaml"), []byte(":::invalid:::"), 0o644)
	os.WriteFile(filepath.Join(goodDir, "channel.yaml"), []byte("id: good-ch\nenabled: true\nlistener:\n  type: http\n"), 0o644)

	result, err := FindChannelDir(dir, "good-ch")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != goodDir {
		t.Fatalf("expected %s, got %s", goodDir, result)
	}
}

// --- ChannelDestination.UnmarshalYAML ---

func TestChannelDestination_UnmarshalYAML_Scalar(t *testing.T) {
	yamlData := `- my-dest-ref`
	var dests []ChannelDestination
	if err := yaml.Unmarshal([]byte(yamlData), &dests); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dests) != 1 {
		t.Fatalf("expected 1 dest, got %d", len(dests))
	}
	if dests[0].Name != "my-dest-ref" {
		t.Fatalf("expected name=my-dest-ref, got %q", dests[0].Name)
	}
	if dests[0].Ref != "my-dest-ref" {
		t.Fatalf("expected ref=my-dest-ref, got %q", dests[0].Ref)
	}
}

func TestChannelDestination_UnmarshalYAML_NamedKeyMapping(t *testing.T) {
	yamlData := `- my-file-dest:
    type: file
    file:
      directory: /output
`
	var dests []ChannelDestination
	if err := yaml.Unmarshal([]byte(yamlData), &dests); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dests) != 1 {
		t.Fatalf("expected 1 dest, got %d", len(dests))
	}
	if dests[0].Name != "my-file-dest" {
		t.Fatalf("expected name=my-file-dest, got %q", dests[0].Name)
	}
	if dests[0].Type != "file" {
		t.Fatalf("expected type=file, got %q", dests[0].Type)
	}
	if dests[0].File == nil || dests[0].File.Directory != "/output" {
		t.Fatalf("expected file directory=/output, got %v", dests[0].File)
	}
}

func TestChannelDestination_UnmarshalYAML_StandardMapping(t *testing.T) {
	yamlData := `- name: std-dest
  type: http
  http:
    url: http://example.com
    method: POST
`
	var dests []ChannelDestination
	if err := yaml.Unmarshal([]byte(yamlData), &dests); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dests) != 1 {
		t.Fatalf("expected 1 dest, got %d", len(dests))
	}
	if dests[0].Name != "std-dest" {
		t.Fatalf("expected name=std-dest, got %q", dests[0].Name)
	}
	if dests[0].Type != "http" {
		t.Fatalf("expected type=http, got %q", dests[0].Type)
	}
	if dests[0].HTTP == nil || dests[0].HTTP.URL != "http://example.com" {
		t.Fatal("expected http url")
	}
}

func TestChannelDestination_UnmarshalYAML_MixedNamedKey(t *testing.T) {
	yamlData := `- custom-name:
  type: tcp
  tcp:
    host: 10.0.0.1
    port: 2575
`
	var dests []ChannelDestination
	if err := yaml.Unmarshal([]byte(yamlData), &dests); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dests[0].Name != "custom-name" {
		t.Fatalf("expected name=custom-name, got %q", dests[0].Name)
	}
}

// --- ToDestination ---

func TestToDestination_HTTP(t *testing.T) {
	cd := &ChannelDestination{
		Type: "http",
		HTTP: &HTTPDestConfig{URL: "http://dest.com", Method: "POST"},
	}
	dest := cd.ToDestination()
	if dest.Type != "http" {
		t.Fatalf("expected type=http, got %s", dest.Type)
	}
	if dest.HTTP == nil || dest.HTTP.URL != "http://dest.com" {
		t.Fatal("expected HTTP config")
	}
}

func TestToDestination_TCP(t *testing.T) {
	cd := &ChannelDestination{
		Type: "tcp",
		TCP:  &TCPDestConfig{Host: "10.0.0.1", Port: 2575, Mode: "mllp", TimeoutMs: 5000},
	}
	dest := cd.ToDestination()
	if dest.TCP == nil {
		t.Fatal("expected TCP config")
	}
	if dest.TCP.Host != "10.0.0.1" || dest.TCP.Port != 2575 {
		t.Fatalf("expected 10.0.0.1:2575, got %s:%d", dest.TCP.Host, dest.TCP.Port)
	}
	if dest.TCP.Mode != "mllp" {
		t.Fatalf("expected mode=mllp, got %s", dest.TCP.Mode)
	}
}

func TestToDestination_TCPWithTLS(t *testing.T) {
	cd := &ChannelDestination{
		Type: "tcp",
		TCP: &TCPDestConfig{
			Host: "host", Port: 443,
			TLS: &TLSConfig{Enabled: true, CertFile: "/cert.pem", KeyFile: "/key.pem"},
		},
	}
	dest := cd.ToDestination()
	if dest.TCP.TLS == nil {
		t.Fatal("expected TLS config")
	}
	if !dest.TCP.TLS.Enabled {
		t.Fatal("expected TLS enabled")
	}
	if dest.TCP.TLS.CertFile != "/cert.pem" {
		t.Fatalf("expected cert_file=/cert.pem, got %s", dest.TCP.TLS.CertFile)
	}
}

func TestToDestination_File(t *testing.T) {
	cd := &ChannelDestination{
		Type: "file",
		File: &FileDestConfig{Scheme: "local", Directory: "/out", FilenamePattern: "${id}.dat"},
	}
	dest := cd.ToDestination()
	if dest.File == nil {
		t.Fatal("expected File config")
	}
	if dest.File.Directory != "/out" {
		t.Fatalf("expected directory=/out, got %s", dest.File.Directory)
	}
	if dest.File.Scheme != "local" {
		t.Fatalf("expected scheme=local, got %s", dest.File.Scheme)
	}
}

func TestToDestination_Database(t *testing.T) {
	cd := &ChannelDestination{
		Type:     "database",
		Database: &DBDestConfig{Driver: "postgres", DSN: "host=localhost", Statement: "INSERT INTO msgs"},
	}
	dest := cd.ToDestination()
	if dest.Database == nil {
		t.Fatal("expected Database config")
	}
	if dest.Database.Driver != "postgres" {
		t.Fatalf("expected driver=postgres, got %s", dest.Database.Driver)
	}
	if dest.Database.Statement != "INSERT INTO msgs" {
		t.Fatalf("expected statement, got %s", dest.Database.Statement)
	}
}

func TestToDestination_SMTP(t *testing.T) {
	cd := &ChannelDestination{
		Type: "smtp",
		SMTP: &SMTPDestConfig{
			Host: "smtp.example.com", Port: 587,
			From: "a@b.com", To: []string{"c@d.com"}, Subject: "Alert",
		},
	}
	dest := cd.ToDestination()
	if dest.SMTP == nil {
		t.Fatal("expected SMTP config")
	}
	if dest.SMTP.Host != "smtp.example.com" {
		t.Fatalf("expected host, got %s", dest.SMTP.Host)
	}
	if dest.SMTP.Subject != "Alert" {
		t.Fatalf("expected subject=Alert, got %s", dest.SMTP.Subject)
	}
}

func TestToDestination_SMTPWithAuth(t *testing.T) {
	cd := &ChannelDestination{
		Type: "smtp",
		SMTP: &SMTPDestConfig{
			Host: "smtp.example.com",
			Auth: &AuthConfig{Type: "basic", Username: "user", Password: "pass"},
		},
	}
	dest := cd.ToDestination()
	if dest.SMTP.Auth == nil {
		t.Fatal("expected SMTP auth")
	}
	if dest.SMTP.Auth.Username != "user" {
		t.Fatalf("expected username=user, got %s", dest.SMTP.Auth.Username)
	}
}

func TestToDestination_SMTPWithTLS(t *testing.T) {
	cd := &ChannelDestination{
		Type: "smtp",
		SMTP: &SMTPDestConfig{
			Host: "smtp.example.com",
			TLS:  &TLSConfig{Enabled: true},
		},
	}
	dest := cd.ToDestination()
	if dest.SMTP.TLS == nil {
		t.Fatal("expected SMTP TLS")
	}
}

func TestToDestination_Channel(t *testing.T) {
	cd := &ChannelDestination{
		Type:        "channel",
		ChannelDest: &ChannelDestRef{TargetChannelID: "target"},
	}
	dest := cd.ToDestination()
	if dest.Channel == nil {
		t.Fatal("expected Channel config")
	}
	if dest.Channel.TargetChannelID != "target" {
		t.Fatalf("expected target_channel_id=target, got %s", dest.Channel.TargetChannelID)
	}
}

func TestToDestination_DICOM(t *testing.T) {
	cd := &ChannelDestination{
		Type:  "dicom",
		DICOM: &DICOMDestConfig{Host: "dicom.local", Port: 11112, AETitle: "STORE", CalledAETitle: "REMOTE"},
	}
	dest := cd.ToDestination()
	if dest.DICOM == nil {
		t.Fatal("expected DICOM config")
	}
	if dest.DICOM.Host != "dicom.local" {
		t.Fatalf("expected host, got %s", dest.DICOM.Host)
	}
	if dest.DICOM.CalledAETitle != "REMOTE" {
		t.Fatalf("expected called_ae_title=REMOTE, got %s", dest.DICOM.CalledAETitle)
	}
}

func TestToDestination_JMS(t *testing.T) {
	cd := &ChannelDestination{
		Type: "jms",
		JMS:  &JMSDestConfig{Provider: "activemq", URL: "tcp://localhost:61616", Queue: "test-queue"},
	}
	dest := cd.ToDestination()
	if dest.JMS == nil {
		t.Fatal("expected JMS config")
	}
	if dest.JMS.Provider != "activemq" {
		t.Fatalf("expected provider=activemq, got %s", dest.JMS.Provider)
	}
	if dest.JMS.Queue != "test-queue" {
		t.Fatalf("expected queue, got %s", dest.JMS.Queue)
	}
}

func TestToDestination_JMSWithAuth(t *testing.T) {
	cd := &ChannelDestination{
		Type: "jms",
		JMS: &JMSDestConfig{
			Provider: "activemq", URL: "tcp://localhost:61616",
			Auth: &AuthConfig{Type: "basic", Username: "admin"},
		},
	}
	dest := cd.ToDestination()
	if dest.JMS.Auth == nil {
		t.Fatal("expected JMS auth")
	}
	if dest.JMS.Auth.Username != "admin" {
		t.Fatalf("expected username=admin, got %s", dest.JMS.Auth.Username)
	}
}

func TestToDestination_FHIR(t *testing.T) {
	cd := &ChannelDestination{
		Type: "fhir",
		FHIR: &FHIRDestConfig{BaseURL: "http://fhir.server/r4", Version: "R4", Operations: []string{"create", "update"}},
	}
	dest := cd.ToDestination()
	if dest.FHIR == nil {
		t.Fatal("expected FHIR config")
	}
	if dest.FHIR.BaseURL != "http://fhir.server/r4" {
		t.Fatalf("expected base_url, got %s", dest.FHIR.BaseURL)
	}
	if len(dest.FHIR.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(dest.FHIR.Operations))
	}
}

func TestToDestination_FHIRWithAuth(t *testing.T) {
	cd := &ChannelDestination{
		Type: "fhir",
		FHIR: &FHIRDestConfig{
			BaseURL: "http://fhir.server",
			Auth:    &AuthConfig{Type: "oauth2", TokenURL: "http://auth/token", ClientID: "id", ClientSecret: "sec"},
		},
	}
	dest := cd.ToDestination()
	if dest.FHIR.Auth == nil {
		t.Fatal("expected FHIR auth")
	}
	if dest.FHIR.Auth.TokenURL != "http://auth/token" {
		t.Fatalf("expected token_url, got %s", dest.FHIR.Auth.TokenURL)
	}
}

func TestToDestination_Direct(t *testing.T) {
	cd := &ChannelDestination{
		Type:   "direct",
		Direct: &DirectDestConfig{To: "dest@direct.com", From: "src@direct.com", Certificate: "/cert.pem"},
	}
	dest := cd.ToDestination()
	if dest.Direct == nil {
		t.Fatal("expected Direct config")
	}
	if dest.Direct.To != "dest@direct.com" {
		t.Fatalf("expected to, got %s", dest.Direct.To)
	}
	if dest.Direct.Certificate != "/cert.pem" {
		t.Fatalf("expected certificate, got %s", dest.Direct.Certificate)
	}
}

func TestToDestination_DirectWithSMTP(t *testing.T) {
	cd := &ChannelDestination{
		Type: "direct",
		Direct: &DirectDestConfig{
			To:   "dest@direct.com",
			From: "src@direct.com",
			SMTP: &SMTPDestConfig{
				Host: "smtp.direct.com",
				Port: 25,
				TLS:  &TLSConfig{Enabled: true, MinVersion: "1.2"},
			},
		},
	}
	dest := cd.ToDestination()
	if dest.Direct.SMTPHost != "smtp.direct.com" {
		t.Fatalf("expected smtp_host, got %s", dest.Direct.SMTPHost)
	}
	if dest.Direct.SMTPPort != 25 {
		t.Fatalf("expected smtp_port=25, got %d", dest.Direct.SMTPPort)
	}
	if dest.Direct.TLS == nil {
		t.Fatal("expected Direct TLS from SMTP config")
	}
	if dest.Direct.TLS.MinVersion != "1.2" {
		t.Fatalf("expected min_version=1.2, got %s", dest.Direct.TLS.MinVersion)
	}
}

func TestToDestination_Kafka(t *testing.T) {
	cd := &ChannelDestination{
		Type:  "kafka",
		Kafka: &KafkaDestConfig{Brokers: []string{"b1", "b2"}, Topic: "out"},
	}
	dest := cd.ToDestination()
	if dest.Kafka == nil {
		t.Fatal("expected Kafka config")
	}
	if dest.Kafka.Topic != "out" {
		t.Fatalf("expected topic=out, got %s", dest.Kafka.Topic)
	}
}

func TestToDestination_Empty(t *testing.T) {
	cd := &ChannelDestination{Type: "noop"}
	dest := cd.ToDestination()
	if dest.Type != "noop" {
		t.Fatalf("expected type=noop, got %s", dest.Type)
	}
}

// --- convertTLSConfig ---

func TestConvertTLSConfig(t *testing.T) {
	tls := &TLSConfig{
		Enabled:            true,
		CertFile:           "/cert.pem",
		KeyFile:            "/key.pem",
		CAFile:             "/ca.pem",
		ClientCertFile:     "/client-cert.pem",
		ClientKeyFile:      "/client-key.pem",
		MinVersion:         "1.2",
		InsecureSkipVerify: true,
	}
	result := convertTLSConfig(tls)
	if !result.Enabled {
		t.Fatal("expected enabled")
	}
	if result.CertFile != "/cert.pem" {
		t.Fatalf("expected cert_file, got %s", result.CertFile)
	}
	if result.CAFile != "/ca.pem" {
		t.Fatalf("expected ca_file, got %s", result.CAFile)
	}
	if result.MinVersion != "1.2" {
		t.Fatalf("expected min_version=1.2, got %s", result.MinVersion)
	}
	if !result.InsecureSkipVerify {
		t.Fatal("expected insecure_skip_verify")
	}
}

// --- convertAuthConfig ---

func TestConvertAuthConfig(t *testing.T) {
	auth := &AuthConfig{
		Type:           "oauth2",
		Username:       "user",
		Password:       "pass",
		Token:          "tok",
		Key:            "key",
		Header:         "Authorization",
		QueryParam:     "api_key",
		TokenURL:       "http://auth/token",
		ClientID:       "cid",
		ClientSecret:   "csec",
		Scopes:         []string{"read", "write"},
		PrivateKeyFile: "/pk.pem",
		Passphrase:     "phrase",
	}
	result := convertAuthConfig(auth)
	if result.Type != "oauth2" {
		t.Fatalf("expected type=oauth2, got %s", result.Type)
	}
	if result.Token != "tok" {
		t.Fatalf("expected token=tok, got %s", result.Token)
	}
	if result.ClientID != "cid" {
		t.Fatalf("expected client_id=cid, got %s", result.ClientID)
	}
	if len(result.Scopes) != 2 {
		t.Fatalf("expected 2 scopes, got %d", len(result.Scopes))
	}
	if result.PrivateKeyFile != "/pk.pem" {
		t.Fatalf("expected private_key_file, got %s", result.PrivateKeyFile)
	}
}

// --- MatchesProfile (additional edge cases) ---

func TestMatchesProfile_EmptyStringProfilesList(t *testing.T) {
	ch := &ChannelConfig{ID: "ch-1", Profiles: []string{}}
	if !ch.MatchesProfile("anything") {
		t.Fatal("empty profiles list should match anything")
	}
}

// --- LoadChannelConfig with rich YAML ---

func TestLoadChannelConfig_FullFeatured(t *testing.T) {
	dir := t.TempDir()
	yamlContent := `id: full-channel
enabled: true
description: Full featured channel
profiles:
  - dev
tags:
  - hl7
  - fhir
group: integration
priority: high
data_types:
  inbound: hl7v2
  outbound: fhir
listener:
  type: tcp
  tcp:
    port: 2575
    mode: mllp
pipeline:
  preprocessor: pre.ts
  validator: val.ts
  source_filter: filter.ts
  transformer: transform.ts
  postprocessor: post.ts
destinations:
  - name: http-dest
    type: http
    http:
      url: http://dest.com
      method: POST
    filter: filterScript.ts
    transformer:
      entrypoint: transform.ts
`
	os.WriteFile(filepath.Join(dir, "channel.yaml"), []byte(yamlContent), 0o644)

	cfg, err := LoadChannelConfig(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Description != "Full featured channel" {
		t.Fatalf("expected description, got %s", cfg.Description)
	}
	if cfg.Group != "integration" {
		t.Fatalf("expected group=integration, got %s", cfg.Group)
	}
	if cfg.Priority != "high" {
		t.Fatalf("expected priority=high, got %s", cfg.Priority)
	}
	if cfg.Pipeline == nil {
		t.Fatal("expected pipeline")
	}
	if cfg.Pipeline.Transformer != "transform.ts" {
		t.Fatalf("expected transformer, got %s", cfg.Pipeline.Transformer)
	}
	if cfg.DataTypes == nil || cfg.DataTypes.Inbound != "hl7v2" {
		t.Fatal("expected data_types")
	}
	if len(cfg.Destinations) != 1 {
		t.Fatalf("expected 1 destination, got %d", len(cfg.Destinations))
	}
	if cfg.Destinations[0].Filter != "filterScript.ts" {
		t.Fatalf("expected filter, got %s", cfg.Destinations[0].Filter)
	}
}
