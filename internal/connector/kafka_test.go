package connector

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
	"golang.org/x/crypto/ssh"
)

// ===================================================================
// KafkaSource — parseMessageSet with a valid single-message response
// ===================================================================

func TestKafkaSource_ParseMessageSet_SingleMessage(t *testing.T) {
	ks := &KafkaSource{
		cfg:    &config.KafkaListener{Topic: "test-topic"},
		logger: slog.Default(),
	}

	value := []byte("hello-kafka")

	var data []byte
	data = appendInt32(data, 1) // correlationId
	data = appendInt32(data, 1) // topic count

	topicName := "test-topic"
	data = append(data, byte(len(topicName)>>8), byte(len(topicName)))
	data = append(data, []byte(topicName)...)
	data = appendInt32(data, 1) // partition count

	data = appendInt32(data, 0)  // partition
	data = append(data, 0, 0)   // error code
	data = appendInt64(data, 0)  // high watermark offset

	var msgBuf []byte
	msgBuf = append(msgBuf, 0, 0, 0, 0) // CRC placeholder
	msgBuf = append(msgBuf, 0x00)        // magic
	msgBuf = append(msgBuf, 0x00)        // attributes
	msgBuf = appendInt32(msgBuf, -1)     // key length (null)
	msgBuf = appendInt32(msgBuf, int32(len(value)))
	msgBuf = append(msgBuf, value...)

	var msgSet []byte
	msgSet = appendInt64(msgSet, 0)                  // offset
	msgSet = appendInt32(msgSet, int32(len(msgBuf))) // message size
	msgSet = append(msgSet, msgBuf...)

	data = appendInt32(data, int32(len(msgSet)))
	data = append(data, msgSet...)

	msgs := ks.parseMessageSet(data)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if string(msgs[0]) != "hello-kafka" {
		t.Fatalf("expected 'hello-kafka', got %q", string(msgs[0]))
	}
}

func TestKafkaSource_ParseMessageSet_MultipleMessages(t *testing.T) {
	ks := &KafkaSource{
		cfg:    &config.KafkaListener{Topic: "t"},
		logger: slog.Default(),
	}

	values := []string{"msg-1", "msg-2", "msg-3"}

	var msgSet []byte
	for i, v := range values {
		val := []byte(v)
		var msgBuf []byte
		msgBuf = append(msgBuf, 0, 0, 0, 0)
		msgBuf = append(msgBuf, 0x00, 0x00)
		msgBuf = appendInt32(msgBuf, -1)
		msgBuf = appendInt32(msgBuf, int32(len(val)))
		msgBuf = append(msgBuf, val...)

		msgSet = appendInt64(msgSet, int64(i))
		msgSet = appendInt32(msgSet, int32(len(msgBuf)))
		msgSet = append(msgSet, msgBuf...)
	}

	var data []byte
	data = appendInt32(data, 1) // correlationId
	data = appendInt32(data, 1) // topic count
	topicName := "t"
	data = append(data, byte(len(topicName)>>8), byte(len(topicName)))
	data = append(data, []byte(topicName)...)
	data = appendInt32(data, 1)
	data = appendInt32(data, 0)
	data = append(data, 0, 0)
	data = appendInt64(data, 0)
	data = appendInt32(data, int32(len(msgSet)))
	data = append(data, msgSet...)

	msgs := ks.parseMessageSet(data)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	for i, v := range values {
		if string(msgs[i]) != v {
			t.Fatalf("message %d: expected %q, got %q", i, v, string(msgs[i]))
		}
	}
}

func TestKafkaSource_ParseMessageSet_WithKey(t *testing.T) {
	ks := &KafkaSource{
		cfg:    &config.KafkaListener{Topic: "t"},
		logger: slog.Default(),
	}

	key := []byte("my-key")
	value := []byte("my-value")

	var msgBuf []byte
	msgBuf = append(msgBuf, 0, 0, 0, 0) // CRC
	msgBuf = append(msgBuf, 0x00, 0x00)  // magic, attributes
	msgBuf = appendInt32(msgBuf, int32(len(key)))
	msgBuf = append(msgBuf, key...)
	msgBuf = appendInt32(msgBuf, int32(len(value)))
	msgBuf = append(msgBuf, value...)

	var msgSet []byte
	msgSet = appendInt64(msgSet, 0)
	msgSet = appendInt32(msgSet, int32(len(msgBuf)))
	msgSet = append(msgSet, msgBuf...)

	var data []byte
	data = appendInt32(data, 1)
	data = appendInt32(data, 1)
	topicName := "t"
	data = append(data, byte(len(topicName)>>8), byte(len(topicName)))
	data = append(data, []byte(topicName)...)
	data = appendInt32(data, 1)
	data = appendInt32(data, 0)
	data = append(data, 0, 0)
	data = appendInt64(data, 0)
	data = appendInt32(data, int32(len(msgSet)))
	data = append(data, msgSet...)

	msgs := ks.parseMessageSet(data)
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
	if string(msgs[0]) != "my-value" {
		t.Fatalf("expected my-value, got %q", string(msgs[0]))
	}
}

func TestKafkaSource_ParseMessageSet_ZeroTopics(t *testing.T) {
	ks := &KafkaSource{
		cfg:    &config.KafkaListener{Topic: "t"},
		logger: slog.Default(),
	}
	var data []byte
	data = appendInt32(data, 1) // correlationId
	data = appendInt32(data, 0) // 0 topics

	msgs := ks.parseMessageSet(data)
	if len(msgs) != 0 {
		t.Fatalf("expected 0, got %d", len(msgs))
	}
}

// ===================================================================
// KafkaSource — Start validation
// ===================================================================

func TestKafkaSource_StartNoBrokers(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{Topic: "t"}, slog.Default())
	err := ks.Start(context.Background(), func(ctx context.Context, msg *message.Message) error { return nil })
	if err == nil {
		t.Fatal("expected error for no brokers")
	}
}

func TestKafkaSource_StartNoTopic(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{Brokers: []string{"localhost:9092"}}, slog.Default())
	err := ks.Start(context.Background(), func(ctx context.Context, msg *message.Message) error { return nil })
	if err == nil {
		t.Fatal("expected error for no topic")
	}
}

func TestKafkaSource_TypeReturnsKafka(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{}, slog.Default())
	if ks.Type() != "kafka" {
		t.Fatalf("expected 'kafka', got %q", ks.Type())
	}
}

func TestKafkaSource_StopBeforeStart(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{}, slog.Default())
	if err := ks.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ===================================================================
// KafkaDest — getConn error cases
// ===================================================================

func TestKafkaDest_GetConnNoBrokers(t *testing.T) {
	kd := NewKafkaDest("test", &config.KafkaDestConfig{}, slog.Default())
	_, err := kd.getConn()
	if err == nil {
		t.Fatal("expected error with no brokers")
	}
}

func TestKafkaDest_GetConnUnreachableBroker(t *testing.T) {
	kd := NewKafkaDest("test", &config.KafkaDestConfig{
		Brokers: []string{"127.0.0.1:1"},
	}, slog.Default())
	_, err := kd.getConn()
	if err == nil {
		t.Fatal("expected error connecting to unreachable broker")
	}
}

func TestKafkaDest_TypeReturnsKafka(t *testing.T) {
	kd := NewKafkaDest("x", &config.KafkaDestConfig{}, slog.Default())
	if kd.Type() != "kafka" {
		t.Fatalf("expected 'kafka', got %q", kd.Type())
	}
}

func TestKafkaDest_StopClosesConnection(t *testing.T) {
	kd := NewKafkaDest("x", &config.KafkaDestConfig{}, slog.Default())
	if err := kd.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestKafkaDest_SendNoBrokersReturns502(t *testing.T) {
	kd := NewKafkaDest("test", &config.KafkaDestConfig{Topic: "t"}, slog.Default())
	msg := message.New("ch1", []byte("payload"))
	resp, err := kd.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

// ===================================================================
// SFTPSource — authUsername
// ===================================================================

func TestSFTPSource_AuthUsername_WithAuth(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{
		Auth: &config.AuthConfig{Username: "myuser"},
	}, slog.Default())
	if got := s.authUsername(); got != "myuser" {
		t.Fatalf("expected 'myuser', got %q", got)
	}
}

func TestSFTPSource_AuthUsername_NilAuth(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{}, slog.Default())
	if got := s.authUsername(); got != "anonymous" {
		t.Fatalf("expected 'anonymous', got %q", got)
	}
}

func TestSFTPSource_AuthUsername_EmptyUsername(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{
		Auth: &config.AuthConfig{Username: ""},
	}, slog.Default())
	if got := s.authUsername(); got != "anonymous" {
		t.Fatalf("expected 'anonymous', got %q", got)
	}
}

// ===================================================================
// SFTPSource — buildAuthMethods
// ===================================================================

func TestSFTPSource_BuildAuthMethods_NilAuth(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{}, slog.Default())
	methods := s.buildAuthMethods()
	if methods != nil {
		t.Fatalf("expected nil methods for nil auth, got %d", len(methods))
	}
}

func TestSFTPSource_BuildAuthMethods_Password(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{
		Auth: &config.AuthConfig{Type: "password", Password: "secret"},
	}, slog.Default())
	methods := s.buildAuthMethods()
	if len(methods) != 1 {
		t.Fatalf("expected 1 auth method, got %d", len(methods))
	}
}

func TestSFTPSource_BuildAuthMethods_PasswordEmpty(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{
		Auth: &config.AuthConfig{Type: "password", Password: ""},
	}, slog.Default())
	methods := s.buildAuthMethods()
	if len(methods) != 0 {
		t.Fatalf("expected 0 methods for empty password, got %d", len(methods))
	}
}

func TestSFTPSource_BuildAuthMethods_KeyFile(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := tmpDir + "/test_key"

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen failed: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("signer failed: %v", err)
	}
	_ = signer

	pemBlock, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(pemBlock), 0600); err != nil {
		t.Fatalf("write key failed: %v", err)
	}

	s := NewSFTPSource(&config.SFTPListener{
		Auth: &config.AuthConfig{Type: "key", PrivateKeyFile: keyPath},
	}, slog.Default())
	methods := s.buildAuthMethods()
	if len(methods) != 1 {
		t.Fatalf("expected 1 auth method for key, got %d", len(methods))
	}
}

func TestSFTPSource_BuildAuthMethods_KeyFileNotFound(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{
		Auth: &config.AuthConfig{Type: "key", PrivateKeyFile: "/nonexistent/path/key"},
	}, slog.Default())
	methods := s.buildAuthMethods()
	if len(methods) != 0 {
		t.Fatalf("expected 0 methods for missing key file, got %d", len(methods))
	}
}

func TestSFTPSource_BuildAuthMethods_DefaultWithPasswordAndKey(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := tmpDir + "/test_key"

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	pemBlock, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	os.WriteFile(keyPath, pem.EncodeToMemory(pemBlock), 0600)

	s := NewSFTPSource(&config.SFTPListener{
		Auth: &config.AuthConfig{
			Type:           "unknown",
			Password:       "pass",
			PrivateKeyFile: keyPath,
		},
	}, slog.Default())
	methods := s.buildAuthMethods()
	if len(methods) != 2 {
		t.Fatalf("expected 2 auth methods (password+key), got %d", len(methods))
	}
}

// ===================================================================
// SFTPSource — sortEntries
// ===================================================================

type mockFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (m mockFileInfo) Name() string      { return m.name }
func (m mockFileInfo) Size() int64       { return m.size }
func (m mockFileInfo) ModTime() time.Time { return m.modTime }
func (m mockFileInfo) Mode() os.FileMode  { return 0644 }
func (m mockFileInfo) IsDir() bool        { return false }
func (m mockFileInfo) Sys() any           { return nil }

func TestSFTPSource_SortEntries_ByName(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{SortBy: "name"}, slog.Default())
	entries := []os.FileInfo{
		mockFileInfo{name: "c.txt"},
		mockFileInfo{name: "a.txt"},
		mockFileInfo{name: "b.txt"},
	}
	s.sortEntries(entries)
	if entries[0].Name() != "a.txt" || entries[1].Name() != "b.txt" || entries[2].Name() != "c.txt" {
		t.Fatalf("expected sorted by name a,b,c got %s,%s,%s", entries[0].Name(), entries[1].Name(), entries[2].Name())
	}
}

func TestSFTPSource_SortEntries_ByModified(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{SortBy: "modified"}, slog.Default())
	t3 := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	entries := []os.FileInfo{
		mockFileInfo{name: "c", modTime: t3},
		mockFileInfo{name: "a", modTime: t1},
		mockFileInfo{name: "b", modTime: t2},
	}
	s.sortEntries(entries)
	if entries[0].Name() != "a" || entries[1].Name() != "b" || entries[2].Name() != "c" {
		t.Fatalf("sort by modified wrong order: %s,%s,%s", entries[0].Name(), entries[1].Name(), entries[2].Name())
	}
}

func TestSFTPSource_SortEntries_BySize(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{SortBy: "size"}, slog.Default())
	entries := []os.FileInfo{
		mockFileInfo{name: "big", size: 1000},
		mockFileInfo{name: "small", size: 10},
		mockFileInfo{name: "medium", size: 500},
	}
	s.sortEntries(entries)
	if entries[0].Name() != "small" || entries[1].Name() != "medium" || entries[2].Name() != "big" {
		t.Fatalf("sort by size wrong order: %s,%s,%s", entries[0].Name(), entries[1].Name(), entries[2].Name())
	}
}

func TestSFTPSource_SortEntries_UnknownSortByDoesNothing(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{SortBy: "random"}, slog.Default())
	entries := []os.FileInfo{
		mockFileInfo{name: "c"},
		mockFileInfo{name: "a"},
		mockFileInfo{name: "b"},
	}
	s.sortEntries(entries)
	if entries[0].Name() != "c" || entries[1].Name() != "a" || entries[2].Name() != "b" {
		t.Fatal("unknown sort_by should not change order")
	}
}

func TestSFTPSource_SortEntries_Empty(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{SortBy: "name"}, slog.Default())
	entries := []os.FileInfo{}
	s.sortEntries(entries)
	if len(entries) != 0 {
		t.Fatal("expected empty slice")
	}
}

func TestSFTPSource_TypeReturnsSFTP(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{}, slog.Default())
	if s.Type() != "sftp" {
		t.Fatalf("expected 'sftp', got %q", s.Type())
	}
}

func TestSFTPSource_StopBeforeStart(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{}, slog.Default())
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ===================================================================
// SFTPDest — authUsername
// ===================================================================

func TestSFTPDest_AuthUsername_WithAuth(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Auth: &config.HTTPAuthConfig{Username: "sftpuser"},
	}, slog.Default())
	if got := d.authUsername(); got != "sftpuser" {
		t.Fatalf("expected 'sftpuser', got %q", got)
	}
}

func TestSFTPDest_AuthUsername_NilAuth(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{}, slog.Default())
	if got := d.authUsername(); got != "anonymous" {
		t.Fatalf("expected 'anonymous', got %q", got)
	}
}

func TestSFTPDest_AuthUsername_EmptyUsername(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Auth: &config.HTTPAuthConfig{Username: ""},
	}, slog.Default())
	if got := d.authUsername(); got != "anonymous" {
		t.Fatalf("expected 'anonymous', got %q", got)
	}
}

// ===================================================================
// SFTPDest — buildAuthMethods
// ===================================================================

func TestSFTPDest_BuildAuthMethods_NilAuth(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{}, slog.Default())
	methods := d.buildAuthMethods()
	if methods != nil {
		t.Fatalf("expected nil methods for nil auth, got %d", len(methods))
	}
}

func TestSFTPDest_BuildAuthMethods_Password(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Auth: &config.HTTPAuthConfig{Type: "password", Password: "s3cret"},
	}, slog.Default())
	methods := d.buildAuthMethods()
	if len(methods) != 1 {
		t.Fatalf("expected 1 method, got %d", len(methods))
	}
}

func TestSFTPDest_BuildAuthMethods_EmptyPassword(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Auth: &config.HTTPAuthConfig{Type: "password", Password: ""},
	}, slog.Default())
	methods := d.buildAuthMethods()
	if len(methods) != 0 {
		t.Fatalf("expected 0 methods for empty password, got %d", len(methods))
	}
}

func TestSFTPDest_BuildAuthMethods_KeyFile(t *testing.T) {
	tmpDir := t.TempDir()
	keyPath := tmpDir + "/test_key"

	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	pemBlock, _ := ssh.MarshalPrivateKey(priv, "")
	os.WriteFile(keyPath, pem.EncodeToMemory(pemBlock), 0600)

	d := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Auth: &config.HTTPAuthConfig{Type: "key", PrivateKeyFile: keyPath},
	}, slog.Default())
	methods := d.buildAuthMethods()
	if len(methods) != 1 {
		t.Fatalf("expected 1 method for key auth, got %d", len(methods))
	}
}

func TestSFTPDest_BuildAuthMethods_DefaultFallback(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Auth: &config.HTTPAuthConfig{Type: "unknown_type", Password: "fallback"},
	}, slog.Default())
	methods := d.buildAuthMethods()
	if len(methods) != 1 {
		t.Fatalf("expected 1 method for default fallback with password, got %d", len(methods))
	}
}

func TestSFTPDest_TypeReturnsSFTP(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{}, slog.Default())
	if d.Type() != "sftp" {
		t.Fatalf("expected 'sftp', got %q", d.Type())
	}
}

func TestSFTPDest_StopIsNoop(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{}, slog.Default())
	if err := d.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSFTPDest_SendEmptyHostReturns400(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{Host: ""}, slog.Default())
	msg := message.New("ch1", []byte("data"))
	resp, err := d.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

// ===================================================================
// FHIRSubscriptionSource — constructor, Type, Addr, Stop
// ===================================================================

func TestFHIRSubscriptionSource_Constructor(t *testing.T) {
	cfg := &config.FHIRSubscriptionListener{
		ChannelType:    "rest-hook",
		Port:           9090,
		Path:           "/fhir/notify",
		Version:        "R4",
		SubscriptionID: "sub-123",
	}
	src := NewFHIRSubscriptionSource(cfg, slog.Default())
	if src == nil {
		t.Fatal("constructor returned nil")
	}
}

func TestFHIRSubscriptionSource_TypeReturnsFHIRSubscription(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{}, slog.Default())
	if src.Type() != "fhir_subscription" {
		t.Fatalf("expected 'fhir_subscription', got %q", src.Type())
	}
}

func TestFHIRSubscriptionSource_AddrEmptyBeforeStart(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{}, slog.Default())
	if src.Addr() != "" {
		t.Fatalf("expected empty addr before start, got %q", src.Addr())
	}
}

func TestFHIRSubscriptionSource_StopBeforeStart(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{}, slog.Default())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFHIRSubscriptionSource_StartUnsupportedChannelType(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "invalid",
	}, slog.Default())
	err := src.Start(context.Background(), func(ctx context.Context, msg *message.Message) error { return nil })
	if err == nil {
		t.Fatal("expected error for unsupported channel type")
	}
}

func TestFHIRSubscriptionSource_StartWebSocketNoURL(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType:  "websocket",
		WebSocketURL: "",
	}, slog.Default())
	err := src.Start(context.Background(), func(ctx context.Context, msg *message.Message) error { return nil })
	if err == nil {
		t.Fatal("expected error for missing websocket URL")
	}
}

func TestFHIRSubscriptionSource_RestHookStartStop(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
	}, slog.Default())
	err := src.Start(context.Background(), func(ctx context.Context, msg *message.Message) error { return nil })
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}

	addr := src.Addr()
	if addr == "" {
		t.Fatal("expected non-empty addr after start")
	}

	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

// ===================================================================
// KafkaDest — clientID (extended beyond existing tests)
// ===================================================================

func TestKafkaDest_ClientID_EmptyString(t *testing.T) {
	kd := NewKafkaDest("dest", &config.KafkaDestConfig{ClientID: ""}, slog.Default())
	if kd.clientID() != "intu-kafka-dest" {
		t.Fatalf("expected default, got %q", kd.clientID())
	}
}

func TestKafkaDest_ClientID_WithCustom(t *testing.T) {
	kd := NewKafkaDest("dest", &config.KafkaDestConfig{ClientID: "my-app"}, slog.Default())
	if kd.clientID() != "my-app" {
		t.Fatalf("expected 'my-app', got %q", kd.clientID())
	}
}

// ===================================================================
// KafkaSource — buildDebugInfo edge cases
// ===================================================================

func TestKafkaSource_BuildDebugInfo_EmptyBrokers(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{
		Brokers: []string{},
		Topic:   "t",
	}, slog.Default())
	info := ks.buildDebugInfo()
	if info["type"] != "kafka" {
		t.Fatal("expected type=kafka")
	}
	if _, ok := info["group_id"]; ok {
		t.Fatal("group_id should be absent when empty")
	}
	if _, ok := info["offset"]; ok {
		t.Fatal("offset should be absent when empty")
	}
}

func TestKafkaSource_DebugJSON_ValidJSON(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{
		Brokers: []string{"b1:9092"},
		Topic:   "my-topic",
		GroupID: "grp",
		Offset:  "earliest",
	}, slog.Default())
	js := ks.debugJSON()
	if js == "" {
		t.Fatal("expected non-empty JSON")
	}
}
