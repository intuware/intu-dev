package connector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// A) KafkaSource — parseMessageSet edge cases
// ===================================================================

func TestKafkaSource_ParseMessageSet_TruncatedMsgSet(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{Topic: "t"}, slog.Default())

	var data []byte
	data = appendInt32(data, 1)
	data = appendInt32(data, 1) // 1 topic
	topicName := "t"
	data = append(data, byte(len(topicName)>>8), byte(len(topicName)))
	data = append(data, []byte(topicName)...)
	data = appendInt32(data, 1) // 1 partition
	data = appendInt32(data, 0)
	data = append(data, 0, 0)
	data = appendInt64(data, 0)
	data = appendInt32(data, 999) // message set size larger than remaining data

	msgs := ks.parseMessageSet(data)
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages for truncated msg set, got %d", len(msgs))
	}
}

func TestKafkaSource_ParseMessageSet_ZeroValueMsg(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{Topic: "t"}, slog.Default())

	value := []byte("")

	var msgBuf []byte
	msgBuf = append(msgBuf, 0, 0, 0, 0) // CRC
	msgBuf = append(msgBuf, 0x00, 0x00)  // magic, attributes
	msgBuf = appendInt32(msgBuf, -1)      // no key
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
	// Empty value has length 0, so valueLen==0 => not appended
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages for zero-length value, got %d", len(msgs))
	}
}

func TestKafkaSource_ParseMessageSet_NilData(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{Topic: "t"}, slog.Default())
	msgs := ks.parseMessageSet(nil)
	if len(msgs) != 0 {
		t.Fatalf("expected 0, got %d", len(msgs))
	}
}

func TestKafkaSource_ParseMessageSet_SmallData(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{Topic: "t"}, slog.Default())
	msgs := ks.parseMessageSet([]byte{0x00, 0x00})
	if len(msgs) != 0 {
		t.Fatalf("expected 0, got %d", len(msgs))
	}
}

// ===================================================================
// A) Kafka helpers — additional appendInt tests
// ===================================================================

func TestAppendInt16_MaxPositive(t *testing.T) {
	buf := appendInt16(nil, 0x7FFF)
	if buf[0] != 0x7F || buf[1] != 0xFF {
		t.Fatalf("expected [7F FF], got [%02X %02X]", buf[0], buf[1])
	}
}

func TestAppendInt32_One(t *testing.T) {
	buf := appendInt32(nil, 1)
	if buf[0] != 0 || buf[1] != 0 || buf[2] != 0 || buf[3] != 1 {
		t.Fatalf("expected [0 0 0 1], got %v", buf)
	}
}

func TestAppendInt64_Negative(t *testing.T) {
	buf := appendInt64(nil, -1)
	if len(buf) != 8 {
		t.Fatalf("expected 8 bytes")
	}
	for i, b := range buf {
		if b != 0xFF {
			t.Fatalf("byte %d: expected 0xFF, got 0x%02X", i, b)
		}
	}
}

func TestAppendKafkaString_LongString(t *testing.T) {
	s := "abcdefghijklmnopqrstuvwxyz"
	buf := appendKafkaString(nil, s)
	if len(buf) != 2+len(s) {
		t.Fatalf("expected %d bytes, got %d", 2+len(s), len(buf))
	}
	if string(buf[2:]) != s {
		t.Fatalf("expected %q, got %q", s, string(buf[2:]))
	}
}

// ===================================================================
// A) KafkaSource — buildDebugInfo with all optional fields
// ===================================================================

func TestKafkaSource_BuildDebugInfo_AllFields(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{
		Brokers: []string{"b1:9092", "b2:9092"},
		Topic:   "events",
		GroupID: "consumer-group",
		Offset:  "latest",
	}, slog.Default())

	info := ks.buildDebugInfo()
	if info["type"] != "kafka" {
		t.Fatalf("expected type=kafka")
	}
	if info["group_id"] != "consumer-group" {
		t.Fatalf("expected group_id=consumer-group, got %v", info["group_id"])
	}
	if info["offset"] != "latest" {
		t.Fatalf("expected offset=latest, got %v", info["offset"])
	}
}

// ===================================================================
// A) KafkaSource — consumeLoop context cancellation
// ===================================================================

func TestKafkaSource_ConsumeLoop_ContextCancel(t *testing.T) {
	ks := NewKafkaSource(&config.KafkaListener{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "t",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		ks.consumeLoop(ctx, func(ctx context.Context, msg *message.Message) error {
			return nil
		})
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("consumeLoop did not exit after context cancel")
	}
}

// ===================================================================
// B) KafkaDest — produce via mock TCP server
// ===================================================================

func TestKafkaDest_Produce_MockTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	received := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 65536)
		n, _ := conn.Read(buf)
		if n > 0 {
			received <- buf[:n]
		}
		// Send a minimal produce response
		resp := make([]byte, 4+4) // size + correlationId
		resp[0] = 0
		resp[1] = 0
		resp[2] = 0
		resp[3] = 4 // 4-byte body
		resp[4] = 0
		resp[5] = 0
		resp[6] = 0
		resp[7] = 201 // correlationId
		conn.Write(resp)
	}()

	addr := ln.Addr().(*net.TCPAddr)
	kd := NewKafkaDest("test", &config.KafkaDestConfig{
		Brokers: []string{fmt.Sprintf("127.0.0.1:%d", addr.Port)},
		Topic:   "test-topic",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", addr.Port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	err = kd.produce(conn, []byte("hello-produce"))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	select {
	case data := <-received:
		if len(data) == 0 {
			t.Fatal("expected non-empty data sent to broker")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for produce data")
	}
}

func TestKafkaDest_ClientID_Variations(t *testing.T) {
	tests := []struct {
		clientID string
		want     string
	}{
		{"", "intu-kafka-dest"},
		{"custom-id", "custom-id"},
		{"my-app-producer", "my-app-producer"},
	}
	for _, tc := range tests {
		kd := NewKafkaDest("d", &config.KafkaDestConfig{ClientID: tc.clientID}, slog.Default())
		got := kd.clientID()
		if got != tc.want {
			t.Errorf("clientID(%q) = %q, want %q", tc.clientID, got, tc.want)
		}
	}
}

func TestKafkaDest_GetConn_NoBrokers(t *testing.T) {
	kd := NewKafkaDest("test", &config.KafkaDestConfig{
		Brokers: nil,
		Topic:   "t",
	}, slog.Default())

	_, err := kd.getConn()
	if err == nil {
		t.Fatal("expected error with nil brokers")
	}
}

func TestKafkaDest_GetConn_EmptyBrokerList(t *testing.T) {
	kd := NewKafkaDest("test", &config.KafkaDestConfig{
		Brokers: []string{},
		Topic:   "t",
	}, slog.Default())

	_, err := kd.getConn()
	if err == nil {
		t.Fatal("expected error with empty broker list")
	}
}

// ===================================================================
// C) EmailSource — constructor, Type(), Stop
// ===================================================================

func TestEmailSource_Constructor(t *testing.T) {
	cfg := &config.EmailListener{
		Host:     "imap.example.com",
		Port:     993,
		Protocol: "imap",
	}
	src := NewEmailSource(cfg, slog.Default())
	if src == nil {
		t.Fatal("expected non-nil EmailSource")
	}
	if src.cfg != cfg {
		t.Fatal("cfg not set correctly")
	}
}

func TestEmailSource_Type_IMAP(t *testing.T) {
	src := NewEmailSource(&config.EmailListener{Protocol: "imap"}, slog.Default())
	if src.Type() != "email/imap" {
		t.Fatalf("expected 'email/imap', got %q", src.Type())
	}
}

func TestEmailSource_Type_POP3(t *testing.T) {
	src := NewEmailSource(&config.EmailListener{Protocol: "pop3"}, slog.Default())
	if src.Type() != "email/pop3" {
		t.Fatalf("expected 'email/pop3', got %q", src.Type())
	}
}

func TestEmailSource_Type_DefaultProtocol(t *testing.T) {
	src := NewEmailSource(&config.EmailListener{Protocol: ""}, slog.Default())
	if src.Type() != "email/imap" {
		t.Fatalf("expected 'email/imap' as default, got %q", src.Type())
	}
}

func TestEmailSource_StopBeforeStart(t *testing.T) {
	src := NewEmailSource(&config.EmailListener{Host: "localhost"}, slog.Default())
	err := src.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop before Start should not error: %v", err)
	}
}

// ===================================================================
// D) DatabaseSource — driverName for all driver types
// ===================================================================

func TestDatabaseSource_DriverName_Oracle(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{Driver: "oracle"}, slog.Default())
	got := src.driverName()
	if got != "oracle" {
		t.Fatalf("driverName(oracle) = %q, want 'oracle'", got)
	}
}

func TestDatabaseSource_DriverName_EmptyPassthrough(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{Driver: "custom-driver"}, slog.Default())
	got := src.driverName()
	if got != "custom-driver" {
		t.Fatalf("driverName(custom-driver) = %q, want 'custom-driver'", got)
	}
}

func TestDatabaseSource_Constructor_Minimal(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{
		Driver: "postgres",
		DSN:    "host=localhost",
	}, slog.Default())
	if src == nil {
		t.Fatal("expected non-nil")
	}
	if src.Type() != "database" {
		t.Fatalf("expected type 'database', got %q", src.Type())
	}
}

func TestDatabaseSource_StopBeforeStart_NoError(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{Driver: "sqlite", DSN: ":memory:"}, slog.Default())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDatabaseDest_Constructor_Minimal(t *testing.T) {
	dest := NewDatabaseDest("test-dest", &config.DBDestMapConfig{
		Driver: "mysql",
		DSN:    "user:pass@tcp(localhost)/db",
	}, slog.Default())
	if dest == nil {
		t.Fatal("expected non-nil")
	}
	if dest.name != "test-dest" {
		t.Fatalf("expected name 'test-dest', got %q", dest.name)
	}
	if dest.Type() != "database" {
		t.Fatalf("expected type 'database', got %q", dest.Type())
	}
}

func TestDatabaseDest_DriverName_AllVariants(t *testing.T) {
	cases := []struct {
		driver string
		want   string
	}{
		{"postgres", "postgres"},
		{"postgresql", "postgres"},
		{"mysql", "mysql"},
		{"mssql", "sqlserver"},
		{"sqlserver", "sqlserver"},
		{"sqlite", "sqlite3"},
		{"sqlite3", "sqlite3"},
		{"oracle", "oracle"},
	}
	for _, tc := range cases {
		dest := NewDatabaseDest("t", &config.DBDestMapConfig{Driver: tc.driver}, slog.Default())
		got := dest.driverName()
		if got != tc.want {
			t.Errorf("driverName(%q) = %q, want %q", tc.driver, got, tc.want)
		}
	}
}

func TestDatabaseDest_StopWithoutConnection(t *testing.T) {
	dest := NewDatabaseDest("t", &config.DBDestMapConfig{Driver: "sqlite"}, slog.Default())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// E) FHIRSubscriptionSource — constructor and methods
// ===================================================================

func TestFHIRSubscriptionSource_Constructor_AllFields(t *testing.T) {
	cfg := &config.FHIRSubscriptionListener{
		ChannelType:    "rest-hook",
		Port:           8080,
		Path:           "/notify",
		Version:        "R4",
		SubscriptionID: "sub-456",
	}
	src := NewFHIRSubscriptionSource(cfg, slog.Default())
	if src == nil {
		t.Fatal("expected non-nil")
	}
	if src.cfg != cfg {
		t.Fatal("cfg not set")
	}
}

func TestFHIRSubscriptionSource_StartRestHook_Setup(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
		Path:        "/test/notify",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	err := src.Start(context.Background(), func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	addr := src.Addr()
	if addr == "" {
		t.Fatal("expected non-empty addr after start")
	}
}

func TestFHIRSubscriptionSource_Type(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{}, slog.Default())
	if src.Type() != "fhir_subscription" {
		t.Fatalf("expected 'fhir_subscription', got %q", src.Type())
	}
}

func TestFHIRSubscriptionSource_Addr_Empty_Before_Start(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{}, slog.Default())
	if src.Addr() != "" {
		t.Fatalf("expected empty addr before start, got %q", src.Addr())
	}
}

func TestFHIRSubscriptionSource_Stop_Before_Start(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{}, slog.Default())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFHIRSubscriptionSource_Start_UnsupportedType(t *testing.T) {
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType: "unsupported",
	}, slog.Default())
	err := src.Start(context.Background(), noopHandler)
	if err == nil {
		t.Fatal("expected error for unsupported channel type")
	}
}

// ===================================================================
// F) DirectDest — constructor with various configs, SMTP host derivation
// ===================================================================

func TestDirectDest_Constructor_AllFields(t *testing.T) {
	cfg := &config.DirectDestMapConfig{
		To:       "recipient@direct.hospital.org",
		From:     "sender@direct.lab.org",
		SMTPHost: "smtp.direct.hospital.org",
		SMTPPort: 465,
	}
	dest := NewDirectDest("direct-1", cfg, slog.Default())
	if dest == nil {
		t.Fatal("expected non-nil")
	}
	if dest.name != "direct-1" {
		t.Fatalf("expected name 'direct-1', got %q", dest.name)
	}
	if dest.cfg.SMTPHost != "smtp.direct.hospital.org" {
		t.Fatalf("expected smtp_host, got %q", dest.cfg.SMTPHost)
	}
}

func TestDirectDest_Type_Returns_Direct(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{}, slog.Default())
	if dest.Type() != "direct" {
		t.Fatalf("expected 'direct', got %q", dest.Type())
	}
}

func TestDirectDest_Stop_Noop(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{}, slog.Default())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestDirectDest_Send_MissingTo(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{
		From: "sender@lab.org",
	}, slog.Default())
	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestDirectDest_Send_MissingFrom(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{
		To: "recipient@direct.org",
	}, slog.Default())
	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestDirectDest_SMTPHostDerived_FromToAddress(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{
		To:   "user@direct.hospital.org",
		From: "sender@lab.org",
	}, slog.Default())

	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	// Will fail to connect but should attempt to derive host from "To" address
	if resp.Error == nil {
		t.Fatal("expected connection error (derived host doesn't exist)")
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502 for connection error, got %d", resp.StatusCode)
	}
}

func TestDirectDest_SMTPHostCannotBeDetermined(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{
		To:   "noatsign",
		From: "sender@lab.org",
	}, slog.Default())

	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 when SMTP host cannot be determined, got %d", resp.StatusCode)
	}
}

func TestDirectDest_Constructor_WithExplicitSMTPHost(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{
		To:       "user@direct.org",
		From:     "sender@direct.org",
		SMTPHost: "smtp.custom.org",
		SMTPPort: 587,
	}, slog.Default())

	if dest.cfg.SMTPHost != "smtp.custom.org" {
		t.Fatalf("expected smtp_host 'smtp.custom.org', got %q", dest.cfg.SMTPHost)
	}
	if dest.cfg.SMTPPort != 587 {
		t.Fatalf("expected smtp_port 587, got %d", dest.cfg.SMTPPort)
	}
}
