package connector

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// KafkaSource — error paths and helpers via mock TCP
// ===================================================================

func TestPush_KafkaSource_StartNoBrokers(t *testing.T) {
	cfg := &config.KafkaListener{Brokers: nil, Topic: "t"}
	src := NewKafkaSource(cfg, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err == nil || !strings.Contains(err.Error(), "at least one broker") {
		t.Fatalf("expected broker error, got %v", err)
	}
}

func TestPush_KafkaSource_StartNoTopic(t *testing.T) {
	cfg := &config.KafkaListener{Brokers: []string{"localhost:9092"}, Topic: ""}
	src := NewKafkaSource(cfg, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err == nil || !strings.Contains(err.Error(), "topic") {
		t.Fatalf("expected topic error, got %v", err)
	}
}

func TestPush_KafkaSource_DialBrokerPlainTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	cfg := &config.KafkaListener{Brokers: []string{ln.Addr().String()}, Topic: "t"}
	src := NewKafkaSource(cfg, testLogger())
	conn, err := src.dialBroker(ln.Addr().String())
	if err != nil {
		t.Fatalf("dialBroker failed: %v", err)
	}
	conn.Close()
}

func TestPush_KafkaSource_DialBrokerNoPort(t *testing.T) {
	cfg := &config.KafkaListener{Brokers: []string{"127.0.0.1"}, Topic: "t"}
	src := NewKafkaSource(cfg, testLogger())
	_, err := src.dialBroker("127.0.0.1")
	if err == nil {
		t.Fatal("expected error dialing broker without valid port")
	}
}

func TestPush_KafkaSource_PerformSASL_NoAuth(t *testing.T) {
	cfg := &config.KafkaListener{Brokers: []string{"localhost:9092"}, Topic: "t"}
	src := NewKafkaSource(cfg, testLogger())
	conn, _ := net.Pipe()
	defer conn.Close()
	err := src.performSASL(conn)
	if err != nil {
		t.Fatalf("expected nil for no auth, got %v", err)
	}
}

func TestPush_KafkaSource_PerformSASL_NoneType(t *testing.T) {
	cfg := &config.KafkaListener{
		Brokers: []string{"localhost:9092"},
		Topic:   "t",
		Auth:    &config.AuthConfig{Type: "none"},
	}
	src := NewKafkaSource(cfg, testLogger())
	conn, _ := net.Pipe()
	defer conn.Close()
	err := src.performSASL(conn)
	if err != nil {
		t.Fatalf("expected nil for none type, got %v", err)
	}
}

func TestPush_KafkaSource_PerformSASL_UnsupportedType(t *testing.T) {
	cfg := &config.KafkaListener{
		Brokers: []string{"localhost:9092"},
		Topic:   "t",
		Auth:    &config.AuthConfig{Type: "kerberos"},
	}
	src := NewKafkaSource(cfg, testLogger())
	conn, _ := net.Pipe()
	defer conn.Close()
	err := src.performSASL(conn)
	if err != nil {
		t.Fatalf("expected nil (warn + skip) for unsupported type, got %v", err)
	}
}

func TestPush_KafkaSource_SASLPlainHandshake_MockBroker(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cfg := &config.KafkaListener{
		Brokers: []string{"localhost:9092"},
		Topic:   "t",
		Auth:    &config.AuthConfig{Type: "sasl_plain", Username: "user", Password: "pass"},
	}
	src := NewKafkaSource(cfg, testLogger())

	go func() {
		sizeBuf := make([]byte, 4)
		io.ReadFull(server, sizeBuf)
		sz := int(sizeBuf[0])<<24 | int(sizeBuf[1])<<16 | int(sizeBuf[2])<<8 | int(sizeBuf[3])
		body := make([]byte, sz)
		io.ReadFull(server, body)

		resp := make([]byte, 4+4+2+4)
		binary.BigEndian.PutUint32(resp[0:4], uint32(len(resp)-4))
		server.Write(resp)

		io.ReadFull(server, sizeBuf)
		authSz := int(sizeBuf[0])<<24 | int(sizeBuf[1])<<16 | int(sizeBuf[2])<<8 | int(sizeBuf[3])
		authBody := make([]byte, authSz)
		io.ReadFull(server, authBody)

		authResp := make([]byte, 4+4)
		binary.BigEndian.PutUint32(authResp[0:4], uint32(len(authResp)-4))
		server.Write(authResp)
	}()

	err := src.saslPlainHandshake(client, "user", "pass")
	if err != nil {
		t.Fatalf("saslPlainHandshake: %v", err)
	}
}

func TestPush_KafkaSource_BuildDebugInfo(t *testing.T) {
	cfg := &config.KafkaListener{
		Brokers: []string{"b1:9092", "b2:9092"},
		Topic:   "my-topic",
		GroupID: "grp",
		Offset:  "latest",
	}
	src := NewKafkaSource(cfg, testLogger())
	info := src.buildDebugInfo()
	if info["topic"] != "my-topic" {
		t.Fatalf("expected topic=my-topic, got %v", info["topic"])
	}
	if info["group_id"] != "grp" {
		t.Fatalf("expected group_id=grp, got %v", info["group_id"])
	}
	if info["offset"] != "latest" {
		t.Fatalf("expected offset=latest, got %v", info["offset"])
	}
}

func TestPush_KafkaSource_DebugJSON(t *testing.T) {
	cfg := &config.KafkaListener{Brokers: []string{"b1:9092"}, Topic: "t"}
	src := NewKafkaSource(cfg, testLogger())
	j := src.debugJSON()
	if !strings.Contains(j, "t") {
		t.Fatalf("expected topic in JSON, got %s", j)
	}
}

func TestPush_KafkaSource_StopBeforeStart(t *testing.T) {
	cfg := &config.KafkaListener{Brokers: []string{"localhost:9092"}, Topic: "t"}
	src := NewKafkaSource(cfg, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// KafkaDest — mock TCP produce
// ===================================================================

func TestPush_KafkaDest_ProduceMockTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		for i := 0; i < 3; i++ {
			sizeBuf := make([]byte, 4)
			if _, err := io.ReadFull(conn, sizeBuf); err != nil {
				return
			}
			sz := int(sizeBuf[0])<<24 | int(sizeBuf[1])<<16 | int(sizeBuf[2])<<8 | int(sizeBuf[3])
			body := make([]byte, sz)
			io.ReadFull(conn, body)

			resp := make([]byte, 4+20)
			binary.BigEndian.PutUint32(resp[0:4], uint32(len(resp)-4))
			conn.Write(resp)
		}
	}()

	cfg := &config.KafkaDestConfig{
		Brokers: []string{ln.Addr().String()},
		Topic:   "test-topic",
	}
	dest := NewKafkaDest("test-kafka-push", cfg, testLogger())

	msg := message.New("ch1", []byte("hello-kafka"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d (err: %v)", resp.StatusCode, resp.Error)
	}
	if msg.Transport != "kafka" {
		t.Fatalf("expected transport=kafka, got %q", msg.Transport)
	}
	dest.Stop(context.Background())
}

func TestPush_KafkaDest_PerformSASL_Nil(t *testing.T) {
	cfg := &config.KafkaDestConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "t",
		Auth:    nil,
	}
	dest := NewKafkaDest("test", cfg, testLogger())
	conn, _ := net.Pipe()
	defer conn.Close()
	err := dest.performSASL(conn)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestPush_KafkaDest_PerformSASL_EmptyType(t *testing.T) {
	cfg := &config.KafkaDestConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "t",
		Auth:    &config.HTTPAuthConfig{Type: ""},
	}
	dest := NewKafkaDest("test", cfg, testLogger())
	conn, _ := net.Pipe()
	defer conn.Close()
	err := dest.performSASL(conn)
	if err != nil {
		t.Fatalf("expected nil for empty type, got %v", err)
	}
}

func TestPush_KafkaDest_PerformSASL_UnsupportedType(t *testing.T) {
	cfg := &config.KafkaDestConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "t",
		Auth:    &config.HTTPAuthConfig{Type: "kerberos"},
	}
	dest := NewKafkaDest("test", cfg, testLogger())
	conn, _ := net.Pipe()
	defer conn.Close()
	err := dest.performSASL(conn)
	if err != nil {
		t.Fatalf("expected nil (warn + skip), got %v", err)
	}
}

// ===================================================================
// SFTP Source — error paths
// ===================================================================

func TestPush_SFTPSource_StartStopCycle(t *testing.T) {
	cfg := &config.SFTPListener{
		Host:         "127.0.0.1",
		Port:         1,
		PollInterval: "100ms",
	}
	src := NewSFTPSource(cfg, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("Start should not fail (poll runs async): %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPush_SFTPSource_AuthUsername_Defaults(t *testing.T) {
	src := NewSFTPSource(&config.SFTPListener{Host: "localhost"}, testLogger())
	if src.authUsername() != "anonymous" {
		t.Fatalf("expected anonymous, got %q", src.authUsername())
	}
}

func TestPush_SFTPSource_BuildAuthMethods_PasswordType(t *testing.T) {
	src := NewSFTPSource(&config.SFTPListener{
		Host: "localhost",
		Auth: &config.AuthConfig{Type: "password", Password: "secret"},
	}, testLogger())
	methods := src.buildAuthMethods()
	if len(methods) != 1 {
		t.Fatalf("expected 1 auth method, got %d", len(methods))
	}
}

func TestPush_SFTPSource_Type(t *testing.T) {
	src := NewSFTPSource(&config.SFTPListener{Host: "localhost"}, testLogger())
	if src.Type() != "sftp" {
		t.Fatalf("expected sftp, got %q", src.Type())
	}
}

// ===================================================================
// SFTP Dest — error paths
// ===================================================================

func TestPush_SFTPDest_SendConnectionRefused(t *testing.T) {
	cfg := &config.SFTPDestMapConfig{Host: "127.0.0.1", Port: 1}
	dest := NewSFTPDest("test-sftp-fail", cfg, testLogger())
	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for refused connection")
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestPush_SFTPDest_SendMissingHost(t *testing.T) {
	cfg := &config.SFTPDestMapConfig{}
	dest := NewSFTPDest("test-sftp-nohost", cfg, testLogger())
	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestPush_SFTPDest_AuthUsername(t *testing.T) {
	dest := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Host: "localhost",
		Auth: &config.HTTPAuthConfig{Username: "myuser"},
	}, testLogger())
	if dest.authUsername() != "myuser" {
		t.Fatalf("expected myuser, got %q", dest.authUsername())
	}
}

func TestPush_SFTPDest_BuildAuthMethods_KeyType(t *testing.T) {
	dest := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Host: "localhost",
		Auth: &config.HTTPAuthConfig{Type: "key", PrivateKeyFile: "/nonexistent/key"},
	}, testLogger())
	methods := dest.buildAuthMethods()
	if len(methods) != 0 {
		t.Fatalf("expected 0 methods (bad key file), got %d", len(methods))
	}
}

// ===================================================================
// EmailSource — error paths and protocol detection
// ===================================================================

func TestPush_EmailSource_StartStopCycle(t *testing.T) {
	cfg := &config.EmailListener{
		Host:         "127.0.0.1",
		Port:         1,
		PollInterval: "100ms",
	}
	src := NewEmailSource(cfg, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("Start should not fail (poll runs async): %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	src.Stop(context.Background())
}

func TestPush_EmailSource_Type_DefaultIMAP(t *testing.T) {
	src := NewEmailSource(&config.EmailListener{Host: "localhost"}, testLogger())
	if src.Type() != "email/imap" {
		t.Fatalf("expected email/imap, got %q", src.Type())
	}
}

func TestPush_EmailSource_Type_POP3(t *testing.T) {
	src := NewEmailSource(&config.EmailListener{Host: "localhost", Protocol: "pop3"}, testLogger())
	if src.Type() != "email/pop3" {
		t.Fatalf("expected email/pop3, got %q", src.Type())
	}
}

func TestPush_EmailSource_PollUnsupportedProtocol(t *testing.T) {
	src := NewEmailSource(&config.EmailListener{Host: "localhost", Protocol: "xmpp"}, testLogger())
	err := src.poll(context.Background(), noopHandler)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported protocol error, got %v", err)
	}
}

func TestPush_EmailSource_MockIMAPServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("* OK IMAP4rev1 Service Ready\r\n"))
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "A001 LOGIN") {
				conn.Write([]byte("A001 OK LOGIN completed\r\n"))
			} else if strings.HasPrefix(line, "A002 SELECT") {
				conn.Write([]byte("* 0 EXISTS\r\n"))
				conn.Write([]byte("A002 OK SELECT completed\r\n"))
			} else if strings.HasPrefix(line, "A003 SEARCH") {
				conn.Write([]byte("* SEARCH\r\n"))
				conn.Write([]byte("A003 OK SEARCH completed\r\n"))
			} else if strings.HasPrefix(line, "A099 LOGOUT") {
				conn.Write([]byte("* BYE\r\nA099 OK LOGOUT\r\n"))
				return
			}
		}
	}()

	cfg := &config.EmailListener{
		Host: "127.0.0.1",
		Port: port,
		Auth: &config.AuthConfig{Username: "user", Password: "pass"},
	}
	src := NewEmailSource(cfg, testLogger())
	err = src.pollIMAP(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("pollIMAP: %v", err)
	}
}

func TestPush_EmailSource_MockPOP3Server(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("+OK POP3 server ready\r\n"))
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "USER") {
				conn.Write([]byte("+OK\r\n"))
			} else if strings.HasPrefix(line, "PASS") {
				conn.Write([]byte("+OK\r\n"))
			} else if strings.HasPrefix(line, "LIST") {
				conn.Write([]byte("+OK\r\n.\r\n"))
			} else if strings.HasPrefix(line, "QUIT") {
				conn.Write([]byte("+OK bye\r\n"))
				return
			}
		}
	}()

	cfg := &config.EmailListener{
		Host:     "127.0.0.1",
		Port:     port,
		Protocol: "pop3",
		Auth:     &config.AuthConfig{Username: "user", Password: "pass"},
	}
	src := NewEmailSource(cfg, testLogger())
	err = src.pollPOP3(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("pollPOP3: %v", err)
	}
}

// ===================================================================
// DatabaseSource — error paths
// ===================================================================

func TestPush_DatabaseSource_StartStopCycle(t *testing.T) {
	cfg := &config.DBListener{
		Driver:       "sqlite3",
		DSN:          ":memory:",
		PollInterval: "100ms",
	}
	src := NewDatabaseSource(cfg, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	src.Stop(context.Background())
}

func TestPush_DatabaseSource_DriverMappings(t *testing.T) {
	tests := []struct {
		in, out string
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
	for _, tt := range tests {
		src := NewDatabaseSource(&config.DBListener{Driver: tt.in}, testLogger())
		if src.driverName() != tt.out {
			t.Errorf("driverName(%q)=%q, want %q", tt.in, src.driverName(), tt.out)
		}
	}
}

func TestPush_DatabaseSource_Type(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{Driver: "postgres"}, testLogger())
	if src.Type() != "database" {
		t.Fatalf("expected database, got %q", src.Type())
	}
}

// ===================================================================
// DatabaseDest — more error paths
// ===================================================================

func TestPush_DatabaseDest_SendNoStatement(t *testing.T) {
	cfg := &config.DBDestMapConfig{Driver: "sqlite3", DSN: ":memory:"}
	dest := NewDatabaseDest("test-db", cfg, testLogger())
	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestPush_DatabaseDest_DriverMapping(t *testing.T) {
	tests := []struct {
		in, out string
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
	for _, tt := range tests {
		dest := NewDatabaseDest("test", &config.DBDestMapConfig{Driver: tt.in, DSN: "test"}, testLogger())
		if dest.driverName() != tt.out {
			t.Errorf("driverName(%q)=%q, want %q", tt.in, dest.driverName(), tt.out)
		}
	}
}

// ===================================================================
// FHIR Subscription — rest-hook and websocket errors
// ===================================================================

func TestPush_FHIRSubscription_RestHookPostNotification(t *testing.T) {
	cfg := &config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
		Path:        "/notify",
	}
	src := NewFHIRSubscriptionSource(cfg, testLogger())
	capture := &msgCapture{}

	if err := src.Start(context.Background(), capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	addr := src.Addr()
	if addr == "" {
		t.Fatal("expected non-empty addr")
	}

	resp, err := http.Post("http://"+addr+"/notify", "application/json",
		strings.NewReader(`{"subscription":"sub-123","eventNumber":5}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	time.Sleep(50 * time.Millisecond)
	if capture.count() != 1 {
		t.Fatalf("expected 1 message, got %d", capture.count())
	}
	msg := capture.get(0)
	if msg.Transport != "fhir_subscription" {
		t.Fatalf("expected transport=fhir_subscription, got %q", msg.Transport)
	}
}

func TestPush_FHIRSubscription_WebSocketNoURL(t *testing.T) {
	cfg := &config.FHIRSubscriptionListener{
		ChannelType: "websocket",
	}
	src := NewFHIRSubscriptionSource(cfg, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err == nil || !strings.Contains(err.Error(), "websocket_url") {
		t.Fatalf("expected websocket_url error, got %v", err)
	}
}

func TestPush_FHIRSubscription_UnsupportedChannelType(t *testing.T) {
	cfg := &config.FHIRSubscriptionListener{ChannelType: "email"}
	src := NewFHIRSubscriptionSource(cfg, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported error, got %v", err)
	}
}

func TestPush_FHIRSubscription_WebSocketDialError(t *testing.T) {
	cfg := &config.FHIRSubscriptionListener{
		ChannelType:          "websocket",
		WebSocketURL:         "ws://127.0.0.1:1/ws",
		MaxReconnectAttempts: 1,
		ReconnectBackoff:     "50ms",
	}
	src := NewFHIRSubscriptionSource(cfg, testLogger())
	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("Start should not error (reconnect loop runs async): %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	src.Stop(context.Background())
}

func TestPush_FHIRSubscription_RestHookMethodNotAllowed(t *testing.T) {
	cfg := &config.FHIRSubscriptionListener{
		ChannelType: "rest-hook",
		Port:        0,
	}
	src := NewFHIRSubscriptionSource(cfg, testLogger())
	if err := src.Start(context.Background(), noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	resp, err := http.Get("http://" + src.Addr() + "/fhir/subscription-notification")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

// ===================================================================
// HTTP — additional scenarios
// ===================================================================

func TestPush_HTTPSource_SharedListener_DifferentPaths(t *testing.T) {
	ResetSharedHTTPListeners()

	cfg1 := &config.HTTPListener{Port: 0, Path: "/path1"}
	src1 := NewHTTPSource(cfg1, testLogger())
	capture1 := &msgCapture{}

	if err := src1.Start(context.Background(), capture1.handler()); err != nil {
		t.Fatalf("start src1: %v", err)
	}
	defer src1.Stop(context.Background())

	addr := src1.Addr()
	resp, err := http.Post("http://"+addr+"/path1", "text/plain", strings.NewReader("msg1"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if capture1.count() != 1 {
		t.Fatalf("expected 1 message, got %d", capture1.count())
	}
}

func TestPush_HTTPDest_QueryParams(t *testing.T) {
	var queryString string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryString = r.URL.RawQuery
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:         server.URL,
		QueryParams: map[string]string{"key1": "val1", "key2": "val2"},
	}
	dest := NewHTTPDest("test-http-qp", cfg, testLogger())
	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if !strings.Contains(queryString, "key1=val1") || !strings.Contains(queryString, "key2=val2") {
		t.Fatalf("expected query params, got %q", queryString)
	}
}

// ===================================================================
// TCP — edge cases
// ===================================================================

func TestPush_TCPSource_MLLPMultipleMessages(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := &config.TCPListener{Port: port, Mode: "mllp", TimeoutMs: 5000}
	src := NewTCPSource(cfg, testLogger())
	capture := &msgCapture{}

	if err := src.Start(context.Background(), capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("MSH|^~\\&|S|F|R|F|20230101||ADT^A01|%d|P|2.5\r", 1000+i)
		conn.Write([]byte{0x0B})
		conn.Write([]byte(msg))
		conn.Write([]byte{0x1C, 0x0D})
	}
	conn.Close()

	time.Sleep(200 * time.Millisecond)

	if capture.count() < 3 {
		t.Fatalf("expected at least 3 messages, got %d", capture.count())
	}
}

func TestPush_TCPDest_TransportStamping(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			io.ReadAll(conn)
			conn.Close()
		}
	}()

	cfg := &config.TCPDestMapConfig{Host: host, Port: port, Mode: "raw", TimeoutMs: 5000}
	dest := NewTCPDest("test-tcp-stamp", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	msg.Transport = "http"
	msg.HTTP = &message.HTTPMeta{Method: "POST"}

	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if msg.Transport != "tcp" {
		t.Fatalf("expected transport=tcp, got %q", msg.Transport)
	}
	if msg.HTTP != nil {
		t.Fatal("expected HTTP meta to be cleared")
	}
	if msg.TCP == nil {
		t.Fatal("expected TCP meta to be set")
	}
	dest.Stop(context.Background())
}

// ===================================================================
// DICOM — edge cases
// ===================================================================

func TestPush_DICOMSource_StopBeforeStart(t *testing.T) {
	cfg := &config.DICOMListener{Port: 0, AETitle: "TEST"}
	src := NewDICOMSource(cfg, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPush_DICOMDest_TLS_ConnectionRefused(t *testing.T) {
	cfg := &config.DICOMDestMapConfig{
		Host:      "127.0.0.1",
		Port:      1,
		TimeoutMs: 500,
		TLS:       &config.TLSMapConfig{Enabled: true, InsecureSkipVerify: true},
	}
	dest := NewDICOMDest("test-dicom-tls-fail", cfg, testLogger())
	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for refused connection")
	}
}

func TestPush_DICOMDest_Stop(t *testing.T) {
	cfg := &config.DICOMDestMapConfig{Host: "localhost", Port: 104}
	dest := NewDICOMDest("test-dicom", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// Helper function coverage: appendInt*, appendKafkaString
// ===================================================================

func TestPush_AppendHelpers(t *testing.T) {
	buf16 := appendInt16(nil, 0x0102)
	if len(buf16) != 2 || buf16[0] != 0x01 || buf16[1] != 0x02 {
		t.Fatalf("appendInt16 wrong: %v", buf16)
	}

	buf32 := appendInt32(nil, 0x01020304)
	if len(buf32) != 4 {
		t.Fatalf("appendInt32 wrong length: %d", len(buf32))
	}

	buf64 := appendInt64(nil, 0x0102030405060708)
	if len(buf64) != 8 {
		t.Fatalf("appendInt64 wrong length: %d", len(buf64))
	}

	ks := appendKafkaString(nil, "hello")
	if len(ks) != 2+5 {
		t.Fatalf("appendKafkaString wrong length: %d", len(ks))
	}
}

// ===================================================================
// ChannelBus — additional coverage
// ===================================================================

func TestPush_ChannelBus_PublishReceive(t *testing.T) {
	bus := GetChannelBus()
	chID := fmt.Sprintf("push-test-%d", time.Now().UnixNano())
	sub := bus.Subscribe(chID)

	msg := message.New("", []byte("push-bus-test"))
	bus.Publish(chID, msg)

	select {
	case received := <-sub:
		if string(received.Raw) != "push-bus-test" {
			t.Fatalf("expected push-bus-test, got %q", string(received.Raw))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for bus message")
	}
}

// ===================================================================
// Factory — SFTP destination
// ===================================================================

func TestPush_Factory_SFTPDest(t *testing.T) {
	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("sftp-out", config.Destination{
		Type: "sftp",
		SFTP: &config.SFTPDestMapConfig{Host: "localhost", Directory: "/tmp"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if dest.Type() != "sftp" {
		t.Fatalf("expected sftp, got %s", dest.Type())
	}
}

func TestPush_Factory_SFTPDestNilConfig(t *testing.T) {
	factory := NewFactory(testLogger())
	_, err := factory.CreateDestination("sftp-nil", config.Destination{Type: "sftp"})
	if err == nil {
		t.Fatal("expected error for nil sftp config")
	}
}
