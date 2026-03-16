package runtime

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNodeRunner_BasicCall(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "math.js", `
exports.add = function add(a, b) {
	return a + b;
};
exports.multiply = function multiply(a, b) {
	return a * b;
};
`)

	nr, err := NewNodeRunner(2, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	result, err := nr.Call("add", filepath.Join(dir, "math.js"), 3, 4)
	if err != nil {
		t.Fatalf("Call add: %v", err)
	}

	val, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T: %v", result, result)
	}
	if val != 7 {
		t.Fatalf("expected 7, got %v", val)
	}

	result, err = nr.Call("multiply", filepath.Join(dir, "math.js"), 5, 6)
	if err != nil {
		t.Fatalf("Call multiply: %v", err)
	}
	val = result.(float64)
	if val != 30 {
		t.Fatalf("expected 30, got %v", val)
	}
}

func TestNodeRunner_TransformMessage(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "transformer.js", `
exports.transform = function transform(msg, ctx) {
	return { transformed: true, original: msg, channelId: ctx.channelId };
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	msg := "hello world"
	ctx := map[string]any{"channelId": "test-channel"}

	result, err := nr.Call("transform", filepath.Join(dir, "transformer.js"), msg, ctx)
	if err != nil {
		t.Fatalf("Call transform: %v", err)
	}

	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["transformed"] != true {
		t.Fatalf("expected transformed=true, got %v", m["transformed"])
	}
	if m["original"] != "hello world" {
		t.Fatalf("expected original='hello world', got %v", m["original"])
	}
	if m["channelId"] != "test-channel" {
		t.Fatalf("expected channelId='test-channel', got %v", m["channelId"])
	}
}

func TestNodeRunner_PreloadModule(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "preloaded.js", `
exports.greet = function greet(name) {
	return "Hello, " + name + "!";
};
`)

	nr, err := NewNodeRunner(2, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	modPath := filepath.Join(dir, "preloaded.js")
	if err := nr.PreloadModule(modPath); err != nil {
		t.Fatalf("PreloadModule: %v", err)
	}

	result, err := nr.Call("greet", modPath, "World")
	if err != nil {
		t.Fatalf("Call greet: %v", err)
	}
	if result != "Hello, World!" {
		t.Fatalf("expected 'Hello, World!', got %v", result)
	}
}

func TestNodeRunner_ErrorHandling(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "erroring.js", `
exports.fail = function fail() {
	throw new Error("intentional test error");
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	_, err = nr.Call("fail", filepath.Join(dir, "erroring.js"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "intentional test error") {
		t.Fatalf("expected error to contain 'intentional test error', got: %v", err)
	}
}

func TestNodeRunner_FunctionNotFound(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "empty.js", `
exports.existing = function existing() {
	return true;
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	_, err = nr.Call("nonexistent", filepath.Join(dir, "empty.js"))
	if err == nil {
		t.Fatal("expected error for nonexistent function, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf("expected error about nonexistent function, got: %v", err)
	}
}

func TestNodeRunner_ConsoleLogForwarding(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "logging.js", `
exports.logStuff = function logStuff() {
	console.log("info message");
	console.warn("warn message");
	console.error("error message");
	console.debug("debug message");
	return "done";
};
`)

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	nr, err := NewNodeRunner(1, logger)
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	result, err := nr.Call("logStuff", filepath.Join(dir, "logging.js"))
	if err != nil {
		t.Fatalf("Call logStuff: %v", err)
	}
	if result != "done" {
		t.Fatalf("expected 'done', got %v", result)
	}

	logOutput := buf.String()
	if !strings.Contains(logOutput, "info message") {
		t.Fatalf("expected 'info message' in logs, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "warn message") {
		t.Fatalf("expected 'warn message' in logs, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "error message") {
		t.Fatalf("expected 'error message' in logs, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "debug message") {
		t.Fatalf("expected 'debug message' in logs, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "source=js") {
		t.Fatalf("expected 'source=js' attribute in logs, got:\n%s", logOutput)
	}
}

func TestNodeRunner_AsyncFunction(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "async.js", `
exports.asyncAdd = async function asyncAdd(a, b) {
	return new Promise(function(resolve) {
		setTimeout(function() { resolve(a + b); }, 10);
	});
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	result, err := nr.Call("asyncAdd", filepath.Join(dir, "async.js"), 10, 20)
	if err != nil {
		t.Fatalf("Call asyncAdd: %v", err)
	}
	val, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T: %v", result, result)
	}
	if val != 30 {
		t.Fatalf("expected 30, got %v", val)
	}
}

func TestNodeRunner_ObjectArguments(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "objects.js", `
exports.process = function process(patient) {
	return {
		resourceType: "Patient",
		id: patient.mrn,
		name: [{ family: patient.lastName, given: [patient.firstName] }],
		active: true
	};
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	patient := map[string]any{
		"mrn":       "MRN001",
		"firstName": "John",
		"lastName":  "Doe",
	}

	result, err := nr.Call("process", filepath.Join(dir, "objects.js"), patient)
	if err != nil {
		t.Fatalf("Call process: %v", err)
	}

	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["resourceType"] != "Patient" {
		t.Fatalf("expected Patient, got %v", m["resourceType"])
	}
	if m["id"] != "MRN001" {
		t.Fatalf("expected MRN001, got %v", m["id"])
	}
	if m["active"] != true {
		t.Fatalf("expected active=true, got %v", m["active"])
	}
}

func TestNodeRunner_ConcurrentCalls(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "counter.js", `
exports.identity = function identity(val) {
	return val;
};
`)

	nr, err := NewNodeRunner(4, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	const numCalls = 50
	var wg sync.WaitGroup
	results := make([]any, numCalls)
	errors := make([]error, numCalls)

	for i := 0; i < numCalls; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errors[idx] = nr.Call("identity", filepath.Join(dir, "counter.js"), idx)
		}(i)
	}
	wg.Wait()

	for i := 0; i < numCalls; i++ {
		if errors[i] != nil {
			t.Fatalf("call %d error: %v", i, errors[i])
		}
		val, ok := results[i].(float64)
		if !ok {
			t.Fatalf("call %d: expected float64, got %T: %v", i, results[i], results[i])
		}
		if int(val) != i {
			t.Fatalf("call %d: expected %d, got %v", i, i, val)
		}
	}
}

func TestNodeRunner_ValidatorBoolean(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "validator.js", `
exports.validate = function validate(msg, ctx) {
	return msg !== null && msg !== undefined;
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	result, err := nr.Call("validate", filepath.Join(dir, "validator.js"), "hello", map[string]any{})
	if err != nil {
		t.Fatalf("Call validate: %v", err)
	}
	if result != true {
		t.Fatalf("expected true, got %v (%T)", result, result)
	}

	result, err = nr.Call("validate", filepath.Join(dir, "validator.js"), nil, map[string]any{})
	if err != nil {
		t.Fatalf("Call validate with nil: %v", err)
	}
	if result != false {
		t.Fatalf("expected false for nil input, got %v (%T)", result, result)
	}
}

func TestNodeRunner_ModuleNotFound(t *testing.T) {
	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	_, err = nr.Call("anything", "/nonexistent/path/module.js")
	if err == nil {
		t.Fatal("expected error for nonexistent module, got nil")
	}
}

func TestNodeRunner_Benchmark(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "fast.js", `
exports.transform = function transform(msg, ctx) {
	return { processed: true, data: msg };
};
`)

	nr, err := NewNodeRunner(2, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	modPath := filepath.Join(dir, "fast.js")
	if err := nr.PreloadModule(modPath); err != nil {
		t.Fatalf("PreloadModule: %v", err)
	}

	const iterations = 1000
	for i := 0; i < iterations; i++ {
		_, err := nr.Call("transform", modPath, "test-data", map[string]any{"channelId": "bench"})
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
}

func TestNodeRunner_WorkerRestartOnCrash(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "simple.js", `
exports.echo = function echo(val) {
	return val;
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	result, err := nr.Call("echo", filepath.Join(dir, "simple.js"), "before-kill")
	if err != nil {
		t.Fatalf("Call before kill: %v", err)
	}
	if result != "before-kill" {
		t.Fatalf("expected 'before-kill', got %v", result)
	}

	// Kill the worker process to simulate a crash
	nr.workers[0].mu.Lock()
	nr.workers[0].cmd.Process.Kill()
	nr.workers[0].cmd.Wait()
	nr.workers[0].dead = true
	nr.workers[0].mu.Unlock()

	// Next call should transparently restart the worker and succeed
	result, err = nr.Call("echo", filepath.Join(dir, "simple.js"), "after-kill")
	if err != nil {
		t.Fatalf("Call after kill: %v", err)
	}
	if result != "after-kill" {
		t.Fatalf("expected 'after-kill', got %v", result)
	}
}

func TestNodeRunner_WorkerRestartOnSendFailure(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "simple.js", `
exports.echo = function echo(val) {
	return val;
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	// Kill the process without setting the dead flag — simulates an
	// unexpected crash detected during send (broken pipe)
	nr.workers[0].mu.Lock()
	nr.workers[0].cmd.Process.Kill()
	nr.workers[0].cmd.Wait()
	nr.workers[0].mu.Unlock()

	result, err := nr.Call("echo", filepath.Join(dir, "simple.js"), "recovered")
	if err != nil {
		t.Fatalf("Call after undetected crash: %v", err)
	}
	if result != "recovered" {
		t.Fatalf("expected 'recovered', got %v", result)
	}
}

func TestNodeRunner_PreloadRestartsDeadWorker(t *testing.T) {
	dir := t.TempDir()
	writeTestJS(t, dir, "mod.js", `
exports.greet = function greet(name) {
	return "hi " + name;
};
`)

	nr, err := NewNodeRunner(1, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}
	defer nr.Close()

	// Kill the worker
	nr.workers[0].mu.Lock()
	nr.workers[0].cmd.Process.Kill()
	nr.workers[0].cmd.Wait()
	nr.workers[0].dead = true
	nr.workers[0].mu.Unlock()

	modPath := filepath.Join(dir, "mod.js")
	if err := nr.PreloadModule(modPath); err != nil {
		t.Fatalf("PreloadModule after crash: %v", err)
	}

	result, err := nr.Call("greet", modPath, "world")
	if err != nil {
		t.Fatalf("Call after preload restart: %v", err)
	}
	if result != "hi world" {
		t.Fatalf("expected 'hi world', got %v", result)
	}
}

func TestNodeRunner_CloseWithTimeout(t *testing.T) {
	nr, err := NewNodeRunner(2, testLogger())
	if err != nil {
		t.Fatalf("NewNodeRunner: %v", err)
	}

	start := time.Now()
	if err := nr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)

	// Normal shutdown should be well under the 5s timeout
	if elapsed > 3*time.Second {
		t.Fatalf("Close took too long: %v", elapsed)
	}
}

func writeTestJS(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write JS %s: %v", name, err)
	}
}
