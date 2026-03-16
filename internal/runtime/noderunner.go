package runtime

import (
	"bufio"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed worker.js
var workerFS embed.FS

type nodeRequest struct {
	ID     int64  `json:"id"`
	Type   string `json:"type"`
	Module string `json:"module,omitempty"`
	Fn     string `json:"fn,omitempty"`
	Args   []any  `json:"args,omitempty"`
}

type nodeResponse struct {
	ID      int64  `json:"id"`
	Type    string `json:"type"`
	Value   any    `json:"value,omitempty"`
	Message string `json:"message,omitempty"`
	Stack   string `json:"stack,omitempty"`
	Level   string `json:"level,omitempty"`
	Args    []any  `json:"args,omitempty"`
}

type nodeWorker struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	mu     sync.Mutex
	dead   bool
}

type NodeRunner struct {
	workers    []*nodeWorker
	workerIdx  uint64
	logger     *slog.Logger
	nextID     int64
	workerJS   string
	cleanupDir string
}

func NewNodeRunner(poolSize int, logger *slog.Logger) (*NodeRunner, error) {
	if poolSize <= 0 {
		poolSize = runtime.NumCPU()
		if poolSize < 1 {
			poolSize = 1
		}
		if poolSize > 8 {
			poolSize = 8
		}
	}

	tmpDir, err := os.MkdirTemp("", "intu-worker-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir for worker.js: %w", err)
	}

	workerJSPath := filepath.Join(tmpDir, "worker.js")
	data, err := workerFS.ReadFile("worker.js")
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("read embedded worker.js: %w", err)
	}
	if err := os.WriteFile(workerJSPath, data, 0o644); err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("write worker.js to temp: %w", err)
	}

	nr := &NodeRunner{
		workers:    make([]*nodeWorker, 0, poolSize),
		logger:     logger,
		workerJS:   workerJSPath,
		cleanupDir: tmpDir,
	}

	for i := 0; i < poolSize; i++ {
		w, err := nr.startWorker()
		if err != nil {
			nr.Close()
			return nil, fmt.Errorf("start worker %d: %w", i, err)
		}
		nr.workers = append(nr.workers, w)
	}

	logger.Info("node worker pool started", "pool_size", poolSize)
	return nr, nil
}

func (nr *NodeRunner) startWorker() (*nodeWorker, error) {
	cmd := exec.Command("node", nr.workerJS)
	cmd.Stderr = os.Stderr
	setProcessGroup(cmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		stdin.Close()
		return nil, fmt.Errorf("start node process: %w", err)
	}

	return &nodeWorker{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewScanner(stdout),
	}, nil
}

func (nr *NodeRunner) acquireWorker() *nodeWorker {
	idx := atomic.AddUint64(&nr.workerIdx, 1) - 1
	return nr.workers[idx%uint64(len(nr.workers))]
}

// restartWorker kills the old process and spawns a fresh one.
// Caller must hold w.mu.
func (nr *NodeRunner) restartWorker(w *nodeWorker) error {
	if w.stdin != nil {
		w.stdin.Close()
	}
	if w.cmd != nil && w.cmd.Process != nil {
		killProcessGroup(w.cmd.Process.Pid)
		_ = w.cmd.Wait()
	}

	fresh, err := nr.startWorker()
	if err != nil {
		return err
	}

	w.cmd = fresh.cmd
	w.stdin = fresh.stdin
	w.stdout = fresh.stdout
	w.dead = false
	nr.logger.Info("node worker restarted")
	return nil
}

func (nr *NodeRunner) nextRequestID() int64 {
	return atomic.AddInt64(&nr.nextID, 1)
}

func (nr *NodeRunner) PreloadModule(module string) error {
	var firstErr error
	for _, w := range nr.workers {
		if err := nr.loadOnWorker(w, module); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (nr *NodeRunner) loadOnWorker(w *nodeWorker, module string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.dead {
		if err := nr.restartWorker(w); err != nil {
			return fmt.Errorf("restart dead worker: %w", err)
		}
	}

	reqID := nr.nextRequestID()
	req := nodeRequest{
		ID:     reqID,
		Type:   "load",
		Module: module,
	}

	if err := nr.sendRequest(w, &req); err != nil {
		nr.logger.Warn("worker send failed during load, restarting", "error", err)
		if restartErr := nr.restartWorker(w); restartErr != nil {
			return fmt.Errorf("send failed: %v; restart failed: %w", err, restartErr)
		}
		reqID = nr.nextRequestID()
		req.ID = reqID
		if err := nr.sendRequest(w, &req); err != nil {
			w.dead = true
			return fmt.Errorf("send failed after restart: %w", err)
		}
	}

	resp, err := nr.readResponse(w, reqID)
	if err != nil {
		w.dead = true
		return err
	}

	if resp.Type == "error" {
		return fmt.Errorf("load module %s: %s", module, resp.Message)
	}
	return nil
}

func (nr *NodeRunner) Call(fn string, entrypoint string, args ...any) (any, error) {
	w := nr.acquireWorker()
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.dead {
		if err := nr.restartWorker(w); err != nil {
			return nil, fmt.Errorf("restart dead worker: %w", err)
		}
	}

	reqID := nr.nextRequestID()
	req := nodeRequest{
		ID:     reqID,
		Type:   "call",
		Module: entrypoint,
		Fn:     fn,
		Args:   args,
	}

	if err := nr.sendRequest(w, &req); err != nil {
		// Send failed before execution started — safe to restart and retry
		nr.logger.Warn("worker send failed, restarting", "error", err)
		if restartErr := nr.restartWorker(w); restartErr != nil {
			return nil, fmt.Errorf("send failed: %v; restart failed: %w", err, restartErr)
		}
		reqID = nr.nextRequestID()
		req.ID = reqID
		if err := nr.sendRequest(w, &req); err != nil {
			w.dead = true
			return nil, fmt.Errorf("send failed after restart: %w", err)
		}
	}

	resp, err := nr.readResponse(w, reqID)
	if err != nil {
		// Worker died mid-execution — restart for future calls but don't retry
		// (script may have had side effects)
		w.dead = true
		return nil, err
	}

	if resp.Type == "error" {
		return nil, fmt.Errorf("call %s in %s: %s", fn, entrypoint, resp.Message)
	}

	return resp.Value, nil
}

func (nr *NodeRunner) sendRequest(w *nodeWorker, req *nodeRequest) error {
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	data = append(data, '\n')
	if _, err := w.stdin.Write(data); err != nil {
		return fmt.Errorf("write to worker stdin: %w", err)
	}
	return nil
}

func (nr *NodeRunner) readResponse(w *nodeWorker, expectedID int64) (*nodeResponse, error) {
	for {
		if !w.stdout.Scan() {
			if err := w.stdout.Err(); err != nil {
				return nil, fmt.Errorf("read worker stdout: %w", err)
			}
			return nil, fmt.Errorf("worker process terminated unexpectedly")
		}

		line := w.stdout.Bytes()
		var resp nodeResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			nr.logger.Warn("unparseable worker output", "line", string(line))
			continue
		}

		if resp.Type == "log" {
			nr.forwardLog(&resp)
			continue
		}

		if resp.ID == expectedID {
			return &resp, nil
		}

		nr.logger.Warn("unexpected response id", "expected", expectedID, "got", resp.ID)
	}
}

func (nr *NodeRunner) forwardLog(resp *nodeResponse) {
	msg := ""
	if len(resp.Args) > 0 {
		msg = fmt.Sprint(resp.Args...)
	}

	switch resp.Level {
	case "error":
		nr.logger.Error(msg, "source", "js")
	case "warn":
		nr.logger.Warn(msg, "source", "js")
	case "debug":
		nr.logger.Debug(msg, "source", "js")
	default:
		nr.logger.Info(msg, "source", "js")
	}
}

const workerShutdownTimeout = 5 * time.Second

func (nr *NodeRunner) Close() error {
	for _, w := range nr.workers {
		w.stdin.Close()
	}

	done := make(chan struct{})
	go func() {
		for _, w := range nr.workers {
			_ = w.cmd.Wait()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(workerShutdownTimeout):
		nr.logger.Warn("worker pool shutdown timed out, force killing")
		for _, w := range nr.workers {
			if w.cmd != nil && w.cmd.Process != nil {
				killProcessGroup(w.cmd.Process.Pid)
			}
		}
		<-done
	}

	if nr.cleanupDir != "" {
		os.RemoveAll(nr.cleanupDir)
	}
	nr.logger.Info("node worker pool stopped")
	return nil
}
