package logging

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/intuware/intu/pkg/config"
)

type LogTransport interface {
	io.Writer
	Close() error
}

type stdoutTransport struct{}

func (s *stdoutTransport) Write(p []byte) (int, error) {
	return os.Stdout.Write(p)
}

func (s *stdoutTransport) Close() error {
	return nil
}

type MultiTransport struct {
	mu         sync.Mutex
	transports []LogTransport
}

func NewMultiTransport(transports ...LogTransport) *MultiTransport {
	return &MultiTransport{transports: transports}
}

func (m *MultiTransport) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, t := range m.transports {
		if _, err := t.Write(p); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return len(p), firstErr
}

func (m *MultiTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, t := range m.transports {
		if err := t.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func NewTransportFromConfig(cfg *config.LoggingConfig) (LogTransport, error) {
	if cfg == nil || len(cfg.Transports) == 0 {
		return &stdoutTransport{}, nil
	}

	var transports []LogTransport
	for _, tc := range cfg.Transports {
		t, err := buildTransport(tc)
		if err != nil {
			for _, prev := range transports {
				prev.Close()
			}
			return nil, fmt.Errorf("build transport %q: %w", tc.Type, err)
		}
		transports = append(transports, t)
	}

	if len(transports) == 1 {
		return transports[0], nil
	}
	return NewMultiTransport(transports...), nil
}

func buildTransport(tc config.LogTransportConfig) (LogTransport, error) {
	switch tc.Type {
	case "stdout", "":
		return &stdoutTransport{}, nil
	case "cloudwatch":
		if tc.CloudWatch == nil {
			return nil, fmt.Errorf("cloudwatch config is required when type is cloudwatch")
		}
		return NewCloudWatchTransport(tc.CloudWatch)
	case "datadog":
		if tc.Datadog == nil {
			return nil, fmt.Errorf("datadog config is required when type is datadog")
		}
		return NewDatadogTransport(tc.Datadog)
	case "sumologic":
		if tc.SumoLogic == nil {
			return nil, fmt.Errorf("sumologic config is required when type is sumologic")
		}
		return NewSumoLogicTransport(tc.SumoLogic)
	case "elasticsearch":
		if tc.Elasticsearch == nil {
			return nil, fmt.Errorf("elasticsearch config is required when type is elasticsearch")
		}
		return NewElasticsearchTransport(tc.Elasticsearch)
	case "file":
		if tc.File == nil {
			return nil, fmt.Errorf("file config is required when type is file")
		}
		return NewFileTransport(tc.File)
	default:
		return nil, fmt.Errorf("unknown log transport type: %s", tc.Type)
	}
}
