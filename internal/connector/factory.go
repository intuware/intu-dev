package connector

import (
	"fmt"
	"log/slog"

	"github.com/intuware/intu/pkg/config"
)

type Factory struct {
	logger *slog.Logger
}

func NewFactory(logger *slog.Logger) *Factory {
	return &Factory{logger: logger}
}

func (f *Factory) CreateSource(listenerCfg config.ListenerConfig) (SourceConnector, error) {
	switch listenerCfg.Type {
	case "http":
		if listenerCfg.HTTP == nil {
			return nil, fmt.Errorf("http listener config is nil")
		}
		return NewHTTPSource(listenerCfg.HTTP, f.logger), nil
	case "tcp":
		if listenerCfg.TCP == nil {
			return nil, fmt.Errorf("tcp listener config is nil")
		}
		return NewTCPSource(listenerCfg.TCP, f.logger), nil
	case "file":
		if listenerCfg.File == nil {
			return nil, fmt.Errorf("file listener config is nil")
		}
		return NewFileSource(listenerCfg.File, f.logger), nil
	case "channel":
		if listenerCfg.Channel == nil {
			return nil, fmt.Errorf("channel listener config is nil")
		}
		return NewChannelSource(listenerCfg.Channel, f.logger), nil
	case "sftp", "database", "kafka", "email", "dicom", "soap", "fhir", "ihe":
		return NewStubSource(listenerCfg.Type, f.logger), nil
	default:
		return nil, fmt.Errorf("unsupported source type: %s", listenerCfg.Type)
	}
}

func (f *Factory) CreateDestination(name string, dest config.Destination) (DestinationConnector, error) {
	switch dest.Type {
	case "http":
		if dest.HTTP == nil {
			return nil, fmt.Errorf("http destination config is nil for %s", name)
		}
		return NewHTTPDest(name, dest.HTTP, f.logger), nil
	case "file":
		if dest.File == nil {
			return NewLogDest(name, f.logger), nil
		}
		return NewFileDest(name, dest.File, f.logger), nil
	case "channel":
		if dest.Channel != nil {
			return NewChannelDest(name, dest.Channel.TargetChannelID, f.logger), nil
		}
		return NewLogDest(name, f.logger), nil
	case "kafka", "tcp", "database", "smtp", "dicom", "jms", "fhir", "direct":
		return NewLogDest(name, f.logger), nil
	default:
		return nil, fmt.Errorf("unsupported destination type: %s", dest.Type)
	}
}
