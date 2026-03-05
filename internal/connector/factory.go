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
	case "kafka":
		return NewLogDest(name+"-kafka", f.logger), nil
	case "tcp":
		return NewLogDest(name+"-tcp", f.logger), nil
	case "file":
		return NewLogDest(name+"-file", f.logger), nil
	case "database":
		return NewLogDest(name+"-database", f.logger), nil
	case "smtp":
		return NewLogDest(name+"-smtp", f.logger), nil
	case "channel":
		return NewLogDest(name+"-channel", f.logger), nil
	case "dicom":
		return NewLogDest(name+"-dicom", f.logger), nil
	case "jms":
		return NewLogDest(name+"-jms", f.logger), nil
	case "fhir":
		return NewLogDest(name+"-fhir", f.logger), nil
	case "direct":
		return NewLogDest(name+"-direct", f.logger), nil
	default:
		return nil, fmt.Errorf("unsupported destination type: %s", dest.Type)
	}
}
