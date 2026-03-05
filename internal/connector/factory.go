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

	case "sftp":
		if listenerCfg.SFTP == nil {
			return nil, fmt.Errorf("sftp listener config is nil")
		}
		return NewSFTPSource(listenerCfg.SFTP, f.logger), nil

	case "database":
		if listenerCfg.Database == nil {
			return nil, fmt.Errorf("database listener config is nil")
		}
		return NewDatabaseSource(listenerCfg.Database, f.logger), nil

	case "kafka":
		if listenerCfg.Kafka == nil {
			return nil, fmt.Errorf("kafka listener config is nil")
		}
		return NewKafkaSource(listenerCfg.Kafka, f.logger), nil

	case "email":
		if listenerCfg.Email == nil {
			return nil, fmt.Errorf("email listener config is nil")
		}
		return NewEmailSource(listenerCfg.Email, f.logger), nil

	case "dicom":
		if listenerCfg.DICOM == nil {
			return nil, fmt.Errorf("dicom listener config is nil")
		}
		return NewDICOMSource(listenerCfg.DICOM, f.logger), nil

	case "soap":
		if listenerCfg.SOAP == nil {
			return nil, fmt.Errorf("soap listener config is nil")
		}
		return NewSOAPSource(listenerCfg.SOAP, f.logger), nil

	case "fhir":
		if listenerCfg.FHIR == nil {
			return nil, fmt.Errorf("fhir listener config is nil")
		}
		return NewFHIRSource(listenerCfg.FHIR, f.logger), nil

	case "ihe":
		if listenerCfg.IHE == nil {
			return nil, fmt.Errorf("ihe listener config is nil")
		}
		return NewIHESource(listenerCfg.IHE, f.logger), nil

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
			return nil, fmt.Errorf("file destination config is nil for %s", name)
		}
		return NewFileDest(name, dest.File, f.logger), nil
	case "channel":
		if dest.Channel == nil {
			return nil, fmt.Errorf("channel destination config is nil for %s", name)
		}
		return NewChannelDest(name, dest.Channel.TargetChannelID, f.logger), nil
	case "tcp":
		if dest.TCP == nil {
			return nil, fmt.Errorf("tcp destination config is nil for %s", name)
		}
		return NewTCPDest(name, dest.TCP, f.logger), nil
	case "kafka":
		if dest.Kafka == nil {
			return nil, fmt.Errorf("kafka destination config is nil for %s", name)
		}
		return NewKafkaDest(name, dest.Kafka, f.logger), nil
	case "database":
		if dest.Database == nil {
			return nil, fmt.Errorf("database destination config is nil for %s", name)
		}
		return NewDatabaseDest(name, dest.Database, f.logger), nil
	case "smtp":
		if dest.SMTP == nil {
			return nil, fmt.Errorf("smtp destination config is nil for %s", name)
		}
		return NewSMTPDest(name, dest.SMTP, f.logger), nil
	case "dicom":
		if dest.DICOM == nil {
			return nil, fmt.Errorf("dicom destination config is nil for %s", name)
		}
		return NewDICOMDest(name, dest.DICOM, f.logger), nil
	case "jms":
		if dest.JMS == nil {
			return nil, fmt.Errorf("jms destination config is nil for %s", name)
		}
		return NewJMSDest(name, dest.JMS, f.logger), nil
	case "sftp":
		if dest.SFTP == nil {
			return nil, fmt.Errorf("sftp destination config is nil for %s", name)
		}
		return NewSFTPDest(name, dest.SFTP, f.logger), nil
	case "fhir":
		if dest.FHIR == nil {
			return nil, fmt.Errorf("fhir destination config is nil for %s", name)
		}
		return NewFHIRDest(name, dest.FHIR, f.logger), nil
	case "direct":
		if dest.Direct == nil {
			return nil, fmt.Errorf("direct destination config is nil for %s", name)
		}
		return NewDirectDest(name, dest.Direct, f.logger), nil
	case "log":
		return NewLogDest(name, f.logger), nil
	default:
		return nil, fmt.Errorf("unsupported destination type: %s", dest.Type)
	}
}
