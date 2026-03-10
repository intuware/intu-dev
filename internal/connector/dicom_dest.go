package connector

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type DICOMDest struct {
	name   string
	cfg    *config.DICOMDestMapConfig
	logger *slog.Logger
}

func NewDICOMDest(name string, cfg *config.DICOMDestMapConfig, logger *slog.Logger) *DICOMDest {
	return &DICOMDest{name: name, cfg: cfg, logger: logger}
}

func (d *DICOMDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	if d.cfg.Host == "" {
		return &message.Response{StatusCode: 400, Error: fmt.Errorf("dicom destination %s: host not configured", d.name)}, nil
	}

	port := d.cfg.Port
	if port == 0 {
		port = 104
	}
	addr := net.JoinHostPort(d.cfg.Host, fmt.Sprintf("%d", port))

	timeout := 30 * time.Second
	if d.cfg.TimeoutMs > 0 {
		timeout = time.Duration(d.cfg.TimeoutMs) * time.Millisecond
	}

	var conn net.Conn
	var err error

	if d.cfg.TLS != nil && d.cfg.TLS.Enabled {
		tlsCfg, tlsErr := auth.BuildTLSConfigFromMap(d.cfg.TLS)
		if tlsErr != nil {
			return &message.Response{StatusCode: 502, Error: fmt.Errorf("dicom TLS config: %w", tlsErr)}, nil
		}
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, tlsCfg)
	} else {
		conn, err = net.DialTimeout("tcp", addr, timeout)
	}

	if err != nil {
		d.logger.Error("dicom dest connect failed", "destination", d.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("dicom connect to %s: %w", addr, err)}, nil
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(timeout))

	if err := d.sendAssociateRQ(conn); err != nil {
		d.logger.Error("dicom dest A-ASSOCIATE-RQ failed", "destination", d.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("dicom associate: %w", err)}, nil
	}

	pduType, _, err := d.readPDU(conn)
	if err != nil {
		d.logger.Error("dicom dest read associate response failed", "destination", d.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("dicom read associate response: %w", err)}, nil
	}

	if pduType == 0x03 {
		d.logger.Error("dicom dest association rejected", "destination", d.name)
		return &message.Response{StatusCode: 403, Error: fmt.Errorf("dicom association rejected by remote SCP")}, nil
	}

	if pduType != 0x02 {
		d.logger.Error("dicom dest unexpected PDU type", "destination", d.name, "type", fmt.Sprintf("0x%02X", pduType))
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("dicom unexpected PDU type: 0x%02X", pduType)}, nil
	}

	conn.SetDeadline(time.Now().Add(timeout))
	if err := d.sendPData(conn, msg.Raw); err != nil {
		d.logger.Error("dicom dest P-DATA send failed", "destination", d.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("dicom P-DATA send: %w", err)}, nil
	}

	d.sendReleaseRQ(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	d.readPDU(conn) // read A-RELEASE-RP, ignore errors

	d.logger.Debug("dicom dest message sent",
		"destination", d.name,
		"host", addr,
		"bytes", len(msg.Raw),
	)

	body, _ := json.Marshal(map[string]any{"status": "sent", "ae_title": d.callingAE()})
	return &message.Response{StatusCode: 200, Body: body}, nil
}

func (d *DICOMDest) callingAE() string {
	if d.cfg.AETitle != "" {
		return d.cfg.AETitle
	}
	return "INTU_SCU"
}

func (d *DICOMDest) calledAE() string {
	if d.cfg.CalledAETitle != "" {
		return d.cfg.CalledAETitle
	}
	return "ANY_SCP"
}

func (d *DICOMDest) sendAssociateRQ(conn net.Conn) error {
	callingAE := fmt.Sprintf("%-16s", d.callingAE())
	calledAE := fmt.Sprintf("%-16s", d.calledAE())

	var pduData []byte
	pduData = append(pduData, 0x00, 0x01) // protocol version
	pduData = append(pduData, 0x00, 0x00) // reserved
	pduData = append(pduData, []byte(calledAE)...)
	pduData = append(pduData, []byte(callingAE)...)
	pduData = append(pduData, make([]byte, 32)...) // reserved

	pdu := make([]byte, 6+len(pduData))
	pdu[0] = 0x01 // A-ASSOCIATE-RQ
	pdu[1] = 0x00
	binary.BigEndian.PutUint32(pdu[2:6], uint32(len(pduData)))
	copy(pdu[6:], pduData)

	_, err := conn.Write(pdu)
	return err
}

func (d *DICOMDest) sendPData(conn net.Conn, data []byte) error {
	pdu := make([]byte, 6+len(data))
	pdu[0] = 0x04 // P-DATA-TF
	pdu[1] = 0x00
	binary.BigEndian.PutUint32(pdu[2:6], uint32(len(data)))
	copy(pdu[6:], data)

	_, err := conn.Write(pdu)
	return err
}

func (d *DICOMDest) sendReleaseRQ(conn net.Conn) {
	pdu := make([]byte, 10)
	pdu[0] = 0x05 // A-RELEASE-RQ
	pdu[1] = 0x00
	binary.BigEndian.PutUint32(pdu[2:6], 4)
	conn.Write(pdu)
}

func (d *DICOMDest) readPDU(conn net.Conn) (byte, []byte, error) {
	header := make([]byte, 6)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}

	pduType := header[0]
	pduLength := binary.BigEndian.Uint32(header[2:6])

	if pduLength > 16*1024*1024 {
		return pduType, nil, fmt.Errorf("PDU too large: %d bytes", pduLength)
	}

	if pduLength > 0 {
		data := make([]byte, pduLength)
		if _, err := io.ReadFull(conn, data); err != nil {
			return pduType, nil, fmt.Errorf("read PDU body: %w", err)
		}
		return pduType, data, nil
	}

	return pduType, nil, nil
}

func (d *DICOMDest) Stop(ctx context.Context) error {
	return nil
}

func (d *DICOMDest) Type() string {
	return "dicom"
}
