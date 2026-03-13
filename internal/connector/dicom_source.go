package connector

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu-dev/internal/auth"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type DICOMSource struct {
	cfg      *config.DICOMListener
	listener net.Listener
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewDICOMSource(cfg *config.DICOMListener, logger *slog.Logger) *DICOMSource {
	return &DICOMSource{cfg: cfg, logger: logger}
}

func (d *DICOMSource) Start(ctx context.Context, handler MessageHandler) error {
	addr := ":" + strconv.Itoa(d.cfg.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("DICOM listen on %s: %w", addr, err)
	}

	tlsEnabled := false
	if d.cfg.TLS != nil && d.cfg.TLS.Enabled {
		tlsCfg, tlsErr := auth.BuildTLSConfig(d.cfg.TLS)
		if tlsErr != nil {
			ln.Close()
			return fmt.Errorf("DICOM TLS config: %w", tlsErr)
		}
		ln = tls.NewListener(ln, tlsCfg)
		tlsEnabled = true
	}

	d.listener = ln
	ctx, d.cancel = context.WithCancel(ctx)

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					d.logger.Error("DICOM accept error", "error", err)
					continue
				}
			}
			d.wg.Add(1)
			go func(c net.Conn) {
				defer d.wg.Done()
				defer c.Close()
				d.handleConnection(ctx, c, handler)
			}(conn)
		}
	}()

	aeTitle := d.cfg.AETitle
	if aeTitle == "" {
		aeTitle = "INTU_SCP"
	}

	d.logger.Info("DICOM source (SCP) started",
		"addr", addr,
		"ae_title", aeTitle,
		"tls", tlsEnabled,
	)
	return nil
}

func (d *DICOMSource) handleConnection(ctx context.Context, conn net.Conn, handler MessageHandler) {
	conn.SetDeadline(time.Now().Add(60 * time.Second))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		pduType, pduData, err := d.readPDU(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return
			}
			d.logger.Debug("DICOM read error", "error", err)
			return
		}

		switch pduType {
		case 0x01:
			callingAE := d.extractCallingAETitle(pduData)
			d.logger.Debug("DICOM A-ASSOCIATE-RQ received", "size", len(pduData), "calling_ae", callingAE)
			if !d.validateCallingAETitle(callingAE) {
				d.logger.Warn("DICOM A-ASSOCIATE rejected: unauthorized calling AE Title", "calling_ae", callingAE)
				d.sendAssociateRJ(conn)
				return
			}
			d.sendAssociateAC(conn)

		case 0x04:
			d.logger.Debug("DICOM P-DATA-TF received", "size", len(pduData))
			msg := message.New("", pduData)
			msg.Transport = "dicom"
			msg.ContentType = "dicom"
			msg.DICOM = &message.DICOMMeta{
				CalledAE: d.cfg.AETitle,
			}
			msg.Metadata["source"] = "dicom"
			msg.Metadata["ae_title"] = d.cfg.AETitle
			msg.Metadata["remote_addr"] = conn.RemoteAddr().String()
			msg.Metadata["pdu_type"] = fmt.Sprintf("0x%02X", pduType)

			if err := handler(ctx, msg); err != nil {
				d.logger.Error("DICOM handler error", "error", err)
			}

		case 0x05:
			d.logger.Debug("DICOM A-RELEASE-RQ received")
			d.sendReleaseRP(conn)
			return

		case 0x07:
			d.logger.Debug("DICOM A-ABORT received")
			return

		default:
			d.logger.Debug("DICOM unknown PDU type", "type", fmt.Sprintf("0x%02X", pduType))
			msg := message.New("", pduData)
			msg.Transport = "dicom"
			msg.ContentType = "dicom"
			msg.DICOM = &message.DICOMMeta{
				CalledAE: d.cfg.AETitle,
			}
			msg.Metadata["source"] = "dicom"
			msg.Metadata["pdu_type"] = fmt.Sprintf("0x%02X", pduType)

			if err := handler(ctx, msg); err != nil {
				d.logger.Error("DICOM handler error", "error", err)
			}
		}

		conn.SetDeadline(time.Now().Add(60 * time.Second))
	}
}

func (d *DICOMSource) readPDU(conn net.Conn) (byte, []byte, error) {
	header := make([]byte, 6)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}

	pduType := header[0]
	pduLength := binary.BigEndian.Uint32(header[2:6])

	if pduLength > 16*1024*1024 {
		return pduType, nil, fmt.Errorf("PDU too large: %d bytes", pduLength)
	}

	data := make([]byte, pduLength)
	if _, err := io.ReadFull(conn, data); err != nil {
		return pduType, nil, fmt.Errorf("read PDU body: %w", err)
	}

	return pduType, data, nil
}

func (d *DICOMSource) extractCallingAETitle(data []byte) string {
	// A-ASSOCIATE-RQ: bytes 0-1=protocol version, 2-3=reserved,
	// 4-19=called AE title (16 bytes), 20-35=calling AE title (16 bytes)
	if len(data) < 36 {
		return ""
	}
	return strings.TrimSpace(string(data[20:36]))
}

func (d *DICOMSource) validateCallingAETitle(callingAE string) bool {
	if len(d.cfg.CallingAETitles) == 0 {
		return true
	}
	for _, allowed := range d.cfg.CallingAETitles {
		if strings.EqualFold(allowed, callingAE) {
			return true
		}
	}
	return false
}

func (d *DICOMSource) sendAssociateRJ(conn net.Conn) {
	pdu := make([]byte, 10)
	pdu[0] = 0x03 // A-ASSOCIATE-RJ
	pdu[1] = 0x00
	binary.BigEndian.PutUint32(pdu[2:6], 4)
	pdu[7] = 0x01 // result: rejected-permanent
	pdu[8] = 0x01 // source: DICOM UL service-user
	pdu[9] = 0x03 // reason: calling AE title not recognized
	conn.Write(pdu)
}

func (d *DICOMSource) sendAssociateAC(conn net.Conn) {
	aeTitle := d.cfg.AETitle
	if aeTitle == "" {
		aeTitle = "INTU_SCP"
	}

	paddedAE := fmt.Sprintf("%-16s", aeTitle)
	calledAE := paddedAE
	callingAE := paddedAE

	var pduData []byte
	pduData = append(pduData, 0x00, 0x01)
	pduData = append(pduData, 0x00, 0x00)
	pduData = append(pduData, []byte(calledAE)...)
	pduData = append(pduData, []byte(callingAE)...)
	pduData = append(pduData, make([]byte, 32)...)

	pdu := make([]byte, 6+len(pduData))
	pdu[0] = 0x02
	pdu[1] = 0x00
	binary.BigEndian.PutUint32(pdu[2:6], uint32(len(pduData)))
	copy(pdu[6:], pduData)

	conn.Write(pdu)
}

func (d *DICOMSource) sendReleaseRP(conn net.Conn) {
	pdu := make([]byte, 10)
	pdu[0] = 0x06
	pdu[1] = 0x00
	binary.BigEndian.PutUint32(pdu[2:6], 4)
	conn.Write(pdu)
}

func (d *DICOMSource) Addr() string {
	if d.listener != nil {
		return d.listener.Addr().String()
	}
	return ""
}

func (d *DICOMSource) Stop(ctx context.Context) error {
	if d.cancel != nil {
		d.cancel()
	}
	if d.listener != nil {
		d.listener.Close()
	}
	d.wg.Wait()
	return nil
}

func (d *DICOMSource) Type() string {
	return "dicom"
}
