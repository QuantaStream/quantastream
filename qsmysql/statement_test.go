package qsmysql

import (
	"bytes"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestOKPayloadEncodesStatementMetadata(t *testing.T) {
	payload := OKPayload(qsbridge.StatementResult{
		AffectedRows: 3,
		LastInsertID: 42,
		Warnings:     2,
		Status:       "Rows matched",
	})

	want := []byte{okPacketHeader, 3, 42, byte(StatusAutocommit), 0, 2, 0, 12}
	want = append(want, []byte("Rows matched")...)
	if !bytes.Equal(payload, want) {
		t.Fatalf("OK payload = %v, want %v", payload, want)
	}
}

func TestOKPayloadOmitsEmptyInfoWithoutSessionTrack(t *testing.T) {
	payload := OKPayload(qsbridge.StatementResult{})
	if !bytes.Equal(payload, []byte{okPacketHeader, 0, 0, byte(StatusAutocommit), 0, 0, 0}) {
		t.Fatalf("OK payload = %v, want compact empty OK", payload)
	}
}

func TestOKPayloadWithSessionTrackIncludesEmptyInfo(t *testing.T) {
	payload := OKPayloadWithCapabilities(qsbridge.StatementResult{}, CapabilitySessionTrack)
	if !bytes.Equal(payload, []byte{okPacketHeader, 0, 0, byte(StatusAutocommit), 0, 0, 0, 0}) {
		t.Fatalf("OK payload = %v, want session-track empty info field", payload)
	}
}

func TestOKPayloadWithSessionTrackLengthEncodesStatusInfo(t *testing.T) {
	payload := OKPayloadWithCapabilities(qsbridge.StatementResult{Status: "View sample_view created"}, CapabilitySessionTrack)
	want := []byte{okPacketHeader, 0, 0, byte(StatusAutocommit), 0, 0, 0, 24}
	want = append(want, []byte("View sample_view created")...)
	if !bytes.Equal(payload, want) {
		t.Fatalf("OK payload = %v, want %v", payload, want)
	}
}

func TestERRPayloadEncodesProtocolError(t *testing.T) {
	payload := ERRPayload(qsbridge.ProtocolError{
		SQLState:   qsbridge.SQLStateSyntaxError,
		VendorCode: 1064,
		Message:    "bad SQL",
	})

	wantPrefix := []byte{errPacketHeader, 0x28, 0x04, sqlStateMarker, '4', '2', '0', '0', '0'}
	if !bytes.Equal(payload[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("ERR prefix = %v, want %v", payload[:len(wantPrefix)], wantPrefix)
	}
	if string(payload[len(wantPrefix):]) != "bad SQL" {
		t.Fatalf("ERR message = %q", payload[len(wantPrefix):])
	}
}

func TestERRPayloadDefaultsInvalidSQLStateAndCode(t *testing.T) {
	payload := ERRPayload(qsbridge.ProtocolError{Message: "boom"})
	if !bytes.Equal(payload[:9], []byte{errPacketHeader, 0x51, 0x04, sqlStateMarker, 'H', 'Y', '0', '0', '0'}) {
		t.Fatalf("ERR default prefix = %v", payload[:9])
	}
}
