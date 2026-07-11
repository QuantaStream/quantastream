package qsmysql

import (
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

const (
	okPacketHeader  = 0x00
	errPacketHeader = 0xff
	sqlStateMarker  = '#'
)

// OKPacket returns a MySQL OK packet for a statement-style response.
func OKPacket(sequenceID byte, statement qsbridge.StatementResult) Packet {
	return Packet{SequenceID: sequenceID, Payload: OKPayload(statement)}
}

// OKPayload encodes statement metadata as a MySQL OK packet payload.
func OKPayload(statement qsbridge.StatementResult) []byte {
	payload := []byte{okPacketHeader}
	payload = appendLengthEncodedInteger(payload, statement.AffectedRows)
	payload = appendLengthEncodedInteger(payload, statement.LastInsertID)
	payload = appendUint16LE(payload, uint16(StatusAutocommit))
	payload = appendUint16LE(payload, statement.Warnings)
	if statement.Status != "" {
		payload = appendLengthEncodedString(payload, statement.Status)
	}
	return payload
}

// ERRPacket returns a MySQL ERR packet for a protocol error.
func ERRPacket(sequenceID byte, protocolError qsbridge.ProtocolError) Packet {
	return Packet{SequenceID: sequenceID, Payload: ERRPayload(protocolError)}
}

// ERRPayload encodes a protocol error as a MySQL ERR packet payload.
func ERRPayload(protocolError qsbridge.ProtocolError) []byte {
	vendorCode := protocolError.VendorCode
	if vendorCode == 0 {
		vendorCode = 1105
	}
	message := protocolError.Message
	if message == "" {
		message = "unknown error"
	}
	sqlState := normalizedSQLState(protocolError.SQLState)
	payload := []byte{errPacketHeader}
	payload = appendUint16LE(payload, uint16(vendorCode))
	payload = append(payload, sqlStateMarker)
	payload = append(payload, []byte(sqlState)...)
	payload = append(payload, []byte(message)...)
	return payload
}

// ProtocolErrorFromError converts an ordinary Go error into a generic MySQL protocol error.
func ProtocolErrorFromError(err error) qsbridge.ProtocolError {
	message := "unknown error"
	if err != nil {
		message = err.Error()
	}
	return qsbridge.ProtocolError{
		SQLState:   qsbridge.SQLStateGeneralError,
		VendorCode: 1105,
		Message:    message,
	}
}

func normalizedSQLState(sqlState qsbridge.SQLState) string {
	value := strings.TrimSpace(string(sqlState))
	if len(value) != 5 {
		return string(qsbridge.SQLStateGeneralError)
	}
	return value
}
