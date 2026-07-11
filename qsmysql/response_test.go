package qsmysql

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestCommandResponsesEncodeExpectedPacketShapes(t *testing.T) {
	ok := StatementOKResponse(qsbridge.StatementResult{AffectedRows: 2})
	if ok.Kind != CommandResponseOK || len(ok.Packets) != 1 || ok.Packets[0].Payload[0] != okPacketHeader {
		t.Fatalf("OK response = %#v", ok)
	}

	err := ErrorResponse(qsbridge.ProtocolError{Message: "bad"})
	if err.Kind != CommandResponseError || len(err.Packets) != 1 || err.Packets[0].Payload[0] != errPacketHeader {
		t.Fatalf("ERR response = %#v", err)
	}

	quit := QuitResponse()
	if quit.Kind != CommandResponseClose || !quit.Close || len(quit.Packets) != 0 {
		t.Fatalf("quit response = %#v", quit)
	}
}

func TestQueryResponseUsesTextResultSetPackets(t *testing.T) {
	result := qsbridge.ExecutionResult{
		Kind:    qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{{Name: "id", Type: qsbridge.DataTypeInt}},
	}
	result = result.WithChunk(qsbridge.ResultChunk{Rows: []qsbridge.ResultRow{{{Kind: qsbridge.ValueInt, Value: int64(7)}}}, Final: true})

	response, err := QueryResponse(result)
	if err != nil {
		t.Fatalf("QueryResponse failed: %v", err)
	}
	if response.Kind != CommandResponseQuery || len(response.Packets) != 5 {
		t.Fatalf("query response = %#v", response)
	}
}
