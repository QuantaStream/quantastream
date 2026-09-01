package qsmysql

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDecodeCommandPreparedStatementCommands(t *testing.T) {
	prepare, err := DecodeCommand(append([]byte{byte(CommandStmtPrepare)}, []byte("select o_orderkey from orders where o_orderkey = ?")...))
	if err != nil {
		t.Fatalf("DecodeCommand prepare failed: %v", err)
	}
	if prepare.Kind != CommandKindStmtPrepare || prepare.SQL == "" {
		t.Fatalf("prepare command = %#v", prepare)
	}

	executePayload := []byte{byte(CommandStmtExecute)}
	executePayload = appendUint32LE(executePayload, 42)
	executePayload = append(executePayload, 0)
	executePayload = appendUint32LE(executePayload, 1)
	executePayload = append(executePayload, 0, 0)
	execute, err := DecodeCommand(executePayload)
	if err != nil {
		t.Fatalf("DecodeCommand execute failed: %v", err)
	}
	if execute.Kind != CommandKindStmtExecute || execute.StatementID != 42 || execute.Execute.IterationCount != 1 {
		t.Fatalf("execute command = %#v", execute)
	}

	longDataPayload := []byte{byte(CommandStmtSendLongData)}
	longDataPayload = appendUint32LE(longDataPayload, 42)
	longDataPayload = appendUint16LE(longDataPayload, 1)
	longDataPayload = append(longDataPayload, []byte("chunk")...)
	longData, err := DecodeCommand(longDataPayload)
	if err != nil {
		t.Fatalf("DecodeCommand long data failed: %v", err)
	}
	if longData.Kind != CommandKindStmtSendLongData || longData.StatementID != 42 || longData.LongData.ParameterIndex() != 2 || string(longData.LongData.Data) != "chunk" {
		t.Fatalf("long data command = %#v", longData)
	}

	closeCommand, err := DecodeCommand([]byte{byte(CommandStmtClose), 42, 0, 0, 0})
	if err != nil {
		t.Fatalf("DecodeCommand close failed: %v", err)
	}
	if closeCommand.Kind != CommandKindStmtClose || closeCommand.StatementID != 42 {
		t.Fatalf("close command = %#v", closeCommand)
	}

	reset, err := DecodeCommand([]byte{byte(CommandStmtReset), 42, 0, 0, 0})
	if err != nil {
		t.Fatalf("DecodeCommand reset failed: %v", err)
	}
	if reset.Kind != CommandKindStmtReset || reset.StatementID != 42 {
		t.Fatalf("reset command = %#v", reset)
	}
}

func TestDecodePreparedExecuteParameters(t *testing.T) {
	payload := []byte{byte(CommandStmtExecute)}
	payload = appendUint32LE(payload, 7)
	payload = append(payload, 0)
	payload = appendUint32LE(payload, 1)
	payload = append(payload, 0b00000010, 1)
	payload = append(payload,
		byte(ColumnTypeLongLong), 0,
		byte(ColumnTypeVarString), 0,
		byte(ColumnTypeDouble), 0,
	)
	payload = appendUint64LE(payload, uint64(42))
	payload = appendLengthEncodedString(payload, "ignored because null")
	payload = payload[:len(payload)-len("ignored because null")-1]
	payload = appendUint64LE(payload, math.Float64bits(12.5))

	execute, err := DecodePreparedExecuteCommand(payload)
	if err != nil {
		t.Fatalf("DecodePreparedExecuteCommand failed: %v", err)
	}
	values, err := DecodePreparedExecuteParameters(execute, []qsbridge.ParameterRef{
		{Index: 1, Type: qsbridge.DataTypeInt},
		{Index: 2, Type: qsbridge.DataTypeString, Nullable: true},
		{Index: 3, Type: qsbridge.DataTypeFloat},
	})
	if err != nil {
		t.Fatalf("DecodePreparedExecuteParameters failed: %v", err)
	}
	if len(values) != 3 {
		t.Fatalf("values = %#v, want 3", values)
	}
	if values[0].Kind != qsbridge.ValueInt || values[0].Value != int64(42) {
		t.Fatalf("value 1 = %#v, want int64(42)", values[0])
	}
	if values[1].Kind != qsbridge.ValueNull || values[1].Value != nil {
		t.Fatalf("value 2 = %#v, want null", values[1])
	}
	if values[2].Kind != qsbridge.ValueFloat || values[2].Value != 12.5 {
		t.Fatalf("value 3 = %#v, want 12.5", values[2])
	}
}

func TestDecodePreparedExecuteParametersUsesCachedTypes(t *testing.T) {
	payload := []byte{byte(CommandStmtExecute)}
	payload = appendUint32LE(payload, 7)
	payload = append(payload, 0)
	payload = appendUint32LE(payload, 1)
	payload = append(payload, 0, 0)
	payload = appendUint64LE(payload, 99)

	execute, err := DecodePreparedExecuteCommand(payload)
	if err != nil {
		t.Fatalf("DecodePreparedExecuteCommand failed: %v", err)
	}
	values, types, err := DecodePreparedExecuteParametersWithOptions(execute, []qsbridge.ParameterRef{
		{Index: 1, Type: qsbridge.DataTypeInt},
	}, PreparedExecuteDecodeOptions{
		CachedTypes: []PreparedParameterType{{Type: ColumnTypeLongLong}},
	})
	if err != nil {
		t.Fatalf("DecodePreparedExecuteParametersWithOptions failed: %v", err)
	}
	if len(types) != 1 || types[0].Type != ColumnTypeLongLong {
		t.Fatalf("types = %#v, want cached longlong", types)
	}
	if len(values) != 1 || values[0].Value != int64(99) {
		t.Fatalf("values = %#v, want int64(99)", values)
	}
}

func TestDecodePreparedExecuteParametersSubstitutesLongData(t *testing.T) {
	payload := []byte{byte(CommandStmtExecute)}
	payload = appendUint32LE(payload, 7)
	payload = append(payload, 0)
	payload = appendUint32LE(payload, 1)
	payload = append(payload, 0, 1)
	payload = append(payload,
		byte(ColumnTypeString), 0,
		byte(ColumnTypeLongLong), 0,
	)
	payload = appendUint64LE(payload, 123)

	execute, err := DecodePreparedExecuteCommand(payload)
	if err != nil {
		t.Fatalf("DecodePreparedExecuteCommand failed: %v", err)
	}
	values, _, err := DecodePreparedExecuteParametersWithOptions(execute, []qsbridge.ParameterRef{
		{Index: 1, Type: qsbridge.DataTypeString},
		{Index: 2, Type: qsbridge.DataTypeInt},
	}, PreparedExecuteDecodeOptions{
		LongData: map[int][]byte{1: []byte("large payload")},
	})
	if err != nil {
		t.Fatalf("DecodePreparedExecuteParametersWithOptions failed: %v", err)
	}
	if len(values) != 2 || values[0].Kind != qsbridge.ValueString || values[0].Value != "large payload" || values[1].Value != int64(123) {
		t.Fatalf("values = %#v, want long data plus inline int", values)
	}
}

func TestDecodePreparedExecuteParametersScalarEdges(t *testing.T) {
	payload := []byte{byte(CommandStmtExecute)}
	payload = appendUint32LE(payload, 7)
	payload = append(payload, 0)
	payload = appendUint32LE(payload, 1)
	payload = append(payload, 0b00001000, 1)
	payload = append(payload,
		byte(ColumnTypeLongLong), 0x80,
		byte(ColumnTypeDateTime), 0,
		byte(ColumnTypeNewDecimal), 0,
		byte(ColumnTypeNull), 0,
	)
	payload = appendUint64LE(payload, uint64(1<<63)+5)
	payload = append(payload, 7)
	payload = appendUint16LE(payload, 2026)
	payload = append(payload, 8, 17, 12, 34, 56)
	payload = appendLengthEncodedString(payload, "12.34")

	execute, err := DecodePreparedExecuteCommand(payload)
	if err != nil {
		t.Fatalf("DecodePreparedExecuteCommand failed: %v", err)
	}
	values, _, err := DecodePreparedExecuteParametersWithOptions(execute, []qsbridge.ParameterRef{
		{Index: 1, Type: qsbridge.DataTypeInt},
		{Index: 2, Type: qsbridge.DataTypeTime},
		{Index: 3, Type: qsbridge.DataTypeString},
		{Index: 4, Type: qsbridge.DataTypeString, Nullable: true},
	}, PreparedExecuteDecodeOptions{})
	if err != nil {
		t.Fatalf("DecodePreparedExecuteParametersWithOptions failed: %v", err)
	}
	if values[0].Kind != qsbridge.ValueInt || values[0].Value != uint64(1<<63)+5 {
		t.Fatalf("unsigned value = %#v", values[0])
	}
	timestamp, ok := values[1].Value.(time.Time)
	if !ok || !timestamp.Equal(time.Date(2026, 8, 17, 12, 34, 56, 0, time.UTC)) {
		t.Fatalf("time value = %#v", values[1])
	}
	if values[2].Kind != qsbridge.ValueString || values[2].Value != "12.34" {
		t.Fatalf("decimal value = %#v", values[2])
	}
	if values[3].Kind != qsbridge.ValueNull || values[3].Value != nil {
		t.Fatalf("null value = %#v", values[3])
	}
}

func TestDecodePreparedExecuteParametersRejectsInvalidDateTime(t *testing.T) {
	payload := []byte{byte(CommandStmtExecute)}
	payload = appendUint32LE(payload, 7)
	payload = append(payload, 0)
	payload = appendUint32LE(payload, 1)
	payload = append(payload, 0, 1)
	payload = append(payload, byte(ColumnTypeDateTime), 0)
	payload = append(payload, 7)
	payload = appendUint16LE(payload, 2026)
	payload = append(payload, 2, 30, 12, 34, 56)

	execute, err := DecodePreparedExecuteCommand(payload)
	if err != nil {
		t.Fatalf("DecodePreparedExecuteCommand failed: %v", err)
	}
	_, _, err = DecodePreparedExecuteParametersWithOptions(execute, []qsbridge.ParameterRef{
		{Index: 1, Type: qsbridge.DataTypeTime},
	}, PreparedExecuteDecodeOptions{})
	if err == nil || !strings.Contains(err.Error(), "invalid datetime 2026-02-30") {
		t.Fatalf("DecodePreparedExecuteParametersWithOptions error = %v, want invalid datetime", err)
	}
}

func TestPreparedStatementResponseEncodesPrepareOKMetadata(t *testing.T) {
	response, err := PreparedStatementResponse(qsbridge.PreparedPlanDescription{
		Handle: qsbridge.PreparedStatementHandle{ID: 99},
		Parameters: []qsbridge.ParameterRef{
			{Index: 1, Type: qsbridge.DataTypeInt},
			{Index: 2, Type: qsbridge.DataTypeString, Nullable: true},
		},
		ResultColumns: []qsbridge.ResultColumn{{Name: "order_id", Type: qsbridge.DataTypeInt, Source: "orders.o_orderkey"}},
	})
	if err != nil {
		t.Fatalf("PreparedStatementResponse failed: %v", err)
	}
	if response.Kind != CommandResponsePrepared || len(response.Packets) != 6 {
		t.Fatalf("response = %#v, want prepare OK + params + columns", response)
	}
	first := response.Packets[0].Payload
	if first[0] != okPacketHeader || readUint32LE(first[1:5]) != 99 || readUint16LE(first[5:7]) != 1 || readUint16LE(first[7:9]) != 2 {
		t.Fatalf("prepare OK payload = %v", first)
	}
	if response.Packets[0].SequenceID != 1 || response.Packets[5].SequenceID != 6 {
		t.Fatalf("packet sequence ids = %#v", response.Packets)
	}
}

func TestBinaryResultSetPacketsEncodeBinaryRows(t *testing.T) {
	result := qsbridge.ExecutionResult{
		Kind: qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{
			{Name: "id", Type: qsbridge.DataTypeInt},
			{Name: "created_at", Type: qsbridge.DataTypeTime},
			{Name: "name", Type: qsbridge.DataTypeString, Nullable: true},
		},
	}
	result = result.WithChunk(qsbridge.ResultChunk{
		Rows: []qsbridge.ResultRow{
			{
				{Kind: qsbridge.ValueInt, Value: int64(7)},
				{Kind: qsbridge.ValueTime, Value: time.Date(2026, 8, 17, 12, 34, 56, 0, time.UTC)},
				{Kind: qsbridge.ValueString, Value: "Abe"},
			},
			{
				{Kind: qsbridge.ValueInt, Value: int64(8)},
				{Kind: qsbridge.ValueTime, Value: time.Time{}},
				{Kind: qsbridge.ValueNull},
			},
		},
		Final: true,
	})

	packets, err := BinaryResultSetPackets(result)
	if err != nil {
		t.Fatalf("BinaryResultSetPackets failed: %v", err)
	}
	if len(packets) != 8 {
		t.Fatalf("packet count = %d, want 8", len(packets))
	}
	firstRow := packets[5].Payload
	wantPrefix := []byte{0, 0}
	wantPrefix = appendUint64LE(wantPrefix, 7)
	wantPrefix = append(wantPrefix, 7, 0xea, 0x07, 8, 17, 12, 34, 56, 3, 'A', 'b', 'e')
	if !bytes.Equal(firstRow, wantPrefix) {
		t.Fatalf("first binary row = %v, want %v", firstRow, wantPrefix)
	}
	secondRow := packets[6].Payload
	if len(secondRow) < 2 || secondRow[0] != 0 || secondRow[1] != 0b00010000 {
		t.Fatalf("second binary row null bitmap = %v", secondRow)
	}
}
