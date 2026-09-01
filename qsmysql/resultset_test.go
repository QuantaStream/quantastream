package qsmysql

import (
	"bytes"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestTextResultSetPacketsEncodeColumnDefinitionsAndRows(t *testing.T) {
	result := qsbridge.ExecutionResult{
		Kind: qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{
			{Name: "id", Type: qsbridge.DataTypeInt, Source: "orders.o_orderkey"},
			{Name: "name", Type: qsbridge.DataTypeString, Nullable: true, Source: "customer.c_name"},
			{Name: "active", Type: qsbridge.DataTypeBool},
		},
	}
	result = result.WithChunk(qsbridge.ResultChunk{
		Rows: []qsbridge.ResultRow{
			{
				{Kind: qsbridge.ValueInt, Value: int64(7)},
				{Kind: qsbridge.ValueString, Value: "Abe"},
				{Kind: qsbridge.ValueBool, Value: true},
			},
			{
				{Kind: qsbridge.ValueInt, Value: int64(8)},
				{Kind: qsbridge.ValueNull},
				{Kind: qsbridge.ValueBool, Value: false},
			},
		},
		Final: true,
	})

	packets, err := TextResultSetPackets(result)
	if err != nil {
		t.Fatalf("TextResultSetPackets failed: %v", err)
	}
	if len(packets) != 8 {
		t.Fatalf("packet count = %d, want 8", len(packets))
	}
	for i, packet := range packets {
		if packet.SequenceID != byte(i+1) {
			t.Fatalf("packet %d sequence = %d, want %d", i, packet.SequenceID, i+1)
		}
	}
	if !bytes.Equal(packets[0].Payload, []byte{3}) {
		t.Fatalf("column count payload = %v, want 3", packets[0].Payload)
	}
	if !bytes.Contains(packets[1].Payload, []byte("orders")) || !bytes.Contains(packets[1].Payload, []byte("o_orderkey")) {
		t.Fatalf("id column definition payload = %q, want source metadata", packets[1].Payload)
	}
	typeOffset := len(packets[1].Payload) - 6
	if packets[1].Payload[typeOffset] != byte(ColumnTypeLongLong) {
		t.Fatalf("id column type = %x, want longlong", packets[1].Payload[typeOffset])
	}
	if !bytes.Equal(packets[4].Payload, []byte{eofPacketHeader, 0, 0, byte(StatusAutocommit), 0}) {
		t.Fatalf("first EOF payload = %v", packets[4].Payload)
	}
	if !bytes.Equal(packets[5].Payload, []byte{1, '7', 3, 'A', 'b', 'e', 1, '1'}) {
		t.Fatalf("first row payload = %v", packets[5].Payload)
	}
	if !bytes.Equal(packets[6].Payload, []byte{1, '8', nullTextValue, 1, '0'}) {
		t.Fatalf("second row payload = %v", packets[6].Payload)
	}
}

func TestTextResultSetPacketsWithOptionsAnnotatesTableColumnsWithSchema(t *testing.T) {
	result := qsbridge.ExecutionResult{
		Kind: qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{{
			Name:   "row_id",
			Type:   qsbridge.DataTypeInt,
			Source: "superstore_orders.row_id",
		}},
	}
	result = result.WithChunk(qsbridge.ResultChunk{
		Rows:  []qsbridge.ResultRow{{{Kind: qsbridge.ValueInt, Value: int64(1)}}},
		Final: true,
	})

	packets, err := TextResultSetPacketsWithOptions(result, ResultSetOptions{DefaultSchema: "quanta"})
	if err != nil {
		t.Fatalf("TextResultSetPacketsWithOptions failed: %v", err)
	}
	column := packets[1].Payload
	for _, want := range [][]byte{[]byte("def"), []byte("quanta"), []byte("superstore_orders"), []byte("row_id")} {
		if !bytes.Contains(column, want) {
			t.Fatalf("column definition payload = %q, want %q", column, want)
		}
	}
}

func TestTextResultSetPacketsWithOptionsLeavesDerivedColumnsUnscoped(t *testing.T) {
	result := qsbridge.ExecutionResult{
		Kind:    qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{{Name: "COL", Type: qsbridge.DataTypeInt}},
	}
	result = result.WithChunk(qsbridge.ResultChunk{
		Rows:  []qsbridge.ResultRow{{{Kind: qsbridge.ValueInt, Value: int64(1)}}},
		Final: true,
	})

	packets, err := TextResultSetPacketsWithOptions(result, ResultSetOptions{DefaultSchema: "quanta"})
	if err != nil {
		t.Fatalf("TextResultSetPacketsWithOptions failed: %v", err)
	}
	column := packets[1].Payload
	if bytes.Contains(column, []byte("quanta")) {
		t.Fatalf("derived column definition payload = %q, want no default schema", column)
	}
}

func TestFieldListPacketsWithOptionsEncodeColumnDefinitionsWithoutResultHeader(t *testing.T) {
	result := qsbridge.ExecutionResult{
		Kind: qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{
			{Name: "row_id", Type: qsbridge.DataTypeInt, Source: "superstore_orders.row_id"},
			{Name: "order_id", Type: qsbridge.DataTypeString, Nullable: true, Source: "superstore_orders.order_id"},
			{Name: "customer_name", Type: qsbridge.DataTypeString, Nullable: true, Source: "superstore_orders.customer_name"},
		},
	}

	packets, err := FieldListPacketsWithOptions(result, ResultSetOptions{DefaultSchema: "quanta"}, "order_%")
	if err != nil {
		t.Fatalf("FieldListPacketsWithOptions failed: %v", err)
	}
	if len(packets) != 2 {
		t.Fatalf("packet count = %d, want one definition plus EOF", len(packets))
	}
	if packets[0].SequenceID != 1 || packets[1].SequenceID != 2 {
		t.Fatalf("packet sequence ids = %#v", packets)
	}
	if bytes.Equal(packets[0].Payload, []byte{1}) {
		t.Fatalf("first packet looks like a result-set column count: %v", packets[0].Payload)
	}
	for _, want := range [][]byte{[]byte("quanta"), []byte("superstore_orders"), []byte("order_id")} {
		if !bytes.Contains(packets[0].Payload, want) {
			t.Fatalf("field-list column payload = %q, want %q", packets[0].Payload, want)
		}
	}
	if packets[0].Payload[len(packets[0].Payload)-1] != nullTextValue {
		t.Fatalf("field-list column payload = %v, want trailing NULL default value", packets[0].Payload)
	}
	if !bytes.Equal(packets[1].Payload, []byte{eofPacketHeader, 0, 0, byte(StatusAutocommit), 0}) {
		t.Fatalf("EOF payload = %v", packets[1].Payload)
	}
}

func TestTextResultSetPacketsRejectRowWidthMismatch(t *testing.T) {
	result := qsbridge.ExecutionResult{
		Kind:    qsbridge.ResultQuery,
		Columns: []qsbridge.ResultColumn{{Name: "id", Type: qsbridge.DataTypeInt}},
	}
	result = result.WithChunk(qsbridge.ResultChunk{Rows: []qsbridge.ResultRow{{}}})
	if _, err := TextResultSetPackets(result); err == nil {
		t.Fatal("expected row width mismatch to fail")
	}
}

func TestTextResultSetPacketsRejectMissingColumns(t *testing.T) {
	if _, err := TextResultSetPackets(qsbridge.ExecutionResult{Kind: qsbridge.ResultQuery}); err == nil {
		t.Fatal("expected missing columns to fail")
	}
}

func TestTextCellValueFormatsCoreTypes(t *testing.T) {
	tests := []struct {
		name string
		cell qsbridge.ResultCell
		want string
	}{
		{name: "int", cell: qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: uint64(42)}, want: "42"},
		{name: "float", cell: qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: 12.5}, want: "12.5"},
		{name: "bool", cell: qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: true}, want: "1"},
		{name: "time", cell: qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: time.Date(2026, 7, 11, 12, 30, 45, 0, time.UTC)}, want: "2026-07-11 12:30:45"},
		{name: "string", cell: qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: "mail"}, want: "mail"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := textCellValue(tt.cell)
			if err != nil {
				t.Fatalf("textCellValue failed: %v", err)
			}
			if got != tt.want {
				t.Fatalf("textCellValue = %q, want %q", got, tt.want)
			}
		})
	}
}
