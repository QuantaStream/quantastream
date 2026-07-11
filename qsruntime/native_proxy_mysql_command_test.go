package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

func TestNativeProxyFrontDoorHandlesPingAndQuitCommands(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{})

	ping, err := frontDoor.HandleMySQLCommand(context.Background(), qsmysql.Command{Kind: qsmysql.CommandKindPing}, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("HandleMySQLCommand ping failed: %v", err)
	}
	if ping.Kind != qsmysql.CommandResponseOK || len(ping.Packets) != 1 {
		t.Fatalf("ping response = %#v", ping)
	}

	quit, err := frontDoor.HandleMySQLCommand(context.Background(), qsmysql.Command{Kind: qsmysql.CommandKindQuit}, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("HandleMySQLCommand quit failed: %v", err)
	}
	if quit.Kind != qsmysql.CommandResponseClose || !quit.Close {
		t.Fatalf("quit response = %#v", quit)
	}
}

func TestNativeProxyFrontDoorHandlesQueryCommandAsResultset(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "orders",
			Rownums: []qsbridge.QuantaRownum{7},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Field: qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(7)},
				},
			}},
		}}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})

	response, err := frontDoor.HandleMySQLCommand(
		context.Background(),
		qsmysql.Command{Kind: qsmysql.CommandKindQuery, SQL: "select o_orderkey from orders where o_orderkey >= 7"},
		qsbridge.ExecutionOptions{},
	)
	if err != nil {
		t.Fatalf("HandleMySQLCommand query failed: %v", err)
	}
	if response.Kind != qsmysql.CommandResponseQuery || len(response.Packets) != 5 {
		t.Fatalf("query response = %#v", response)
	}
	if string(response.Packets[3].Payload) != "\x017" {
		t.Fatalf("row payload = %v", response.Packets[3].Payload)
	}
}

func TestNativeProxyFrontDoorHandlesQueryDiagnosticsAsError(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{Runtime: newTestSQLRuntime(t)}, NativeProxyFrontDoorConfig{})

	response, err := frontDoor.HandleMySQLCommand(
		context.Background(),
		qsmysql.Command{Kind: qsmysql.CommandKindQuery, SQL: "select from"},
		qsbridge.ExecutionOptions{},
	)
	if err != nil {
		t.Fatalf("HandleMySQLCommand query failed: %v", err)
	}
	if response.Kind != qsmysql.CommandResponseError || len(response.Packets) != 1 || response.Packets[0].Payload[0] != 0xff {
		t.Fatalf("error response = %#v", response)
	}
}

func TestNativeProxyFrontDoorHandlesMySQLMetadataSelectsAtProtocolBoundary(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{Runtime: newTestSQLRuntime(t)}, NativeProxyFrontDoorConfig{})

	tests := []struct {
		sql     string
		payload string
	}{
		{sql: "SELECT @@max_allowed_packet", payload: "\x0867108864"},
		{sql: "select database()", payload: "\x09analytics"},
		{sql: "select version()", payload: "\x128.0.0-quantastream"},
		{sql: "select connection_id()", payload: "\x0242"},
	}
	for _, test := range tests {
		response, err := frontDoor.HandleMySQLCommand(
			context.Background(),
			qsmysql.Command{Kind: qsmysql.CommandKindQuery, SQL: test.sql, ConnectionID: 42, Database: "analytics"},
			qsbridge.ExecutionOptions{},
		)
		if err != nil {
			t.Fatalf("HandleMySQLCommand(%q) failed: %v", test.sql, err)
		}
		if response.Kind != qsmysql.CommandResponseQuery || len(response.Packets) != 5 {
			t.Fatalf("response for %q = %#v", test.sql, response)
		}
		if string(response.Packets[3].Payload) != test.payload {
			t.Fatalf("row payload for %q = %q, want %q", test.sql, string(response.Packets[3].Payload), test.payload)
		}
	}
}

func TestNativeProxyFrontDoorServeMySQLCommandUsesPacketLoop(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "orders",
			Rownums: []qsbridge.QuantaRownum{7},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Field:  qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(7)}},
			}},
		}}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	reader := &nativeProxyTestPacketReader{packets: []qsmysql.Packet{{Payload: append([]byte{byte(qsmysql.CommandQuery)}, []byte("select o_orderkey from orders where o_orderkey >= 7")...)}}}
	writer := &nativeProxyTestPacketWriter{}

	response, err := frontDoor.ServeMySQLCommand(context.Background(), reader, writer, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ServeMySQLCommand failed: %v", err)
	}
	if response.Kind != qsmysql.CommandResponseQuery || len(writer.packets) != 5 || string(writer.packets[3].Payload) != "\x017" {
		t.Fatalf("response = %#v written=%#v", response, writer.packets)
	}
}

type nativeProxyTestPacketReader struct {
	packets []qsmysql.Packet
}

func (r *nativeProxyTestPacketReader) ReadPacket(ctx context.Context) (qsmysql.Packet, error) {
	packet := r.packets[0]
	r.packets = r.packets[1:]
	return packet, nil
}

type nativeProxyTestPacketWriter struct {
	packets []qsmysql.Packet
}

func (w *nativeProxyTestPacketWriter) WritePacket(ctx context.Context, packet qsmysql.Packet) error {
	w.packets = append(w.packets, packet)
	return nil
}
