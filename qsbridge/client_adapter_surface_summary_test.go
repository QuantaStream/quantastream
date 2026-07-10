package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientAdapterSurfacesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientAdapterSurfaces(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter surface summary", exchange)
	}
	row := exchange.Row
	if row.SurfaceCount != 4 || row.ClientFacingCount != 2 || row.ControlPlaneCount != 1 {
		t.Fatalf("row = %#v, want adapter surface counts", row)
	}
	if row.EmbeddedCount != 1 || row.InternalCount != 1 || row.UsesQSBridgeCount != 4 {
		t.Fatalf("row = %#v, want placement and metadata counts", row)
	}
	if row.MySQLProtocolCount != 1 || row.GRPCProtocolCount != 1 || row.InProcessTransportCount != 1 {
		t.Fatalf("row = %#v, want protocol and transport counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("result/schema = %#v/%#v, want one-row adapter surface summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 4 || resultRow[1].Value != 2 || resultRow[5].Value != 4 {
		t.Fatalf("result row = %#v, want adapter surface summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientAdapterSurfacesFiltersAudience(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientAdapterSurfaces(connection, AdapterSurfaceAudienceEmbedded)
	row := exchange.Row
	if row.SurfaceCount != 1 || row.EmbeddedCount != 1 || row.ClientFacingCount != 0 {
		t.Fatalf("row = %#v, want embedded-only adapter surface summary", row)
	}
	if row.InProcessTransportCount != 1 || row.UsesQSBridgeCount != 1 {
		t.Fatalf("row = %#v, want embedded transport and metadata counts", row)
	}
}

func TestPlanningServiceSummarizeClientAdapterSurfacesReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.SummarizeClientAdapterSurfaces(connection, AdapterSurfaceAudienceSQLClient)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Row.SurfaceCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed adapter surface summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientAdapterSurfacesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientAdapterSurfaces(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.SurfaceCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientAdapterSurfaces(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.SurfaceCount != 4 || again.Row.UsesQSBridgeCount != 4 || again.Result.Chunks[0].Rows[0][0].Value != 4 {
		t.Fatalf("adapter surface summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
