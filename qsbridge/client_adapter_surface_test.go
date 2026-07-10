package qsbridge

import "testing"

func TestPlanningServiceListClientAdapterSurfacesReturnsAllRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientAdapterSurfaces(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter surface metadata", exchange)
	}
	if !adapterSurfacesContain(exchange.Surfaces, AdapterSurfaceMySQLServer) ||
		!adapterSurfacesContain(exchange.Surfaces, AdapterSurfaceGRPCAPI) ||
		!adapterSurfacesContain(exchange.Surfaces, AdapterSurfaceEmbedded) ||
		!adapterSurfacesContain(exchange.Surfaces, AdapterSurfaceInternalExecution) {
		t.Fatalf("surfaces = %#v, want all adapter lanes", exchange.Surfaces)
	}
	if len(exchange.ResultSchema.Columns) != 12 || exchange.ResultSchema.Columns[0].Name != "Kind" {
		t.Fatalf("schema = %#v, want adapter surface columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Surfaces)) {
		t.Fatalf("result = %#v, want one row per adapter surface", exchange.Result)
	}
}

func TestPlanningServiceListClientAdapterSurfacesFiltersAudience(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientAdapterSurfaces(connection, AdapterSurfaceAudienceEmbedded)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported embedded adapter surface metadata", exchange)
	}
	if len(exchange.Surfaces) != 1 || exchange.Surfaces[0].Kind != AdapterSurfaceEmbedded {
		t.Fatalf("surfaces = %#v, want only embedded QIAB surface", exchange.Surfaces)
	}
	if !exchange.Surfaces[0].Embedded || exchange.Surfaces[0].ClientFacing {
		t.Fatalf("surface = %#v, want non-client embedded surface", exchange.Surfaces[0])
	}
}

func TestPlanningServiceListClientAdapterSurfacesReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientAdapterSurfaces(connection, AdapterSurfaceAudienceSQLClient)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Surfaces) != 0 {
		t.Fatalf("result = %#v surfaces=%#v, want failed rowless exchange", exchange.Result, exchange.Surfaces)
	}
}

func TestPlanningServiceListClientAdapterSurfacesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientAdapterSurfaces(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Surfaces[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][11].Value = "mutated"

	again := service.ListClientAdapterSurfaces(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Surfaces[0].Detail == "mutated" {
		t.Fatalf("surfaces leaked mutation: %#v", again.Surfaces[0])
	}
	if again.Result.Columns[0].Name != "Kind" || again.ResultSchema.Columns[0].Name != "Kind" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][11].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func adapterSurfacesContain(surfaces []AdapterSurface, kind AdapterSurfaceKind) bool {
	_, ok := adapterSurfaceByKind(surfaces, kind)
	return ok
}
