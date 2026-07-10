package qsbridge

import "testing"

func TestSummarizeAdapterContractsAggregatesBySurface(t *testing.T) {
	summaries := AdapterContractSummariesForSurface(AdapterSurfaceMySQLServer)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %#v, want one MySQL summary", summaries)
	}
	summary := summaries[0]
	if summary.Surface != AdapterSurfaceMySQLServer || summary.ContractCount != 7 || summary.RequiredCount != 7 {
		t.Fatalf("summary = %#v, want seven required MySQL contracts", summary)
	}
	if summary.MetadataOnlyCount != 3 || summary.BoundaryOnlyCount != 4 {
		t.Fatalf("summary = %#v, want three metadata and four boundary contracts", summary)
	}
	if summary.AdapterOwnedCount != 4 || summary.RuntimeOwnedCount != 0 || summary.QSBridgeOwnedCount != 3 {
		t.Fatalf("summary = %#v, want adapter/qsbridge ownership counts", summary)
	}
}

func TestPlanningServiceSummarizeClientAdapterContractsReturnsAllSurfaces(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.SummarizeClientAdapterContracts(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter contract summary metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want one summary per adapter surface", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 9 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter contract summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientAdapterContractsFiltersSurface(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientAdapterContracts(connection, AdapterSurfaceInternalExecution)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported internal summary metadata", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Surface != AdapterSurfaceInternalExecution {
		t.Fatalf("rows = %#v, want internal execution summary", exchange.Rows)
	}
	if exchange.Rows[0].DeferredCount != 1 || exchange.Rows[0].RuntimeOwnedCount != 2 {
		t.Fatalf("row = %#v, want deferred topology and runtime ownership counts", exchange.Rows[0])
	}
}

func TestPlanningServiceSummarizeClientAdapterContractsReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.SummarizeClientAdapterContracts(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientAdapterContractsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientAdapterContracts(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].ContractCount = -1
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientAdapterContracts(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].ContractCount <= 0 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
