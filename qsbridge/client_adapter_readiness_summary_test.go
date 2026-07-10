package qsbridge

import "testing"

func TestDefaultAdapterReadinessSummaryAggregatesAllSurfaces(t *testing.T) {
	summary := DefaultAdapterReadinessSummary()
	if summary.SurfaceCount != 4 || summary.MetadataReadyCount != 3 || summary.RuntimeReadyCount != 0 {
		t.Fatalf("summary = %#v, want three metadata-ready and zero runtime-ready surfaces", summary)
	}
	if summary.ClientFacingCount != 2 || summary.ControlPlaneCount != 1 ||
		summary.EmbeddedCount != 1 || summary.InternalCount != 1 {
		t.Fatalf("summary = %#v, want expected surface type counts", summary)
	}
	if summary.ContractCount != 14 || summary.DeferredContracts != 1 {
		t.Fatalf("summary = %#v, want contract totals", summary)
	}
	if summary.PhaseCount != 20 || summary.DeferredPhases != 12 || summary.RuntimeBlockingPhases != 16 {
		t.Fatalf("summary = %#v, want rollout totals", summary)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessReturnsOneRow(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.SummarizeClientAdapterReadiness(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter readiness summary metadata", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one aggregate readiness summary", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 15 || exchange.ResultSchema.Columns[0].Name != "Surfaces" {
		t.Fatalf("schema = %#v, want adapter readiness summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want one row returned", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.SummarizeClientAdapterReadiness(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientAdapterReadiness(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].SurfaceCount = -1
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.SummarizeClientAdapterReadiness(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].SurfaceCount != 4 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surfaces" || again.ResultSchema.Columns[0].Name != "Surfaces" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
