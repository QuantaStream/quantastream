package qsbridge

import "testing"

func TestSummarizeAdapterRolloutStepsAggregatesBySurface(t *testing.T) {
	summaries := AdapterRolloutSummariesForSurface(AdapterSurfaceMySQLServer)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %#v, want one MySQL summary", summaries)
	}
	summary := summaries[0]
	if summary.Surface != AdapterSurfaceMySQLServer || summary.PhaseCount != 5 {
		t.Fatalf("summary = %#v, want five MySQL phases", summary)
	}
	if summary.MetadataOnlyCount != 1 || summary.BoundaryOnlyCount != 1 || summary.DeferredCount != 3 {
		t.Fatalf("summary = %#v, want metadata/boundary/deferred phase counts", summary)
	}
	if summary.BlocksRuntime != 4 || summary.QSBridgeOwnedCount != 1 ||
		summary.AdapterOwnedCount != 3 || summary.RuntimeOwnedCount != 1 {
		t.Fatalf("summary = %#v, want runtime blocking and owner counts", summary)
	}
}

func TestPlanningServiceSummarizeClientAdapterRolloutReturnsAllSurfaces(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.SummarizeClientAdapterRollout(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter rollout summary metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want one summary per adapter surface", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 9 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter rollout summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientAdapterRolloutFiltersSurface(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientAdapterRollout(connection, AdapterSurfaceInternalExecution)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported internal rollout summary metadata", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Surface != AdapterSurfaceInternalExecution {
		t.Fatalf("rows = %#v, want internal execution rollout summary", exchange.Rows)
	}
	if exchange.Rows[0].PhaseCount != 5 || exchange.Rows[0].RuntimeOwnedCount != 4 {
		t.Fatalf("row = %#v, want internal runtime-owned rollout counts", exchange.Rows[0])
	}
}

func TestPlanningServiceSummarizeClientAdapterRolloutReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.SummarizeClientAdapterRollout(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientAdapterRolloutCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientAdapterRollout(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].PhaseCount = -1
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientAdapterRollout(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].PhaseCount <= 0 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
