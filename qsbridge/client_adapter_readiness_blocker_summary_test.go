package qsbridge

import "testing"

func TestSummarizeAdapterReadinessBlockersAggregatesBySurface(t *testing.T) {
	summaries := AdapterReadinessBlockerSummariesForSurface(AdapterSurfaceMySQLServer)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %#v, want one MySQL summary", summaries)
	}
	summary := summaries[0]
	if summary.Surface != AdapterSurfaceMySQLServer || summary.BlockerCount != 4 {
		t.Fatalf("summary = %#v, want four MySQL blockers", summary)
	}
	if summary.ContractBlockers != 0 || summary.RolloutBlockers != 4 {
		t.Fatalf("summary = %#v, want rollout-only MySQL blockers", summary)
	}
	if summary.DeferredCount != 3 || summary.BoundaryOnlyCount != 1 || summary.RuntimeBlockingCount != 4 {
		t.Fatalf("summary = %#v, want deferred/boundary/runtime blocker counts", summary)
	}
	if summary.AdapterOwnedCount != 3 || summary.RuntimeOwnedCount != 1 || summary.QSBridgeOwnedCount != 0 {
		t.Fatalf("summary = %#v, want adapter/runtime owner counts", summary)
	}
}

func TestSummarizeAdapterReadinessBlockersIncludesInternalTopologyContract(t *testing.T) {
	summaries := AdapterReadinessBlockerSummariesForSurface(AdapterSurfaceInternalExecution)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %#v, want one internal summary", summaries)
	}
	summary := summaries[0]
	if summary.BlockerCount != 5 || summary.ContractBlockers != 1 || summary.RolloutBlockers != 4 {
		t.Fatalf("summary = %#v, want deferred topology contract plus rollout blockers", summary)
	}
	if summary.DeferredCount != 4 || summary.BoundaryOnlyCount != 1 || summary.RuntimeBlockingCount != 5 {
		t.Fatalf("summary = %#v, want internal deferred/boundary/runtime counts", summary)
	}
	if summary.RuntimeOwnedCount != 5 || summary.AdapterOwnedCount != 0 || summary.QSBridgeOwnedCount != 0 {
		t.Fatalf("summary = %#v, want runtime-owned internal blockers", summary)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessBlockersReturnsAllSurfaces(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.SummarizeClientAdapterReadinessBlockers(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter readiness blocker summary metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want one blocker summary per adapter surface", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 10 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter readiness blocker summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per blocker summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessBlockersFiltersSurface(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientAdapterReadinessBlockers(connection, AdapterSurfaceGRPCAPI)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported gRPC blocker summary metadata", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Surface != AdapterSurfaceGRPCAPI {
		t.Fatalf("rows = %#v, want gRPC blocker summary", exchange.Rows)
	}
	if exchange.Rows[0].BlockerCount != 4 || exchange.Rows[0].AdapterOwnedCount != 3 {
		t.Fatalf("row = %#v, want gRPC rollout blocker counts", exchange.Rows[0])
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessBlockersReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.SummarizeClientAdapterReadinessBlockers(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessBlockersCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientAdapterReadinessBlockers(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].BlockerCount = -1
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientAdapterReadinessBlockers(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].BlockerCount <= 0 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
