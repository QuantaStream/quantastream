package qsbridge

import "testing"

func TestAdapterReadinessReportsCombineSurfaceContractsAndRollout(t *testing.T) {
	reports := AdapterReadinessReportsForSurface(AdapterSurfaceMySQLServer)
	if len(reports) != 1 {
		t.Fatalf("reports = %#v, want one MySQL readiness report", reports)
	}
	report := reports[0]
	if report.Surface != AdapterSurfaceMySQLServer || !report.ClientFacing || report.ControlPlane {
		t.Fatalf("report = %#v, want MySQL client-facing surface", report)
	}
	if !report.MetadataReady || report.RuntimeReady {
		t.Fatalf("report = %#v, want metadata-ready but not runtime-ready", report)
	}
	if report.NextPhase != AdapterRolloutAdapterShell {
		t.Fatalf("next phase = %q, want adapter shell", report.NextPhase)
	}
	if report.ContractCount != 7 || report.QSBridgeContracts != 3 || report.AdapterOwnedContracts != 4 {
		t.Fatalf("report = %#v, want MySQL contract counts", report)
	}
	if report.PhaseCount != 5 || report.DeferredPhases != 3 || report.RuntimeBlockingPhases != 4 {
		t.Fatalf("report = %#v, want rollout counts", report)
	}
}

func TestAdapterReadinessReportsForInternalSurfaceRemainRuntimeBlocked(t *testing.T) {
	reports := AdapterReadinessReportsForSurface(AdapterSurfaceInternalExecution)
	if len(reports) != 1 {
		t.Fatalf("reports = %#v, want one internal readiness report", reports)
	}
	report := reports[0]
	if !report.Internal || report.ClientFacing {
		t.Fatalf("report = %#v, want internal non-client surface", report)
	}
	if report.DeferredContracts != 1 || report.RuntimeOwnedContracts != 2 ||
		report.RuntimeBlockingPhases != 4 || report.RuntimeReady {
		t.Fatalf("report = %#v, want runtime-blocked internal execution", report)
	}
}

func TestPlanningServiceListClientAdapterReadinessReturnsAllSurfaces(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientAdapterReadiness(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter readiness metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want one readiness row per adapter surface", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 21 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter readiness columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per readiness report", exchange.Result)
	}
}

func TestPlanningServiceListClientAdapterReadinessFiltersSurface(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientAdapterReadiness(connection, AdapterSurfaceGRPCAPI)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported gRPC readiness metadata", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Surface != AdapterSurfaceGRPCAPI {
		t.Fatalf("rows = %#v, want only gRPC readiness", exchange.Rows)
	}
	if !exchange.Rows[0].ControlPlane || exchange.Rows[0].RuntimeReady {
		t.Fatalf("row = %#v, want control-plane surface that is not runtime-ready", exchange.Rows[0])
	}
}

func TestPlanningServiceListClientAdapterReadinessReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientAdapterReadiness(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientAdapterReadinessCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientAdapterReadiness(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][20].Value = "mutated"

	again := service.ListClientAdapterReadiness(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Detail == "mutated" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][20].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
