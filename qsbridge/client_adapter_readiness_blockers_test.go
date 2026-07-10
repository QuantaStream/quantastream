package qsbridge

import "testing"

func TestAdapterReadinessBlockersForMySQLComeFromRollout(t *testing.T) {
	blockers := AdapterReadinessBlockersForSurface(AdapterSurfaceMySQLServer)
	if len(blockers) != 4 {
		t.Fatalf("blockers = %#v, want four MySQL rollout blockers", blockers)
	}
	if !adapterReadinessBlockersContain(blockers, AdapterReadinessBlockerRollout, AdapterRolloutAdapterShell, "") ||
		!adapterReadinessBlockersContain(blockers, AdapterReadinessBlockerRollout, AdapterRolloutRuntimeEnablement, "") {
		t.Fatalf("blockers = %#v, want adapter shell and runtime enablement blockers", blockers)
	}
	if adapterReadinessBlockersContain(blockers, AdapterReadinessBlockerContract, "", AdapterContractTopology) {
		t.Fatalf("blockers = %#v, did not expect contract blockers for MySQL", blockers)
	}
}

func TestAdapterReadinessBlockersForInternalIncludeDeferredTopologyContract(t *testing.T) {
	blockers := AdapterReadinessBlockersForSurface(AdapterSurfaceInternalExecution)
	if len(blockers) != 5 {
		t.Fatalf("blockers = %#v, want deferred topology contract plus four rollout blockers", blockers)
	}
	if !adapterReadinessBlockersContain(blockers, AdapterReadinessBlockerContract, "", AdapterContractTopology) {
		t.Fatalf("blockers = %#v, want deferred topology contract blocker", blockers)
	}
}

func TestPlanningServiceListClientAdapterReadinessBlockersReturnsAllRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientAdapterReadinessBlockers(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter readiness blocker metadata", exchange)
	}
	if len(exchange.Rows) != 17 {
		t.Fatalf("rows = %#v, want sixteen rollout blockers plus deferred topology contract", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter readiness blocker columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per blocker", exchange.Result)
	}
}

func TestPlanningServiceListClientAdapterReadinessBlockersFiltersSurface(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientAdapterReadinessBlockers(connection, AdapterSurfaceGRPCAPI)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported gRPC blocker metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want four gRPC rollout blockers", exchange.Rows)
	}
	for _, row := range exchange.Rows {
		if row.Surface != AdapterSurfaceGRPCAPI {
			t.Fatalf("row = %#v, want only gRPC blockers", row)
		}
	}
}

func TestPlanningServiceListClientAdapterReadinessBlockersReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientAdapterReadinessBlockers(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientAdapterReadinessBlockersCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientAdapterReadinessBlockers(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][7].Value = "mutated"

	again := service.ListClientAdapterReadinessBlockers(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Detail == "mutated" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][7].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func adapterReadinessBlockersContain(blockers []AdapterReadinessBlocker, source AdapterReadinessBlockerSource, phase AdapterRolloutPhase, concern AdapterContractConcern) bool {
	for _, blocker := range blockers {
		if blocker.Source == source && blocker.Phase == phase && blocker.Concern == concern {
			return true
		}
	}
	return false
}
