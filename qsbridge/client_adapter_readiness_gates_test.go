package qsbridge

import "testing"

func TestAdapterReadinessGatesForMySQLIdentifyNextAdapterShell(t *testing.T) {
	gates := AdapterReadinessGatesForSurface(AdapterSurfaceMySQLServer)
	if len(gates) != 6 {
		t.Fatalf("gates = %#v, want contract gate plus five rollout gates", gates)
	}
	contractGate := adapterReadinessGateByKind(gates, AdapterReadinessGateContracts)
	if contractGate == nil || !contractGate.Ready || contractGate.BlocksRuntime || contractGate.BlockerCount != 0 {
		t.Fatalf("contract gate = %#v, want ready MySQL contracts", contractGate)
	}
	metadataGate := adapterReadinessGateByKind(gates, AdapterReadinessGateMetadataInventory)
	if metadataGate == nil || !metadataGate.Ready || metadataGate.BlocksRuntime {
		t.Fatalf("metadata gate = %#v, want ready MySQL metadata inventory", metadataGate)
	}
	nextGate := adapterReadinessNextGate(gates)
	if nextGate == nil || nextGate.Gate != AdapterReadinessGateAdapterShell ||
		!nextGate.BlocksRuntime || nextGate.BlockerCount != 1 {
		t.Fatalf("next gate = %#v, want blocked adapter shell", nextGate)
	}
}

func TestAdapterReadinessGatesForInternalIdentifyContractGate(t *testing.T) {
	gates := AdapterReadinessGatesForSurface(AdapterSurfaceInternalExecution)
	if len(gates) != 6 {
		t.Fatalf("gates = %#v, want contract gate plus five rollout gates", gates)
	}
	nextGate := adapterReadinessNextGate(gates)
	if nextGate == nil || nextGate.Gate != AdapterReadinessGateContracts {
		t.Fatalf("next gate = %#v, want deferred contract gate first", nextGate)
	}
	if nextGate.Ready || !nextGate.BlocksRuntime || nextGate.BlockerCount != 1 ||
		nextGate.Owner != WireAdapterOwnerExecutor {
		t.Fatalf("next gate = %#v, want runtime-owned deferred topology contract blocker", nextGate)
	}
	metadataGate := adapterReadinessGateByKind(gates, AdapterReadinessGateMetadataInventory)
	if metadataGate == nil || metadataGate.Ready || metadataGate.BlockerCount != 1 {
		t.Fatalf("metadata gate = %#v, want internal metadata inventory to remain blocked", metadataGate)
	}
}

func TestPlanningServiceListClientAdapterReadinessGatesReturnsAllRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientAdapterReadinessGates(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter readiness gate metadata", exchange)
	}
	if len(exchange.Rows) != 24 {
		t.Fatalf("rows = %#v, want six gates per adapter surface", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 10 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter readiness gate columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per gate", exchange.Result)
	}
}

func TestPlanningServiceListClientAdapterReadinessGatesFiltersSurface(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientAdapterReadinessGates(connection, AdapterSurfaceGRPCAPI)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported gRPC gate metadata", exchange)
	}
	if len(exchange.Rows) != 6 {
		t.Fatalf("rows = %#v, want six gRPC gates", exchange.Rows)
	}
	for _, row := range exchange.Rows {
		if row.Surface != AdapterSurfaceGRPCAPI {
			t.Fatalf("row = %#v, want only gRPC gates", row)
		}
	}
}

func TestPlanningServiceListClientAdapterReadinessGatesReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientAdapterReadinessGates(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientAdapterReadinessGatesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientAdapterReadinessGates(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Ready = false
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][5].Value = "mutated"

	again := service.ListClientAdapterReadinessGates(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if !again.Rows[0].Ready {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][5].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func adapterReadinessGateByKind(gates []AdapterReadinessGate, kind AdapterReadinessGateKind) *AdapterReadinessGate {
	for i := range gates {
		if gates[i].Gate == kind {
			return &gates[i]
		}
	}
	return nil
}

func adapterReadinessNextGate(gates []AdapterReadinessGate) *AdapterReadinessGate {
	for i := range gates {
		if gates[i].Next {
			return &gates[i]
		}
	}
	return nil
}
