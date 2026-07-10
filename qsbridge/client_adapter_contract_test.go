package qsbridge

import "testing"

func TestPlanningServiceListClientAdapterContractsReturnsSurfaceRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientAdapterContracts(connection, AdapterSurfaceMySQLServer)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter contract metadata", exchange)
	}
	if !adapterContractsContain(exchange.Contracts, AdapterSurfaceMySQLServer, AdapterContractProtocolDecode) ||
		!adapterContractsContain(exchange.Contracts, AdapterSurfaceMySQLServer, AdapterContractResultSerialization) {
		t.Fatalf("contracts = %#v, want MySQL protocol and serialization contracts", exchange.Contracts)
	}
	if adapterContractsContain(exchange.Contracts, AdapterSurfaceGRPCAPI, AdapterContractInspection) {
		t.Fatalf("contracts = %#v, did not expect gRPC contracts in MySQL view", exchange.Contracts)
	}
	if len(exchange.ResultSchema.Columns) != 11 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter contract columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Contracts)) {
		t.Fatalf("result = %#v, want one row per adapter contract", exchange.Result)
	}
}

func TestPlanningServiceListClientAdapterContractsReturnsAllRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientAdapterContracts(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter contract metadata", exchange)
	}
	if !adapterContractsContain(exchange.Contracts, AdapterSurfaceGRPCAPI, AdapterContractInspection) ||
		!adapterContractsContain(exchange.Contracts, AdapterSurfaceEmbedded, AdapterContractExecutionDispatch) ||
		!adapterContractsContain(exchange.Contracts, AdapterSurfaceInternalExecution, AdapterContractTopology) {
		t.Fatalf("contracts = %#v, want gRPC, embedded, and internal contracts", exchange.Contracts)
	}
}

func TestPlanningServiceListClientAdapterContractsReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientAdapterContracts(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Contracts) != 0 {
		t.Fatalf("result = %#v contracts=%#v, want failed rowless exchange", exchange.Result, exchange.Contracts)
	}
}

func TestPlanningServiceListClientAdapterContractsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientAdapterContracts(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Contracts[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][10].Value = "mutated"

	again := service.ListClientAdapterContracts(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Contracts[0].Detail == "mutated" {
		t.Fatalf("contracts leaked mutation: %#v", again.Contracts[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][10].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
