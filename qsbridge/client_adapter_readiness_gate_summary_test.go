package qsbridge

import "testing"

func TestSummarizeAdapterReadinessGatesAggregatesMySQLGates(t *testing.T) {
	summaries := AdapterReadinessGateSummariesForSurface(AdapterSurfaceMySQLServer)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %#v, want one MySQL gate summary", summaries)
	}
	summary := summaries[0]
	if summary.Surface != AdapterSurfaceMySQLServer || summary.GateCount != 6 || summary.ReadyCount != 2 {
		t.Fatalf("summary = %#v, want six MySQL gates with contracts and metadata ready", summary)
	}
	if summary.RuntimeBlockCount != 4 || summary.BlockerCount != 4 || summary.RuntimeReady {
		t.Fatalf("summary = %#v, want four runtime-blocking rollout gates", summary)
	}
	if !summary.ContractsReady || !summary.MetadataReady {
		t.Fatalf("summary = %#v, want contract and metadata gates ready", summary)
	}
	if summary.NextGate != AdapterReadinessGateAdapterShell || summary.NextGateOrder != 2 {
		t.Fatalf("summary = %#v, want adapter shell as next gate", summary)
	}
}

func TestSummarizeAdapterReadinessGatesAggregatesInternalContractBlocker(t *testing.T) {
	summaries := AdapterReadinessGateSummariesForSurface(AdapterSurfaceInternalExecution)
	if len(summaries) != 1 {
		t.Fatalf("summaries = %#v, want one internal gate summary", summaries)
	}
	summary := summaries[0]
	if summary.GateCount != 6 || summary.ReadyCount != 0 || summary.RuntimeReady {
		t.Fatalf("summary = %#v, want no ready internal gates", summary)
	}
	if summary.RuntimeBlockCount != 6 || summary.BlockerCount != 6 {
		t.Fatalf("summary = %#v, want all internal gates blocked", summary)
	}
	if summary.ContractsReady || summary.MetadataReady {
		t.Fatalf("summary = %#v, want internal contracts and metadata blocked", summary)
	}
	if summary.NextGate != AdapterReadinessGateContracts || summary.NextGateOrder != 0 {
		t.Fatalf("summary = %#v, want contracts as next internal gate", summary)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessGatesReturnsAllSurfaces(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.SummarizeClientAdapterReadinessGates(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter readiness gate summary metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want one gate summary per adapter surface", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 10 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter readiness gate summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per gate summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessGatesFiltersSurface(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientAdapterReadinessGates(connection, AdapterSurfaceEmbedded)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported embedded gate summary metadata", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Surface != AdapterSurfaceEmbedded {
		t.Fatalf("rows = %#v, want embedded gate summary", exchange.Rows)
	}
	if exchange.Rows[0].NextGate != AdapterReadinessGateAdapterShell ||
		!exchange.Rows[0].ContractsReady || !exchange.Rows[0].MetadataReady {
		t.Fatalf("row = %#v, want embedded adapter shell as next gate", exchange.Rows[0])
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessGatesReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.SummarizeClientAdapterReadinessGates(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientAdapterReadinessGatesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientAdapterReadinessGates(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].GateCount = -1
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientAdapterReadinessGates(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].GateCount <= 0 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
