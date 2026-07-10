package qsbridge

import "testing"

func TestPlanningServiceListClientPackageBoundarySummaryBuildsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.ListClientPackageBoundarySummary(clientStatementConnection())
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported package-boundary summary", exchange)
	}
	if len(exchange.Rows) != len(DefaultPackageBoundaries()) {
		t.Fatalf("rows = %#v, want one row per default package boundary", exchange.Rows)
	}
	clientRow, ok := packageBoundarySummaryRowByName(exchange.Rows, PackageBoundaryClient)
	if !ok {
		t.Fatalf("rows = %#v, want client boundary", exchange.Rows)
	}
	if clientRow.FilePrefixCount != 1 || clientRow.FilePrefixes[0] != "client_" {
		t.Fatalf("client row = %#v, want client_ prefix", clientRow)
	}
	if clientRow.SplitPhase != PackageSplitClient || clientRow.SplitOrder != 5 {
		t.Fatalf("client row = %#v, want client split phase/order", clientRow)
	}
	if clientRow.DependencyCount != 6 || !packageBoundaryNamesContain(clientRow.MayDependOn, PackageBoundaryCore) ||
		!packageBoundaryNamesContain(clientRow.MayDependOn, PackageBoundaryCatalog) ||
		!packageBoundaryNamesContain(clientRow.MayDependOn, PackageBoundaryPlanning) ||
		!packageBoundaryNamesContain(clientRow.MayDependOn, PackageBoundaryExecution) {
		t.Fatalf("client row = %#v, want core/catalog/planning/execution/protocol/cache dependencies", clientRow)
	}
	if len(exchange.ResultSchema.Columns) != 11 || exchange.ResultSchema.Columns[0].Name != "Boundary" {
		t.Fatalf("schema = %#v, want package-boundary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one result row per boundary", exchange.Result)
	}
	if exchange.Result.Chunks[0].Rows[0][0].Value != string(PackageBoundaryClient) {
		t.Fatalf("first result row = %#v, want client boundary first", exchange.Result.Chunks[0].Rows[0])
	}
}

func TestPlanningServiceListClientPackageBoundarySummaryReturnsFailedEnvelopeForBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhaseClassify, "unsupported")}

	exchange := service.ListClientPackageBoundarySummary(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection diagnostics", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want failed package-boundary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientPackageBoundarySummaryCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientPackageBoundarySummary(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Boundaries[0].Responsibilities[0] = "mutated"
	exchange.Boundaries[0].MayDependOn[0] = PackageBoundaryTestkit
	exchange.Rows[0].Responsibilities[0] = "mutated"
	exchange.Rows[0].MayDependOn[0] = PackageBoundaryTestkit
	exchange.Result.Chunks[0].Rows[0][7].Value = "mutated"

	again := service.ListClientPackageBoundarySummary(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Boundaries[0].Responsibilities[0] == "mutated" || again.Rows[0].Responsibilities[0] == "mutated" {
		t.Fatalf("boundary rows leaked mutation: %#v/%#v", again.Boundaries[0], again.Rows[0])
	}
	if again.Boundaries[0].MayDependOn[0] == PackageBoundaryTestkit || again.Rows[0].MayDependOn[0] == PackageBoundaryTestkit {
		t.Fatalf("boundary dependencies leaked mutation: %#v/%#v", again.Boundaries[0], again.Rows[0])
	}
	if again.Result.Chunks[0].Rows[0][7].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks[0].Rows[0])
	}
}

func packageBoundarySummaryRowByName(rows []ClientPackageBoundarySummaryRow, name PackageBoundaryName) (ClientPackageBoundarySummaryRow, bool) {
	for _, row := range rows {
		if row.Name == name {
			return row, true
		}
	}
	return ClientPackageBoundarySummaryRow{}, false
}

func packageBoundaryNamesContain(names []PackageBoundaryName, target PackageBoundaryName) bool {
	for _, name := range names {
		if name == target {
			return true
		}
	}
	return false
}
