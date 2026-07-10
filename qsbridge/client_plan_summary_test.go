package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPlanBundleReturnsPlanRows(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "fallback",
		Session:       SessionContext{User: "planner-default"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	plan := ConnectionPlanOptions{
		CatalogVersion: "catalog-v1",
		Scope: PhysicalScope{
			Placement: PlacementLocal,
			Cache:     CacheSession,
		},
	}

	bundle := NewClientStatementBundle(connection, plan, "select 1", "select 2")
	planned := service.PrepareClientStatementBundle(bundle)
	exchange := service.SummarizeClientPlanBundle(planned)
	if !exchange.Supported() || len(exchange.Rows) != 2 {
		t.Fatalf("exchange = %#v, want supported planning summary rows", exchange)
	}
	row := exchange.Rows[0]
	if row.Ordinal != 0 || row.Kind != QueryKindSelect || row.Schema != "quanta" || row.CatalogVersion != "catalog-v1" {
		t.Fatalf("row = %#v, want plan metadata", row)
	}
	if row.User != "moli" || !row.Supported || row.Parameters != 1 || row.ResultColumns != 1 || row.AccessRequirements != 1 {
		t.Fatalf("row = %#v, want prepared metadata counts", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read SELECT lifecycle metadata", row)
	}
	if row.LogicalRoot != PlanNodeProject || row.PhysicalRoot != PhysicalNodeProject || row.Placement != PlacementLocal || row.CacheScope != CacheSession {
		t.Fatalf("row = %#v, want physical/logical scope metadata", row)
	}
	if exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 18 {
		t.Fatalf("result/schema = %#v/%#v, want plan summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 0 || resultRow[1].Value != string(QueryKindSelect) || resultRow[5].Value != true || resultRow[6].Value != string(PhysicalAccessRead) || resultRow[7].Value != string(ClientPlanLifecycleSelect) || resultRow[11].Value != 1 {
		t.Fatalf("result row = %#v, want planning summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPlanBundleReturnsMutationLifecycleRoute(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	prepared := PreparedPlan{
		SQL:            "update orders set o_totalprice = ? where o_orderkey = ?",
		Kind:           QueryKindUpdate,
		DefaultSchema:  "quanta",
		CatalogVersion: "catalog-v2",
		Session:        SessionContext{User: "moli"},
		Scope:          PhysicalScope{Placement: PlacementPrimary, Cache: CacheQuery},
		Logical:        LogicalPlan{Root: StatementNode{Kind: QueryKindUpdate}},
		Physical:       PhysicalPlan{Root: PhysicalStatementNode{Kind: QueryKindUpdate}},
		Parameters:     []ParameterRef{{Index: 1, Type: DataTypeFloat}, {Index: 2, Type: DataTypeInt}},
		Result:         ResultShape{Kind: ResultStatement},
		Supported:      true,
	}
	bundle := ClientPlanBundle{
		Connection: connection,
		Statements: []ClientStatementPlan{{
			Statement: ClientStatementText{Ordinal: 0, SQL: prepared.SQL},
			Prepared:  prepared,
		}},
	}

	exchange := service.SummarizeClientPlanBundle(bundle)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported mutation plan summary", exchange)
	}
	row := exchange.Rows[0]
	if row.Kind != QueryKindUpdate || row.AccessIntent != PhysicalAccessWrite || row.Lifecycle != ClientPlanLifecycleMutation || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want update write mutation lifecycle route", row)
	}
	if row.LogicalRoot != PlanNodeStatement || row.PhysicalRoot != PhysicalNodeStatement || row.Parameters != 2 {
		t.Fatalf("row = %#v, want statement roots and two parameters", row)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != string(QueryKindUpdate) || resultRow[6].Value != string(PhysicalAccessWrite) || resultRow[7].Value != string(ClientPlanLifecycleMutation) {
		t.Fatalf("result row = %#v, want mutation routing cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPlanBundleReportsPreparedDiagnosticsAsData(t *testing.T) {
	parser := &countingParserBridge{
		statement: serviceSelectStatement(),
		diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "bad statement"),
		},
	}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select broken")
	planned := service.PrepareClientStatementBundle(bundle)
	exchange := service.SummarizeClientPlanBundle(planned)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, prepared diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported statement row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticParserBoundary) {
		t.Fatalf("diagnostics = %#v, want parser boundary", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientPlanBundleFailsForBundleDiagnostics(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1", "select 2")
	planned := service.PrepareClientStatementBundle(bundle)
	exchange := service.SummarizeClientPlanBundle(planned)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported bundle diagnostics", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless planning summary", exchange.Result, exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.ExchangeDiagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.ExchangeDiagnostics)
	}
}

func TestPlanningServiceSummarizeClientPlanBundleCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	planned := service.PrepareClientStatementBundle(bundle)

	exchange := service.SummarizeClientPlanBundle(planned)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Plan.Statements[0].Prepared.Parameters[0].Type = DataTypeString
	exchange.Rows[0].Schema = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.SummarizeClientPlanBundle(planned)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Plan.Statements[0].Prepared.Parameters[0].Type != DataTypeInt {
		t.Fatalf("plan metadata leaked mutation: %#v", again.Plan.Statements[0].Prepared.Parameters)
	}
	if again.Rows[0].Schema != "quanta" {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Ordinal" || again.ResultSchema.Columns[0].Name != "Ordinal" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][3].Value != "quanta" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
