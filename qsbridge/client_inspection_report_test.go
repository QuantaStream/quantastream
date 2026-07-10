package qsbridge

import (
	"strings"
	"testing"
)

func TestPlanningServicePrepareClientInspectionReportBuildsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{
		Table: lineitem,
		Name:  "l_shipmode",
		Type:  DataTypeString,
		Index: IndexStringEnum,
		Encoding: EncodingProfile{
			Kind:                   EncodingStringEnum,
			Multiplicity:           MultiplicityScalar,
			Rehydration:            RehydrationProfile{Kind: RehydrationLookup},
			PredicateCapabilities:  PredicateCapabilities{PredicateCapabilityEquality},
			ProjectionCapabilities: ProjectionCapabilities{ProjectionCapabilityLookup},
		},
	}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{lineitem},
		Projection: []ProjectionColumn{{
			Expr: FunctionCall(FunctionDefinition{
				Name:          "lower",
				Kind:          FunctionScalar,
				Origin:        FunctionOriginMySQLCompatible,
				ReturnType:    DataTypeString,
				Deterministic: true,
			}, Field(shipMode)),
		}},
		Result: ResultShape{Kind: ResultQuery, Columns: []FieldRef{shipMode}},
	}

	report := InspectQuery(query, PhysicalScope{})
	exchange := service.PrepareClientInspectionReport(connection, report)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported inspection metadata", exchange)
	}
	if exchange.Report.Query.Kind != QueryKindSelect || len(exchange.Rows) == 0 {
		t.Fatalf("exchange = %#v, want copied report rows", exchange)
	}
	if exchange.Rows[0].Category != "summary" || exchange.Rows[0].Name != "kind" || exchange.Rows[0].Value != "select" {
		t.Fatalf("first row = %#v, want summary kind", exchange.Rows[0])
	}
	if !hasInspectionRow(exchange.Rows, "source", "lineitem.as l") {
		t.Fatalf("rows = %#v, want source row", exchange.Rows)
	}
	if !hasInspectionRow(exchange.Rows, "field", "l.l_shipmode") {
		t.Fatalf("rows = %#v, want field row", exchange.Rows)
	}
	if !hasInspectionCategory(exchange.Rows, "encoding") {
		t.Fatalf("rows = %#v, want encoding row", exchange.Rows)
	}
	if !hasInspectionRow(exchange.Rows, "function", "lower") {
		t.Fatalf("rows = %#v, want function usage row", exchange.Rows)
	}
	if !hasInspectionSummaryValue(exchange.Rows, "functions", "1") {
		t.Fatalf("rows = %#v, want function count summary", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 4 || exchange.ResultSchema.Columns[0].Name != "Category" || exchange.ResultSchema.Columns[3].Name != "Detail" {
		t.Fatalf("schema = %#v, want inspection result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) || exchange.Result.Chunks[0].Rows[0][2].Value != "select" {
		t.Fatalf("result = %#v, want inspection rows", exchange.Result)
	}
}

func hasInspectionSummaryValue(rows []ClientInspectionRow, name string, value string) bool {
	for _, row := range rows {
		if row.Category == "summary" && row.Name == name && row.Value == value {
			return true
		}
	}
	return false
}

func TestPlanningServicePrepareClientInspectionReportReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	report := InspectionReport{
		Query: QueryInspection{
			Blockers: []NativeBlocker{{
				Code:       DiagnosticScalarSubquery,
				Capability: CapabilityScalarSubquery,
				Reason:     "scalar subquery",
				Span:       SourceSpan{StartLine: 3, StartCol: 14, EndLine: 3, EndCol: 28},
			}},
			NativeBlockers: 1,
		},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedPredicate, PhasePlan, "unsupported"),
		},
	}

	exchange := service.PrepareClientInspectionReport(connection, report)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking inspection diagnostics", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 4 {
		t.Fatalf("result/schema = %#v/%#v, want failed inspection envelope", exchange.Result, exchange.ResultSchema)
	}
	if !hasInspectionRow(exchange.Rows, "blocker", string(DiagnosticScalarSubquery)) {
		t.Fatalf("rows = %#v, want native blocker row", exchange.Rows)
	}
	blocker, ok := inspectionRowByCategoryAndName(exchange.Rows, "blocker", string(DiagnosticScalarSubquery))
	if !ok || !strings.Contains(blocker.Detail, "span=start_line=3,start_col=14,end_line=3,end_col=28") {
		t.Fatalf("blocker row = %#v/%v, want source span detail", blocker, ok)
	}
	if !hasInspectionSummaryValue(exchange.Rows, "native_blockers", "1") {
		t.Fatalf("rows = %#v, want native blocker count summary", exchange.Rows)
	}
}

func TestPlanningServicePrepareClientInspectionReportCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	report := InspectionReport{
		Query: QueryInspection{
			Kind:    QueryKindSelect,
			Sources: []string{"orders"},
			Fields:  []string{"orders.o_orderkey"},
		},
		Supported: true,
	}

	exchange := service.PrepareClientInspectionReport(connection, report)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Report.Query.Sources[0] = "mutated"
	exchange.Rows[0].Value = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.PrepareClientInspectionReport(connection, report)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Report.Query.Sources[0] != "orders" {
		t.Fatalf("report leaked mutation: %#v", again.Report)
	}
	if again.Rows[0].Value != "select" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Category" || again.ResultSchema.Columns[0].Name != "Category" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "select" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func hasInspectionRow(rows []ClientInspectionRow, category string, name string) bool {
	_, ok := inspectionRowByCategoryAndName(rows, category, name)
	return ok
}

func inspectionRowByCategoryAndName(rows []ClientInspectionRow, category string, name string) (ClientInspectionRow, bool) {
	for _, row := range rows {
		if row.Category == category && row.Name == name {
			return row, true
		}
	}
	return ClientInspectionRow{}, false
}

func hasInspectionCategory(rows []ClientInspectionRow, category string) bool {
	for _, row := range rows {
		if row.Category == category {
			return true
		}
	}
	return false
}
