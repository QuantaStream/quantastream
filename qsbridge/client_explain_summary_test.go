package qsbridge

import "testing"

func TestPlanningServicePrepareClientExplainSummaryReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	report := sampleInspectionReportForExplainOptions()
	report.Diagnostics = DiagnosticSet{{
		Code:     DiagnosticNativeBlocker,
		Severity: SeverityWarning,
		Phase:    PhaseClassify,
		Message:  "note",
	}}
	bundle := ExplainInspectionReport(report, VerboseExplainOptions())

	exchange := service.PrepareClientExplainSummary(connection, bundle)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported explain summary", exchange)
	}
	row := exchange.Row
	if row.RowCount != 12 || row.SelectedSectionCount != 7 || row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 || !row.Supported {
		t.Fatalf("row = %#v, want explain row, section, and support counts", row)
	}
	if row.LogicalCount != 1 || row.PhysicalCount != 1 || row.OptimizerCount != 1 || row.OptimizerSummaryCount != 1 {
		t.Fatalf("row = %#v, want plan and optimizer counts", row)
	}
	if row.DiagnosticCount != 1 || row.FunctionCount != 1 || row.NativeBlockerCount != 1 {
		t.Fatalf("row = %#v, want diagnostic/function/blocker counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 13 {
		t.Fatalf("result/schema = %#v/%#v, want explain summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 12 || resultRow[1].Value != 7 || resultRow[2].Value != string(PhysicalAccessRead) || resultRow[3].Value != string(ClientPlanLifecycleSelect) || resultRow[4].Value != 7 || resultRow[12].Value != true {
		t.Fatalf("result row = %#v, want explain summary cells", resultRow)
	}
}

func TestPlanningServicePrepareClientExplainSummaryReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	bundle := ExplainBundle{
		Supported: false,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticNativeBlocker, PhasePlan, "explain failed"),
		},
	}

	exchange := service.PrepareClientExplainSummary(connection, bundle)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking explain diagnostics", exchange)
	}
	if exchange.Row.RowCount != 6 || exchange.Row.DiagnosticCount != 1 || exchange.Row.Supported {
		t.Fatalf("row = %#v, want explain rows counted even when result fails", exchange.Row)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 13 {
		t.Fatalf("result/schema = %#v/%#v, want failed explain summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePrepareClientExplainSummaryCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	report := sampleInspectionReportForExplainOptions()
	report.Diagnostics = DiagnosticSet{{
		Code:     DiagnosticNativeBlocker,
		Severity: SeverityWarning,
		Phase:    PhaseClassify,
		Message:  "note",
	}}
	bundle := ExplainInspectionReport(report, VerboseExplainOptions())

	exchange := service.PrepareClientExplainSummary(connection, bundle)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Bundle.Logical.Nodes[0].Summary = "mutated"
	exchange.Row.RowCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.PrepareClientExplainSummary(connection, bundle)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Bundle.Logical.Nodes[0].Summary != "logical" {
		t.Fatalf("bundle leaked mutation: %#v", again.Bundle)
	}
	if again.Row.RowCount != 12 || again.Result.Chunks[0].Rows[0][0].Value != 12 {
		t.Fatalf("explain summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
