package qsbridge

import "testing"

func TestPlanningServicePrepareClientBatchExecutionProfileSummaryReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Profile: ExecutionProfile{
			RequestID:      "item-1",
			AccessIntent:   PhysicalAccessRead,
			Lifecycle:      ClientPlanLifecycleSelect,
			LifecycleSteps: 7,
			LogicalPlan:    "scan(orders)",
			PhysicalPlan:   "bitmap_scan(orders)",
			Timings:        []ExecutionTiming{{Name: "scan", Elapsed: 12, Unit: "ms"}},
		},
	}).WithItem(ExecutionResult{
		RequestID: "item-2",
		Profile: ExecutionProfile{
			RequestID:      "item-2",
			AccessIntent:   PhysicalAccessWrite,
			Lifecycle:      ClientPlanLifecycleMutation,
			LifecycleSteps: 7,
			Counters:       []ExecutionCounter{{Name: "rows", Value: 7, Unit: "rows"}},
			Diagnostics: DiagnosticSet{{
				Code:     DiagnosticNativeBlocker,
				Severity: SeverityWarning,
				Phase:    PhasePlan,
				Message:  "note",
			}},
		},
	}).WithItem(ExecutionResult{RequestID: "item-3"})

	exchange := service.PrepareClientBatchExecutionProfileSummary(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch profile summary", exchange)
	}
	row := exchange.Row
	if row.ItemCount != 3 || row.ProfiledItems != 2 || row.RowCount != 5 {
		t.Fatalf("row = %#v, want item and row counts", row)
	}
	if row.ReadIntentItems != 1 || row.WriteIntentItems != 1 || row.SelectLifecycleItems != 1 || row.MutationLifecycleItems != 1 {
		t.Fatalf("row = %#v, want lifecycle item counts", row)
	}
	if row.ExplainCount != 2 || row.TimingCount != 1 || row.CounterCount != 1 || row.DiagnosticCount != 1 {
		t.Fatalf("row = %#v, want profile section counts", row)
	}
	if exchange.Result.RequestID != "batch-1" || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want batch profile summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[1].Value != 2 || resultRow[2].Value != 5 || resultRow[5].Value != 1 || resultRow[7].Value != 2 {
		t.Fatalf("result row = %#v, want batch profile summary cells", resultRow)
	}
}

func TestPlanningServicePrepareClientBatchExecutionProfileSummarySupportsEmptyBatch(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.PrepareClientBatchExecutionProfileSummary(connection, BatchExecutionResult{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty batch profile summary", exchange)
	}
	if exchange.Row.ItemCount != 0 || exchange.Row.RowCount != 0 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("row/result = %#v/%#v, want empty batch profile summary row", exchange.Row, exchange.Result)
	}
}

func TestPlanningServicePrepareClientBatchExecutionProfileSummaryReturnsFailedEnvelopeForBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Profile: ExecutionProfile{
			RequestID:   "item-1",
			LogicalPlan: "scan(orders)",
			Diagnostics: DiagnosticSet{
				ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "profile failed"),
			},
		},
	})

	exchange := service.PrepareClientBatchExecutionProfileSummary(connection, batch)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking profile diagnostics", exchange)
	}
	if exchange.Row.RowCount != 2 || exchange.Row.ExplainCount != 1 || exchange.Row.DiagnosticCount != 1 {
		t.Fatalf("row = %#v, want profile rows counted even when result fails", exchange.Row)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want failed batch profile summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePrepareClientBatchExecutionProfileSummaryCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Profile: ExecutionProfile{
			RequestID: "item-1",
			Timings:   []ExecutionTiming{{Name: "scan"}},
		},
	})

	exchange := service.PrepareClientBatchExecutionProfileSummary(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Batch.Items[0].Profile.Timings[0].Name = "mutated"
	exchange.Row.RowCount = 99
	exchange.Result.Chunks[0].Rows[0][2].Value = 99

	again := service.PrepareClientBatchExecutionProfileSummary(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Batch.Items[0].Profile.Timings[0].Name != "scan" {
		t.Fatalf("batch profile leaked mutation: %#v", again.Batch)
	}
	if again.Row.RowCount != 1 || again.Row.TimingCount != 1 || again.Result.Chunks[0].Rows[0][2].Value != 1 {
		t.Fatalf("batch profile summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}
