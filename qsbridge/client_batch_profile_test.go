package qsbridge

import "testing"

func TestPlanningServicePrepareClientBatchExecutionProfileBuildsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Profile: ExecutionProfile{
			RequestID:      "item-1",
			AccessIntent:   PhysicalAccessRead,
			Lifecycle:      ClientPlanLifecycleSelect,
			LifecycleSteps: 7,
			TraceExplain:   true,
			IncludeProfile: true,
			LogicalPlan:    "scan(orders)",
			Timings: []ExecutionTiming{{
				Name:    "scan",
				Elapsed: 12,
				Unit:    "ms",
			}},
		},
	}).WithItem(ExecutionResult{
		RequestID: "item-2",
		Profile: ExecutionProfile{
			RequestID:      "item-2",
			AccessIntent:   PhysicalAccessWrite,
			Lifecycle:      ClientPlanLifecycleMutation,
			LifecycleSteps: 7,
			IncludeProfile: true,
			Counters: []ExecutionCounter{{
				Name:  "rows",
				Value: 7,
				Unit:  "rows",
			}},
			Diagnostics: DiagnosticSet{{
				Code:     DiagnosticNativeBlocker,
				Severity: SeverityWarning,
				Phase:    PhasePlan,
				Message:  "note",
			}},
		},
	})

	exchange := service.PrepareClientBatchExecutionProfile(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch profile metadata", exchange)
	}
	if exchange.Batch.RequestID != "batch-1" || len(exchange.Rows) != 4 {
		t.Fatalf("exchange = %#v, want copied batch and four profile rows", exchange)
	}
	if exchange.Rows[0].Item != 0 || exchange.Rows[0].RequestID != "item-1" || exchange.Rows[0].Section != "explain" || exchange.Rows[0].Name != "logical" {
		t.Fatalf("first row = %#v, want item-1 logical explain row", exchange.Rows[0])
	}
	if exchange.Rows[0].AccessIntent != PhysicalAccessRead || exchange.Rows[0].Lifecycle != ClientPlanLifecycleSelect || exchange.Rows[0].LifecycleSteps != 7 {
		t.Fatalf("first row = %#v, want item-1 read/select lifecycle metadata", exchange.Rows[0])
	}
	if exchange.Rows[1].Item != 0 || exchange.Rows[1].Section != "timing" || exchange.Rows[1].Value.Value != int64(12) || exchange.Rows[1].Unit != "ms" {
		t.Fatalf("second row = %#v, want item-1 timing row", exchange.Rows[1])
	}
	if exchange.Rows[2].Item != 1 || exchange.Rows[2].Section != "counter" || exchange.Rows[2].Value.Value != uint64(7) {
		t.Fatalf("third row = %#v, want item-2 counter row", exchange.Rows[2])
	}
	if exchange.Rows[3].Item != 1 || exchange.Rows[3].Section != "diagnostic" || exchange.Rows[3].Name != string(DiagnosticNativeBlocker) {
		t.Fatalf("fourth row = %#v, want item-2 diagnostic row", exchange.Rows[3])
	}
	if exchange.Result.RequestID != "batch-1" || exchange.Result.RowsReturned != 4 || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want batch profile result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[1]
	if resultRow[0].Value != 0 || resultRow[1].Value != "item-1" || resultRow[2].Value != string(PhysicalAccessRead) || resultRow[5].Value != "timing" || resultRow[7].Value != int64(12) {
		t.Fatalf("result row = %#v, want item-1 timing cells", resultRow)
	}
}

func TestPlanningServicePrepareClientBatchExecutionProfileReturnsFailedEnvelopeForBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Profile: ExecutionProfile{
			RequestID: "item-1",
			Diagnostics: DiagnosticSet{
				ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "profile failed"),
			},
		},
	})

	exchange := service.PrepareClientBatchExecutionProfile(connection, batch)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking profile diagnostics", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].RequestID != "item-1" {
		t.Fatalf("rows = %#v, want diagnostic profile row retained", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want failed batch profile envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePrepareClientBatchExecutionProfileCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Profile: ExecutionProfile{
			RequestID: "item-1",
			Timings:   []ExecutionTiming{{Name: "scan"}},
			Counters:  []ExecutionCounter{{Name: "rows"}},
			Diagnostics: DiagnosticSet{{
				Code:   DiagnosticNativeBlocker,
				Fields: []FieldRef{{Name: "original"}},
			}},
		},
	})

	exchange := service.PrepareClientBatchExecutionProfile(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Batch.Items[0].Profile.Timings[0].Name = "mutated"
	exchange.Batch.Items[0].Profile.Diagnostics[0].Fields[0].Name = "mutated"
	exchange.Rows[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.PrepareClientBatchExecutionProfile(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Batch.Items[0].Profile.Timings[0].Name != "scan" {
		t.Fatalf("batch profile leaked mutation: %#v", again.Batch)
	}
	if again.Batch.Items[0].Profile.Diagnostics[0].Fields[0].Name != "original" {
		t.Fatalf("batch profile diagnostics leaked mutation: %#v", again.Batch.Items[0].Profile.Diagnostics)
	}
	if again.Rows[0].Name != "scan" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Item" || again.ResultSchema.Columns[0].Name != "Item" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][6].Value != "scan" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
