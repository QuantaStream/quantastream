package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientBatchStatementResponseStatusReturnsItemCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{
			AffectedRows: 3,
			LastInsertID: 10,
			Status:       "Rows matched: 3",
			Notices:      []StatementNotice{{Level: StatementNoticeWarning, Message: "warning"}},
			SessionActions: []SessionAction{{
				Kind:  SessionActionSetVariable,
				Name:  "last_insert_id",
				Value: "10",
			}},
		},
	}).WithItem(ExecutionResult{
		RequestID: "item-2",
		Statement: StatementResult{
			Status: "BEGIN",
			SessionActions: []SessionAction{{
				Kind: SessionActionBeginTransaction,
			}},
		},
	})

	exchange := service.SummarizeClientBatchStatementResponseStatus(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch statement response status summary", exchange)
	}
	row := exchange.Row
	if row.ItemCount != 2 || row.TotalAffectedRows != 3 || row.ItemsWithAffectedRows != 1 || row.ItemsWithLastInsertID != 1 {
		t.Fatalf("row = %#v, want affected-row and insert-id counts", row)
	}
	if row.TotalWarnings != 1 || row.ItemsWithWarnings != 1 || row.TotalSessionActions != 2 || row.ItemsWithSessionActions != 2 {
		t.Fatalf("row = %#v, want warning and session-action counts", row)
	}
	if row.TransactionItemCount != 1 || row.RowsAffectedFlagCount != 1 || row.LastInsertIDFlagCount != 1 || row.WarningsFlagCount != 1 {
		t.Fatalf("row = %#v, want transaction and status flag counts", row)
	}
	if row.SessionStateChangedCount != 2 || row.TransactionFlagCount != 1 || row.ItemsWithDiagnostics != 0 {
		t.Fatalf("row = %#v, want session-state and diagnostic counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 15 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one batch status summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 2 || resultRow[1].Value != 3 || resultRow[13].Value != 2 {
		t.Fatalf("result row = %#v, want batch status summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientBatchStatementResponseStatusCarriesProtocolDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolHTTP, "http")
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{
			Status:       "Rows matched: 1",
			AffectedRows: 1,
		},
	})

	exchange := service.SummarizeClientBatchStatementResponseStatus(connection, batch)
	if exchange.Supported() {
		t.Fatalf("expected unsupported protocol statement result capability")
	}
	if row := exchange.Row; row.ItemCount != 1 || row.ItemsWithDiagnostics != 1 || row.RowsAffectedFlagCount != 1 {
		t.Fatalf("row = %#v, want unsupported protocol diagnostic counted", exchange.Row)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed metadata result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientBatchStatementResponseStatusCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{AffectedRows: 1, Status: "original"},
	})

	exchange := service.SummarizeClientBatchStatementResponseStatus(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Row.ItemCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientBatchStatementResponseStatus(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Row.ItemCount != 1 || again.Row.TotalAffectedRows != 1 {
		t.Fatalf("batch status summary leaked mutation: batch=%#v row=%#v", again.Batch, again.Row)
	}
	if again.Result.Columns[0].Name != "Item_count" || again.ResultSchema.Columns[0].Name != "Item_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
