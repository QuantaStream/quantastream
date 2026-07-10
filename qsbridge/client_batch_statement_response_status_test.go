package qsbridge

import "testing"

func TestPlanningServiceListClientBatchStatementResponseStatusReturnsItemRows(t *testing.T) {
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

	exchange := service.ListClientBatchStatementResponseStatus(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch statement response status", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want two batch item status rows", exchange.Rows)
	}
	first := exchange.Rows[0]
	if first.Item != 0 || first.RequestID != "item-1" || first.AffectedRows != 3 || first.LastInsertID != 10 || first.Warnings != 1 || first.SessionActions != 1 {
		t.Fatalf("first row = %#v, want item-1 OK metadata", first)
	}
	if !containsProtocolStatusFlag(first.Flags, ProtocolStatusRowsAffected) || !containsProtocolStatusFlag(first.Flags, ProtocolStatusWarnings) {
		t.Fatalf("first flags = %#v, want affected rows and warnings", first.Flags)
	}
	second := exchange.Rows[1]
	if second.Item != 1 || second.RequestID != "item-2" || !second.Transaction || !containsProtocolStatusFlag(second.Flags, ProtocolStatusTransactionAction) {
		t.Fatalf("second row = %#v, want transaction metadata", second)
	}
	if exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want batch status result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != "item-1" || resultRow[2].Value != 3 || resultRow[4].Value != 1 || resultRow[6].Value != 1 {
		t.Fatalf("result row = %#v, want first item cells", resultRow)
	}
}

func TestPlanningServiceListClientBatchStatementResponseStatusCarriesProtocolDiagnostics(t *testing.T) {
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

	exchange := service.ListClientBatchStatementResponseStatus(connection, batch)
	if exchange.Supported() {
		t.Fatalf("expected unsupported protocol statement result capability")
	}
	if len(exchange.Rows) != 1 || !containsDiagnosticCode(exchange.Rows[0].Diagnostics, DiagnosticInvalidExecutionOption) {
		t.Fatalf("rows = %#v, want protocol unsupported diagnostic", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed metadata result", exchange.Result)
	}
}

func TestPlanningServiceListClientBatchStatementResponseStatusCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{AffectedRows: 1, Status: "original"},
	})

	exchange := service.ListClientBatchStatementResponseStatus(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Rows[0].RequestID = "mutated"
	exchange.Rows[0].Flags = append(exchange.Rows[0].Flags, ProtocolStatusWarnings)
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientBatchStatementResponseStatus(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Rows[0].RequestID != "item-1" {
		t.Fatalf("batch status leaked mutation: batch=%#v rows=%#v", again.Batch, again.Rows)
	}
	if containsProtocolStatusFlag(again.Rows[0].Flags, ProtocolStatusWarnings) {
		t.Fatalf("flags leaked mutation: %#v", again.Rows[0].Flags)
	}
	if again.Result.Columns[0].Name != "Item" || again.ResultSchema.Columns[0].Name != "Item" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "item-1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func containsProtocolStatusFlag(flags []ProtocolStatusFlag, want ProtocolStatusFlag) bool {
	for _, flag := range flags {
		if flag == want {
			return true
		}
	}
	return false
}
