package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientTransactionReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	transaction := service.PrepareClientBeginTransaction(connection, nil, ClientTransactionOptions{})
	exchange := service.SummarizeClientTransaction(connection, transaction)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported transaction summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one transaction row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Action != SessionActionBeginTransaction || row.Status != "Transaction started" || !row.Supported {
		t.Fatalf("row = %#v, want begin transaction metadata", row)
	}
	if row.SessionActions != 1 || !protocolStatusFlagsContain(row.StatusFlags, ProtocolStatusSessionStateChanged) {
		t.Fatalf("row = %#v, want session action and status flag", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(SessionActionBeginTransaction) || resultRow[2].Value != true || resultRow[4].Value != 1 {
		t.Fatalf("result row = %#v, want transaction cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientTransactionKeepsUnsupportedActionAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	transaction := service.PrepareClientTransactionAction(connection, nil, SessionAction{Kind: SessionActionSetVariable, Name: "autocommit", Value: "0"}, ClientTransactionOptions{})
	exchange := service.SummarizeClientTransaction(connection, transaction)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want summary metadata to be supported", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported transaction row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want complete metadata result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientTransactionCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)

	transaction := service.PrepareClientCommitTransaction(connection, nil, ClientTransactionOptions{})
	exchange := service.SummarizeClientTransaction(connection, transaction)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Transaction.Response.SessionActions[0].Kind = SessionActionRollbackTransaction
	exchange.Rows[0].Status = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientTransaction(connection, transaction)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Transaction.Response.SessionActions[0].Kind != SessionActionCommitTransaction || again.Rows[0].Status != "Transaction committed" {
		t.Fatalf("transaction summary leaked mutation: %#v/%#v", again.Transaction.Response.SessionActions, again.Rows)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "Transaction committed" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
