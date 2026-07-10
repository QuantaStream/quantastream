package qsbridge

import "testing"

func TestPlanningServiceListClientStatementResponseStatusReturnsOKMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	response := StatementResult{
		AffectedRows: 3,
		LastInsertID: 42,
		Warnings:     1,
		Status:       "Rows matched: 3",
		SessionActions: []SessionAction{{
			Kind: SessionActionCommitTransaction,
		}},
	}.ProtocolStatementResponse(NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	))

	exchange := service.ListClientStatementResponseStatus(connection, response)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported statement response metadata", exchange)
	}
	if exchange.Status.AffectedRows != 3 || exchange.Status.LastInsertID != 42 || !exchange.Status.Transaction {
		t.Fatalf("status = %#v, want OK metadata and transaction flag", exchange.Status)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one status row", exchange.Result, exchange.ResultSchema)
	}
	row := exchange.Result.Chunks[0].Rows[0]
	if row[0].Value != 3 || row[1].Value != 42 || row[5].Value != true || row[6].Value == "" {
		t.Fatalf("row = %#v, want OK status metadata", row)
	}
}

func TestPlanningServiceListClientStatementResponseStatusCarriesDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	response := StatementResult{
		Status: "Database changed",
		SessionActions: []SessionAction{{
			Kind:  SessionActionUseSchema,
			Value: "analytics",
		}},
	}.ProtocolStatementResponse(NewProtocolProfile(ProtocolHTTP, "http"))

	exchange := service.ListClientStatementResponseStatus(connection, response)
	if exchange.Supported() {
		t.Fatalf("expected protocol diagnostics to block status metadata")
	}
	if !containsDiagnosticCode(exchange.Status.Diagnostics, DiagnosticInvalidExecutionOption) {
		t.Fatalf("status diagnostics = %#v, want invalid execution option", exchange.Status.Diagnostics)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed status envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientStatementResponseStatusCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	response := StatementResult{
		AffectedRows: 1,
		Status:       "original",
	}.ProtocolStatementResponse(NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults))

	exchange := service.ListClientStatementResponseStatus(connection, response)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Response.Status = "mutated"
	exchange.Status.Status = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.ListClientStatementResponseStatus(connection, response)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Response.Status != "original" || again.Status.Status != "original" {
		t.Fatalf("status leaked mutation: response=%#v status=%#v", again.Response, again.Status)
	}
	if again.Result.Columns[0].Name != "Affected_rows" || again.ResultSchema.Columns[0].Name != "Affected_rows" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][3].Value != "original" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
