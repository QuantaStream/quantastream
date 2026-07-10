package qsbridge

import "testing"

func TestClientHandoffBundleResultPreviewBuildsQuerySchema(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	})

	preview := handoff.ResultPreviewBundle()
	if !preview.Supported() {
		t.Fatalf("diagnostics = %#v, want supported preview", preview.Diagnostics)
	}
	if len(preview.Statements) != 1 {
		t.Fatalf("statements = %#v, want one preview", preview.Statements)
	}
	statement := preview.Statements[0]
	if !statement.HasSchema || statement.HasStatementResponse {
		t.Fatalf("schema/statement flags = %v/%v, want query schema only", statement.HasSchema, statement.HasStatementResponse)
	}
	if statement.Result.Status != ExecutionPending || statement.Result.Complete {
		t.Fatalf("result status/complete = %q/%v, want pending query", statement.Result.Status, statement.Result.Complete)
	}
	if len(statement.Schema.Columns) != 1 || statement.Schema.Columns[0].WireType != "MYSQL_TYPE_LONGLONG" {
		t.Fatalf("schema = %#v, want mysql bigint metadata", statement.Schema)
	}
}

func TestClientHandoffBundleResultPreviewBuildsStatementResponse(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSessionStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	)
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "use analytics")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{})

	preview := handoff.ResultPreviewBundle()
	if !preview.Supported() {
		t.Fatalf("diagnostics = %#v, want supported statement preview", preview.Diagnostics)
	}
	statement := preview.Statements[0]
	if statement.HasSchema || !statement.HasStatementResponse {
		t.Fatalf("schema/statement flags = %v/%v, want statement response only", statement.HasSchema, statement.HasStatementResponse)
	}
	if statement.Result.Status != ExecutionComplete || !statement.Result.Complete {
		t.Fatalf("result status/complete = %q/%v, want complete statement", statement.Result.Status, statement.Result.Complete)
	}
	if !protocolStatusFlagsContain(statement.StatementResponse.Flags, ProtocolStatusSessionStateChanged) {
		t.Fatalf("flags = %#v, want session state change", statement.StatementResponse.Flags)
	}
}

func TestClientHandoffBundleResultPreviewCarriesRejectedOutcome(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = NativeOnlyRoutingPolicy()
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueString, "bad")},
	})

	preview := handoff.ResultPreviewBundle()
	if preview.Supported() {
		t.Fatalf("expected rejected preview to be unsupported")
	}
	statement := preview.Statements[0]
	if statement.Outcome.Kind != ExecutionHandoffRejected || statement.Result.Status != ExecutionFailed {
		t.Fatalf("outcome/result = %#v/%#v, want rejected failed preview", statement.Outcome, statement.Result)
	}
	if !containsDiagnosticCode(preview.Diagnostics.Codes(), DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejected", preview.Diagnostics.Codes())
	}
	if _, ok := preview.FirstProtocolError(); !ok {
		t.Fatalf("expected protocol error")
	}
}

func TestClientResultPreviewBundleCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	})

	preview := handoff.ResultPreviewBundle()
	preview.Connection.Attributes["client"] = "mutated"
	preview.Statements[0].Result.Columns[0].Name = "mutated"
	preview.Statements[0].Schema.Columns[0].Name = "mutated"
	again := handoff.ResultPreviewBundle()
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Statements[0].Result.Columns[0].Name == "mutated" {
		t.Fatalf("result columns leaked mutation")
	}
	if again.Statements[0].Schema.Columns[0].Name == "mutated" {
		t.Fatalf("schema columns leaked mutation")
	}
}

func serviceSessionStatement() UnboundStatement {
	return UnboundStatement{
		Kind: QueryKindSession,
		Session: UnboundSession{
			Actions: []SessionAction{{
				Kind:  SessionActionUseSchema,
				Value: "analytics",
			}},
			Result: ResultShape{
				Kind: ResultStatement,
				Statement: StatementResult{
					Status: "Database changed",
				},
			},
		},
	}
}
