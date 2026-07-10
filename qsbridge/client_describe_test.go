package qsbridge

import "testing"

func TestPlanningServiceDescribeClientStatementReturnsQueryMetadata(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	described := service.DescribeClientStatement(ClientDescribeStatementRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	})
	if !described.Supported() {
		t.Fatalf("diagnostics = %#v, want supported describe", described.Diagnostics)
	}
	if parser.count != 1 {
		t.Fatalf("parser count = %d, want one planning pass", parser.count)
	}
	if len(described.Description.Parameters) != 1 || described.Description.Parameters[0].Type != DataTypeInt {
		t.Fatalf("parameters = %#v, want one int placeholder", described.Description.Parameters)
	}
	if !described.HasResultSchema || described.HasStatementResponse {
		t.Fatalf("schema/statement flags = %v/%v, want query schema only", described.HasResultSchema, described.HasStatementResponse)
	}
	if len(described.ResultSchema.Columns) != 1 || described.ResultSchema.Columns[0].Name != "order_id" || described.ResultSchema.Columns[0].WireType != "MYSQL_TYPE_LONGLONG" {
		t.Fatalf("schema = %#v, want mysql order_id metadata", described.ResultSchema)
	}
}

func TestPlanningServiceDescribeClientStatementReturnsStatementMetadata(t *testing.T) {
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

	described := service.DescribeClientStatement(ClientDescribeStatementRequest{
		Connection: connection,
		SQL:        "use analytics",
	})
	if !described.Supported() {
		t.Fatalf("diagnostics = %#v, want supported statement describe", described.Diagnostics)
	}
	if described.HasResultSchema || !described.HasStatementResponse {
		t.Fatalf("schema/statement flags = %v/%v, want statement response only", described.HasResultSchema, described.HasStatementResponse)
	}
	if described.StatementResponse.Status != "Database changed" {
		t.Fatalf("statement response = %#v, want database changed status", described.StatementResponse)
	}
	if !protocolStatusFlagsContain(described.StatementResponse.Flags, ProtocolStatusSessionStateChanged) {
		t.Fatalf("flags = %#v, want session state changed", described.StatementResponse.Flags)
	}
}

func TestPlanningServiceDescribeClientPreparedStatementUsesRegistryWithoutReparse(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)

	described := service.DescribeClientPreparedStatement(connection, registry, prepared.Description.Handle)
	if !described.Supported() {
		t.Fatalf("diagnostics = %#v, want supported prepared describe", described.Diagnostics)
	}
	if parser.count != 1 {
		t.Fatalf("parser count = %d, want prepared describe to avoid parsing", parser.count)
	}
	if described.Description.Handle.ID != prepared.Description.Handle.ID {
		t.Fatalf("handle = %#v, want prepared handle %#v", described.Description.Handle, prepared.Description.Handle)
	}
	if !described.HasResultSchema || len(described.ResultSchema.Columns) != 1 {
		t.Fatalf("schema = %#v, want prepared result schema", described.ResultSchema)
	}
}

func TestPlanningServiceDescribeClientPreparedStatementReportsMissingRegistryOrHandle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)

	missingRegistry := service.DescribeClientPreparedStatement(connection, nil, PreparedStatementHandle{ID: 1})
	if missingRegistry.Supported() || !containsDiagnosticCode(missingRegistry.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing registry = %#v, want unsupported diagnostic", missingRegistry)
	}

	missingHandle := service.DescribeClientPreparedStatement(connection, NewMemoryPreparedStatementRegistry(), PreparedStatementHandle{ID: 99})
	if missingHandle.Supported() || !containsDiagnosticCode(missingHandle.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing handle = %#v, want unsupported diagnostic", missingHandle)
	}
}

func TestPlanningServiceDescribeClientStatementCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	described := service.DescribeClientStatement(ClientDescribeStatementRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	})
	described.Connection.Attributes["client"] = "mutated"
	described.Description.Parameters[0].Type = DataTypeString
	described.Description.ResultColumns[0].Name = "mutated"
	described.ResultSchema.Columns[0].Name = "mutated"

	again := service.DescribeClientStatement(ClientDescribeStatementRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Description.Parameters[0].Type != DataTypeInt {
		t.Fatalf("parameter metadata leaked mutation: %#v", again.Description.Parameters)
	}
	if again.Description.ResultColumns[0].Name != "order_id" || again.ResultSchema.Columns[0].Name != "order_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Description.ResultColumns, again.ResultSchema.Columns)
	}
}
