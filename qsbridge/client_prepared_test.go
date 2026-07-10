package qsbridge

import "testing"

func TestPlanningServicePrepareClientPreparedStatementRegistersDescription(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)

	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)
	if !prepared.Supported() || !prepared.Registered {
		t.Fatalf("prepared = %#v, want supported registered prepare", prepared)
	}
	if parser.count != 1 {
		t.Fatalf("parser count = %d, want one prepare", parser.count)
	}
	if prepared.Description.Handle.ID == 0 || prepared.Description.Handle.Name != "stmt_orders" {
		t.Fatalf("handle = %#v, want allocated named handle", prepared.Description.Handle)
	}
	if len(prepared.Description.Parameters) != 1 || len(prepared.Description.ResultColumns) != 1 {
		t.Fatalf("description = %#v, want parameter and result metadata", prepared.Description)
	}
	if _, ok := registry.Get(prepared.Description.Handle); !ok {
		t.Fatalf("expected registered handle lookup")
	}
}

func TestPlanningServiceExecuteClientPreparedStatementUsesRegistryWithoutReparse(t *testing.T) {
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

	executed := service.ExecuteClientPreparedStatement(connection, registry, prepared.Description.Handle, ClientPreparedExecutionOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	})
	if !executed.Supported() {
		t.Fatalf("diagnostics = %#v, want supported prepared execution", executed.Diagnostics)
	}
	if parser.count != 1 {
		t.Fatalf("parser count = %d, want execute by handle to avoid parsing", parser.count)
	}
	if executed.Handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kind = %q, want native", executed.Handoff.HandoffKind())
	}
	if executed.Response.Kind != ClientResponseQuery || !executed.Response.Final {
		t.Fatalf("response = %#v, want final query response", executed.Response)
	}
}

func TestPlanningServiceExecuteClientPreparedStatementReportsMissingHandle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)

	executed := service.ExecuteClientPreparedStatement(connection, NewMemoryPreparedStatementRegistry(), PreparedStatementHandle{ID: 99}, ClientPreparedExecutionOptions{})
	if executed.Supported() {
		t.Fatalf("expected missing handle to be unsupported")
	}
	if executed.Response.Kind != ClientResponseError || len(executed.Response.Errors) == 0 {
		t.Fatalf("response = %#v, want protocol error response", executed.Response)
	}
	if !containsDiagnosticCode(executed.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", executed.Diagnostics)
	}
}

func TestPlanningServiceExecuteClientPreparedBatchStatementUsesRegistryWithoutReparse(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityBatchExecution)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)

	executed := service.ExecuteClientPreparedBatchStatement(connection, registry, prepared.Description.Handle, ClientPreparedBatchExecutionOptions{
		Options: ExecutionOptions{RequestID: "batch-1", BatchSize: 2},
		ParameterSets: []ParameterValueSet{
			ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
			ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
		},
	})
	if !executed.Supported() {
		t.Fatalf("diagnostics = %#v, want supported prepared batch execution", executed.Diagnostics)
	}
	if parser.count != 1 {
		t.Fatalf("parser count = %d, want batch execute by handle to avoid parsing", parser.count)
	}
	if executed.Handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kind = %q, want native", executed.Handoff.HandoffKind())
	}
	if len(executed.Handoff.Request.ParameterSets) != 2 {
		t.Fatalf("parameter sets = %#v, want two sets", executed.Handoff.Request.ParameterSets)
	}
	if executed.Result.RequestID != "batch-1" || executed.Result.Kind != ResultQuery {
		t.Fatalf("result = %#v, want pending query batch envelope", executed.Result)
	}
}

func TestPlanningServiceExecuteClientPreparedBatchStatementRejectsUnsupportedProtocol(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
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

	executed := service.ExecuteClientPreparedBatchStatement(connection, registry, prepared.Description.Handle, ClientPreparedBatchExecutionOptions{
		ParameterSets: []ParameterValueSet{
			ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		},
	})
	if executed.Supported() {
		t.Fatalf("expected missing batch protocol capability to be unsupported")
	}
	if executed.Handoff.HandoffKind() != ExecutionHandoffProtocolRejected {
		t.Fatalf("handoff kind = %q, want protocol rejected", executed.Handoff.HandoffKind())
	}
	if !containsDiagnosticCode(executed.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", executed.Diagnostics)
	}
}

func TestPlanningServiceExecuteClientPreparedBatchStatementReportsInvalidParameterSet(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityBatchExecution)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)

	executed := service.ExecuteClientPreparedBatchStatement(connection, registry, prepared.Description.Handle, ClientPreparedBatchExecutionOptions{
		ParameterSets: []ParameterValueSet{
			ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
			ParameterValues(IndexedParameterValue(1, ValueString, "bad")),
		},
	})
	if executed.Supported() {
		t.Fatalf("expected invalid parameter set to be unsupported")
	}
	if executed.Handoff.HandoffKind() != ExecutionHandoffLegacyFallback {
		t.Fatalf("handoff kind = %q, want legacy fallback", executed.Handoff.HandoffKind())
	}
	if !containsDiagnosticCode(executed.Diagnostics.Codes(), DiagnosticParameterTypeMismatch) {
		t.Fatalf("diagnostics = %#v, want parameter type mismatch", executed.Diagnostics)
	}
}

func TestPlanningServiceExecuteClientPreparedBatchStatementReportsMissingHandle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityBatchExecution)

	executed := service.ExecuteClientPreparedBatchStatement(connection, NewMemoryPreparedStatementRegistry(), PreparedStatementHandle{ID: 99}, ClientPreparedBatchExecutionOptions{})
	if executed.Supported() {
		t.Fatalf("expected missing handle to be unsupported")
	}
	if !executed.Result.Complete || executed.Result.Status != ExecutionFailed {
		t.Fatalf("result = %#v, want failed complete batch result", executed.Result)
	}
	if !containsDiagnosticCode(executed.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", executed.Diagnostics)
	}
}

func TestPlanningServiceCloseClientPreparedStatementClosesRegisteredHandle(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		Handle:     PreparedStatementHandle{Name: "stmt_orders"},
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)

	closed := service.CloseClientPreparedStatement(connection, registry, prepared.Description.Handle)
	if !closed.Supported() || !closed.Closed {
		t.Fatalf("closed = %#v, want supported close", closed)
	}
	if closed.Response.Kind != ClientResponseStatement || !closed.Response.Final {
		t.Fatalf("response = %#v, want final statement response", closed.Response)
	}
	if closed.Response.StatementResponse.Status != "Prepared statement closed" {
		t.Fatalf("status = %q, want close status", closed.Response.StatementResponse.Status)
	}
	if _, ok := registry.Get(prepared.Description.Handle); ok {
		t.Fatalf("expected close to remove registered handle")
	}
}

func TestPlanningServiceCloseClientPreparedStatementReportsMissingHandle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)

	closed := service.CloseClientPreparedStatement(connection, NewMemoryPreparedStatementRegistry(), PreparedStatementHandle{ID: 99})
	if closed.Supported() || closed.Closed {
		t.Fatalf("closed = %#v, want missing handle to be unsupported", closed)
	}
	if closed.Response.Kind != ClientResponseError || len(closed.Response.Errors) == 0 {
		t.Fatalf("response = %#v, want protocol error response", closed.Response)
	}
	if !containsDiagnosticCode(closed.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", closed.Diagnostics)
	}
}

func TestPlanningServiceCloseClientPreparedStatementRejectsEmptyHandle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)

	closed := service.CloseClientPreparedStatement(connection, NewMemoryPreparedStatementRegistry(), PreparedStatementHandle{})
	if closed.Supported() || closed.Closed {
		t.Fatalf("closed = %#v, want empty handle to be unsupported", closed)
	}
	if closed.Response.Kind != ClientResponseError || len(closed.Response.Errors) == 0 {
		t.Fatalf("response = %#v, want protocol error response", closed.Response)
	}
	if !containsDiagnosticCode(closed.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", closed.Diagnostics)
	}
}

func TestPlanningServiceCloseClientPreparedStatementKeepsHandleWhenProtocolCannotRespond(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
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

	closed := service.CloseClientPreparedStatement(connection, registry, prepared.Description.Handle)
	if closed.Supported() || closed.Closed {
		t.Fatalf("closed = %#v, want protocol response capability error", closed)
	}
	if closed.Response.Kind != ClientResponseError || len(closed.Response.Errors) == 0 {
		t.Fatalf("response = %#v, want protocol error response", closed.Response)
	}
	if _, ok := registry.Get(prepared.Description.Handle); !ok {
		t.Fatalf("expected unsupported protocol close to keep registered handle")
	}
}

func TestPlanningServiceResetClientPreparedStatementClearsLongDataButKeepsHandle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	preparedRegistry := NewMemoryPreparedStatementRegistry()
	longDataRegistry := NewMemoryPreparedLongDataRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)
	description := preparedRegistry.Register(PreparedPlan{
		Handle:     PreparedStatementHandle{Name: "stmt_blob"},
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeString, Nullable: true}},
		Supported:  true,
	})
	fragment := PreparedLongDataFragment{
		Handle:     description.Handle,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	}
	if stored := service.StoreClientPreparedLongData(connection, preparedRegistry, longDataRegistry, fragment); !stored.Supported() {
		t.Fatalf("stored = %#v, want supported long-data setup", stored)
	}

	reset := service.ResetClientPreparedStatement(connection, preparedRegistry, longDataRegistry, description.Handle)
	if !reset.Supported() || !reset.Reset || !reset.ClearedLongData {
		t.Fatalf("reset = %#v, want supported reset with cleared long data", reset)
	}
	if reset.Response.Kind != ClientResponseStatement || reset.Response.StatementResponse.Status != "Prepared statement reset" {
		t.Fatalf("response = %#v, want reset OK response", reset.Response)
	}
	if _, ok := longDataRegistry.Get(description.Handle, fragment.Parameter); ok {
		t.Fatalf("expected reset to clear long-data state")
	}
	if _, ok := preparedRegistry.Get(description.Handle); !ok {
		t.Fatalf("expected reset to keep prepared handle registered")
	}
}

func TestPlanningServiceResetClientPreparedStatementReportsMissingHandle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)

	reset := service.ResetClientPreparedStatement(connection, NewMemoryPreparedStatementRegistry(), NewMemoryPreparedLongDataRegistry(), PreparedStatementHandle{ID: 99})
	if reset.Supported() || reset.Reset {
		t.Fatalf("reset = %#v, want missing handle to be unsupported", reset)
	}
	if reset.Response.Kind != ClientResponseError || len(reset.Response.Errors) == 0 {
		t.Fatalf("response = %#v, want protocol error response", reset.Response)
	}
	if !containsDiagnosticCode(reset.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", reset.Diagnostics)
	}
}

func TestPlanningServiceStoreClientPreparedLongDataAccumulatesMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	preparedRegistry := NewMemoryPreparedStatementRegistry()
	longDataRegistry := NewMemoryPreparedLongDataRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	description := preparedRegistry.Register(PreparedPlan{
		Handle:     PreparedStatementHandle{Name: "stmt_blob"},
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeString, Nullable: true}},
		Supported:  true,
	})

	first := service.StoreClientPreparedLongData(connection, preparedRegistry, longDataRegistry, PreparedLongDataFragment{
		Handle:     description.Handle,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	})
	if !first.Supported() || !first.Stored {
		t.Fatalf("first = %#v, want supported stored long-data fragment", first)
	}

	second := service.StoreClientPreparedLongData(connection, preparedRegistry, longDataRegistry, PreparedLongDataFragment{
		Handle:     description.Handle,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 5,
		Final:      true,
	})
	if !second.Supported() || !second.Stored {
		t.Fatalf("second = %#v, want supported stored long-data fragment", second)
	}
	if second.State.Chunks != 2 || second.State.TotalBytes != 17 || !second.State.Final {
		t.Fatalf("state = %#v, want accumulated chunks bytes final", second.State)
	}
}

func TestPlanningServiceStoreClientPreparedLongDataReportsMissingHandle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)

	stored := service.StoreClientPreparedLongData(connection, NewMemoryPreparedStatementRegistry(), NewMemoryPreparedLongDataRegistry(), PreparedLongDataFragment{
		Handle:     PreparedStatementHandle{ID: 99},
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	})
	if stored.Supported() || stored.Stored {
		t.Fatalf("stored = %#v, want missing handle to be unsupported", stored)
	}
	if !containsDiagnosticCode(stored.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", stored.Diagnostics)
	}
}

func TestPlanningServiceStoreClientPreparedLongDataReportsUnknownParameter(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	preparedRegistry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	description := preparedRegistry.Register(PreparedPlan{
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeString, Nullable: true}},
		Supported:  true,
	})

	stored := service.StoreClientPreparedLongData(connection, preparedRegistry, NewMemoryPreparedLongDataRegistry(), PreparedLongDataFragment{
		Handle:     description.Handle,
		Parameter:  IndexedParameterValue(2, ValueString, nil),
		ChunkBytes: 12,
	})
	if stored.Supported() || stored.Stored {
		t.Fatalf("stored = %#v, want unknown parameter to be unsupported", stored)
	}
	if !containsDiagnosticCode(stored.Diagnostics.Codes(), DiagnosticParameterExtra) {
		t.Fatalf("diagnostics = %#v, want parameter extra", stored.Diagnostics)
	}
}

func TestClientPreparedExchangeCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	prepared := service.PrepareClientPreparedStatement(ClientPrepareRequest{
		Connection: connection,
		SQL:        "select o_orderkey from orders where o_orderkey = ?",
	}, registry)

	executed := service.ExecuteClientPreparedStatement(connection, registry, prepared.Description.Handle, ClientPreparedExecutionOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	})
	executed.Connection.Attributes["client"] = "mutated"
	executed.Response.Schema.Columns[0].Name = "mutated"
	again := service.ExecuteClientPreparedStatement(connection, registry, prepared.Description.Handle, ClientPreparedExecutionOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Response.Schema.Columns[0].Name == "mutated" {
		t.Fatalf("response schema leaked mutation")
	}
}
