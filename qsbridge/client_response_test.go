package qsbridge

import "testing"

func TestClientExchangeResponseSequenceLabelsQueryItems(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	exchange := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
		"select 2",
	)

	sequence := exchange.ResponseSequence()
	if !sequence.Supported() {
		t.Fatalf("diagnostics = %#v, want supported sequence", sequence.Diagnostics)
	}
	if len(sequence.Items) != 2 {
		t.Fatalf("items = %#v, want two response items", sequence.Items)
	}
	if sequence.Items[0].Kind != ClientResponseQuery || sequence.Items[1].Kind != ClientResponseQuery {
		t.Fatalf("kinds = %q/%q, want query/query", sequence.Items[0].Kind, sequence.Items[1].Kind)
	}
	if !sequence.Items[0].MoreResults || sequence.Items[0].Final || sequence.Items[1].MoreResults || !sequence.Items[1].Final {
		t.Fatalf("more/final flags = %#v/%#v, want first more and second final", sequence.Items[0], sequence.Items[1])
	}
	if !clientResponseFlagsContain(sequence.Items[0].Flags, ClientResponseFlagMoreResults) || !clientResponseFlagsContain(sequence.Items[0].Flags, ClientResponseFlagQuery) {
		t.Fatalf("first flags = %#v, want more-results query metadata", sequence.Items[0].Flags)
	}
	if !clientResponseFlagsContain(sequence.Items[1].Flags, ClientResponseFlagFinal) || !clientResponseFlagsContain(sequence.Items[1].Flags, ClientResponseFlagQuery) {
		t.Fatalf("second flags = %#v, want final query metadata", sequence.Items[1].Flags)
	}
	if sequence.Items[0].Schema.Columns[0].WireType != "MYSQL_TYPE_LONGLONG" {
		t.Fatalf("schema = %#v, want mysql bigint", sequence.Items[0].Schema)
	}
}

func TestClientExchangeResponseSequenceLabelsStatementItems(t *testing.T) {
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
	exchange := service.PrepareConnectionClientExchange(connection, ConnectionPlanOptions{}, ClientHandoffOptions{}, "use analytics")

	sequence := exchange.ResponseSequence()
	if !sequence.Supported() {
		t.Fatalf("diagnostics = %#v, want supported sequence", sequence.Diagnostics)
	}
	if len(sequence.Items) != 1 || sequence.Items[0].Kind != ClientResponseStatement {
		t.Fatalf("items = %#v, want one statement response", sequence.Items)
	}
	if !clientResponseFlagsContain(sequence.Items[0].Flags, ClientResponseFlagStatement) || !clientResponseFlagsContain(sequence.Items[0].Flags, ClientResponseFlagComplete) {
		t.Fatalf("flags = %#v, want complete statement metadata", sequence.Items[0].Flags)
	}
	if !protocolStatusFlagsContain(sequence.Items[0].StatementResponse.Flags, ProtocolStatusSessionStateChanged) {
		t.Fatalf("flags = %#v, want session state changed", sequence.Items[0].StatementResponse.Flags)
	}
}

func TestClientExchangeResponseSequenceLabelsErrors(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = NativeOnlyRoutingPolicy()
	exchange := service.PrepareConnectionClientExchange(
		clientStatementConnection(),
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueString, "bad")}},
		"select 1",
	)

	sequence := exchange.ResponseSequence()
	if sequence.Supported() {
		t.Fatalf("expected sequence to be unsupported")
	}
	if len(sequence.Items) != 1 || sequence.Items[0].Kind != ClientResponseError {
		t.Fatalf("items = %#v, want one error response", sequence.Items)
	}
	if len(sequence.Items[0].Errors) == 0 {
		t.Fatalf("expected protocol errors")
	}
	if !clientResponseFlagsContain(sequence.Items[0].Flags, ClientResponseFlagError) || !clientResponseFlagsContain(sequence.Items[0].Flags, ClientResponseFlagFinal) {
		t.Fatalf("flags = %#v, want final error metadata", sequence.Items[0].Flags)
	}
}

func TestClientStatementResultPreviewResponseItemCarriesStreamingCursorFlags(t *testing.T) {
	preview := ClientStatementResultPreview{
		Statement: ClientStatementText{Ordinal: 1, SQL: "select streaming"},
		Outcome:   ExecutionHandoffOutcome{Supported: true},
		Result: ExecutionResult{
			Status: ExecutionStreaming,
			Kind:   ResultQuery,
			Cursor: CursorDescriptor{
				ID:    "cursor-1",
				Mode:  CursorForwardOnly,
				State: CursorStateOpen,
			},
		},
		Schema:    NewProtocolResultSchema(NewProtocolProfile(ProtocolMySQL, "mysql"), nil),
		HasSchema: true,
	}

	item := preview.responseItem(NewProtocolProfile(ProtocolMySQL, "mysql"), 0, 1)
	if item.Kind != ClientResponseQuery {
		t.Fatalf("item = %#v, want query response", item)
	}
	for _, flag := range []ClientResponseFlag{
		ClientResponseFlagFinal,
		ClientResponseFlagQuery,
		ClientResponseFlagStreaming,
		ClientResponseFlagCursorOpen,
	} {
		if !clientResponseFlagsContain(item.Flags, flag) {
			t.Fatalf("flags = %#v, want %q", item.Flags, flag)
		}
	}
}

func TestClientResponseSequenceCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	exchange := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
	)

	sequence := exchange.ResponseSequence()
	sequence.Connection.Attributes["client"] = "mutated"
	sequence.Items[0].Schema.Columns[0].Name = "mutated"
	again := exchange.ResponseSequence()
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Items[0].Schema.Columns[0].Name == "mutated" {
		t.Fatalf("schema leaked mutation")
	}
}

func clientResponseFlagsContain(flags []ClientResponseFlag, want ClientResponseFlag) bool {
	for _, flag := range flags {
		if flag == want {
			return true
		}
	}
	return false
}
