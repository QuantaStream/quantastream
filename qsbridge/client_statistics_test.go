package qsbridge

import "testing"

func TestPlanningServicePrepareClientStatisticsFormatsSortedSummary(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	variables := []ClientStatusVariable{
		{Name: "Threads_connected", Value: "2"},
		{Name: "Connections", Value: "10"},
		{Name: "Questions", Value: "42"},
	}

	exchange := service.PrepareClientStatistics(connection, variables)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported statistics metadata", exchange)
	}
	if len(exchange.Variables) != 3 || exchange.Variables[0].Name != "Connections" || exchange.Variables[2].Name != "Threads_connected" {
		t.Fatalf("variables = %#v, want sorted status values", exchange.Variables)
	}
	want := "Connections: 10  Questions: 42  Threads_connected: 2"
	if exchange.Summary != want {
		t.Fatalf("summary = %q, want %q", exchange.Summary, want)
	}
}

func TestPlanningServicePrepareClientStatisticsReturnsUnsupportedConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.PrepareClientStatistics(connection, nil)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", exchange.Diagnostics)
	}
}

func TestPlanningServicePrepareClientStatisticsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	variables := []ClientStatusVariable{{Name: "Connections", Value: "10"}}

	exchange := service.PrepareClientStatistics(connection, variables)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Variables[0].Value = "mutated"

	again := service.PrepareClientStatistics(connection, variables)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Variables[0].Value != "10" || again.Summary != "Connections: 10" {
		t.Fatalf("statistics metadata leaked mutation: %#v/%q", again.Variables, again.Summary)
	}
}
