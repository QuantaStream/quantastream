package qsbridge

import "testing"

func TestAdapterReadinessNextActionsPickOneGatePerSurface(t *testing.T) {
	actions := DefaultAdapterReadinessNextActions()
	if len(actions) != 4 {
		t.Fatalf("actions = %#v, want one next action per surface", actions)
	}
	mysql := adapterReadinessNextActionBySurface(actions, AdapterSurfaceMySQLServer)
	if mysql == nil || mysql.Gate != AdapterReadinessGateAdapterShell ||
		mysql.Order != 2 || mysql.BlockerCount != 1 ||
		mysql.Owner != WireAdapterOwnerProtocolAdapter {
		t.Fatalf("mysql action = %#v, want protocol adapter shell next", mysql)
	}
	internal := adapterReadinessNextActionBySurface(actions, AdapterSurfaceInternalExecution)
	if internal == nil || internal.Gate != AdapterReadinessGateContracts ||
		internal.Order != 0 || internal.BlockerCount != 1 ||
		internal.Owner != WireAdapterOwnerExecutor {
		t.Fatalf("internal action = %#v, want runtime-owned contract gate next", internal)
	}
}

func TestAdapterReadinessNextActionsFilterSurface(t *testing.T) {
	actions := AdapterReadinessNextActionsForSurface(AdapterSurfaceEmbedded)
	if len(actions) != 1 {
		t.Fatalf("actions = %#v, want one embedded next action", actions)
	}
	action := actions[0]
	if action.Surface != AdapterSurfaceEmbedded || action.Gate != AdapterReadinessGateAdapterShell ||
		action.Owner != WireAdapterOwnerExecutor || !action.BlocksRuntime {
		t.Fatalf("action = %#v, want embedded adapter shell action", action)
	}
}

func TestPlanningServiceListClientAdapterReadinessNextActionsReturnsAllSurfaces(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientAdapterReadinessNextActions(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter readiness next-action metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want one next-action row per adapter surface", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter readiness next-action columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per next action", exchange.Result)
	}
}

func TestPlanningServiceListClientAdapterReadinessNextActionsFiltersSurface(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientAdapterReadinessNextActions(connection, AdapterSurfaceGRPCAPI)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported gRPC next-action metadata", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Surface != AdapterSurfaceGRPCAPI {
		t.Fatalf("rows = %#v, want gRPC next action", exchange.Rows)
	}
	if exchange.Rows[0].Gate != AdapterReadinessGateAdapterShell ||
		exchange.Rows[0].Owner != WireAdapterOwnerProtocolAdapter {
		t.Fatalf("row = %#v, want gRPC adapter shell next action", exchange.Rows[0])
	}
}

func TestPlanningServiceListClientAdapterReadinessNextActionsReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientAdapterReadinessNextActions(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientAdapterReadinessNextActionsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientAdapterReadinessNextActions(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][7].Value = "mutated"

	again := service.ListClientAdapterReadinessNextActions(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Detail == "mutated" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][7].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func adapterReadinessNextActionBySurface(actions []AdapterReadinessNextAction, surface AdapterSurfaceKind) *AdapterReadinessNextAction {
	for i := range actions {
		if actions[i].Surface == surface {
			return &actions[i]
		}
	}
	return nil
}
