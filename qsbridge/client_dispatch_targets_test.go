package qsbridge

import "testing"

func TestDefaultDispatchTargetProfilesDescribeExecutorBoundaries(t *testing.T) {
	profiles := DefaultDispatchTargetProfiles()
	if len(profiles) != 3 {
		t.Fatalf("profiles = %#v, want native, legacy, and no-dispatch targets", profiles)
	}
	native := dispatchTargetProfileByTarget(profiles, DispatchTargetNative)
	if native == nil || native.Handoff != ExecutionHandoffNative ||
		!native.RuntimeOwned || !native.RequiresExecutor || !native.Configurable || native.Terminal {
		t.Fatalf("native = %#v, want configurable native executor target", native)
	}
	legacy := dispatchTargetProfileByTarget(profiles, DispatchTargetLegacy)
	if legacy == nil || legacy.Handoff != ExecutionHandoffLegacyFallback ||
		!legacy.RuntimeOwned || !legacy.RequiresExecutor || !legacy.Configurable || legacy.Terminal {
		t.Fatalf("legacy = %#v, want configurable legacy executor target", legacy)
	}
	none := dispatchTargetProfileByTarget(profiles, DispatchTargetNone)
	if none == nil || none.Handoff != ExecutionHandoffRejected ||
		none.RuntimeOwned || none.RequiresExecutor || none.Configurable || !none.Terminal {
		t.Fatalf("none = %#v, want terminal rejected target", none)
	}
}

func TestPlanningServiceListClientDispatchTargetsReturnsProfiles(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientDispatchTargets(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported dispatch target metadata", exchange)
	}
	if len(exchange.Rows) != 3 {
		t.Fatalf("rows = %#v, want dispatch target profiles", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Target" {
		t.Fatalf("schema = %#v, want dispatch target columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per dispatch target", exchange.Result)
	}
}

func TestPlanningServiceListClientDispatchTargetsReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientDispatchTargets(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientDispatchTargetsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientDispatchTargets(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][6].Value = "mutated"

	again := service.ListClientDispatchTargets(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Detail == "mutated" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Target" || again.ResultSchema.Columns[0].Name != "Target" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][6].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func dispatchTargetProfileByTarget(profiles []DispatchTargetProfile, target DispatchTarget) *DispatchTargetProfile {
	for i := range profiles {
		if profiles[i].Target == target {
			return &profiles[i]
		}
	}
	return nil
}
