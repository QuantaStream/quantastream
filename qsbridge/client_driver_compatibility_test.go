package qsbridge

import "testing"

func TestPlanningServiceListClientDriverCompatibilityReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientDriverCompatibility(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported driver compatibility metadata", exchange)
	}
	if len(exchange.Profiles) != len(DefaultClientDriverCompatibility()) {
		t.Fatalf("profiles = %#v, want default driver profiles", exchange.Profiles)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Name" {
		t.Fatalf("schema = %#v, want driver compatibility columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Profiles)) {
		t.Fatalf("result = %#v, want one row per driver profile", exchange.Result)
	}
	if !driverCompatibilityRowsContain(exchange.Profiles, ClientDriverEcosystemNodeJS) ||
		!driverCompatibilityRowsContain(exchange.Profiles, ClientDriverEcosystemPython) ||
		!driverCompatibilityRowsContain(exchange.Profiles, ClientDriverEcosystemJava) ||
		!driverCompatibilityRowsContain(exchange.Profiles, ClientDriverEcosystemGo) ||
		!driverCompatibilityRowsContain(exchange.Profiles, ClientDriverEcosystemGRPC) {
		t.Fatalf("profiles = %#v, want primary client ecosystems", exchange.Profiles)
	}
}

func TestPlanningServiceListClientDriverCompatibilityFiltersByPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.ListClientDriverCompatibility(clientStatementConnection(), "mysql2")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered metadata", exchange)
	}
	if len(exchange.Profiles) != 1 || exchange.Profiles[0].Ecosystem != ClientDriverEcosystemNodeJS {
		t.Fatalf("profiles = %#v, want mysql2 Node.js profile", exchange.Profiles)
	}
}

func TestPlanningServiceListClientDriverCompatibilityReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhaseBind, "blocked")}

	exchange := service.ListClientDriverCompatibility(connection, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocked connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want failed driver compatibility envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientDriverCompatibilityCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientDriverCompatibility(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Profiles[0].Drivers[0] = "mutated"
	exchange.Profiles[0].Capabilities[0] = "mutated"
	exchange.Profiles[0].AuthPlugins[0] = "mutated"
	exchange.Result.Chunks[0].Rows[0][4].Value = "mutated"

	again := service.ListClientDriverCompatibility(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Profiles[0].Drivers[0] == "mutated" || again.Profiles[0].Capabilities[0] == "mutated" || again.Profiles[0].AuthPlugins[0] == "mutated" {
		t.Fatalf("profiles leaked mutation: %#v", again.Profiles[0])
	}
	if again.Result.Chunks[0].Rows[0][4].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks[0].Rows[0])
	}
}

func driverCompatibilityRowsContain(profiles []ClientDriverCompatibility, ecosystem ClientDriverEcosystem) bool {
	for _, profile := range profiles {
		if profile.Ecosystem == ecosystem {
			return true
		}
	}
	return false
}
