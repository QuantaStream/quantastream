package qsbridge

import "testing"

func TestPlanningServiceListClientTopologyFiltersAndBuildsResult(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	placements := []ClientShardPlacement{
		{Schema: "quanta", Table: "orders", Shard: "orders-002", Replica: "replica-b", Role: ClientReplicaRoleFollower, Region: "us", Zone: "us-b", Address: "10.0.0.2:4000", Healthy: true},
		{Schema: "quanta", Table: "orders", Shard: "orders-001", Replica: "replica-a", Role: ClientReplicaRolePrimary, Region: "us", Zone: "us-a", Address: "10.0.0.1:4000", Healthy: true, Local: true},
		{Schema: "quanta", Table: "partsupp", Shard: "partsupp-001", Replica: "replica-c", Role: ClientReplicaRolePrimary, Healthy: true},
	}

	exchange := service.ListClientTopology(connection, placements, "orders%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported topology metadata", exchange)
	}
	if exchange.Pattern != "orders%" || len(exchange.Placements) != 2 {
		t.Fatalf("exchange = %#v, want filtered order placements", exchange)
	}
	if exchange.Placements[0].Shard != "orders-001" || exchange.Placements[1].Shard != "orders-002" {
		t.Fatalf("placements = %#v, want sorted shard placements", exchange.Placements)
	}
	if len(exchange.ResultSchema.Columns) != 10 || exchange.ResultSchema.Columns[2].Name != "Shard" || exchange.ResultSchema.Columns[8].Name != "Healthy" {
		t.Fatalf("schema = %#v, want topology result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 2 || exchange.Result.Chunks[0].Rows[0][3].Value != "replica-a" || exchange.Result.Chunks[0].Rows[0][9].Value != true {
		t.Fatalf("result rows = %#v, want topology rows", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientTopologyFiltersByReplica(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	placements := []ClientShardPlacement{
		{Schema: "quanta", Table: "orders", Shard: "orders-001", Replica: "replica-a"},
		{Schema: "quanta", Table: "orders", Shard: "orders-001", Replica: "replica-b"},
	}

	exchange := service.ListClientTopology(connection, placements, "replica-b")
	if len(exchange.Placements) != 1 || exchange.Placements[0].Replica != "replica-b" {
		t.Fatalf("placements = %#v, want replica filter match", exchange.Placements)
	}
}

func TestPlanningServiceListClientTopologyReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "blocked"),
	}

	exchange := service.ListClientTopology(connection, []ClientShardPlacement{{Schema: "quanta", Table: "orders"}}, "")
	if exchange.Supported() || len(exchange.Placements) != 0 {
		t.Fatalf("exchange = %#v, want unsupported rowless topology metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want failed topology envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientTopologyCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	placements := []ClientShardPlacement{{
		Schema:  "quanta",
		Table:   "orders",
		Shard:   "orders-001",
		Replica: "replica-a",
	}}

	exchange := service.ListClientTopology(connection, placements, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Placements[0].Table = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientTopology(connection, placements, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Placements[0].Table != "orders" {
		t.Fatalf("placements leaked mutation: %#v", again.Placements)
	}
	if again.Result.Columns[0].Name != "Schema" || again.ResultSchema.Columns[0].Name != "Schema" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "quanta" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}
